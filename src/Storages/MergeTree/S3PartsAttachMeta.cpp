#include <Storages/MergeTree/S3PartsAttachMeta.h>
#include <memory>
#include <mutex>
#include <Common/ThreadPool.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/RAReadBufferFromS3.h>
#include <Disks/DiskByteS3.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

S3PartsLazyCleaner::S3PartsLazyCleaner(const S3::S3Util& s3_util_, size_t batch_clean_size_,
    const String& data_key_prefix_, const S3ObjectMetadata::PartGeneratorID& generator_id_,
    size_t max_threads_):
        logger(&Poco::Logger::get("S3PartsLazyCleaner")), batch_clean_size(batch_clean_size_),
        s3_util(s3_util_), data_key_prefix(data_key_prefix_),
        generator_id(generator_id_), parallel_clean_pool(max_threads_ == 1 ? nullptr : std::make_unique<ThreadPool>(max_threads_))
{
}

void S3PartsLazyCleaner::push(const String& part_relative_key_)
{
    auto task = createExceptionHandledJob([this, part_relative_key_]() {
        String part_key = data_key_prefix + part_relative_key_ + "data";

        if (!s3_util.exists(part_key))
        {
            LOG_DEBUG(logger, fmt::format("Object {} not eixst, skip cleaning",
                part_key));
            return;
        }

        std::map<String, String> object_metadata = s3_util.getObjectMeta(part_key);

        String generator_meta;
        if (auto iter = object_metadata.find(S3ObjectMetadata::PART_GENERATOR_ID);
            iter != object_metadata.end())
        {
            // Verify meta and remove if necessary
            generator_meta = iter->second;

            if (generator_id.verify(generator_meta))
            {
                lazyRemove(part_key);
                return;
            }
        }

        LOG_INFO(logger, fmt::format("Skip part clean since meta not match, "
            "part key: {}, generator metadata: {}", part_key, generator_meta));
    }, except_handler);
    
    if (parallel_clean_pool == nullptr)
    {
        task();
    }
    else
    {
        parallel_clean_pool->schedule(std::move(task));
    }
}

void S3PartsLazyCleaner::finalize()
{
    if (parallel_clean_pool != nullptr)
    {
        parallel_clean_pool->wait();
    }

    // Remove remained parts
    if (!remove_collection.empty())
    {
        s3_util.deleteObjectsInBatch(remove_collection);
    }

    except_handler.throwIfException();
}

void S3PartsLazyCleaner::lazyRemove(const String& part_key_)
{
    std::vector<String> parts_to_remove_this_round;

    {
        std::lock_guard<std::mutex> lock(remove_collection_mu);
        remove_collection.push_back(part_key_);

        if (remove_collection.size() >= batch_clean_size)
        {
            remove_collection.swap(parts_to_remove_this_round);
        }
    }

    if (!parts_to_remove_this_round.empty())
    {
        s3_util.deleteObjectsInBatch(parts_to_remove_this_round);
    }
}

void MultiDiskS3PartsLazyCleaner::push(const DiskPtr& disk_, const String& part_relative_key_)
{
    S3PartsLazyCleaner* cleaner = nullptr;
    {
        std::lock_guard<std::mutex> lock(cleaner_mu);

        auto iter = cleaners.find(disk_->getName());
        if (iter == cleaners.end())
        {
            if (std::shared_ptr<DiskByteS3> s3_disk = std::dynamic_pointer_cast<DiskByteS3>(disk_);
                s3_disk != nullptr)
            {
                std::unique_ptr<S3PartsLazyCleaner> disk_cleaner = std::make_unique<S3PartsLazyCleaner>(
                    s3_disk->getS3Util(), batch_clean_size, s3_disk->getPath(),
                    generator_id, max_threads
                );
                cleaner = disk_cleaner.get();
                cleaners.emplace(disk_->getName(),
                    std::pair<DiskPtr, std::unique_ptr<S3PartsLazyCleaner>>(disk_, std::move(disk_cleaner)));
            }
            else
            {
                throw Exception(fmt::format("Passing a non s3 disk to MultiDiskS3PartsLazyCleaner, disk {} has type {}",
                    disk_->getName(), DiskType::toString(disk_->getType())),
                    ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else
        {
            cleaner = iter->second.second.get();
        }
    }

    cleaner->push(part_relative_key_);
}

void MultiDiskS3PartsLazyCleaner::finalize()
{
    ExceptionHandler except_handler;
    ThreadPool pool(cleaners.size());

    for (auto iter = cleaners.begin(); iter != cleaners.end(); ++iter)
    {
        pool.schedule(createExceptionHandledJob([iter]() {
            iter->second.second->finalize();
        }, except_handler));
    }

    pool.wait();
    except_handler.throwIfException();
}

void S3PartsAttachMeta::Writer::write(const std::vector<PartMeta>& metas_)
{
    String meta_file_key = meta.data_key_prefix + metaFileKey(meta.generator_id.id,
        next_write_idx++);

    {
        WriteBufferFromS3 writer(meta.s3_util.getClient(), meta.s3_util.getBucket(),
            meta_file_key);
        writeVectorBinary(metas_, writer);
        writer.finalize();
    }
}

const std::vector<S3PartsAttachMeta::PartMeta>& S3PartsAttachMeta::Reader::metas()
{
    if (!part_metas.has_value())
    {
        std::vector<String> meta_files = meta.listMetaFiles();
        part_metas = meta.listPartsInMetaFile(meta_files, thread_num);
    }
    return part_metas.value();
}

void S3PartsAttachMeta::Cleaner::clean()
{
    if (!clean_meta && !clean_data)
    {
        return;
    }

    // List all meta files and part files
    std::vector<String> meta_files = meta.listMetaFiles();

    // Clean data objects generated by this task
    if (clean_data)
    {
        std::vector<PartMeta> part_metas = meta.listPartsInMetaFile(meta_files,
            clean_thread_num);
        S3PartsLazyCleaner cleaner(meta.s3_util, 500, meta.data_key_prefix,
            meta.generator_id, clean_thread_num);
        for (const PartMeta& part_meta : part_metas)
        {
            cleaner.push(part_meta.second + '/');
        }
        cleaner.finalize();
    }

    // Clean meta files
    if (clean_meta)
    {
        meta.s3_util.deleteObjectsInBatch(meta_files);
    }
}

S3PartsAttachMeta::S3PartsAttachMeta(const std::shared_ptr<Aws::S3::S3Client>& client_,
    const String& bucket_, const String& data_prefix_, const String& generator_id_):
        data_key_prefix(data_prefix_),
        generator_id(S3ObjectMetadata::PartGeneratorID::PART_WRITER, generator_id_),
        s3_util(client_, bucket_) {}

String S3PartsAttachMeta::metaPrefix(const String& task_id_)
{
    return fmt::format("attach_meta/{}/", task_id_);
}

String S3PartsAttachMeta::metaFileKey(const String& task_id_, size_t idx_)
{
    return fmt::format("attach_meta/{}/{}.am", task_id_, idx_);
}

std::vector<String> S3PartsAttachMeta::listMetaFiles()
{
    std::vector<String> all_keys;
    String meta_prefix = data_key_prefix + metaPrefix(generator_id.id);

    bool more = false;
    std::optional<String> token = std::nullopt;
    std::vector<String> keys;

    do
    {
        std::tie(more, token, keys) = s3_util.listObjectsWithPrefix(meta_prefix, token);

        all_keys.reserve(all_keys.size() + keys.size());
        std::for_each(keys.begin(), keys.end(), [&all_keys](const String& key) {
            if (endsWith(key, ".am"))
            {
                all_keys.push_back(key);
            }
        });
    } while(more);

    return all_keys;
}

std::vector<S3PartsAttachMeta::PartMeta> S3PartsAttachMeta::listPartsInMetaFile(
    const std::vector<String>& meta_files_, size_t thread_num_)
{
    std::mutex mu;
    std::vector<PartMeta> part_metas;

    ExceptionHandler handler;
    ThreadPool pool(std::min(thread_num_, meta_files_.size()));
    for (const String& file : meta_files_)
    {
        pool.schedule(createExceptionHandledJob([this, file, &mu, &part_metas]() {
            RAReadBufferFromS3 reader(s3_util.getClient(), s3_util.getBucket(), file);
            std::vector<PartMeta> partial_metas;
            readVectorBinary(partial_metas, reader);

            std::lock_guard lock(mu);
            part_metas.reserve(part_metas.size() + partial_metas.size());
            part_metas.insert(part_metas.end(), partial_metas.begin(), partial_metas.end());
        }, handler));
    }
    pool.wait();
    handler.throwIfException();

    return part_metas;
}

}