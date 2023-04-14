#pragma once

#include <memory>
#include <IO/S3Common.h>
#include <Storages/MergeTree/S3ObjectMetadata.h>
#include <Poco/Logger.h>

namespace DB
{

class S3PartsLazyCleaner
{
public:
    S3PartsLazyCleaner(const S3::S3Util& s3_util_, size_t batch_clean_size_,
        const String& data_key_prefix_, const S3ObjectMetadata::PartGeneratorID& generator_id_,
        size_t max_threads_);

    void push(const String& part_id_);
    void finalize();

private:
    void lazyRemove(const String& part_key_);

    Poco::Logger* logger;

    size_t batch_clean_size;
    const S3::S3Util& s3_util;
    String data_key_prefix;
    S3ObjectMetadata::PartGeneratorID generator_id;

    ExceptionHandler except_handler;
    std::unique_ptr<ThreadPool> parallel_clean_pool;

    std::mutex remove_collection_mu;
    std::vector<String> remove_collection;
};

class MultiDiskS3PartsLazyCleaner
{
public:
    MultiDiskS3PartsLazyCleaner(size_t batch_clean_size_,
        const S3ObjectMetadata::PartGeneratorID& generator_id_):
            batch_clean_size(batch_clean_size_), generator_id(generator_id_) {}

    void push(const DiskPtr& disk_, const String& part_id_);
    void finalize();

private:
    size_t max_threads;
    size_t batch_clean_size;
    S3ObjectMetadata::PartGeneratorID generator_id;

    std::mutex cleaner_mu;
    std::map<String, std::pair<DiskPtr, std::unique_ptr<S3PartsLazyCleaner>>> cleaners;
};

class S3PartsAttachMeta
{
public:
    // First is part name, second is part id
    using PartMeta = std::pair<String, String>;

    class Writer
    {
    public:
        Writer(S3PartsAttachMeta& meta_): meta(meta_), next_write_idx(0) {}

        void write(const std::vector<PartMeta>& metas_);

    private:
        S3PartsAttachMeta& meta;

        size_t next_write_idx;

        std::vector<PartMeta> part_metas;
    };

    class Reader
    {
    public:
        Reader(S3PartsAttachMeta& meta_, size_t thread_num_): meta(meta_),
            thread_num(thread_num_) {}

        const std::vector<PartMeta>& metas();

    private:
        S3PartsAttachMeta& meta;

        const size_t thread_num;

        std::optional<std::vector<PartMeta>> part_metas;
    };

    class Cleaner
    {
    public:
        Cleaner(S3PartsAttachMeta& meta_, bool clean_meta_, bool clean_data_, size_t thread_num_):
            meta(meta_), clean_meta(clean_meta_), clean_data(clean_data_),
            clean_thread_num(thread_num_) {}

        void clean();

    private:
        S3PartsAttachMeta& meta;

        const bool clean_meta;
        const bool clean_data;
        const size_t clean_thread_num;
    };

    static String metaPrefix(const String& task_id_);
    static String metaFileKey(const String& task_id_, size_t idx_);

    S3PartsAttachMeta(const std::shared_ptr<Aws::S3::S3Client>& client_,
        const String& bucket_, const String& data_prefix_, const String& generator_id_);

    const S3ObjectMetadata::PartGeneratorID& id() const { return generator_id; }

private:

    std::vector<String> listMetaFiles();
    std::vector<PartMeta> listPartsInMetaFile(const std::vector<String>& meta_files,
        size_t thread_num);

    const String data_key_prefix;
    const S3ObjectMetadata::PartGeneratorID generator_id;

    S3::S3Util s3_util;
};

}