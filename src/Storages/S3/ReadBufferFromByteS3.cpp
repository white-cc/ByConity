
#include <Storages/S3/ReadBufferFromByteS3.h>
#include <IO/ReadBufferFromIStream.h>
#include <Common/Stopwatch.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>

#include <utility>

namespace ProfileEvents
{
    extern const Event ReadBufferFromS3Read;
    extern const Event ReadBufferFromS3ReadFailed;
    extern const Event ReadBufferFromS3ReadBytes;
    extern const Event ReadBufferFromS3ReadMS;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
}


ReadBufferFromByteS3::ReadBufferFromByteS3(
    const std::shared_ptr<Aws::S3::S3Client>& client_, const String& bucket_,
    const String& key_, size_t read_retry_, size_t buffer_size_,
    char* existing_memory_, size_t alignment_, const ThrottlerPtr& throttler_)
    : ReadBufferFromFileBase(buffer_size_, existing_memory_, alignment_)
    , s3_util(client_, bucket_)
    , key(key_)
    , max_single_read_retries(read_retry_)
    , offset_in_object(0)
    , throttler(throttler_)
    , log(&Poco::Logger::get("ReadBufferFromS3"))
{}

bool ReadBufferFromByteS3::nextImpl()
{
    auto sleep_time_with_backoff_milliseconds = std::chrono::milliseconds(100);
    for (size_t attempt = 0; true; ++attempt)
    {
        try
        {
            bool more_data = readFragment();

            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadBytes, buffer().size());
            offset_in_object += buffer().size();

            return more_data;
        }
        catch (const Exception& e)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadFailed, 1);

            LOG_INFO(log, "Caught exception while reading S3 object. Bucket: {}, Key: {}, Offset: {}, Attempt: {}, Message: {}, Backoff: {} ms.",
                s3_util.getBucket(), key, offset_in_object, attempt, e.message(), sleep_time_with_backoff_milliseconds.count());

            if (attempt >= max_single_read_retries)
                throw e;

            std::this_thread::sleep_for(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }
}

off_t ReadBufferFromByteS3::seek(off_t off, int whence)
{
    if (whence != SEEK_SET)
    {
        throw Exception("ReadBufferFromS3::seek expects SEEK_SET as whence",
            ErrorCodes::BAD_ARGUMENTS);
    }

    // Seek in buffer(or end of current buffer)
    off_t buffer_start_offset = offset_in_object - static_cast<off_t>(working_buffer.size());
    if (hasPendingData() && off <= offset_in_object && off >= buffer_start_offset)
    {
        pos = working_buffer.begin() + off - buffer_start_offset;
    }
    else
    {
        pos = working_buffer.end();
        offset_in_object = off;
    }
    return off;
}

bool ReadBufferFromByteS3::readFragment()
{
    bool more = false;
    {
        Stopwatch watch;
        SCOPE_EXIT({
            auto time = watch.elapsedMicroseconds();
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadMS, watch.elapsedMicroseconds());

            LOG_TRACE(log, "Read S3 object. Bucket: {}, key: {}, Offset: {}, Size: {}, Time: {}.",
                s3_util.getBucket(), key, offset_in_object, internalBuffer().size(), time);
        });

        ProfileEvents::increment(ProfileEvents::ReadBufferFromS3Read, 1);

        more = s3_util.read(key, offset_in_object, internalBuffer().size(),
            buffer());
    }

    if (throttler)
    {
        throttler->add(buffer().size());
    }

    return more;
}

}
