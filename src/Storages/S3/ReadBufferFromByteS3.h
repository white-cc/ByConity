#pragma once

#include <memory>
#include <Common/Throttler.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/S3Common.h>
#include <IO/ReadBufferFromFileBase.h>
#include <aws/s3/model/GetObjectResult.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{
/**
 * Perform S3 HTTP GET request and provide response to read.
 */
class ReadBufferFromByteS3 : public ReadBufferFromFileBase
{
private:
    S3::S3Util s3_util;
    String key;
    size_t max_single_read_retries;

    off_t offset_in_object;

    ThrottlerPtr throttler;

    Poco::Logger * log;

public:
    explicit ReadBufferFromByteS3(
        const std::shared_ptr<Aws::S3::S3Client>& client_,
        const String& bucket_,
        const String& key_,
        size_t read_retry_ = 3,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        char* existing_memory_ = nullptr,
        size_t alignment_ = 0,
        const ThrottlerPtr& throttler_ = nullptr);

    virtual bool nextImpl() override;

    virtual off_t getPosition() override
    {
        return offset_in_object - (working_buffer.end() - pos);
    }

    virtual String getFileName() const override
    {
        return s3_util.getBucket() + "/" + key;
    }

protected:
    virtual off_t seek(off_t off, int whence) override;

private:
    bool readFragment();
};

}
