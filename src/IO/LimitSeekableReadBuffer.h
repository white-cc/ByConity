#pragma once

#include <optional>
#include <Core/Types.h>
#include <IO/SeekableReadBuffer.h>

namespace DB
{

class LimitSeekableReadBuffer: public SeekableReadBuffer
{
public:
    // size == nullopt means this file has no limit
    LimitSeekableReadBuffer(SeekableReadBuffer& in_, UInt64 offset_,
        std::optional<UInt64> size_ = std::nullopt, bool should_throw_ = false);

    virtual off_t seek(off_t offset, int whence) override;

    virtual off_t getPosition() override
    {
        return underlying_cursor - (working_buffer.end() - position());
    }

private:
    virtual bool nextImpl() override;

    void adoptInputBuffer();

    SeekableReadBuffer& in;
    UInt64 range_start;
    // range_end_ == nullopt means in's end
    std::optional<UInt64> range_end;
    bool should_throw;

    // Underlying cursor of current working buffer end
    UInt64 underlying_cursor;
};

}
