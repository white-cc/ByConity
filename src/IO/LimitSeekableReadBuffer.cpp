#include <IO/LimitSeekableReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

LimitSeekableReadBuffer::LimitSeekableReadBuffer(SeekableReadBuffer& in,
    UInt64 offset_, std::optional<UInt64> size_, bool should_throw_):
        SeekableReadBuffer(nullptr, 0), in(in), range_start(offset_),
        range_end(std::nullopt), should_throw(should_throw_),
        underlying_cursor(0)
{
    if (size_.has_value())
        range_end = range_start + size_.value();

    in.seek(range_start, SEEK_SET);

    adoptInputBuffer();
}

off_t LimitSeekableReadBuffer::seek(off_t offset, int whence)
{
    if (whence != SEEK_SET)
        throw Exception("Seek mode " + std::to_string(whence) + " not supported",
            ErrorCodes::BAD_ARGUMENTS);

    UInt64 under_buf_start = underlying_cursor - working_buffer.size();
    UInt64 under_new_pos = range_start + offset;
    if (under_new_pos >= under_buf_start && under_new_pos <= underlying_cursor)
    {
        // Seek in current working buffer
        pos = working_buffer.begin() + (under_new_pos - under_buf_start);
        return offset;
    }
    else
    {
        if (under_new_pos < range_start || (range_end.has_value() && under_new_pos > range_end.value()))
            throw Exception("Try to seek to " + std::to_string(under_new_pos) \
                + ", which outside range " + std::to_string(range_start) + "-" \
                + (range_end.has_value() ? std::to_string(range_end.value()) : "eof"), \
                ErrorCodes::BAD_ARGUMENTS);

        offset = in.seek(under_new_pos, whence);

        adoptInputBuffer();

        return offset;
    }
}

void LimitSeekableReadBuffer::adoptInputBuffer()
{
    UInt64 under_buf_end = in.getPosition() + (in.buffer().end() - in.position());
    if (in.hasPendingData())
    {
        UInt64 under_buf_start = under_buf_end - in.buffer().size();

        UInt64 usable_start_offset = under_buf_start < range_start ? range_start - under_buf_start : 0;
        UInt64 usable_end_offset = in.buffer().size();
        if (range_end.has_value())
        {
            usable_end_offset = under_buf_end > range_end.value() ? in.buffer().size() - (under_buf_end - range_end.value()) : in.buffer().size();
        }

        working_buffer = Buffer(in.buffer().begin() + usable_start_offset, in.buffer().begin() + usable_end_offset);
        pos = working_buffer.begin();

        underlying_cursor = under_buf_start + usable_end_offset;
    }
    else
    {
        working_buffer = in.buffer();
        working_buffer.resize(0);
        pos = working_buffer.begin();

        underlying_cursor = under_buf_end;
    }
}

bool LimitSeekableReadBuffer::nextImpl()
{
    in.position() = in.buffer().end();

    if (range_end.has_value() && underlying_cursor >= range_end.value())
    {
        if (should_throw)
            throw Exception("Attempt to read after eof", ErrorCodes::BAD_ARGUMENTS);
        else
            return false;
    }

    if (!in.next())
        return false;

    UInt64 under_end_pos = in.getPosition() + (in.buffer().end() - in.position());
    UInt64 usable_end_offset = in.buffer().size();
    if (range_end.has_value())
    {
        usable_end_offset = range_end.value() < under_end_pos ? in.buffer().size() - (under_end_pos - range_end.value()) : in.buffer().size();
    }

    working_buffer = Buffer(in.position(), in.buffer().begin() + usable_end_offset);
    pos = working_buffer.begin();
    underlying_cursor = under_end_pos - in.buffer().size() + usable_end_offset;

    return true;
}

}
