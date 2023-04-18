/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <memory>
#include <time.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/UncompressedCache.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <Compression/CompressedReadBufferBase.h>

namespace DB
{


/** A buffer for reading from a compressed file using the cache of decompressed blocks.
  * The external cache is passed as an argument to the constructor.
  * Allows you to increase performance in cases where the same blocks are often read.
  * Disadvantages:
  * - in case you need to read a lot of data in a row, but of them only a part is cached, you have to do seek-and.
  */
class CachedCompressedReadBuffer : public CompressedReadBufferBase, public ReadBuffer
{
private:
    const std::string path;
    size_t begin_offset;
    std::optional<size_t> end_offset;
    UncompressedCache * cache;
    std::function<std::unique_ptr<ReadBufferFromFileBase>()> reader_creator;

    std::unique_ptr<ReadBufferFromFileBase> raw_reader;
    std::unique_ptr<LimitSeekableReadBuffer> limit_reader;

    size_t file_pos;

    /// A piece of data from the cache, or a piece of read data that we put into the cache.
    UncompressedCache::MappedPtr owned_cell;

    void initInput();
    bool nextImpl() override;

    /// Passed into file_in.
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type {};

public:
    CachedCompressedReadBuffer(
        const std::string& path_, std::function<std::unique_ptr<ReadBufferFromFileBase>()> reader_creator_,
        UncompressedCache* cache_, bool allow_different_codecs_ = false,
        off_t begin_offset_ = 0, std::optional<size_t> end_offset_ = std::nullopt);

    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        profile_callback = profile_callback_;
        clock_type = clock_type_;
    }

    String getPath() const
    {
        return path;
    }

    size_t getSizeCompressed() const { return owned_cell->compressed_size; }

    size_t compressedOffset() const
    {
        return file_pos;
    }
};

}
