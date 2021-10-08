// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/vlog.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/options.h"

#include "table/block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct VLog::Rep {
  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
};

Status VLog::Open(const Options& options, RandomAccessFile* file, VLog** vlog) {
  *vlog = nullptr;

  Rep* rep = new VLog::Rep;
  rep->options = options;
  rep->file = file;
  rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
  *vlog = new VLog(rep);

  return Status::OK();
  ;
}

VLog::~VLog() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

Status VLog::InternalGet(const ReadOptions& options, const Slice& ikey,
                         Slice& value_addr, std::string* v, void* arg,
                         void (*handle_result)(void*, const Slice&,
                                               const Slice&)) {
  Status s;
  uint64_t offset, size, offset_in_block;
  if (!(GetVarint64(&value_addr, &offset) && GetVarint64(&value_addr, &size) &&
        GetVarint64(&value_addr, &offset_in_block))) {
    assert(false);
    s = Status::Corruption("bad value address in SSTable");
    return s;
  }
  Cache* vlog_block_cache = rep_->options.vlog_block_cache;
  Block* vlog_block = nullptr;
  Cache::Handle* vlog_cache_handle = nullptr;
  BlockHandle handle;
  handle.set_offset(offset);
  handle.set_size(size);
  BlockContents contents;
  if (vlog_block_cache != nullptr) {
    char cache_key_buffer[16];
    EncodeFixed64(cache_key_buffer, rep_->cache_id);
    EncodeFixed64(cache_key_buffer + 8, handle.offset());
    Slice key(cache_key_buffer, sizeof(cache_key_buffer));
    vlog_cache_handle = vlog_block_cache->Lookup(key);
    if (vlog_cache_handle != nullptr) {
      vlog_block =
          reinterpret_cast<Block*>(vlog_block_cache->Value(vlog_cache_handle));
    } else {
      s = ReadBlock(rep_->file, options, handle, &contents);
      if (s.ok()) {
        vlog_block = new Block(contents, false);
        if (contents.cachable && options.fill_cache && v == nullptr) {
          vlog_cache_handle = vlog_block_cache->Insert(
              key, vlog_block, vlog_block->size(), &DeleteCachedBlock);
        }
      }
    }
  } else {
    s = ReadBlock(rep_->file, options, handle, &contents);
    if (s.ok()) {
      vlog_block = new Block(contents, false);
    }
  }

  if (vlog_block != nullptr) {
    Slice res;
    s = vlog_block->ValueAt(offset_in_block, res);
    if (!s.ok()) {
      return s;
    }
    if (v == nullptr) {
      (*handle_result)(arg, ikey, res);
    } else {
      v->assign(res.data(), res.size());
    }
    if (vlog_cache_handle == nullptr) {
      DeleteBlock(vlog_block, nullptr);
    } else {
      ReleaseBlock(vlog_block_cache, vlog_cache_handle);
    }
  } else {
    assert(false);
    s = Status::Corruption("bad block in VLog");
  }
  return s;
}

}  // namespace leveldb
