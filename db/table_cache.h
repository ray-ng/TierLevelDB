// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include "db/dbformat.h"
#include <stdint.h>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/iterator.h"
#include "leveldb/table.h"

#include "port/port.h"

namespace leveldb {

class Env;
class VLog;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

  VLog* GetVLogCached(Cache::Handle* vlog_handle);
  Table* GetTableCached(Cache::Handle* table_handle);

  inline void ReleaseVLogCached(Cache::Handle* vlog_handle) {
    cache_->Release(vlog_handle);
  };

  class VLogIterator;

 private:
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
  Status FindVLog(uint64_t file_number, Cache::Handle**);

  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
};

Iterator* NewVLogIterator(TableCache* table_cache_, const ReadOptions& options,
                          uint64_t file_num, uint64_t file_size);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
