// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_VLOG_H_
#define STORAGE_LEVELDB_INCLUDE_VLOG_H_

#include <stdint.h>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class LEVELDB_EXPORT VLog {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options, RandomAccessFile* file, VLog** vlog);

  VLog(const VLog&) = delete;
  VLog& operator=(const VLog&) = delete;

  ~VLog();

 private:
  friend class TableCache;
  struct Rep;

  Status InternalGet(const ReadOptions& options, const Slice& ikey,
                           Slice& value_addr, std::string* v = nullptr, void* arg = nullptr,
                           void (*handle_result)(void*, const Slice&, const Slice&) = nullptr);

  explicit VLog(Rep* rep) : rep_(rep) {}

  Rep* const rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_VLOG_H_
