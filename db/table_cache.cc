// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "leveldb/vlog.h"
#include "util/coding.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  union
  {
    Table* table;
    VLog* vlog;
  } ptr;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->ptr.table;
  delete tf->file;
  delete tf;
}

static void DeleteVLogEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->ptr.vlog;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->ptr.table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Status TableCache::FindVLog(uint64_t file_number, Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = VLogFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    VLog* vlog = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (s.ok()) {
      s = VLog::Open(options_, file, &vlog);
    }

    if (!s.ok()) {
      assert(vlog == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->ptr.vlog = vlog;
      *handle = cache_->Insert(key, tf, 1, &DeleteVLogEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->ptr.table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  std::string ikey_str, v_str;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->ptr.table;
    s = t->InternalGet(options, k, arg, ikey_str, v_str, handle_result);
    cache_->Release(handle);
  }
  if (!s.ok() || ikey_str.size() == 0) {
    return s;
  }
  Slice ikey(ikey_str);
  ParsedInternalKey internal_key;
  ParseInternalKey(ikey, &internal_key);
  ValueType value_type = internal_key.type;
  if (value_type == kTypeAddress &&
      internal_key.user_key.compare(ExtractUserKey(k)) == 0) {
    Cache::Handle* vlog_handle = nullptr;
    Slice v_addr(v_str);
    uint64_t vfnum;
    if ( !GetVarint64(&v_addr, &vfnum) ) {
      s = Status::Corruption("bad vlog file number in SSTable");
      return s;
    }
    s = FindVLog(vfnum, &vlog_handle);
    if (s.ok()) {
      VLog* l = reinterpret_cast<TableAndFile*>(cache_->Value(vlog_handle))->ptr.vlog;
      s = l->InternalGet(options, ikey, v_addr, nullptr, arg, handle_result);
      cache_->Release(vlog_handle);
    }
    return s;
  }
  return Status::NotFound("No such key");
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

VLog* TableCache::GetVLogCached(Cache::Handle* vlog_handle) {
    return reinterpret_cast<TableAndFile*>(cache_->Value(vlog_handle))->ptr.vlog;
}

class TableCache::VLogIterator : public Iterator {
 public:
  VLogIterator(TableCache* table_cache, const ReadOptions& options, Iterator* iter):
      table_cache_(table_cache), table_iter_(iter), options_(options) {}
  ~VLogIterator() = default;
  void Seek(const Slice& target) override {
    table_iter_.Seek(target);
    
  }
  void SeekToFirst() override {
    table_iter_.SeekToFirst();
    if (table_iter_.Valid()) {
      GetValue();
    }
  }
  void SeekToLast() override {
    table_iter_.SeekToLast();
    if (table_iter_.Valid()) {
      GetValue();
    }
  }
  void Next() override {
    table_iter_.Next();
    if (table_iter_.Valid()) {
      GetValue();
    }
  }
  void Prev() override {
    table_iter_.Prev();
    if (table_iter_.Valid()) {
      GetValue();
    }
  }
  bool Valid() const override { 
    if (!table_iter_.Valid()) {
      return table_iter_.Valid();
    }
    return status_.ok();
  }
  Slice key() const override {
    assert(Valid());
    return table_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return value_;
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!table_iter_.status().ok()) {
      return table_iter_.status();
    }
    return status_;
  }
 private:
  IteratorWrapper table_iter_;
  std::string value_str_;
  Slice value_;
  const ReadOptions options_;
  Status status_;
  TableCache* const table_cache_;

  void GetValue() {
    Slice ikey = table_iter_.key();
    value_ = Slice();
    ParsedInternalKey internal_key;
    if (!ParseInternalKey(ikey, &internal_key)) {
      return;
    }
    ValueType value_type = internal_key.type;
    if (value_type != kTypeAddress) {
      value_ = table_iter_.value();
      return;
    }
    Cache::Handle* vlog_handle = nullptr;
    Slice v_addr = table_iter_.value();
    uint64_t vfnum;
    if (!GetVarint64(&v_addr, &vfnum)) {
      status_ = Status::Corruption("bad vlog file number in SSTable");
      return;
    }
    status_ = table_cache_->FindVLog(vfnum, &vlog_handle);
    if (!status_.ok()) {
      return;
    }
    VLog* l = table_cache_->GetVLogCached(vlog_handle);
    status_ = l->InternalGet(options_, ikey, v_addr, &value_str_);
    table_cache_->ReleaseVLogCached(vlog_handle);
    if (!status_.ok()) {
      return;
    }
    value_ = Slice(value_str_);
  }
};

Iterator* NewVLogIterator(TableCache* table_cache, const ReadOptions& options,
                          uint64_t file_num, uint64_t file_size) {
  Iterator* iter = table_cache->NewIterator(options, file_num, file_size);
  return new TableCache::VLogIterator(table_cache, options, iter);
}

}  // namespace leveldb
