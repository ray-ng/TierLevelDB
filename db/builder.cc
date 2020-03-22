// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter,
                  FileMetaData* meta, FileMetaData* vmeta) {
  Status s;
  meta->file_size = 0;
  vmeta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  std::string vfname = VLogFileName(dbname, vmeta->number);
  if (iter->Valid()) {
    WritableFile* vfile;
    s = env->NewWritableFile(vfname, &vfile);
    if (!s.ok()) {
      return s;
    }
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    VLogBuilder* vbuilder = new VLogBuilder(options, vfile, builder, vmeta->number);
    vmeta->smallest.DecodeFrom(iter->key());
    meta->smallest.DecodeFrom(iter->key());
    uint64_t cnt = 0;
    for (; iter->Valid(); iter->Next()) {
      cnt++;
      Slice key = iter->key();
      vmeta->largest.DecodeFrom(key);
      meta->largest.DecodeFrom(key);
      vbuilder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    s = vbuilder->Finish();
    assert(cnt == vbuilder->cnt_);
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      vmeta->file_size = vbuilder->FileSize();
      assert(meta->file_size > 0);
    }
    delete vbuilder;
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = vfile->Sync();
    }
    if (s.ok()) {
      s = vfile->Close();
    }
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    delete vfile;
    file = nullptr;
    vfile = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
    env->RemoveFile(vfname);
  }
  return s;
}

}  // namespace leveldb
