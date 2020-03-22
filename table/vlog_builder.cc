// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/vlog_builder.h"

#include <assert.h>
#include <vector>

#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/vlog_block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct VLogBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        file(f),
        offset(0),
        // cur_offset_in_block(0),
        saved_values_size(0),
        data_block(&options),
        num_entries(0),
        closed(false),
        pending_index_entry(false) {}

  Options options;
  WritableFile* file;
  uint64_t offset;
  // uint64_t cur_offset_in_block;
  Status status;
  VLogBlockBuilder data_block;
  std::vector<InternalKey> saved_keys;
  std::vector<Slice> saved_values;
  uint64_t saved_values_size;
  std::vector<uint64_t> offsets_in_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

VLogBuilder::VLogBuilder(const Options& options, WritableFile* file, TableBuilder* builder, uint64_t vfnum) :
                        rep_(new Rep(options, file)), builder_(builder), vfnum_(vfnum), cnt_(0) {}

VLogBuilder::~VLogBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_;
}

Status VLogBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  return Status::OK();
}

void VLogBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    assert(r->offsets_in_block.size() + r->saved_values.size() == r->saved_keys.size());
    std::string handle_encoding;
    for (size_t i = 0, j = 0, k = 0; i < r->saved_keys.size(); i++) {
      ParsedInternalKey internal_key;
      ParseInternalKey(r->saved_keys[i].Encode(), &internal_key);
      ValueType value_type = internal_key.type;
      if (value_type == kTypeAddress) {
        assert(false);
        PutVarint64(&handle_encoding, vfnum_);
        r->pending_handle.EncodeTo(&handle_encoding);
        PutVarint64(&handle_encoding, r->offsets_in_block[j]);
        builder_->Add(r->saved_keys[i].Encode(), Slice(handle_encoding));
        j++;
      } else {
        assert(k < r->saved_values.size());
        builder_->Add(r->saved_keys[i].Encode(), r->saved_values[k]);
        k++;
        cnt_++;
      }
      handle_encoding.clear();
    }
    // r->cur_offset_in_block = 0;
    r->saved_values_size = 0;
    r->offsets_in_block.clear();
    r->saved_keys.clear();
    r->saved_values.clear();
    r->pending_index_entry = false;
  }
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  ParsedInternalKey internal_key;
  if(!ParseInternalKey(key, &internal_key)) {
    assert(false);
    return;
  }
  ValueType value_type = internal_key.type;
  if (value_type == kTypeDeletion || (value_type == kTypeValue && value.size() <= config::kSepSizeGate)) {
    r->saved_keys.emplace_back(internal_key.user_key, internal_key.sequence, internal_key.type);
    r->saved_values.push_back(value);
    r->saved_values_size += value.size();
  } else {
    assert(false);
    r->offsets_in_block.push_back(r->data_block.offset());
    r->data_block.Add(key, value);
    // r->cur_offset_in_block += value.size();
    r->saved_keys.emplace_back(internal_key.user_key, internal_key.sequence, kTypeAddress);
  }
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size ||
      r->saved_values_size >= builder_->BlockSizeGate()) {
    Flush();
  }
}

void VLogBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  assert(ok());
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
}

void VLogBuilder::WriteBlock(VLogBlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void VLogBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status VLogBuilder::status() const { return rep_->status; }

Status VLogBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;
  Status s;

  assert(ok());
  if (ok()) {
    // if (r->pending_index_entry) {
    assert(r->data_block.empty());
    assert(r->offsets_in_block.size() + r->saved_values.size() == r->saved_keys.size());
    std::string handle_encoding;
    for (size_t i = 0, j = 0, k = 0; i < r->saved_keys.size(); i++) {
      ParsedInternalKey internal_key;
      if(!ParseInternalKey(r->saved_keys[i].Encode(), &internal_key)) {
        assert(false);
        return Status::Corruption(Slice());
      }
      ValueType value_type = internal_key.type;
      if (value_type == kTypeAddress) {
        assert(false);
        PutVarint64(&handle_encoding, vfnum_);
        r->pending_handle.EncodeTo(&handle_encoding);
        PutVarint64(&handle_encoding, r->offsets_in_block[j]);
        builder_->Add(r->saved_keys[i].Encode(), Slice(handle_encoding));
        j++;
      } else {
        assert(k < r->saved_values.size());
        builder_->Add(r->saved_keys[i].Encode(), r->saved_values[k]);
        k++;
        cnt_++;
      }
      handle_encoding.clear();
    }
    // r->cur_offset_in_block = 0;
    r->saved_values_size = 0;
    r->offsets_in_block.clear();
    r->saved_keys.clear();
    r->saved_values.clear();
    r->pending_index_entry = false;
    // }
  }
  if (!ok()) {
    return r->status;
  }
  s = builder_->Finish();
  return s;
}

void VLogBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t VLogBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t VLogBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
