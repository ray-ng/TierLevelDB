// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/vlog_block_builder.h"

#include <assert.h>

#include <algorithm>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

VLogBlockBuilder::VLogBlockBuilder(const Options* options)
    : options_(options), counter_(0), finished_(false) {}

void VLogBlockBuilder::Reset() {
  buffer_.clear();
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t VLogBlockBuilder::CurrentSizeEstimate() const {
  return buffer_.size();                       // Raw data buffer
}

Slice VLogBlockBuilder::Finish() {
  // Append restart array
  finished_ = true;
  return Slice(buffer_);
}

void VLogBlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);

  // Add "<value_size>" to buffer_
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(value.data(), value.size());

  // Update state
  last_key_.assign(key.data(), key.size());
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
