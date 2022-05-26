//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() {
  //  mu_.lock();
  //  replace_list_.clear();
  //  hash_replace_map_.clear();
  std::destroy(replace_list_.begin(), replace_list_.end());
  std::destroy(hash_replace_map_.begin(), hash_replace_map_.end());
  //  mu_.unlock();
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  mu_.lock();
  if (hash_replace_map_.empty()) {
    mu_.unlock();
    return false;
  }
  *frame_id = *replace_list_.crbegin();
  hash_replace_map_.erase(*frame_id);
  replace_list_.pop_back();

  mu_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  mu_.lock();
  auto it = hash_replace_map_.find(frame_id);
  if (it != hash_replace_map_.end()) {
    replace_list_.erase(hash_replace_map_[frame_id]);
    hash_replace_map_.erase(it);
  }
  mu_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mu_.lock();
  auto it = hash_replace_map_.find(frame_id);
  if (it == hash_replace_map_.end()) {
    replace_list_.push_front(frame_id);
    hash_replace_map_[frame_id] = replace_list_.begin();
  }
  mu_.unlock();
}

size_t LRUReplacer::Size() { return replace_list_.size(); }

}  // namespace bustub
