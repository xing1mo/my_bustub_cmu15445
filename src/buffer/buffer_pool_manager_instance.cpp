//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(static_cast<page_id_t>(instance_index_)),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
  page_table_.clear();
  frame_to_page_.clear();
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lck(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  // write data to disk
  int frame_id = page_table_[page_id];
  //  pages_[frame_id].RLatch();
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  //  pages_[frame_id].RUnlatch();

  //  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> lck(latch_);
  //  latch_.lock();
  for (auto item : page_table_) {
    //    pages_[item.second].RLatch();
    disk_manager_->WritePage(item.first, pages_[item.second].GetData());
    //    pages_[item.second].RUnlatch();
  }
  //  latch_.unlock();
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  // 自动释放锁
  std::lock_guard<std::mutex> lck(latch_);
  auto frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = *free_list_.begin();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
  }
  if (frame_id == -1) {
    return nullptr;
  }

  if (frame_to_page_.count(frame_id) != 0) {
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
    }
    page_table_.erase(pages_[frame_id].page_id_);
  }

  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;
  frame_to_page_[frame_id] = *page_id;
  pages_[frame_id].ResetMemory();
  // ????privata data ,why?
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->WritePage(*page_id, pages_[frame_id].GetData());

  return &pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lck(latch_);
  if (page_table_.count(page_id) == 0) {
    auto frame_id = -1;
    if (!free_list_.empty()) {
      frame_id = *free_list_.begin();
      free_list_.pop_front();
    } else {
      replacer_->Victim(&frame_id);
    }
    if (frame_id == -1) {
      return nullptr;
    }

    if (frame_to_page_.count(frame_id) != 0) {
      if (pages_[frame_id].is_dirty_) {
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
      }
      page_table_.erase(pages_[frame_id].page_id_);
    }
    page_table_[page_id] = frame_id;
    frame_to_page_[frame_id] = page_id;

    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

    return &pages_[frame_id];
  }
  auto frame_id = page_table_[page_id];
  replacer_->Pin(frame_id);
  pages_[frame_id].pin_count_++;
  return &pages_[frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lck(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  pages_[frame_id].ResetMemory();
  replacer_->Pin(frame_id);
  page_table_.erase(page_id);
  frame_to_page_.erase(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lck(latch_);
  if (page_table_.count(page_id) == 0 || pages_[page_table_[page_id]].pin_count_ <= 0) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  --pages_[frame_id].pin_count_;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += static_cast<page_id_t>(num_instances_);
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
