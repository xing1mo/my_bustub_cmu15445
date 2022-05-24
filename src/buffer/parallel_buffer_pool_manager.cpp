//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : pool_size_(num_instances * pool_size), num_instances_(num_instances) {
  // Allocate and create individual BufferPoolManagerInstances
  buffer_pool_manager_array_ = alloc_.allocate(num_instances);
  start_index_ = 0;
  for (uint32_t i = 0; i < num_instances; ++i) {
    alloc_.construct(buffer_pool_manager_array_ + i, pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (uint32_t i = 0; i < num_instances_; ++i) {
    alloc_.destroy(buffer_pool_manager_array_ + i);
  }
  alloc_.deallocate(buffer_pool_manager_array_, num_instances_);
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pool_manager_array_ + (page_id % num_instances_);
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->FetchPgImp(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->UnpinPgImp(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->FlushPgImp(page_id);

}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
//  std::lock_guard<std::mutex> lck(latch_);
  Page *result = nullptr;
  for (uint32_t i = 0; i < num_instances_; ++i) {
    result = dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager_array_ + ((i + start_index_) % num_instances_))->NewPgImp(page_id);
    if (result != nullptr) {
      break ;
    }
  }
  //无论是否成功都应该修改
  start_index_ = (start_index_+1)%static_cast<int>(num_instances_);
  return result;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (uint32_t i = 0; i < num_instances_; ++i) {
    dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager_array_ + i)->FlushAllPgsImp();
  }
}

}  // namespace bustub
