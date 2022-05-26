//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  directory_page_ = reinterpret_cast<HashTableDirectoryPage*
  //  >(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  // 创建一个bucket
  page_id_t bucket_id;
  buffer_pool_manager_->NewPage(&bucket_id);
  dir_page->SetBucketPageId(0, bucket_id);
  buffer_pool_manager_->UnpinPage(bucket_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & (dir_page->GetGlobalDepthMask());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_id = KeyToPageId(key, dir_page);

  Page *page = buffer_pool_manager_->FetchPage(bucket_id);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool is_get = bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_id, false);

  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  return is_get;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_id = KeyToPageId(key, dir_page);

  Page *page = buffer_pool_manager_->FetchPage(bucket_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  if (bucket_page->IsDuplicate(key, value, comparator_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_id, false);
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return false;
  }
  bool is_insert = bucket_page->Insert(key, value, comparator_);
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  if (!is_insert) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_id, false);
    bool flag = SplitInsert(transaction, key, value);
    return flag;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_id, true);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  // dir扩展
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  if (dir_page->GetLocalDepth(bucket_index) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  // bucket分裂，(如果dir扩展了，两个互相映射的bucket此时指向同一个page_id)
  page_id_t bucket_id1 = KeyToPageId(key, dir_page);
  Page *page1 = buffer_pool_manager_->FetchPage(bucket_id1);
  page1->WLatch();
  page_id_t bucket_id2;
  Page *page2 = buffer_pool_manager_->NewPage(&bucket_id2);
  page2->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page1 = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page1->GetData());
  HASH_TABLE_BUCKET_TYPE *bucket_page2 = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page2->GetData());

  // 该page对应的真实index
  uint32_t local_index = Hash(key) & (dir_page->GetLocalDepthMask(bucket_index));
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_index);
  uint32_t ld_add = 1 << local_depth;

  bucket_index = KeyToDirectoryIndex(key, dir_page);
  // 修改所有指向该page的bucket
  for (uint32_t i = local_index; i < dir_page->Size(); i += ld_add) {
    if (((bucket_index >> local_depth) & 1) != ((i >> local_depth) & 1)) {
      dir_page->SetBucketPageId(i, bucket_id2);
    } else {
      // 当前key插入page1
      dir_page->SetBucketPageId(i, bucket_id1);
    }
    dir_page->IncrLocalDepth(i);
  }

  uint32_t mask = dir_page->GetLocalDepthMask(bucket_index);
  local_index = Hash(key) & mask;

  // 当前key插入page1
  // 将page1原本的数据按照新的bucketIndex分配
  std::vector<MappingType> page_tmp;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (bucket_page1->IsReadable(i)) {
      if ((Hash(bucket_page1->KeyAt(i)) & mask) == local_index) {
        page_tmp.push_back(MappingType(bucket_page1->KeyAt(i), bucket_page1->ValueAt(i)));
      } else {
        bucket_page2->Insert(bucket_page1->KeyAt(i), bucket_page1->ValueAt(i), comparator_);
      }
    }
  }
  bucket_page1->Reset();
  for (uint i = 0; i < page_tmp.size(); ++i) {
    bucket_page1->Insert(page_tmp[i].first, page_tmp[i].second, comparator_);
  }
  //  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
  //    if (!bucket_page1->IsOccupied(i)){
  //      break ;
  //    }
  //    if (bucket_page1->IsReadable(i)){
  //      std::cout << "(" <<bucket_page1->KeyAt(i)<< ","<<bucket_page1->ValueAt(i) <<")";
  //    }
  //  }
  //  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
  //    if (!bucket_page2->IsOccupied(i)){
  //      break ;
  //    }
  //    if (bucket_page2->IsReadable(i)){
  //      std::cout << "(" <<bucket_page2->KeyAt(i)<< ","<<bucket_page2->ValueAt(i) <<")";
  //    }
  //  }

  // 最后插入原数据
  bool is_insert = bucket_page1->Insert(key, value, comparator_);
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  page1->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_id1, true);
  page2->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_id2, true);

  if (!is_insert) {
    return SplitInsert(transaction, key, value);
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_id1 = KeyToPageId(key, dir_page);

  Page *page1 = buffer_pool_manager_->FetchPage(bucket_id1);
  page1->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page1 = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page1->GetData());
  bool is_remove = bucket_page1->Remove(key, value, comparator_);

  if (!is_remove) {
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    page1->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_id1, false);
    return false;
  }

  uint32_t bucket_index1 = KeyToDirectoryIndex(key, dir_page);
  //  uint32_t local_depth = dir_page->GetLocalDepth(bucket_index1);
  uint32_t global_depth = dir_page->GetGlobalDepth();

  page1->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_id1, true);
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  MyMerge(transaction, bucket_index1, global_depth);
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::MyMerge(Transaction *transaction, int bucket_index1, uint32_t global_depth1) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  // 因为中途放锁，可能此时的depth已经改变，这时bucket_index不一定还有意义
  if (bucket_index1 >= static_cast<int>(dir_page->Size()) || dir_page->GetGlobalDepth() != global_depth1) {
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_index1);
  if (local_depth == 0) {
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }

  page_id_t bucket_id1 = dir_page->GetBucketPageId(bucket_index1);
  Page *page1 = buffer_pool_manager_->FetchPage(bucket_id1);
  page1->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page1 = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page1->GetData());

  uint32_t bucket_index2 = dir_page->GetSplitImageIndex(bucket_index1);
  page_id_t bucket_id2 = dir_page->GetBucketPageId(bucket_index2);
  //  assert(bucket_index2 < dir_page->Size());
  if (bucket_page1->IsEmpty() && dir_page->GetLocalDepth(bucket_index1) == dir_page->GetLocalDepth(bucket_index2) &&
      bucket_id2 != bucket_id1) {
    // 进行bucket合并

    //    Page* page2 = buffer_pool_manager_->FetchPage(bucket_id2);
    //    HASH_TABLE_BUCKET_TYPE* bucket_page2 = reinterpret_cast<HASH_TABLE_BUCKET_TYPE* >(page2->GetData());

    // 删除page1，指向page1的bucket全都改为page2
    uint32_t local_d = dir_page->GetLocalDepth(bucket_index1) - 1;
    uint32_t local_index = bucket_index1 & ((1 << local_d) - 1);
    local_depth = 1 << local_d;
    for (uint32_t i = local_index; i < dir_page->Size(); i += local_depth) {
      //      assert(dir_page->GetLocalDepth(i) == local_d + 1 &&
      //             (dir_page->GetBucketPageId(i) == bucket_id1 || dir_page->GetBucketPageId(i) == bucket_id2));
      dir_page->SetBucketPageId(i, bucket_id2);
      dir_page->DecrLocalDepth(i);
    }

    page1->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_id1, true);
    buffer_pool_manager_->DeletePage(bucket_id1);

    // shrink,当所有local_depth都小于global_depth时，减小global_depth
    bool flag = true;
    for (uint32_t i = 0; i < dir_page->Size(); ++i) {
      if (dir_page->GetLocalDepth(i) == dir_page->GetGlobalDepth()) {
        flag = false;
        break;
      }
    }
    if (flag) {
      dir_page->DecrGlobalDepth();
      uint32_t global_depth = dir_page->GetGlobalDepth();
      table_latch_.WUnlock();
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);
      // shrink后可能本来两个不互为image的bucket现在成为image，可以合并了
      for (uint32_t i = 0; i < dir_page->Size(); ++i) {
        MyMerge(transaction, i, global_depth);
      }
    } else {
      table_latch_.WUnlock();
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    }

  } else {
    page1->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_id1, false);
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  }
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
