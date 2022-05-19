// /**
//  * grading_b_plus_tree_test.cpp
//  */
//
// #include <algorithm>
// #include <cstdio>
// #include <iostream>
// #include <sstream>
//
// #include "buffer/buffer_pool_manager.h"
// #include "common/logger.h"
// #include "index/b_plus_tree.h"
// #include "vtable/virtual_table.h"
// #include "gtest/gtest.h"
//
// namespace cmudb {
// TEST(BPlusTreeTests, SplitTest) {
//   LOG_DEBUG("current page size is 96 bytes");
//   // create KeyComparator and index schema
//   Schema *key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema);
//
//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
//                                                            comparator);
//   GenericKey<8> index_key;
//   RID rid;
//
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(page_id);
//   (void)header_page;
//
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5};
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set((int32_t)(key >> 32), value);
//     index_key.SetFromInteger(key);
//     EXPECT_EQ(true, tree.Insert(index_key, rid));
//   }
//   // insert into repetitive key, all failed
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set((int32_t)(key >> 32), value);
//     index_key.SetFromInteger(key);
//     EXPECT_EQ(false, tree.Insert(index_key, rid));
//   }
//   index_key.SetFromInteger(1);
//   auto leaf_node = tree.FindLeafPage(index_key);
//   ASSERT_NE(nullptr, leaf_node);
//   EXPECT_NE(5, leaf_node->GetSize());
//   EXPECT_NE(5, leaf_node->GetMaxSize());
//   EXPECT_NE(INVALID_PAGE_ID, leaf_node->GetNextPageId());
//
//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }
//
// TEST(BPlusTreeTests, MergeTest) {
//   LOG_DEBUG("current page size is 96 bytes");
//   // create KeyComparator and index schema
//   Schema *key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema);
//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
//                                                            comparator);
//   GenericKey<8> index_key;
//   RID rid;
//
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(page_id);
//   (void)header_page;
//
//   std::vector<int64_t> keys = {2, 1, 3, 5, 4};
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set((int32_t)(key >> 32), value);
//     index_key.SetFromInteger(key);
//     EXPECT_EQ(true, tree.Insert(index_key, rid));
//   }
//
//   std::vector<int64_t> remove_keys = {1, 5};
//   for (auto key : remove_keys) {
//     index_key.SetFromInteger(key);
//     tree.Remove(index_key);
//   }
//   index_key.SetFromInteger(2);
//   auto leaf_node = tree.FindLeafPage(index_key);
//   ASSERT_NE(nullptr, leaf_node);
//   EXPECT_NE(5, leaf_node->GetSize());
//   EXPECT_EQ(INVALID_PAGE_ID, leaf_node->GetNextPageId());
//
//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }
//
// TEST(BPlusTreeTests, InsertTest1) {
//   // create KeyComparator and index schema
//   Schema *key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema);
//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
//                                                            comparator);
//   GenericKey<8> index_key;
//   RID rid;
//
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(page_id);
//   (void)header_page;
//
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5};
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set((int32_t)(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid);
//   }
//
//   std::vector<RID> rids;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, rids);
//     EXPECT_EQ(1, rids.size());
//
//     int64_t value = key & 0xFFFFFFFF;
//     EXPECT_EQ(value, rids[0].GetSlotNum());
//   }
//
//   int64_t start_key = 1;
//   int64_t current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(0, location.GetPageId());
//     EXPECT_EQ(current_key, location.GetSlotNum());
//     current_key = current_key + 1;
//   }
//
//   EXPECT_EQ(keys.size() + 1, current_key);
//
//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }
//
// TEST(BPlusTreeTests, InsertTest2) {
//   // create KeyComparator and index schema
//   Schema *key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema);
//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
//                                                            comparator);
//   GenericKey<8> index_key;
//   RID rid;
//
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(page_id);
//   (void)header_page;
//
//   std::vector<int64_t> keys = {5, 4, 3, 2, 1};
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set((int32_t)(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid);
//   }
//
//   std::vector<RID> rids;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, rids);
//     EXPECT_EQ(1, rids.size());
//
//     int64_t value = key;
//     EXPECT_EQ(value, rids[0].GetSlotNum());
//   }
//
//   int64_t start_key = 1;
//   int64_t current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(0, location.GetPageId());
//     EXPECT_EQ(current_key, location.GetSlotNum());
//     current_key = current_key + 1;
//   }
//
//   EXPECT_EQ(keys.size() + 1, current_key);
//
//   start_key = 3;
//   current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(0, location.GetPageId());
//     EXPECT_EQ(current_key, location.GetSlotNum());
//     current_key = current_key + 1;
//   }
//
//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }
//
// TEST(BPlusTreeTests, DeleteTest1) {
//   // create KeyComparator and index schema
//   std::string createStmt = "a bigint";
//   Schema *key_schema = ParseCreateStatement(createStmt);
//   GenericComparator<8> comparator(key_schema);
//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
//                                                            comparator);
//   GenericKey<8> index_key;
//   RID rid;
//
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(page_id);
//   (void)header_page;
//
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5};
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set((int32_t)(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid);
//   }
//
//   std::vector<RID> rids;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, rids);
//     EXPECT_EQ(1, rids.size());
//
//     int64_t value = key & 0xFFFFFFFF;
//     EXPECT_EQ(value, rids[0].GetSlotNum());
//   }
//
//   int64_t start_key = 1;
//   int64_t current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(0, location.GetPageId());
//     EXPECT_EQ(current_key, location.GetSlotNum());
//     current_key = current_key + 1;
//   }
//
//   EXPECT_EQ(keys.size() + 1, current_key);
//
//   std::vector<int64_t> remove_keys = {1, 5};
//   for (auto key : remove_keys) {
//     index_key.SetFromInteger(key);
//     tree.Remove(index_key);
//   }
//
//   start_key = 2;
//   current_key = start_key;
//   int64_t size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(0, location.GetPageId());
//     EXPECT_EQ(current_key, location.GetSlotNum());
//     current_key = current_key + 1;
//     size = size + 1;
//   }
//
//   EXPECT_EQ(3, size);
//
//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }
//
// TEST(BPlusTreeTests, DeleteTest2) {
//   // create KeyComparator and index schema
//   Schema *key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema);
//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
//                                                            comparator);
//   GenericKey<8> index_key;
//   RID rid;
//
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(page_id);
//   (void)header_page;
//
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5};
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set((int32_t)(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid);
//   }
//
//   std::vector<RID> rids;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, rids);
//     EXPECT_EQ(1, rids.size());
//
//     int value = key;
//     EXPECT_EQ(value, rids[0].GetSlotNum());
//   }
//
//   int64_t start_key = 1;
//   int64_t current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(0, location.GetPageId());
//     EXPECT_EQ(current_key, location.GetSlotNum());
//     current_key = current_key + 1;
//   }
//
//   EXPECT_EQ(keys.size() + 1, current_key);
//
//   std::vector<int64_t> remove_keys = {1, 5, 3, 4};
//   for (auto key : remove_keys) {
//     index_key.SetFromInteger(key);
//     tree.Remove(index_key);
//   }
//
//   start_key = 2;
//   current_key = start_key;
//   int64_t size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(0, location.GetPageId());
//     EXPECT_EQ(current_key, location.GetSlotNum());
//     current_key = current_key + 1;
//     size = size + 1;
//   }
//
//   EXPECT_EQ(1, size);
//
//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }
//
// TEST(BPlusTreeTests, ScaleTest) {
//   // create KeyComparator and index schema
//   Schema *key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema);
//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bpm = new BufferPoolManager(20, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
//                                                            comparator);
//   GenericKey<8> index_key;
//   RID rid;
//
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(page_id);
//   (void)header_page;
//
//   int64_t scale = 10000;
//   std::vector<int64_t> keys;
//   for (int64_t key = 1; key < scale; key++) {
//     keys.push_back(key);
//   }
//   // shuffle keys
//   std::random_shuffle(keys.begin(), keys.end());
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set((int32_t)(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid);
//   }
//
//   std::vector<RID> rids;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, rids);
//     EXPECT_EQ(1, rids.size());
//
//     int64_t value = key & 0xFFFFFFFF;
//     EXPECT_EQ(value, rids[0].GetSlotNum());
//   }
//
//   int64_t start_key = 1;
//   int64_t current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     current_key = current_key + 1;
//   }
//
//   EXPECT_EQ(keys.size() + 1, current_key);
//
//   int64_t remove_scale = 9900;
//   std::vector<int64_t> remove_keys;
//   for (int64_t key = 1; key < remove_scale; key++) {
//     remove_keys.push_back(key);
//   }
//
//   for (auto key : remove_keys) {
//     index_key.SetFromInteger(key);
//     tree.Remove(index_key);
//   }
//
//   start_key = remove_scale;
//   int64_t size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
//        ++iterator) {
//     size = size + 1;
//   }
//   EXPECT_EQ(100, size);
//   remove_keys.clear();
//   for (int64_t key = remove_scale; key < scale; key++) {
//     remove_keys.push_back(key);
//   }
//   for (auto key : remove_keys) {
//     index_key.SetFromInteger(key);
//     tree.Remove(index_key);
//   }
//   EXPECT_EQ(true, tree.IsEmpty());
//
//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }
//
// } // namespace cmudb