//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page, int index, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return static_cast<bool>(page_ == itr.page_ && index_ == itr.index_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return page_ != itr.page_ || index_ != itr.index_; }

 private:
  // add your own private member variables here
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page_{nullptr};
  int index_{BUSTUB_PAGE_SIZE};
  BufferPoolManager *bpm_{nullptr};
};

}  // namespace bustub
