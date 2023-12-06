//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  // KeyType key{};
  return (array_ + index)->first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return (array_ + index)->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) -> void {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void {
  array_[index].second = value;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int {
  if(GetSize() >= GetMaxSize()){
    return -1;
  }
  int index = 0;
  int mysize = GetSize();
  for(; index < mysize; ++index){
    auto res = comparator((array_ + index) ->first, key);
    if(0 == res){
      return 1;
    }
    if(0 < res){
      char tmp[BUSTUB_PAGE_SIZE];
      int cp_size = sizeof(MappingType)*(GetSize()-index);
      memcpy(tmp, reinterpret_cast<char*>(array_ + index), cp_size);
      memcpy(reinterpret_cast<char*>(array_ + index + 1), tmp, cp_size);
      break;
    }
  }
  (array_ + index) -> first = key;
  (array_ + index) -> second = value;
  IncreaseSize(1);
  return 0;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SpInsert(BPlusTreeLeafPage &page, int index, const KeyType &key, 
                                          const ValueType &value) -> int{
  MappingType mid_array[BUSTUB_PAGE_SIZE * 2];
  memset(reinterpret_cast<void*>(mid_array), 0, sizeof(mid_array));
  assert(GetSize() == GetMaxSize());
  if(0 == index){
    mid_array[0].first = key;
    mid_array[0].second = value;
    memcpy(reinterpret_cast<void*>(mid_array+1), reinterpret_cast<void*>(array_), sizeof(MappingType)*GetSize());
  }
  else if(GetMaxSize() == index){
    memcpy(reinterpret_cast<void*>(mid_array), reinterpret_cast<void*>(array_), sizeof(MappingType)*GetSize());
    mid_array[index].first = key;
    mid_array[index].second = value;
  }
  else{
    memcpy(reinterpret_cast<void*>(mid_array), reinterpret_cast<void*>(array_), sizeof(MappingType)*index);
    mid_array[index].first = key;
    mid_array[index].second = value;
    memcpy(reinterpret_cast<void*>(mid_array+index+1), reinterpret_cast<void*>(array_+index), sizeof(MappingType)*(GetSize()-index));
  }
  int cursize = GetMaxSize()+1;
  int mid_index = cursize /  2;
  memcpy(reinterpret_cast<void*>(array_), reinterpret_cast<void*>(mid_array), sizeof(MappingType)*mid_index);
  memcpy(reinterpret_cast<void *>(page.array_), reinterpret_cast<void *>(mid_array + mid_index), sizeof(MappingType) * (cursize - mid_index));
  SetSize(mid_index);
  page.SetSize(cursize-mid_index);
  return 0;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> int {
  int mysize = GetSize();
  if(mysize == 0){
    return -1;
  }
  int index = 0;
  for(; index < mysize; ++index){
    auto res = comparator((array_ + index) ->first, key);
    if(0 == res){
      if(index != mysize-1){
        int cp_size = sizeof(MappingType)*(GetSize()-index-1);
        memcpy(reinterpret_cast<char*>(array_ + index), reinterpret_cast<char*>(array_ + index + 1), cp_size);
      }
      IncreaseSize(-1);
      if(GetSize() < GetMinSize()){
        return 1;
      }
      return 0;
    }
  }
  return 0;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage &page) -> int {
  auto mysize = GetSize();
  auto cp_size = sizeof(MappingType)*page.GetSize();
  memcpy(reinterpret_cast<void*>(array_ + mysize), reinterpret_cast<void*>(page.array_),cp_size);
  IncreaseSize(page.GetSize());
  next_page_id_ = page.next_page_id_;
  return 0;
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
