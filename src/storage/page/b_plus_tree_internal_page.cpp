//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  // if(0 != index){
  //   return *(array_ + index);
  // }
  return (array_ + index)->first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { (array_ + index)->first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { (array_ + index)->second = value; }
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return (array_ + index)->second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(int index, const KeyType &key, const ValueType &value) ->int {
  if(GetSize() >= GetMaxSize()){
    return -1;
  }
  char tmp[BUSTUB_PAGE_SIZE];
  int cp_size = sizeof(MappingType)*(GetSize()-index);
  memcpy(tmp, reinterpret_cast<char*>(array_ + index), cp_size);
  memcpy(reinterpret_cast<char*>(array_ + index + 1), tmp, cp_size);
  SetKeyAt(index, key);
  SetValueAt(index, value);
  IncreaseSize(1);
  return 0;                                            
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SpInsert(BPlusTreeInternalPage &page, int index, 
                                    const KeyType &key, const ValueType &value,
                                    KeyType &upkey) ->int{
  MappingType mid_array[BUSTUB_PAGE_SIZE*2];
  memset(reinterpret_cast<void*>(mid_array), 0, sizeof(mid_array));
  assert(GetSize() == GetMaxSize());
  if(GetMaxSize() == index){
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
  memcpy(reinterpret_cast<void *>(page.array_+1), reinterpret_cast<void *>(mid_array + mid_index+1), sizeof(MappingType) * (cursize - mid_index-1));
  page.SetValueAt(0, mid_array[mid_index].second);
  upkey = mid_array[mid_index].first;
  SetSize(mid_index);
  page.SetSize(cursize-mid_index);
  return 0;
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
