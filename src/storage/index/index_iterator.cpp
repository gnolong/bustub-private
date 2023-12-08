/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page, int index,
                                  BufferPoolManager *bpm)
    : page_(page), index_(index), bpm_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return static_cast<bool>(page_ == nullptr && index_ == BUSTUB_PAGE_SIZE); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return page_->MapAt(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ + 1 < page_->GetSize()) {
    ++index_;
    return *this;
  }
  if (page_->GetNextPageId() != INVALID_PAGE_ID) {
    index_ = 0;
    auto guard = bpm_->FetchPageRead(page_->GetNextPageId());
    page_ = const_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(guard.template As<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>());
    return *this;
  }
  page_ = nullptr;
  index_ = BUSTUB_PAGE_SIZE;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
