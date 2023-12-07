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
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page,
                  int index,BufferPoolManager *bpm):
                  page_(page), index_(index), bpm_(bpm){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    return static_cast<bool>(page_ == nullptr && index_ == BUSTUB_PAGE_SIZE);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    return page_->MapAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    if(++index_ < page_->GetSize()){
        return *this;
    }
    if(page_->GetNextPageId() != INVALID_PAGE_ID){
        index_ = 0;
        auto wguard = bpm_->FetchPageWrite(page_->GetNextPageId());
        page_ = wguard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
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
