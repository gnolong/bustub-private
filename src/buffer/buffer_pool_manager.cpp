//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  page_id_t paid;
  if(!free_list_.empty()){
    paid = AllocatePage();
    auto num_free = free_list_.back();
    Page * ppage = pages_ + num_free;
    ppage->WLatch();
    ppage->page_id_ = paid;
    ppage->ResetMemory();
    ppage->is_dirty_ = true;
    ++((ppage)->pin_count_);
    ppage->WUnlatch();
    free_list_.pop_back();
    page_table_.emplace(paid,num_free);
    replacer_->RecordAccess(num_free);
    replacer_->SetEvictable(num_free, false);
    *page_id = paid;
    return ppage;
  }
  frame_id_t fid = 0;
  if(!replacer_->Evict(&fid)){
    return nullptr;
  }
  paid = AllocatePage();
  Page * ppage = pages_ + fid;
  ppage->RLatch();
  page_table_.erase(page_table_.find(ppage->page_id_));
  if(0 != ppage->pin_count_){
    throw Exception("pin count should not be that");
  }
  ppage->RUnlatch();
  ppage->WLatch();
  if(ppage->is_dirty_){
    disk_manager_->WritePage(ppage->page_id_, ppage->data_);
  }
  ppage->ResetMemory();
  ppage->is_dirty_ = true;
  ppage->page_id_ = paid;
  ++((ppage)->pin_count_);
  ppage->WUnlatch();
  page_table_.emplace(paid, fid);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  *page_id = paid;
  return ppage;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  auto ite = page_table_.find(page_id);
  if(page_table_.end() != ite){
    return pages_ + ite->second;
  }
  if(!free_list_.empty()){
    auto num_free = free_list_.back();
    Page * ppage = pages_ + num_free;
    ppage->WLatch();
    ppage->ResetMemory();
    disk_manager_->ReadPage(page_id, (ppage)->data_);
    ppage->page_id_ = page_id;
    ++((ppage)->pin_count_);
    ppage->WUnlatch();
    free_list_.pop_back();
    page_table_.emplace(page_id,num_free);
    replacer_->RecordAccess(num_free);
    replacer_->SetEvictable(num_free, false);
    return ppage;
  }
  frame_id_t fid = 0;
  if(!replacer_->Evict(&fid)){
    return nullptr;
  }
  Page * ppage = pages_ + fid;
  ppage->RLatch();
  if(0 != ppage->pin_count_){
    throw Exception("pin count should not be that");
  }
  page_table_.erase(page_table_.find(ppage->page_id_));
  ppage->RUnlatch();
  ppage->WLatch();
  if(ppage->is_dirty_){
    disk_manager_->WritePage(ppage->page_id_, ppage->data_);
    ppage->is_dirty_ = false;
  }
  ppage->ResetMemory();
  disk_manager_->ReadPage(page_id, (ppage)->data_);
  ppage->page_id_ = page_id;
  ++((ppage)->pin_count_);
  ppage->WUnlatch();
  page_table_.emplace(page_id, fid);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return ppage;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto ite = page_table_.find(page_id);
  if(page_table_.end() == ite || 0 == pages_[ite->second].GetPinCount()){
    throw Exception("pincount already 0");
    return false;
  }
  Page * ppage = pages_ + ite->second;
  ppage->WLatch();
  --(ppage->pin_count_);
  ppage->is_dirty_ = is_dirty;
  if(0 >= ppage->pin_count_){
    replacer_->SetEvictable(ite->second, true);
  }
  ppage->WUnlatch();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto ite = page_table_.find(page_id);
  if(page_table_.end() == ite){
    throw Exception("no page to be flushed");
    return false;
  }
  auto fid = ite->second;
  replacer_->Remove(fid);
  free_list_.push_front(fid);
  page_table_.erase(ite);
  Page * ppage = pages_ + fid;
  ppage->WLatch();
  disk_manager_->WritePage(page_id, ppage->data_);
  ppage->ResetMemory();
  ppage->page_id_ = INVALID_PAGE_ID;
  ppage->is_dirty_ = false;
  ppage->WUnlatch();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for(auto pair : page_table_){
    auto fid = pair.second;
    replacer_->Remove(fid);
    free_list_.push_front(fid);
    page_table_.erase(page_table_.find(pair.first));
    Page * ppage = pages_ + fid;
    ppage->WLatch();
    disk_manager_->WritePage(pair.first, ppage->data_);
    ppage->ResetMemory();
    ppage->page_id_ = INVALID_PAGE_ID;
    ppage->is_dirty_ = false;
    ppage->WUnlatch();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto ite = page_table_.find(page_id);
  if(page_table_.end() == ite){
    return true;
  }
  Page * ppage = pages_ + ite->second;
  ppage->RLatch();
  if(0 < ppage->pin_count_){
    return false;
  }
  ppage->RUnlatch();
  auto fid = ite->second;
  free_list_.push_front(fid);
  replacer_->Remove(fid);
  page_table_.erase(ite);
  DeallocatePage(page_id);
  ppage->WLatch();
  ppage->ResetMemory();
  ppage->page_id_ = INVALID_PAGE_ID;
  ppage->is_dirty_ = false;
  ppage->WUnlatch();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
