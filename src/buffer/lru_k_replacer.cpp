//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <mutex>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::recursive_mutex> lock(latch_);
    auto r_ite = first_list_.rbegin();
    for(; r_ite != first_list_.rend(); r_ite++){
        if(r_ite->is_evictable_){
            *frame_id = r_ite->fid_;
            Remove(*frame_id);
            return true;
        }
    }
    r_ite = second_list_.rbegin();
    for(; r_ite != second_list_.rend(); r_ite++){
        if(r_ite->is_evictable_){
            *frame_id = r_ite->fid_;
            Remove(*frame_id);
            return true;
        }
    }
    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::recursive_mutex> lock(latch_);
    ++current_timestamp_;
    auto ite = node_store_.find(frame_id);
    if(node_store_.end() == ite){
        if(curr_size_ >= replacer_size_){
            throw Exception("larger than replacer size");
        }
        first_list_.push_front(LRUKNode(frame_id,current_timestamp_));
        node_store_.emplace(frame_id, first_list_.begin());
        ++curr_size_;
    }
    else {
        ite->second->history_.push_front(current_timestamp_);
        if(ite->second->k_ < k_-1 || k_ == 1){
            ++ite->second->k_;
            first_list_.push_front(std::move(*ite->second));
            first_list_.erase(ite->second);
            node_store_[frame_id] = first_list_.begin();
        }
        else if(ite->second->k_ == k_-1){
            ++ite->second->k_;
            second_list_.push_front(std::move(*ite->second));
            first_list_.erase(ite->second);
            node_store_[frame_id] = second_list_.begin();
        }
        else{
            second_list_.push_front(std::move(*ite->second));
            second_list_.erase(ite->second);
            node_store_[frame_id] = second_list_.begin();
        }
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::recursive_mutex> lock(latch_);
    auto ite = node_store_.find(frame_id)->second;
    if(ite->is_evictable_ == set_evictable){
        return;
    }
    ite->is_evictable_ = set_evictable;
    if(set_evictable){
        ++curr_evitc_size_;
    }
    else{
        --curr_evitc_size_;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::recursive_mutex> lock(latch_);
    auto ite = node_store_.find(frame_id); 
    if(ite == node_store_.end()){
        return;
    }
    if(!ite->second->is_evictable_){
        throw Exception("the frame is not evictable");
    }
    auto ite_remove = ite->second;
    --curr_evitc_size_;
    --curr_size_;
    node_store_.erase(ite);
    if(ite_remove->k_ == k_){
        second_list_.erase(ite_remove);
    }
    else{
        first_list_.erase(ite_remove);
    }
}

auto LRUKReplacer::Size() -> size_t {
    std::lock_guard<std::recursive_mutex> lock(latch_);
    return curr_evitc_size_;
}

}  // namespace bustub
