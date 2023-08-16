#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  this->page_ = that.page_;
  that.page_ = nullptr;
  this->is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  this->page_ = that.page_;
  that.page_ = nullptr;
  this->is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_->RUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_->RUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;

}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_->WUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_->WUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}  // NOLINT

}  // namespace bustub
