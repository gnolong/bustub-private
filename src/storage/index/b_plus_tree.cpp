#include <cstring>
#include <sstream>
#include <stack>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  if (leaf_max_size <= 1) {
    std::cout << "\nleaf_max_size <=1\n";
  }
  std::cout << "leaf_max_size:" << leaf_max_size << '\n';
  if (internal_max_size <= 2) {
    std::cout << "\ninternal_max_size <=2\n";
  }
  std::cout << "internal_max_size:" << internal_max_size << '\n';
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (INVALID_PAGE_ID == root_page->root_page_id_) {
    return true;
  }
  guard = bpm_->FetchPageRead(root_page->root_page_id_);
  auto ppage = guard.As<InternalPage>();
  return 0 == ppage->GetSize();
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  // Context ctx;
  // (void)ctx;
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (INVALID_PAGE_ID == root_page->root_page_id_) {
    return false;
  }
  guard = bpm_->FetchPageRead(root_page->root_page_id_);
  auto ppage = guard.As<InternalPage>();
  if (0 == ppage->GetSize()) {
    return false;
  }
  while (!ppage->IsLeafPage()) {
    auto cursize = ppage->GetSize();
    int i = 1;
    while (i < cursize && 0 <= comparator_(key, ppage->KeyAt(i))) {
      ++i;
    }
    guard = bpm_->FetchPageRead(ppage->ValueAt(i - 1));
    ppage = guard.As<InternalPage>();
  }
  auto ppage_leaf = reinterpret_cast<const LeafPage *>(ppage);
  auto cursize = ppage_leaf->GetSize();
  for (int i = 0; i < cursize; ++i) {
    if (0 == comparator_(key, ppage_leaf->KeyAt(i))) {
      // std::cout << "GetValue:" << key << '\n';
      result->push_back(ppage_leaf->ValueAt(i));
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  // std::cout << "Insert:" << key << '\n';
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  if (INVALID_PAGE_ID == head_page->root_page_id_) {
    page_id_t page_id = INVALID_PAGE_ID;
    auto bguard = bpm_->NewPageGuarded(&page_id);
    head_page->root_page_id_ = page_id;
    auto ppage = bguard.AsMut<LeafPage>();
    ppage->Init(leaf_max_size_);
  }
  ctx.root_page_id_ = head_page->root_page_id_;

  auto wguard = bpm_->FetchPageWrite(head_page->root_page_id_);
  auto ppage = wguard.AsMut<InternalPage>();
  while (!ppage->IsLeafPage()) {
    // pay attention to lvalue and rvalue
    ctx.write_set_.push_back(std::move(wguard));
    auto cursize = ppage->GetSize();
    int i = 1;
    while (i < cursize && 0 <= comparator_(key, ppage->KeyAt(i))) {
      ++i;
    }
    ctx.write_index_set_.push_back(i);
    wguard = bpm_->FetchPageWrite(ppage->ValueAt(i - 1));
    ppage = wguard.AsMut<InternalPage>();
  }
  auto ppage_lf = reinterpret_cast<LeafPage *>(ppage);
  auto cursize = ppage_lf->GetSize();
  /* leaf_page insertion with a simple method*/

  // return normal inertion
  auto res = ppage_lf->Insert(key, value, comparator_);
  // succeed
  if (0 == res) {
    return true;
  }

  while (ctx.write_set_.size() > 1) {
    auto itr_t = ctx.write_set_.begin() + 1;
    auto page_t = itr_t->AsMut<InternalPage>();
    if (page_t->GetSize() < page_t->GetMaxSize()) {
      if (ctx.IsRootPage(ctx.write_set_.front().PageId())) {
        ctx.header_page_.reset();
      }
      ctx.write_set_.pop_front();
      ctx.write_index_set_.pop_front();
      continue;
    }
    break;
  }

  // duplicate key
  if (1 == res) {
    return false;
  }
  // should be split
  page_id_t pid1 = 0;
  auto page1 = bpm_->NewPageGuarded(&pid1);
  page1.Drop();
  auto guard_lf1 = bpm_->FetchPageWrite(pid1);
  auto ppage_lf1 = guard_lf1.AsMut<LeafPage>();
  ppage_lf1->Init(leaf_max_size_);

  {
    int i = 0;
    while (i < cursize && 0 <= comparator_(key, ppage_lf->KeyAt(i))) {
      ++i;
    }
    ppage_lf->SpInsert(*ppage_lf1, i, key, value);
    ppage_lf1->SetNextPageId(ppage_lf->GetNextPageId());
    ppage_lf->SetNextPageId(pid1);
  }

  auto upkey = ppage_lf1->KeyAt(0);
  auto pid_lf = wguard.PageId();
  auto pid_rt = pid1;

  /* internal page process */
  while (!ctx.write_set_.empty()) {
    ppage = ctx.write_set_.back().AsMut<InternalPage>();
    int idx = ctx.write_index_set_.back();
    auto ans = ppage->Insert(idx, upkey, pid_rt);
    if (!ans) {
      return true;
    }
    auto page_inter = bpm_->NewPageGuarded(&pid1);
    page_inter.Drop();
    auto guard_inter = bpm_->FetchPageWrite(pid1);
    auto ppage_inter = guard_inter.AsMut<InternalPage>();
    ppage_inter->Init(internal_max_size_);
    ppage->SpInsert(*ppage_inter, idx, upkey, pid_rt, upkey);
    pid_lf = ctx.write_set_.back().PageId();
    pid_rt = pid1;
    ctx.write_index_set_.pop_back();
    ctx.write_set_.pop_back();
  }

  auto page_inter = bpm_->NewPageGuarded(&pid1);
  page_inter.Drop();
  auto guard_inter = bpm_->FetchPageWrite(pid1);
  auto ppage_inter = guard_inter.AsMut<InternalPage>();
  ppage_inter->Init(internal_max_size_);
  ppage_inter->SetKeyAt(1, upkey);
  ppage_inter->SetValueAt(0, pid_lf);
  ppage_inter->SetValueAt(1, pid_rt);
  ppage_inter->IncreaseSize(2);
  // refer to the number of value

  head_page->root_page_id_ = pid1;
  // ctx.root_page_id_ = pid1;

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  // std::cout << "Remove:" << key << '\n';
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  if (INVALID_PAGE_ID == head_page->root_page_id_) {
    return;
  }
  ctx.root_page_id_ = head_page->root_page_id_;

  auto wguard = bpm_->FetchPageWrite(head_page->root_page_id_);
  auto ppage = wguard.AsMut<InternalPage>();
  while (!ppage->IsLeafPage()) {
    // pay attention to lvalue and rvalue
    ctx.write_set_.push_back(std::move(wguard));
    auto cursize = ppage->GetSize();
    int i = 1;
    while (i < cursize && 0 <= comparator_(key, ppage->KeyAt(i))) {
      ++i;
    }
    ctx.write_index_set_.push_back(i);
    wguard = bpm_->FetchPageWrite(ppage->ValueAt(i - 1));
    ppage = wguard.AsMut<InternalPage>();
  }
  auto ppage_lf = reinterpret_cast<LeafPage *>(ppage);

  // leaf page is root page
  auto pid_tmp_lf = wguard.PageId();
  if (ctx.IsRootPage(pid_tmp_lf)) {
    ppage_lf->Remove(key, comparator_);
    if (ppage_lf->GetSize() == 0) {
      bpm_->DeletePage(pid_tmp_lf);
      wguard.Drop();
      head_page->root_page_id_ = pid_tmp_lf;
      ctx.root_page_id_ = head_page->root_page_id_;
    }
    return;
  }

  // leaf page is not root page
  int res = ppage_lf->Remove(key, comparator_);
  // do not have the member or normal removal
  if (0 == res) {
    return;
  }

  while (ctx.write_set_.size() > 1) {
    auto itr_t = ctx.write_set_.begin() + 1;
    auto page_t = itr_t->AsMut<InternalPage>();
    if (page_t->GetSize() < page_t->GetMinSize()) {
      if (ctx.IsRootPage(ctx.write_set_.front().PageId())) {
        ctx.header_page_.reset();
      }
      ctx.write_set_.pop_front();
      ctx.write_index_set_.pop_front();
      continue;
    }
    break;
  }
  // page size is zero before removing
  if (-1 == res) {
    throw Exception("page size is zero before removing");
    return;
  }

  /*redistribute or merge leaf page */
  // if(ctx.write_set_.empty()){
  //   //root page
  //   return;
  // }

  int idx_del = ~0;
  int idx_2cur = ctx.write_index_set_.back();
  auto ppage_p1 = ctx.write_set_.back().AsMut<InternalPage>();
  int size_p1 = ppage_p1->GetSize();

  if (idx_2cur == size_p1) {
    // page tail
    auto wguard_bro1 = bpm_->FetchPageWrite(ppage_p1->ValueAt(idx_2cur - 2));
    auto ppage_bro1 = wguard_bro1.template AsMut<LeafPage>();
    auto size_bro1 = ppage_bro1->GetSize();
    if (size_bro1 > ppage_bro1->GetMinSize()) {
      auto key_bro1 = ppage_bro1->KeyAt(size_bro1 - 1);
      ppage_lf->Insert(key_bro1, ppage_bro1->ValueAt(size_bro1 - 1), comparator_);
      ppage_p1->SetKeyAt(idx_2cur - 1, key_bro1);
      ppage_bro1->IncreaseSize(-1);
      return;
    }
    // merge leaf page
    ppage_bro1->Merge(*ppage_lf);
    auto pid_tmp = wguard.PageId();
    bpm_->DeletePage(pid_tmp);
    wguard.Drop();
    idx_del = idx_2cur - 1;
  } else if (idx_2cur == 1) {
    // page head
    auto wguard_bro2 = bpm_->FetchPageWrite(ppage_p1->ValueAt(idx_2cur));
    auto ppage_bro2 = wguard_bro2.template AsMut<LeafPage>();
    auto size_bro2 = ppage_bro2->GetSize();
    if (size_bro2 > ppage_bro2->GetMinSize()) {
      auto key_bro2 = ppage_bro2->KeyAt(0);
      auto value_bro2 = ppage_bro2->ValueAt(0);
      ppage_bro2->Remove(key_bro2, comparator_);
      ppage_lf->SetKeyAt(ppage_lf->GetSize(), key_bro2);
      ppage_lf->SetValueAt(ppage_lf->GetSize(), value_bro2);
      ppage_lf->IncreaseSize(1);
      ppage_p1->SetKeyAt(idx_2cur, ppage_bro2->KeyAt(0));
      return;
    }
    // merge leaf page
    ppage_lf->Merge(*ppage_bro2);
    auto pid_tmp = wguard_bro2.PageId();
    bpm_->DeletePage(pid_tmp);
    wguard_bro2.Drop();
    idx_del = idx_2cur;
  } else {
    // leaf page mid
    auto wguard_bro1 = bpm_->FetchPageWrite(ppage_p1->ValueAt(idx_2cur - 2));
    auto ppage_bro1 = wguard_bro1.template AsMut<LeafPage>();
    auto size_bro1 = ppage_bro1->GetSize();
    if (size_bro1 > ppage_bro1->GetMinSize()) {
      auto key_bro1 = ppage_bro1->KeyAt(size_bro1 - 1);
      ppage_lf->Insert(key_bro1, ppage_bro1->ValueAt(size_bro1 - 1), comparator_);
      ppage_p1->SetKeyAt(idx_2cur - 1, key_bro1);
      ppage_bro1->IncreaseSize(-1);
      return;
    }
    auto wguard_bro2 = bpm_->FetchPageWrite(ppage_p1->ValueAt(idx_2cur));
    auto ppage_bro2 = wguard_bro2.template AsMut<LeafPage>();
    auto size_bro2 = ppage_bro2->GetSize();
    if (size_bro2 > ppage_bro2->GetMinSize()) {
      auto key_bro2 = ppage_bro2->KeyAt(0);
      auto value_bro2 = ppage_bro2->ValueAt(0);
      ppage_bro2->Remove(key_bro2, comparator_);
      ppage_lf->SetKeyAt(ppage_lf->GetSize(), key_bro2);
      ppage_lf->SetValueAt(ppage_lf->GetSize(), value_bro2);
      ppage_lf->IncreaseSize(1);
      ppage_p1->SetKeyAt(idx_2cur, ppage_bro2->KeyAt(0));
      return;
    }
    // merge leaf page
    ppage_bro1->Merge(*ppage_lf);
    auto pid_tmp = wguard.PageId();
    bpm_->DeletePage(pid_tmp);
    wguard.Drop();
    idx_del = idx_2cur - 1;
  }

  ctx.write_index_set_.pop_back();

  /* internal page removal */
  while (!ctx.write_set_.empty()) {
    ppage = ctx.write_set_.back().AsMut<InternalPage>();

    // root page
    if (ctx.IsRootPage(ctx.write_set_.back().PageId())) {
      ppage->Remove(idx_del);
      int size_cur = ppage->GetSize();
      if (1 == size_cur) {
        head_page->root_page_id_ = ppage->ValueAt(0);
        ctx.root_page_id_ = head_page->root_page_id_;
        bpm_->DeletePage(ctx.write_set_.back().PageId());
        ctx.write_set_.back().Drop();
      }
      return;
    }

    // not root page
    res = ppage->Remove(idx_del);
    if (0 == res) {
      return;
    }

    auto ite_guard_p1 = ctx.write_set_.rbegin() + 1;
    idx_2cur = ctx.write_index_set_.back();
    ppage_p1 = ite_guard_p1->AsMut<InternalPage>();
    size_p1 = ppage_p1->GetSize();

    if (idx_2cur == size_p1) {
      // page tail
      auto wguard_bro1 = bpm_->FetchPageWrite(ppage_p1->ValueAt(idx_2cur - 2));
      auto ppage_bro1 = wguard_bro1.template AsMut<InternalPage>();
      auto size_bro1 = ppage_bro1->GetSize();
      if (size_bro1 > ppage_bro1->GetMinSize()) {
        auto key_bro1 = ppage_bro1->KeyAt(size_bro1 - 1);
        auto value_bro1 = ppage_bro1->ValueAt(size_bro1 - 1);
        ppage_bro1->IncreaseSize(-1);
        ppage->Insert(1, ppage_p1->KeyAt(idx_2cur - 1), ppage->ValueAt(0));
        ppage->SetValueAt(0, value_bro1);
        ppage_p1->SetKeyAt(idx_2cur - 1, key_bro1);
        return;
      }
      // merge internal page
      ppage_bro1->Merge(*ppage_p1, idx_2cur - 1, *ppage);
      auto pid_tmp = ctx.write_set_.back().PageId();
      bpm_->DeletePage(pid_tmp);
      ctx.write_set_.back().Drop();
      idx_del = idx_2cur - 1;
    } else if (idx_2cur == 1) {
      // page head
      auto wguard_bro2 = bpm_->FetchPageWrite(ppage_p1->ValueAt(idx_2cur));
      auto ppage_bro2 = wguard_bro2.template AsMut<InternalPage>();
      auto size_bro2 = ppage_bro2->GetSize();
      if (size_bro2 > ppage_bro2->GetMinSize()) {
        ppage->SetKeyAt(ppage->GetSize(), ppage_p1->KeyAt(idx_2cur));
        ppage->SetValueAt(ppage->GetSize(), ppage_bro2->ValueAt(0));
        ppage->IncreaseSize(1);
        ppage_bro2->SetValueAt(0, ppage_bro2->ValueAt(1));
        ppage_p1->SetKeyAt(idx_2cur, ppage_bro2->KeyAt(1));
        ppage_bro2->Remove(1);
        return;
      }
      // merge internal page
      ppage->Merge(*ppage_p1, idx_2cur, *ppage_bro2);
      auto pid_tmp = wguard_bro2.PageId();
      bpm_->DeletePage(pid_tmp);
      wguard_bro2.Drop();
      idx_del = idx_2cur;
    } else {
      // internal page mid
      auto wguard_bro1 = bpm_->FetchPageWrite(ppage_p1->ValueAt(idx_2cur - 2));
      auto ppage_bro1 = wguard_bro1.template AsMut<InternalPage>();
      auto size_bro1 = ppage_bro1->GetSize();
      if (size_bro1 > ppage_bro1->GetMinSize()) {
        auto key_bro1 = ppage_bro1->KeyAt(size_bro1 - 1);
        auto value_bro1 = ppage_bro1->ValueAt(size_bro1 - 1);
        ppage_bro1->IncreaseSize(-1);
        ppage->Insert(1, ppage_p1->KeyAt(idx_2cur - 1), ppage->ValueAt(0));
        ppage->SetValueAt(0, value_bro1);
        ppage_p1->SetKeyAt(idx_2cur - 1, key_bro1);
        return;
      }
      auto wguard_bro2 = bpm_->FetchPageWrite(ppage_p1->ValueAt(idx_2cur));
      auto ppage_bro2 = wguard_bro2.template AsMut<InternalPage>();
      auto size_bro2 = ppage_bro2->GetSize();
      if (size_bro2 > ppage_bro2->GetMinSize()) {
        ppage->SetKeyAt(ppage->GetSize(), ppage_p1->KeyAt(idx_2cur));
        ppage->SetValueAt(ppage->GetSize(), ppage_bro2->ValueAt(0));
        ppage->IncreaseSize(1);
        ppage_bro2->SetValueAt(0, ppage_bro2->ValueAt(1));
        ppage_p1->SetKeyAt(idx_2cur, ppage_bro2->KeyAt(1));
        ppage_bro2->Remove(1);
        return;
      }
      // merge internal page
      ppage_bro1->Merge(*ppage_p1, idx_2cur - 1, *ppage);
      auto pid_tmp = ctx.write_set_.back().PageId();
      bpm_->DeletePage(pid_tmp);
      ctx.write_set_.back().Drop();
      idx_del = idx_2cur - 1;
    }
    ctx.write_set_.pop_back();
    ctx.write_index_set_.pop_back();
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // std::cout << "use tree.Begin()" << '\n';
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (INVALID_PAGE_ID == root_page->root_page_id_) {
    return INDEXITERATOR_TYPE(nullptr, BUSTUB_PAGE_SIZE, bpm_);
  }
  guard = bpm_->FetchPageWrite(root_page->root_page_id_);
  auto ppage = guard.AsMut<InternalPage>();
  if (0 == ppage->GetSize()) {
    return INDEXITERATOR_TYPE(nullptr, BUSTUB_PAGE_SIZE, bpm_);
  }
  while (!ppage->IsLeafPage()) {
    guard = bpm_->FetchPageWrite(ppage->ValueAt(0));
    ppage = guard.AsMut<InternalPage>();
  }
  auto ppage_leaf = reinterpret_cast<LeafPage *>(ppage);
  return INDEXITERATOR_TYPE(ppage_leaf, 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // std::cout << "use tree.Begin(key), key =" << key << '\n';
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  if (INVALID_PAGE_ID == root_page->root_page_id_) {
    return INDEXITERATOR_TYPE(nullptr, BUSTUB_PAGE_SIZE, bpm_);
  }
  guard = bpm_->FetchPageWrite(root_page->root_page_id_);
  auto ppage = guard.AsMut<InternalPage>();
  if (0 == ppage->GetSize()) {
    return INDEXITERATOR_TYPE(nullptr, BUSTUB_PAGE_SIZE, bpm_);
  }
  while (!ppage->IsLeafPage()) {
    auto cursize = ppage->GetSize();
    int i = 1;
    while (i < cursize && 0 <= comparator_(key, ppage->KeyAt(i))) {
      ++i;
    }
    guard = bpm_->FetchPageWrite(ppage->ValueAt(i - 1));
    ppage = guard.AsMut<InternalPage>();
  }
  auto ppage_leaf = reinterpret_cast<LeafPage *>(ppage);
  auto cursize = ppage_leaf->GetSize();
  for (int i = 0; i < cursize; ++i) {
    if (0 == comparator_(key, ppage_leaf->KeyAt(i))) {
      return INDEXITERATOR_TYPE(ppage_leaf, i, bpm_);
    }
  }
  return INDEXITERATOR_TYPE(nullptr, BUSTUB_PAGE_SIZE, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // std::cout << "use tree.End()" << '\n';
  return INDEXITERATOR_TYPE(nullptr, BUSTUB_PAGE_SIZE, bpm_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << '\n';
    std::cout << '\n';

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << '\n';

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << '\n';
    std::cout << '\n';
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << '\n';
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << '\n';
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
