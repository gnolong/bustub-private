#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <stack>
#include <type_traits>
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  
  // you should walk through the trie to find the node corresponding to the key. if the node doesn't exist, return$
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if(nullptr == root_) return nullptr;
  std::shared_ptr<const TrieNode> TmpNode{root_};
  for(auto s : key){
    if(TmpNode->children_.find(s) != TmpNode->children_.end())
      TmpNode = TmpNode->children_.at(s);
    else return nullptr;
  }
  if(TmpNode->is_value_node_)  
    if(auto tnode = dynamic_cast<const TrieNodeWithValue<T> *>(TmpNode.get())) return tnode->value_.get();
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  const Trie * trie;
  if(root_ == nullptr) {
    trie = new Trie(std::make_shared<const TrieNode>());
  }
  else{
    trie = this;
  }
  int len = key.size();
  if(len == 0 && trie->root_->is_value_node_ && *(std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(trie->root_)->value_) == value) return *this;
  else if(len == 0 && (trie->root_->is_value_node_ == false || (dynamic_cast<const TrieNodeWithValue<T>*>(trie->root_.get()))->value_.get()!= &value)) 
    return Trie(std::make_shared<const TrieNodeWithValue<T>>(trie->root_->children_, std::make_shared<T>(std::move(value))));
  int i = 0;
  // Trie *newtrie;
  auto rootnode = std::shared_ptr<TrieNode>(trie->root_->Clone());
  // TrieNode * curnode = rootnode.get();
  auto TmpNode{rootnode.get()};
  auto prenode{rootnode.get()};
  for(; i < len; i++){
    if(TmpNode != rootnode.get()){
      auto tp= std::shared_ptr<TrieNode>(TmpNode->Clone());
      prenode->children_[key[i]] = static_cast<std::shared_ptr<const TrieNode>>(tp);
      prenode =  tp.get();
    }
    if(TmpNode->children_.find(key[i]) == TmpNode->children_.end()){
      // if(i == len-1) 
      //   TmpNode->children_.emplace{key[i], std::shared_ptr<const TrieNode>(new TrieNodeWithValue(value))};
      // else{
      //   TmpNode->children_.emplace{key[i], std::shared_ptr<const TrieNode>(new TrieNode())};
      //   Tmp
      // }
      auto tp = TmpNode;
      while(i < len-1){
        auto tnode = std::make_shared<TrieNode>();
        tp->children_.emplace(key[i],static_cast<std::shared_ptr<const TrieNode>>(tnode));
        tp = tnode.get();
        ++i;
      }
        std::shared_ptr<TrieNode> tnode = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
        tp->children_.emplace(key[i],static_cast<std::shared_ptr<const TrieNode>>(tnode));
        return Trie(rootnode);
    }
    // prenode = TmpNode;
    if(i == len-1){
      if(TmpNode->children_.find(key[i])->second->is_value_node_){
        if(std::)
        if(dynamic_cast<const TrieNodeWithValue<T>*>(TmpNode->children_.find(key[i])->second.get()))->value_.get() == &value)
        return *this;
      }
      else{
        TmpNode->children_.emplace(key[i],std::make_shared<const TrieNodeWithValue<T>>(TmpNode->children_.find(key[i])->second->children_, std::make_shared<T>(std::move(value))));
        return Trie(rootnode);
      }

    }
    TmpNode = const_cast<TrieNode*>(TmpNode->children_.find(key[i])->second.get());
  }
  return Trie(static_cast<std::shared_ptr<const TrieNode>>(rootnode));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::stack<std::shared_ptr<const TrieNode>> st; 
  std::shared_ptr<const TrieNode> tmpnode = root_;
  std::stack<char> stchar;
  int len = key.size();
  if(len == 0) return *this;
  for(auto ch : key){
    auto ite = tmpnode->children_.find(ch);
    if(ite == tmpnode->children_.end()) return *this;
    st.push(tmpnode);
    stchar.push(ch);
    tmpnode = ite->second;
  }
  while(true){
    if(tmpnode->children_.empty()){
      auto tp = const_cast<TrieNode*>(st.top().get());
      tp->children_.erase(tp->children_.find(stchar.top()));
      tmpnode = st.top();
      if(tmpnode->is_value_node_) break;
      st.pop();
      stchar.pop();
      continue;
    }
    break;
  }
  return *this;
}
// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
