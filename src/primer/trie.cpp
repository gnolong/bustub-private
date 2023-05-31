#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <stack>

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  
  // you should walk through the trie to find the node corresponding to the key. if the node doesn't exist, return$
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if(!root_) return nullptr;
  std::shared_ptr<const TrieNode> curnode{root_};
  for(auto s : key){
    if(curnode->children_.find(s) != curnode->children_.end())
      curnode = curnode->children_.find(s)->second;
    else return nullptr;
  }
  if(curnode->is_value_node_){
    if(auto tnode = dynamic_cast<const TrieNodeWithValue<T> *>(curnode.get())){
      // std::cout << *(tnode->value_.get()) << std::endl;
      return tnode->value_.get();
    } 
    else return nullptr;
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  int len = key.size();
  if(len == 0){
    if(root_){
      Trie trie(std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value))));
      // root_->children_ = trie.root_->children_;
      return trie;
    }
    std::shared_ptr<TrieNode> tp = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    Trie trie(tp);
    return trie;
  }
  int i = 0;
  // Trie *newtrie;
  std::shared_ptr<TrieNode> rootnode{nullptr};
  if(root_){
    rootnode = std::shared_ptr<TrieNode>(root_->Clone());
  }
  else rootnode = std::make_shared<TrieNode>();
  std::shared_ptr<TrieNode> prenode{nullptr};
  // TrieNode * curnode = rootnode.get();
  auto curnode = rootnode; 
  for(; i < len; i++){
    if(prenode){
      auto tp= std::shared_ptr<TrieNode>(prenode->children_[key[i-1]]->Clone());
      prenode->children_[key[i-1]] = static_cast<std::shared_ptr<const TrieNode>>(tp);//i-1这个很关键,太愚蠢了
      prenode = tp;
      curnode = tp;
    }
    else{
      prenode = rootnode;
    }
    if(curnode->children_.find(key[i]) == curnode->children_.end()){
      auto tp = curnode;
      while(i < len-1){
        auto tnode = std::make_shared<TrieNode>();
        tp->children_.emplace(key[i],static_cast<std::shared_ptr<const TrieNode>>(tnode));
        tp = tnode;
        ++i;
      }
      std::shared_ptr<TrieNode> tnode = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      tp->children_.emplace(key[i],tnode);
      return Trie(rootnode);
    }
    // prenode = curnode;
    if(i == len-1){
      // std::cout << *(std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(curnode->children_[key[i]])->value_.get()) << std::endl;
      std::shared_ptr<TrieNode> tp = std::make_shared<TrieNodeWithValue<T>>(curnode->children_[key[i]]->children_, std::make_shared<T>(std::move(value)));
      //无论是clone还是这里的新建都用到了TrieNode的构造函数，其中的std::move(children),move的是形参中拷贝的map。
      curnode->children_.erase(key[i]);
      curnode->children_.emplace(key[i],tp);
      // std::cout << *(std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(curnode->children_[key[i]])->value_.get()) << std::endl;
      return Trie(rootnode);
    }
  }
  return Trie(rootnode);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if(!root_) return Trie(std::make_shared<TrieNode>());
  int len = key.size();
  if(len == 0)
    return Trie(std::make_shared<TrieNode>(root_->children_));
  std::shared_ptr<TrieNode> rootnode = std::shared_ptr<TrieNode>(root_->Clone());
  auto curnode = rootnode;
  std::shared_ptr<TrieNode> prenode{nullptr};
  std::stack<std::shared_ptr<TrieNode>> st; 
  std::stack<char> stchar;
  // std::shared_ptr<TrieNode> gtp{nullptr};
  int i = 0;
  for(; i < len; i++){
    if(prenode){
      prenode = std::shared_ptr<TrieNode>(prenode->children_[key[i-1]]->Clone());
      curnode = prenode;
    }
    else{
      prenode = rootnode;
    }
    // auto ite = curnode->children_.find(key[i]);
    if(curnode->children_.find(key[i]) == curnode->children_.end()){
      // while(!st.empty()){
      //   auto tp = st.top();
      //   tp->children_[stchar.top()] = std::shared_ptr<const TrieNode>(std::move(curnode));
      //   curnode = tp.get();
      //   st.pop();
      //   stchar.pop();
      // }
      return Trie(rootnode);
    }
    st.push(prenode);
    stchar.push(key[i]);
  }
  // curnode = const_cast<TrieNode*>(curnode->children_[key[i-1]].get());
  if(!curnode->children_[key[i-1]]->is_value_node_) return Trie(rootnode);
  curnode = std::shared_ptr<TrieNode>(curnode->children_[key[i-1]]->Clone());
  while(true){
    if(curnode->children_.empty()){
      auto tp = st.top();
      tp->children_.erase(tp->children_.find(stchar.top()));
      curnode = tp;
      st.pop();
      stchar.pop();
      if(curnode->is_value_node_) break;
      if(st.empty()) break;
      continue;
    }
    else if(!curnode->children_.empty() && curnode->is_value_node_){
      auto tp = st.top();
      auto tnode = std::make_shared<const TrieNode>(curnode->children_);
      tp->children_[stchar.top()] = tnode;
      curnode = tp;
      st.pop();
      stchar.pop();
      break;
    }
    break;
  }
  while(!st.empty()){
    auto tp = st.top();
    tp->children_[stchar.top()] = std::shared_ptr<const TrieNode>(std::move(curnode));
    curnode = tp;
    st.pop();
    stchar.pop();
  }
  return Trie(curnode);
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
