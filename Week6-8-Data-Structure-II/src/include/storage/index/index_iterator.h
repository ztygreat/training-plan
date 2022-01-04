//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page,int index,int max_size,BufferPoolManager *buffer_pool_manager_);
//   IndexIterator(IndexIterator &old);
  // IndexIterator &operator=(const IndexIterator &other) {
  //   index=other.index;
  //   max_size=other.max_size;
  //   buffer_pool_manager_=other.buffer_pool_manager_;
  //   leaf_page=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>((buffer_pool_manager_->FetchPage(other.leaf_page->GetParentPageId()))->GetData());
  //   return *this;
  // }

  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const { 
      if(leaf_page==itr.leaf_page && index==itr.index)
          return true; 
      return false;
  }

  bool operator!=(const IndexIterator &itr) const { 
       if(leaf_page!=itr.leaf_page || index!=itr.index)
          return true; 
      return false;
  }

 private:
    BufferPoolManager *buffer_pool_manager_;
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page; 
    int index;
    int max_size;
  // add your own private member variables here
};

}  // namespace bustub
