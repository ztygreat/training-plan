/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(){
    this->index=-1;
    this->max_size=0;
    this->buffer_pool_manager_=nullptr;
    this->leaf_page=nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page,int index,int max_size,BufferPoolManager *buffer_pool_manager_){
    this->index=index;
    this->max_size=max_size;
    this->buffer_pool_manager_=buffer_pool_manager_;
    this->leaf_page=leaf_page;
}

// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator(IndexIterator &old){
//     index=old.index;
//     max_size=old.max_size;
//     buffer_pool_manager_=old.buffer_pool_manager_;
//     leaf_page=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>((buffer_pool_manager_->FetchPage(old.leaf_page->GetParentPageId()))->GetData());
// }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
    if (leaf_page != nullptr) {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
    }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { 
    return leaf_page == nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
    assert(leaf_page != nullptr); 
    return leaf_page->GetItem(index);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
    if (isEnd()) {
        return *this;
    } 
    if(index<leaf_page->GetSize()-1){
        index++;
        return *this;
    } else if(index>=leaf_page->GetSize()-1 && leaf_page->GetNextPageId()!=INVALID_PAGE_ID){
        page_id_t next_page_id=leaf_page->GetNextPageId();
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
        leaf_page=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>((buffer_pool_manager_->FetchPage(next_page_id))->GetData());
        max_size=leaf_page->GetMaxSize();
        index=0;
        return *this;
    } else if(index>=leaf_page->GetSize()-1 && leaf_page->GetNextPageId()==INVALID_PAGE_ID){
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
        leaf_page=nullptr;
        index=0;

        return *this;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
