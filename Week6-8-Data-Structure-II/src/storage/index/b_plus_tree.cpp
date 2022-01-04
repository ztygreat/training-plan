//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
    if(root_page_id_==INVALID_PAGE_ID)
        return true;
    return false; 
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
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
    Page *page=FindLeafPage(key,false,transaction,ActionType::SEARCH);
    if(page==nullptr)
        return false;
    LeafPage *leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
    ValueType value_temp;
    if(leaf_page->Lookup(key,&value_temp,comparator_)){
        result->push_back(value_temp);
        buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
        if(transaction!=nullptr)
            Unlock(ActionType::SEARCH,transaction);
        return true;
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    if(transaction!=nullptr)
        Unlock(ActionType::SEARCH,transaction);
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
    // Page *page=FindLeafPage(key,false);
    if(IsEmpty()){
        //树为空
        Lock(ActionType::INSERT,&virtual_root_page,transaction);
        StartNewTree(key,value);
        Unlock(ActionType::INSERT,transaction);
        return true;
    }
    if(!InsertIntoLeaf(key,value,transaction)){
        //key值重复
        return false;
    }
    return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    Page *page=buffer_pool_manager_->NewPage(&root_page_id_);
    UpdateRootPageId(1);
    LeafPage *leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
    leaf_page->Init(root_page_id_,INVALID_PAGE_ID,leaf_max_size_);
    leaf_page->Insert(key,value,comparator_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
    // std::vector<ValueType> result;
    // if(GetValue(key,&result,transaction))
    //     return false;
    Page *page=FindLeafPage(key,false,transaction,ActionType::INSERT);
    LeafPage *leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
    ValueType value_temp;
    if(leaf_page->Lookup(key,&value_temp,comparator_)){
        // result->push_back(value_temp);
        buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
        if(transaction!=nullptr)
            Unlock(ActionType::INSERT,transaction);
        return false;
    }

    // Page *page=FindLeafPage(key,false,transaction,ActionType::INSERT);
    // LeafPage *leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
    if(leaf_page->GetSize() < (leaf_page->GetMaxSize()-1)){
        //叶子页面有空闲的空间，直接插入
        leaf_page->Insert(key,value,comparator_);
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
        if(transaction!=nullptr){
            Unlock(ActionType::INSERT,transaction);
        }
    } else{
        //插入后叶子页面会满，需要分割
        leaf_page->Insert(key,value,comparator_);
        LeafPage *new_leaf_page=Split(leaf_page);
        new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
        leaf_page->SetNextPageId(new_leaf_page->GetPageId());
        //再插入到父节点中
        InsertIntoParent(leaf_page,new_leaf_page->KeyAt(0),new_leaf_page,transaction);
    }
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t new_page_id=INVALID_PAGE_ID;
    Page *new_page=buffer_pool_manager_->NewPage(&new_page_id);
    N *new_leaf_or_internal_page=reinterpret_cast<N *>(new_page->GetData());
    

    if(node->IsLeafPage()){
        new_leaf_or_internal_page->Init(new_page_id,node->GetParentPageId(),leaf_max_size_);
        reinterpret_cast<LeafPage *>(node)->MoveHalfTo(reinterpret_cast<LeafPage *>(new_leaf_or_internal_page));
    }else{
        new_leaf_or_internal_page->Init(new_page_id,node->GetParentPageId(),internal_max_size_);
        reinterpret_cast<InternalPage *>(node)->MoveHalfTo(reinterpret_cast<InternalPage *>(new_leaf_or_internal_page),buffer_pool_manager_);
    }
    return new_leaf_or_internal_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
    Page *page=nullptr;
    if(old_node->GetParentPageId()==INVALID_PAGE_ID){
        page_id_t new_root_page_id=INVALID_PAGE_ID;
        page=buffer_pool_manager_->NewPage(&new_root_page_id);
        root_page_id_=new_root_page_id;
        UpdateRootPageId();
        InternalPage *internal_page=reinterpret_cast<InternalPage *>(page->GetData());
        internal_page->Init(new_root_page_id,INVALID_PAGE_ID,internal_max_size_);
        internal_page->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
        old_node->SetParentPageId(new_root_page_id);
        new_node->SetParentPageId(new_root_page_id);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);
        if(transaction!=nullptr){
            Unlock(ActionType::INSERT,transaction);
        }
        return;
    }
    
    page=buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    InternalPage *internal_page=reinterpret_cast<InternalPage *>(page->GetData());
    if(internal_page->GetSize() < (internal_page->GetMaxSize()-1) ){
        //父节点中有空闲的空间，直接插入
        internal_page->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(),true);
        if(transaction!=nullptr){
            Unlock(ActionType::INSERT,transaction);
        }
        return;
    }
    //父节点中没有空闲空间，先分割
    internal_page->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
    InternalPage *new_page=Split(internal_page);

    KeyType temp_key=new_page->KeyAt(0);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);
    InsertIntoParent(internal_page,temp_key,new_page,transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    if(IsEmpty())
        return;
    Page *page=FindLeafPage(key,false,transaction,ActionType::DELETE);
    LeafPage *leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
    leaf_page->RemoveAndDeleteRecord(key,comparator_);
    if(leaf_page->GetSize()>=leaf_page->GetMinSize()){
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
        if(transaction!=nullptr){
            Unlock(ActionType::DELETE,transaction);
        }
        return;
    }
    CoalesceOrRedistribute(leaf_page,transaction);
    Unlock(ActionType::DELETE,transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    if(node->IsRootPage()){
        if(AdjustRoot(node)){
            if(transaction==nullptr){
                buffer_pool_manager_->DeletePage(node->GetPageId());
            } else{
                transaction->AddIntoDeletedPageSet(node->GetPageId());
                // Unlock(ActionType::DELETE,transaction);
            }
        }
        return true;
    }

    page_id_t sibling_page_id=INVALID_PAGE_ID;
    N *sibling_node=nullptr;
    InternalPage *parent_page=nullptr;
    parent_page=reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage(node->GetParentPageId()))->GetData());
    int index=parent_page->ValueIndex(node->GetPageId());
    if(index==0){
        sibling_page_id=parent_page->ValueAt(index+1);
    }else{
        sibling_page_id=parent_page->ValueAt(index-1);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),false);
    Page *sibling_page=buffer_pool_manager_->FetchPage(sibling_page_id);
    sibling_page->WLatch();
    transaction->AddIntoPageSet(sibling_page);
    sibling_node=reinterpret_cast<N *>(sibling_page->GetData());
    // if(sibling_node->GetSize()-1 >= node->GetMinSize()){
    if(sibling_node->GetSize() + node->GetSize() > node->GetMaxSize()){
        Redistribute(sibling_node,node,index);
        // Unlock(ActionType::DELETE,transaction);
        return false;
    }else{
        Coalesce(&sibling_node,&node,nullptr,index,transaction);
        // Unlock(ActionType::DELETE,transaction);
        return index==0?false:true;
    }
        
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
    InternalPage *parent_page=nullptr;
    parent_page=reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage((*node)->GetParentPageId()))->GetData());
    parent=&(parent_page);
    if(index==0){
        //目标节点在兄弟节点的前面
        N **temp;
        temp=neighbor_node;
        neighbor_node=node;
        node=temp;
        index=1;
    }
    if(!(*node)->IsLeafPage()){
        reinterpret_cast<InternalPage *>(*node)->MoveAllTo(reinterpret_cast<InternalPage *>(*neighbor_node),(*parent)->KeyAt(index),buffer_pool_manager_);
    } else{
        reinterpret_cast<LeafPage *>(*node)->MoveAllTo(reinterpret_cast<LeafPage *>(*neighbor_node));
        reinterpret_cast<LeafPage *>(*neighbor_node)->SetNextPageId(reinterpret_cast<LeafPage *>(*node)->GetNextPageId());
    }

    (*parent)->Remove((*parent)->ValueIndex((*node)->GetPageId()));
    buffer_pool_manager_->UnpinPage((*node)->GetPageId(),true);
    buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(),true);
    if(transaction==nullptr){
        buffer_pool_manager_->DeletePage((*node)->GetPageId());
    } else{
        transaction->AddIntoDeletedPageSet((*node)->GetPageId());
    }
    if((*parent)->GetSize()<(*parent)->GetMinSize()){
        return CoalesceOrRedistribute((*parent),transaction);
    }
    buffer_pool_manager_->UnpinPage((*parent)->GetPageId(),true);
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    InternalPage *parent_node=reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage(node->GetParentPageId()))->GetData());
    //节点从他的前一个节点借一个KV对
    if(index!=0){
        if(!node->IsLeafPage()){
            KeyType key_m=neighbor_node->KeyAt(neighbor_node->GetSize()-1);
            reinterpret_cast<InternalPage *>(neighbor_node)->MoveLastToFrontOf(reinterpret_cast<InternalPage *>(node),parent_node->KeyAt(index),buffer_pool_manager_);
            parent_node->SetKeyAt(index,key_m);
        }else{
            KeyType key_m=neighbor_node->KeyAt(neighbor_node->GetSize()-1);
            reinterpret_cast<LeafPage *>(neighbor_node)->MoveLastToFrontOf(reinterpret_cast<LeafPage *>(node));
            parent_node->SetKeyAt(index,key_m);
        }
    }else{
        //节点从他的后一个节点借一个KV对
        if(!node->IsLeafPage()){
            KeyType key_m=neighbor_node->KeyAt(1);
            reinterpret_cast<InternalPage *>(neighbor_node)->MoveFirstToEndOf(reinterpret_cast<InternalPage *>(node),parent_node->KeyAt(index+1),buffer_pool_manager_);
            parent_node->SetKeyAt(index+1,key_m);
        }else{
            KeyType key_m=neighbor_node->KeyAt(1);
            reinterpret_cast<LeafPage *>(neighbor_node)->MoveFirstToEndOf(reinterpret_cast<LeafPage *>(node));
            parent_node->SetKeyAt(index+1,key_m);
        }
    }
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    page_id_t old_root_node_id=old_root_node->GetPageId();

    if(old_root_node->IsLeafPage()){
        //case2
        buffer_pool_manager_->UnpinPage(old_root_node_id,true);
        root_page_id_=INVALID_PAGE_ID;
        UpdateRootPageId();
        return true;
    }

    root_page_id_=((InternalPage *)old_root_node)->ValueAt(0);
    UpdateRootPageId();
    Page *new_root_page=buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *new_root_node=reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_,true);
    buffer_pool_manager_->UnpinPage(old_root_node_id,true);
    return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
    KeyType random_key; 
    LeafPage *left_most_leaf_page=reinterpret_cast<LeafPage *>(FindLeafPage(random_key,true)->GetData());
    return INDEXITERATOR_TYPE(left_most_leaf_page,0,left_most_leaf_page->GetMaxSize(),buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
    LeafPage *leaf_page=reinterpret_cast<LeafPage *>(FindLeafPage(key)->GetData());
    int index=leaf_page->KeyIndex(key,comparator_);
    return INDEXITERATOR_TYPE(leaf_page,index,leaf_page->GetMaxSize(),buffer_pool_manager_); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
    // page_id_t next_page_id=INVALID_PAGE_ID;
    // Page *page=buffer_pool_manager_->FetchPage(root_page_id_);
    // BPlusTreePage *b_page=reinterpret_cast<BPlusTreePage *>(page->GetData());
    // while(!b_page->IsLeafPage()){
    //     InternalPage *internal_page=static_cast<InternalPage *>(b_page);
    //     next_page_id=internal_page->ValueAt(internal_page->GetSize()-1);
    //     buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    //     page=buffer_pool_manager_->FetchPage(next_page_id);
    //     b_page=reinterpret_cast<BPlusTreePage *>(page->GetData());
    // }

    return INDEXITERATOR_TYPE(nullptr,0,leaf_max_size_,buffer_pool_manager_); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Transaction *transaction,ActionType action_type) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
    if(IsEmpty())
        return nullptr;
    if(transaction==nullptr){
        page_id_t next_page_id=INVALID_PAGE_ID;
        Page *page=buffer_pool_manager_->FetchPage(root_page_id_);
        BPlusTreePage *b_page=reinterpret_cast<BPlusTreePage *>(page->GetData());
        while(!b_page->IsLeafPage()){
            InternalPage *internal_page=static_cast<InternalPage *>(b_page);
            if(leftMost)
                next_page_id=internal_page->ValueAt(0);
            else
                next_page_id=internal_page->Lookup(key,comparator_);
        
            buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
            page=buffer_pool_manager_->FetchPage(next_page_id);
            b_page=reinterpret_cast<BPlusTreePage *>(page->GetData());
        }
        return page;
    }
    page_id_t next_page_id=root_page_id_;
    Lock(action_type,&virtual_root_page,transaction);//保护root_page_id_
    Page *page=buffer_pool_manager_->FetchPage(root_page_id_);
    Lock(action_type,page,transaction);
    BPlusTreePage *node=reinterpret_cast<BPlusTreePage *>(page->GetData());
    while(!node->IsLeafPage()){
        InternalPage *internal_page=static_cast<InternalPage *>(node);
        if(leftMost)
            next_page_id=internal_page->ValueAt(0);
        else
            next_page_id=internal_page->Lookup(key,comparator_);
        // buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
        page=buffer_pool_manager_->FetchPage(next_page_id);
        Lock(action_type,page,transaction);
        node=reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    return buffer_pool_manager_->FetchPage(next_page_id);//fetch两次
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsSafe(ActionType action_type,BPlusTreePage *node) const { 
    if(action_type==ActionType::SEARCH){
        return true;
    }
    if(action_type==ActionType::INSERT){
        if(node->GetSize()<node->GetMaxSize()-1)
            return true;
        return false;
    }
    //动作是删除操作
    if(node->GetSize()>node->GetMinSize())
        return true;
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Lock(ActionType action_type,Page *page,Transaction *transaction){ 
    if(action_type==ActionType::SEARCH){
        page->RLatch();
    }else{
        page->WLatch();
    }
    if(page==&virtual_root_page){
        transaction->AddIntoPageSet(page);
        return;
    }
    BPlusTreePage *node=reinterpret_cast<BPlusTreePage *>(page->GetData());
    if(IsSafe(action_type,node)){
        Unlock(action_type,transaction);
    }
    transaction->AddIntoPageSet(page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Unlock(ActionType action_type,Transaction *transaction){
    Page *page=transaction->GetPageSet()->at(0);//判断是否是virtual_root_page_
    if(page==&virtual_root_page){
        if(action_type==ActionType::SEARCH){
            page->RUnlatch();
        } else{
            page->WUnlatch();
        }
        transaction->GetPageSet()->pop_front();
    } 
    for(Page *page : *(transaction->GetPageSet())){
        int curPid = page->GetPageId();
        if(action_type==ActionType::SEARCH){
            page->RUnlatch();
            buffer_pool_manager_->UnpinPage(curPid,false);
        } else{
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(curPid,true);
        }
        if(transaction->GetDeletedPageSet()->find(curPid) != transaction->GetDeletedPageSet()->end()){
            buffer_pool_manager_->DeletePage(curPid);
            transaction->GetDeletedPageSet()->erase(curPid);
        }
    }
    transaction->GetPageSet()->clear();
}
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
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
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
