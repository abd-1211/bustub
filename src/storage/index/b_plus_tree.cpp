//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <cstddef>
#include <optional>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/generic_key.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_); // acquire a read lock on the current B+ tree header.
  auto header_page = guard.As<BPlusTreeHeaderPage>(); // convert the raw bytes from the guard to a read only bplustreeheader type object
  auto root_id = header_page->root_page_id_;// get the id of the root page to check wether its valid or not (exists or not)
  if(root_id == INVALID_PAGE_ID )
  {
    return true;
  }
  else
  {
    return false;
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_pg = header_guard.As<BPlusTreeHeaderPage>();
  auto header_root_id = header_pg->root_page_id_;
  header_guard.Drop();
  if(header_root_id == INVALID_PAGE_ID) // check if tree even exists
  {
    return false;
  }
  
  ReadPageGuard curr_guard = bpm_->ReadPage(header_root_id);
  auto curr_page = curr_guard.As<BPlusTreePage>();
  while(!curr_page->IsLeafPage())
  {
    auto internal = curr_guard.As<InternalPage>();
   
      int hi = curr_page->GetSize(),lo = 1;
      while(lo<hi)
      {
        
        int mid = lo + (hi-lo)/2;
        auto comp = comparator_(internal->KeyAt(mid),key);
        if(comp > 0) // if key is smaller than the key at current mid
        {
          hi = mid;
        }
        else  // if key is smaller than or equal to the key at current mid
        {
          lo = mid+1;
          
        }
        
      }
      page_id_t child_id = internal->ValueAt(lo -1);
      curr_guard = bpm_->ReadPage(child_id);
      curr_page = curr_guard.As<BPlusTreePage>();
      
    }
      auto leaf_pg = curr_guard.As<LeafPage>();
      int hi = curr_page->GetSize();
      int lo = 0;
      while(lo<hi)
      {
        int mid = lo + (hi-lo)/2; 
        auto comp = comparator_(leaf_pg->KeyAt(mid),key);
        if(comp > 0) // if key is smaller than the key at current mid
        {
          hi = mid;
        }
        else if (comp<0) // if key is smaller than or equal to the key at current mid
        {
          lo = mid+1;
          
        }
        else
        {
          auto tombstones = leaf_pg->GetTombstones();
          for(auto &tombs : tombstones)
          {
            if(comparator_(tombs,key) == 0)
            {
              return false; // the key has been logically deleted
            }
          }
          result->push_back(leaf_pg->ValueAt(mid));
          return true;
        }
      }
      return false;
    
  
  
}




FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticInsert(const KeyType &key, const ValueType &value) -> std::optional<bool> {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_pg = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_pg_id = header_pg->root_page_id_;
  header_guard.Drop();

  if (root_pg_id == INVALID_PAGE_ID) {
    return std::nullopt; // empty tree needs pessimistic
  }

  ReadPageGuard curr_guard = bpm_->ReadPage(root_pg_id);
  auto curr_pg = curr_guard.As<BPlusTreePage>();

  while (!curr_pg->IsLeafPage()) {
    auto internal = curr_guard.As<InternalPage>();
    int lo = 1, hi = internal->GetSize();
    while (lo < hi) {
      int mid = lo + (hi - lo) / 2;
      if (comparator_(internal->KeyAt(mid), key) <= 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    page_id_t child_id = internal->ValueAt(lo - 1);
    curr_guard = bpm_->ReadPage(child_id);
    curr_pg = curr_guard.As<BPlusTreePage>();
  }

  page_id_t leaf_id = curr_guard.GetPageId();
  int expected_size = curr_pg->GetSize();
  curr_guard.Drop();
  WritePageGuard leaf_guard = bpm_->WritePage(leaf_id);
  auto leaf_pg = leaf_guard.AsMut<LeafPage>();

  if(leaf_pg->GetSize() != expected_size)
  {
    return std::nullopt;
  }

  if (leaf_pg->GetSize() >= leaf_pg->GetMaxSize() - 1) {
    return std::nullopt; // leaf will split, need pessimistic
  }
 

  int lo = 0, hi = leaf_pg->GetSize();
  while (lo < hi) {
    int mid = lo + (hi - lo) / 2;
    int cmp = comparator_(leaf_pg->KeyAt(mid), key);
    if (cmp == 0) return false; // duplicate
    if (cmp < 0) lo = mid + 1;
    else hi = mid;
  }

  for (int i = leaf_pg->GetSize(); i > lo; i--) {
    leaf_pg->SetKeyAt(i, leaf_pg->KeyAt(i - 1));
    leaf_pg->SetValueAt(i, leaf_pg->ValueAt(i - 1));
  }
  leaf_pg->SetKeyAt(lo, key);
  leaf_pg->SetValueAt(lo, value);
  leaf_pg->ChangeSizeBy(1);
  return true;
}




/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  // auto optimistic_result = OptimisticInsert(key, value);
  // if (optimistic_result.has_value()) 
  // {
  //   return optimistic_result.value();
  // }
  Context ctx;
  WritePageGuard header_guard = bpm_->WritePage(header_page_id_); // get a write guard on header pg
  auto header_pg = header_guard.AsMut<BPlusTreeHeaderPage>(); // get a pointer to the guard as a headertype obj
  auto root_pg_id = header_pg->root_page_id_; // get roots page id from the header
  ctx.header_page_=std::move(header_guard); // push into ctx headerpage to propogate back if needed

  if(root_pg_id == INVALID_PAGE_ID) // tree doesnt exist
  {
    auto leaf_pg_id = bpm_->NewPage(); // since tree doesnt exist create root which would be the leaf
    WritePageGuard leaf_guard = bpm_->WritePage(leaf_pg_id); // take a writeguard on that leaf
    auto leaf_pg = leaf_guard.AsMut<LeafPage>(); // get a mutable pointer to it as a leafpage obj
    leaf_pg->Init(leaf_max_size_); // set max size
    leaf_pg->SetValueAt(0,value); // since its leaf idx 0 is valid so insert the value
    leaf_pg->SetKeyAt(0,key);     // and key at first idx(0)
    leaf_pg->ChangeSizeBy(1); // increment current size
    header_pg->root_page_id_ = leaf_pg_id; // set the node u made as the root inside the headerpg.
    ctx.header_page_ = std::nullopt; // empty the context header since no longer needed
    return true;
  }
  //tree exists
  WritePageGuard curr_guard = bpm_->WritePage(root_pg_id);
  auto curr_pg = curr_guard.AsMut<BPlusTreePage>();
  
  while(!curr_pg->IsLeafPage()) // internal page
  {
    
    auto internal = curr_guard.AsMut<InternalPage>();
    int lo = 1; 
    int hi = curr_pg->GetSize();
    
    while(lo<hi)
    {
    int mid = lo + (hi - lo)/2;
    int cmp = comparator_(internal->KeyAt(mid),key);
    if(cmp > 0) // if key is smaller than the key at current mid
      {
        hi = mid;
      }
    else  // if key is smaller than or equal to the key at current mid
      {
        lo = mid+1;
        
      }
       
    } 
    
    page_id_t child_id = internal->ValueAt(lo-1);
    ctx.write_set_.push_back(std::move(curr_guard));
    curr_guard = bpm_->WritePage(child_id);
    curr_pg = curr_guard.AsMut<BPlusTreePage>();
    
  }
  //leaf page
  page_id_t curr_leaf_id = curr_guard.GetPageId();
  auto leaf_pg = curr_guard.AsMut<LeafPage>();
  int hi=leaf_pg->GetSize();
  int lo = 0;
  
  int mid = 0;
  while(lo<hi)
  {
    mid = lo + (hi-lo)/2;
    int cmp = comparator_(leaf_pg->KeyAt(mid),key);
    
      if(cmp>0)
      {
        hi = mid;
      }
      else if (cmp <0)
      {
        lo = mid+1;
      }
      else
      {
        return false; // duplicate key inserted
      }
    
  }
  for(int i=leaf_pg->GetSize();i>lo;i--) // shift entries to the right to make space to insert the new k-v pairs
  {
    leaf_pg->SetKeyAt(i,leaf_pg->KeyAt(i-1));
    leaf_pg->SetValueAt(i,leaf_pg->ValueAt(i-1));
  }

  leaf_pg->SetValueAt(lo,value);
  leaf_pg->SetKeyAt(lo,key);
  leaf_pg->ChangeSizeBy(1);
  if(leaf_pg->GetSize()>=leaf_pg->GetMaxSize()) // leaf has expanded beyond max allowable capacity so we must split the node
  {
    
    page_id_t new_leaf_id = bpm_->NewPage(); // create a new page for which to split right entries of previous page into
    WritePageGuard new_leaf_guard = bpm_->WritePage(new_leaf_id);
    auto new_leaf_pg = new_leaf_guard.AsMut<LeafPage>();
    new_leaf_pg->Init(leaf_max_size_);
    int split = leaf_pg->GetSize()/2;
    int idx = 0;
    
    for(int i=split;i<leaf_pg->GetSize();i++) // set right values of original leaf page to the new leaf page
    {
      
      new_leaf_pg->SetValueAt(idx,leaf_pg->ValueAt(i));
      new_leaf_pg->SetKeyAt(idx,leaf_pg->KeyAt(i));
      idx++;
    }
    new_leaf_pg->SetSize(idx); // no of inserted k-v pairs is the size of the new page
    leaf_pg->SetSize(leaf_pg->GetSize()-idx); // subtract the no of inserted pairs to the new page to get the size of old page
    KeyType up_key = new_leaf_pg->KeyAt(0); // the key on which the nodes were split (the first key of right node in leaves)
    new_leaf_pg->SetNextPageId(leaf_pg->GetNextPageId()); // connect the new node to the one pointed to by prev
    leaf_pg->SetNextPageId(new_leaf_id); // connect the prev node to the new node. struct now is old->new->olds prev next

    InsertIntoParent(ctx, curr_leaf_id, up_key, new_leaf_id);
  }
  ctx.header_page_ = std::nullopt;
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(Context &ctx, page_id_t old_id, const KeyType &key, page_id_t new_id)
{
  if(ctx.write_set_.empty()) // if current leaf was the root , make a new root
  {
    page_id_t new_root_id = bpm_->NewPage();
    WritePageGuard new_root_guard = bpm_->WritePage(new_root_id);
    auto new_root = new_root_guard.AsMut<InternalPage>();
    new_root->Init(internal_max_size_);
    new_root->SetValueAt(0,old_id); // first index contains left node
    new_root->SetValueAt(1,new_id); // second contains new right node
    new_root->SetKeyAt(1,key);
    new_root->SetSize(2);
    auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_root_id; // make the header point to the new root
    ctx.header_page_ = std::nullopt; // release header 
  }
  else { // if current leaf was not a root
    WritePageGuard parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    
    auto parent = parent_guard.AsMut<InternalPage>();
    int old_pg_idx = parent->ValueIndex(old_id); // find location of old page in the node
    int insert_pos = old_pg_idx + 1;

    if(parent->GetSize() < parent->GetMaxSize()) // if node is not full, shift and insert directly
    {
      for(int i=parent->GetSize(); i>insert_pos; i--) // shift existing entries to right to make space for 
      {
        parent->SetKeyAt(i,parent->KeyAt(i-1));
        parent->SetValueAt(i,parent->ValueAt(i-1));
      }
      parent->SetKeyAt(insert_pos,key); // place the seperator key at the new location we freed
      parent->SetValueAt(insert_pos,new_id); // this is where the nodes split into two, so the new node begins
      parent->ChangeSizeBy(1);
      ctx.header_page_ = std::nullopt;
      
    }
    else// split the parent
    {
      std::vector<KeyType> keys; // make temp vectors to store contents of the parent
      std::vector<page_id_t> vals;
      
      int parent_size = parent->GetSize();
      for(int i=0; i<parent_size; i++) // fill the temp vectors
      {
        keys.push_back(parent->KeyAt(i)); 
        vals.push_back(parent->ValueAt(i));
       
      }
      keys.insert(keys.begin() + insert_pos , key); // append the keys and vals at the insert position
      vals.insert(vals.begin() + insert_pos, new_id ); //

      int count = keys.size(); // get the no of k-vs that were present in the parent
      int mid = count/2; // calculate mid index of the no of keys

      for(int i=0; i<mid ; i++)
      {
        parent->SetValueAt(i,vals[i]); // update the added entry for the parent node
        parent->SetKeyAt(i,keys[i]);
      }

      parent->SetSize(mid); // remove the entries that we will move to the new node. now entries are [0,mid)
      
      page_id_t new_node_id = bpm_->NewPage(); // now create the new node that we will split into
      WritePageGuard new_node_guard = bpm_->WritePage(new_node_id); //
      auto new_node = new_node_guard.AsMut<InternalPage>(); //
      new_node->Init(internal_max_size_); //
      new_node->SetValueAt(0,vals[mid]); // set value of 0 idx of split node to the mid val which would be there in the actual parent node
      int idx = 0;                                    // also idx 0 does not have a key in internal nodes
      
      for(int i=mid+1;i<count;i++) // copy the values from (mid,count)
      {
        new_node->SetKeyAt(idx+1,keys[i]);
        new_node->SetValueAt(idx+1,vals[i]);
        idx++;
      }
      new_node->SetSize(idx+1); // all the indexes plus the 0 idx which does not have a key 

      KeyType up_key = keys[mid]; // the split key which will be present in parent only incase of internal nodes

      page_id_t parent_id = parent_guard.GetPageId(); 

      InsertIntoParent(ctx, parent_id, up_key, new_node_id);
    }
    
  }
}




FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticRemove(const KeyType &key) -> std::optional<bool> {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_pg = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_pg_id = header_pg->root_page_id_;
  header_guard.Drop();

  if (root_pg_id == INVALID_PAGE_ID) {
    return true; // empty tree
  }

  ReadPageGuard curr_guard = bpm_->ReadPage(root_pg_id);
  auto curr_pg = curr_guard.As<BPlusTreePage>();

  while (!curr_pg->IsLeafPage()) {
    auto internal = curr_guard.As<InternalPage>();
    int lo = 1, hi = internal->GetSize();
    while (lo < hi) {
      int mid = lo + (hi - lo) / 2;
      if (comparator_(internal->KeyAt(mid), key) <= 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    page_id_t child_id = internal->ValueAt(lo - 1);
    curr_guard = bpm_->ReadPage(child_id);
    curr_pg = curr_guard.As<BPlusTreePage>();
  }

  page_id_t leaf_id = curr_guard.GetPageId();
  int expected_size = curr_pg->GetSize();
  curr_guard.Drop();
  WritePageGuard leaf_guard = bpm_->WritePage(leaf_id);
  auto leaf_pg = leaf_guard.AsMut<LeafPage>();

  if(leaf_pg->GetSize() != expected_size)
  {
    return std::nullopt;
  }

  if (leaf_pg->GetSize() <= leaf_pg->GetMinSize()) {
    return std::nullopt; // will underflow, need pessimistic
  }

  int lo = 0, hi = leaf_pg->GetSize(), mid = 0;
  while (lo < hi) {
    mid = lo + (hi - lo) / 2;
    int cmp = comparator_(leaf_pg->KeyAt(mid), key);
    if (cmp == 0) {
      for (int i = mid; i < leaf_pg->GetSize() - 1; i++) {
        leaf_pg->SetKeyAt(i, leaf_pg->KeyAt(i + 1));
        leaf_pg->SetValueAt(i, leaf_pg->ValueAt(i + 1));
      }
      leaf_pg->ChangeSizeBy(-1);
      return true;
    }
    if (cmp < 0) lo = mid + 1;
    else hi = mid;
  }
  return true; // key not found
}






/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
 
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  // auto optimistic_result = OptimisticRemove(key);
  // if (optimistic_result.has_value())
  // {
  //   return;
  // }
  Context ctx;
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  WritePageGuard header_guard = bpm_->WritePage(header_page_id_); // get a write guard on header pg
  auto header_pg = header_guard.AsMut<BPlusTreeHeaderPage>(); // get a pointer to the guard as a headertype obj
  auto root_pg_id = header_pg->root_page_id_; // get roots page id from the header
  ctx.header_page_=std::move(header_guard); // push into ctx headerpage to propogate back if needed

  if(root_pg_id == INVALID_PAGE_ID) // tree doesnt exist
  {
    return;
  }
  //tree exists
  WritePageGuard curr_guard = bpm_->WritePage(root_pg_id);
  auto curr_pg = curr_guard.AsMut<BPlusTreePage>();
   while(!curr_pg->IsLeafPage()) // internal page
  {
    
    auto internal = curr_guard.AsMut<InternalPage>();
    int lo = 1; 
    int hi = curr_pg->GetSize();
    
    while(lo<hi)
    {
    int mid = lo + (hi - lo)/2;
    int cmp = comparator_(internal->KeyAt(mid),key);
    if(cmp > 0) // if key is smaller than the key at current mid
      {
        hi = mid;
      }
    else  // if key is smaller than or equal to the key at current mid
      {
        lo = mid+1;
        
      }
       
    } 
    
    page_id_t child_id = internal->ValueAt(lo-1);
    ctx.write_set_.push_back(std::move(curr_guard));
    curr_guard = bpm_->WritePage(child_id);
    curr_pg = curr_guard.AsMut<BPlusTreePage>();
    
  }
  //leaf
  page_id_t curr_leaf_id = curr_guard.GetPageId();
  
  
  auto leaf_pg = curr_guard.AsMut<LeafPage>();
  
  
  int hi=leaf_pg->GetSize();
  int lo = 0;
  
  int mid = 0;
  while(lo<hi)
  {
    mid = lo + (hi-lo)/2;
    int cmp = comparator_(leaf_pg->KeyAt(mid),key);
    
      if(cmp>0)
      {
        hi = mid;
      }
      else if (cmp <0)
      {
        lo = mid+1;
      }
      else // key found
      {
        for(int i = mid;i<leaf_pg->GetSize()-1;i++) // shift keys to the left
        {
          leaf_pg->SetValueAt(i,leaf_pg->ValueAt(i+1));
          leaf_pg->SetKeyAt(i,leaf_pg->KeyAt(i+1));
        }
        leaf_pg->ChangeSizeBy(-1);
        if(ctx.write_set_.empty())
        {
          if(leaf_pg->GetSize()==0)
          {
            ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
          }
          ctx.header_page_ = std::nullopt;
          return;
        }
        
        if(leaf_pg->GetSize()>=leaf_pg->GetMinSize())
        {
          ctx.header_page_ = std::nullopt;
          return;
        }
        WritePageGuard parent_guard = std::move(ctx.write_set_.back());
        ctx.write_set_.pop_back();
        auto parent = parent_guard.AsMut<InternalPage>();
        auto parent_id = parent_guard.GetPageId();
        int leaf_idx = parent->ValueIndex(curr_leaf_id);
        page_id_t left_sibling_id = INVALID_PAGE_ID;
        page_id_t right_sibling_id = INVALID_PAGE_ID;
        if(leaf_idx > 0) // sibling is to the left
        {
          left_sibling_id = parent->ValueAt(leaf_idx-1);
        }
        if(leaf_idx < parent->GetSize()-1) // sibling is to the right
        {
          right_sibling_id= parent->ValueAt(leaf_idx+1);
        }

        if(left_sibling_id !=INVALID_PAGE_ID)
        {
          WritePageGuard left_guard = bpm_->WritePage(left_sibling_id);
          auto left = left_guard.AsMut<LeafPage>();
          int left_size = left->GetSize();
          if(left_size>left->GetMinSize())
          {
            //borrow
            for(int i=leaf_pg->GetSize() ; i>0 ;i--) // make index 0 empty by right shifting
            {
              leaf_pg->SetKeyAt(i,leaf_pg->KeyAt(i-1));
              leaf_pg->SetValueAt(i,leaf_pg->ValueAt(i-1));
            }
            auto left_val=left->ValueAt(left_size-1);
            KeyType left_key = left->KeyAt(left_size-1);
            
            leaf_pg->SetKeyAt(0,left_key);
            leaf_pg->SetValueAt(0,left_val);
            leaf_pg->ChangeSizeBy(1);
            left->ChangeSizeBy(-1);

            parent->SetKeyAt(leaf_idx,leaf_pg->KeyAt(0));
            ctx.header_page_ = std::nullopt;
            return;
          }
          else {
            //merge
            std::vector<KeyType> keys;
            std::vector<ValueType> vals; //temp vectors to store 
            int count =0;
            for(int i=0;i<left->GetSize();i++)
            {
              keys.push_back(left->KeyAt(i));
              vals.push_back(left->ValueAt(i));
              count++;
            }
            for(int i=0;i<leaf_pg->GetSize();i++)
            {
              keys.push_back(leaf_pg->KeyAt(i));
              vals.push_back(leaf_pg->ValueAt(i));
              count++;
            }
            
            if(count<=left->GetMaxSize())  
            {  for(int i=0;i<count;i++)
              {
                left->SetValueAt(i,vals[i]);
                
                left->SetKeyAt(i,keys[i]);
              
              }
              left->SetSize(count);
              left->SetNextPageId(leaf_pg->GetNextPageId());
              leaf_pg->SetSize(0);
              for(int i = leaf_idx; i<parent->GetSize()-1; i++) // remove the leaf seperator at leaf_idx by left shifting
              {
                parent->SetKeyAt(i,parent->KeyAt(i+1));
                parent->SetValueAt(i,parent->ValueAt(i+1));
              }
              parent->ChangeSizeBy(-1);
              
              if(parent->GetSize()<parent->GetMinSize())
              {

                RemoveFromParent(ctx,parent_id, std::move(parent_guard));

                return;
                
              }
              else {
              ctx.header_page_ = std::nullopt;
              return;
              }
            }
          }
        }
        
        else if(right_sibling_id != INVALID_PAGE_ID)
        { //borrow
          WritePageGuard right_guard = bpm_->WritePage(right_sibling_id);
          auto right = right_guard.AsMut<LeafPage>();
          int right_size = right->GetSize();
          if(right_size>right->GetMinSize())
          {
            auto right_val = right->ValueAt(0);
            auto right_key = right->KeyAt(0);
            leaf_pg->SetValueAt(leaf_pg->GetSize(),right_val);
            leaf_pg->SetKeyAt(leaf_pg->GetSize(),right_key);
            leaf_pg->ChangeSizeBy(1);
            for(int i= 0 ;i<right->GetSize()-1;i++) // push back the entries to the left in right node to fill the 0 idx
            {
              right->SetValueAt(i,right->ValueAt(i+1));
              right->SetKeyAt(i,right->KeyAt(i+1));
            }
            right->ChangeSizeBy(-1);
            parent->SetKeyAt(leaf_idx+1,right->KeyAt(0));
            ctx.header_page_ = std::nullopt;
            return;
          }
          else
          {
            // merge
            int leaf_size = leaf_pg->GetSize();
            for(int i=0;i<right->GetSize();i++)
            {
              leaf_pg->SetValueAt(leaf_size+i,right->ValueAt(i));
              leaf_pg->SetKeyAt(leaf_size+i,right->KeyAt(i));
            }
            leaf_pg->ChangeSizeBy(right->GetSize());
            leaf_pg->SetNextPageId(right->GetNextPageId());
            right->SetSize(0);
            
            for(int i=leaf_idx+1;i<parent->GetSize()-1;i++)
            {
              parent->SetValueAt(i,parent->ValueAt(i+1));
              parent->SetKeyAt(i,parent->KeyAt(i+1));
            }
            parent->ChangeSizeBy(-1);
            if(parent->GetSize()<parent->GetMinSize())
            {
              RemoveFromParent(ctx,parent_id, std::move(parent_guard));
              return;
            }
            else {
              ctx.header_page_ = std::nullopt;
              return;
            }
          }
        }
        
      }
    
  }
  ctx.header_page_ = std::nullopt;
  return;
  
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromParent(Context &ctx, page_id_t node_id,WritePageGuard node_guard)
{
  if(ctx.write_set_.empty()) // if the selected node is the root
  {
    
    auto root = node_guard.AsMut<InternalPage>();
    if(root->GetSize() == 1) // has only one child so make the child the root
    {
      page_id_t new_root_id = root->ValueAt(0);
      ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
      
    }
    // if more than one child keep as is since there is no min max size limit for root
    ctx.header_page_ = std::nullopt;
    return;
  }
  WritePageGuard parent_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  page_id_t parent_id = parent_guard.GetPageId();

  auto parent = parent_guard.AsMut<InternalPage>();
  int nodes_idx = parent->ValueIndex(node_id);

  // WritePageGuard curr_node_guard = bpm_->WritePage(node_id);
   auto curr_node = node_guard.AsMut<InternalPage>();
  

  page_id_t left_sibling_id = INVALID_PAGE_ID;
  page_id_t right_sibling_id = INVALID_PAGE_ID;
  if(nodes_idx>0) // left sibling exists
  {
    left_sibling_id = parent->ValueAt(nodes_idx-1);
  }
  if (nodes_idx<parent->GetSize()-1) // right sibling exists
  {
    right_sibling_id = parent->ValueAt(nodes_idx+1);
  }


  ////// sibling cases

  ////CASE 1
  if(left_sibling_id != INVALID_PAGE_ID)
  {
    WritePageGuard left_guard = bpm_->WritePage(left_sibling_id);
    auto left = left_guard.AsMut<InternalPage>();
    int left_size = left->GetSize();
    if(left_size>left->GetMinSize())
    { // borrow
      page_id_t send_to_curr_id =left->ValueAt(left_size-1); // value to send to current node 0 idx
      for(int i=curr_node->GetSize(); i>0 ;i--) // right shift to make space for old seperator key
      {
        curr_node->SetValueAt(i,curr_node->ValueAt(i-1));
        curr_node->SetKeyAt(i,curr_node->KeyAt(i-1));
      }
      curr_node->SetValueAt(0,send_to_curr_id);
      curr_node->ChangeSizeBy(1);
      left->ChangeSizeBy(-1);
      curr_node->SetKeyAt(1,parent->KeyAt(nodes_idx));
      parent->SetKeyAt(nodes_idx ,left->KeyAt(left_size-1)); // send left nodes last key to replace parents curr seperator
      ctx.header_page_ = std::nullopt;
      return;
    }
    else {
    //merge
      
      left->SetKeyAt(left_size,parent->KeyAt(nodes_idx)); // parents seperator sent to end of left sibling
      left->SetValueAt(left_size,curr_node->ValueAt(0)); // set val of 1st key of currnode to last slot in left sibling
      left->ChangeSizeBy(1);
      
      for(int i=1;i<curr_node->GetSize();i++) // append curr nodes entries to after seperator key and merge
      {
        left->SetValueAt(left_size+i,curr_node->ValueAt(i));
        left->SetKeyAt(left_size+i, curr_node->KeyAt(i));
      }
      left->SetSize(left_size + curr_node->GetSize()); // adjust the size of merged node
      curr_node->SetSize(0); // nullify the empty node after merge
      for(int i=nodes_idx;i<parent->GetSize()-1;i++) // remove seperator key from the parent after merge
      {
        parent->SetValueAt(i,parent->ValueAt(i+1));
        parent->SetKeyAt(i,parent->KeyAt(i+1));
      }
      parent->ChangeSizeBy(-1); // adjust the size of parent after removing seperator
      if(parent->GetSize()<parent->GetMinSize())
      {
        RemoveFromParent(ctx, parent_id,std::move(parent_guard));
        return;
      }
      ctx.header_page_ = std::nullopt;
      return;
    }
  }



  ////CASE 2
  else if (right_sibling_id != INVALID_PAGE_ID) // curr = right, left = curr
  {
    
    WritePageGuard right_guard = bpm_->WritePage(right_sibling_id);
    auto right = right_guard.AsMut<InternalPage>();
    int curr_size = curr_node->GetSize();
    int right_size = right->GetSize();
    if(right->GetSize()>right->GetMinSize())
    { // borrow
      page_id_t send_to_curr_id =right->ValueAt(0); // value to send to curr node last idx
      KeyType new_parent_sep = right->KeyAt(1);

      curr_node->SetKeyAt(curr_size,parent->KeyAt(nodes_idx+1)); // move old seperator to currs last index
      curr_node->SetValueAt(curr_size,send_to_curr_id);
      curr_node->ChangeSizeBy(1);

      for(int i=0; i<right_size-1 ;i++) // right shift to make space for old seperator key
      {
        right->SetValueAt(i,right->ValueAt(i+1));
        right->SetKeyAt(i,right->KeyAt(i+1));
      }
      right->ChangeSizeBy(-1);

      parent->SetKeyAt(nodes_idx+1,new_parent_sep);
      ctx.header_page_ = std::nullopt;
      return;
    }
    else {
      // merge
      curr_node->SetKeyAt(curr_size,parent->KeyAt(nodes_idx +1)); // append seperator to front of currnode
      curr_node->SetValueAt(curr_size,right->ValueAt(0));
      curr_node->ChangeSizeBy(1);

      for(int i=1; i<right_size;i++) // copy entries from right to curr to merge
      {
        curr_node->SetValueAt(curr_size+i,right->ValueAt(i));
        curr_node->SetKeyAt(curr_size+i,right->KeyAt(i));
      }
      curr_node->SetSize(curr_size + right_size); // adjust the size of curr node after merging
      right->SetSize(0); // null the empty node

      for(int i=nodes_idx+1;i<parent->GetSize()-1;i++) //remove the seperator from the parent by left shifting
      {
        parent->SetValueAt(i, parent->ValueAt(i+1));
        parent->SetKeyAt(i, parent->KeyAt(i+1));
      }

      parent->ChangeSizeBy(-1); // adjust size after removing sep

      if(parent->GetSize() < parent->GetMinSize())
      {
        RemoveFromParent(ctx, parent_id,std::move(parent_guard));
        return;
      }
      ctx.header_page_ = std::nullopt;
      return;
    }
    
  }

}//func

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.As<BPlusTreeHeaderPage>();
  int root_id = header->root_page_id_;

  if(root_id == INVALID_PAGE_ID)
  {
    return INDEXITERATOR_TYPE();
  }


  ReadPageGuard curr_guard = bpm_->ReadPage(root_id);
  auto curr_page = curr_guard.As<BPlusTreePage>();
  while(!curr_page->IsLeafPage())
  {
    auto internal = curr_guard.As<InternalPage>();
    page_id_t child_id = internal->ValueAt(0);
      curr_guard = bpm_->ReadPage(child_id);
      curr_page = curr_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_,std::move(curr_guard),0);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_pg = header_guard.As<BPlusTreeHeaderPage>();
  auto header_root_id = header_pg->root_page_id_;
  header_guard.Drop();
  if(header_root_id == INVALID_PAGE_ID) // check if tree even exists
  {
    return INDEXITERATOR_TYPE();
  }
  
  ReadPageGuard curr_guard = bpm_->ReadPage(header_root_id);
  auto curr_page = curr_guard.As<BPlusTreePage>();
  while(!curr_page->IsLeafPage())
  {
    auto internal = curr_guard.As<InternalPage>();
   
      int hi = curr_page->GetSize(),lo = 1;
      while(lo<hi)
      {
        
        int mid = lo + (hi-lo)/2;
        auto comp = comparator_(internal->KeyAt(mid),key);
        if(comp > 0) // if key is smaller than the key at current mid
        {
          hi = mid;
        }
        else  // if key is smaller than or equal to the key at current mid
        {
          lo = mid+1;
          
        }
        
      }
      page_id_t child_id = internal->ValueAt(lo -1);
      curr_guard = bpm_->ReadPage(child_id);
      curr_page = curr_guard.As<BPlusTreePage>();
      
    }
      auto leaf_pg = curr_guard.As<LeafPage>();
      int hi = curr_page->GetSize();
      int lo = 0;
      while(lo<hi)
      {
        int mid = lo + (hi-lo)/2; 
        auto comp = comparator_(leaf_pg->KeyAt(mid),key);
        if(comp > 0) // if key is smaller than the key at current mid
        {
          hi = mid;
        }
        else if (comp<0) // if key is smaller than or equal to the key at current mid
        {
          lo = mid+1;
          
        }
        else
        {
          auto tombstones = leaf_pg->GetTombstones();
          for(auto &tombs : tombstones)
          {
            if(comparator_(tombs,key) == 0)
            {
              return INDEXITERATOR_TYPE(); // the key has been logically deleted
            }
          }
          return INDEXITERATOR_TYPE(bpm_,std::move(curr_guard),lo);
          
        }
      }
      return INDEXITERATOR_TYPE();
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  return INDEXITERATOR_TYPE();
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_pg = header_guard.As<BPlusTreeHeaderPage>();
  return header_pg->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
