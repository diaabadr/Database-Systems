//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <algorithm>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
 const std::lock_guard<std::mutex> guard(latch_);

  auto page=page_table_.find(page_id);
  if(page!=page_table_.end())
  {
    auto frame=&pages_[page->second];
    frame->pin_count_++;
    replacer_->Pin(page->second);
    return frame;
    }
    frame_id_t frame_id;
    if(free_list_.empty())
    {
      bool check=replacer_->Victim(&frame_id);
      if(!check)
      return nullptr;
    }
    else{
      frame_id=free_list_.front();
      free_list_.pop_front();
    }
    auto deleted_frame=&pages_[frame_id];
    if(deleted_frame->IsDirty())
    {
      deleted_frame->is_dirty_=false;
      disk_manager_->WritePage(deleted_frame->GetPageId(),deleted_frame->GetData());
    }

    page_table_.erase(deleted_frame->GetPageId());
    page_table_.insert({page_id,frame_id});

    deleted_frame->page_id_=page_id;
    deleted_frame->pin_count_++;
    replacer_->Pin(frame_id);

    disk_manager_->ReadPage(page_id,deleted_frame->GetData());

    return deleted_frame;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {

    /*
     * Implementation of unpin page
     * if pin_count>0, decrement it and if it becomes zero, put it back to
     * replacer if pin_count<=0 before this call, return false. is_dirty: set the
     * dirty flag of this page
     */

 const std::lock_guard<std::mutex> guard(latch_);

 auto page_frame=page_table_.find(page_id);
 if(page_frame==page_table_.end())
 return false;
 auto page=&pages_[page_frame->second];
 page->pin_count_--;
 page->is_dirty_|=is_dirty;
 if(!page->GetPinCount())
 {
  replacer_->Unpin(page_frame->second);
 }
 return page->GetPinCount()+1>0;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
/*
     * Used to flush a particular page of the buffer pool to disk. Should call the
     * write_page method of the disk manager
     * if page is not found in page table, return false
     * NOTE: make sure page_id != INVALID_PAGE_ID
     */
  const std::lock_guard<std::mutex> guard(latch_);
  auto page_frame=page_table_.find(page_id);
  if(page_id==INVALID_PAGE_ID)
  return false;
  if(page_frame==page_table_.end())
  return false;

  auto page=&pages_[page_frame->second];
  if(page->IsDirty())
    disk_manager_->WritePage(page->GetPageId(),page->GetData());
    page->is_dirty_=false;
    return true;

}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

   const std::lock_guard<std::mutex> guard(latch_);
   bool flage=false;
   for(size_t i=0;i<pool_size_;i++)
   {
    if(pages_[i].GetPinCount()<=0)
    {
    flage=true;
    break;
    }
   }
   if(!flage)
   return nullptr;
   frame_id_t frame_id;
   if(free_list_.empty())
   {
    bool check=replacer_->Victim(&frame_id);
    if(!check)
    return nullptr;
   }
   frame_id=free_list_.front();
   free_list_.pop_front();

   auto frame=&pages_[frame_id];
   if(frame->IsDirty())
   {
    disk_manager_->WritePage(frame->GetPageId(),frame->GetData());
    frame->is_dirty_=false;
   }
   page_table_.erase(frame->GetPageId());
   auto new_id=disk_manager_->AllocatePage();
   frame->page_id_=new_id;
   replacer_->Pin(frame_id);
   frame->pin_count_++;
   frame->ResetMemory();

   page_table_.insert({frame->GetPageId(),frame_id});

   *page_id=new_id;
   return frame;

 
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {

  const std::lock_guard<std::mutex> guard(latch_);
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

auto page_frame=page_table_.find(page_id);
if(page_frame==page_table_.end())
return true;

auto page=&pages_[page_frame->second];
if(page->GetPinCount()!=0)
return false;
page_table_.erase(page_frame);
page->ResetMemory();
page->page_id_=INVALID_PAGE_ID;
page->pin_count_=0;
page->is_dirty_=false;
free_list_.push_front(page_frame->second);
return true;

}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!

  for(size_t i=0;i<pool_size_;i++)
  {
    if(pages_[i].GetPageId()!=INVALID_PAGE_ID&&pages_[i].IsDirty())
    {
      disk_manager_->WritePage(pages_[i].GetPageId(),pages_[i].GetData());
      pages_[i].is_dirty_=false;
    }
  }

}

}  // namespace bustub
