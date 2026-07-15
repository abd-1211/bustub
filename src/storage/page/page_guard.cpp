//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include "buffer/arc_replacer.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_->rwlatch_.lock_shared();
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  page_id_ = std::exchange(that.page_id_, INVALID_PAGE_ID);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  frame_ = std::move(that.frame_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = std::exchange(that.is_valid_, false);

}  // moves exisiting objs attr to new obj

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();  // drop the shared lock
    bpm_latch_ = std::move(that.bpm_latch_);
    replacer_ = std::move(that.replacer_);
    page_id_ = std::exchange(that.page_id_, INVALID_PAGE_ID);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    frame_ = std::move(that.frame_);
    is_valid_ = std::exchange(that.is_valid_, false);
  }

  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Flush() {
  auto prom = disk_scheduler_->CreatePromise();
  auto fut = prom.get_future();
  bpm_latch_->lock();
  std::vector<DiskRequest> req;
  DiskRequest r{.is_write_ = true, .data_ = frame_->data_.data(), .page_id_ = page_id_, .callback_ = std::move(prom)};
  req.push_back(std::move(r));
  disk_scheduler_->Schedule(req);
  bpm_latch_->unlock();
  fut.get();
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  if (!is_valid_)  // check if not valid
  {
    return;
  }

  frame_->rwlatch_.unlock_shared();  // allow the system to access the read latch
  bpm_latch_->lock();                // lock the lock for the bufferpoolmanager to protect replacer state
  frame_->pin_count_--;              // decrement the pin count on the frame
  if (frame_->pin_count_ == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);  // set the frame containing the data as evictable
  }
  bpm_latch_->unlock();  // unlock the lock to revert sys state to normal

  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
  is_valid_ = false;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  frame_->rwlatch_.lock();
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(
    WritePageGuard &&that) noexcept {  // move ownership from that to this instance of pageguard.
  // page_id_ = that.page_id_; // integer value can be copied because it does not invoke guard
  page_id_ = std::exchange(
      that.page_id_,
      INVALID_PAGE_ID);  // use this instead of copying integer and then invalidating. This gives the value of thats
                         // page id to this and puts invalid page id to this' instance of the obj pageid
  frame_ = std::move(that.frame_);
  bpm_latch_ = std::move(that.bpm_latch_);  // all rest moved instead of copied because if theyre copied, they will
  disk_scheduler_ =
      std::move(that.disk_scheduler_);    // both point to the same objects. that invokes guard for that as well as this
  replacer_ = std::move(that.replacer_);  // causes double freeing of guards which gives undefined behavior according
                                          // to implementation guidelines
  is_valid_ = std::exchange(that.is_valid_, false);
  // that.page_id_ = INVALID_PAGE_ID;      // invalidate page id of that to affirm that the guard for this instance is
  // dead
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that)  // no self assignment allowed
  {
    Drop();  // drop guard before changing it

    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);  // perform
    frame_ = std::move(that.frame_);                    // the
    page_id_ = std::exchange(that.page_id_,
                             INVALID_PAGE_ID);  // move from that to this and ensure prev obj set to invalid state
    replacer_ = std::move(that.replacer_);
    is_valid_ = std::exchange(that.is_valid_, false);
  }

  return *this;
}  // moves resources to existing object

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() {
  auto prom = disk_scheduler_->CreatePromise();
  auto fut = prom.get_future();
  bpm_latch_->lock();
  std::vector<DiskRequest> req;
  DiskRequest r{.is_write_ = true, .data_ = frame_->data_.data(), .page_id_ = page_id_, .callback_ = std::move(prom)};
  req.push_back(std::move(r));
  disk_scheduler_->Schedule(req);
  bpm_latch_->unlock();
  fut.get();
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  if (!is_valid_)  // check if not valid
  {
    return;
  }
  frame_->is_dirty_ = true;
  frame_->rwlatch_.unlock();  // allow the system to access the write latch
  bpm_latch_->lock();         // lock the lock for the bufferpoolmanager to protect replacer state
  frame_->pin_count_--;       // decrement the pin count on the frame
  if (frame_->pin_count_ == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);  // set the frame containing the data as evictable
  }
  bpm_latch_->unlock();  // unlock the lock to revert sys state to normal

  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
  is_valid_ = false;
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
