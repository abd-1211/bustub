//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  for (auto &obj : requests) {
    request_queue_.Put(std::move(obj));  // std move because promise included in diskrequest class is not copyable
  }
}
/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto req = request_queue_.Get();  // get 1 request from the scheduled requests using FIFO
    if (!req.has_value())             // if no more requests break loop destructor puts null output
    {
      break;
    } else  // if requests exist
    {
      if (req->is_write_)  // write reqs
      {
        disk_manager_->WritePage(req->page_id_, req->data_);
      } else  // read reqs
      {
        disk_manager_->ReadPage(req->page_id_, req->data_);
      }
      req->callback_.set_value(true);  // signal to the request issuer that the req has been completed
    }
  }
}

}  // namespace bustub
