//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <vector>
#include "common/macros.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

auto MergeSortRun::Iterator::operator++() -> Iterator & {
  tuple_idx_++;
  while (page_idx_ < run_->pages_.size()) {
    auto guard = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
    const auto *page = guard.As<IntermediateResultPage>();
    if (tuple_idx_ < page->GetTupleCount()) {
      return *this;
    }
    page_idx_++;
    tuple_idx_ = 0;
  }
  return *this;
}

auto MergeSortRun::Iterator::operator*() -> Tuple {
  auto guard = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
  const auto *page = guard.As<IntermediateResultPage>();
  return page->GetTupleAt(tuple_idx_);
}

auto MergeSortRun::Iterator::operator==(const Iterator &other) const -> bool {
  return run_ == other.run_ && page_idx_ == other.page_idx_ && tuple_idx_ == other.tuple_idx_;
}

auto MergeSortRun::Iterator::operator!=(const Iterator &other) const -> bool { return !(*this == other); }

auto MergeSortRun::Begin() -> Iterator {
  Iterator it(this);
  it.page_idx_ = 0;
  it.tuple_idx_ = 0;
  return it;
}

auto MergeSortRun::End() -> Iterator {
  Iterator it(this);
  it.page_idx_ = pages_.size();
  it.tuple_idx_ = 0;
  return it;
}

namespace {

/** Merges two sorted runs into a single new sorted run, writing output across as many
 * fresh pages as needed. Does not delete the input runs' pages -- caller's job. */
auto MergeTwoRuns(MergeSortRun &left, MergeSortRun &right, BufferPoolManager *bpm, const TupleComparator &cmp,
                  const std::vector<OrderBy> &order_bys, const Schema &schema) -> MergeSortRun {
  std::vector<page_id_t> out_pages;
  page_id_t cur_pid = bpm->NewPage();
  out_pages.push_back(cur_pid);
  auto cur_guard = bpm->WritePage(cur_pid);
  auto *cur_page = cur_guard.AsMut<IntermediateResultPage>();
  cur_page->Init();

  auto write_tuple = [&](const Tuple &t) {
    if (!cur_page->InsertTuple(t)) {
      cur_pid = bpm->NewPage();
      out_pages.push_back(cur_pid);
      cur_guard = bpm->WritePage(cur_pid);
      cur_page = cur_guard.AsMut<IntermediateResultPage>();
      cur_page->Init();
      cur_page->InsertTuple(t);
    }
  };

  auto it_l = left.Begin();
  auto end_l = left.End();
  auto it_r = right.Begin();
  auto end_r = right.End();

  while (it_l != end_l && it_r != end_r) {
    Tuple tl = *it_l;
    Tuple tr = *it_r;
    SortEntry el{GenerateSortKey(tl, order_bys, schema), tl};
    SortEntry er{GenerateSortKey(tr, order_bys, schema), tr};
    if (cmp(el, er)) {
      write_tuple(tl);
      ++it_l;
    } else {
      write_tuple(tr);
      ++it_r;
    }
  }
  while (it_l != end_l) {
    write_tuple(*it_l);
    ++it_l;
  }
  while (it_r != end_r) {
    write_tuple(*it_r);
    ++it_r;
  }

  return MergeSortRun(out_pages, bpm);
}

}  // namespace

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  const auto &schema = child_executor_->GetOutputSchema();
  const auto &order_bys = plan_->GetOrderBy();

  std::vector<MergeSortRun> runs;
  std::vector<Tuple> current_tuples;
  size_t current_size = 0;

  auto flush = [&]() {
    if (current_tuples.empty()) {
      return;
    }
    std::vector<SortEntry> entries;
    for (auto &t : current_tuples) {
      entries.emplace_back(GenerateSortKey(t, order_bys, schema), t);
    }
    std::sort(entries.begin(), entries.end(), cmp_);

    

    page_id_t pid = bpm->NewPage();
    {
      auto guard = bpm->WritePage(pid);
      auto *page = guard.AsMut<IntermediateResultPage>();
      page->Init();
      for (auto &e : entries) {
        page->InsertTuple(e.second);
      }
    }
    runs.emplace_back(std::vector<page_id_t>{pid}, bpm);
    current_tuples.clear();
    current_size = 0;
  };

  
  std::vector<Tuple> batch;
  std::vector<RID> rids;
  while (child_executor_->Next(&batch, &rids, BUSTUB_BATCH_SIZE)) {
    
    for (auto &t : batch) {
      size_t needed = sizeof(uint32_t) + t.GetLength();
      if (current_size + needed > IntermediateResultPage::DATA_SIZE) {
        flush();
      }
      current_tuples.push_back(t);
      current_size += needed;
    }
  }
  flush();

  
  

  while (runs.size() > 1) {
    std::vector<MergeSortRun> next_runs;
    size_t i = 0;
    for (; i + 1 < runs.size(); i += 2) {
      next_runs.push_back(MergeTwoRuns(runs[i], runs[i + 1], bpm, cmp_, order_bys, schema));
      for (auto pid : runs[i].GetPages()) {
        bpm->DeletePage(pid);
      }
      for (auto pid : runs[i + 1].GetPages()) {
        bpm->DeletePage(pid);
      }
    }
    if (i < runs.size()) {
      next_runs.push_back(runs[i]);
    }
    runs = std::move(next_runs);
    
    
  }

  if (runs.empty()) {
    result_run_ = MergeSortRun({}, bpm);
  } else {
    result_run_ = runs[0];
  }

  
  

  iter_ = result_run_->Begin();
  end_iter_ = result_run_->End();
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  while (iter_ != end_iter_ && tuple_batch->size() < batch_size) {
    tuple_batch->push_back(*iter_);
    rid_batch->push_back(RID{});
    ++iter_;
  }
  return !tuple_batch->empty();
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub