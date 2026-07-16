
#include <sys/types.h>


#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"

#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort and hash join.
 * Supports variable-length tuples.
 */
class IntermediateResultPage {
 public:
  /**
   * TODO(P3): Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */

   // size of the page data block, everything other than the header
   static constexpr size_t DATA_SIZE = BUSTUB_PAGE_SIZE - 2 * sizeof(uint32_t);
   // initializes empty pg
   void Init();
   // gets no of tuples in the page atm
   auto GetTupleCount() const -> uint32_t {return tuple_count_;}
   // adds tuple to the end of page and returns true. Returns false if not enough space in page (get a new page and insert there).
   auto InsertTuple(const Tuple &tuple) -> bool;
   // gives the place in the current offset index of the page
   auto GetTupleAt(uint32_t index) const -> Tuple;
   // gives how many free bytes are remaining in the pages data block
   auto GetFreeSpaceRemaining() const -> size_t {return DATA_SIZE - free_space_offset_;}
 private:
  /**
   * TODO(P3): Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
   uint32_t tuple_count_;
   uint32_t free_space_offset_;
   char data_[DATA_SIZE];
};

  // Sanity check: this class must fit exactly within one page.
  static_assert(sizeof(IntermediateResultPage) == BUSTUB_PAGE_SIZE,
              "IntermediateResultPage is not the size of a page.");

}  // namespace bustub
