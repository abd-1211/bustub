#include "storage/page/intermediate_result_page.h"
#include <sys/types.h>
#include <cstdint>
#include "storage/table/tuple.h"

namespace bustub {

void IntermediateResultPage::Init()
{
    tuple_count_ = 0;
    free_space_offset_ = 0;
}

auto IntermediateResultPage::InsertTuple(const Tuple &tuple) -> bool
{
    // each tuple occupies: 4 bytes (length prefix) + tuple.GetLength() bytes
    uint32_t required = sizeof(uint32_t) + tuple.GetLength();
    if(free_space_offset_ + required > DATA_SIZE)
    {
        return false; // too big to fit in this page, new page should be used
    }

    tuple.SerializeTo(data_ + free_space_offset_); // fits so add the tuple in the page
    free_space_offset_ += required;
    tuple_count_++;
    return true;
}

auto IntermediateResultPage::GetTupleAt(uint32_t index) const -> Tuple
{
    BUSTUB_ASSERT(index < tuple_count_, "IntermediateResultPage::GetTupleAt index out of range");
    uint32_t offset = 0;
    for(uint32_t i=0; i< index; i++)
    {
        uint32_t len = *reinterpret_cast<const uint32_t *>(data_ + offset); // get current length of the page
        offset += sizeof(uint32_t) + len; // increment byte offset as you traverse
    }

    Tuple tuple;
    tuple.DeserializeFrom(data_ + offset);
    return tuple;

}



}