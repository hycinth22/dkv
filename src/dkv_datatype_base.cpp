#include "dkv_datatype_base.hpp"
#include "dkv_utils.hpp"

namespace dkv {

void DataItem::touch() { last_accessed_ = Utils::getCurrentTime(); }
Timestamp DataItem::getLastAccessed() const { return last_accessed_; }
void DataItem::incrementFrequency() { access_frequency_++; }
uint64_t DataItem::getAccessFrequency() const { return access_frequency_; }

} // namespace dkv
