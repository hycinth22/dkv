#include "dkv_datatype_base.hpp"
#include "dkv_utils.hpp"

namespace dkv {

DataItem::DataItem() 
    : expire_time_(Timestamp::min()) {
}

DataItem::DataItem(Timestamp expire_time) 
    : expire_time_(expire_time) {
}

// TTL方法实现
bool DataItem::isExpired() const {
    if (!hasExpiration()) {
        return false;
    }
    return getExpiration() < Utils::getCurrentTime();
}

void DataItem::setExpiration(Timestamp expire_time) {
    expire_time_ = expire_time;
}

Timestamp DataItem::getExpiration() const {
    return expire_time_;
}

bool DataItem::hasExpiration() const {
    return expire_time_.load() != Timestamp::min();
}

// 淘汰策略方法实现
void DataItem::touch() { last_accessed_ = Utils::getCurrentTime(); }
Timestamp DataItem::getLastAccessed() const { return last_accessed_; }
void DataItem::incrementFrequency() { access_frequency_++; }
uint64_t DataItem::getAccessFrequency() const { return access_frequency_; }

} // namespace dkv
