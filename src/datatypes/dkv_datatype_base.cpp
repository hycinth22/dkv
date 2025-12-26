#include "datatypes/dkv_datatype_base.hpp"
#include "dkv_utils.hpp"
#include <shared_mutex>

namespace dkv {

DataItem::DataItem() 
    : expire_time_(Timestamp::min()) {
}

DataItem::DataItem(Timestamp expire_time) 
    : expire_time_(expire_time) {
}

DataItem::DataItem(const DataItem& other)
    : expire_time_(other.expire_time_.load()),
      last_accessed_(other.last_accessed_.load()),
      access_frequency_(other.access_frequency_.load()) {
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


// 锁操作方法实现
std::unique_lock<std::shared_mutex> DataItem::lock() {
    return std::unique_lock<std::shared_mutex>(item_mutex_);
}

std::shared_lock<std::shared_mutex> DataItem::rlock() {
    return std::shared_lock<std::shared_mutex>(item_mutex_);
}

std::shared_mutex& DataItem::getMutex() {
    return item_mutex_;
}

} // namespace dkv
