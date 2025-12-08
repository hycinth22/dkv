#include "dkv_transaction.hpp"
#include "dkv_logger.hpp"

namespace dkv {

Transaction::Transaction(TransactionID transaction_id)
    : transaction_id_(transaction_id)  {
    start_timestamp_ = Utils::getCurrentTime();
    // DKV_LOG_INFO("Transaction {} created at timestamp: {}", transaction_id_, start_timestamp_.time_since_epoch());
}

Transaction::~Transaction() {
}

void Transaction::push_version(const std::string& key, DataItem* item) {
    versions_.push_back({key, item});
}

const std::vector<TransactionRecordVersion>& Transaction::get_versions() const {
    return versions_;
}


} // namespace dkv