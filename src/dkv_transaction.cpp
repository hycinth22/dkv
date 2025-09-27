#include "dkv_transaction.hpp"
#include "dkv_logger.hpp"

namespace dkv {

Transaction::Transaction(TransactionID transaction_id)
    : active_(true), transaction_id_(transaction_id)  {
    start_timestamp_ = Utils::getCurrentTime();
    // DKV_LOG_INFO("Transaction {} began at timestamp: {}", transaction_id_, start_timestamp_.time_since_epoch());
}

Transaction::~Transaction() {
    if (active_) {
        // 如果析构时事务仍处于活跃状态，自动回滚
        DKV_LOG_WARNING("Active transaction being destructed, rolling back...");
        // rollback();
    }
}

void Transaction::deactivate() {
    if (!active_) {
        DKV_LOG_ERROR("Cannot deactivate inactive transaction");
        return;
    }
    active_ = false;
    // DKV_LOG_INFO("Transaction {} deactivated at timestamp: {}", transaction_id_, start_timestamp_.time_since_epoch());
}

} // namespace dkv