#include "transaction/dkv_transaction.hpp"
#include "dkv_logger.hpp"

namespace dkv {

Transaction::Transaction(TransactionID transaction_id, ReadView read_view)
    : transaction_id_(transaction_id), read_view_(read_view)  {
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

void Transaction::push_command(const Command& command) {
    commands_.push_back(command);
}

const std::vector<Command>& Transaction::get_commands() const {
    return commands_;
}

const ReadView& Transaction::get_read_view() const {
    return read_view_;
}


} // namespace dkv