#include "dkv_storage_mvcc.hpp"
#include "dkv_storage.hpp"
#include "dkv_logger.hpp"
#include <vector>
#include <memory>
#include <atomic>
#include <algorithm>
#include <cassert>
using namespace std;

namespace dkv {

// MVCC类，提供多版本并发控制

// 获取指定事务可见的版本
DataItem* MVCC::get(ReadView& read_view, const Key& key) {
    auto readlock = inner_storage_.rlock();
    // 查找键
    auto it = inner_storage_.find(key);
    if (it == inner_storage_.end()) {
        return nullptr;
    }
    const unique_ptr<DataItem>& entry = it->second;
    assert(entry);
    DKV_LOG_DEBUG("latest version for key ", key, " is writeen by tx ", entry->getTransactionId());

    // 检查事务可见性
    if (!read_view.isVisible(entry->getTransactionId())) {
        // 最新版本对事务不可见，需要找历史版本
        DKV_LOG_DEBUG("Lookup history version for key:", key, " with read_view: ", read_view);
        UndoLog *undo_log = entry->getUndoLog().get();
        assert(undo_log->old_value != nullptr && "undo_log->old_value should not be nullptr");
        while(undo_log != nullptr) {
            if (read_view.isVisible(undo_log->old_value->getTransactionId())) {
                break;
            } else {
                DKV_LOG_DEBUG("history version ", undo_log->old_value->getTransactionId(), " is not visible", key);
            }
            DKV_LOG_DEBUG("undo_log->old_value->getTransactionId(): ", undo_log->old_value->getTransactionId());
            undo_log = undo_log->old_value->getUndoLog().get();
        }
        if (undo_log == nullptr) {
            // 没有可见的历史版本，返回空指针
            DKV_LOG_DEBUG("no visible history version for key: {}", key);
            return nullptr;
        }
        // 如果历史版本是删除状态，返回空指针
        if (undo_log->old_value->isDeleted()) {
            DKV_LOG_DEBUG("history version for key: {} is deleted", key);
            return nullptr;
        }
        // 找到可见的历史版本，返回其旧值
        DKV_LOG_DEBUG("visible history version for key: {} is", key);
        return undo_log->old_value.get();
    }
    // 返回最新版本
    return it->second.get();
}

// 设置键值，并记录到UNDOLOG
bool MVCC::set(TransactionID tx_id, const Key& key, unique_ptr<DataItem> item) {
    auto writelock = inner_storage_.wlock();

    unique_ptr<DataItem>& entry = inner_storage_.getRefOrInsert(key);

    // 保存旧值到UndoLog
    unique_ptr<UndoLog> new_undo_log = make_unique<UndoLog>();
    new_undo_log->ty = UndoLogType::SET;
    new_undo_log->old_value = move(entry);
    if (!new_undo_log->old_value) {
        new_undo_log->old_value = item->clone();
        new_undo_log->old_value->setDeleted(true);
    }

    // 存储新值
    entry = move(item);
    entry->setTransactionId(tx_id);
    entry->setUndoLog(move(new_undo_log));
    return true;
}

// 删除键，并记录到UNDOLOG
bool MVCC::del(TransactionID tx_id, const Key& key, unique_ptr<DataItem> virtual_item) {
    auto writelock = inner_storage_.wlock();
    
    unique_ptr<DataItem>& entry = inner_storage_.getRefOrInsert(key);

    // 保存旧值到UndoLog
    unique_ptr<UndoLog> new_undo_log = make_unique<UndoLog>();
    new_undo_log->ty = UndoLogType::DELETE;
    new_undo_log->old_value = move(entry);

    // 删除键
    entry = move(virtual_item);
    entry->setTransactionId(tx_id);
    entry->setDeleted(true);
    entry->setUndoLog(move(new_undo_log));
    return true;
}

// 创建ReadView，用于实现可重复读隔离级别
ReadView MVCC::createReadView(TransactionID tx_id, TransactionManager& txm) {
    ReadView read_view;
    read_view.creator = tx_id;
    read_view.actives = txm.getActiveTransactions();
    auto pmin = min_element(read_view.actives.begin(), read_view.actives.end());
    read_view.low = (pmin != read_view.actives.end() ? *pmin : 0);
    read_view.high = txm.peekNextTransactionID();
    return read_view;
}

// 检查数据项对指定ReadView是否可见
bool ReadView::isVisible(TransactionID tx_id) {
    // 如果数据项的事务ID小于read_view.low，说明事务已提交，则可见
    if (tx_id < this->low) {
        return true;
    }
    // 如果数据项的事务ID大于等于read_view.high，说明事务未开始，则不可见
    if (tx_id >= this->high) {
        return false;
    }
    // 如果数据项的事务ID等于read_view.creator，则可见
    if (tx_id == this->creator) {
        return true;
    }
    // 如果tx_id在read_view.actives中，说明事务已开始未提交，则不可见
    if (find(this->actives.begin(), this->actives.end(), tx_id) != this->actives.end()) {
        return false;
    }
    // 否则说明tx_id已提交，则可见
    return true;
}

} // namespace dkv