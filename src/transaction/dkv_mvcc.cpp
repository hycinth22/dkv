#include "transaction/dkv_mvcc.hpp"
#include "storage/dkv_storage.hpp"
#include "dkv_logger.hpp"
#include <vector>
#include <memory>
#include <atomic>
#include <cassert>
using namespace std;

namespace dkv {

// MVCC类，提供多版本并发控制

// 获取指定事务可见的版本
DataItem* MVCC::get(const ReadView& read_view, const Key& key) const{
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
    if (read_view.isVisible(entry->getTransactionId()) && !entry->isDiscard()) {
        // 最新版本对事务可见，直接返回
        return it->second.get();
    }
    // 最新版本对事务不可见或已删除，需要找历史版本
    DKV_LOG_DEBUG("Lookup history version for key:", key, " with read_view: ", read_view);
    UndoLog *undo_log = entry->getUndoLog().get();
    while(undo_log != nullptr) {
        const unique_ptr<DataItem>& old_item = undo_log->old_value;
        if (read_view.isVisible(old_item->getTransactionId()) && !old_item->isDiscard()) {
            // 找到可见的历史版本
            // 如果历史版本是删除状态，返回空指针
            if (undo_log->old_value->isDeleted()) {
                DKV_LOG_DEBUG("history version for key: {} is deleted", key);
                return nullptr;
            }
            // 如果历史版本是非删除状态，返回数据项
            DKV_LOG_DEBUG("visible history version for key: {} is tx {}", key, old_item->getTransactionId());
            return old_item.get();
        }
        DKV_LOG_DEBUG("history version tx {} for key {} is not visible", old_item->getTransactionId(), key);
        undo_log = old_item->getUndoLog().get();
    }
    // 没有可见的历史版本，返回空指针
    DKV_LOG_DEBUG("no visible history version for key: {}", key);
    return nullptr;
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
bool MVCC::del(TransactionID tx_id, const Key& key) {
    auto writelock = inner_storage_.wlock();

    unique_ptr<DataItem>& entry = inner_storage_.getRefOrInsert(key);

    // 保存旧值到UndoLog
    unique_ptr<UndoLog> new_undo_log = make_unique<UndoLog>();
    new_undo_log->ty = UndoLogType::DELETE;
    new_undo_log->old_value = move(entry);

    // 创建删除标记的虚拟数据项
    // 使用之前保存到undo_log的旧值来克隆，而不是空的entry
    unique_ptr<DataItem> virtual_item = new_undo_log->old_value->clone();
    virtual_item->setTransactionId(tx_id);
    virtual_item->setDeleted(true);
    virtual_item->setUndoLog(move(new_undo_log));
    entry = move(virtual_item);
    return true;
}

// 检查数据项对指定ReadView是否可见
bool ReadView::isVisible(TransactionID tx_id) const {
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