/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 * 
 * 实现逻辑：
 * 1. 检查传入的事务指针是否为空
 * 2. 如果为空，创建新的Transaction对象，分配唯一的事务ID和时间戳
 * 3. 将新事务加入全局事务表txn_map中，便于后续通过事务ID查找事务
 * 4. 设置事务状态为GROWING（增长阶段，可以获取锁）
 * 5. 返回事务指针
 * 
 * 注意事项：
 * - 使用原子变量next_txn_id_和next_timestamp_保证ID分配的线程安全
 * - 使用互斥锁latch_保护全局事务表txn_map的并发访问
 * - 事务创建后进入GROWING阶段，此时可以申请各种锁
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Step 1: 判断传入事务参数是否为空指针
    if (txn == nullptr) {
        // Step 2: 如果为空指针，创建新事务
        // 分配唯一的事务ID，使用原子操作保证线程安全
        txn_id_t new_txn_id = next_txn_id_.fetch_add(1);
        // 创建新的Transaction对象，默认使用可串行化隔离级别
        txn = new Transaction(new_txn_id);
        // 分配事务开始时间戳
        txn->set_start_ts(next_timestamp_.fetch_add(1));
    }
    
    // Step 3: 把开始事务加入到全局事务表中
    // 使用互斥锁保护txn_map的并发访问，防止多线程同时修改导致数据竞争
    std::unique_lock<std::mutex> lock(latch_);
    txn_map[txn->get_transaction_id()] = txn;
    lock.unlock();
    
    // 设置事务状态为GROWING（两阶段封锁协议的增长阶段）
    // 在GROWING阶段，事务可以申请各种锁，但不能释放锁
    txn->set_state(TransactionState::GROWING);
    
    // Step 4: 返回当前事务指针
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 * 
 * 实现逻辑：
 * 1. 清空事务的写操作集合（write_set_）
 *    - 提交时不需要回滚，所以直接清空写记录
 * 2. 释放事务持有的所有锁（两阶段封锁的收缩阶段）
 *    - 遍历lock_set_，对每个锁调用unlock释放
 * 3. 清空锁集合
 * 4. 更新事务状态为COMMITTED
 * 
 * 两阶段封锁协议说明：
 * - 增长阶段(GROWING)：事务可以申请锁，但不能释放锁
 * - 收缩阶段(SHRINKING)：事务可以释放锁，但不能申请锁
 * - 提交时进入收缩阶段，释放所有锁后事务结束
 * 
 * 注意事项：
 * - 写操作记录需要在事务中维护，以支持可能的回滚操作
 * - 锁的释放应该在事务提交/终止时统一进行
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Step 1: 清空写操作集合
    // 提交成功后，写操作记录不再需要（用于回滚的数据可以丢弃）
    auto write_set = txn->get_write_set();
    // 释放写操作记录占用的内存
    while (!write_set->empty()) {
        delete write_set->back();  // 释放WriteRecord对象
        write_set->pop_back();
    }
    write_set->clear();
    
    // Step 2: 释放所有锁（进入两阶段封锁的收缩阶段）
    // 获取事务持有的所有锁的集合
    auto lock_set = txn->get_lock_set();
    // 遍历锁集合，逐个释放锁
    for (const auto& lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }
    
    // Step 3: 清空锁集合
    lock_set->clear();
    
    // Step 4: 更新事务状态为已提交
    // COMMITTED状态表示事务已成功完成，其修改已持久化
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 * 
 * 实现逻辑：
 * 1. 按LIFO（后进先出）顺序回滚所有写操作
 *    - 对于INSERT操作：删除插入的记录
 *    - 对于DELETE操作：重新插入被删除的记录到原位置
 *    - 对于UPDATE操作：将记录恢复为原值
 * 2. 释放事务持有的所有锁
 * 3. 清空写操作集合和锁集合
 * 4. 更新事务状态为ABORTED
 * 
 * 回滚顺序说明：
 * - 必须按照与执行顺序相反的顺序回滚（LIFO）
 * - 这样可以正确处理对同一记录的多次操作
 * - 例如：先INSERT再UPDATE，回滚时先撤销UPDATE再撤销INSERT
 * 
 * 注意事项：
 * - 回滚DELETE时必须将记录插入到原位置（保持Rid不变）
 *   因为可能存在其他事务或索引引用该Rid
 * - 写操作记录(WriteRecord)包含了回滚所需的所有信息
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Step 1: 回滚所有写操作
    // 获取事务的写操作集合
    auto write_set = txn->get_write_set();
    
    // 按LIFO顺序（从后向前）遍历写操作，逐个撤销
    // 使用反向迭代，确保后执行的操作先被撤销
    while (!write_set->empty()) {
        // 取出最后一个写操作
        WriteRecord* write_record = write_set->back();
        write_set->pop_back();
        
        // 获取该写操作相关的表名和记录句柄
        std::string& tab_name = write_record->GetTableName();
        RmFileHandle* fh = sm_manager_->fhs_.at(tab_name).get();
        
        // 根据写操作类型执行相应的回滚操作
        switch (write_record->GetWriteType()) {
            case WType::INSERT_TUPLE: {
                // 回滚INSERT操作：删除插入的记录
                // INSERT记录只存储了rid，需要删除该位置的记录
                fh->delete_record(write_record->GetRid(), nullptr);
                break;
            }
            case WType::DELETE_TUPLE: {
                // 回滚DELETE操作：将被删除的记录重新插入到原位置
                // DELETE记录存储了rid和原始数据，需要将数据插回原位置
                // 注意：必须插入到原位置，因为可能存在索引等引用该rid
                fh->insert_record(write_record->GetRid(), write_record->GetRecord().data);
                break;
            }
            case WType::UPDATE_TUPLE: {
                // 回滚UPDATE操作：将记录恢复为原值
                // UPDATE记录存储了rid和修改前的数据，需要用原数据覆盖当前数据
                fh->update_record(write_record->GetRid(), write_record->GetRecord().data, nullptr);
                break;
            }
        }
        
        // 释放写操作记录占用的内存
        delete write_record;
    }
    write_set->clear();
    
    // Step 2: 释放所有锁
    // 获取事务持有的锁集合
    auto lock_set = txn->get_lock_set();
    // 遍历并释放所有锁
    for (const auto& lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }
    
    // Step 3: 清空锁集合
    lock_set->clear();
    
    // Step 4: 更新事务状态为已终止
    // ABORTED状态表示事务已被撤销，其所有修改已被回滚
    txn->set_state(TransactionState::ABORTED);
}