/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * 锁相容矩阵（Lock Compatibility Matrix）
 * 
 * 多粒度锁协议中，不同类型的锁之间存在相容性关系。
 * 两个锁相容意味着它们可以同时被不同事务持有。
 * 
 *          | IS  | IX  |  S  |  X  | SIX |
 *    ------+-----+-----+-----+-----+-----+
 *      IS  |  √  |  √  |  √  |  ×  |  √  |
 *      IX  |  √  |  √  |  ×  |  ×  |  ×  |
 *       S  |  √  |  ×  |  √  |  ×  |  ×  |
 *       X  |  ×  |  ×  |  ×  |  ×  |  ×  |
 *     SIX  |  √  |  ×  |  ×  |  ×  |  ×  |
 * 
 * 说明：
 * - IS (Intention Shared): 意向共享锁，表示下层有共享锁
 * - IX (Intention Exclusive): 意向排他锁，表示下层有排他锁
 * - S (Shared): 共享锁，允许读操作
 * - X (Exclusive): 排他锁，允许读写操作
 * - SIX (S + IX): 共享+意向排他锁
 * 
 * No-Wait死锁预防策略：
 * 当事务无法立即获得锁时，直接抛出异常终止事务，
 * 避免等待导致的死锁问题。
 */

/**
 * @description: 申请行级共享锁（S锁）
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的文件描述符
 * 
 * 实现逻辑：
 * 1. 检查事务状态，SHRINKING阶段不能申请锁
 * 2. 构造锁ID（LockDataId）
 * 3. 检查事务是否已持有该锁
 * 4. 检查锁相容性，不相容则使用no-wait策略抛出异常
 * 5. 将锁请求加入锁表和事务的锁集合
 * 6. 更新锁队列的组锁模式
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // Step 1: 检查事务状态
    // 两阶段封锁协议：SHRINKING阶段不能再申请新锁
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // Step 2: 构造行级锁的唯一标识
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    
    // Step 3: 加锁保护lock_table_的并发访问
    std::unique_lock<std::mutex> lock(latch_);
    
    // Step 4: 获取或创建该数据项的锁请求队列
    auto& request_queue = lock_table_[lock_data_id];
    
    // Step 5: 检查事务是否已持有该锁
    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            // 事务已持有该锁，检查是否需要锁升级
            // 如果已持有S锁或X锁，则无需再申请S锁
            if (req.lock_mode_ == LockMode::SHARED || req.lock_mode_ == LockMode::EXLUCSIVE) {
                return true;  // 已持有相同或更强的锁
            }
        }
    }
    
    // Step 6: 检查锁相容性（使用no-wait策略）
    // S锁与X锁不相容
    if (request_queue.group_lock_mode_ == GroupLockMode::X) {
        // 存在排他锁，无法获得共享锁，使用no-wait策略抛出异常
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // Step 7: 将锁请求加入队列并标记为已授予
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // Step 8: 更新锁队列的组锁模式
    // 如果当前组锁模式为NON_LOCK或IS，则更新为S
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
        request_queue.group_lock_mode_ == GroupLockMode::IS) {
        request_queue.group_lock_mode_ = GroupLockMode::S;
    }
    // 如果当前为IX，加上S锁后变为SIX
    else if (request_queue.group_lock_mode_ == GroupLockMode::IX) {
        request_queue.group_lock_mode_ = GroupLockMode::SIX;
    }
    
    // Step 9: 将锁ID加入事务的锁集合，用于事务提交/终止时释放
    txn->get_lock_set()->insert(lock_data_id);
    
    return true;
}

/**
 * @description: 申请行级排他锁（X锁）
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的文件描述符
 * 
 * 实现逻辑与lock_shared_on_record类似，但：
 * 1. X锁与所有其他锁都不相容（除了同一事务的锁）
 * 2. 需要支持锁升级（从S锁升级到X锁）
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // Step 1: 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // Step 2: 构造行级锁的唯一标识
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    
    // Step 3: 加锁保护lock_table_
    std::unique_lock<std::mutex> lock(latch_);
    
    // Step 4: 获取或创建锁请求队列
    auto& request_queue = lock_table_[lock_data_id];
    
    // Step 5: 检查事务是否已持有该锁
    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::EXLUCSIVE) {
                // 已持有X锁，无需再申请
                return true;
            }
            if (req.lock_mode_ == LockMode::SHARED) {
                // 持有S锁，尝试升级为X锁
                // 检查是否只有当前事务持有锁
                if (request_queue.request_queue_.size() == 1) {
                    // 只有当前事务，可以安全升级
                    req.lock_mode_ = LockMode::EXLUCSIVE;
                    request_queue.group_lock_mode_ = GroupLockMode::X;
                    return true;
                } else {
                    // 有其他事务持有锁，使用no-wait策略
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                }
            }
        }
    }
    
    // Step 6: 检查锁相容性
    // X锁与任何非空的锁都不相容
    if (request_queue.group_lock_mode_ != GroupLockMode::NON_LOCK) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // Step 7: 将锁请求加入队列
    LockRequest lock_request(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // Step 8: 更新组锁模式为X
    request_queue.group_lock_mode_ = GroupLockMode::X;
    
    // Step 9: 将锁ID加入事务的锁集合
    txn->get_lock_set()->insert(lock_data_id);
    
    return true;
}

/**
 * @description: 申请表级读锁（S锁）
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的文件描述符
 * 
 * 表级S锁的相容性：
 * - 与IS、S相容
 * - 与IX、X、SIX不相容
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    // Step 1: 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // Step 2: 构造表级锁的唯一标识
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    
    // Step 3: 加锁保护lock_table_
    std::unique_lock<std::mutex> lock(latch_);
    
    auto& request_queue = lock_table_[lock_data_id];
    
    // Step 4: 检查事务是否已持有该锁
    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            // 检查已持有的锁类型
            if (req.lock_mode_ == LockMode::SHARED || 
                req.lock_mode_ == LockMode::EXLUCSIVE ||
                req.lock_mode_ == LockMode::S_IX) {
                return true;  // 已持有相同或更强的锁
            }
            // 如果持有IS锁，尝试升级为S锁
            if (req.lock_mode_ == LockMode::INTENTION_SHARED) {
                // 检查是否可以升级（与IX、X、SIX不相容）
                if (request_queue.group_lock_mode_ == GroupLockMode::IX ||
                    request_queue.group_lock_mode_ == GroupLockMode::X ||
                    request_queue.group_lock_mode_ == GroupLockMode::SIX) {
                    // 检查是否只有当前事务
                    bool only_current_txn = true;
                    for (auto& r : request_queue.request_queue_) {
                        if (r.txn_id_ != txn->get_transaction_id() && r.granted_) {
                            only_current_txn = false;
                            break;
                        }
                    }
                    if (!only_current_txn) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
                req.lock_mode_ = LockMode::SHARED;
                // 重新计算组锁模式
                request_queue.group_lock_mode_ = GroupLockMode::S;
                return true;
            }
            // 如果持有IX锁，升级为SIX锁
            if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
                // 检查相容性
                if (request_queue.group_lock_mode_ == GroupLockMode::IX ||
                    request_queue.group_lock_mode_ == GroupLockMode::X ||
                    request_queue.group_lock_mode_ == GroupLockMode::SIX) {
                    bool only_current_txn = true;
                    for (auto& r : request_queue.request_queue_) {
                        if (r.txn_id_ != txn->get_transaction_id() && r.granted_) {
                            only_current_txn = false;
                            break;
                        }
                    }
                    if (!only_current_txn) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
                req.lock_mode_ = LockMode::S_IX;
                request_queue.group_lock_mode_ = GroupLockMode::SIX;
                return true;
            }
        }
    }
    
    // Step 5: 检查锁相容性
    // S锁与IX、X、SIX不相容
    if (request_queue.group_lock_mode_ == GroupLockMode::IX ||
        request_queue.group_lock_mode_ == GroupLockMode::X ||
        request_queue.group_lock_mode_ == GroupLockMode::SIX) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // Step 6: 加入锁请求
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // Step 7: 更新组锁模式
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
        request_queue.group_lock_mode_ == GroupLockMode::IS) {
        request_queue.group_lock_mode_ = GroupLockMode::S;
    }
    
    txn->get_lock_set()->insert(lock_data_id);
    
    return true;
}

/**
 * @description: 申请表级写锁（X锁）
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的文件描述符
 * 
 * 表级X锁与所有其他锁都不相容
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    // Step 1: 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // Step 2: 构造表级锁的唯一标识
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    
    // Step 3: 加锁保护lock_table_
    std::unique_lock<std::mutex> lock(latch_);
    
    auto& request_queue = lock_table_[lock_data_id];
    
    // Step 4: 检查事务是否已持有该锁
    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::EXLUCSIVE) {
                return true;  // 已持有X锁
            }
            // 尝试升级为X锁
            // 检查是否只有当前事务持有锁
            if (request_queue.request_queue_.size() == 1) {
                req.lock_mode_ = LockMode::EXLUCSIVE;
                request_queue.group_lock_mode_ = GroupLockMode::X;
                return true;
            } else {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }
    
    // Step 5: 检查锁相容性
    // X锁与任何非空锁都不相容
    if (request_queue.group_lock_mode_ != GroupLockMode::NON_LOCK) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // Step 6: 加入锁请求
    LockRequest lock_request(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // Step 7: 更新组锁模式
    request_queue.group_lock_mode_ = GroupLockMode::X;
    
    txn->get_lock_set()->insert(lock_data_id);
    
    return true;
}

/**
 * @description: 申请表级意向读锁（IS锁）
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的文件描述符
 * 
 * IS锁的相容性：
 * - 与IS、IX、S、SIX相容
 * - 只与X不相容
 * 
 * 意向锁说明：
 * - 在申请行级锁之前，必须先申请对应的表级意向锁
 * - IS锁表示事务打算在表的某些行上加S锁
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    // Step 1: 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // Step 2: 构造表级锁的唯一标识
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    
    // Step 3: 加锁保护lock_table_
    std::unique_lock<std::mutex> lock(latch_);
    
    auto& request_queue = lock_table_[lock_data_id];
    
    // Step 4: 检查事务是否已持有该锁
    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            // 已持有任何锁（IS、IX、S、X、SIX）都满足IS锁的需求
            return true;
        }
    }
    
    // Step 5: 检查锁相容性
    // IS锁只与X不相容
    if (request_queue.group_lock_mode_ == GroupLockMode::X) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // Step 6: 加入锁请求
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // Step 7: 更新组锁模式
    // 只有当前为NON_LOCK时才更新为IS
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
        request_queue.group_lock_mode_ = GroupLockMode::IS;
    }
    
    txn->get_lock_set()->insert(lock_data_id);
    
    return true;
}

/**
 * @description: 申请表级意向写锁（IX锁）
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的文件描述符
 * 
 * IX锁的相容性：
 * - 与IS、IX相容
 * - 与S、X、SIX不相容
 * 
 * 意向锁说明：
 * - IX锁表示事务打算在表的某些行上加X锁
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    // Step 1: 检查事务状态
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // Step 2: 构造表级锁的唯一标识
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    
    // Step 3: 加锁保护lock_table_
    std::unique_lock<std::mutex> lock(latch_);
    
    auto& request_queue = lock_table_[lock_data_id];
    
    // Step 4: 检查事务是否已持有该锁
    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            // 已持有IX、X、SIX锁满足IX需求
            if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                req.lock_mode_ == LockMode::EXLUCSIVE ||
                req.lock_mode_ == LockMode::S_IX) {
                return true;
            }
            // 持有IS锁，尝试升级为IX锁
            if (req.lock_mode_ == LockMode::INTENTION_SHARED) {
                // 检查相容性
                if (request_queue.group_lock_mode_ == GroupLockMode::S ||
                    request_queue.group_lock_mode_ == GroupLockMode::X ||
                    request_queue.group_lock_mode_ == GroupLockMode::SIX) {
                    // 检查是否只有当前事务
                    bool only_current_txn = true;
                    for (auto& r : request_queue.request_queue_) {
                        if (r.txn_id_ != txn->get_transaction_id() && r.granted_) {
                            only_current_txn = false;
                            break;
                        }
                    }
                    if (!only_current_txn) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
                req.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
                // 重新计算组锁模式
                if (request_queue.group_lock_mode_ == GroupLockMode::IS ||
                    request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
                    request_queue.group_lock_mode_ = GroupLockMode::IX;
                }
                return true;
            }
            // 持有S锁，升级为SIX锁
            if (req.lock_mode_ == LockMode::SHARED) {
                // 检查相容性
                if (request_queue.group_lock_mode_ == GroupLockMode::IX ||
                    request_queue.group_lock_mode_ == GroupLockMode::X ||
                    request_queue.group_lock_mode_ == GroupLockMode::SIX) {
                    bool only_current_txn = true;
                    for (auto& r : request_queue.request_queue_) {
                        if (r.txn_id_ != txn->get_transaction_id() && r.granted_) {
                            only_current_txn = false;
                            break;
                        }
                    }
                    if (!only_current_txn) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
                req.lock_mode_ = LockMode::S_IX;
                request_queue.group_lock_mode_ = GroupLockMode::SIX;
                return true;
            }
        }
    }
    
    // Step 5: 检查锁相容性
    // IX锁与S、X、SIX不相容
    if (request_queue.group_lock_mode_ == GroupLockMode::S ||
        request_queue.group_lock_mode_ == GroupLockMode::X ||
        request_queue.group_lock_mode_ == GroupLockMode::SIX) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    
    // Step 6: 加入锁请求
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    lock_request.granted_ = true;
    request_queue.request_queue_.push_back(lock_request);
    
    // Step 7: 更新组锁模式
    // IX比IS强，所以如果当前是NON_LOCK或IS，更新为IX
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
        request_queue.group_lock_mode_ == GroupLockMode::IS) {
        request_queue.group_lock_mode_ = GroupLockMode::IX;
    }
    
    txn->get_lock_set()->insert(lock_data_id);
    
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 * 
 * 实现逻辑：
 * 1. 在锁表中找到对应的锁请求队列
 * 2. 从队列中移除该事务的锁请求
 * 3. 重新计算队列的组锁模式
 * 4. 设置事务状态为SHRINKING（进入收缩阶段）
 * 
 * 两阶段封锁协议：
 * - 一旦释放锁，事务进入SHRINKING阶段
 * - SHRINKING阶段不能再申请新锁
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    // Step 1: 加锁保护lock_table_
    std::unique_lock<std::mutex> lock(latch_);
    
    // Step 2: 检查锁表中是否存在该锁
    if (lock_table_.find(lock_data_id) == lock_table_.end()) {
        return false;  // 锁不存在
    }
    
    auto& request_queue = lock_table_[lock_data_id];
    
    // Step 3: 在队列中查找并移除该事务的锁请求
    bool found = false;
    for (auto it = request_queue.request_queue_.begin(); 
         it != request_queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            request_queue.request_queue_.erase(it);
            found = true;
            break;
        }
    }
    
    if (!found) {
        return false;  // 事务没有持有该锁
    }
    
    // Step 4: 重新计算队列的组锁模式
    // 遍历剩余的锁请求，确定最强的锁类型
    request_queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
    for (auto& req : request_queue.request_queue_) {
        if (!req.granted_) continue;
        
        switch (req.lock_mode_) {
            case LockMode::EXLUCSIVE:
                // X是最强的锁
                request_queue.group_lock_mode_ = GroupLockMode::X;
                break;
            case LockMode::S_IX:
                // SIX仅次于X
                if (request_queue.group_lock_mode_ != GroupLockMode::X) {
                    request_queue.group_lock_mode_ = GroupLockMode::SIX;
                }
                break;
            case LockMode::SHARED:
                // S锁
                if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
                    request_queue.group_lock_mode_ == GroupLockMode::IS) {
                    request_queue.group_lock_mode_ = GroupLockMode::S;
                } else if (request_queue.group_lock_mode_ == GroupLockMode::IX) {
                    request_queue.group_lock_mode_ = GroupLockMode::SIX;
                }
                break;
            case LockMode::INTENTION_EXCLUSIVE:
                // IX锁
                if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
                    request_queue.group_lock_mode_ == GroupLockMode::IS) {
                    request_queue.group_lock_mode_ = GroupLockMode::IX;
                } else if (request_queue.group_lock_mode_ == GroupLockMode::S) {
                    request_queue.group_lock_mode_ = GroupLockMode::SIX;
                }
                break;
            case LockMode::INTENTION_SHARED:
                // IS是最弱的锁
                if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
                    request_queue.group_lock_mode_ = GroupLockMode::IS;
                }
                break;
        }
    }
    
    // Step 5: 设置事务状态为SHRINKING
    // 表示事务进入两阶段封锁的收缩阶段
    txn->set_state(TransactionState::SHRINKING);
    
    return true;
}