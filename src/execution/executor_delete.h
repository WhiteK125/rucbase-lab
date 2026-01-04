/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @class DeleteExecutor
 * @brief 删除算子，用于执行DELETE语句
 *
 * 该算子实现了DELETE FROM table WHERE condition语句的执行。
 * 删除操作需要完成以下工作：
 * 1. 删除记录文件中的数据
 * 2. 删除所有相关索引中的索引项
 *
 * 注意：rids_是在执行前由扫描算子（SeqScan或IndexScan）
 * 提前收集好的所有满足WHERE条件的记录位置。
 *
 * 工作流程：
 * 1. 申请表级意向排他锁（IX锁）
 * 2. 遍历rids_中的每个记录位置
 * 3. 对于每条记录，先记录写操作（用于回滚）
 * 4. 删除其在所有索引中的索引项
 * 5. 然后删除记录文件中的数据
 */
class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据，包含列信息和索引信息
    std::vector<Condition> conds_;  // delete的条件（已在扫描阶段使用）
    RmFileHandle* fh_;              // 表的数据文件句柄，用于删除记录
    std::vector<Rid> rids_;         // 需要删除的记录的位置列表（由扫描算子提供）
    std::string tab_name_;          // 表名称
    SmManager* sm_manager_;         // 系统管理器，用于访问索引句柄

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器
     * @param tab_name 要删除记录的表名
     * @param conds 删除条件（已用于扫描，此处仅保存）
     * @param rids 需要删除的记录位置列表
     * @param context 上下文信息
     *
     * 注意：rids是在执行DELETE语句前，通过扫描算子
     * 根据WHERE条件收集好的所有需要删除的记录位置。
     */
    DeleteExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    /**
     * @brief 执行删除操作
     * @return nullptr（删除操作不返回记录）
     *
     * 删除流程：
     * 1. 申请表级意向排他锁（IX锁）
     * 2. 遍历所有需要删除的记录（rids_）
     * 3. 对于每条记录：
     *    a. 先读取记录内容（用于回滚和删除索引项）
     *    b. 记录写操作（用于事务回滚）
     *    c. 删除该记录在所有索引中的索引项
     *    d. 删除记录文件中的数据
     *
     * 重要：
     * - 必须先读取记录再删除，因为删除后数据就不存在了
     * - 必须记录写操作以支持事务回滚
     */
    std::unique_ptr<RmRecord> Next() override {
        // Step 1: 申请表级意向排他锁（IX锁）
        // IX锁表示事务将要在该表的某些记录上申请排他锁
        if (context_ != nullptr && context_->lock_mgr_ != nullptr && context_->txn_ != nullptr) {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
        }
        
        // Step 2: 遍历所有需要删除的记录
        for (auto& rid : rids_) {
            // Step 2.1: 读取记录内容（用于构造索引键和回滚）
            // 必须在删除记录之前读取，因为删除后数据就不存在了
            auto rec = fh_->get_record(rid, context_);

            // Step 2.2: 记录写操作（用于事务回滚）
            // DELETE操作需要记录rid和原始数据，回滚时将数据插入回原位置
            if (context_ != nullptr && context_->txn_ != nullptr) {
                WriteRecord* write_record = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *rec);
                context_->txn_->append_write_record(write_record);
            }

            // Step 2.3: 删除该记录在所有索引中的索引项
            // 遍历表上的所有索引
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];

                // 获取索引句柄
                // 索引文件名格式：表名_列名1_列名2_..._列名n.idx
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();

                // 构造索引键
                // 索引键由多个列的值连接而成
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) {
                    // 从记录中提取该索引列的值
                    memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }

                // 从B+树索引中删除该键值对
                ih->delete_entry(key, context_->txn_);

                // 释放键值缓冲区
                delete[] key;
            }

            // Step 2.4: 删除记录文件中的数据
            // 使用rid定位并删除记录
            fh_->delete_record(rid, context_);
        }

        return nullptr;
    }

    /**
     * @brief 获取当前记录的位置
     * @return Rid引用（删除算子不维护当前位置）
     */
    Rid& rid() override { return _abstract_rid; }
};