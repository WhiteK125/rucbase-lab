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
 * @class UpdateExecutor
 * @brief 更新算子，用于执行UPDATE语句
 * 
 * 该算子实现了UPDATE table SET col=val,... WHERE condition语句的执行。
 * 更新操作需要完成以下工作：
 * 1. 修改记录文件中的数据
 * 2. 如果被修改的列上有索引，需要更新索引
 * 
 * 索引更新策略：
 * - 先删除旧的索引项
 * - 再插入新的索引项
 * 
 * 注意：rids_是在执行前由扫描算子提前收集好的
 * 所有满足WHERE条件的记录位置。
 * 
 * 工作流程：
 * 1. 遍历rids_中的每个记录位置
 * 2. 读取原记录并构造新记录
 * 3. 更新涉及的索引（删除旧键，插入新键）
 * 4. 更新记录文件中的数据
 */
class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                       // 表的元数据
    std::vector<Condition> conds_;      // 更新条件（已在扫描阶段使用）
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<Rid> rids_;             // 需要更新的记录的位置列表
    std::string tab_name_;              // 表名称
    std::vector<SetClause> set_clauses_; // SET子句，指定要更新的列和新值
    SmManager *sm_manager_;             // 系统管理器

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器
     * @param tab_name 要更新的表名
     * @param set_clauses SET子句列表（col=val对）
     * @param conds 更新条件（已用于扫描）
     * @param rids 需要更新的记录位置列表
     * @param context 上下文信息
     */
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    /**
     * @brief 执行更新操作
     * @return nullptr（更新操作不返回记录）
     * 
     * 更新流程：
     * 1. 遍历所有需要更新的记录（rids_）
     * 2. 对于每条记录：
     *    a. 读取原记录
     *    b. 根据SET子句构造新记录
     *    c. 更新涉及的索引（删旧键，插新键）
     *    d. 更新记录文件中的数据
     */
    std::unique_ptr<RmRecord> Next() override {
        // 遍历所有需要更新的记录
        for (auto &rid : rids_) {
            // Step 1: 读取原记录
            auto old_rec = fh_->get_record(rid, context_);
            
            // Step 2: 构造新记录（复制原记录，然后修改指定列）
            // 先复制原记录的所有数据
            RmRecord new_rec(old_rec->size);
            memcpy(new_rec.data, old_rec->data, old_rec->size);
            
            // 根据SET子句修改新记录中的相应列
            for (auto &set_clause : set_clauses_) {
                // 找到要更新的列的元数据
                auto col_it = tab_.get_col(set_clause.lhs.col_name);
                
                // 检查类型是否匹配
                if (col_it->type != set_clause.rhs.type) {
                    throw IncompatibleTypeError(coltype2str(col_it->type), coltype2str(set_clause.rhs.type));
                }
                
                // 将新值写入新记录的对应位置
                Value val = set_clause.rhs;
                val.init_raw(col_it->len);
                memcpy(new_rec.data + col_it->offset, val.raw->data, col_it->len);
            }
            
            // Step 3: 更新索引
            // 对于每个索引，需要检查该索引涉及的列是否被更新
            // 如果被更新，需要删除旧的索引项并插入新的索引项
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                
                // 检查该索引涉及的列是否有被更新
                bool index_affected = false;
                for (auto &idx_col : index.cols) {
                    for (auto &set_clause : set_clauses_) {
                        if (idx_col.name == set_clause.lhs.col_name) {
                            index_affected = true;
                            break;
                        }
                    }
                    if (index_affected) break;
                }
                
                // 如果索引涉及的列被更新，需要更新索引
                if (index_affected) {
                    // 获取索引句柄
                    auto ih = sm_manager_->ihs_.at(
                        sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)
                    ).get();
                    
                    // 构造旧的索引键
                    char *old_key = new char[index.col_tot_len];
                    int offset = 0;
                    for (size_t j = 0; j < index.col_num; ++j) {
                        memcpy(old_key + offset, old_rec->data + index.cols[j].offset, index.cols[j].len);
                        offset += index.cols[j].len;
                    }
                    
                    // 构造新的索引键
                    char *new_key = new char[index.col_tot_len];
                    offset = 0;
                    for (size_t j = 0; j < index.col_num; ++j) {
                        memcpy(new_key + offset, new_rec.data + index.cols[j].offset, index.cols[j].len);
                        offset += index.cols[j].len;
                    }
                    
                    // 删除旧的索引项
                    ih->delete_entry(old_key, context_->txn_);
                    
                    // 插入新的索引项
                    ih->insert_entry(new_key, rid, context_->txn_);
                    
                    // 释放键值缓冲区
                    delete[] old_key;
                    delete[] new_key;
                }
            }
            
            // Step 4: 更新记录文件中的数据
            fh_->update_record(rid, new_rec.data, context_);
        }
        
        return nullptr;
    }

    /**
     * @brief 获取当前记录的位置
     * @return Rid引用（更新算子不维护当前位置）
     */
    Rid &rid() override { return _abstract_rid; }
};