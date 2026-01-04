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

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器
     * @param tab_name 要插入记录的表名
     * @param values 要插入的值列表
     * @param context 上下文信息
     */
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    /**
     * @brief 执行插入操作
     * @return nullptr（插入操作不返回记录）
     * 
     * 实现逻辑：
     * 1. 申请表级意向排他锁（IX锁）
     * 2. 构造记录数据
     * 3. 插入记录文件
     * 4. 记录写操作（用于回滚）
     * 5. 更新相关索引
     * 
     * 加锁说明：
     * - 插入操作需要申请IX锁，表示将要在某些行上加X锁
     * - 行级X锁在insert_record时隐式获得（新插入的记录）
     */
    std::unique_ptr<RmRecord> Next() override {
        // Step 1: 申请表级意向排他锁（IX锁）
        // IX锁表示事务将要在该表的某些记录上申请排他锁
        if (context_ != nullptr && context_->lock_mgr_ != nullptr && context_->txn_ != nullptr) {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
        }
        
        // Step 2: 构造记录缓冲区
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            // 检查类型是否匹配
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            // 初始化值的raw格式
            val.init_raw(col.len);
            // 将值复制到记录缓冲区的对应位置
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        
        // Step 3: 插入记录文件
        rid_ = fh_->insert_record(rec.data, context_);
        
        // Step 4: 记录写操作（用于事务回滚）
        // INSERT操作只需要记录rid，回滚时直接删除该记录即可
        if (context_ != nullptr && context_->txn_ != nullptr) {
            WriteRecord* write_record = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
            context_->txn_->append_write_record(write_record);
        }
        
        // Step 5: 更新相关索引
        // 遍历表上的所有索引，将新记录的索引项插入B+树
        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            // 获取索引句柄
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            // 构造索引键
            char* key = new char[index.col_tot_len];
            int offset = 0;
            for(size_t j = 0; j < index.col_num; ++j) {
                memcpy(key + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            // 插入索引项
            ih->insert_entry(key, rid_, context_->txn_);
            delete[] key;
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};