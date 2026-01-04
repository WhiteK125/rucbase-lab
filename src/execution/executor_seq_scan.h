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
 * @class SeqScanExecutor
 * @brief 顺序扫描算子，用于全表扫描
 *
 * 该算子实现了火山模型中的顺序扫描操作，通过遍历表中的所有记录，
 * 找出满足条件的元组。每次调用Next()返回一条满足条件的记录。
 *
 * 工作流程：
 * 1. beginTuple(): 初始化扫描，定位到第一条满足条件的记录
 * 2. Next(): 返回当前记录
 * 3. nextTuple(): 移动到下一条满足条件的记录
 * 4. is_end(): 判断扫描是否结束
 */
class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件（WHERE子句中的条件）
    RmFileHandle* fh_;                  // 表的数据文件句柄，用于读取记录
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段元数据
    size_t len_;                        // scan后生成的每条记录的长度（字节数）
    std::vector<Condition> fed_conds_;  // 同conds_，用于条件过滤

    Rid rid_;                        // 当前扫描到的记录的位置（页号+槽号）
    std::unique_ptr<RecScan> scan_;  // 表迭代器，用于遍历表中的记录

    SmManager* sm_manager_;  // 系统管理器，用于访问表和索引

   public:
    /**
     * @brief 构造函数
     * @param sm_manager 系统管理器
     * @param tab_name 要扫描的表名
     * @param conds 扫描条件（WHERE子句）
     * @param context 上下文信息
     */
    SeqScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta& tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        // 计算记录长度：最后一个字段的偏移量 + 最后一个字段的长度
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    /**
     * @brief 获取扫描输出的记录长度
     * @return 每条记录的字节长度
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取输出记录的字段元数据
     * @return 字段元数据向量的引用
     */
    const std::vector<ColMeta>& cols() const override { return cols_; }

    /**
     * @brief 获取算子类型名称
     * @return 算子类型字符串
     */
    std::string getType() override { return "SeqScanExecutor"; }

    /**
     * @brief 判断扫描是否结束
     * @return true表示已扫描完所有记录，false表示还有记录
     */
    bool is_end() const override { return scan_->is_end(); }

    /**
     * @brief 初始化扫描，定位到第一条满足条件的记录
     *
     * 创建表迭代器RmScan，然后不断调用next()直到找到第一条
     * 满足所有条件的记录，或者扫描结束。
     * 
     * 加锁说明：
     * - 在扫描表之前，需要先申请表级意向共享锁（IS锁）
     * - IS锁表示事务打算在表的某些行上加S锁
     * - 行级S锁在get_record时自动申请
     */
    void beginTuple() override {
        // Step 1: 申请表级意向共享锁（IS锁）
        // IS锁表示事务将要在该表的某些记录上申请共享锁
        // 这是多粒度锁协议的要求：申请行级锁前必须先申请表级意向锁
        if (context_ != nullptr && context_->lock_mgr_ != nullptr && context_->txn_ != nullptr) {
            context_->lock_mgr_->lock_IS_on_table(context_->txn_, fh_->GetFd());
        }
        
        // Step 2: 创建表扫描迭代器，RmScan会自动定位到第一条记录
        scan_ = std::make_unique<RmScan>(fh_);

        // Step 3: 找到第一条满足条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();  // 获取当前记录的位置
            // 读取当前记录并检查是否满足条件
            auto rec = fh_->get_record(rid_, context_);
            if (check_conditions(rec.get())) {
                // 找到满足条件的记录，返回
                return;
            }
            // 当前记录不满足条件，继续扫描下一条
            scan_->next();
        }
    }

    /**
     * @brief 移动到下一条满足条件的记录
     *
     * 从当前位置开始，不断调用scan_->next()直到找到下一条
     * 满足所有条件的记录，或者扫描结束。
     */
    void nextTuple() override {
        // 先移动到下一条记录
        scan_->next();

        // 继续扫描直到找到满足条件的记录或扫描结束
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (check_conditions(rec.get())) {
                return;
            }
            scan_->next();
        }
    }

    /**
     * @brief 返回当前满足条件的记录
     * @return 当前记录的智能指针
     *
     * 该函数假设调用前已经通过beginTuple()或nextTuple()
     * 定位到了一条满足条件的记录。
     */
    std::unique_ptr<RmRecord> Next() override { return fh_->get_record(rid_, context_); }

    /**
     * @brief 获取当前记录的位置
     * @return 当前记录的Rid引用
     */
    Rid& rid() override { return rid_; }

   private:
    /**
     * @brief 检查记录是否满足所有条件
     * @param rec 要检查的记录
     * @return true表示满足所有条件，false表示不满足
     *
     * 遍历fed_conds_中的所有条件，对每个条件调用check_condition()
     * 只有当所有条件都满足时才返回true（条件之间是AND关系）
     */
    bool check_conditions(RmRecord* rec) {
        for (auto& cond : fed_conds_) {
            if (!check_condition(rec, cond)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 检查记录是否满足单个条件
     * @param rec 要检查的记录
     * @param cond 要检查的条件
     * @return true表示满足条件，false表示不满足
     *
     * 条件格式：lhs_col op rhs_col 或 lhs_col op rhs_val
     * 支持的比较操作：=, <>, <, >, <=, >=
     */
    bool check_condition(RmRecord* rec, const Condition& cond) {
        // 获取左边列的元数据
        auto lhs_col = get_col(cols_, cond.lhs_col);
        // 获取左边列在记录中的值
        char* lhs_data = rec->data + lhs_col->offset;

        // 根据条件右边是值还是列，获取右边的数据
        char* rhs_data;
        ColType rhs_type;
        int rhs_len;

        if (cond.is_rhs_val) {
            // 右边是一个常量值
            rhs_type = cond.rhs_val.type;
            rhs_len = lhs_col->len;
            // 需要将Value转换为raw格式进行比较
            // 这里直接根据类型获取值的地址
            if (rhs_type == TYPE_INT) {
                rhs_data = (char*)&cond.rhs_val.int_val;
            } else if (rhs_type == TYPE_FLOAT) {
                rhs_data = (char*)&cond.rhs_val.float_val;
            } else {
                rhs_data = const_cast<char*>(cond.rhs_val.str_val.c_str());
            }
        } else {
            // 右边是另一个列（用于连接条件）
            auto rhs_col = get_col(cols_, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs_len = rhs_col->len;
            rhs_data = rec->data + rhs_col->offset;
        }

        // 执行比较操作
        return compare_values(lhs_data, rhs_data, lhs_col->type, lhs_col->len, cond.op);
    }

    /**
     * @brief 比较两个值
     * @param lhs 左边值的地址
     * @param rhs 右边值的地址
     * @param type 值的类型
     * @param len 值的长度（用于字符串比较）
     * @param op 比较操作符
     * @return 比较结果
     *
     * 根据值的类型（INT/FLOAT/STRING）执行相应的比较操作
     */
    bool compare_values(char* lhs, char* rhs, ColType type, int len, CompOp op) {
        int cmp_result;

        switch (type) {
            case TYPE_INT: {
                int lhs_val = *(int*)lhs;
                int rhs_val = *(int*)rhs;
                cmp_result = (lhs_val < rhs_val) ? -1 : ((lhs_val > rhs_val) ? 1 : 0);
                break;
            }
            case TYPE_FLOAT: {
                float lhs_val = *(float*)lhs;
                float rhs_val = *(float*)rhs;
                cmp_result = (lhs_val < rhs_val) ? -1 : ((lhs_val > rhs_val) ? 1 : 0);
                break;
            }
            case TYPE_STRING: {
                cmp_result = memcmp(lhs, rhs, len);
                break;
            }
            default:
                throw InternalError("Unsupported column type");
        }

        // 根据比较操作符返回结果
        switch (op) {
            case OP_EQ:
                return cmp_result == 0;  // 等于
            case OP_NE:
                return cmp_result != 0;  // 不等于
            case OP_LT:
                return cmp_result < 0;  // 小于
            case OP_GT:
                return cmp_result > 0;  // 大于
            case OP_LE:
                return cmp_result <= 0;  // 小于等于
            case OP_GE:
                return cmp_result >= 0;  // 大于等于
            default:
                throw InternalError("Unsupported comparison operator");
        }
    }
};