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
 * @class NestedLoopJoinExecutor
 * @brief 嵌套循环连接算子，用于实现两表连接操作
 * 
 * 该算子实现了最基础的嵌套循环连接（Nested Loop Join）算法：
 * - 外层循环遍历左表（left_）的每一条记录
 * - 内层循环遍历右表（right_）的每一条记录
 * - 对于每对记录，检查是否满足连接条件
 * - 满足条件的记录对被连接成一条新记录输出
 * 
 * 算子树结构示例：
 * SELECT * FROM A, B, C 生成的算子树：
 *           P (Projection)
 *           |
 *          L2  (LoopJoin)
 *         /  \
 *        A    L1
 *            /  \
 *           B    C  (C table scan)
 * 
 * 注意：在本系统中，连接算子作为右子树递归嵌套
 * 
 * 工作流程：
 * 1. beginTuple(): 初始化两表的扫描，找到第一对满足条件的记录
 * 2. Next(): 返回当前满足条件的连接结果
 * 3. nextTuple(): 找到下一对满足条件的记录
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左子算子（外层循环）
    std::unique_ptr<AbstractExecutor> right_;   // 右子算子（内层循环）
    size_t len_;                                // 连接后记录的总长度
    std::vector<ColMeta> cols_;                 // 连接后记录的字段元数据

    std::vector<Condition> fed_conds_;          // 连接条件
    bool isend;                                 // 是否已经遍历完所有记录对

   public:
    /**
     * @brief 构造函数
     * @param left 左子算子（外层循环遍历的表）
     * @param right 右子算子（内层循环遍历的表）
     * @param conds 连接条件（如 A.id = B.id）
     * 
     * 构造时完成以下工作：
     * 1. 计算连接后记录的总长度（左表长度 + 右表长度）
     * 2. 合并左右两表的列元数据，右表列的偏移量需要加上左表长度
     */
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        // 连接后记录长度 = 左表记录长度 + 右表记录长度
        len_ = left_->tupleLen() + right_->tupleLen();
        
        // 合并列元数据：先复制左表的所有列
        cols_ = left_->cols();
        
        // 再添加右表的列，需要调整偏移量
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            // 右表列的偏移量需要加上左表的长度
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        
        isend = false;
        fed_conds_ = std::move(conds);
    }

    /**
     * @brief 获取连接后记录的长度
     * @return 每条连接记录的字节长度
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取连接后的列元数据
     * @return 合并后的字段元数据向量
     */
    const std::vector<ColMeta> &cols() const override { return cols_; }

    /**
     * @brief 获取算子类型名称
     * @return 算子类型字符串
     */
    std::string getType() override { return "NestedLoopJoinExecutor"; }

    /**
     * @brief 判断连接是否结束
     * @return true表示已遍历完所有可能的记录对
     */
    bool is_end() const override { return isend; }

    /**
     * @brief 初始化连接，找到第一对满足条件的记录
     * 
     * 嵌套循环算法：
     * 1. 初始化左表扫描，定位到第一条记录
     * 2. 对于左表的每一条记录，扫描整个右表
     * 3. 检查每对记录是否满足连接条件
     * 4. 找到第一对满足条件的记录后返回
     */
    void beginTuple() override {
        // 初始化左表扫描
        left_->beginTuple();
        
        // 如果左表为空，直接结束
        if (left_->is_end()) {
            isend = true;
            return;
        }
        
        // 初始化右表扫描
        right_->beginTuple();
        
        // 如果右表为空，直接结束
        if (right_->is_end()) {
            isend = true;
            return;
        }
        
        // 查找第一对满足条件的记录
        // 如果当前组合不满足条件，调用nextTuple()继续查找
        auto left_rec = left_->Next();
        auto right_rec = right_->Next();
        if (!check_join_conditions(left_rec.get(), right_rec.get())) {
            nextTuple();
        }
    }

    /**
     * @brief 移动到下一对满足条件的记录
     * 
     * 嵌套循环遍历策略：
     * 1. 右表前进一步
     * 2. 如果右表到达末尾，左表前进一步，右表重新开始
     * 3. 如果左表也到达末尾，设置isend=true
     * 4. 检查当前记录对是否满足条件，不满足则递归查找
     */
    void nextTuple() override {
        // 右表向前移动
        right_->nextTuple();
        
        // 如果右表扫描完毕，左表向前移动，右表重新开始
        while (right_->is_end()) {
            left_->nextTuple();
            
            // 如果左表也扫描完毕，连接结束
            if (left_->is_end()) {
                isend = true;
                return;
            }
            
            // 右表重新从头开始扫描
            right_->beginTuple();
            
            // 如果右表为空（不应该发生，但做防御性检查）
            if (right_->is_end()) {
                isend = true;
                return;
            }
        }
        
        // 检查当前记录对是否满足连接条件
        auto left_rec = left_->Next();
        auto right_rec = right_->Next();
        if (!check_join_conditions(left_rec.get(), right_rec.get())) {
            // 不满足条件，继续查找下一对
            nextTuple();
        }
    }

    /**
     * @brief 返回当前满足条件的连接结果
     * @return 连接后的记录（左表记录 + 右表记录）
     * 
     * 将左表记录和右表记录合并成一条新记录：
     * - 新记录前半部分是左表记录
     * - 新记录后半部分是右表记录
     */
    std::unique_ptr<RmRecord> Next() override {
        // 获取左右两表的当前记录
        auto left_rec = left_->Next();
        auto right_rec = right_->Next();
        
        // 创建连接后的记录
        auto join_rec = std::make_unique<RmRecord>(len_);
        
        // 复制左表记录到前半部分
        memcpy(join_rec->data, left_rec->data, left_->tupleLen());
        
        // 复制右表记录到后半部分
        memcpy(join_rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
        
        return join_rec;
    }

    /**
     * @brief 获取当前记录的位置
     * @return Rid引用（连接算子不管理单独的Rid）
     */
    Rid &rid() override { return _abstract_rid; }

   private:
    /**
     * @brief 检查左右记录是否满足所有连接条件
     * @param left_rec 左表记录
     * @param right_rec 右表记录
     * @return true表示满足所有条件，false表示不满足
     * 
     * 连接条件的特点：通常比较两个不同表的列
     * 例如：A.id = B.aid
     */
    bool check_join_conditions(RmRecord *left_rec, RmRecord *right_rec) {
        for (auto &cond : fed_conds_) {
            if (!check_join_condition(left_rec, right_rec, cond)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 检查单个连接条件
     * @param left_rec 左表记录
     * @param right_rec 右表记录
     * @param cond 连接条件
     * @return true表示满足条件，false表示不满足
     * 
     * 需要处理两种情况：
     * 1. 条件两边都是列（如 A.id = B.aid）
     * 2. 条件一边是列，一边是常量值（如 A.id = 1）
     */
    bool check_join_condition(RmRecord *left_rec, RmRecord *right_rec, const Condition &cond) {
        // 获取左边列的数据
        char *lhs_data = nullptr;
        ColType lhs_type;
        int lhs_len;
        
        // 查找左边列属于哪个表
        auto &left_cols = left_->cols();
        auto &right_cols = right_->cols();
        
        // 尝试在左表中查找左边的列
        auto lhs_col_it = std::find_if(left_cols.begin(), left_cols.end(),
            [&](const ColMeta &col) {
                return col.tab_name == cond.lhs_col.tab_name && col.name == cond.lhs_col.col_name;
            });
        
        if (lhs_col_it != left_cols.end()) {
            // 左边列在左表中
            lhs_data = left_rec->data + lhs_col_it->offset;
            lhs_type = lhs_col_it->type;
            lhs_len = lhs_col_it->len;
        } else {
            // 左边列在右表中（需要减去左表长度得到原始偏移量）
            auto it = std::find_if(right_cols.begin(), right_cols.end(),
                [&](const ColMeta &col) {
                    return col.tab_name == cond.lhs_col.tab_name && col.name == cond.lhs_col.col_name;
                });
            if (it == right_cols.end()) {
                throw ColumnNotFoundError(cond.lhs_col.tab_name + "." + cond.lhs_col.col_name);
            }
            // 注意：right_cols中的offset已经加上了left_tupleLen，所以需要减去
            lhs_data = right_rec->data + (it->offset - left_->tupleLen());
            lhs_type = it->type;
            lhs_len = it->len;
        }
        
        // 获取右边的数据
        char *rhs_data = nullptr;
        ColType rhs_type;
        int rhs_len;
        
        if (cond.is_rhs_val) {
            // 右边是常量值
            rhs_type = cond.rhs_val.type;
            rhs_len = lhs_len;
            if (rhs_type == TYPE_INT) {
                rhs_data = (char *)&cond.rhs_val.int_val;
            } else if (rhs_type == TYPE_FLOAT) {
                rhs_data = (char *)&cond.rhs_val.float_val;
            } else {
                rhs_data = const_cast<char *>(cond.rhs_val.str_val.c_str());
            }
        } else {
            // 右边是列，查找它属于哪个表
            auto rhs_col_it = std::find_if(left_cols.begin(), left_cols.end(),
                [&](const ColMeta &col) {
                    return col.tab_name == cond.rhs_col.tab_name && col.name == cond.rhs_col.col_name;
                });
            
            if (rhs_col_it != left_cols.end()) {
                // 右边列在左表中
                rhs_data = left_rec->data + rhs_col_it->offset;
                rhs_type = rhs_col_it->type;
                rhs_len = rhs_col_it->len;
            } else {
                // 右边列在右表中
                auto it = std::find_if(right_cols.begin(), right_cols.end(),
                    [&](const ColMeta &col) {
                        return col.tab_name == cond.rhs_col.tab_name && col.name == cond.rhs_col.col_name;
                    });
                if (it == right_cols.end()) {
                    throw ColumnNotFoundError(cond.rhs_col.tab_name + "." + cond.rhs_col.col_name);
                }
                rhs_data = right_rec->data + (it->offset - left_->tupleLen());
                rhs_type = it->type;
                rhs_len = it->len;
            }
        }
        
        // 执行比较
        return compare_values(lhs_data, rhs_data, lhs_type, lhs_len, cond.op);
    }

    /**
     * @brief 比较两个值
     * @param lhs 左值地址
     * @param rhs 右值地址
     * @param type 值类型
     * @param len 值长度（用于字符串）
     * @param op 比较操作符
     * @return 比较结果
     */
    bool compare_values(char *lhs, char *rhs, ColType type, int len, CompOp op) {
        int cmp_result;
        
        switch (type) {
            case TYPE_INT: {
                int lhs_val = *(int *)lhs;
                int rhs_val = *(int *)rhs;
                cmp_result = (lhs_val < rhs_val) ? -1 : ((lhs_val > rhs_val) ? 1 : 0);
                break;
            }
            case TYPE_FLOAT: {
                float lhs_val = *(float *)lhs;
                float rhs_val = *(float *)rhs;
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
        
        switch (op) {
            case OP_EQ: return cmp_result == 0;
            case OP_NE: return cmp_result != 0;
            case OP_LT: return cmp_result < 0;
            case OP_GT: return cmp_result > 0;
            case OP_LE: return cmp_result <= 0;
            case OP_GE: return cmp_result >= 0;
            default:
                throw InternalError("Unsupported comparison operator");
        }
    }
};