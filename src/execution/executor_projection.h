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
 * @class ProjectionExecutor
 * @brief 投影算子，用于从记录中选择指定的字段
 * 
 * 投影算子是SELECT语句中选择列的实现。例如：
 * SELECT col1, col2 FROM table 中，投影算子负责从完整记录中
 * 提取col1和col2字段，组成新的输出记录。
 * 
 * 在火山模型中，投影算子位于算子树的上层，它从子算子（如扫描算子）
 * 获取完整记录，然后提取需要的字段组成新记录返回。
 * 
 * 工作流程：
 * 1. 构造时确定需要投影的列及其在子算子输出中的位置
 * 2. beginTuple()调用子算子的beginTuple()
 * 3. Next()从子算子获取记录，提取指定列组成新记录
 * 4. nextTuple()调用子算子的nextTuple()
 */
class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的子节点（数据来源）
    std::vector<ColMeta> cols_;                     // 需要投影的字段的元数据
    size_t len_;                                    // 投影后每条记录的总长度
    std::vector<size_t> sel_idxs_;                  // 选择的列在子算子输出中的索引位置

   public:
    /**
     * @brief 构造函数
     * @param prev 子算子（数据来源）
     * @param sel_cols 需要投影的列（表名.列名）
     * 
     * 构造时完成以下工作：
     * 1. 遍历sel_cols，在子算子的输出列中找到对应的位置
     * 2. 计算投影后每列的新偏移量（从0开始连续排列）
     * 3. 记录选择的列索引，用于Next()时提取数据
     */
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();  // 获取子算子输出的所有列
        
        for (auto &sel_col : sel_cols) {
            // 在子算子的输出列中查找目标列
            auto pos = get_col(prev_cols, sel_col);
            // 记录该列在子算子输出中的索引位置
            sel_idxs_.push_back(pos - prev_cols.begin());
            
            // 复制列元数据，并更新偏移量为投影后的新位置
            auto col = *pos;
            col.offset = curr_offset;  // 新的偏移量从0开始连续排列
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;  // 投影后记录的总长度
    }

    /**
     * @brief 获取投影后记录的长度
     * @return 每条投影记录的字节长度
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取投影后的列元数据
     * @return 投影后的字段元数据向量
     */
    const std::vector<ColMeta> &cols() const override { return cols_; }

    /**
     * @brief 获取算子类型名称
     * @return 算子类型字符串
     */
    std::string getType() override { return "ProjectionExecutor"; }

    /**
     * @brief 判断是否扫描结束
     * @return true表示子算子已经没有更多数据
     */
    bool is_end() const override { return prev_->is_end(); }

    /**
     * @brief 初始化，调用子算子的beginTuple()
     */
    void beginTuple() override {
        prev_->beginTuple();
    }

    /**
     * @brief 移动到下一条记录，调用子算子的nextTuple()
     */
    void nextTuple() override {
        prev_->nextTuple();
    }

    /**
     * @brief 返回投影后的记录
     * @return 仅包含选择列的记录
     * 
     * 工作流程：
     * 1. 从子算子获取完整记录
     * 2. 创建新的记录缓冲区（大小为投影后的长度）
     * 3. 根据sel_idxs_从原记录中提取指定列的数据
     * 4. 按新的偏移量组装成投影后的记录
     */
    std::unique_ptr<RmRecord> Next() override {
        // 从子算子获取完整记录
        auto prev_rec = prev_->Next();
        
        // 创建投影后的记录缓冲区
        auto proj_rec = std::make_unique<RmRecord>(len_);
        
        // 获取子算子的列元数据，用于定位原记录中的数据
        auto &prev_cols = prev_->cols();
        
        // 遍历需要投影的每一列
        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            // 获取该列在子算子输出中的索引
            size_t idx = sel_idxs_[i];
            // 获取原列的元数据（包含原偏移量和长度）
            const ColMeta &prev_col = prev_cols[idx];
            // 获取投影后该列的元数据（包含新偏移量）
            const ColMeta &proj_col = cols_[i];
            
            // 从原记录中复制数据到投影记录
            // 源地址：原记录数据 + 原偏移量
            // 目标地址：投影记录数据 + 新偏移量
            memcpy(proj_rec->data + proj_col.offset, 
                   prev_rec->data + prev_col.offset, 
                   prev_col.len);
        }
        
        return proj_rec;
    }

    /**
     * @brief 获取当前记录的位置
     * @return Rid引用（投影算子本身不管理Rid，返回抽象类的默认值）
     */
    Rid &rid() override { return _abstract_rid; }
};