/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle* file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_ = Rid{RM_FIRST_RECORD_PAGE, -1};
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    if (file_handle_ == nullptr) {
        rid_ = Rid{RM_NO_PAGE, RM_NO_PAGE};
        return;
    }
    int page_no = rid_.page_no;
    int slot_no = rid_.slot_no;
    const RmFileHdr& fh = file_handle_->file_hdr_;
    while (page_no >= RM_FIRST_RECORD_PAGE && page_no < fh.num_pages) {
        RmPageHandle ph = file_handle_->fetch_page_handle(page_no);
        int next_slot = Bitmap::next_bit(true, ph.bitmap, fh.num_records_per_page, slot_no);
        // unpin current page immediately after inspection
        file_handle_->buffer_pool_manager_->unpin_page(ph.page->get_page_id(), false);
        if (next_slot < fh.num_records_per_page) {
            rid_.page_no = page_no;
            rid_.slot_no = next_slot;
            return;
        }
        // move to next page
        page_no++;
        slot_no = -1;
    }
    // end reached
    rid_ = Rid{RM_NO_PAGE, RM_NO_PAGE};
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const { return rid_; }