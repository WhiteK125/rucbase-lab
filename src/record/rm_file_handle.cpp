/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context 上下文信息，包含事务和锁管理器
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 * 
 * 实现逻辑：
 * 1. 申请行级共享锁（S锁）以保证读取一致性
 * 2. 获取指定记录所在的page handle
 * 3. 初始化一个指向RmRecord的指针
 * 
 * 加锁说明：
 * - 读操作需要申请S锁，防止其他事务同时修改该记录
 * - 在申请行级锁之前，需要先申请表级意向锁（IS锁）
 * - 意向锁的申请在executor层完成
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Step 1: 申请行级共享锁（如果有事务上下文）
    // 读取记录需要S锁，防止脏读（读取到其他事务未提交的数据）
    if (context != nullptr && context->lock_mgr_ != nullptr && context->txn_ != nullptr) {
        context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
    }
    
    // Step 2: 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // Step 3: 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        // 记录不存在，释放页面并抛出异常
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    // Step 4: 初始化RmRecord并复制数据
    auto rec = std::make_unique<RmRecord>(file_hdr_.record_size);
    char* src = page_handle.get_slot(rid.slot_no);
    memcpy(rec->data, src, file_hdr_.record_size);
    
    // Step 5: 释放页面
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return rec;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    (void)context;
    RmPageHandle page_handle = create_page_handle();
    // 找到空闲slot
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    if (slot_no >= file_hdr_.num_records_per_page) {
        // 不应发生：create_page_handle保证有空闲slot
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        return Rid{-1, -1};
    }
    // 写入数据
    memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records += 1;
    // 如果页满了，更新空闲页链
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // 从空闲链表移除该页
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
    }
    BufferPoolManager::mark_dirty(page_handle.page);
    PageId pid = page_handle.page->get_page_id();
    buffer_pool_manager_->unpin_page(pid, true);
    return Rid{pid.page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 * 
 * 该方法主要用于事务回滚时恢复被删除的记录到原位置
 * 
 * 实现逻辑：
 * 1. 获取指定页面的page handle
 * 2. 检查目标slot是否为空（正常情况应该为空）
 * 3. 将数据复制到指定slot位置
 * 4. 更新bitmap和记录计数
 * 
 * 注意事项：
 * - 如果页面原本已满，需要考虑空闲页链表的更新
 * - 该方法假设rid是有效的且该位置当前为空
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // Step 1: 获取指定页面的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // Step 2: 将数据复制到指定slot位置
    char* slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // Step 3: 更新bitmap，标记该slot已被占用
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    
    // Step 4: 更新页面的记录计数
    // 如果页面原本是满的（不在空闲链表中），需要考虑处理
    // 但在回滚场景下，被删除的记录会使页面变为非满状态
    // 所以这里只需要简单增加计数即可
    page_handle.page_hdr->num_records += 1;
    
    // 标记页面为脏页，需要写回磁盘
    BufferPoolManager::mark_dirty(page_handle.page);
    
    // 释放页面
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context 上下文信息，包含事务和锁管理器
 * 
 * 实现逻辑：
 * 1. 申请行级排他锁（X锁）以保证删除操作的独占性
 * 2. 获取指定记录所在的page handle
 * 3. 更新page_handle.page_hdr中的数据结构
 * 
 * 加锁说明：
 * - 删除操作需要申请X锁，防止其他事务同时读取或修改该记录
 * - X锁与其他所有锁都不相容
 * - 在申请行级锁之前，需要先申请表级意向锁（IX锁）
 * - 意向锁的申请在executor层完成
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Step 1: 申请行级排他锁（如果有事务上下文）
    // 删除记录需要X锁，防止其他事务读取到即将被删除的数据
    if (context != nullptr && context->lock_mgr_ != nullptr && context->txn_ != nullptr) {
        context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    }
    
    // Step 2: 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // Step 3: 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    // Step 4: 检查页面原本是否已满
    bool was_full = (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);
    
    // Step 5: 更新bitmap，标记该slot为空闲
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    
    // Step 6: 更新记录计数
    page_handle.page_hdr->num_records -= 1;
    
    // Step 7: 如果页面原本已满，现在有空闲空间，需要加入空闲页链表
    if (was_full) {
        release_page_handle(page_handle);
    }
    
    // Step 8: 标记页面为脏页并释放
    BufferPoolManager::mark_dirty(page_handle.page);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context 上下文信息，包含事务和锁管理器
 * 
 * 实现逻辑：
 * 1. 申请行级排他锁（X锁）以保证更新操作的独占性
 * 2. 获取指定记录所在的page handle
 * 3. 用新数据覆盖原记录
 * 
 * 加锁说明：
 * - 更新操作需要申请X锁，防止其他事务同时读取或修改该记录
 * - X锁与其他所有锁都不相容
 * - 在申请行级锁之前，需要先申请表级意向锁（IX锁）
 * - 意向锁的申请在executor层完成
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Step 1: 申请行级排他锁（如果有事务上下文）
    // 更新记录需要X锁，防止其他事务读取到不一致的数据
    if (context != nullptr && context->lock_mgr_ != nullptr && context->txn_ != nullptr) {
        context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    }
    
    // Step 2: 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // Step 3: 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    // Step 4: 用新数据覆盖原记录
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    
    // Step 5: 标记页面为脏页并释放
    BufferPoolManager::mark_dirty(page_handle.page);
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if (page_no < RM_FIRST_RECORD_PAGE || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    }
    Page* page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    PageId new_id{.fd = fd_, .page_no = INVALID_PAGE_ID};
    Page* page = buffer_pool_manager_->new_page(&new_id);
    // 初始化页头与bitmap
    RmPageHandle ph(&file_hdr_, page);
    ph.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    ph.page_hdr->num_records = 0;
    Bitmap::init(ph.bitmap, file_hdr_.bitmap_size);
    // 更新文件头
    file_hdr_.first_free_page_no = new_id.page_no;
    file_hdr_.num_pages += 1;
    BufferPoolManager::mark_dirty(page);
    return ph;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        return create_new_page_handle();
    }
    // 获取第一个空闲页
    int page_no = file_hdr_.first_free_page_no;
    Page* page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    int page_no = page_handle.page->get_page_id().page_no;
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_no;
}