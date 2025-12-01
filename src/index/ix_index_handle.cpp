/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"

#include "ix_scan.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 * 
 * 实现思路：使用二分查找在有序的key数组中查找第一个>=target的key位置
 * - 二分查找的左边界为0，右边界为num_key
 * - 每次比较中间位置的key与target，若key<target，则向右半部分搜索
 * - 否则向左半部分搜索（包括等于的情况，因为要找第一个>=的位置）
 * - 使用ix_compare()函数比较两个key，该函数支持多种类型(int/float/string)
 */
int IxNodeHandle::lower_bound(const char *target) const {
    // 获取当前节点的key数量
    int num_key = page_hdr->num_key;
    
    // 使用二分查找找到第一个>=target的key位置
    // 左闭右开区间 [left, right)
    int left = 0, right = num_key;
    while (left < right) {
        int mid = left + (right - left) / 2;
        // 获取mid位置的key，并与target比较
        // ix_compare返回值: <0表示a<b, =0表示a==b, >0表示a>b
        int cmp = ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp < 0) {
            // key[mid] < target，需要在右半部分继续查找
            left = mid + 1;
        } else {
            // key[mid] >= target，需要在左半部分继续查找（可能还有更小的>=target的位置）
            right = mid;
        }
    }
    // left即为第一个>=target的key位置
    // 如果所有key都<target，则left=num_key
    return left;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 * 
 * 实现思路：使用二分查找在有序的key数组中查找第一个>target的key位置
 * - 二分查找的左边界为1（注意从1开始，因为内部节点的第0个key有特殊用途）
 * - 右边界为num_key
 * - 每次比较中间位置的key与target，若key<=target，则向右半部分搜索
 * - 否则向左半部分搜索
 * - 使用ix_compare()函数比较两个key
 */
int IxNodeHandle::upper_bound(const char *target) const {
    // 获取当前节点的key数量
    int num_key = page_hdr->num_key;
    
    // 使用二分查找找到第一个>target的key位置
    // 注意：文档指出范围从1开始，这是因为在内部节点中，第0个位置的key存储的是子树中的最小key
    // 在查找孩子节点时，需要从位置1开始搜索
    int left = 1, right = num_key;
    while (left < right) {
        int mid = left + (right - left) / 2;
        // 获取mid位置的key，并与target比较
        int cmp = ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp <= 0) {
            // key[mid] <= target，需要在右半部分继续查找
            left = mid + 1;
        } else {
            // key[mid] > target，需要在左半部分继续查找（可能还有更小的>target的位置）
            right = mid;
        }
    }
    // left即为第一个>target的key位置
    // 如果所有key都<=target，则left=num_key
    return left;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 * 
 * 实现思路：
 * 1. 使用lower_bound()找到第一个>=key的位置
 * 2. 检查该位置是否有效（不超出范围）且该位置的key是否等于目标key
 * 3. 如果匹配，将对应的Rid指针赋值给传出参数value
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    // 1. 在叶子节点中获取目标key所在位置
    // lower_bound返回第一个>=key的位置
    int pos = lower_bound(key);
    
    // 2. 判断目标key是否存在
    // 条件1: pos必须在有效范围内 [0, num_key)
    // 条件2: pos位置的key必须等于目标key（lower_bound找到的是>=的位置，可能是>）
    if (pos < page_hdr->num_key) {
        // 比较pos位置的key与目标key是否相等
        int cmp = ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp == 0) {
            // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
            *value = get_rid(pos);
            return true;
        }
    }
    
    // key不存在
    return false;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 * 
 * 实现思路：
 * - B+树内部节点的结构：key[0], key[1], ..., key[n-1] 和 value[0], value[1], ..., value[n-1]
 * - value[i]指向的孩子节点包含的key满足：key[i] <= child_keys < key[i+1]
 * - 特别地，value[0]指向的孩子包含所有 < key[1] 的key
 * 
 * 查找逻辑：
 * 1. 使用upper_bound找到第一个 > target 的key位置pos
 * 2. 则target应该在位置pos-1对应的孩子中（因为key[pos-1] <= target < key[pos]）
 * 
 * 边界情况：
 * - 如果upper_bound返回1，说明target < key[1]，应返回value[0]
 * - 如果upper_bound返回num_key，说明target >= key[num_key-1]，应返回value[num_key-1]
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // upper_bound返回第一个>key的位置（范围[1, num_key]）
    int pos = upper_bound(key);
    
    // 2. 获取该孩子节点（子树）所在页面的编号
    // target应该在位置pos-1对应的孩子中
    // 因为 key[pos-1] <= target < key[pos]（或target >= key[pos-1]当pos=num_key时）
    int child_idx = pos - 1;
    
    // 3. 返回页面编号
    // get_rid返回Rid结构，其page_no字段存储了孩子节点的页面编号
    return get_rid(child_idx)->page_no;
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 * 
 * 实现思路：
 * 1. 验证插入位置pos的合法性（0 <= pos <= num_key）
 * 2. 将原有的[pos, num_key)位置的数据向后移动n个位置，腾出空间
 * 3. 将新的n个键值对复制到pos位置
 * 4. 更新节点的键值对数量
 * 
 * 内存布局：
 * - keys数组：每个key占用col_tot_len_字节
 * - rids数组：每个rid占用sizeof(Rid)字节
 * - keys和rids是分开存储的，需要分别处理
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    // 1. 判断pos的合法性
    // pos应该在[0, num_key]范围内
    assert(pos >= 0 && pos <= page_hdr->num_key);
    
    // 获取需要移动的元素数量
    int num_to_move = page_hdr->num_key - pos;
    
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 首先将[pos, num_key)的keys向后移动n个位置
    if (num_to_move > 0) {
        // 使用memmove处理可能的内存重叠情况
        // 源地址：get_key(pos)，目标地址：get_key(pos + n)
        memmove(get_key(pos + n), get_key(pos), num_to_move * file_hdr->col_tot_len_);
    }
    // 将n个新key复制到pos位置
    memcpy(get_key(pos), key, n * file_hdr->col_tot_len_);
    
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 首先将[pos, num_key)的rids向后移动n个位置
    if (num_to_move > 0) {
        // 源地址：get_rid(pos)，目标地址：get_rid(pos + n)
        memmove(get_rid(pos + n), get_rid(pos), num_to_move * sizeof(Rid));
    }
    // 将n个新rid复制到pos位置
    memcpy(get_rid(pos), rid, n * sizeof(Rid));
    
    // 4. 更新当前节点的键数量
    page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 * 
 * 实现思路：
 * 1. 使用lower_bound找到第一个>=key的位置
 * 2. 检查该位置的key是否与待插入的key相同（B+树不支持重复键）
 * 3. 如果不重复，使用insert_pairs在该位置插入键值对
 * 4. 返回插入后的键值对数量
 * 
 * 注意：B+树的key数组是有序的，插入后仍需保持有序性
 */
int IxNodeHandle::insert(const char *key, const Rid &value) {
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // lower_bound返回第一个>=key的位置，这就是我们应该插入的位置
    int pos = lower_bound(key);
    
    // 2. 如果key重复则不插入
    // 检查pos位置的key是否与待插入的key相同
    if (pos < page_hdr->num_key) {
        int cmp = ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp == 0) {
            // key已存在，不插入，直接返回当前键值对数量
            return page_hdr->num_key;
        }
    }
    
    // 3. 如果key不重复则插入键值对
    // 使用insert_pairs插入单个键值对
    insert_pairs(pos, key, &value, 1);
    
    // 4. 返回完成插入操作之后的键值对数量
    return page_hdr->num_key;
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 * 
 * 实现思路：
 * 1. 将[pos+1, num_key)位置的数据向前移动一位，覆盖pos位置的数据
 * 2. 更新节点的键值对数量（减1）
 * 
 * 注意：需要分别处理keys数组和rids数组
 */
void IxNodeHandle::erase_pair(int pos) {
    // 验证pos的合法性
    assert(pos >= 0 && pos < page_hdr->num_key);
    
    // 计算需要移动的元素数量
    int num_to_move = page_hdr->num_key - pos - 1;
    
    // 1. 删除该位置的key
    // 将[pos+1, num_key)的keys向前移动一位
    if (num_to_move > 0) {
        memmove(get_key(pos), get_key(pos + 1), num_to_move * file_hdr->col_tot_len_);
    }
    
    // 2. 删除该位置的rid
    // 将[pos+1, num_key)的rids向前移动一位
    if (num_to_move > 0) {
        memmove(get_rid(pos), get_rid(pos + 1), num_to_move * sizeof(Rid));
    }
    
    // 3. 更新结点的键值对数量
    page_hdr->num_key--;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 * 
 * 实现思路：
 * 1. 使用lower_bound找到第一个>=key的位置
 * 2. 检查该位置的key是否等于目标key
 * 3. 如果存在，调用erase_pair删除该位置的键值对
 * 4. 返回删除后的键值对数量
 */
int IxNodeHandle::remove(const char *key) {
    // 1. 查找要删除键值对的位置
    int pos = lower_bound(key);
    
    // 2. 如果要删除的键值对存在，删除键值对
    // 首先检查pos是否在有效范围内，以及该位置的key是否等于目标key
    if (pos < page_hdr->num_key) {
        int cmp = ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp == 0) {
            // key存在，删除该键值对
            erase_pair(pos);
        }
    }
    
    // 3. 返回完成删除操作后的键值对数量
    return page_hdr->num_key;
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    char* buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    
    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 * 
 * 实现思路：
 * 1. 从根节点开始，沿着B+树向下遍历
 * 2. 对于每个内部节点，使用internal_lookup找到包含目标key的孩子节点
 * 3. 重复步骤2直到到达叶子节点
 * 4. 返回叶子节点
 * 
 * 注意事项：
 * - 遍历过程中需要unpin已经访问过的内部节点（避免缓冲池溢出）
 * - 返回的叶子节点需要由调用者负责unpin
 */
std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                            Transaction *transaction, bool find_first) {
    // 1. 获取根节点
    // file_hdr_->root_page_存储了根节点的页面编号
    IxNodeHandle *node = fetch_node(file_hdr_->root_page_);
    
    // 2. 从根节点开始不断向下查找目标key
    // 循环直到找到叶子节点
    while (!node->is_leaf_page()) {
        // 当前节点是内部节点，使用internal_lookup找到目标key所在的孩子节点
        page_id_t child_page_no = node->internal_lookup(key);
        
        // 获取孩子节点
        IxNodeHandle *child = fetch_node(child_page_no);
        
        // 释放当前内部节点（unpin），因为已经不需要了
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        
        // 释放IxNodeHandle对象的内存
        delete node;
        
        // 移动到孩子节点继续查找
        node = child;
    }
    
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点
    // 注意：叶子节点的unpin由调用者负责
    // 第二个返回值root_is_latched在基础版本中为false（粗粒度并发时可能会用到）
    return std::make_pair(node, false);
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 * 
 * 实现思路：
 * 1. 使用find_leaf_page找到包含目标key的叶子节点
 * 2. 在叶子节点中使用leaf_lookup查找目标key对应的Rid
 * 3. 如果找到，将Rid添加到result容器中
 * 4. 释放叶子节点的page（unpin）
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // 并发控制：获取锁（粗粒度，整棵树加锁）
    std::scoped_lock lock{root_latch_};
    
    // 1. 获取目标key值所在的叶子结点
    auto [leaf_node, root_is_latched] = find_leaf_page(key, Operation::FIND, transaction);
    
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    Rid *rid = nullptr;
    bool found = leaf_node->leaf_lookup(key, &rid);
    
    // 3. 如果找到，把rid存入result参数中
    if (found && rid != nullptr) {
        result->push_back(*rid);
    }
    
    // 4. 使用完buffer_pool提供的page之后，记得unpin page
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    
    // 释放IxNodeHandle对象的内存
    delete leaf_node;
    
    return found;
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 * 
 * 实现思路：
 * 1. 创建一个新节点作为原节点的右兄弟
 * 2. 计算分裂点：将原节点的键值对平均分配
 *    - 左半部分保留在原节点
 *    - 右半部分移动到新节点
 * 3. 初始化新节点的page_hdr
 * 4. 如果是叶子节点，更新叶子链表（prev_leaf和next_leaf指针）
 * 5. 如果是内部节点，更新移动到新节点的孩子的父指针
 * 
 * 分裂策略：
 * - 分裂点为 split_pos = num_key / 2
 * - 左半部分 [0, split_pos) 保留在原节点
 * - 右半部分 [split_pos, num_key) 移动到新节点
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    // 1. 创建新的右兄弟结点
    IxNodeHandle *new_node = create_node();
    
    // 计算分裂点，将键值对平均分配
    int split_pos = node->get_size() / 2;
    int num_to_move = node->get_size() - split_pos;
    
    // 初始化新节点的page_hdr内容
    new_node->page_hdr->is_leaf = node->is_leaf_page();
    new_node->page_hdr->parent = node->get_parent_page_no();
    new_node->page_hdr->num_key = 0;  // 暂时设为0，insert_pairs会更新
    new_node->page_hdr->next_free_page_no = IX_NO_PAGE;
    
    // 2. 将右半部分的键值对移动到新节点
    // 使用insert_pairs将[split_pos, num_key)的键值对插入到新节点
    new_node->insert_pairs(0, node->get_key(split_pos), node->get_rid(split_pos), num_to_move);
    
    // 更新原节点的键值对数量（删除右半部分）
    node->page_hdr->num_key = split_pos;
    
    // 3. 如果新的右兄弟结点是叶子结点，更新叶子链表指针
    if (new_node->is_leaf_page()) {
        // 获取原节点的下一个叶子节点
        page_id_t next_leaf_no = node->get_next_leaf();
        
        // 设置新节点的prev和next指针
        new_node->set_prev_leaf(node->get_page_no());
        new_node->set_next_leaf(next_leaf_no);
        
        // 更新原节点的next指针指向新节点
        node->set_next_leaf(new_node->get_page_no());
        
        // 更新原来的下一个叶子节点的prev指针指向新节点
        // 注意：即使next_leaf_no是LEAF_HEADER_PAGE，也需要更新它的prev指针
        IxNodeHandle *next_node = fetch_node(next_leaf_no);
        next_node->set_prev_leaf(new_node->get_page_no());
        buffer_pool_manager_->unpin_page(next_node->get_page_id(), true);
        delete next_node;
        
        // 如果原节点是最后一个叶子节点，更新file_hdr的last_leaf
        if (file_hdr_->last_leaf_ == node->get_page_no()) {
            file_hdr_->last_leaf_ = new_node->get_page_no();
        }
    } else {
        // 4. 如果新的右兄弟结点不是叶子结点，更新其所有孩子结点的父节点信息
        for (int i = 0; i < new_node->get_size(); i++) {
            maintain_child(new_node, i);
        }
    }
    
    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 * 
 * 实现思路：
 * 1. 检查old_node是否为根节点
 *    - 如果是根节点，需要创建新的根节点
 *    - 新根的两个孩子分别是old_node和new_node
 * 2. 如果old_node不是根节点，获取其父节点
 * 3. 将(key, new_node的page_no)插入到父节点
 * 4. 如果父节点插入后满了，递归分裂父节点
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    // 1. 分裂前的结点（原结点, old_node）是否为根结点
    if (old_node->is_root_page()) {
        // 如果为根结点，需要分配新的root
        IxNodeHandle *new_root = create_node();
        
        // 初始化新根节点
        new_root->page_hdr->is_leaf = false;  // 根节点是内部节点
        new_root->page_hdr->parent = IX_NO_PAGE;  // 根节点没有父节点
        new_root->page_hdr->num_key = 0;
        new_root->page_hdr->next_free_page_no = IX_NO_PAGE;
        
        // 插入第一个键值对：old_node的第一个key和指向old_node的指针
        Rid old_rid = {.page_no = old_node->get_page_no(), .slot_no = 0};
        new_root->insert_pairs(0, old_node->get_key(0), &old_rid, 1);
        
        // 插入第二个键值对：new_node的第一个key和指向new_node的指针
        Rid new_rid = {.page_no = new_node->get_page_no(), .slot_no = 0};
        new_root->insert_pairs(1, key, &new_rid, 1);
        
        // 更新old_node和new_node的父节点指针
        old_node->set_parent_page_no(new_root->get_page_no());
        new_node->set_parent_page_no(new_root->get_page_no());
        
        // 更新根节点页面编号
        file_hdr_->root_page_ = new_root->get_page_no();
        
        // unpin新根节点
        buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
        delete new_root;
        
        return;
    }
    
    // 2. 获取原结点（old_node）的父亲结点
    IxNodeHandle *parent = fetch_node(old_node->get_parent_page_no());
    
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 找到old_node在父节点中的位置
    int old_idx = parent->find_child(old_node);
    
    // 在old_idx + 1位置插入(key, new_node的page_no)
    Rid new_rid = {.page_no = new_node->get_page_no(), .slot_no = 0};
    parent->insert_pairs(old_idx + 1, key, &new_rid, 1);
    
    // 更新new_node的父节点指针
    new_node->set_parent_page_no(parent->get_page_no());
    
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 判断条件：parent->get_size() == parent->get_max_size()
    if (parent->get_size() == parent->get_max_size()) {
        // 需要分裂父节点
        IxNodeHandle *new_parent = split(parent);
        
        // 递归向上插入
        // 注意：要插入的key是new_parent的第一个key
        insert_into_parent(parent, new_parent->get_key(0), new_parent, transaction);
        
        // unpin新分裂出的父节点
        buffer_pool_manager_->unpin_page(new_parent->get_page_id(), true);
        delete new_parent;
    }
    
    // unpin父节点
    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    delete parent;
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 * 
 * 实现思路：
 * 1. 使用find_leaf_page找到应该插入键值对的叶子节点
 * 2. 在叶子节点中插入键值对
 * 3. 检查叶子节点是否已满（size == max_size）
 *    - 如果已满，调用split分裂节点
 *    - 调用insert_into_parent将新节点信息插入父节点
 * 4. 更新相关信息（如last_leaf）
 * 5. 释放节点资源（unpin）
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // 并发控制：获取锁（粗粒度，整棵树加锁）
    std::scoped_lock lock{root_latch_};
    
    // 1. 查找key值应该插入到哪个叶子节点
    auto [leaf_node, root_is_latched] = find_leaf_page(key, Operation::INSERT, transaction);
    
    // 记录叶子节点的page_no作为返回值
    page_id_t leaf_page_no = leaf_node->get_page_no();
    
    // 获取插入前的size，用于判断是否插入成功（key是否重复）
    int old_size = leaf_node->get_size();
    
    // 2. 在该叶子节点中插入键值对
    int new_size = leaf_node->insert(key, value);
    
    // 检查是否插入成功（如果size没变，说明key重复，没有插入）
    if (new_size == old_size) {
        // key重复，插入失败
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        delete leaf_node;
        return leaf_page_no;
    }
    
    // 更新叶子节点第一个key的信息到父节点（如果需要）
    // 只有当插入的key成为了该节点的最小key时才需要更新
    maintain_parent(leaf_node);
    
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    if (leaf_node->get_size() == leaf_node->get_max_size()) {
        // 节点已满，需要分裂
        IxNodeHandle *new_leaf = split(leaf_node);
        
        // 将新节点的第一个key插入到父节点
        insert_into_parent(leaf_node, new_leaf->get_key(0), new_leaf, transaction);
        
        // unpin新叶子节点
        buffer_pool_manager_->unpin_page(new_leaf->get_page_id(), true);
        delete new_leaf;
    }
    
    // unpin叶子节点
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    delete leaf_node;
    
    return leaf_page_no;
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 * 
 * 实现思路：
 * 1. 使用find_leaf_page找到包含该key的叶子节点
 * 2. 在叶子节点中删除该key的键值对
 * 3. 检查删除是否成功
 * 4. 如果删除后叶子节点的size < min_size，调用coalesce_or_redistribute处理
 * 5. 更新父节点中的key信息（如果删除的是第一个key）
 * 
 * @return bool 是否删除成功
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    // 并发控制：获取锁（粗粒度，整棵树加锁）
    std::scoped_lock lock{root_latch_};
    
    // 1. 获取该键值对所在的叶子结点
    auto [leaf_node, root_is_latched] = find_leaf_page(key, Operation::DELETE, transaction);
    
    // 获取删除前的size
    int old_size = leaf_node->get_size();
    
    // 2. 在该叶子结点中删除键值对
    int new_size = leaf_node->remove(key);
    
    // 检查是否删除成功
    if (new_size == old_size) {
        // key不存在，删除失败
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        delete leaf_node;
        return false;
    }
    
    // 更新父节点中的key信息（如果删除的是第一个key且节点非空）
    if (new_size > 0) {
        maintain_parent(leaf_node);
    }
    
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作
    // 检查是否需要合并或重分配
    bool leaf_deleted = coalesce_or_redistribute(leaf_node, transaction, nullptr);
    
    // 如果叶子节点没有被删除，需要unpin
    if (!leaf_deleted) {
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    }
    delete leaf_node;
    
    return true;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点（指传入的node是否被删除）
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 * 
 * 实现思路：
 * 1. 如果node是根节点，使用adjust_root处理
 * 2. 如果node的size >= min_size，不需要处理
 * 3. 否则，找到node的兄弟节点（优先选择前驱）
 * 4. 根据node和兄弟的size之和决定重分配还是合并
 *    - 如果能支撑两个节点，则重分配
 *    - 否则合并
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
    // 1. 判断node结点是否为根节点
    if (node->is_root_page()) {
        // 1.1 如果是根节点，需要调用AdjustRoot()函数来进行处理
        return adjust_root(node);
    }
    
    // 1.2 如果不是根节点，检查是否需要执行合并或重分配操作
    // 根据B+树的性质，如果节点的size >= min_size，则不需要处理
    if (node->get_size() >= node->get_min_size()) {
        return false;  // 不需要删除任何节点
    }
    
    // 2. 获取node结点的父亲结点
    IxNodeHandle *parent = fetch_node(node->get_parent_page_no());
    
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 首先找到node在parent中的位置
    int node_idx = parent->find_child(node);
    
    // 选择兄弟节点：优先选择前驱（左兄弟），如果没有则选择后继（右兄弟）
    int neighbor_idx;
    if (node_idx > 0) {
        // 有前驱结点，选择前驱
        neighbor_idx = node_idx - 1;
    } else {
        // 没有前驱结点，选择后继
        neighbor_idx = node_idx + 1;
    }
    
    // 获取兄弟节点
    IxNodeHandle *neighbor_node = fetch_node(parent->value_at(neighbor_idx));
    
    // 4. 判断是重分配还是合并
    // 如果node.size + neighbor.size >= 2 * min_size，则重分配
    // 否则合并
    bool node_deleted = false;
    
    if (node->get_size() + neighbor_node->get_size() >= node->get_min_size() * 2) {
        // 4.1 重新分配键值对
        redistribute(neighbor_node, node, parent, node_idx);
        node_deleted = false;
        
        // unpin兄弟节点和父节点
        buffer_pool_manager_->unpin_page(neighbor_node->get_page_id(), true);
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        delete neighbor_node;
        delete parent;
    } else {
        // 5. 合并两个结点
        // 需要判断node在左还是在右
        // 合并后，右边的节点会被删除
        
        // node_idx == 0 意味着node在左边，neighbor在右边
        // node_idx > 0 意味着neighbor在左边，node在右边（因为我们选择了前驱）
        
        if (node_idx == 0) {
            // node在左，neighbor在右，neighbor将被删除
            // 将neighbor的内容合并到node
            int node_size = node->get_size();
            int neighbor_size = neighbor_node->get_size();
            
            // 将neighbor的所有键值对追加到node的末尾
            node->insert_pairs(node_size, neighbor_node->get_key(0), neighbor_node->get_rid(0), neighbor_size);
            
            // 如果是内部节点，更新被移动孩子的父节点信息
            if (!neighbor_node->is_leaf_page()) {
                for (int i = 0; i < neighbor_size; i++) {
                    maintain_child(node, node_size + i);
                }
            }
            
            // 如果是叶子结点，需要更新叶子链表
            if (neighbor_node->is_leaf_page()) {
                erase_leaf(neighbor_node);
                if (file_hdr_->last_leaf_ == neighbor_node->get_page_no()) {
                    file_hdr_->last_leaf_ = node->get_page_no();
                }
            }
            
            // 释放neighbor节点
            release_node_handle(*neighbor_node);
            buffer_pool_manager_->unpin_page(neighbor_node->get_page_id(), true);
            delete neighbor_node;
            
            // 删除parent中neighbor的信息（neighbor在parent中的位置是neighbor_idx = 1）
            parent->erase_pair(neighbor_idx);
            
            // node没有被删除
            node_deleted = false;
        } else {
            // neighbor在左，node在右，node将被删除
            // 将node的内容合并到neighbor
            int neighbor_size = neighbor_node->get_size();
            int node_size = node->get_size();
            
            // 将node的所有键值对追加到neighbor的末尾
            neighbor_node->insert_pairs(neighbor_size, node->get_key(0), node->get_rid(0), node_size);
            
            // 如果是内部节点，更新被移动孩子的父节点信息
            if (!node->is_leaf_page()) {
                for (int i = 0; i < node_size; i++) {
                    maintain_child(neighbor_node, neighbor_size + i);
                }
            }
            
            // 如果是叶子结点，需要更新叶子链表
            if (node->is_leaf_page()) {
                erase_leaf(node);
                if (file_hdr_->last_leaf_ == node->get_page_no()) {
                    file_hdr_->last_leaf_ = neighbor_node->get_page_no();
                }
            }
            
            // 释放node节点
            release_node_handle(*node);
            
            // 删除parent中node的信息
            parent->erase_pair(node_idx);
            
            // unpin neighbor节点
            buffer_pool_manager_->unpin_page(neighbor_node->get_page_id(), true);
            delete neighbor_node;
            
            // node被删除
            node_deleted = true;
        }
        
        // 递归检查父节点
        coalesce_or_redistribute(parent, transaction, root_is_latched);
        
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        delete parent;
    }
    
    return node_deleted;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 * 
 * 实现思路：
 * 1. 如果old_root_node是内部结点，并且大小为1，则它只有一个孩子
 *    - 将这个唯一的孩子提升为新的根节点
 *    - 删除旧的根节点
 * 2. 如果old_root_node是叶结点，且大小为0，则B+树为空
 *    - 将root_page设置为IX_NO_PAGE
 * 3. 其他情况不需要处理（根节点可以少于min_size个键值对）
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1) {
        // 获取唯一的孩子节点
        page_id_t child_page_no = old_root_node->value_at(0);
        IxNodeHandle *child = fetch_node(child_page_no);
        
        // 将孩子节点设置为新的根节点
        child->set_parent_page_no(IX_NO_PAGE);
        
        // 更新文件头中的根节点页号
        file_hdr_->root_page_ = child_page_no;
        
        // unpin孩子节点
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        delete child;
        
        // 释放旧根节点
        release_node_handle(*old_root_node);
        
        return true;  // 根节点需要被删除
    }
    
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0) {
        // B+树变为空树，但根据测试要求，一般不会删到完全为空
        // 这里保留处理逻辑
        file_hdr_->root_page_ = IX_NO_PAGE;
        
        // 释放旧根节点
        release_node_handle(*old_root_node);
        
        return true;  // 根节点需要被删除
    }
    
    // 3. 除了上述两种情况，不需要进行操作
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 * 
 * 实现思路：
 * 1. 通过index判断neighbor_node与node的位置关系
 * 2. 根据位置关系，从neighbor移动一个键值对到node
 *    - 如果neighbor在右边(index=0)：将neighbor的第一个键值对移到node的末尾
 *    - 如果neighbor在左边(index>0)：将neighbor的最后一个键值对移到node的开头
 * 3. 更新父节点中对应的key
 * 4. 如果是内部节点，更新被移动孩子的父指针
 */
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    if (index == 0) {
        // neighbor在node的右边（neighbor是后继结点）
        // node(left)      neighbor(right)
        // 将neighbor的第一个键值对移动到node的末尾
        
        // 获取neighbor的第一个键值对
        char *move_key = neighbor_node->get_key(0);
        Rid *move_rid = neighbor_node->get_rid(0);
        
        // 插入到node的末尾
        node->insert_pairs(node->get_size(), move_key, move_rid, 1);
        
        // 从neighbor中删除第一个键值对
        neighbor_node->erase_pair(0);
        
        // 更新父节点中neighbor对应的key（父节点中指向neighbor的key应更新为neighbor的新第一个key）
        // neighbor在parent中的位置是index+1
        memcpy(parent->get_key(index + 1), neighbor_node->get_key(0), file_hdr_->col_tot_len_);
        
        // 如果是内部节点，更新被移动孩子的父指针
        if (!node->is_leaf_page()) {
            maintain_child(node, node->get_size() - 1);
        }
    } else {
        // neighbor在node的左边（neighbor是前驱结点）
        // neighbor(left)  node(right)
        // 将neighbor的最后一个键值对移动到node的开头
        
        // 获取neighbor的最后一个键值对
        int last_idx = neighbor_node->get_size() - 1;
        char *move_key = neighbor_node->get_key(last_idx);
        Rid *move_rid = neighbor_node->get_rid(last_idx);
        
        // 插入到node的开头
        node->insert_pairs(0, move_key, move_rid, 1);
        
        // 从neighbor中删除最后一个键值对
        neighbor_node->erase_pair(last_idx);
        
        // 更新父节点中node对应的key（父节点中指向node的key应更新为node的新第一个key）
        // node在parent中的位置是index
        memcpy(parent->get_key(index), node->get_key(0), file_hdr_->col_tot_len_);
        
        // 如果是内部节点，更新被移动孩子的父指针
        if (!node->is_leaf_page()) {
            maintain_child(node, 0);
        }
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 * 
 * 实现思路：
 * 1. 确保neighbor_node在左，node在右（如果不是，交换它们）
 * 2. 将node的所有键值对移动到neighbor_node
 * 3. 如果是内部节点，更新被移动孩子的父指针
 * 4. 如果是叶子节点，更新叶子链表
 * 5. 释放node节点
 * 6. 从parent中删除指向node的键值对
 * 7. 递归检查parent是否需要合并或重分配
 */
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched) {
    // 1. 用index判断neighbor_node是否为node的前驱结点
    // 如果index=0，说明node在左边，需要交换
    if (index == 0) {
        // 交换node和neighbor_node，保证neighbor_node在左边，node在右边
        std::swap(*neighbor_node, *node);
        // 交换后，被删除的node（原neighbor）在右边，其在parent中的index为1
        index = 1;
    }
    
    // 2. 把node结点的键值对移动到neighbor_node中
    int neighbor_size = (*neighbor_node)->get_size();
    int node_size = (*node)->get_size();
    
    // 将node的所有键值对追加到neighbor_node的末尾
    (*neighbor_node)->insert_pairs(neighbor_size, (*node)->get_key(0), (*node)->get_rid(0), node_size);
    
    // 如果是内部节点，更新被移动孩子的父节点信息
    if (!(*node)->is_leaf_page()) {
        for (int i = 0; i < node_size; i++) {
            maintain_child(*neighbor_node, neighbor_size + i);
        }
    }
    
    // 如果是叶子结点，需要更新叶子链表
    if ((*node)->is_leaf_page()) {
        // 更新叶子链表：neighbor的next指向node的next
        erase_leaf(*node);
        
        // 如果node是最右叶子结点，需要更新file_hdr_.last_leaf
        if (file_hdr_->last_leaf_ == (*node)->get_page_no()) {
            file_hdr_->last_leaf_ = (*neighbor_node)->get_page_no();
        }
    }
    
    // 3. 释放和删除node结点
    release_node_handle(**node);
    
    // 删除parent中node结点的信息（删除index位置的键值对）
    (*parent)->erase_pair(index);
    
    // 返回parent是否需要被删除
    // 递归检查父节点：如果父节点删除后size < min_size，需要继续处理
    return coalesce_or_redistribute(*parent, transaction, root_is_latched);
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return *node->get_rid(iid.slot_no);
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 * 
 * 实现思路：
 * 1. 使用find_leaf_page找到可能包含该key的叶子节点
 * 2. 在叶子节点中使用lower_bound找到第一个>=key的位置
 * 3. 如果该位置等于节点大小，说明key比该叶子节点中的所有key都大，需要到下一个叶子节点
 * 4. 返回找到的位置(page_no, slot_no)
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    // 1. 找到可能包含该key的叶子节点
    auto [leaf_node, root_is_latched] = find_leaf_page(key, Operation::FIND, nullptr);
    
    // 2. 在叶子节点中使用lower_bound找到第一个>=key的位置
    int pos = leaf_node->lower_bound(key);
    
    // 3. 检查是否需要到下一个叶子节点
    while (pos == leaf_node->get_size()) {
        // 当前叶子节点中没有>=key的位置，需要到下一个叶子节点
        page_id_t next_leaf = leaf_node->get_next_leaf();
        
        // 如果没有下一个叶子节点，返回leaf_end()
        if (next_leaf == IX_LEAF_HEADER_PAGE) {
            Iid iid = {.page_no = leaf_node->get_page_no(), .slot_no = pos};
            buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
            delete leaf_node;
            return iid;
        }
        
        // 移动到下一个叶子节点
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        delete leaf_node;
        leaf_node = fetch_node(next_leaf);
        pos = 0;  // 从下一个叶子节点的第一个位置开始
    }
    
    // 4. 返回找到的位置
    Iid iid = {.page_no = leaf_node->get_page_no(), .slot_no = pos};
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    delete leaf_node;
    
    return iid;
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 * 
 * 实现思路：
 * 1. 使用find_leaf_page找到可能包含该key的叶子节点
 * 2. 在叶子节点中找到第一个>key的位置
 * 3. 如果该位置等于节点大小，说明key大于等于该叶子节点中的所有key，需要到下一个叶子节点
 * 4. 返回找到的位置(page_no, slot_no)
 * 
 * 注意：对于叶子节点，我们需要找的是第一个>key的位置，
 * 所以使用lower_bound找到>=key的位置后，如果等于key，则继续向后找
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    // 1. 找到可能包含该key的叶子节点
    auto [leaf_node, root_is_latched] = find_leaf_page(key, Operation::FIND, nullptr);
    
    // 2. 在叶子节点中找到第一个>key的位置
    // 首先用lower_bound找到第一个>=key的位置
    int pos = leaf_node->lower_bound(key);
    
    // 如果找到了>=key的位置，检查是否等于key
    while (pos < leaf_node->get_size()) {
        int cmp = ix_compare(leaf_node->get_key(pos), key, file_hdr_->col_types_, file_hdr_->col_lens_);
        if (cmp > 0) {
            // 找到了第一个>key的位置
            break;
        }
        // cmp == 0，继续向后找
        pos++;
    }
    
    // 3. 检查是否需要到下一个叶子节点
    while (pos == leaf_node->get_size()) {
        // 当前叶子节点中没有>key的位置，需要到下一个叶子节点
        page_id_t next_leaf = leaf_node->get_next_leaf();
        
        // 如果没有下一个叶子节点，返回leaf_end()
        if (next_leaf == IX_LEAF_HEADER_PAGE) {
            Iid iid = {.page_no = leaf_node->get_page_no(), .slot_no = pos};
            buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
            delete leaf_node;
            return iid;
        }
        
        // 移动到下一个叶子节点
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        delete leaf_node;
        leaf_node = fetch_node(next_leaf);
        pos = 0;  // 从下一个叶子节点的第一个位置开始
    }
    
    // 4. 返回找到的位置
    Iid iid = {.page_no = leaf_node->get_page_no(), .slot_no = pos};
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    delete leaf_node;
    
    return iid;
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::create_node() {
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_, page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);  // 修改了parent node
        curr = parent;

        assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    file_hdr_->num_pages_--;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    }
}
