/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta* new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 *
 * 实现思路：
 * 1. 检查数据库目录是否存在，不存在则抛出异常
 * 2. 进入数据库目录
 * 3. 从元数据文件(db.meta)中读取数据库元数据信息
 * 4. 打开所有表的记录文件，将文件句柄存入fhs_哈希表
 * 5. 打开所有索引文件，将索引句柄存入ihs_哈希表
 */
void SmManager::open_db(const std::string& db_name) {
    // Step 1: 检查数据库目录是否存在
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }

    // Step 2: 进入数据库目录
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    // Step 3: 从元数据文件中读取数据库元数据
    // DB_META_NAME是在sm_defs.h中定义的常量，表示元数据文件名"db.meta"
    std::ifstream ifs(DB_META_NAME);
    ifs >> db_;  // 利用重载的>>运算符从文件读取DbMeta对象

    // Step 4: 打开所有表的记录文件
    // db_.tabs_是一个map，存储了所有表的元数据TabMeta
    // 每个表对应一个同名的记录文件
    for (auto& entry : db_.tabs_) {
        const std::string& tab_name = entry.first;
        // rm_manager_是记录管理器，用于管理记录文件的打开、关闭等操作
        // open_file返回一个unique_ptr<RmFileHandle>，表示表的数据文件句柄
        fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

        // Step 5: 打开该表的所有索引文件
        // entry.second是TabMeta，其中indexes存储了该表的所有索引元数据
        TabMeta& tab = entry.second;
        for (auto& index : tab.indexes) {
            // 获取索引文件名：表名_列名1_列名2_..._.idx
            std::string ix_name = ix_manager_->get_index_name(tab_name, index.cols);
            // ix_manager_是索引管理器，用于管理索引文件的打开、关闭等操作
            // open_index返回一个unique_ptr<IxIndexHandle>，表示索引文件句柄
            ihs_.emplace(ix_name, ix_manager_->open_index(tab_name, index.cols));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 *
 * 实现思路：
 * 1. 将数据库元数据信息写入磁盘文件(db.meta)
 * 2. 关闭所有表的记录文件句柄
 * 3. 关闭所有索引文件句柄
 * 4. 清空内存中的元数据和句柄信息
 * 5. 回退到上级目录
 */
void SmManager::close_db() {
    // Step 1: 将数据库元数据写入磁盘
    // flush_meta()函数将db_中的元数据写入DB_META_NAME文件
    flush_meta();

    // Step 2: 关闭所有表的记录文件
    // fhs_是一个unordered_map，key是表名，value是RmFileHandle的unique_ptr
    // rm_manager_->close_file会将缓冲区中的数据刷入磁盘并关闭文件
    for (auto& entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
    fhs_.clear();  // 清空记录文件句柄映射

    // Step 3: 关闭所有索引文件
    // ihs_是一个unordered_map，key是索引文件名，value是IxIndexHandle的unique_ptr
    for (auto& entry : ihs_) {
        ix_manager_->close_index(entry.second.get());
    }
    ihs_.clear();  // 清空索引文件句柄映射

    // Step 4: 清空数据库元数据
    // 将db_对象重置为空状态，表示当前没有打开的数据库
    db_.name_.clear();
    db_.tabs_.clear();

    // Step 5: 回退到上级目录
    // 数据库操作在数据库目录下进行，关闭后需要返回原目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto& col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context 上下文信息，用于事务和锁管理
 *
 * 实现思路：
 * 1. 申请表级排他锁（X锁）保证删除操作的独占性
 * 2. 获取表的元数据，检查表是否存在
 * 3. 关闭并删除表上的所有索引文件
 * 4. 关闭并删除表的记录文件
 * 5. 从数据库元数据中删除该表的信息
 * 6. 刷新元数据到磁盘
 * 
 * 加锁说明：
 * - drop_table是DDL操作，需要申请表级X锁
 * - X锁与所有其他锁都不相容，保证删除时没有其他事务在访问该表
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    // Step 1: 获取表的元数据
    // get_table会检查表是否存在，不存在会抛出TableNotFoundError
    TabMeta& tab = db_.get_table(tab_name);

    // Step 2: 申请表级排他锁（X锁）
    // DDL操作需要独占访问表，防止其他事务同时访问
    if (context != nullptr && context->lock_mgr_ != nullptr && 
        context->txn_ != nullptr && fhs_.count(tab_name)) {
        context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_.at(tab_name)->GetFd());
    }

    // Step 3: 关闭并删除表上的所有索引文件
    // 遍历表的所有索引，先关闭索引句柄，再删除索引文件
    for (auto& index : tab.indexes) {
        // 获取索引文件名
        std::string ix_name = ix_manager_->get_index_name(tab_name, index.cols);

        // 关闭索引文件句柄
        // ihs_中可能存在该索引的句柄（如果数据库已打开）
        if (ihs_.count(ix_name)) {
            ix_manager_->close_index(ihs_.at(ix_name).get());
            ihs_.erase(ix_name);
        }

        // 删除索引文件
        ix_manager_->destroy_index(tab_name, index.cols);
    }

    // Step 4: 关闭并删除表的记录文件
    // 先关闭文件句柄
    if (fhs_.count(tab_name)) {
        rm_manager_->close_file(fhs_.at(tab_name).get());
        fhs_.erase(tab_name);
    }
    // 删除记录文件
    rm_manager_->destroy_file(tab_name);

    // Step 5: 从数据库元数据中删除该表
    db_.tabs_.erase(tab_name);

    // Step 6: 刷新元数据到磁盘，确保删除操作持久化
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context 上下文信息，用于事务和锁管理
 *
 * 实现思路：
 * 1. 检查表是否存在
 * 2. 申请表级意向排他锁（IX锁）
 * 3. 检查索引是否已存在（在元数据中）
 * 4. 从表元数据中获取索引字段的ColMeta信息
 * 5. 如果索引文件已存在于磁盘上，先删除它（处理元数据和文件不一致的情况）
 * 6. 调用ix_manager_创建索引文件
 * 7. 更新表元数据，记录新索引
 * 
 * 加锁说明：
 * - create_index是DDL操作，需要申请表级IX锁
 * - IX锁表示事务将要修改表的结构
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // Step 1: 检查表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 获取表元数据
    TabMeta& tab = db_.get_table(tab_name);

    // Step 2: 申请表级意向排他锁（IX锁）
    // 创建索引需要独占访问表结构
    if (context != nullptr && context->lock_mgr_ != nullptr && 
        context->txn_ != nullptr && fhs_.count(tab_name)) {
        context->lock_mgr_->lock_IX_on_table(context->txn_, fhs_.at(tab_name)->GetFd());
    }

    // Step 3: 检查索引是否已存在（在元数据中）
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }

    // Step 4: 从表元数据中获取索引字段的ColMeta信息
    std::vector<ColMeta> index_cols;
    int col_tot_len = 0;
    for (const auto& col_name : col_names) {
        auto col_it = tab.get_col(col_name);
        index_cols.push_back(*col_it);
        col_tot_len += col_it->len;
    }

    // Step 5: 如果索引文件已存在于磁盘上，先删除它（处理元数据和文件不一致的情况）
    if (ix_manager_->exists(tab_name, index_cols)) {
        ix_manager_->destroy_index(tab_name, index_cols);
    }

    // Step 6: 调用ix_manager_创建索引文件
    ix_manager_->create_index(tab_name, index_cols);

    // Step 7: 更新表元数据，记录新索引
    IndexMeta index_meta;
    index_meta.tab_name = tab_name;
    index_meta.col_tot_len = col_tot_len;
    index_meta.col_num = col_names.size();
    index_meta.cols = index_cols;
    tab.indexes.push_back(index_meta);

    // 刷新元数据到磁盘
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context 上下文信息，用于事务和锁管理
 *
 * 实现思路：
 * 1. 检查表是否存在
 * 2. 申请表级意向排他锁（IX锁）
 * 3. 检查索引是否存在于表的元数据中
 * 4. 从ihs_中关闭并移除索引句柄
 * 5. 删除磁盘上的索引文件
 * 6. 从表元数据中移除索引信息
 * 7. 刷新元数据到磁盘
 * 
 * 加锁说明：
 * - drop_index是DDL操作，需要申请表级IX锁
 * - IX锁表示事务将要修改表的结构
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // Step 1: 检查表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 获取表元数据
    TabMeta& tab = db_.get_table(tab_name);

    // Step 2: 申请表级意向排他锁（IX锁）
    // 删除索引需要独占访问表结构
    if (context != nullptr && context->lock_mgr_ != nullptr && 
        context->txn_ != nullptr && fhs_.count(tab_name)) {
        context->lock_mgr_->lock_IX_on_table(context->txn_, fhs_.at(tab_name)->GetFd());
    }

    // Step 3: 检查索引是否存在
    if (!tab.is_index(col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    // Step 4: 获取索引文件名并关闭索引句柄
    // 首先需要获取列的ColMeta信息，用于构建索引文件名
    std::vector<ColMeta> index_cols;
    for (const auto& col_name : col_names) {
        auto col_it = tab.get_col(col_name);
        index_cols.push_back(*col_it);
    }
    std::string ix_name = ix_manager_->get_index_name(tab_name, index_cols);

    // Step 5: 如果索引句柄存在于ihs_中，先关闭它
    if (ihs_.count(ix_name)) {
        ix_manager_->close_index(ihs_.at(ix_name).get());
        ihs_.erase(ix_name);
    }

    // Step 4: 删除磁盘上的索引文件
    ix_manager_->destroy_index(tab_name, index_cols);

    // Step 5: 从表元数据中移除索引信息
    // get_index_meta返回指向目标索引的迭代器
    auto index_it = tab.get_index_meta(col_names);
    tab.indexes.erase(index_it);

    // Step 6: 刷新元数据到磁盘，确保删除操作持久化
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} cols 索引包含的字段元数据
 * @param {Context*} context 上下文信息
 *
 * 实现思路：
 * 这是drop_index的重载版本，接受ColMeta向量而非字段名向量
 * 内部将ColMeta转换为字段名，然后调用另一个drop_index版本
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    // 将ColMeta向量转换为字段名向量
    std::vector<std::string> col_names;
    for (const auto& col : cols) {
        col_names.push_back(col.name);
    }

    // 调用接受字段名向量的drop_index版本
    drop_index(tab_name, col_names, context);
}
