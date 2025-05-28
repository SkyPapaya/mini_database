## 错误处理记录

### 1. 数据块写入缓冲区溢出问题

#### 问题描述
在使用选项6(删除记录)或选项7(更新记录)时，出现以下错误： 
```
Error writing to file: pack_into requires a buffer of at least 4114 bytes for packing 38 bytes at offset 4076 (actual buffer size is 4096)
```

这个错误发生在 `storage_db.py` 的 `write_block_to_file()` 函数中，当试图在数据块末尾写入记录时，请求的写入位置超出了预定义的块大小(4096字节)。

#### 原因分析
1. 数据块管理问题：
   - 原实现中没有正确处理块边界
   - 没有考虑记录可能跨越块边界的情况
   - 缺乏对大量记录的分块存储机制

2. 缓冲区管理问题：
   - 固定使用4096字节的块大小
   - 在块末尾写入数据时没有检查剩余空间
   - 缺乏溢出处理机制

#### 解决方案
在 `storage_db.py` 中重写了 `write_block_to_file()` 函数，主要改进：

1. 块大小管理：
   ```python
   # 计算每个块可以存储的最大记录数
   max_records_per_block = (BLOCK_SIZE - struct.calcsize('!ii')) // (record_len + struct.calcsize('!i'))
   if max_records_per_block < 1:
       raise ValueError("Record size too large for block size")
   ```

2. 多块存储：
   ```python
   # 计算需要的总块数
   num_blocks = (len(self.record_list) + max_records_per_block - 1) // max_records_per_block
   ```

3. 记录分布优化：
   - 从块尾部开始写入数据
   - 正确计算和更新偏移量
   - 确保不会超出块边界


---


### 2. 源代码删除操作错误
#### 错误描述
在测试时，发现以下操作序列会导致系统崩溃：
1. 使用 choice 4 删除所有表和数据
2. 尝试创建新表
3. 系统报错：TypeError，表明尝试将列表索引用作字典键

## 错误定位
1. 文件：`schema_db.py`
2. 问题函数：`deleteAll()` 和 `appendTable()`
3. 错误原因：
   - `deleteAll()` 方法中错误地将 `tableFields` 初始化为列表 `[]`
   - 而在 `appendTable()` 中试图将其作为字典使用
   - 导致类型不匹配错误

## 代码修改

### 1. deleteAll() 方法修改
```python
# 修改前
def deleteAll(self):
    self.headObj.tableFields = []  # 错误：初始化为列表
    self.headObj.tableNames = []
    self.fileObj.seek(0)
    self.fileObj.truncate(0)
    self.headObj.isStored = False
    self.headObj.lenOfTableNum = 0
    self.headObj.offsetOfBody = self.body_begin_index
    self.fileObj.flush()

# 修改后
def deleteAll(self):
    """删除所有表结构和数据"""
    self.headObj.tableFields = {}  # 修正：初始化为字典
    self.headObj.tableNames = []
    self.fileObj.seek(0)
    self.fileObj.truncate(0)
    self.headObj.isStored = False
    self.headObj.lenOfTableNum = 0
    self.headObj.offsetOfBody = self.body_begin_index
    
    # 添加：写入初始化的元数据
    buf = struct.pack('!?ii', False, 0, self.body_begin_index)
    self.fileObj.seek(0)
    self.fileObj.write(buf)
    self.fileObj.flush()
```

### 2. appendTable() 方法相关修改（尚未实现，单纯修改deleteAll() 已经可正常运行）
```python
def appendTable(self, tableName, fieldList):
    # ... (previous code)
    
    # 修改前
    self.headObj.tableFields[tableName] = fieldList
    
    # 修改后：添加类型检查和转换
    if not hasattr(self.headObj, 'tableFields'):
        self.headObj.tableFields = {}
    table_name_key = tableName.decode('utf-8') if isinstance(tableName, bytes) else tableName
    self.headObj.tableFields[table_name_key] = fieldList
```
- 到底是谁把字典初始化成了列表😩😩😩😩😩😩😩😩😩

---
### 3. Python 3 Map 对象访问错误

#### 问题描述
在使用删除表功能时，系统报错：`'map' object is not subscriptable`。这个错误发生在 `schema_db.py` 的 `delete_table_schema` 方法中，当尝试访问 map 对象的元素时出现。

#### 原因分析
1. Python 2 和 Python 3 的 `map()` 函数行为差异：
   - Python 2 中 `map()` 返回列表
   - Python 3 中 `map()` 返回迭代器对象
   - 迭代器对象不支持索引访问

2. 代码中的问题：
   ```python
   name_list = map(lambda x: x[0], self.headObj.tableNames)
   field_num_per_table = map(lambda x: x[1], self.headObj.tableNames)
   table_offset = list(map(lambda x: x[2], self.headObj.tableNames))
   ```
   - 前两个 map 对象没有转换为列表就尝试进行索引访问
   - `zip()` 的结果也需要转换为列表

#### 解决方案
修改 `delete_table_schema` 方法，将所有 map 对象转换为列表：

```python
# 修改后的代码
name_list = list(map(lambda x: x[0], self.headObj.tableNames))
field_num_per_table = list(map(lambda x: x[1], self.headObj.tableNames))
table_offset = list(map(lambda x: x[2], self.headObj.tableNames))

# ...

self.headObj.tableNames = list(zip(name_list,field_num_per_table,table_offset))
```

这样修改后，所有的数据结构都支持索引访问，解决了 `'map' object is not subscriptable` 错误。