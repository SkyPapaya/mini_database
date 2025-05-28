# 数据库管理系统实现

## 个人完成记录


### 1. main_db.py完善
1. 完成了记录添加功能(choice=1)：
   - 实现了向已存在表中添加记录的功能
   - 支持多种数据类型(STRING, VARSTRING, INTEGER, BOOLEAN)的输入验证
   - 实现了连续添加多条记录的功能

2. 完成了删除记录功能(choice=6)：
   - 实现了根据字段关键字删除记录
   - 支持不同数据类型的字段匹配
   - 添加了完整的错误处理机制

3. 完成了更新记录功能(choice=7)：
   - 先显示所有记录并选择要修改的记录，然后选择要修改的字段并更新其值。
   - 实现了根据字段关键字更新记录
   - 支持字段值的类型验证和转换
   - 确保数据一致性
   

--- 

### 2. schema_db.py完善
1. 实现`viewTableStructure()`函数：
   ```python
   def viewTableStructure(self, table_name):
        """
        Implemented by student
        显示指定表的结构
        Args:
            table_name: 表名
        """
   ```

2. 修复了几个bug，见错误处理日志

---

### 3. storage_db.py新增功能
1. 实现了`update_record()`函数：
   ```python
   def update_record(self, field_name, old_value, new_value):
       """更新记录中指定字段的值
       Args:
           field_name: 字段名 (str)
           old_value: 旧值
           new_value: 新值
       """
   ```
   - 支持所有数据类型的字段更新
   - 实现了字段值的类型检查和转换
   - 提供了详细的错误信息
   - 确保数据一致性

2. 实现了`del_one_record()`函数：
   ```python
   def del_one_record(self, field_name, field_value, field_list):
       """删除符合条件的记录
       Args:
           field_name: 字段名 (str)
           field_value: 字段值 (str)
           field_list: 字段信息列表
       """
   ```
   - 支持按字段值精确删除
   - 处理不同数据类型的比较
   - 提供操作结果反馈

3. 实现了`write_block_to_file()`函数：
   ```python
   def write_block_to_file(self):
       """将数据块写入文件
       - 解决了数据块边界问题
       - 实现了高效的块管理
       - 确保数据完整性
       """
   ```
   主要功能：
   - 计算最优块大小和记录分布
   - 实现多块数据存储
   - 处理数据块边界
   - 确保数据完整性

---

### 4. 关键技术实现
1. 数据块管理优化：
   - 动态计算块大小和记录数
   - `write_block_to_file()`
   ```python
   max_records_per_block = (BLOCK_SIZE - struct.calcsize('!ii')) // (record_len + struct.calcsize('!i'))
   num_blocks = (len(self.record_list) + max_records_per_block - 1) // max_records_per_block
   ```

2. 数据类型处理：
   - 统一的类型转换机制
   ```python
   if field_type == 2:  # INTEGER
       match_value = int(field_value)
   elif field_type == 3:  # BOOLEAN
       match_value = field_value.lower() in ('true', '1', 'yes')
   ```

---

### 5. 使用注意事项
1. 数据类型支持：
   - STRING (0)
   - VARSTRING (1)
   - INTEGER (2)
   - BOOLEAN (3)

2. 布尔值输入格式：
   - True: 'true', '1', 'yes'
   - False: 'false', '0', 'no'

