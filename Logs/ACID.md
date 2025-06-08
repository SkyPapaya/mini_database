# ACID 事务管理实现日志

## ACID 属性实现概述

本数据库系统通过事务管理器 (TransactionManager) 实现完整的 ACID 属性：
- **原子性 (Atomicity)**: 通过事务日志和回滚机制确保事务全部成功或全部失败
- **一致性 (Consistency)**: 通过约束检查和事务隔离确保数据库状态一致
- **隔离性 (Isolation)**: 通过活跃事务表管理并发访问
- **持久性 (Durability)**: 通过预写日志 (WAL) 和强制写入确保提交事务的持久化

---

## 关键数据结构

### 1. 事务管理器 (TransactionManager)

```python
class TransactionManager:
    def __init__(self):
        self.current_transaction_id = 0              # 当前事务ID计数器
        self.active_transactions = {}                # 活跃事务表: trans_id -> transaction_info
        self.committed_transactions = set()          # 已提交事务集合
        
        # 日志文件路径
        self.before_image_file = "before_images.log"     # 前置镜像日志
        self.after_image_file = "after_images.log"       # 后置镜像日志
        self.transaction_log_file = "transaction.log"    # 事务状态日志
```

### 2. 事务状态常量

```python
# 事务状态
TRANS_ACTIVE = 1        # 活跃状态
TRANS_COMMITTED = 2     # 已提交状态
TRANS_ABORTED = 3       # 已中止状态

# 日志记录类型
LOG_BEGIN = 1           # 事务开始
LOG_BEFORE_IMAGE = 2    # 前置镜像
LOG_AFTER_IMAGE = 3     # 后置镜像
LOG_COMMIT = 4          # 事务提交
LOG_ABORT = 5           # 事务中止
```

### 3. 事务信息结构

```python
transaction_info = {
    'start_time': time.time(),    # 事务开始时间
    'state': TRANS_ACTIVE,        # 事务状态
    'operations': []              # 操作列表 [(操作类型, 日志条目), ...]
}
```

### 4. 日志条目结构

```python
log_entry = {
    'trans_id': trans_id,         # 事务ID
    'table_name': table_name,     # 表名
    'block_id': block_id,         # 数据块ID
    'data': data,                 # 数据内容
    'timestamp': time.time()      # 时间戳
}
```

---

## 技术实现步骤

### 1. 预写日志 (Write-Ahead Logging, WAL) 规则实现

#### 步骤1: 记录前置镜像
```python
def log_before_image(self, trans_id, table_name, block_id, old_data):
    """记录修改前的数据状态 - WAL规则核心"""
    # 1. 验证事务状态
    if trans_id not in self.active_transactions:
        return False
    
    # 2. 创建日志条目
    log_entry = {
        'trans_id': trans_id,
        'table_name': table_name,
        'block_id': block_id,
        'data': old_data,
        'timestamp': time.time()
    }
    
    # 3. 写入前置镜像日志文件 (必须在修改数据前完成)
    self._write_to_log_file(self.before_image_file, log_entry)
    
    # 4. 记录到事务操作列表
    self.active_transactions[trans_id]['operations'].append(('before', log_entry))
```

#### 步骤2: 记录后置镜像
```python
def log_after_image(self, trans_id, table_name, block_id, new_data):
    """记录修改后的数据状态"""
    # 类似前置镜像的处理流程
    # 写入 after_images.log 文件
```

### 2. 提交规则 (Commit Rule) 实现

```python
def commit_transaction(self, trans_id):
    """事务提交 - 确保持久性"""
    # 1. 验证事务存在且活跃
    if trans_id not in self.active_transactions:
        return False
        
    # 2. 强制写入所有后置镜像 (提交规则)
    self._flush_after_images(trans_id)
    
    # 3. 写入提交日志
    self._write_transaction_log(trans_id, LOG_COMMIT, None)
    
    # 4. 更新事务状态
    self.committed_transactions.add(trans_id)
    del self.active_transactions[trans_id]
```

### 3. 日志文件管理

#### 日志文件初始化
```python
def _init_log_files(self):
    """初始化日志文件结构"""
    for filename in [self.before_image_file, self.after_image_file, self.transaction_log_file]:
        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                # 写入空的头部块
                header = ctypes.create_string_buffer(BLOCK_SIZE)
                struct.pack_into('!ii', header, 0, 0, 0)  # next_block_id, num_records
                f.write(header)
```

#### 日志条目写入
```python
def _write_to_log_file(self, filename, log_entry):
    """将日志条目写入指定日志文件"""
    with open(filename, 'r+b') as f:
        # 1. 读取文件头获取写入位置
        f.seek(0)
        header_data = f.read(8)
        next_block_id, num_records = struct.unpack('!ii', header_data)
        
        # 2. 序列化日志条目
        table_name_bytes = log_entry['table_name'].encode('utf-8')[:50]
        data_bytes = log_entry['data'][:BLOCK_SIZE-100]
        
        # 3. 计算写入位置并写入
        write_pos = BLOCK_SIZE + num_records * BLOCK_SIZE
        f.seek(write_pos)
        
        # 4. 构造并写入日志条目
        entry_buffer = ctypes.create_string_buffer(BLOCK_SIZE)
        struct.pack_into('!50siid i', entry_buffer, 0, 
                       table_name_bytes, 
                       log_entry['trans_id'],
                       log_entry['block_id'], 
                       log_entry['timestamp'],
                       len(data_bytes))
        
        # 5. 更新文件头
        f.seek(0)
        struct.pack_into('!ii', header_data, 0, next_block_id, num_records + 1)
        f.write(header_data)
        f.flush()  # 强制写入磁盘
```

---

## ACID 属性保证机制

### 1. 原子性 (Atomicity)
- **实现方式**: 通过前置镜像日志支持回滚操作
- **关键机制**: 
  - 事务开始时记录 `LOG_BEGIN`
  - 每次修改前记录前置镜像
  - 事务失败时可通过前置镜像恢复原始状态

### 2. 一致性 (Consistency)
- **实现方式**: 通过事务边界和约束检查
- **关键机制**:
  - 事务内所有操作作为整体执行
  - 提交前验证所有约束条件
  - 失败时回滚到一致状态

### 3. 隔离性 (Isolation)
- **实现方式**: 通过活跃事务表管理并发访问
- **关键机制**:
  - `active_transactions` 跟踪所有活跃事务
  - `is_transaction_active()` 检查事务状态
  - 防止并发事务间的相互干扰

### 4. 持久性 (Durability)
- **实现方式**: 通过预写日志和强制写入
- **关键机制**:
  - WAL 规则: 修改前必须先写日志
  - 提交规则: 提交前必须强制写入所有日志
  - `f.flush()` 确保数据写入非易失性存储

---

## 使用示例

### 典型事务处理流程
```python
# 1. 开始事务
trans_id = global_transaction_manager.begin_transaction()

# 2. 记录前置镜像 (WAL规则)
global_transaction_manager.log_before_image(trans_id, "users", 1, old_data)

# 3. 执行数据修改
# ... 实际的数据修改操作 ...

# 4. 记录后置镜像
global_transaction_manager.log_after_image(trans_id, "users", 1, new_data)

# 5. 提交事务 (提交规则)
global_transaction_manager.commit_transaction(trans_id)
```

### 错误处理流程
```python
try:
    # 事务操作
    pass
except Exception as e:
    # 事务回滚
    global_transaction_manager.abort_transaction(trans_id)
```
