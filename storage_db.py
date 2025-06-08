# -----------------------------------------------------------------------
# storage_db.py
# Author: Jingyu Han  hjymail@163.com
#         Fei Yuan    b22042127@njupt.edu.cn
# -----------------------------------------------------------------------
# the module is to store tables in files
# Each table is stored in a separate file with the suffix ".dat".
# For example, the table named moviestar is stored in file moviestar.dat 
# -----------------------------------------------------------------------

# struct of file is as follows, each block is 4096
# ---------------------------------------------------
# block_0|block_1|...|block_n
# ----------------------------------------------------------------
from common_db import BLOCK_SIZE

# structure of block_0, which stores the meta information and field information
# ---------------------------------------------------------------------------------
# block_id                                # 0
# number_of_dat_blocks                    # at first it is 0 because there is no data in the table
# number_of_fields or number_of_records   # the total number of fields for the table
# -----------------------------------------------------------------------------------------


# the data type is as follows
# ----------------------------------------------------------
# 0->str,1->varstr,2->int,3->bool
# ---------------------------------------------------------------


# structure of data block, whose block id begins with 1
# ----------------------------------------
# block_id       
# number of records
# record_0_offset         # it is a pointer to the data of record
# record_1_offset
# ...
# record_n_offset
# ....
# free space
# ...
# record_n
# ...
# record_1
# record_0
# -------------------------------------------

# structre of one record
# -----------------------------
# pointer                     #offset of table schema in block id 0
# length of record            # including record head and record content
# time stamp of last update  # for example,1999-08-22
# field_0_value
# field_1_value
# ...
# field_n_value
# -------------------------


import struct
import os
import ctypes
import common_db
from transaction_db import global_transaction_manager


# --------------------------------------------
# the class can store table data into files
# functions include insert, delete and update
# --------------------------------------------

class Storage(object):

    # ------------------------------
    # constructor of the class
    # input:
    #       tablename
    # -------------------------------------
    def __init__(self, tablename):
        # print "__init__ of ",Storage.__name__,"begins to execute"
        tablename.strip()

        self.record_list = []
        self.record_Position = []

        if not os.path.exists(tablename + '.dat'.encode('utf-8')):  # the file corresponding to the table does not exist
            print('table file '.encode('utf-8') + tablename + '.dat does not exists'.encode('utf-8'))
            self.f_handle = open(tablename + '.dat'.encode('utf-8'), 'wb+')
            self.f_handle.close()
            self.open = False
            print(tablename + '.dat has been created'.encode('utf-8'))

        self.f_handle = open(tablename + '.dat'.encode('utf-8'), 'rb+')
        print('table file '.encode('utf-8') + tablename + '.dat has been opened'.encode('utf-8'))
        self.open = True

        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        self.f_handle.seek(0)
        self.dir_buf = self.f_handle.read(BLOCK_SIZE)

        self.dir_buf.strip()
        my_len = len(self.dir_buf)
        self.field_name_list = []
        beginIndex = 0

        if my_len == 0:  # there is no data in the block 0, we should write meta data into the block 0
            if isinstance(tablename, bytes):
                self.num_of_fields = input(
                    "please input the number of feilds in table " + tablename.decode('utf-8') + ":")
            else:
                self.num_of_fields = input(
                    "please input the number of feilds in table " + tablename + ":")
            if int(self.num_of_fields) > 0:

                self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
                self.block_id = 0
                self.data_block_num = 0
                struct.pack_into('!iii', self.dir_buf, beginIndex, 0, 0,
                                 int(self.num_of_fields))  # block_id,number_of_data_blocks,number_of_fields

                beginIndex = beginIndex + struct.calcsize('!iii')

                # the following is to write the field name,field type and field length into the buffer in turn
                for i in range(int(self.num_of_fields)):
                    field_name = input("please input the name of field " + str(i) + " :")

                    if len(field_name) < 10:
                        field_name = ' ' * (10 - len(field_name.strip())) + field_name

                    while True:
                        field_type = input(
                            "please input the type of field(0-> str; 1-> varstr; 2-> int; 3-> boolean) " + str(
                                i) + " :")
                        if int(field_type) in [0, 1, 2, 3]:
                            break

                    # to need further modification here
                    field_length = input("please input the length of field " + str(i) + " :")
                    temp_tuple = (field_name, int(field_type), int(field_length))
                    self.field_name_list.append(temp_tuple)
                    if isinstance(field_name, str):
                        field_name = field_name.encode('utf-8')

                    struct.pack_into('!10sii', self.dir_buf, beginIndex, field_name, int(field_type),
                                     int(field_length))
                    beginIndex = beginIndex + struct.calcsize('!10sii')

                self.f_handle.seek(0)
                self.f_handle.write(self.dir_buf)
                self.f_handle.flush()

        else:  # there is something in the file

            self.block_id, self.data_block_num, self.num_of_fields = struct.unpack_from('!iii', self.dir_buf, 0)

            print('number of fields is ', self.num_of_fields)
            print('data_block_num', self.data_block_num)
            beginIndex = struct.calcsize('!iii')

            # the followins is to read field name, field type and field length into main memory structures
            for i in range(self.num_of_fields):
                field_name, field_type, field_length = struct.unpack_from('!10sii', self.dir_buf,
                                                                          beginIndex + i * struct.calcsize(
                                                                              '!10sii'))  # i means no memory alignment

                temp_tuple = (field_name, field_type, field_length)
                self.field_name_list.append(temp_tuple)
                print("the " + str(i) + "th field information (field name,field type,field length) is ", temp_tuple)
        # print self.field_name_list
        record_head_len = struct.calcsize('!ii10s')
        record_content_len = sum(map(lambda x: x[2], self.field_name_list))
        # print record_content_len

        Flag = 1
        while Flag <= self.data_block_num:
            self.f_handle.seek(BLOCK_SIZE * Flag)
            self.active_data_buf = self.f_handle.read(BLOCK_SIZE)
            self.block_id, self.Number_of_Records = struct.unpack_from('!ii', self.active_data_buf, 0)
            print('Block_ID=%s,   Contains %s data' % (self.block_id, self.Number_of_Records))
            # There exists record
            if self.Number_of_Records > 0:
                for i in range(self.Number_of_Records):
                    self.record_Position.append((Flag, i))
                    offset = \
                        struct.unpack_from('!i', self.active_data_buf,
                                           struct.calcsize('!ii') + i * struct.calcsize('!i'))[
                            0]
                    record = struct.unpack_from('!' + str(record_content_len) + 's', self.active_data_buf,
                                                offset + record_head_len)[0]
                    tmp = 0
                    tmpList = []
                    for field in self.field_name_list:
                        t = record[tmp:tmp + field[2]].strip()
                        tmp = tmp + field[2]
                        if field[1] == 2:  # INTEGER
                            if isinstance(t, bytes):
                                t = t.decode('utf-8').strip()
                            t = int(t)
                        elif field[1] == 3:  # BOOLEAN
                            if isinstance(t, bytes):
                                t = t.decode('utf-8').strip()
                            t = t.lower() in ('true', '1', 'yes', 't')
                        else:  # STRING or VARSTRING
                            if isinstance(t, bytes):
                                t = t.decode('utf-8').strip()
                        tmpList.append(t)
                    self.record_list.append(tuple(tmpList))
            Flag += 1

    # ------------------------------
    # return the record list of the table
    # input:
    #       
    # -------------------------------------
    def getRecord(self):
        return self.record_list

    # --------------------------------
    # to insert a record into table
    # param insert_record: list
    # return: True or False
    # -------------------------------
    def insert_record(self, insert_record):
        """Insert with transaction logging support"""

        if self.current_transaction_id is None:
            print("Warning: No active transaction. Starting auto-transaction.")
            self.current_transaction_id = global_transaction_manager.begin_transaction()
            auto_commit = True
        else:
            auto_commit = False

        # example: ['xuyidan','23','123456']

        # step 1 : to check the insert_record is True or False
        # 同时进行对输入数据进行最终的类型转换
        
        tmpRecord = []
        for idx in range(len(self.field_name_list)):
            insert_record[idx] = insert_record[idx].strip()
            if self.field_name_list[idx][1] == 0 or self.field_name_list[idx][1] == 1:
                if len(insert_record[idx]) > self.field_name_list[idx][2]:
                    return False
                tmpRecord.append(insert_record[idx])
            if self.field_name_list[idx][1] == 2:
                try:
                    tmpRecord.append(int(insert_record[idx]))
                except:
                    return False
            if self.field_name_list[idx][1] == 3:
                try:
                    tmpRecord.append(bool(insert_record[idx]))
                except:
                    return False
            insert_record[idx] = ' ' * (self.field_name_list[idx][2] - len(insert_record[idx])) + insert_record[idx]

        # step2: Add tmpRecord to record_list ; change insert_record into inputstr
        inputstr = ''.join(insert_record)

        self.record_list.append(tuple(tmpRecord))

        # Step3: To calculate MaxNum in each Data Blocks
        record_content_len = len(inputstr)
        record_head_len = struct.calcsize('!ii10s')
        record_len = record_head_len + record_content_len
        MAX_RECORD_NUM = (BLOCK_SIZE - struct.calcsize('!i') - struct.calcsize('!ii')) / (
                record_len + struct.calcsize('!i'))

        # Step4: To calculate new record Position
        if not len(self.record_Position):
            self.data_block_num += 1
            self.record_Position.append((1, 0))
        else:
            last_Position = self.record_Position[-1]
            if last_Position[1] == MAX_RECORD_NUM - 1:
                self.record_Position.append((last_Position[0] + 1, 0))
                self.data_block_num += 1
            else:
                self.record_Position.append((last_Position[0], last_Position[1] + 1))

        last_Position = self.record_Position[-1]
        target_block_id = last_Position[0]

        # Write-Ahead Logging Rule: Log before-image first
        self.f_handle.seek(BLOCK_SIZE * target_block_id)
        old_block_data = self.f_handle.read(BLOCK_SIZE)
        
        # Log before-image
        global_transaction_manager.log_before_image(
            self.current_transaction_id,
            self.f_handle.name,
            target_block_id,
            old_block_data
        )

        # Perform the actual write operations
        # Update data_block_num
        self.f_handle.seek(0)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # Update data block head
        self.f_handle.seek(BLOCK_SIZE * last_Position[0])
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, last_Position[0], last_Position[1] + 1)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # Update data offset
        offset = struct.calcsize('!ii') + last_Position[1] * struct.calcsize('!i')
        beginIndex = BLOCK_SIZE - (last_Position[1] + 1) * record_len
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + offset)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!i'))
        struct.pack_into('!i', self.buf, 0, beginIndex)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # Update data
        record_schema_address = struct.calcsize('!iii')
        update_time = '2016-11-16'  # update time
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + beginIndex)
        self.buf = ctypes.create_string_buffer(record_len)
        struct.pack_into('!ii10s', self.buf, 0, record_schema_address, record_content_len, update_time.encode('utf-8'))
        struct.pack_into('!' + str(record_content_len) + 's', self.buf, record_head_len, inputstr.encode('utf-8'))
        self.f_handle.write(self.buf.raw)
        self.f_handle.flush()

        # Read the new block data for after-image
        self.f_handle.seek(BLOCK_SIZE * target_block_id)
        new_block_data = self.f_handle.read(BLOCK_SIZE)
        
        # Log after-image
        global_transaction_manager.log_after_image(
            self.current_transaction_id,
            self.f_handle.name,
            target_block_id,
            new_block_data
        )

        # Auto-commit if we started the transaction
        if auto_commit:
            global_transaction_manager.commit_transaction(self.current_transaction_id)
            self.current_transaction_id = None

        return True

    # ------------------------------
    # show the data structure and its data
    # input:
    #       t
    # -------------------------------------

    def show_table_data(self):
        print('|    '.join(map(lambda x: x[0].decode('utf-8').strip(), self.field_name_list)))  # show the structure

        # the following is to show the data of the table
        for record in self.record_list:
            print(record)

    # --------------------------------
    # to delete  the data file
    # input
    #       table name
    # output
    #       True or False
    # -----------------------------------
    def delete_table_data(self, tableName):

        # step 1: identify whether the file is still open
        if self.open == True:
            self.f_handle.close()
            self.open = False

        # step 2: remove the file from os   
        tableName.strip()
        if os.path.exists(tableName + '.dat'.encode('utf-8')):
            os.remove(tableName + '.dat'.encode('utf-8'))

        return True

    # ------------------------------
    # get the list of field information, each element of which is (field name, field type, field length)
    # input:
    #       
    # -------------------------------------

    def getFieldList(self):
        return self.field_name_list

    # ----------------------------------------
    # destructor
    # ------------------------------------------------
    def __del__(self):  # write the metahead information in head object to file

        if self.open == True:
            self.f_handle.seek(0)
            self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
            struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
            self.f_handle.write(self.buf)
            self.f_handle.flush()
            self.f_handle.close()


# ----------------------------------------
# The following functions are implemented by the student.
# ----------------------------------------

    # Fei Yuan: 更新记录中指定字段的值.该函数实现了按字段值更新记录的功能，支持所有数据类型(STRING, VARSTRING, INTEGER, BOOLEAN)。会对字段类型进行验证并进行相应的类型转换。
    def update_record(self, field_name, old_value, new_value):
        """Update with transaction logging support"""
        if self.current_transaction_id is None:
            print("Warning: No active transaction. Starting auto-transaction.")
            self.current_transaction_id = global_transaction_manager.begin_transaction()
            auto_commit = True
        else:
            auto_commit = False

        try:
            # Find field index and type
            field_index = -1
            field_type = None
            field_length = None
            
            for i, field_info in enumerate(self.field_name_list):
                current_field_name = field_info[0].decode('utf-8').strip()
                if current_field_name == field_name:
                    field_index = i
                    field_type = field_info[1]
                    field_length = field_info[2]
                    break
                    
            if field_index == -1:
                raise ValueError(f"Field '{field_name}' not found in table")

            # Type handling
            if field_type == 2:  # INTEGER
                try:
                    old_value = int(old_value)
                    new_value = int(new_value)
                except ValueError:
                    raise ValueError("Invalid integer format")
                    
            elif field_type == 3:  # BOOLEAN
                old_value = old_value.lower() in ('true', '1', 'yes')
                new_value = new_value.lower() in ('true', '1', 'yes')
                
            else:  # STRING or VARSTRING
                if len(new_value) > field_length:
                    raise ValueError(f"New value exceeds maximum length of {field_length}")

            # Log before-image of entire file state before any changes
            current_pos = self.f_handle.tell()
            self.f_handle.seek(0)
            entire_file_before = self.f_handle.read()
            self.f_handle.seek(current_pos)
            
            global_transaction_manager.log_before_image(
                self.current_transaction_id,
                self.f_handle.name,
                0,  # Using 0 as a special block_id for entire file
                entire_file_before
            )

            # Update matching records
            updated = False
            for i in range(len(self.record_list)):
                record = list(self.record_list[i])
                current_value = record[field_index]
                
                if field_type in [0, 1]:  # STRING or VARSTRING
                    if isinstance(current_value, bytes):
                        current_value = current_value.decode('utf-8').strip()
                    if isinstance(old_value, bytes):
                        old_value = old_value.decode('utf-8').strip()
                        
                    if current_value == old_value:
                        padded_new_value = new_value.ljust(field_length)
                        record[field_index] = padded_new_value
                        self.record_list[i] = tuple(record)
                        updated = True
                        
                else:  # INTEGER or BOOLEAN
                    if current_value == old_value:
                        record[field_index] = new_value
                        self.record_list[i] = tuple(record)
                        updated = True

            if updated:
                self.write_block_to_file()
                
                # Log after-image
                current_pos = self.f_handle.tell()
                self.f_handle.seek(0)
                entire_file_after = self.f_handle.read()
                self.f_handle.seek(current_pos)
                
                global_transaction_manager.log_after_image(
                    self.current_transaction_id,
                    self.f_handle.name,
                    0,
                    entire_file_after
                )
                
                print(f"Successfully updated records where {field_name}='{old_value}' to '{new_value}'")
            else:
                print(f"No records found with {field_name}='{old_value}'")

            # Auto-commit if we started the transaction
            if auto_commit:
                global_transaction_manager.commit_transaction(self.current_transaction_id)
                self.current_transaction_id = None
                
        except Exception as e:
             print(f"Error updating record: {str(e)}")
             if auto_commit and self.current_transaction_id:
                 global_transaction_manager.abort_transaction(self.current_transaction_id)
                 self.current_transaction_id = None

    # Fei Yuan: 将当前记录列表写入文件，动态计算块大小和记录分布。
    def write_block_to_file(self):
        """Write block to file with existing logic"""
        try:
            # Calculate record size
            record_content_len = sum(field[2] for field in self.field_name_list)
            record_head_len = struct.calcsize('!ii10s')
            record_len = record_head_len + record_content_len
            
            max_records_per_block = (BLOCK_SIZE - struct.calcsize('!ii')) // (record_len + struct.calcsize('!i'))
            if max_records_per_block < 1:
                raise ValueError("Record size too large for block size")
            
            num_blocks = (len(self.record_list) + max_records_per_block - 1) // max_records_per_block
            
            self.data_block_num = num_blocks
            self.f_handle.seek(struct.calcsize('!i'))
            self.f_handle.write(struct.pack('!i', self.data_block_num))
            
            for block_num in range(num_blocks):
                block_buf = ctypes.create_string_buffer(BLOCK_SIZE)
                
                start_idx = block_num * max_records_per_block
                end_idx = min(start_idx + max_records_per_block, len(self.record_list))
                records_in_block = end_idx - start_idx
                
                struct.pack_into('!ii', block_buf, 0, block_num + 1, records_in_block)
                
                offset_pos = struct.calcsize('!ii')
                data_pos = BLOCK_SIZE
                
                for i in range(start_idx, end_idx):
                    record = self.record_list[i]
                    data_pos -= record_len
                    
                    struct.pack_into('!i', block_buf, offset_pos, data_pos)
                    offset_pos += struct.calcsize('!i')
                    
                    record_str = ''
                    for j, value in enumerate(record):
                        field_length = self.field_name_list[j][2]
                        if isinstance(value, str):
                            padded_value = value.ljust(field_length)[:field_length]
                        else:
                            padded_value = str(value).ljust(field_length)[:field_length]
                        record_str += padded_value
                    
                    struct.pack_into('!ii10s' + str(record_content_len) + 's',
                                   block_buf,
                                   data_pos,
                                   0,
                                   record_content_len,
                                   b'timestamp',
                                   record_str.encode('utf-8'))
                
                self.f_handle.seek(BLOCK_SIZE * (block_num + 1))
                self.f_handle.write(block_buf)
            
            self.f_handle.flush()
            
        except Exception as e:
            print(f"Error writing to file: {str(e)}")

    # Yuan Fei: 删除符合条件的记录
    def del_one_record(self, field_name, field_value, field_list):
        """Delete with transaction logging support"""
        if self.current_transaction_id is None:
            print("Warning: No active transaction. Starting auto-transaction.")
            self.current_transaction_id = global_transaction_manager.begin_transaction()
            auto_commit = True
        else:
            auto_commit = False

        try:
            # Log before-image
            current_pos = self.f_handle.tell()
            self.f_handle.seek(0)
            entire_file_before = self.f_handle.read()
            self.f_handle.seek(current_pos)
            
            global_transaction_manager.log_before_image(
                self.current_transaction_id,
                self.f_handle.name,
                0,
                entire_file_before
            )

            # Existing delete logic
            field_name = field_name.decode('utf-8').strip()
            field_index = -1
            field_type = None
            
            for i, field_info in enumerate(self.field_name_list):
                current_field_name = field_info[0].decode('utf-8').strip()
                if current_field_name == field_name:
                    field_index = i
                    field_type = field_info[1]
                    break
                
            if field_index == -1:
                raise ValueError(f"Field '{field_name}' not found in table")
            
            try:
                if field_type == 2:  # INTEGER
                    match_value = int(field_value)
                elif field_type == 3:  # BOOLEAN
                    match_value = field_value.lower() in ('true', '1', 'yes')
                else:  # STRING or VARSTRING
                    match_value = field_value
                
            except ValueError:
                raise ValueError(f"Invalid value format for field type {field_type}")
            
            new_record_list = []
            deleted = False
            
            for record in self.record_list:
                current_value = record[field_index]
                if field_type in [0, 1]:
                    if isinstance(current_value, bytes):
                        current_value = current_value.decode('utf-8').strip()
                    if isinstance(match_value, bytes):
                        match_value = match_value.decode('utf-8').strip()
                    
                if str(current_value) != str(match_value):
                    new_record_list.append(record)
                else:
                    deleted = True
            
            if deleted:
                self.record_list = new_record_list
                self.write_block_to_file()
                
                # Log after-image
                current_pos = self.f_handle.tell()
                self.f_handle.seek(0)
                entire_file_after = self.f_handle.read()
                self.f_handle.seek(current_pos)
                
                global_transaction_manager.log_after_image(
                    self.current_transaction_id,
                    self.f_handle.name,
                    0,
                    entire_file_after
                )
                
                print(f"Successfully deleted records where {field_name}='{field_value}'")
            else:
                print(f"No records found with {field_name}='{field_value}'")

            # Auto-commit if we started the transaction
            if auto_commit:
                global_transaction_manager.commit_transaction(self.current_transaction_id)
                self.current_transaction_id = None
            
        except Exception as e:
            print(f"Error deleting record: {str(e)}")
            if auto_commit and self.current_transaction_id:
                global_transaction_manager.abort_transaction(self.current_transaction_id)
                self.current_transaction_id = None
