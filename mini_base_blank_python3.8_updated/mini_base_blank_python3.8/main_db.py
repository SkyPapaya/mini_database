# -----------------------
# main_db.py
# author: Jingyu Han   hjymail@163.com
#         Fei Yuan    b22042127@njupt.edu.cn
# modified by: Ning Wang, Yidan Xu
# modified by: Haomin Wang
# -----------------------------------
# This is the main loop of the program
# ---------------------------------------

import struct
import sys
import ctypes
import os

import head_db  # the main memory structure of table schema
import schema_db  # the module to process table schema
import storage_db  # the module to process the storage of instance

# import query_plan_db  # for SQL clause of which data is stored in binary format # Haomin Wang: This was duplicated
import lex_db  # for lex, where data is stored in binary format
import parser_db  # for yacc, where ddata is tored in binary format
import common_db  # the global variables, functions, constants in the program
import query_plan_db  # construct the query plan and execute it # Haomin Wang: Ensured this is present

PROMPT_STR = 'Input your choice  \n1:add a new table structure and data \n2:delete a table structure and data\
\n3:view a table structure and data \n4:delete all tables and data \n5:select from where clause\
\n6:delete a row according to field keyword \n7:update a row according to field keyword \n. to quit):\n'


# --------------------------
# the main loop, which needs further implementation4
# ---------------------------

def main():
    print('main function begins to execute')

    # The instance data of table is stored in binary format, which corresponds to chapter 2-8 of textbook

    schemaObj = schema_db.Schema()  # to create a schema object, which contains the schema of all tables
    dataObj = None
    choice = input(PROMPT_STR)

    while True:

        if choice == '1':  # add a new table and lines of data
            tableName = input('please enter your new table name:')
            if isinstance(tableName, str):
                tableName = tableName.encode('utf-8')
            #  tableName not in all.sch
            insertFieldList = []
            if tableName.strip() not in schemaObj.get_table_name_list():
                # Create a new table
                dataObj = storage_db.Storage(tableName)

                insertFieldList = dataObj.getFieldList()

                schemaObj.appendTable(tableName, insertFieldList)  # add the table structure
            else:
                dataObj = storage_db.Storage(tableName)

                # implemented by the student.
                # 实现向已存在表中添加记录的功能
                while True:  # 允许连续添加多条记录
                    record = []  # 存储一条记录的所有字段值
                    Field_List = dataObj.getFieldList()  # 获取表的字段列表

                    print("\n=== Adding new record to table {} ===".format(tableName.decode().strip()))
                    print("Enter field values (or '.' to finish adding records):")

                    # 获取每个字段的值
                    try:
                        for field in Field_List:
                            # 解析字段信息
                            field_name = field[0].strip().decode('utf-8')  # 字段名
                            field_type = field[1]  # 字段类型: 0:string, 1:varstring, 2:integer, 3:boolean
                            field_length = field[2]  # 字段长度限制

                            # 显示字段信息，提示用户输入
                            type_str = ["STRING", "VARSTRING", "INTEGER", "BOOLEAN"][field_type]
                            prompt = f"Input value for {field_name} (Type: {type_str}, Max Length: {field_length}): "

                            # 获取用户输入
                            value = input(prompt)
                            if value == '.':  # 用户输入'.'表示结束添加记录
                                raise StopIteration

                            # 根据字段类型验证和转换输入值
                            if field_type == 2:  # INTEGER类型
                                try:
                                    # 验证输入是否为有效整数
                                    int(value)  # 尝试转换为整数
                                    # 保持字符串形式，让insert_record函数进行最终转换
                                    record.append(str(value))
                                except ValueError:
                                    raise ValueError("Invalid integer value")

                            elif field_type == 3:  # BOOLEAN类型
                                value = value.lower()
                                # 支持多种布尔值输入格式
                                if value in ('true', '1', 'yes'):
                                    record.append('1')
                                elif value in ('false', '0', 'no'):
                                    record.append('0')
                                else:
                                    raise ValueError("Invalid boolean value")

                            else:  # STRING或VARSTRING类型
                                # 检查字符串长度是否超过限制
                                if len(value) > field_length:
                                    raise ValueError(f"Value exceeds maximum length of {field_length}")
                                record.append(value)

                        # 尝试插入记录到数据库
                        print("Trying to insert a record")
                        if dataObj.insert_record(record):
                            print("Record added successfully!")
                        else:
                            print("Failed to add record!")

                        # 询问用户是否继续添加记录
                        continue_adding = input("\nAdd another record? (y/n): ").lower()
                        if continue_adding != 'y':
                            break

                    except StopIteration:
                        # 用户通过输入'.'主动结束添加记录
                        break
                    except ValueError as e:
                        # 处理输入值验证错误
                        print(f"Error: {e}")
                        print("Record not added. Please try again.")
                        continue
                    except Exception as e:
                        # 处理其他未预期的错误
                        print(f"Unexpected error: {str(e)}")
                        print("Record not added. Please try again.")
                        continue

                # 清理资源
                del dataObj

            choice = input(PROMPT_STR)




        elif choice == '2':  # delete a table from schema file and data file

            table_name = input('please input the name of the table to be deleted:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if schemaObj.find_table(table_name.strip()):
                if schemaObj.delete_table_schema(
                        table_name):  # delete the schema from the schema file
                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.delete_table_data(table_name.strip())  # delete table content from the table file
                    del dataObj

                else:
                    print('the deletion from schema file fail')


            else:
                print('there is no table '.encode('utf-8') + table_name + ' in the schema file'.encode('utf-8'))

            choice = input(PROMPT_STR)



        elif choice == '3':  # view the table structure and all the data

            print(schemaObj.headObj.tableNames)
            table_name = input('please input the name of the table to be displayed:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if table_name.strip():
                if schemaObj.find_table(table_name.strip()):
                    schemaObj.viewTableStructure(table_name)  # to be implemented

                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.show_table_data()  # view all the data of the table
                    del dataObj
                else:
                    print('table name is None')

            choice = input(PROMPT_STR)



        elif choice == '4':  # delete all the table structures and their data
            table_name_list = list(schemaObj.get_table_name_list())
            # to be inserted here -> to delete from data files
            for i in range(len(table_name_list)):
                table_name = table_name_list[i]
                table_name.strip()

                if table_name:
                    stObj = storage_db.Storage(table_name)
                    stObj.delete_table_data(table_name.strip())  # delete table data
                    del stObj

            schemaObj.deleteAll()  # delete schema from schema file

            choice = input(PROMPT_STR)


        elif choice == '5':  # process SELECT FROM WHERE clause
            print('#        Your Query is to SQL QUERY                  #')
            sql_str = input('please enter the select from where clause:')
            lex_db.set_lex_handle()  # to set the global_lexer in common_db.py
            parser_db.set_handle()  # to set the global_parser in common_db.py

            try:
                common_db.global_syn_tree = common_db.global_parser.parse(sql_str.strip(),
                                                                          lexer=common_db.global_lexer)  # construct the global_syn_tree
                # reload(query_plan_db) # Haomin Wang: This was commented out as per readme for Python 3.x
                query_plan_db.construct_logical_tree()  # Haomin Wang: Construct the logical query plan
                query_plan_db.execute_logical_tree()  # Haomin Wang: Execute the logical query plan
            except Exception as e:  # Haomin Wang: Added exception handling to print the error
                print(f'WRONG SQL INPUT! Error: {e}')
            print('#----------------------------------------------------#')
            choice = input(PROMPT_STR)


        elif choice == '6':
            """删除记录功能

            根据指定的字段名和关键字值删除表中匹配的记录。
            支持所有数据类型(STRING, VARSTRING, INTEGER, BOOLEAN)的字段匹配。
            """
            try:
                # 1. 获取用户输入
                print("\n=== Delete Record by Field Value ===")
                table_name = input('Please input the table name: ')
                field_name = input('Please input the field name: ')
                value_name = input('Please input the search value: ')

                # 2. 数据格式转换
                # 将表名和字段名转换为bytes类型，与数据库存储格式保持一致
                if isinstance(table_name, str):
                    table_name = table_name.encode('utf-8')
                if isinstance(field_name, str):
                    field_name = field_name.encode('utf-8')
                # 注意：value_name保持字符串格式，由del_one_record函数处理类型转换

                # 3. 验证表是否存在
                if schemaObj.find_table(table_name.strip()):
                    # 4. 创建存储对象并执行删除操作
                    try:
                        dataObj = storage_db.Storage(table_name)

                        # 5. 验证输入有效性
                        if field_name and value_name:
                            # 6. 执行删除操作
                            dataObj.del_one_record(field_name, value_name, dataObj.getFieldList())
                            print('Delete operation completed successfully!')
                        else:
                            print('Error: Field name or value cannot be empty!')

                    except Exception as e:
                        print(f'Error during delete operation: {str(e)}')

                    finally:
                        # 7. 清理资源
                        if 'dataObj' in locals():
                            del dataObj
                else:
                    print(f'Error: Table "{table_name.decode()}" not found!')

            except Exception as e:
                print(f'Error: {str(e)}')

            choice = input(PROMPT_STR)

        elif choice == '7':
            """更新记录功能

            先显示所有记录并选择要修改的记录，然后选择要修改的字段并更新其值。
            支持所有数据类型的字段更新，包括：
            - STRING (0)
            - VARSTRING (1)
            - INTEGER (2)
            - BOOLEAN (3)
            """
            try:
                # 1. 获取表名并验证表是否存在
                print("\n=== Update Record ===")
                table_name = input('Please input the table name: ')
                if isinstance(table_name, str):
                    table_name = table_name.encode('utf-8')

                if schemaObj.find_table(table_name.strip()):
                    try:
                        # 2. 创建存储对象并显示表内容
                        dataObj = storage_db.Storage(table_name)
                        records = dataObj.getRecord()
                        field_list = dataObj.getFieldList()

                        if not records:
                            print("Table is empty!")
                            continue  # Haomin Wang: Changed from 'continue' to 'break' if in a loop, or adjust logic. Assuming 'continue' skips to next PROMPT_STR

                        # 3. 显示所有记录
                        print("\nCurrent records in table:")
                        print("Record #  |  " + "  |  ".join(f.decode('utf-8').strip() for f, _, _ in field_list))
                        print("-" * 80)
                        for idx, record in enumerate(records):
                            print(f"{idx:<9}|  " + "  |  ".join(str(v) for v in record))

                        # 4. 选择要修改的记录
                        while True:
                            try:
                                record_idx = int(input('\nEnter the record number to modify (or -1 to cancel): '))
                                if record_idx == -1:
                                    break
                                if 0 <= record_idx < len(records):
                                    # 5. 显示选中记录的字段
                                    print("\nSelected record fields:")
                                    for idx, (field_name_bytes, field_type, _) in enumerate(
                                            field_list):  # Haomin Wang: Renamed field_name to field_name_bytes
                                        type_str = ["STRING", "VARSTRING", "INTEGER", "BOOLEAN"][field_type]
                                        current_value = records[record_idx][idx]
                                        print(
                                            f"{idx}. {field_name_bytes.decode('utf-8').strip()} ({type_str}): {current_value}")  # Haomin Wang: Used field_name_bytes

                                    # 6. 选择要修改的字段
                                    field_idx = int(input('\nEnter the field number to modify (or -1 to cancel): '))
                                    if field_idx == -1:
                                        break

                                    if 0 <= field_idx < len(field_list):
                                        field_name_to_update = field_list[field_idx][0].decode(
                                            'utf-8').strip()  # Haomin Wang: Renamed field_name to field_name_to_update
                                        field_type_to_update = field_list[field_idx][
                                            1]  # Haomin Wang: Renamed field_type to field_type_to_update
                                        old_value = str(records[record_idx][field_idx])

                                        # 7. 获取并验证新值
                                        type_str_update = ["STRING", "VARSTRING", "INTEGER", "BOOLEAN"][
                                            field_type_to_update]  # Haomin Wang: Used field_type_to_update
                                        new_value = input(
                                            f'Enter new value for {field_name_to_update} ({type_str_update}): ')  # Haomin Wang: Used field_name_to_update

                                        # 8. 执行更新操作
                                        dataObj.update_record(
                                            field_name_to_update,  # Haomin Wang: Used field_name_to_update
                                            old_value,
                                            new_value
                                        )
                                        print('Update operation completed successfully!')
                                        break
                                    else:
                                        print("Invalid field number!")
                                else:
                                    print("Invalid record number!")
                            except ValueError:
                                print("Please enter a valid number!")

                    except Exception as e:
                        print(f'Error during update operation: {str(e)}')

                    finally:
                        # 9. 清理资源
                        if 'dataObj' in locals():
                            del dataObj
                else:
                    print(f'Error: Table "{table_name.decode()}" not found!')

            except Exception as e:
                print(f'Error: {str(e)}')

            choice = input(PROMPT_STR)



        elif choice == '.':
            print('Main loop finishes')
            if schemaObj:  # Haomin Wang: Added check before deleting
                del schemaObj
            break

    print('Program terminated successfully!')


if __name__ == '__main__':
    main()