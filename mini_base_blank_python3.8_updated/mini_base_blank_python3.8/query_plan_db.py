# ------------------------------------------------
# query_plan_db.py
# author: Jingyu Han  hjymail@163.com
# modified by:Shuting Guo shutingnjupt@gmail.com
# modified by: Haomin Wang
# ------------------------------------------------

# ----------------------------------------------------------
# this module can turn a syntax tree into a query plan tree
# ----------------------------------------------------------

import common_db
import storage_db
import itertools


# --------------------------------
# to import the syntax tree, which is defined in parser_db.py
# -------------------------------------------
# from common_db import global_syn_tree as syn_tree # Haomin Wang: Accessing via common_db

# Haomin Wang: It's better to pass the syn_tree as an argument rather than relying on global
# However, to maintain consistency with existing structure that uses global_syn_tree:
# syn_tree = common_db.global_syn_tree

class parseNode:  # Haomin Wang: This class seems to be for extracting info from the old syntax tree. May need adjustment or replacement.
    def __init__(self):
        self.sel_list = []
        self.from_list = []
        self.where_list = []  # Haomin Wang: This might become a tree/object itself for complex conditions

    def get_sel_list(self):
        return self.sel_list

    def get_from_list(self):
        return self.from_list

    def get_where_list(self):  # Haomin Wang: May need to return a condition tree
        return self.where_list

    def update_sel_list(self, sel_list):  # Haomin Wang: Corrected typo from self_list to sel_list
        self.sel_list = sel_list

    def update_from_list(self, from_list):
        self.from_list = from_list

    def update_where_list(self, where_list):
        self.where_list = where_list


# --------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# Modified by: Haomin Wang
# to extract data from global variable syn_tree
# output:
#       sel_list (list of column names or '*')
#       from_list (list of table names)
#       where_conditions (structured representation of conditions)
# --------------------------------
def extract_sfw_data_from_new_tree(syntax_tree_root):  # Haomin Wang: New function for new tree
    print('extract_sfw_data_from_new_tree begins to execute')
    sel_list = []
    from_list = []
    where_conditions = None  # This will be the root of the condition subtree

    if not syntax_tree_root or syntax_tree_root.value != 'QUERY':
        print('Invalid syntax tree root')
        return [], [], None

    for child_node in syntax_tree_root.children:
        if child_node.value == 'SELECT_CLAUSE':
            # Child of SELECT_CLAUSE is SelList (e.g., SELECT_LIST_SINGLE, SELECT_LIST_MULTIPLE)
            sel_list_node = child_node.children[0]
            current_sel_node = sel_list_node
            while current_sel_node:
                if current_sel_node.value == 'SELECT_LIST_SINGLE' or current_sel_node.value == 'SELECT_LIST_MULTIPLE':
                    column_node = current_sel_node.children[0]  # This is COLUMN_NAME node
                    if column_node.value == 'COLUMN_NAME' and column_node.children:
                        sel_list.append(column_node.children[0])  # Actual column name string
                    # Handle SELECT * if ASTERISK token and rule were added
                    # elif column_node.value == 'ALL_COLUMNS':
                    #    sel_list.append('*')
                    if current_sel_node.value == 'SELECT_LIST_MULTIPLE' and len(current_sel_node.children) > 1:
                        current_sel_node = current_sel_node.children[1]  # Move to the next part of SelList
                    else:
                        break  # End of select list
                else:  # Should not happen with current grammar
                    break

        elif child_node.value == 'FROM_CLAUSE':
            # Child of FROM_CLAUSE is FromList
            from_list_node = child_node.children[0]
            current_from_node = from_list_node
            while current_from_node:
                if current_from_node.value == 'FROM_LIST_SINGLE' or current_from_node.value == 'FROM_LIST_MULTIPLE':
                    table_node = current_from_node.children[0]  # This is TABLE_NAME node
                    if table_node.value == 'TABLE_NAME' and table_node.children:
                        from_list.append(table_node.children[0])  # Actual table name string
                    if current_from_node.value == 'FROM_LIST_MULTIPLE' and len(current_from_node.children) > 1:
                        current_from_node = current_from_node.children[1]  # Move to next
                    else:
                        break
                else:
                    break

        elif child_node.value == 'WHERE_CLAUSE':
            # Child of WHERE_CLAUSE is Cond (the root of condition tree)
            if child_node.children:
                where_conditions = child_node.children[0]

    print(f"Extracted Select List: {sel_list}")
    print(f"Extracted From List: {from_list}")
    # common_db.show(where_conditions) # Haomin Wang: For debugging the condition tree
    return sel_list, from_list, where_conditions


# Haomin Wang: The original destruct and show functions are for the old tree structure.
# They might not be directly usable or need significant adaptation for the new tree.
# The new `extract_sfw_data_from_new_tree` replaces the need for the old `extract_sfw_data` and its helpers for the new tree.


# ---------------------------
# input:
#       from_list
# output:
#       a tree node representing JOIN operations (or single table)
# Haomin Wang: This function constructs the "FROM part" of the logical query plan.
# For multiple tables, it creates a tree of cross products (X).
# -----------------------------------
def construct_from_node(from_list):  # Haomin Wang: from_list is a list of table name strings
    if not from_list:
        return None

    if len(from_list) == 1:
        # Single table, create a leaf node for the table
        return common_db.Node('TABLE', [from_list[0]])  # Using 'TABLE' to denote a data source node
        # The child is the table name string itself
    else:
        # Multiple tables, create a tree of cross products (represented by 'X')
        # Build left-deep tree: X(X(T1, T2), T3) ...
        current_join_node = common_db.Node('X',
                                           [common_db.Node('TABLE', [from_list[0]]),
                                            common_db.Node('TABLE', [from_list[1]])])
        for i in range(2, len(from_list)):
            current_join_node = common_db.Node('X',
                                               [current_join_node,
                                                common_db.Node('TABLE', [from_list[i]])])
        return current_join_node


# ---------------------------
# input:
#       from_node (node from construct_from_node)
#       where_conditions (the condition tree from extract_sfw_data_from_new_tree)
# output:
#       a tree node representing selection (filter)
# Haomin Wang: This function adds the "WHERE part" (filter) to the logical query plan.
# -----------------------------------
def construct_where_node(from_node, where_conditions):
    if not from_node:  # Should not happen if FROM clause is mandatory
        return None
    if where_conditions:  # If there's a WHERE clause
        # The where_conditions is already a tree from the parser.
        # We create a 'Filter' node, its first child is the data source (from_node),
        # its second child is the condition tree itself.
        return common_db.Node('Filter', [from_node, where_conditions])
    else:  # No WHERE clause
        return from_node


# ---------------------------
# input:
#       wf_node (node from construct_where_node)
#       sel_list (list of column names or '*')
# output:
#       a tree node representing projection
# Haomin Wang: This function adds the "SELECT part" (projection) to the logical query plan.
# -----------------------------------
def construct_select_node(wf_node, sel_list):
    if not wf_node:
        return None
    if sel_list:
        # Create a 'Proj' node. Its first child is the data source (wf_node).
        # The sel_list (list of column names) is stored in the 'var' attribute of the 'Proj' node.
        return common_db.Node('Proj', [wf_node], varList=sel_list)
    else:  # Should not happen if SELECT clause is mandatory and has columns
        return wf_node

    # ----------------------------------


# Author: Shuting Guo shutingnjupt@gmail.com
# Modified by: Haomin Wang
# to execute the query plan and return the result
# input
#       logical_query_plan_tree (the root of the logical query plan)
# ---------------------------------------------
def execute_logical_tree_recursive(node):  # Haomin Wang: Made it recursive and explicit argument
    if not node:
        return None, None  # result_data, field_names

    node_type = node.value

    if node_type == 'TABLE':
        # Leaf node: represents a table. Load data from storage.
        table_name = node.children[0]  # The table name string
        print(f"Executing: Accessing table {table_name}")
        storage_obj = storage_db.Storage(table_name.encode('utf-8'))  # Assuming table names are strings
        records = storage_obj.getRecord()
        field_infos = storage_obj.getFieldList()  # List of (name_bytes, type, length)
        field_names = [fi[0].decode('utf-8').strip() for fi in field_infos]
        # Haomin Wang: Qualify field names with table name if not already: table.field
        qualified_field_names = [f"{table_name}.{fn}" for fn in field_names]

        # Haomin Wang: Store original field info for type checking in conditions
        # This is a simplified way. A more robust system would carry schema info along with data.
        # For now, we'll fetch it as needed or assume execute_condition handles types.
        # If records are list of tuples: [(val1, val2), (val1,val2)]
        # And field_names: ['col1', 'col2']
        # We want to return data in a structured way, e.g., list of dicts or list of lists with header

        # Return list of records (as lists/tuples) and qualified field names
        return records, qualified_field_names, field_infos

    elif node_type == 'X':  # Cross Product
        # Binary operator: two children representing two data sources
        left_records, left_fields, left_field_infos = execute_logical_tree_recursive(node.children[0])
        right_records, right_fields, right_field_infos = execute_logical_tree_recursive(node.children[1])

        if left_records is None or right_records is None:
            print("Error in executing cross product: one operand is None.")
            return None, None, None

        print(f"Executing: Cross Product between results for {node.children[0].value} and {node.children[1].value}")

        combined_records = []
        for lr in left_records:
            for rr in right_records:
                # Ensure lr and rr are iterables (tuples or lists)
                combined_record = list(lr) + list(rr)
                combined_records.append(tuple(combined_record))

        combined_fields = left_fields + right_fields
        combined_field_infos = left_field_infos + right_field_infos
        return combined_records, combined_fields, combined_field_infos

    elif node_type == 'Filter':
        # Unary operator: first child is data source, second child is condition tree
        source_records, source_fields, source_field_infos = execute_logical_tree_recursive(node.children[0])
        condition_tree = node.children[1]  # The root of the condition expression tree

        if source_records is None:
            print("Error in executing filter: source is None.")
            return None, None, None
        if not condition_tree:  # No condition, should not happen if Filter node exists
            return source_records, source_fields, source_field_infos

        print(f"Executing: Filter on results from {node.children[0].value}")

        filtered_records = []
        for record in source_records:
            # Create a dictionary mapping field names to values for the current record
            # This makes it easier to evaluate conditions. Field names are qualified.
            record_dict = dict(zip(source_fields, record))
            if evaluate_condition(condition_tree, record_dict, source_field_infos, source_fields):
                filtered_records.append(record)

        return filtered_records, source_fields, source_field_infos

    elif node_type == 'Proj':  # Projection
        # Unary operator: first child is data source. Projected columns are in node.var
        source_records, source_fields, source_field_infos = execute_logical_tree_recursive(node.children[0])
        project_columns_requested = node.var  # List of column names to project (could be qualified or not)

        if source_records is None:
            print("Error in executing projection: source is None.")
            return None, None, None

        print(f"Executing: Projection for columns {project_columns_requested} from {node.children[0].value}")

        if not project_columns_requested or (
                len(project_columns_requested) == 1 and project_columns_requested[0] == '*'):
            # SELECT * case
            return source_records, source_fields, source_field_infos

        projected_records = []
        projected_field_names = []
        projected_field_infos_final = []  # Haomin Wang: To carry over field info for projected cols

        # Determine indices of columns to project
        # This needs to handle qualified (table.col) and unqualified (col) names
        # source_fields are already qualified: ['table1.colA', 'table1.colB', 'table2.colC']

        col_indices_to_project = []
        for req_col_name_full in project_columns_requested:
            found = False
            # Try direct match (if requested column is already qualified or unique unqualified)
            for i, src_col_name in enumerate(source_fields):
                if req_col_name_full == src_col_name:  # Exact match for qualified name
                    col_indices_to_project.append(i)
                    projected_field_names.append(src_col_name)
                    projected_field_infos_final.append(source_field_infos[i])
                    found = True
                    break
                # Try matching unqualified name against the field part of qualified source fields
                # e.g., if req_col_name_full is 'colA' and src_col_name is 'table1.colA'
                if '.' in src_col_name and src_col_name.split('.')[1] == req_col_name_full:
                    # This could be ambiguous if 'colA' exists in multiple tables in source_fields
                    # For simplicity, take the first match. A real system would need ambiguity resolution.
                    # Or require all projected columns to be qualified if ambiguity exists.
                    # Let's assume for now: if unqualified, it must uniquely identify a column.
                    # For this pass, we'll just prefer exact match first. If not found, then try unqualified part.
                    # A better way: build a map of unqualified_name -> list_of_qualified_names_indices

                    # Simple check: if not found yet by exact match
                    if not found:
                        # Check for ambiguity for unqualified name
                        possible_matches = [j for j, sf in enumerate(source_fields) if
                                            sf.split('.')[-1] == req_col_name_full]
                        if len(possible_matches) == 1:
                            idx = possible_matches[0]
                            col_indices_to_project.append(idx)
                            projected_field_names.append(source_fields[idx])  # Keep the qualified name
                            projected_field_infos_final.append(source_field_infos[idx])
                            found = True
                            break
                        elif len(possible_matches) > 1:
                            print(f"Error: Column name '{req_col_name_full}' is ambiguous.")
                            return None, None, None  # Ambiguity error

            if not found:
                print(f"Error: Column '{req_col_name_full}' not found in source fields: {source_fields}")
                return None, None, None  # Column not found error

        # Perform the projection
        for record in source_records:
            new_record = tuple(record[i] for i in col_indices_to_project)
            projected_records.append(new_record)

        return projected_records, projected_field_names, projected_field_infos_final

    else:
        print(f"Unknown logical query plan node type: {node_type}")
        return None, None, None


# Haomin Wang: Helper function to evaluate a condition tree for a given record
def evaluate_condition(condition_node, record_dict, field_infos, field_names_qualified):
    if not condition_node:
        return True  # No condition means true

    node_type = condition_node.value

    if node_type == 'SIMPLE_CONDITION':
        # Children: [COLUMN_NAME_COND_node, OPERATOR_node, VALUE_node]
        column_name_node = condition_node.children[0]
        operator_node = condition_node.children[1]
        value_node = condition_node.children[2]

        column_name_in_cond = column_name_node.children[0]  # Actual string like 'TCNAME' or 'TABLE.TCNAME'

        # Resolve column name (it might be qualified or unqualified)
        # record_dict keys are qualified: 'table.field'
        record_value = None
        qualified_col_name_found = None

        if column_name_in_cond in record_dict:  # Directly qualified or unique unqualified already resolved
            record_value = record_dict[column_name_in_cond]
            qualified_col_name_found = column_name_in_cond
        else:  # Try to find as unqualified part
            matches = [qn for qn in record_dict.keys() if qn.split('.')[-1] == column_name_in_cond]
            if len(matches) == 1:
                qualified_col_name_found = matches[0]
                record_value = record_dict[qualified_col_name_found]
            elif len(matches) > 1:
                print(f"Ambiguous column '{column_name_in_cond}' in condition.")
                return False
            else:
                print(f"Column '{column_name_in_cond}' not found in record for condition.")
                return False

        # Get the type of the column from field_infos
        field_idx = field_names_qualified.index(qualified_col_name_found)
        col_type_code = field_infos[field_idx][1]  # 0:str, 1:varstr, 2:int, 3:bool

        operator_str = operator_node.children[0]  # e.g., '=', '>', '<', etc. (token value from lexer)

        constant_val_from_query_node = value_node.children[0]  # This is the raw value from query (string or int)

        # Type cast the constant from the query to match the column's type
        # This is a crucial step for correct comparison.
        try:
            if col_type_code == 2:  # INTEGER
                const_typed = int(constant_val_from_query_node)
                # record_value should already be int if data loading is correct
            elif col_type_code == 3:  # BOOLEAN
                # Assuming query gives 'true'/'false' or 0/1. Parser/lexer should normalize.
                # For now, if STRING_CONST was 'true', it's "true". If CONSTANT was 1, it's 1.
                if isinstance(constant_val_from_query_node, str):
                    const_typed = constant_val_from_query_node.lower() in ['true', '1']
                else:  # Assume it's an int 0 or 1
                    const_typed = bool(constant_val_from_query_node)
                # record_value should already be bool
            else:  # STRING or VARSTRING (types 0, 1)
                const_typed = str(constant_val_from_query_node)
                # record_value should already be string
        except ValueError:
            print(
                f"Type mismatch in condition: cannot convert query value '{constant_val_from_query_node}' for column '{qualified_col_name_found}'.")
            return False

        # Perform comparison
        # Ensure record_value is also of the correct comparable type (should be from data loading)
        if operator_str == '=' or operator_str.upper() == 'EQX': return record_value == const_typed
        if operator_str == '!=': return record_value != const_typed
        if operator_str == '<': return record_value < const_typed
        if operator_str == '>': return record_value > const_typed
        if operator_str == '<=': return record_value <= const_typed
        if operator_str == '>=': return record_value >= const_typed

        print(f"Unknown operator '{operator_str}' in condition.")
        return False

    elif node_type == 'CONDITION_AND':
        return evaluate_condition(condition_node.children[0], record_dict, field_infos, field_names_qualified) and \
            evaluate_condition(condition_node.children[1], record_dict, field_infos, field_names_qualified)

    elif node_type == 'CONDITION_OR':
        return evaluate_condition(condition_node.children[0], record_dict, field_infos, field_names_qualified) or \
            evaluate_condition(condition_node.children[1], record_dict, field_infos, field_names_qualified)

    # Note: LPARENT/RPARENT nodes are handled by parser by just passing the inner Cond node up.
    # So, they don't appear in the condition tree passed to evaluate_condition if parser handles it like:
    # 'Cond : LPARENT Cond RPARENT' with p[0] = p[2]

    print(f"Unknown condition node type: {node_type}")
    return False


def execute_logical_tree():  # Haomin Wang: This is the main entry point called by main_db.py
    if common_db.global_logical_tree:
        print("Executing Logical Query Plan Tree:")
        # common_db.show(common_db.global_logical_tree) # For debugging the plan tree structure

        final_records, final_field_names, _ = execute_logical_tree_recursive(common_db.global_logical_tree)

        if final_records is not None and final_field_names is not None:
            print("\nQuery Results:")
            # Print header
            print(" | ".join(final_field_names))
            print("-" * (sum(len(fn) for fn in final_field_names) + 3 * (
                        len(final_field_names) - 1)))  # Dynamic underline
            # Print records
            for record in final_records:
                print(" | ".join(str(item) for item in record))
            print(f"\n{len(final_records)} row(s) returned.")
        else:
            print("Query execution failed or returned no results.")
    else:
        print('There is no query plan tree for the execution.')


# --------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# Modified by: Haomin Wang
# to construct a logical query plan tree
# output:
#       sets common_db.global_logical_tree
# ---------------------------------
def construct_logical_tree():
    if common_db.global_syn_tree:  # Haomin Wang: Use the global syntax tree
        # Haomin Wang: Use the new extraction function for the modified syntax tree
        sel_list, from_list, where_conditions = extract_sfw_data_from_new_tree(common_db.global_syn_tree)

        # Haomin Wang: Debug prints
        # print(f"Construct Logical Tree - Select: {sel_list}, From: {from_list}")
        # if where_conditions:
        #    print("Construct Logical Tree - Where Conditions Tree:")
        #    common_db.show(where_conditions)
        # else:
        #    print("Construct Logical Tree - No Where Conditions")

        # 1. Construct FROM part (cross products or single table access)
        from_plan_node = construct_from_node(from_list)

        # 2. Construct WHERE part (filtering)
        where_plan_node = construct_where_node(from_plan_node, where_conditions)

        # 3. Construct SELECT part (projection)
        common_db.global_logical_tree = construct_select_node(where_plan_node, sel_list)

        if common_db.global_logical_tree:
            print("Logical Query Plan Tree Constructed Successfully:")
            # common_db.show(common_db.global_logical_tree) # Haomin Wang: For debugging the plan
        else:
            print("Failed to construct logical query plan tree.")
    else:
        print('There is no data in the syntax tree in the construct_logical_tree')


# Haomin Wang: Test code, if needed
'''
if __name__ == '__main__':
    # This would require setting up a mock common_db.global_syn_tree
    # Example:
    # node_val = common_db.Node("VALUE_CONST", [10])
    # node_col = common_db.Node("COLUMN_NAME_COND", ["age"])
    # node_op = common_db.Node("OPERATOR", ['>'])
    # simple_cond = common_db.Node("SIMPLE_CONDITION", [node_col, node_op, node_val])
    # common_db.global_syn_tree = common_db.Node("QUERY", [
    #     common_db.Node("SELECT_CLAUSE", [common_db.Node("SELECT_LIST_SINGLE", [common_db.Node("COLUMN_NAME", ["name"])])]),
    #     common_db.Node("FROM_CLAUSE", [common_db.Node("FROM_LIST_SINGLE", [common_db.Node("TABLE_NAME", ["students"])])]),
    #     common_db.Node("WHERE_CLAUSE", [simple_cond])
    # ])
    #
    # # Mock storage_db.Storage and its methods if needed for testing execute_logical_tree directly
    # class MockStorage:
    #     def __init__(self, tablename):
    #         self.tablename = tablename
    #         if tablename == b"students.dat": # Assuming tablename is bytes
    #             self.records = [("Alice", 20), ("Bob", 22), ("Charlie", 18)]
    #             self.field_list = [(b"name      ", 0, 10), (b"age       ", 2, 10)]
    #         else:
    #             self.records = []
    #             self.field_list = []
    #     def getRecord(self): return self.records
    #     def getFieldList(self): return self.field_list
    #
    # original_storage = storage_db.Storage
    # storage_db.Storage = MockStorage 
    #
    # construct_logical_tree()
    # execute_logical_tree()
    #
    # storage_db.Storage = original_storage # Restore
    pass
'''