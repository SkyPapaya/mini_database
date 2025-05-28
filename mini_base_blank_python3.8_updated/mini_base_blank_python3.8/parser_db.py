#-----------------------------
# parser_db.py
# author: Jingyu Han   hjymail@163.com
# modified by: Haomin Wang
#-------------------------------
# the module is to construct a syntax tree for a "select from where" SQL clause
# the output is a syntax tree
#----------------------------------------------------
import common_db

# the following two packages need to be installed by yourself
import ply.yacc as yacc
# import ply.lex as lex # Haomin Wang: Not directly used here, tokens are imported

# Haomin Wang: Import tokens from lex_db
from lex_db import tokens


#---------------------------------
# Grammar Rules:
# Query    : SELECT SelList FROM FromList WHERE Cond
# Query    : SELECT SelList FROM FromList  // Haomin Wang: Added for queries without WHERE
#
# SelList  : TCNAME COMMA SelList
# SelList  : TCNAME
# SelList  : ASTERISK // Haomin Wang: Added for SELECT *
#
# FromList : TCNAME COMMA FromList
# FromList : TCNAME
#
# Cond     : SimpleCond AND Cond
# Cond     : SimpleCond OR Cond  // Haomin Wang: Added OR
# Cond     : SimpleCond
# Cond     : LPARENT Cond RPARENT // Haomin Wang: Added for parenthesized conditions
#
# SimpleCond : TCNAME CompareOp Value
#
# CompareOp: EQX | NE | LT | GT | LE | GE // Haomin Wang: Defined comparison operators
#
# Value    : CONSTANT        // For numbers
# Value    : STRING_CONST    // For strings
#---------------------------------

# Haomin Wang: Define operator precedence and associativity for AND, OR if mixing them without parentheses
# This is a simple version, for more complex expressions, precedence rules might be needed.
# For now, assuming left-to-right or requiring parentheses for complex logic.
# No explicit precedence defined here, relying on grammar structure or explicit parentheses.

#------------------------------
# check the syntax tree (placeholder)
# input:
#       syntax tree
# output:
#       true or falise
#-----------------------------
def check_syn_tree(syn_tree):
    if syn_tree:
        # Haomin Wang: Placeholder for actual syntax tree validation if needed
        pass


#------------------------------
#(1) construct the node for query expression
#(2) check the tree
#(3) view the data in the tree
# input:
#       p: parser tokens
# output:
#       the root node of syntax tree
#--------------------------------------
def p_expr_query_where(p): # Haomin Wang: Renamed for clarity
    'Query : SELECT SelList FROM FromList WHERE Cond'

    # Haomin Wang: Creating nodes more explicitly
    select_node = common_db.Node('SELECT_CLAUSE', [p[2]])
    from_node = common_db.Node('FROM_CLAUSE', [p[4]])
    where_node = common_db.Node('WHERE_CLAUSE', [p[6]])

    p[0] = common_db.Node('QUERY', [select_node, from_node, where_node])
    common_db.global_syn_tree = p[0]
    check_syn_tree(common_db.global_syn_tree)
    # common_db.show(common_db.global_syn_tree) # Haomin Wang: Moved display to main_db or a dedicated function if needed

    # return p # Haomin Wang: yacc functions typically don't return p directly, p[0] is the result

# Haomin Wang: Rule for SELECT without WHERE clause
def p_expr_query_no_where(p):
    'Query : SELECT SelList FROM FromList'
    select_node = common_db.Node('SELECT_CLAUSE', [p[2]])
    from_node = common_db.Node('FROM_CLAUSE', [p[4]])
    p[0] = common_db.Node('QUERY', [select_node, from_node])
    common_db.global_syn_tree = p[0]
    check_syn_tree(common_db.global_syn_tree)
    # common_db.show(common_db.global_syn_tree)

#------------------------------
#construct the node for select list
#--------------------------------------
def p_expr_sellist_recursive(p): # Haomin Wang: Renamed
    'SelList : TCNAME COMMA SelList'
    # Haomin Wang: Structure for list: Node('SelList', [Item, Next_SelList_Part])
    item_node = common_db.Node('COLUMN_NAME', [p[1]]) # Storing actual TCNAME value
    p[0] = common_db.Node('SELECT_LIST_MULTIPLE', [item_node, p[3]])

def p_expr_sellist_single(p): # Haomin Wang: Renamed
    'SelList : TCNAME'
    item_node = common_db.Node('COLUMN_NAME', [p[1]])
    p[0] = common_db.Node('SELECT_LIST_SINGLE', [item_node])

# Haomin Wang: Added rule for SELECT *
def p_expr_sellist_all(p):
    'SelList : ASTERISK'
    # Haomin Wang: Create a specific node type for '*' or use a special value.
    # Using a node with value '*' to signify all columns.
    item_node = common_db.Node('ALL_COLUMNS', [p[1]]) # p[1] will be the '*' character
    p[0] = common_db.Node('SELECT_LIST_ALL', [item_node])


#---------------------------
#construct the node for from expression
#--------------------------------------
def p_expr_fromlist_recursive(p): # Haomin Wang: Renamed
    'FromList : TCNAME COMMA FromList'
    item_node = common_db.Node('TABLE_NAME', [p[1]])
    p[0] = common_db.Node('FROM_LIST_MULTIPLE', [item_node, p[3]])

def p_expr_fromlist_single(p): # Haomin Wang: Renamed
    'FromList : TCNAME'
    item_node = common_db.Node('TABLE_NAME', [p[1]])
    p[0] = common_db.Node('FROM_LIST_SINGLE', [item_node])

#------------------------------
#construct the node for condition expression
#--------------------------------------

# Haomin Wang: Added rules for AND/OR and parentheses in conditions
def p_expr_condition_and(p):
    'Cond : SimpleCond AND Cond'
    p[0] = common_db.Node('CONDITION_AND', [p[1], p[3]])

def p_expr_condition_or(p):
    'Cond : SimpleCond OR Cond'
    p[0] = common_db.Node('CONDITION_OR', [p[1], p[3]])

def p_expr_condition_simple(p):
    'Cond : SimpleCond'
    p[0] = p[1] # The SimpleCond itself is the condition node

def p_expr_condition_parentheses(p):
    'Cond : LPARENT Cond RPARENT'
    p[0] = p[2] # The inner condition is the node, parentheses just group

# Haomin Wang: Rule for a simple condition (e.g., field = value)
def p_expr_simple_condition(p):
    'SimpleCond : TCNAME CompareOp Value'
    # p[1] is TCNAME (column), p[2] is CompareOp (operator), p[3] is Value
    column_node = common_db.Node('COLUMN_NAME_COND', [p[1]])
    value_node = p[3] # Value node is already created by p_value rules
    p[0] = common_db.Node('SIMPLE_CONDITION', [column_node, p[2], value_node])

# Haomin Wang: Rule for comparison operators
def p_compare_op(p):
    '''CompareOp : EQX
                 | NE
                 | LT
                 | GT
                 | LE
                 | GE'''
    p[0] = common_db.Node('OPERATOR', [p[1]]) # Store the actual operator token value

# Haomin Wang: Rules for values (constant numbers or strings)
def p_value_constant(p):
    'Value : CONSTANT'
    p[0] = common_db.Node('VALUE_CONST', [p[1]]) # p[1] is the integer value

def p_value_string_constant(p):
    'Value : STRING_CONST'
    p[0] = common_db.Node('VALUE_STRING', [p[1]]) # p[1] is the string value (quotes removed by lexer)

#------------------------------
# for error
#--------------------------------------
def p_error(p):
    # Haomin Wang: Improved error reporting
    if p:
        print(f"Syntax error at token {p.type} ('{p.value}') on line {p.lineno}, position {p.lexpos}")
        # Attempt to find the next statement or a recovery point if more complex recovery is implemented.
        # For now, just report.
    else:
        print("Syntax error at EOF")


#------------------------------------------
# to set the global_parser handle in common_db.py
#---------------------------------------------
def set_handle():
    common_db.global_parser = yacc.yacc(write_tables=0) # write_tables=0 to prevent ply from creating parsetab.py
    if common_db.global_parser is None:
        # Haomin Wang: Changed for Python 3 print
        print('wrong when yacc object is created')

# Haomin Wang: Added for testing the parser
'''
if __name__ == '__main__':
    import lex_db # For tokens and lexer
    lex_db.set_lex_handle() # Create lexer instance
    set_handle() # Create parser instance

    while True:
        try:
            s = input('SQL > ')
        except EOFError:
            break
        if not s: continue
        result = common_db.global_parser.parse(s, lexer=common_db.global_lexer)
        if result:
            print("Parsing successful. Syntax Tree:")
            common_db.show(result)
        else:
            print("Parsing failed.")
'''