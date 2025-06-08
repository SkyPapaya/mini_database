#-------------------------------
# lex_db.py
# author: Jingyu Han hjymail@163.com
# modified by: Haomin Wang
#--------------------------------------------
# the module is responsible for
#(1) defining tokens used for parsing SQL statements
#(2) constructing a lex object
#-------------------------------
import ply.lex as lex
import common_db

# Haomin Wang: Added LPARENT, RPARENT for potential future use (e.g. complex conditions)
# Haomin Wang: Added more specific operator tokens like NE, LT, GT, LE, GE for richer comparisons
# Haomin Wang: Added ASTERISK for SELECT *
tokens=('SELECT','FROM','WHERE','AND', 'OR', # Haomin Wang: Added OR
        'TCNAME','EQX', 'NE', 'LT', 'GT', 'LE', 'GE', # Haomin Wang: Added comparison operators
        'COMMA','CONSTANT','SPACE',
        'LPARENT', 'RPARENT', # Haomin Wang: Added parentheses
        'STRING_CONST', # Haomin Wang: Added specific string constant
        'ASTERISK' # Haomin Wang: Added token for *
        )

# the following is to defining rules for each token
def t_SELECT(t):
    r'select' # Haomin Wang: Made case-insensitive
    t.type = 'SELECT' # Haomin Wang: Ensure type is uppercase for consistency with token list
    return t

def t_FROM(t):
    r'from' # Haomin Wang: Made case-insensitive
    t.type = 'FROM'
    return t

def t_WHERE(t):
    r'where' # Haomin Wang: Made case-insensitive
    t.type = 'WHERE'
    return t

def t_AND(t):
    r'and' # Haomin Wang: Made case-insensitive
    t.type = 'AND'
    return t

# Haomin Wang: Added OR token
def t_OR(t):
    r'or' # Haomin Wang: Made case-insensitive
    t.type = 'OR'
    return t

# Haomin Wang: Rule for ASTERISK token *
def t_ASTERISK(t):
    r'\*'
    return t

# Haomin Wang: General identifier, could be table name or column name. Parser will differentiate.
def t_TCNAME(t):
    r'[a-zA-Z_][a-zA-Z_0-9]*' # Haomin Wang: More standard identifier rule
    # Haomin Wang: Check for keywords, this is important if keywords can also be identifiers.
    # For this project, assuming SELECT, FROM, WHERE, AND, OR are reserved.
    # If not, additional logic would be needed here.
    # t.type = reserved.get(t.value.lower(),'TCNAME') # Example if there was a reserved map
    return t

def t_COMMA(t):
    r','
    return t

# Haomin Wang: Expanded comparison operators
def t_EQX(t):
    r'='
    t.type = 'EQX' # Haomin Wang: Standardized to EQX as in original tokens
    return t

def t_NE(t):
    r'!='
    return t

def t_LE(t):
    r'<='
    return t

def t_GE(t):
    r'>='
    return t

def t_LT(t):
    r'<'
    return t

def t_GT(t):
    r'>'
    return t

# Haomin Wang: Modified CONSTANT to be more specific for numbers. Strings are handled by STRING_CONST.
def t_CONSTANT(t):
    r'\d+' # Haomin Wang: For integers
    t.value = int(t.value) # Haomin Wang: Convert to int
    return t

# Haomin Wang: Added specific token for string constants
def t_STRING_CONST(t):
    r'\'[^\']*\'' # Haomin Wang: Matches content within single quotes
    t.value = t.value[1:-1] # Haomin Wang: Remove the quotes
    return t

def t_LPARENT(t):
    r'\('
    return t

def t_RPARENT(t):
    r'\)'
    return t

def t_SPACE(t):
    r'\s+'
    pass # Haomin Wang: Ignore whitespace

#--------------------------
# to cope with the error
#------------------------

def t_error(t):
    # Haomin Wang: Improved error reporting
    print(f"Illegal character '{t.value[0]}' at position {t.lexpos}")
    t.lexer.skip(1)


#------------------------------------------
# to set the global_lexer in common_db.py
#-------------------------------------------
def set_lex_handle():
    common_db.global_lexer=lex.lex()
    if common_db.global_lexer is None:
        # Haomin Wang: Changed print for Python 3
        print('wrong when the global_lex is created')


'''
# Haomin Wang: Updated test for Python 3 and new tokens
def test():
    my_lexer=lex.lex()
    my_lexer.input("select f1,f2 from GOOD where f1='xx' and f2=5 OR f3 < 10")
    while True:
        temp_tok=my_lexer.token()
        if temp_tok is None:
            break
        print(temp_tok) # Haomin Wang: Changed for Python 3 print

if __name__ == '__main__': # Haomin Wang: To allow running test()
    test()
'''