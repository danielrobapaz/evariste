from enum import Enum
from sqlglot import Expression
from sql_manager.executor import Executor

class NodeType(Enum):
    SELECT = 'SELECT'
    JOIN = 'JOIN'
    TABLE = 'TABLE'
    HAVING = 'HAVING'
    GROUP_BY = 'GROUP_BY'

OPERATORS = {
    'eq': 'is',
    'is': 'is',
    'in': 'is',
    'neq': 'is not',
    'not': 'is not',
    'not in': 'is not',
    'gt': 'is greater than',
    'gte': 'is greater than or equal to',
    'lt': 'is less than',
    'lte': 'is less than or equal to'
}

tab_character = '\t'

class Node:
    def __init__(self, type: NodeType):
        self.type: NodeType = type
        self.result: any = None

    def execute(self):
        raise NotImplementedError()
    
    def get_dependency_aliases(self):
        raise NotImplementedError()
    
    def show_execution_plan(self, deep: int):
        raise NotImplementedError()
    
    def execute(self, executor: Executor):
        raise NotImplementedError()

    def __str__(self) -> str:
        return str(self.__dict__)

class Select(Node):
    def __init__(self,
                 columns: list[Expression],
                 table: Node):
        super().__init__(NodeType.SELECT)
        self.columns: list[Expression] = columns
        self.table: Node = table

    def show_execution_plan(self, deep: int):
        print(f'{tab_character*deep}Select')
        print(f'{tab_character*deep}Columns: {self.columns}')
        self.table.show_execution_plan(deep+1)
        print(f'{tab_character*deep}End select')
    
    def execute(self, executor: Executor):
        print('Select')
        self.table.execute(executor)

class Table(Node):
    def __init__(self, 
                 table_name: str,
                 table_alias: str,
                 where_condition: list[str],
                 columns: list[str]):
        super().__init__(NodeType.TABLE)
        self.table_name: str = table_name
        self.table_alias: str = table_alias
        self.where_condition: list[Expression] = where_condition
        self.columns: list[Expression] = columns
    
    def __translate_columns(self):
        columns: str = set()

        for column in self.columns:
            columns.add(column.args.get('this').args.get('this'))

        return ', '.join(list(columns))

    def __translate_where(self):
        where: list[str] = []

        for condition in self.where_condition:
            left_side = condition.this.this
            right_side = condition.args.get('expression').this
            operator = OPERATORS.get(condition.key)
            
            if not operator:
                raise Exception(f'Unexpected operator: {condition.key}')
            
            where.append(f'{left_side} {operator} {right_side}')

        return ' and '.join(where)
        
    def execute(self, 
                executor: Executor, 
                join_condition: str = None):
        prompt = executor.create_prompt(self.table_alias,
                                        'hola',
                                        'adios',
                                        'wururu')
        
        columns: str = self.__translate_columns()
        where: str = self.__translate_where()
        
        prompt = executor.create_prompt(self.table_alias, 
                                        columns, 
                                        where,
                                        join_condition)
        
        self.result = executor.execute_prompt(prompt)

    def get_dependency_aliases(self) -> list[str]:
        return [self.table_alias]

    def show_execution_plan(self, deep: int):
        print(f'{tab_character*deep}Table')
        print(f'{tab_character*deep}Name:   {self.table_alias}')
        print(f'{tab_character*deep}Where: {self.where_condition}')
        print(f'{tab_character*deep}End Table')        

class Join(Node):
    def __init__(self,
                 table1: Node,
                 table2: Node,
                 join_condition: str):
        super().__init__(NodeType.JOIN)
        self.table1: Table = table1
        self.table2: Table = table2
        self.join_condition: str = join_condition

    def show_execution_plan(self, deep: int):
        print(f"{tab_character*deep}Join")
        self.table1.show_execution_plan(deep+1)
        self.table2.show_execution_plan(deep+1)
        print(f"{tab_character*deep}Join condition: {self.join_condition}")
        print(f"{tab_character*deep}End join")

    def get_dependency_aliases(self):
        return self.table1.get_dependency_aliases() + self.table2.get_dependency_aliases()

    def execute(self, executor: Executor):
        print('Join')
        self.table1.execute(executor)
        self.table2.execute(executor)