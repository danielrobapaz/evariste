from enum import Enum
from sqlglot import Expression
from sql_manager.executor import Executor
import pandas as pd
from dataclasses import dataclass

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

@dataclass
class Estimation:
    cardinality = 0
    reduction_factor = 1
    number_of_keys_join = 0

class Node:
    def __init__(self, type: NodeType):
        self.type: NodeType = type
        self.result: pd.DataFrame = pd.DataFrame()
        self.estimation: Estimation = Estimation()
    
    def get_dependency_aliases(self):
        raise NotImplementedError()
    
    def show_execution_plan(self, deep: int = 0):
        raise NotImplementedError()
    
    def execute(self, executor: Executor):
        raise NotImplementedError()

    def estimate(
            self, 
            type: str, 
            executor: Executor, 
            use_static_data: bool = False):
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

    def show_execution_plan(self, deep: int = 0):
        print(f'{tab_character*deep}Select')
        print(f'{tab_character*deep}Columns: {self.columns}')
        self.table.show_execution_plan(deep+1)
        print(f'{tab_character*deep}End select')
    
    def estimate(self, type: str, executor: Executor, use_static_data: bool = False):
        if type == 'sample' and use_static_data:
            raise Exception('Sample estimation does not support static data')
        
        executor.estimation_mode = type
        self.table.estimate(type, executor, use_static_data)
        self.estimation = self.table.estimation

    def execute(self, executor: Executor):
        self.table.execute(executor)
        self.result = self.table.result

        columns_names = [column.this.this for column in self.columns]
        
        if len(self.result) > 0:
            self.result = self.result[columns_names]
        
        print(self.result)

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
    
    def __cardinality_estimation(self, executor: Executor, use_static_data: bool = False):
        table_cardinality, reduction_factor = executor.create_estimation_prompt(self.table_alias, use_static_data)

        self.estimation.cardinality = table_cardinality
        self.estimation.reduction_factor = reduction_factor
        self.estimation.estimated_cardinality = table_cardinality * reduction_factor
        
    def __sample_estimation(self, executor: Executor):
        raise NotImplementedError()
    
    def estimate(self, type, executor, use_static_data: bool = False):
        match type:
            case 'cardinality':
                self.__cardinality_estimation(executor, use_static_data)
            
            case 'sample':
                self.__sample_estimation(executor)
            
            case _:
                raise Exception(f'Unknown estimation type: {type}')

    def __translate_columns(self) -> list[str]:
        columns: str = set()

        for column in self.columns:
            columns.add(column.args.get('this').args.get('this'))

        return list(columns)

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
        
        self.result = executor.execute_prompt(prompt, columns)
        
        print(prompt)
        print('\n')
    
    def get_dependency_aliases(self) -> list[str]:
        return [self.table_alias]

    def show_execution_plan(self, deep: int = 0):
        print(f'{tab_character*deep}Table')
        print(f'{tab_character*deep}Name:   {self.table_alias}')
        print(f'{tab_character*deep}Columns:   {self.columns}')
        print(f'{tab_character*deep}Where: {self.where_condition}')
        print(f'{tab_character*deep}End Table')        

class Join(Node):
    def __init__(self,
                 table1: Node,
                 table2: Node,
                 join_condition: Expression):
        super().__init__(NodeType.JOIN)
        self.table1: Table = table1
        self.table2: Table = table2
        self.join_condition: str = join_condition

    def show_execution_plan(self, deep: int = 0):
        print(f"{tab_character*deep}Join")
        self.table1.show_execution_plan(deep+1)
        self.table2.show_execution_plan(deep+1)
        print(f"{tab_character*deep}Join condition: {self.join_condition}")
        print(f"{tab_character*deep}End join")

    def get_dependency_aliases(self):
        return self.table1.get_dependency_aliases() + self.table2.get_dependency_aliases()

    def __translate_join_condition(self):
        translate_join: str = ""

        left_column = self.join_condition.this.this.this
        right_column = self.join_condition.args.get("expression").this

        if len(self.table1.result) > 0:
            foreign_values = self.table1.result[left_column].unique()

            translate_join += f" where {right_column} in ({', '.join(foreign_values)})"

        return None if translate_join == "" else translate_join
    
    def __join_result(self):
        merged_result = pd.DataFrame()

        if len(self.table1.result) > 0 and len(self.table2.result) > 0:
            merged_result = pd.merge(
                self.table1.result,
                self.table2.result,
                left_on=self.join_condition.this.this.this,
                right_on=self.join_condition.args.get("expression").this.this,
                how='inner')

        self.result = merged_result
    
    def __get_number_of_keys(self, executor: Executor, use_static_data: bool = False):
        left_table = self.join_condition.this.table
        left_column = self.join_condition.this.this.this

        right_table = self.join_condition.args.get("expression").table
        right_column = self.join_condition.args.get("expression").this.this


        return executor.get_number_of_keys(left_table, left_column, right_table, right_column, use_static_data)

    def estimate(self, type, executor, use_static_data: bool = False):
        self.table1.estimate(type, executor, use_static_data)
        self.table2.estimate(type, executor, use_static_data)

        number_of_keys_left_side, number_of_keys_right_side = self.__get_number_of_keys(executor, use_static_data)
        
        result_size_left_side = self.table1.estimation.estimated_cardinality
        result_size_right_side = self.table2.estimation.estimated_cardinality

        self.estimation.cardinality = result_size_left_side * result_size_right_side
        self.estimation.reduction_factor = 1/max(number_of_keys_left_side, number_of_keys_right_side)
        self.estimation.estimated_cardinality = self.estimation.cardinality * self.estimation.cardinality

        print(self.join_condition)
        print(number_of_keys_left_side, number_of_keys_right_side)
        print(self.estimation.__dict__)
        

    def execute(self, executor: Executor):
        self.table1.execute(executor)
        join_condition = self.__translate_join_condition()
        self.table2.execute(executor, join_condition)
        self.__join_result()
