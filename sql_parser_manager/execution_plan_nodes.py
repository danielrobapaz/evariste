from enum import Enum
from sqlglot import Expression

class NodeType(Enum):
    SELECT = 'SELECT'
    JOIN = 'JOIN'
    TABLE = 'TABLE'
    HAVING = 'HAVING'
    GROUP_BY = 'GROUP_BY'


class Node:
    def __init__(self, type: NodeType):
        self.type: NodeType = type
        self.result: any = None

    def execute(self):
        raise NotImplementedError()
    
    def get_dependency_aliases(self):
        raise NotImplementedError()
    
    def __str__(self) -> str:
        return str(self.__dict__)

class Select(Node):
    def __init__(self,
                 columns: list[Expression]):
        super().__init__(NodeType.SELECT)
        self.columns: list[Expression] = columns
        
class Table(Node):
    def __init__(self, 
                 table_name: str,
                 table_alias: str,
                 where_condition: list[str],
                 columns: list[str]):
        super().__init__(NodeType.TABLE)
        self.table_name: str = table_name
        self.table_alias: str = table_alias
        self.where_condition: str = where_condition
        self.columns: list[str] = columns

    def get_dependency_aliases(self) -> list[str]:
        return [self.table_alias]

    def show_execution_plan(self):
        print('Table')
        print(self.table_alias)
        print(f'Where: {self.where_condition}')
        print('End Table')        

class Join(Node):
    def __init__(self,
                 table1: Node,
                 table2: Node,
                 join_condition: str):
        super().__init__(NodeType.JOIN)
        self.table1: Table = table1
        self.table2: Table = table2
        self.join_condition: str = join_condition

    def show_execution_plan(self):
        print(f"Join")
        self.table1.show_execution_plan()
        self.table2.show_execution_plan()
        print(f"Join condition: {self.join_condition}")
        print(f"End join")

    def get_dependency_aliases(self):
        return self.table1.get_dependency_aliases() + self.table2.get_dependency_aliases()