from sql_parser_manager.sql_parser import SQLParser
from sql_parser_manager.execution_plan_nodes import Table, Node, Join
from sqlglot import Expression

class SQLExecutionPlanner(SQLParser):
    def __init__(self, sql: str):
        super().__init__(sql)
        self.execution_plans: list[Node] = []
 
    def create_exeuction_plans(self) -> list[Node]:
        join_conditions: list[Expression] = self.get_join_conditions()
        tables: dict[str, Node] = self.get_single_tables()
        total_tables: int = len(tables) - 1
        
        current_plans: list[Node] = list(tables.values())

        for i in range(total_tables):
            new_plans = []
            for plan in current_plans:
                dependency_aliases = plan.get_dependency_aliases()
                tables_can_join = self.get_join_tables(dependency_aliases)
                
                missing_tables = [table_alias for table_alias in tables.keys() 
                                  if table_alias not in dependency_aliases]
                
                possible_joins = [table for table in tables_can_join
                                       if table in missing_tables]
                
                
                for join in possible_joins:
                    new_plans.append(Join(plan, tables[join], None))

            current_plans = new_plans

        for plan in current_plans:
            plan.show_execution_plan()
            print('------------------')

