from sql_manager.parser import SQLParser
from sql_manager.execution_plan_nodes import Node, Join, Select
from sqlglot import Expression
from sqlglot.expressions import EQ

class SQLExecutionPlanner(SQLParser):
    def __init__(self, sql: str):
        super().__init__(sql)
        self.execution_plans: list[Node] = []
 
    def create_exeuction_plans(self) -> list[Node]:
        tables: dict[str, Node] = self.get_single_tables()
        total_tables: int = len(tables)
        
        current_plans: list[Node] = list(tables.values())

        for _ in range(total_tables - 1):
            new_plans = []
            for plan in current_plans:
                dependency_aliases = plan.get_dependency_aliases()  

                possible_joins = self.get_posible_join(plan, tables)
                
                for join in possible_joins:
                    current_join_condition = self.get_join_condition(join, dependency_aliases)
                    
                    if current_join_condition.this.table in dependency_aliases:
                        join = Join(plan, tables[join], current_join_condition)
                    
                    else:
                        reversed_join_condition = EQ(
                            this=current_join_condition.args.get('expression'),
                            expression=current_join_condition.this
                        )

                        join = Join(plan, tables[join], reversed_join_condition)

                    new_plans.append(join)

            current_plans = new_plans

        select_condition = self.get_select_expressions()
        
        for i in range(len(current_plans)):
            current_plans[i] = Select(select_condition, current_plans[i]) 

        print(f'Total plans: {len(current_plans)}')
        self.execution_plans = current_plans

    def show_execution_plans(self):
        for plan in self.execution_plans:
            plan.show_execution_plan(0)
            print('-------')