from sql_parser_manager.sql_parser import SQLParser
from sql_parser_manager.execution_plan_nodes import Node, Join
from sqlglot import Expression
from sqlglot.expressions import EQ

class SQLExecutionPlanner(SQLParser):
    def __init__(self, sql: str):
        super().__init__(sql)
        self.execution_plans: list[Node] = []
 
    def create_exeuction_plans(self) -> list[Node]:
        join_conditions: list[Expression] = self.get_join_conditions()
        tables: dict[str, Node] = self.get_single_tables()
        total_tables: int = len(tables)
        
        current_plans: list[Node] = list(tables.values())

        for _ in range(total_tables - 1):
            new_plans = []
            for plan in current_plans:
                dependency_aliases = plan.get_dependency_aliases()
                tables_can_join = self.get_join_tables(dependency_aliases)
                
                missing_tables = [table_alias for table_alias in tables.keys() 
                                  if table_alias not in dependency_aliases]
                
                possible_joins = [table for table in tables_can_join
                                       if table in missing_tables]
                
                
                for join in possible_joins:
                    posible_join_condition = [join_condition for join_condition in join_conditions
                                              if (join_condition.this.table == join or join_condition.args.get('expression').table == join)]
                    
                    condition_to_join = [join_condition for join_condition in posible_join_condition
                                         if join_condition.this.table in dependency_aliases \
                                            or join_condition.args.get('expression').table in dependency_aliases][0]
                    
                    if condition_to_join.this.table in dependency_aliases:
                        join = Join(plan, tables[join], condition_to_join)
                    
                    else:
                        reversed_join_condition = EQ(
                            this=condition_to_join.args.get('expression'),
                            expression=condition_to_join.this
                        )

                        join = Join(plan, tables[join], reversed_join_condition)

                    new_plans.append(join)

            current_plans = new_plans

        for plan in current_plans:
             plan.show_execution_plan()
             print('------------------')
