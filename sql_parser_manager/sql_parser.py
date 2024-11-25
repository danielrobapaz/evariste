from sql_parser_manager.execution_plan_nodes import Table, Node
from sqlglot import parse_one, Expression

class SQLParser:
    def __init__(self, sql: str):
        self.ast: Expression = parse_one(sql)
        self.join_conditions = None

    def parse_from_tables(self) -> Table:
        from_tables: Expression = self.ast.args.get('from')

        if not from_tables:
            raise Exception("No FROM statement")
        
        table_name: str = from_tables.this.this.this
        table_alias: str = from_tables.this.alias

        ast_where_conditions: dict = self.get_where_conditions().get('conditions', [])
        
        where_condition: list[str] = [condition for condition in ast_where_conditions
                                      if condition.this.table == table_alias]

        ast_select_expressions = self.ast.args.get('expressions', [])
        columns: list[str] = [column for column in ast_select_expressions
                              if column.table == table_alias]

        return Table(table_name, table_alias, where_condition, columns)
    
    def parse_join_tables(self) -> list[Table]:
        joins = self.ast.args.get('joins')
        
        join_tables = []

        for join in joins:
            table_name: str = join.this.this.this
            table_alias: str = join.this.alias

            ast_where_conditions: dict = self.get_where_conditions().get('conditions', [])
            where_condition: list[str] = [condition for condition in ast_where_conditions
                                          if condition.this.table == table_alias]

            ast_select_expressions = join.args.get('expressions', [])
            columns: list[str] = [column for column in ast_select_expressions
                                  if column.table == table_alias] 

            join_tables.append(Table(table_name, table_alias, where_condition, columns))

        return join_tables
    
    def __get_conditions(self, 
                       initial_conector: Expression,
                       conector_type: str = 'and') -> list[Expression]:
        
        conectors = [initial_conector]
        conditions = []

        while conectors != []:
            conector_actual = conectors.pop(0)

            if conector_actual.key != conector_type:
                conditions.append(conector_actual)
                break

            if conector_actual.this.key != conector_type:
                conditions.append(conector_actual.this)
            else:
                conectors.append(conector_actual.this)

            if conector_actual.args['expression'].key != conector_type:
                conditions.append(conector_actual.args['expression'])

        return conditions

    def get_where_conditions(self) -> list[Expression]:
        if not self.ast.args.get('where'):
            raise Exception(
                f'Missin where clause.')

        all_conditions = self.__get_conditions(self.ast.args['where'].this)

        or_conditions = []
        conditions = []

        for i in range(len(all_conditions)):
            if all_conditions[i].key == 'or' or (all_conditions[i].key == 'paren' and all_conditions[i].this.key == 'or'):
                or_conditions.append(all_conditions[i])
            else:
                conditions.append(all_conditions[i])

        return {'conditions': conditions, 'conditions or': or_conditions}

    def get_join_conditions(self) -> list[Expression]:
        if self.join_conditions:
            return self.join_conditions
        
        joins = self.ast.args.get('joins', [])
        
        join_conditions = []
        for join in joins:
            current_join_condition = join.args.get('on', None)
            if not current_join_condition:
                raise Exception("No JOIN condition")
            
            join_conditions.append(current_join_condition)

        self.join_conditions = join_conditions
        return join_conditions
    
    def get_single_tables(self) -> dict[str, Node]:
        
        tables: dict[str, Node] = {
            table.table_alias: table 
            for table in [self.parse_from_tables()] + self.parse_join_tables()
        }

        return tables
    
    def get_join_tables(self, tables_aliases: list[str]) -> list[str]:
        join_conditions = self.get_join_conditions()

        join_tables = []
        for join in join_conditions:
            current_table_name: str = join.this.table
            current_table_on_name: str = join.args.get('expression').table

            join_tables.append((current_table_name, current_table_on_name))

        posible_joins = [table for table in join_tables
                        if (table[0] in tables_aliases or table[1] in tables_aliases)]
        
        posible_joins = [table[0] for table in posible_joins] + [table[1] for table in posible_joins]

        return [table for table in posible_joins
                if table not in tables_aliases]
    
    def get_posible_join(self, table: Node, all_tables: dict[str, Node]) -> list[Node]:
        dependency_aliases = table.get_dependency_aliases()
        tables_can_join = self.get_join_tables(dependency_aliases)
        missing_tables = [table_alias for table_alias in all_tables.keys() 
                          if table_alias not in dependency_aliases]
        
        return [table for table in tables_can_join
                if table in missing_tables]
    
    def get_join_condition(self, join: str, aliases: list[str]) -> Expression:
        join_conditions = self.get_join_conditions()
        posible_join_conditions = [join_condition for join_condition in join_conditions
                                   if (join_condition.this.table == join or join_condition.args.get('expression').table == join)]
        condition_to_join = [join_condition for join_condition in posible_join_conditions
                              if join_condition.this.table in aliases \
                                 or join_condition.args.get('expression').table in aliases][0]
        
        return condition_to_join
    
    def get_select_expressions(self) -> list[Expression]:
        return self.ast.args.get('expressions', [])