from llm.executor import LLMExecutor
import pandas as pd
import mdpd
import jellyfish
from langchain_openai import AzureChatOpenAI
import json

class Executor:
    def __init__(self) -> None:
        # self.model: LLMExecutor = LLMExecutor(
        #     model=AzureChatOpenAI(
        #         deployment_name="gpt-35-turbo"
        #     )
        # )
        self.estimation_file = self.__read_static_data()
        self.estimation_mode: str = None

    def __read_static_data(self) -> dict:
        with open('sql_manager/estimation.json', 'r') as file:
            return json.load(file)
        
    def __cardinality_estimation_prompt(
        self, 
        table: str,
        use_static_data: bool = False) -> str:
        table_cardinality = 0
        where_reduction_factor = 1
        
        if use_static_data:
            table_data = self.estimation_file.get(table, None)

            if not table_data:
                raise Exception(f'Table {table} not found in the estimation file')
            
            table_cardinality = table_data.get('cardinality', table_cardinality)
            where_reduction_factor = table_data.get('where_reduction_factor', where_reduction_factor)
        
        else:
            raise NotImplementedError()
        
        return table_cardinality, where_reduction_factor
        
    def __sample_estimation_prompt(self, table: str) -> str:
        return f'What is the sample of the table {table}'
    
    def get_number_of_keys(
        self,
        left_table: str,
        left_column: str,
        right_table: str,
        right_column: str,
        use_static_data: bool = False) -> tuple[int, int]:
        
        number_of_keys_left = 1
        number_of_keys_right = 1

        if use_static_data:
            left_table_data = self.estimation_file.get(left_table, None)
            right_table_data = self.estimation_file.get(right_table, None)
            
            if not left_table_data:
                raise Exception(f'Table {left_table} not found in the estimation file')

            if not right_table_data:
                raise Exception(f'Table {right_table} not found in the estimation file')

            number_of_keys_left = left_table_data['join'].get(left_column, number_of_keys_left)
            number_of_keys_right = right_table_data['join'].get(right_column, number_of_keys_right)

        else:
            raise NotImplementedError()

        return number_of_keys_left, number_of_keys_right            
            
    def create_estimation_prompt(
            self, 
            table: str,
            use_static_data: bool = False) -> str:
        match self.estimation_mode:
            case 'cardinality':
                return self.__cardinality_estimation_prompt(table, use_static_data)

            case 'sample':
                return self.__sample_estimation_prompt(table)

            case _:
                raise Exception(f'Unknown estimation mode: {self.estimation_mode}')
    
    def execute_estimation_prompt(self, prompt: str) -> int | pd.DataFrame:
        raise NotImplementedError()
    
    def create_prompt(self,
                      table: str,
                      columns: list[str],
                      where_condition: str = None,
                      join_condition: str = None) -> str:
        
        prompt = f"Give me the {', '.join(columns)} of the {table}"

        if where_condition:
            prompt += f" where {where_condition}"

        if join_condition:
            prompt += f" {join_condition}"
        
        return prompt
    
    def execute_prompt(self, 
                       prompt: str, 
                       columns: list[str]) -> pd.DataFrame:
        
        clean_result = self.model.invoke(prompt, columns)

        try:
            df = mdpd.from_md(clean_result)

        except:
            df = pd.DataFrame()

        if len(df) > 0:
            assignments = {}
            to_assign = list(df.columns)
            to_assign = [x for x in to_assign if type(x) != int]

            while len(to_assign) > 0:
                column_to_assign = to_assign.pop(0)
                sort_key = lambda x: jellyfish.jaro_similarity(x, column_to_assign)
                possible_assignments = sorted(columns, key=sort_key, reverse=True)

                for assignment in possible_assignments:
                    if assignments.get(assignment):
                        assigned_column = assignments.get(assignment)

                        if jellyfish.jaro_similarity(column_to_assign, assignment) > jellyfish.jaro_similarity(assigned_column, assignment):
                            to_assign.append(assigned_column)
                            assignments[assignment] = column_to_assign
                    else:
                        assignments[assignment] = column_to_assign

            new_df = pd.DataFrame()

            df = df.loc[:, ~df.columns.duplicated()]
            
            for column in assignments:
                new_df.insert(len(new_df.columns), column, df[assignments[column]])

            df = new_df
        else:
            pass

        return df