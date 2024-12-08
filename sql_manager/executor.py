from llm.executor import LLMExecutor
import pandas as pd
import mdpd
import jellyfish
from langchain_openai import AzureChatOpenAI

class Executor:
    def __init__(self) -> None:
        self.model: LLMExecutor = LLMExecutor(
            model=AzureChatOpenAI(
                deployment_name="gpt-35-turbo"
            )
        )
        self.estimation_mode: str = None

    def __cardinality_estimation_prompt(self, table: str) -> str:
        return f'What is the cardinality of the table {table}'
    
    def __index_estimation_prompt(self, table: str) -> str:
        return f'What is the index of the table {table}'
    
    def __sample_estimation_prompt(self, table: str) -> str:
        return f'What is the sample of the table {table}'
    
    def create_estimation_prompt(self, table: str):
        match self.estimation_mode:
            case 'cardinality':
                return self.__cardinality_estimation_prompt(table)

            case 'index':
                return self.__index_estimation_prompt(table)

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