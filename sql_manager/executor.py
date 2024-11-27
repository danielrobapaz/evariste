
class Executor:
    def create_prompt(self,
                      table: str,
                      columns: str,
                      where_condition: str = None,
                      join_condition: str = None) -> str:
        
        prompt = f"Give me the {columns} of the {table}"

        if where_condition:
            prompt += f" where {where_condition}"

        if join_condition:
            prompt += f" {join_condition}"
        
        return prompt
    
    def execute_prompt(self, prompt: str):
        return 'Executing prompt'