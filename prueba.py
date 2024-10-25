import os
from langchain_openai import AzureChatOpenAI

llm = AzureChatOpenAI(
  deployment_name="gpt-35-turbo"
)

print(llm.invoke('Tell me a joke better joke').content)