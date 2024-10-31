import os
from langchain_openai import AzureChatOpenAI

llm = AzureChatOpenAI(
  deployment_name="gpt-35-turbo"
)

print(llm.invoke('Give me the contryname, name of the olympic games participant where the name of the olympics is Rio 2016 and the participant got more than 16 gold medals').content)