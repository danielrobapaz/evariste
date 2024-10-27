import os
from langchain_openai import AzureChatOpenAI

llm = AzureChatOpenAI(
  deployment_name="gpt-35-turbo"
)

print(llm.invoke('Give me the contryname, name of the olympic games participant that got mor than 16 gold medals and the olympics where Rio 2016').content)