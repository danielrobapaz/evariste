from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_community.embeddings.huggingface import HuggingFaceEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from embeddings_manager.embeddings_manager import Embeddings

class LLMExecutor:
    def __init__(self, model: BaseChatModel):
        self.model: BaseChatModel = model
        self.executions: int = 0
        self.retriever = self.__get_retriever()
        self.base_prompt = self.__get_base_prompt()

    def __get_retriever(self):
        embeddings = HuggingFaceEmbeddings(
            model_name='thenlper/gte-small',
            model_kwargs={'device': 'cpu'},
            encode_kwargs={'normalize_embeddings': False}
        )

        db = Embeddings.load_embeddings("embeddings_manager/embeddings_index", "olympics_index", embeddings)
        
        return db.as_retriever(search_kwargs={"k": 10})

    def __get_base_prompt(self):
        system_prompt=(
            "You are a highly intelligent question answering bot. "
            "You will answer concisely. "
            "Use only the given context to answer the question. "
            "Context: {context}"
            "\n{format_instructions}"
        )

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                ("human", "In the next Markdown table there are the answer ofthe query: {question}")
            ]
        )

        return prompt
    
    def __format_docs(self, docs):
        text: str = '\n\n'.join(doc.page_content for doc in docs)
        return text
    
    def __create_instructions(self, columns: list[str]):
        text: str = 'Instructions: \n'
        text += 'Format the information as a table with columns for '
        
        if len(columns) == 1:
            text += columns[0]
        elif len(columns) > 1:
            text += ', '.join(columns[:-1]) + f' and {columns[-1]}'
        
        text += ' Your response should be a table\n'
        text += 'If your answer is a number like millions or thousands, return the always all its digits using the format used in America. \n'
        text += 'If I ask you a question that is rooted in truth, you will give you the answer.\n'
        text += 'If I ask you a question that is nonsense, trickery, or has no clear answer, you will respond with "Unknown". '
        
        return (lambda *args: text)

    def invoke(self, prompt: str, columns: list[str]):
        self.executions += 1
        
        columns_translation = type(columns)(columns)

        rag_chain = (
            {'context': self.retriever | self.__format_docs, 
            'question': RunnablePassthrough(),
            'format_instructions': self.__create_instructions(columns_translation)
            }
            | self.base_prompt
            | self.model
            | StrOutputParser()
        )

        result = rag_chain.invoke(prompt)
        
        return result