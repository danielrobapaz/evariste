from langchain_community.vectorstores import FAISS
from langchain_core.documents.base import Document
from langchain_text_splitters import TokenTextSplitter
from langchain_community.document_loaders import TextLoader
from langchain_community.embeddings.huggingface import HuggingFaceEmbeddings

class Embeddings:
    @staticmethod
    def split_files(archivos: list[str],
                    chunk_size:int = 512,
                    chunk_overlap: int = 20) -> list[Document]:
        docs = []
        for archivo in archivos:
            loader = TextLoader(archivo)
            documents = loader.load()
            text_splitter = TokenTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

            docs += text_splitter.split_documents(documents)
        
        return docs

    @staticmethod
    def create_faiss_bd(docs:list[Document], 
                        embeddings: HuggingFaceEmbeddings,
                        folder_path: str,
                        index_name: str):
        db = FAISS.from_documents(docs, embeddings)
        db.save_local(folder_path=folder_path, index_name=index_name)

        return db

    @staticmethod
    def find_related_text(db: FAISS, 
                          query: str,
                          threshold: float,
                          k: int) -> tuple[Document, float]:
        searchDocs = []
        
        for doc, score in db.similarity_search_with_score(query,k):
            if score <= threshold:
                searchDocs.append(doc)
        
        return searchDocs

    def load_embeddings(folder_path: str,
                        index_name: str,
                        embeddings: HuggingFaceEmbeddings):
        
        return FAISS.load_local(folder_path = folder_path, 
                                index_name = index_name, 
                                embeddings = embeddings,
                                allow_dangerous_deserialization = True)