from embeddings_manager.embeddings_manager import Embeddings 
from langchain_community.embeddings.huggingface import HuggingFaceEmbeddings
import os



def main():
    embedding_model_name = os.environ.get('EMBEDDING_MODEL_NAME')
    model_device = os.environ.get('EMBEDDING_MODEL_DEVICE')
    normalize = os.environ.get('EMBEDDING_NORMALIZE', "False") == "True"

    embeddings_model = HuggingFaceEmbeddings(
        model_name=embedding_model_name,
        model_kwargs={'device': model_device},
        encode_kwargs={'normalize': normalize}
    )

    print("Splitting files...")
    
    files_to_split = [f"../documents/{f}" for f in os.listdir("../documents")]
    splitted_files = Embeddings.split_files(files_to_split)
    
    print(f"Creating embedding index...{len(splitted_files)} chunks")
    db = Embeddings.create_faiss_bd(splitted_files, 
                                    embeddings_model, 
                                    "../embeddings_db", 
                                    "olympics_index")
    
if __name__ == "__main__":
    main()