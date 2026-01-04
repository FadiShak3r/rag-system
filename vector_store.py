"""
Vector storage using ChromaDB - Simple and reliable
"""
import chromadb
from chromadb.config import Settings
from typing import List, Dict, Any, Optional
from config import CHROMA_DB_PATH


class VectorStore:
    """Manages vector storage using ChromaDB"""
    
    def __init__(self, collection_name: str = "products_data"):
        self.client = chromadb.PersistentClient(
            path=CHROMA_DB_PATH,
            settings=Settings(anonymized_telemetry=False)
        )
        self.collection_name = collection_name
        self.collection = self._get_or_create_collection()
    
    def _get_or_create_collection(self):
        """Get existing collection or create new one"""
        try:
            collection = self.client.get_collection(name=self.collection_name)
            print(f"  - Loaded collection: {self.collection_name}")
            return collection
        except:
            collection = self.client.create_collection(
                name=self.collection_name,
                metadata={"description": "Product database embeddings"}
            )
            print(f"  - Created new collection: {self.collection_name}")
            return collection
    
    def add_documents(self, documents: List[Dict[str, Any]], batch_size: int = 100):
        """Add documents with embeddings to the vector store"""
        total = len(documents)
        print(f"Adding {total} documents to vector store...")
        
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch = documents[batch_start:batch_end]
            
            ids = []
            texts = []
            embeddings = []
            metadatas = []
            
            for idx, doc in enumerate(batch):
                doc_idx = batch_start + idx
                ids.append(f"doc_{doc_idx}")
                texts.append(doc['text'])
                embeddings.append(doc['embedding'])
                
                # Clean metadata - ChromaDB only accepts str, int, float, bool
                metadata = {}
                for key, value in doc['metadata'].items():
                    if isinstance(value, (str, int, float, bool)):
                        metadata[key] = value
                    elif value is not None:
                        metadata[key] = str(value)
                metadatas.append(metadata)
            
            try:
                self.collection.add(
                    ids=ids,
                    embeddings=embeddings,
                    documents=texts,
                    metadatas=metadatas
                )
                batch_num = (batch_start // batch_size) + 1
                total_batches = (total - 1) // batch_size + 1
                print(f"  ✓ Batch {batch_num}/{total_batches} ({batch_end}/{total} documents)")
            except Exception as e:
                print(f"  ⚠ Error in batch: {e}")
                continue
        
        print(f"✓ Added all documents to vector store")
    
    def clear_collection(self):
        """Clear all documents from the collection"""
        try:
            self.client.delete_collection(name=self.collection_name)
            self.collection = self._get_or_create_collection()
            print("  - Collection cleared")
        except Exception as e:
            print(f"  - Error clearing collection: {e}")
    
    def search(self, query_embedding: List[float], n_results: int = 10) -> List[Dict[str, Any]]:
        """Search for similar documents"""
        try:
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results
            )
            
            formatted = []
            if results['documents'] and len(results['documents'][0]) > 0:
                for i in range(len(results['documents'][0])):
                    formatted.append({
                        'text': results['documents'][0][i],
                        'metadata': results['metadatas'][0][i] if results['metadatas'] else {},
                        'distance': results['distances'][0][i] if results['distances'] else None
                    })
            
            return formatted
        except Exception as e:
            print(f"Search error: {e}")
            return []
    
    def get_by_metadata(self, where: Dict, n_results: int = 100) -> List[Dict[str, Any]]:
        """Get documents by metadata filter"""
        try:
            results = self.collection.get(
                where=where,
                limit=n_results
            )
            
            formatted = []
            if results.get('documents') and len(results['documents']) > 0:
                for i in range(len(results['documents'])):
                    formatted.append({
                        'text': results['documents'][i],
                        'metadata': results['metadatas'][i] if results.get('metadatas') else {},
                        'distance': None
                    })
            
            return formatted
        except Exception as e:
            print(f"Metadata query error: {e}")
            return []
    
    def get_collection_count(self) -> int:
        """Get the number of documents in the collection"""
        try:
            return self.collection.count()
        except:
            return -1
