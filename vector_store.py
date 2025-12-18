"""
Vector storage using ChromaDB
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
            print(f"Loaded existing collection: {self.collection_name}")
            return collection
        except:
            collection = self.client.create_collection(
                name=self.collection_name,
                metadata={"description": "Products data embeddings"}
            )
            print(f"Created new collection: {self.collection_name}")
            return collection
    
    def add_documents(self, chunks: List[Dict[str, Any]], batch_size: int = 100):
        """Add documents with embeddings to the vector store
        
        Args:
            chunks: List of chunk dictionaries with 'text', 'embedding', and 'metadata'
            batch_size: Number of documents to add per batch (default: 100)
        """
        total_chunks = len(chunks)
        print(f"Adding {total_chunks} documents to vector store (in batches of {batch_size})...")
        
        # Process in batches to avoid memory issues and improve progress tracking
        for batch_start in range(0, total_chunks, batch_size):
            batch_end = min(batch_start + batch_size, total_chunks)
            batch_chunks = chunks[batch_start:batch_end]
            
            ids = []
            texts = []
            embeddings = []
            metadatas = []
            
            for idx, chunk in enumerate(batch_chunks):
                chunk_idx = batch_start + idx
                ids.append(f"chunk_{chunk_idx}")
                texts.append(chunk['text'])
                embeddings.append(chunk['embedding'])
                metadata = {}
                for key, value in chunk['metadata'].items():
                    if isinstance(value, (str, int, float, bool)):
                        metadata[key] = value
                    else:
                        metadata[key] = str(value)
                metadatas.append(metadata)
            
            # Add batch to collection
            try:
                self.collection.add(
                    ids=ids,
                    embeddings=embeddings,
                    documents=texts,
                    metadatas=metadatas
                )
                print(f"  ✓ Added batch {batch_start//batch_size + 1}/{(total_chunks-1)//batch_size + 1} ({len(batch_chunks)} documents, {batch_end}/{total_chunks} total)")
            except Exception as e:
                print(f"  ⚠ Error adding batch {batch_start//batch_size + 1}: {e}")
                # Continue with next batch even if one fails
                continue
        
        print(f"✓ Completed adding documents to vector store")
    
    def clear_collection(self):
        """Clear all documents from the collection"""
        try:
            self.client.delete_collection(name=self.collection_name)
            self.collection = self._get_or_create_collection()
            print("Collection cleared")
        except Exception as e:
            print(f"Error clearing collection: {e}")
    
    def search(self, query_embedding: List[float], n_results: int = 5, where: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Search for similar documents
        
        Args:
            query_embedding: The embedding vector to search for
            n_results: Number of results to return
            where: Optional metadata filter (e.g., {"user_id": 5})
        """
        query_kwargs = {
            'query_embeddings': [query_embedding],
            'n_results': n_results
        }
        if where:
            query_kwargs['where'] = where
        
        results = self.collection.query(**query_kwargs)
        
        # Format results
        formatted_results = []
        if results['documents'] and len(results['documents'][0]) > 0:
            for i in range(len(results['documents'][0])):
                formatted_results.append({
                    'text': results['documents'][0][i],
                    'metadata': results['metadatas'][0][i] if results['metadatas'] else {},
                    'distance': results['distances'][0][i] if results['distances'] else None
                })
        
        return formatted_results
    
    def get_by_metadata(self, where: Dict, n_results: int = 100) -> List[Dict[str, Any]]:
        """Get documents by metadata filter without embedding search
        
        Args:
            where: Metadata filter dict. For multiple conditions, use $and operator.
                   Example: {"$and": [{"user_id": 5}, {"type": "user"}]}
        """
        # If where has multiple keys, wrap in $and operator
        if len(where) > 1:
            where_clause = {"$and": [{k: v} for k, v in where.items()]}
        else:
            where_clause = where
        
        try:
            results = self.collection.get(
                where=where_clause,
                limit=n_results
            )
        except Exception as e:
            # If query fails try with just the first condition
            if len(where) > 1:
                first_key = list(where.keys())[0]
                results = self.collection.get(
                    where={first_key: where[first_key]},
                    limit=n_results
                )
            else:
                raise e
        
        # Format results
        formatted_results = []
        if results['documents'] and len(results['documents']) > 0:
            for i in range(len(results['documents'])):
                formatted_results.append({
                    'text': results['documents'][i],
                    'metadata': results['metadatas'][i] if results['metadatas'] else {},
                    'distance': None
                })
        
        return formatted_results
    
    def get_collection_count(self) -> int:
        """Get the number of documents in the collection
        
        Note: For large collections, count() can be very slow or hang.
        This method uses a timeout to prevent hanging.
        """
        import threading
        import queue
        
        result_queue = queue.Queue()
        error_queue = queue.Queue()
        
        def count_thread():
            try:
                # This can hang on large collections
                count = self.collection.count()
                result_queue.put(count)
            except Exception as e:
                error_queue.put(e)
        
        # Run count in a separate thread with timeout
        thread = threading.Thread(target=count_thread, daemon=True)
        thread.start()
        thread.join(timeout=2)  # Max 2 seconds wait
        
        if thread.is_alive():
            # Count is taking too long, return -1 to indicate "unknown"
            return -1
        elif not error_queue.empty():
            # Count failed, try to check if collection has data
            try:
                sample = self.collection.peek(limit=1)
                if sample and sample.get('ids'):
                    return -1  # Has data but count unavailable
                return 0
            except:
                return 0
        else:
            # Got the count successfully
            return result_queue.get()

