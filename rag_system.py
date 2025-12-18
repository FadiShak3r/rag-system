"""
RAG (Retrieval-Augmented Generation) system for querying
"""
from openai import OpenAI
from typing import List, Dict, Any
from config import OPENAI_API_KEY, OPENAI_CHAT_MODEL, OPENAI_EMBEDDING_MODEL
from embedding import EmbeddingGenerator
from vector_store import VectorStore


class RAGSystem:
    """Main RAG system for question answering"""
    
    def __init__(self, collection_name: str = "products_data"):
        if not OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        print("  - Initializing OpenAI client...")
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.chat_model = OPENAI_CHAT_MODEL
        print("  - Creating EmbeddingGenerator...")
        self.embedding_generator = EmbeddingGenerator()
        print("  - Creating VectorStore...")
        self.vector_store = VectorStore(collection_name=collection_name)
        print("  - RAGSystem initialization complete")
    
    def query(self, question: str, n_results: int = 10) -> str:
        """Query the RAG system with a question"""
        # Generate embedding for the question
        question_embedding = self.embedding_generator.generate_embedding(question)
        
        # For aggregation queries, retrieve more results
        question_lower = question.lower()
        is_aggregation_query = any(word in question_lower for word in [
            'total', 'sum', 'all', 'top', 'highest', 'lowest', 'average', 'most', 'least'
        ])
        
        if is_aggregation_query:
            # Try to get collection count, but use a safe default if it fails or is slow
            try:
                collection_count = self.vector_store.get_collection_count()
                if collection_count > 0:
                    n_results = min(50, collection_count)
                else:
                    n_results = 50  # Default for aggregation queries
            except:
                n_results = 50  # Default if count fails
        
        # Retrieve relevant chunks
        relevant_chunks = self.vector_store.search(question_embedding, n_results=n_results)
        
        if not relevant_chunks:
            return "I couldn't find any relevant information in the database to answer your question."
        
        user_ids_to_fetch = set()
        review_chunks = [c for c in relevant_chunks if c['metadata'].get('type') == 'review']
        
        question_lower = question.lower()
        is_user_question = any(word in question_lower for word in ['user', 'name', 'who', 'username', 'full name'])
        
        for chunk in review_chunks:
            user_id = chunk['metadata'].get('user_id')
            if user_id is not None and user_id != '':
                try:
                    user_id = int(user_id)
                    user_ids_to_fetch.add(user_id)
                except (ValueError, TypeError):
                    pass
        
        # If we found reviews with user IDs and it's a user-related question, fetch user records
        if user_ids_to_fetch and review_chunks and is_user_question:
            existing_user_ids = set()
            for c in relevant_chunks:
                if c['metadata'].get('type') == 'user':
                    uid = c['metadata'].get('user_id')
                    if uid is not None:
                        try:
                            existing_user_ids.add(int(uid))
                        except (ValueError, TypeError):
                            pass
            
            for user_id in user_ids_to_fetch:
                if user_id not in existing_user_ids:
                    try:
                        # Try to fetch user by user_id only first (more reliable)
                        user_chunks = self.vector_store.get_by_metadata(
                            where={"user_id": user_id},
                            n_results=10
                        )
                        # Filter to only user type chunks
                        user_chunks = [c for c in user_chunks if c['metadata'].get('type') == 'user']
                        if user_chunks:
                            relevant_chunks.extend(user_chunks)
                    except Exception as e:
                        # If metadata query fails, skip it - we already have user info in review docs
                        pass
        
        # Prioritize summary document if it exists
        summary_chunks = [c for c in relevant_chunks if c['metadata'].get('type') == 'summary']
        product_chunks = [c for c in relevant_chunks if c['metadata'].get('type') == 'product']
        user_chunks = [c for c in relevant_chunks if c['metadata'].get('type') == 'user']
        review_chunks = [c for c in relevant_chunks if c['metadata'].get('type') == 'review']
        
        # Reorder based on question type
        if is_user_question and review_chunks:
            # For user questions, prioritize review chunks (which have user info) and user chunks
            ordered_chunks = review_chunks + user_chunks + summary_chunks + product_chunks[:n_results]
        else:
            # For other questions, summary first, then reviews, then users, then products
            ordered_chunks = summary_chunks + review_chunks + user_chunks + product_chunks[:n_results]
        
        # Build context from retrieved chunks
        context_parts = []
        for chunk in ordered_chunks:
            context_parts.append(chunk['text'])
        context = "\n\n".join(context_parts)
        
        # Create prompt for OpenAI with balanced conciseness
        prompt = f"""Answer the question using the context below. Provide a concise answer with relevant context, but keep it brief and focused.

Context:
{context}

Question: {question}

Answer:"""
        
        # Generate answer using OpenAI
        try:
            response = self.client.chat.completions.create(
                model=self.chat_model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant. Give concise but informative answers. Include relevant context and exact numbers, but avoid unnecessary verbosity. Be clear and direct."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500 
            )
            
            answer = response.choices[0].message.content.strip()
            
            return answer
            
        except Exception as e:
            return f"Error generating answer: {e}"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the vector store
        
        Note: This may take a moment for large collections due to count operation.
        """
        try:
            # get_collection_count() has built-in timeout, returns -1 if unavailable
            doc_count = self.vector_store.get_collection_count()
            return {
                'collection_name': self.vector_store.collection_name,
                'document_count': doc_count if doc_count >= 0 else None,
                'count_available': doc_count >= 0
            }
        except Exception as e:
            # If stats fail, return what we can
            return {
                'collection_name': self.vector_store.collection_name,
                'document_count': None,
                'count_available': False,
                'error': str(e)
            }

