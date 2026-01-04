"""
RAG System - Simple, reliable question answering over product database
"""
from openai import OpenAI
from typing import List, Dict, Any
from config import OPENAI_API_KEY, OPENAI_CHAT_MODEL
from embedding import EmbeddingGenerator
from vector_store import VectorStore


class RAGSystem:
    """Simple, reliable RAG system for product database Q&A"""
    
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
        
        print("  - RAGSystem ready!")
    
    def get_relevant_context(self, question: str, n_results: int = 20) -> str:
        """Retrieve relevant documents for the question"""
        
        # Generate embedding for the question
        question_embedding = self.embedding_generator.generate_embedding(question)
        
        # Search for relevant documents
        results = self.vector_store.search(question_embedding, n_results=n_results)
        
        if not results:
            return ""
        
        # Check if this is a statistics/count question - if so, prioritize summary
        question_lower = question.lower()
        is_stats_question = any(word in question_lower for word in [
            'how many', 'total', 'count', 'number of', 'average', 'highest', 'lowest',
            'most expensive', 'cheapest', 'all products', 'list all', 'statistics'
        ])
        
        # Separate summary and product documents
        summary_docs = [r for r in results if r['metadata'].get('type') == 'summary']
        product_docs = [r for r in results if r['metadata'].get('type') == 'product']
        
        # For stats questions, always try to get the summary
        if is_stats_question and not summary_docs:
            try:
                summary_results = self.vector_store.get_by_metadata(
                    where={"type": "summary"},
                    n_results=1
                )
                if summary_results:
                    summary_docs = summary_results
            except:
                pass
        
        # Order: summary first for stats questions, otherwise products first
        if is_stats_question:
            ordered_results = summary_docs + product_docs
        else:
            ordered_results = product_docs + summary_docs
        
        # Build context string
        context_parts = []
        for result in ordered_results[:15]:  # Limit to 15 documents
            context_parts.append(result['text'])
        
        return "\n\n---\n\n".join(context_parts)
    
    def query(self, question: str) -> str:
        """Answer a question about the product database"""
        
        if not question or not question.strip():
            return "Please ask a question about the products in the database."
        
        question = question.strip()
        
        # Get relevant context
        context = self.get_relevant_context(question)
        
        if not context:
            return "I don't have any product information in my database yet. Please run the indexer first."
        
        # Create the prompt
        prompt = f"""You are a helpful assistant that answers questions about products in a database.

RULES:
1. Answer ONLY using the information provided in the CONTEXT below.
2. If the answer is not in the context, say "I don't know" or "I don't have that information."
3. Be specific - use exact product names, prices, and details from the context.
4. For statistics (totals, averages, counts), look for the DATABASE SUMMARY section.
5. Never make up information.

CONTEXT:
{context}

QUESTION: {question}

ANSWER:"""

        # Generate answer
        try:
            response = self.client.chat.completions.create(
                model=self.chat_model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful product database assistant. Answer questions accurately using only the provided context. If you don't know or the information isn't in the context, say 'I don't know' or 'I don't have that information.'"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Low temperature for accuracy
                max_tokens=1000
            )
            
            answer = response.choices[0].message.content.strip()
            return answer
            
        except Exception as e:
            return f"Sorry, I encountered an error: {str(e)}"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the vector store"""
        try:
            doc_count = self.vector_store.get_collection_count()
            return {
                'collection_name': self.vector_store.collection_name,
                'document_count': doc_count if doc_count >= 0 else None,
                'status': 'ready' if doc_count > 0 else 'empty'
            }
        except Exception as e:
            return {
                'collection_name': self.vector_store.collection_name,
                'document_count': None,
                'status': 'error',
                'error': str(e)
            }
