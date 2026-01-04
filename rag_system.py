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
        
        question_lower = question.lower()
        
        # Check if this is a statistics/count question
        is_stats_question = any(word in question_lower for word in [
            'how many', 'total', 'count', 'number of', 'average', 'highest', 'lowest',
            'most expensive', 'cheapest', 'all products', 'all customers', 'list all', 
            'statistics', 'income', 'gender', 'occupation', 'education'
        ])
        
        # Check if question is about customers, products, or inventory
        is_customer_question = any(word in question_lower for word in [
            'customer', 'customers', 'person', 'people', 'income', 'gender', 'married',
            'single', 'occupation', 'education', 'children', 'email', 'phone', 'address'
        ])
        
        is_product_question = any(word in question_lower for word in [
            'product', 'products', 'price', 'color', 'size', 'weight', 'model'
        ])
        
        is_inventory_question = any(word in question_lower for word in [
            'inventory', 'stock', 'units', 'balance', 'movement', 'units in', 'units out',
            'warehouse', 'supply', 'quantity', 'available'
        ])
        
        # Separate by document type
        summary_docs = [r for r in results if 'summary' in r['metadata'].get('type', '')]
        product_docs = [r for r in results if r['metadata'].get('type') == 'product']
        customer_docs = [r for r in results if r['metadata'].get('type') == 'customer']
        inventory_docs = [r for r in results if r['metadata'].get('type') == 'inventory']
        
        # For stats questions, try to get the appropriate summary
        if is_stats_question:
            if is_inventory_question:
                try:
                    inventory_summaries = self.vector_store.get_by_metadata(
                        where={"type": "inventory_summary"},
                        n_results=1
                    )
                    if inventory_summaries:
                        summary_docs = inventory_summaries + summary_docs
                except:
                    pass
            
            if is_customer_question:
                try:
                    customer_summaries = self.vector_store.get_by_metadata(
                        where={"type": "customer_summary"},
                        n_results=1
                    )
                    if customer_summaries:
                        summary_docs = customer_summaries + summary_docs
                except:
                    pass
            
            if is_product_question or (not is_customer_question and not is_inventory_question):
                try:
                    product_summaries = self.vector_store.get_by_metadata(
                        where={"type": "product_summary"},
                        n_results=1
                    )
                    if product_summaries:
                        summary_docs = product_summaries + summary_docs
                except:
                    pass
        
        # Order results based on question type
        if is_stats_question:
            ordered_results = summary_docs + inventory_docs + customer_docs + product_docs
        elif is_inventory_question:
            ordered_results = inventory_docs + summary_docs + product_docs + customer_docs
        elif is_customer_question:
            ordered_results = customer_docs + summary_docs + product_docs + inventory_docs
        elif is_product_question:
            ordered_results = product_docs + summary_docs + customer_docs + inventory_docs
        else:
            ordered_results = results
        
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
        prompt = f"""You are a helpful assistant that answers questions about products, customers, and inventory in a database.

RULES:
1. Answer ONLY using the information provided in the CONTEXT below.
2. If the answer is not in the context, say "I don't know" or "I don't have that information."
3. Be specific - use exact names, numbers, and details from the context.
4. For statistics (totals, averages, counts), look for the DATABASE STATISTICS section.
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
                        "content": "You are a helpful database assistant for products, customers, and inventory. Answer questions accurately using only the provided context. If you don't know or the information isn't in the context, say 'I don't know' or 'I don't have that information.'"
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
