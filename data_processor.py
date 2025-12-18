"""
Data processing and chunking module for tabular data
"""
import pandas as pd
from typing import List, Dict, Any
from config import CHUNK_SIZE, CHUNK_OVERLAP


class DataProcessor:
    """Processes and chunks tabular data for RAG"""
    
    def __init__(self, chunk_size: int = CHUNK_SIZE, chunk_overlap: int = CHUNK_OVERLAP):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def format_row_as_text(self, row: pd.Series, table_name: str) -> str:
        """Convert a database row to a formatted text string"""
        # Create a natural language description of the row
        text_parts = [f"Table: {table_name}"]
        
        for column, value in row.items():
            if pd.notna(value):
                readable_column = column.replace('_', ' ').title()
                text_parts.append(f"{readable_column}: {value}")
        
        return ". ".join(text_parts) + "."
    
    def create_documents_from_tables(self, tables_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """Create text documents from table data"""
        documents = []
        all_products_data = []
        all_users_data = []
        all_reviews_data = []
        
        users_lookup = {}
        if 'users' in tables_data:
            for idx, row in tables_data['users'].iterrows():
                user_id = row.get('user_id', idx)
                users_lookup[user_id] = {
                    'username': row.get('username', 'Unknown'),
                    'full_name': row.get('full_name', ''),
                    'email': row.get('email', '')
                }
        
        for table_name, df in tables_data.items():
            if table_name == 'products':
                for idx, row in df.iterrows():
                    product_name = row.get('product_name', 'Unknown Product')
                    sale_amount = float(row.get('sale_amount', 0))
                    purchase_count = int(row.get('purchase_count', 0))
                    
                    text_parts = [
                        f"The product named '{product_name}' has generated a total revenue of {sale_amount}",
                        f"and has been sold {purchase_count} times"
                    ]
                    
                    if purchase_count > 0:
                        avg_price = sale_amount / purchase_count
                        text_parts.append(f"The average price per sale is {avg_price:.2f}")
                    else:
                        text_parts.append("No sales have been recorded for this product")
                    
                    full_text = ". ".join(text_parts) + "."
                    
                    metadata = {
                        'table': table_name,
                        'product_id': row.get('id', idx),
                        'product_name': product_name,
                        'sale_amount': sale_amount,
                        'purchase_count': purchase_count,
                        'type': 'product'
                    }
                    
                    documents.append({
                        'text': full_text,
                        'metadata': metadata
                    })
                    
                    all_products_data.append({
                        'name': product_name,
                        'revenue': sale_amount,
                        'purchases': purchase_count
                    })
            
            elif table_name == 'users':
                for idx, row in df.iterrows():
                    user_id = row.get('user_id', idx)
                    username = row.get('username', 'Unknown')
                    email = row.get('email', '')
                    full_name = row.get('full_name', '')
                    created_at = row.get('created_at', '')
                    last_login = row.get('last_login', '')
                    
                    text_parts = [f"User with username '{username}'"]
                    if full_name:
                        text_parts.append(f"full name '{full_name}'")
                    if email:
                        text_parts.append(f"email '{email}'")
                    if created_at:
                        text_parts.append(f"created account on {created_at}")
                    if pd.notna(last_login) and last_login:
                        text_parts.append(f"last login was on {last_login}")
                    
                    full_text = ". ".join(text_parts) + "."
                    
                    metadata = {
                        'table': table_name,
                        'user_id': user_id,
                        'username': username,
                        'email': email,
                        'type': 'user'
                    }
                    
                    documents.append({
                        'text': full_text,
                        'metadata': metadata
                    })
                    
                    all_users_data.append({
                        'user_id': user_id,
                        'username': username,
                        'email': email,
                        'full_name': full_name
                    })
            
            elif table_name == 'user_reviews':
                for idx, row in df.iterrows():
                    review_id = row.get('review_id', idx)
                    user_id = row.get('user_id', '')
                    rating = row.get('rating', '')
                    review_text = row.get('review_text', '')
                    created_at = row.get('created_at', '')
                    
                    text_parts = [f"Review with rating {rating} out of 5"]
                    if pd.notna(review_text) and review_text:
                        text_parts.append(f"review text: '{review_text}'")
                    
                    if user_id and user_id in users_lookup:
                        user_info = users_lookup[user_id]
                        if user_info.get('full_name'):
                            text_parts.append(f"written by user ID {user_id} named {user_info['full_name']} (username: {user_info['username']})")
                        elif user_info.get('username'):
                            text_parts.append(f"written by user ID {user_id} with username {user_info['username']}")
                        else:
                            text_parts.append(f"written by user ID {user_id}")
                    elif user_id:
                        text_parts.append(f"written by user ID {user_id}")
                    
                    if created_at:
                        text_parts.append(f"created on {created_at}")
                    
                    full_text = ". ".join(text_parts) + "."
                    
                    metadata = {
                        'table': table_name,
                        'review_id': review_id,
                        'user_id': user_id,
                        'rating': rating,
                        'type': 'review'
                    }
                    
                    if user_id and user_id in users_lookup:
                        user_info = users_lookup[user_id]
                        metadata['username'] = user_info.get('username', '')
                        metadata['user_full_name'] = user_info.get('full_name', '')
                    
                    documents.append({
                        'text': full_text,
                        'metadata': metadata
                    })
                    
                    all_reviews_data.append({
                        'review_id': review_id,
                        'user_id': user_id,
                        'rating': rating,
                        'review_text': review_text
                    })
            
            else:
                for idx, row in df.iterrows():
                    text_parts = [f"Record from {table_name} table"]
                    for column, value in row.items():
                        if pd.notna(value) and column not in ['id', 'created_at', 'updated_at']:
                            readable_column = column.replace('_', ' ').title()
                            text_parts.append(f"{readable_column}: {value}")
                    
                    full_text = ". ".join(text_parts) + "."
                    
                    metadata = {
                        'table': table_name,
                        'type': 'generic',
                        'row_id': row.get('id', idx)
                    }
                    
                    documents.append({
                        'text': full_text,
                        'metadata': metadata
                    })
        
        if all_products_data:
            summary_parts = ["Complete list of all products:"]
            total_revenue = sum(p['revenue'] for p in all_products_data)
            total_purchases = sum(p['purchases'] for p in all_products_data)
            
            for product in all_products_data:
                summary_parts.append(
                    f"- {product['name']}: revenue {product['revenue']}, purchases {product['purchases']}"
                )
            
            summary_parts.append(f"Total revenue across all products: {total_revenue}")
            summary_parts.append(f"Total purchases across all products: {total_purchases}")
            
            summary_text = "\n".join(summary_parts)
            documents.append({
                'text': summary_text,
                'metadata': {
                    'table': 'products',
                    'type': 'summary',
                    'total_products': len(all_products_data),
                    'total_revenue': total_revenue,
                    'total_purchases': total_purchases
                }
            })
        
        if all_reviews_data:
            summary_parts = ["Complete list of all reviews:"]
            total_reviews = len(all_reviews_data)
            avg_rating = sum(r['rating'] for r in all_reviews_data) / total_reviews if total_reviews > 0 else 0
            
            for review in all_reviews_data:
                summary_parts.append(
                    f"- Review ID {review['review_id']}: rating {review['rating']}/5 by user {review['user_id']}"
                )
            
            summary_parts.append(f"Total reviews: {total_reviews}")
            summary_parts.append(f"Average rating: {avg_rating:.2f}")
            
            summary_text = "\n".join(summary_parts)
            documents.append({
                'text': summary_text,
                'metadata': {
                    'table': 'user_reviews',
                    'type': 'summary',
                    'total_reviews': total_reviews,
                    'average_rating': avg_rating
                }
            })
        
        return documents
    
    def chunk_text(self, text: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Split text into chunks with overlap"""
        words = text.split()
    
        doc_type = metadata.get('type')
        if doc_type not in ['summary'] or len(words) <= self.chunk_size:
            return [{'text': text, 'metadata': metadata}]
        
        chunks = []
        start = 0
        while start < len(words):
            end = start + self.chunk_size
            chunk_words = words[start:end]
            chunk_text = ' '.join(chunk_words)
            
            chunks.append({
                'text': chunk_text,
                'metadata': {**metadata, 'chunk_index': len(chunks)}
            })
            
            start = end - self.chunk_overlap
        
        return chunks
    
    def process_tables(self, tables_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """Main processing function: creates documents and chunks them"""
        documents = self.create_documents_from_tables(tables_data)
        
        all_chunks = []
        for doc in documents:
            chunks = self.chunk_text(doc['text'], doc['metadata'])
            all_chunks.extend(chunks)
        
        print(f"Created {len(all_chunks)} chunks from {len(documents)} documents")
        return all_chunks

