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
        
        for table_name, df in tables_data.items():
            for idx, row in df.iterrows():
                # Build text description from all relevant fields
                text_parts = []
                
                # Primary product information
                product_key = row.get('ProductKey', idx)
                english_name = row.get('EnglishProductName', 'Unknown Product')
                if english_name:
                    text_parts.append(f"Product: {english_name}")
                
                # Product identification
                if pd.notna(row.get('ProductAlternateKey')):
                    text_parts.append(f"Product Alternate Key: {row.get('ProductAlternateKey')}")
                
                # Pricing information
                if pd.notna(row.get('ListPrice')):
                    text_parts.append(f"List Price: {row.get('ListPrice')}")
                if pd.notna(row.get('StandardCost')):
                    text_parts.append(f"Standard Cost: {row.get('StandardCost')}")
                if pd.notna(row.get('DealerPrice')):
                    text_parts.append(f"Dealer Price: {row.get('DealerPrice')}")
                
                # Product characteristics
                if pd.notna(row.get('Color')):
                    text_parts.append(f"Color: {row.get('Color')}")
                if pd.notna(row.get('Size')):
                    text_parts.append(f"Size: {row.get('Size')}")
                if pd.notna(row.get('SizeRange')):
                    text_parts.append(f"Size Range: {row.get('SizeRange')}")
                if pd.notna(row.get('Weight')):
                    text_parts.append(f"Weight: {row.get('Weight')}")
                if pd.notna(row.get('WeightUnitMeasureCode')):
                    text_parts.append(f"Weight Unit: {row.get('WeightUnitMeasureCode')}")
                if pd.notna(row.get('SizeUnitMeasureCode')):
                    text_parts.append(f"Size Unit: {row.get('SizeUnitMeasureCode')}")
                
                # Product classification
                if pd.notna(row.get('ProductLine')):
                    text_parts.append(f"Product Line: {row.get('ProductLine')}")
                if pd.notna(row.get('Class')):
                    text_parts.append(f"Class: {row.get('Class')}")
                if pd.notna(row.get('Style')):
                    text_parts.append(f"Style: {row.get('Style')}")
                if pd.notna(row.get('ModelName')):
                    text_parts.append(f"Model Name: {row.get('ModelName')}")
                
                # Manufacturing information
                if pd.notna(row.get('DaysToManufacture')):
                    text_parts.append(f"Days to Manufacture: {row.get('DaysToManufacture')}")
                if pd.notna(row.get('FinishedGoodsFlag')):
                    finished_flag = "Yes" if row.get('FinishedGoodsFlag') else "No"
                    text_parts.append(f"Finished Goods: {finished_flag}")
                
                # Inventory information
                if pd.notna(row.get('SafetyStockLevel')):
                    text_parts.append(f"Safety Stock Level: {row.get('SafetyStockLevel')}")
                if pd.notna(row.get('ReorderPoint')):
                    text_parts.append(f"Reorder Point: {row.get('ReorderPoint')}")
                
                # Multilingual names
                if pd.notna(row.get('SpanishProductName')):
                    text_parts.append(f"Spanish Name: {row.get('SpanishProductName')}")
                if pd.notna(row.get('FrenchProductName')):
                    text_parts.append(f"French Name: {row.get('FrenchProductName')}")
                
                # Descriptions (prioritize English, but include others if available)
                if pd.notna(row.get('EnglishDescription')):
                    text_parts.append(f"Description: {row.get('EnglishDescription')}")
                elif pd.notna(row.get('FrenchDescription')):
                    text_parts.append(f"Description (French): {row.get('FrenchDescription')}")
                elif pd.notna(row.get('GermanDescription')):
                    text_parts.append(f"Description (German): {row.get('GermanDescription')}")
                
                # Date information
                if pd.notna(row.get('StartDate')):
                    text_parts.append(f"Start Date: {row.get('StartDate')}")
                if pd.notna(row.get('EndDate')):
                    text_parts.append(f"End Date: {row.get('EndDate')}")
                if pd.notna(row.get('Status')):
                    text_parts.append(f"Status: {row.get('Status')}")
                
                full_text = ". ".join(text_parts) + "."
                
                # Build metadata with key fields
                metadata = {
                    'table': table_name,
                    'type': 'product',
                    'ProductKey': product_key,
                    'EnglishProductName': english_name,
                }
                
                # Add optional fields to metadata if they exist
                if pd.notna(row.get('ProductAlternateKey')):
                    metadata['ProductAlternateKey'] = row.get('ProductAlternateKey')
                if pd.notna(row.get('Color')):
                    metadata['Color'] = row.get('Color')
                if pd.notna(row.get('ListPrice')):
                    metadata['ListPrice'] = row.get('ListPrice')
                if pd.notna(row.get('ProductLine')):
                    metadata['ProductLine'] = row.get('ProductLine')
                if pd.notna(row.get('Class')):
                    metadata['Class'] = row.get('Class')
                if pd.notna(row.get('Style')):
                    metadata['Style'] = row.get('Style')
                
                documents.append({
                    'text': full_text,
                    'metadata': metadata
                })
                
                all_products_data.append({
                    'ProductKey': product_key,
                    'EnglishProductName': english_name,
                    'ListPrice': row.get('ListPrice') if pd.notna(row.get('ListPrice')) else None,
                    'Color': row.get('Color') if pd.notna(row.get('Color')) else None,
                })
        
        # Create summary document
        if all_products_data:
            summary_parts = ["Complete list of all products:"]
            total_products = len(all_products_data)
            products_with_price = [p for p in all_products_data if p['ListPrice'] is not None]
            
            for product in all_products_data[:50]:  # Limit to first 50 for summary
                price_info = f", List Price: {product['ListPrice']}" if product['ListPrice'] else ""
                color_info = f", Color: {product['Color']}" if product['Color'] else ""
                summary_parts.append(
                    f"- {product['EnglishProductName']} (Key: {product['ProductKey']}{price_info}{color_info})"
                )
            
            if total_products > 50:
                summary_parts.append(f"... and {total_products - 50} more products")
            
            summary_parts.append(f"Total products: {total_products}")
            if products_with_price:
                avg_price = sum(p['ListPrice'] for p in products_with_price) / len(products_with_price)
                summary_parts.append(f"Average list price: {avg_price:.2f}")
            
            summary_text = "\n".join(summary_parts)
            documents.append({
                'text': summary_text,
                'metadata': {
                    'table': 'dbo.dimProduct',
                    'type': 'summary',
                    'total_products': total_products,
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

