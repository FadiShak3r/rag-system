"""
Data processing module - Creates rich, complete documents for embedding
Each product becomes a self-contained document with ALL available information
"""
import pandas as pd
from typing import List, Dict, Any


class DataProcessor:
    """Processes database tables into rich documents for embedding"""
    
    def __init__(self):
        pass
    
    def process_product_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a product row into a rich, complete document"""
        
        # Get product key and name
        product_key = row.get('ProductKey', 'Unknown')
        product_name = row.get('EnglishProductName', 'Unknown Product')
        
        # Build a comprehensive text description with ALL available fields
        lines = []
        
        # === PRODUCT IDENTITY ===
        lines.append(f"PRODUCT: {product_name}")
        lines.append(f"Product Key: {product_key}")
        
        if pd.notna(row.get('ProductAlternateKey')):
            lines.append(f"Alternate Key: {row.get('ProductAlternateKey')}")
        
        if pd.notna(row.get('ProductSubcategoryKey')):
            lines.append(f"Subcategory Key: {row.get('ProductSubcategoryKey')}")
        
        # === PRICING ===
        if pd.notna(row.get('ListPrice')):
            lines.append(f"List Price: ${float(row.get('ListPrice')):.2f}")
        
        if pd.notna(row.get('StandardCost')):
            lines.append(f"Standard Cost: ${float(row.get('StandardCost')):.2f}")
        
        if pd.notna(row.get('DealerPrice')):
            lines.append(f"Dealer Price: ${float(row.get('DealerPrice')):.2f}")
        
        # === PHYSICAL CHARACTERISTICS ===
        if pd.notna(row.get('Color')):
            lines.append(f"Color: {row.get('Color')}")
        
        if pd.notna(row.get('Size')):
            lines.append(f"Size: {row.get('Size')}")
        
        if pd.notna(row.get('SizeRange')):
            lines.append(f"Size Range: {row.get('SizeRange')}")
        
        if pd.notna(row.get('SizeUnitMeasureCode')):
            lines.append(f"Size Unit: {row.get('SizeUnitMeasureCode')}")
        
        if pd.notna(row.get('Weight')):
            lines.append(f"Weight: {row.get('Weight')}")
        
        if pd.notna(row.get('WeightUnitMeasureCode')):
            lines.append(f"Weight Unit: {row.get('WeightUnitMeasureCode')}")
        
        # === CLASSIFICATION ===
        if pd.notna(row.get('ProductLine')):
            product_line = row.get('ProductLine')
            line_names = {'M': 'Mountain', 'R': 'Road', 'S': 'Specialty', 'T': 'Touring'}
            line_display = line_names.get(product_line, product_line)
            lines.append(f"Product Line: {line_display} ({product_line})")
        
        if pd.notna(row.get('Class')):
            product_class = row.get('Class')
            class_names = {'H': 'High', 'M': 'Medium', 'L': 'Low'}
            class_display = class_names.get(product_class, product_class)
            lines.append(f"Class: {class_display} ({product_class})")
        
        if pd.notna(row.get('Style')):
            style = row.get('Style')
            style_names = {'M': 'Men', 'W': 'Women', 'U': 'Universal'}
            style_display = style_names.get(style, style)
            lines.append(f"Style: {style_display} ({style})")
        
        if pd.notna(row.get('ModelName')):
            lines.append(f"Model: {row.get('ModelName')}")
        
        # === MANUFACTURING ===
        if pd.notna(row.get('DaysToManufacture')):
            lines.append(f"Days to Manufacture: {row.get('DaysToManufacture')}")
        
        if pd.notna(row.get('FinishedGoodsFlag')):
            finished = "Yes" if row.get('FinishedGoodsFlag') else "No"
            lines.append(f"Finished Good: {finished}")
        
        # === INVENTORY ===
        if pd.notna(row.get('SafetyStockLevel')):
            lines.append(f"Safety Stock Level: {row.get('SafetyStockLevel')}")
        
        if pd.notna(row.get('ReorderPoint')):
            lines.append(f"Reorder Point: {row.get('ReorderPoint')}")
        
        # === MULTILINGUAL NAMES ===
        if pd.notna(row.get('SpanishProductName')):
            lines.append(f"Spanish Name: {row.get('SpanishProductName')}")
        
        if pd.notna(row.get('FrenchProductName')):
            lines.append(f"French Name: {row.get('FrenchProductName')}")
        
        # === DESCRIPTION (try multiple languages) ===
        description = None
        for desc_field in ['EnglishDescription', 'GermanDescription', 'FrenchDescription', 
                           'ChineseDescription', 'ArabicDescription', 'HebrewDescription',
                           'ThaiDescription', 'JapaneseDescription', 'TurkishDescription']:
            if pd.notna(row.get(desc_field)):
                description = row.get(desc_field)
                break
        
        if description:
            lines.append(f"Description: {description}")
        
        # === DATES & STATUS ===
        if pd.notna(row.get('StartDate')):
            lines.append(f"Start Date: {row.get('StartDate')}")
        
        if pd.notna(row.get('EndDate')):
            lines.append(f"End Date: {row.get('EndDate')}")
        
        if pd.notna(row.get('Status')):
            lines.append(f"Status: {row.get('Status')}")
        
        # Create the full document text
        full_text = "\n".join(lines)
        
        # Build metadata for filtering
        metadata = {
            'table': table_name,
            'type': 'product',
            'ProductKey': int(product_key) if pd.notna(product_key) else 0,
            'EnglishProductName': str(product_name),
        }
        
        # Add optional metadata fields
        if pd.notna(row.get('Color')):
            metadata['Color'] = str(row.get('Color'))
        
        if pd.notna(row.get('ListPrice')):
            metadata['ListPrice'] = float(row.get('ListPrice'))
        
        if pd.notna(row.get('ProductLine')):
            metadata['ProductLine'] = str(row.get('ProductLine'))
        
        if pd.notna(row.get('Class')):
            metadata['Class'] = str(row.get('Class'))
        
        if pd.notna(row.get('Style')):
            metadata['Style'] = str(row.get('Style'))
        
        if pd.notna(row.get('ModelName')):
            metadata['ModelName'] = str(row.get('ModelName'))
        
        return {
            'text': full_text,
            'metadata': metadata
        }
    
    def create_summary_document(self, products: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise summary document with database statistics (fits in token limits)"""
        
        total_products = len(products)
        
        # Collect all prices
        prices = []
        for p in products:
            price = p.get('ListPrice')
            if price is not None:
                prices.append((price, p.get('EnglishProductName', 'Unknown')))
        
        # Collect colors
        color_counts = {}
        for p in products:
            color = p.get('Color')
            if color:
                color_counts[color] = color_counts.get(color, 0) + 1
        
        # Collect product lines
        line_counts = {}
        line_names = {'M': 'Mountain', 'R': 'Road', 'S': 'Specialty', 'T': 'Touring'}
        for p in products:
            line = p.get('ProductLine')
            if line:
                line_counts[line] = line_counts.get(line, 0) + 1
        
        # Collect classes
        class_counts = {}
        class_names = {'H': 'High', 'M': 'Medium', 'L': 'Low'}
        for p in products:
            cls = p.get('Class')
            if cls:
                class_counts[cls] = class_counts.get(cls, 0) + 1
        
        # Build concise summary text
        lines = [
            "DATABASE STATISTICS",
            "===================",
            "",
            f"TOTAL PRODUCTS: {total_products}",
            "",
        ]
        
        # Price statistics
        if prices:
            prices_sorted = sorted(prices, key=lambda x: x[0], reverse=True)
            avg_price = sum(p[0] for p in prices) / len(prices)
            
            lines.extend([
                "PRICING:",
                f"- Products with prices: {len(prices)}",
                f"- Average price: ${avg_price:.2f}",
                f"- Highest price: ${prices_sorted[0][0]:.2f} ({prices_sorted[0][1]})",
                f"- Lowest price: ${prices_sorted[-1][0]:.2f} ({prices_sorted[-1][1]})",
                "",
                "TOP 10 MOST EXPENSIVE:",
            ])
            for price, name in prices_sorted[:10]:
                lines.append(f"  ${price:.2f} - {name}")
            
            lines.append("")
            lines.append("TOP 10 CHEAPEST:")
            for price, name in prices_sorted[-10:]:
                lines.append(f"  ${price:.2f} - {name}")
            lines.append("")
        
        # Color breakdown
        if color_counts:
            lines.append(f"COLORS ({len(color_counts)} different colors):")
            for color, count in sorted(color_counts.items(), key=lambda x: -x[1]):
                lines.append(f"- {color}: {count} products")
            lines.append("")
        
        # Product line breakdown
        if line_counts:
            lines.append("PRODUCT LINES:")
            for line, count in sorted(line_counts.items(), key=lambda x: -x[1]):
                line_display = line_names.get(line, line)
                lines.append(f"- {line_display} ({line}): {count} products")
            lines.append("")
        
        # Class breakdown
        if class_counts:
            lines.append("PRODUCT CLASSES:")
            for cls, count in sorted(class_counts.items(), key=lambda x: -x[1]):
                cls_display = class_names.get(cls, cls)
                lines.append(f"- {cls_display} ({cls}): {count} products")
        
        summary_text = "\n".join(lines)
        
        # Metadata
        metadata = {
            'table': table_name,
            'type': 'summary',
            'total_products': total_products,
        }
        
        if prices:
            metadata['avg_price'] = sum(p[0] for p in prices) / len(prices)
            metadata['max_price'] = max(p[0] for p in prices)
            metadata['min_price'] = min(p[0] for p in prices)
        
        return {
            'text': summary_text,
            'metadata': metadata
        }
    
    def process_tables(self, tables_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """Process all tables and create documents for embedding"""
        documents = []
        
        for table_name, df in tables_data.items():
            print(f"Processing table: {table_name} ({len(df)} rows)")
            
            # Collect product info for summary
            products_info = []
            
            # Process each row
            for idx, row in df.iterrows():
                doc = self.process_product_row(row, table_name)
                documents.append(doc)
                
                # Collect info for summary
                products_info.append({
                    'ProductKey': row.get('ProductKey'),
                    'EnglishProductName': row.get('EnglishProductName'),
                    'ListPrice': float(row.get('ListPrice')) if pd.notna(row.get('ListPrice')) else None,
                    'Color': row.get('Color') if pd.notna(row.get('Color')) else None,
                    'ProductLine': row.get('ProductLine') if pd.notna(row.get('ProductLine')) else None,
                    'Class': row.get('Class') if pd.notna(row.get('Class')) else None,
                })
            
            # Create summary document
            summary_doc = self.create_summary_document(products_info, table_name)
            documents.append(summary_doc)
            
            print(f"Created {len(df)} product documents + 1 summary document")
        
        print(f"\nTotal documents created: {len(documents)}")
        return documents
