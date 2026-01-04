"""
Data processing module - Creates rich, complete documents for embedding
Each record becomes a self-contained document with ALL available information
"""
import pandas as pd
from typing import List, Dict, Any


class DataProcessor:
    """Processes database tables into rich documents for embedding"""
    
    def __init__(self):
        pass
    
    # ==================== PRODUCT PROCESSING ====================
    
    def process_product_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a product row into a rich, complete document"""
        
        product_key = row.get('ProductKey', 'Unknown')
        product_name = row.get('EnglishProductName', 'Unknown Product')
        
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
        
        # === DESCRIPTION ===
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
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'product',
            'ProductKey': int(product_key) if pd.notna(product_key) else 0,
            'EnglishProductName': str(product_name),
        }
        
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
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_product_summary(self, products: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise product summary document"""
        
        total_products = len(products)
        
        prices = []
        for p in products:
            price = p.get('ListPrice')
            if price is not None:
                prices.append((price, p.get('EnglishProductName', 'Unknown')))
        
        color_counts = {}
        for p in products:
            color = p.get('Color')
            if color:
                color_counts[color] = color_counts.get(color, 0) + 1
        
        line_counts = {}
        line_names = {'M': 'Mountain', 'R': 'Road', 'S': 'Specialty', 'T': 'Touring'}
        for p in products:
            line = p.get('ProductLine')
            if line:
                line_counts[line] = line_counts.get(line, 0) + 1
        
        class_counts = {}
        class_names = {'H': 'High', 'M': 'Medium', 'L': 'Low'}
        for p in products:
            cls = p.get('Class')
            if cls:
                class_counts[cls] = class_counts.get(cls, 0) + 1
        
        lines = [
            "PRODUCT DATABASE STATISTICS",
            "===========================",
            "",
            f"TOTAL PRODUCTS: {total_products}",
            "",
        ]
        
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
        
        if color_counts:
            lines.append(f"COLORS ({len(color_counts)} different):")
            for color, count in sorted(color_counts.items(), key=lambda x: -x[1]):
                lines.append(f"- {color}: {count} products")
            lines.append("")
        
        if line_counts:
            lines.append("PRODUCT LINES:")
            for line, count in sorted(line_counts.items(), key=lambda x: -x[1]):
                line_display = line_names.get(line, line)
                lines.append(f"- {line_display} ({line}): {count} products")
            lines.append("")
        
        if class_counts:
            lines.append("PRODUCT CLASSES:")
            for cls, count in sorted(class_counts.items(), key=lambda x: -x[1]):
                cls_display = class_names.get(cls, cls)
                lines.append(f"- {cls_display} ({cls}): {count} products")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'product_summary',
            'total_products': total_products,
        }
        
        if prices:
            metadata['avg_price'] = sum(p[0] for p in prices) / len(prices)
            metadata['max_price'] = max(p[0] for p in prices)
            metadata['min_price'] = min(p[0] for p in prices)
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== CUSTOMER PROCESSING ====================
    
    def process_customer_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a customer row into a rich, complete document"""
        
        customer_key = row.get('CustomerKey', 'Unknown')
        
        # Build full name
        first_name = row.get('FirstName', '') if pd.notna(row.get('FirstName')) else ''
        middle_name = row.get('MiddleName', '') if pd.notna(row.get('MiddleName')) else ''
        last_name = row.get('LastName', '') if pd.notna(row.get('LastName')) else ''
        
        name_parts = [first_name, middle_name, last_name]
        full_name = ' '.join(p for p in name_parts if p).strip() or 'Unknown Customer'
        
        lines = []
        
        # === CUSTOMER IDENTITY ===
        lines.append(f"CUSTOMER: {full_name}")
        lines.append(f"Customer Key: {customer_key}")
        
        if pd.notna(row.get('CustomerAlternateKey')):
            lines.append(f"Customer ID: {row.get('CustomerAlternateKey')}")
        
        if pd.notna(row.get('Title')):
            lines.append(f"Title: {row.get('Title')}")
        
        if pd.notna(row.get('Suffix')):
            lines.append(f"Suffix: {row.get('Suffix')}")
        
        # === DEMOGRAPHICS ===
        if pd.notna(row.get('Gender')):
            gender = row.get('Gender')
            gender_display = {'M': 'Male', 'F': 'Female'}.get(gender, gender)
            lines.append(f"Gender: {gender_display}")
        
        if pd.notna(row.get('BirthDate')):
            lines.append(f"Birth Date: {row.get('BirthDate')}")
        
        if pd.notna(row.get('MaritalStatus')):
            status = row.get('MaritalStatus')
            status_display = {'M': 'Married', 'S': 'Single'}.get(status, status)
            lines.append(f"Marital Status: {status_display}")
        
        if pd.notna(row.get('TotalChildren')):
            lines.append(f"Total Children: {row.get('TotalChildren')}")
        
        if pd.notna(row.get('NumberChildrenAtHome')):
            lines.append(f"Children at Home: {row.get('NumberChildrenAtHome')}")
        
        # === INCOME & OCCUPATION ===
        if pd.notna(row.get('YearlyIncome')):
            lines.append(f"Yearly Income: ${float(row.get('YearlyIncome')):,.2f}")
        
        if pd.notna(row.get('EnglishOccupation')):
            lines.append(f"Occupation: {row.get('EnglishOccupation')}")
        
        if pd.notna(row.get('EnglishEducation')):
            lines.append(f"Education: {row.get('EnglishEducation')}")
        
        # === HOME & TRANSPORTATION ===
        if pd.notna(row.get('HouseOwnerFlag')):
            owner = "Yes" if row.get('HouseOwnerFlag') == '1' else "No"
            lines.append(f"Home Owner: {owner}")
        
        if pd.notna(row.get('NumberCarsOwned')):
            lines.append(f"Cars Owned: {row.get('NumberCarsOwned')}")
        
        if pd.notna(row.get('CommuteDistance')):
            lines.append(f"Commute Distance: {row.get('CommuteDistance')}")
        
        # === CONTACT INFO ===
        if pd.notna(row.get('EmailAddress')):
            lines.append(f"Email: {row.get('EmailAddress')}")
        
        if pd.notna(row.get('Phone')):
            lines.append(f"Phone: {row.get('Phone')}")
        
        # === ADDRESS ===
        if pd.notna(row.get('AddressLine1')):
            lines.append(f"Address: {row.get('AddressLine1')}")
        
        if pd.notna(row.get('AddressLine2')):
            lines.append(f"Address Line 2: {row.get('AddressLine2')}")
        
        if pd.notna(row.get('GeographyKey')):
            lines.append(f"Geography Key: {row.get('GeographyKey')}")
        
        # === PURCHASE HISTORY ===
        if pd.notna(row.get('DateFirstPurchase')):
            lines.append(f"First Purchase Date: {row.get('DateFirstPurchase')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'customer',
            'CustomerKey': int(customer_key) if pd.notna(customer_key) else 0,
            'FullName': full_name,
        }
        
        if pd.notna(row.get('Gender')):
            metadata['Gender'] = str(row.get('Gender'))
        if pd.notna(row.get('MaritalStatus')):
            metadata['MaritalStatus'] = str(row.get('MaritalStatus'))
        if pd.notna(row.get('YearlyIncome')):
            metadata['YearlyIncome'] = float(row.get('YearlyIncome'))
        if pd.notna(row.get('EnglishOccupation')):
            metadata['Occupation'] = str(row.get('EnglishOccupation'))
        if pd.notna(row.get('EnglishEducation')):
            metadata['Education'] = str(row.get('EnglishEducation'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_customer_summary(self, customers: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise customer summary document"""
        
        total_customers = len(customers)
        
        # Gender breakdown
        gender_counts = {}
        for c in customers:
            gender = c.get('Gender')
            if gender:
                gender_display = {'M': 'Male', 'F': 'Female'}.get(gender, gender)
                gender_counts[gender_display] = gender_counts.get(gender_display, 0) + 1
        
        # Marital status breakdown
        marital_counts = {}
        for c in customers:
            status = c.get('MaritalStatus')
            if status:
                status_display = {'M': 'Married', 'S': 'Single'}.get(status, status)
                marital_counts[status_display] = marital_counts.get(status_display, 0) + 1
        
        # Income statistics
        incomes = []
        for c in customers:
            income = c.get('YearlyIncome')
            if income is not None:
                incomes.append(income)
        
        # Occupation breakdown
        occupation_counts = {}
        for c in customers:
            occ = c.get('Occupation')
            if occ:
                occupation_counts[occ] = occupation_counts.get(occ, 0) + 1
        
        # Education breakdown
        education_counts = {}
        for c in customers:
            edu = c.get('Education')
            if edu:
                education_counts[edu] = education_counts.get(edu, 0) + 1
        
        lines = [
            "CUSTOMER DATABASE STATISTICS",
            "============================",
            "",
            f"TOTAL CUSTOMERS: {total_customers}",
            "",
        ]
        
        # Gender
        if gender_counts:
            lines.append("GENDER DISTRIBUTION:")
            for gender, count in sorted(gender_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_customers) * 100
                lines.append(f"- {gender}: {count} ({pct:.1f}%)")
            lines.append("")
        
        # Marital status
        if marital_counts:
            lines.append("MARITAL STATUS:")
            for status, count in sorted(marital_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_customers) * 100
                lines.append(f"- {status}: {count} ({pct:.1f}%)")
            lines.append("")
        
        # Income
        if incomes:
            avg_income = sum(incomes) / len(incomes)
            max_income = max(incomes)
            min_income = min(incomes)
            lines.extend([
                "INCOME STATISTICS:",
                f"- Customers with income data: {len(incomes)}",
                f"- Average yearly income: ${avg_income:,.2f}",
                f"- Highest income: ${max_income:,.2f}",
                f"- Lowest income: ${min_income:,.2f}",
                "",
            ])
        
        # Occupation
        if occupation_counts:
            lines.append("OCCUPATIONS:")
            for occ, count in sorted(occupation_counts.items(), key=lambda x: -x[1])[:10]:
                lines.append(f"- {occ}: {count} customers")
            lines.append("")
        
        # Education
        if education_counts:
            lines.append("EDUCATION LEVELS:")
            for edu, count in sorted(education_counts.items(), key=lambda x: -x[1]):
                lines.append(f"- {edu}: {count} customers")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'customer_summary',
            'total_customers': total_customers,
        }
        
        if incomes:
            metadata['avg_income'] = sum(incomes) / len(incomes)
            metadata['max_income'] = max(incomes)
            metadata['min_income'] = min(incomes)
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== INVENTORY PROCESSING ====================
    
    def process_inventory_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert an inventory row into a document"""
        
        product_key = row.get('ProductKey', 'Unknown')
        date_key = row.get('DateKey', 'Unknown')
        
        lines = []
        
        lines.append(f"INVENTORY RECORD")
        lines.append(f"Product Key: {product_key}")
        lines.append(f"Date Key: {date_key}")
        
        if pd.notna(row.get('MovementDate')):
            lines.append(f"Movement Date: {row.get('MovementDate')}")
        
        if pd.notna(row.get('UnitCost')):
            lines.append(f"Unit Cost: ${float(row.get('UnitCost')):.2f}")
        
        if pd.notna(row.get('UnitsIn')):
            lines.append(f"Units In: {row.get('UnitsIn')}")
        
        if pd.notna(row.get('UnitsOut')):
            lines.append(f"Units Out: {row.get('UnitsOut')}")
        
        if pd.notna(row.get('UnitsBalance')):
            lines.append(f"Units Balance: {row.get('UnitsBalance')}")
        
        # Calculate net movement
        units_in = int(row.get('UnitsIn', 0)) if pd.notna(row.get('UnitsIn')) else 0
        units_out = int(row.get('UnitsOut', 0)) if pd.notna(row.get('UnitsOut')) else 0
        net_movement = units_in - units_out
        lines.append(f"Net Movement: {net_movement}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'inventory',
            'ProductKey': int(product_key) if pd.notna(product_key) else 0,
            'DateKey': int(date_key) if pd.notna(date_key) else 0,
        }
        
        if pd.notna(row.get('UnitsBalance')):
            metadata['UnitsBalance'] = int(row.get('UnitsBalance'))
        if pd.notna(row.get('UnitCost')):
            metadata['UnitCost'] = float(row.get('UnitCost'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_inventory_summary(self, inventory_records: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise inventory summary document"""
        
        total_records = len(inventory_records)
        
        # Unique products
        unique_products = set(r.get('ProductKey') for r in inventory_records if r.get('ProductKey'))
        
        # Total units calculations
        total_units_in = sum(r.get('UnitsIn', 0) or 0 for r in inventory_records)
        total_units_out = sum(r.get('UnitsOut', 0) or 0 for r in inventory_records)
        total_net_movement = total_units_in - total_units_out
        
        # Unit costs
        unit_costs = [r.get('UnitCost') for r in inventory_records if r.get('UnitCost') is not None]
        
        # Current balances (latest per product)
        product_balances = {}
        for r in inventory_records:
            pk = r.get('ProductKey')
            if pk and r.get('UnitsBalance') is not None:
                # Keep latest (assuming records are in order)
                product_balances[pk] = r.get('UnitsBalance')
        
        total_current_inventory = sum(product_balances.values())
        
        lines = [
            "INVENTORY DATABASE STATISTICS",
            "==============================",
            "",
            f"TOTAL INVENTORY RECORDS: {total_records}",
            f"UNIQUE PRODUCTS WITH INVENTORY: {len(unique_products)}",
            "",
            "MOVEMENT TOTALS:",
            f"- Total Units In: {total_units_in:,}",
            f"- Total Units Out: {total_units_out:,}",
            f"- Net Movement: {total_net_movement:,}",
            "",
            f"CURRENT TOTAL INVENTORY (sum of balances): {total_current_inventory:,} units",
            "",
        ]
        
        if unit_costs:
            avg_cost = sum(unit_costs) / len(unit_costs)
            max_cost = max(unit_costs)
            min_cost = min(unit_costs)
            lines.extend([
                "UNIT COST STATISTICS:",
                f"- Average unit cost: ${avg_cost:.2f}",
                f"- Highest unit cost: ${max_cost:.2f}",
                f"- Lowest unit cost: ${min_cost:.2f}",
                "",
            ])
        
        # Products with highest/lowest inventory
        if product_balances:
            sorted_balances = sorted(product_balances.items(), key=lambda x: x[1], reverse=True)
            
            lines.append("TOP 10 PRODUCTS BY INVENTORY BALANCE:")
            for pk, balance in sorted_balances[:10]:
                lines.append(f"  Product {pk}: {balance:,} units")
            
            lines.append("")
            lines.append("BOTTOM 10 PRODUCTS BY INVENTORY BALANCE:")
            for pk, balance in sorted_balances[-10:]:
                lines.append(f"  Product {pk}: {balance:,} units")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'inventory_summary',
            'total_records': total_records,
            'unique_products': len(unique_products),
            'total_units_in': total_units_in,
            'total_units_out': total_units_out,
            'total_inventory': total_current_inventory,
        }
        
        if unit_costs:
            metadata['avg_unit_cost'] = sum(unit_costs) / len(unit_costs)
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== MAIN PROCESSING ====================
    
    def process_tables(self, tables_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """Process all tables and create documents for embedding"""
        documents = []
        
        for table_name, df in tables_data.items():
            print(f"Processing table: {table_name} ({len(df)} rows)")
            
            # Determine table type based on name
            table_lower = table_name.lower()
            
            if 'inventory' in table_lower:
                # Process as inventory table
                inventory_info = []
                for idx, row in df.iterrows():
                    doc = self.process_inventory_row(row, table_name)
                    documents.append(doc)
                    inventory_info.append({
                        'ProductKey': row.get('ProductKey'),
                        'DateKey': row.get('DateKey'),
                        'UnitsIn': int(row.get('UnitsIn')) if pd.notna(row.get('UnitsIn')) else 0,
                        'UnitsOut': int(row.get('UnitsOut')) if pd.notna(row.get('UnitsOut')) else 0,
                        'UnitsBalance': int(row.get('UnitsBalance')) if pd.notna(row.get('UnitsBalance')) else 0,
                        'UnitCost': float(row.get('UnitCost')) if pd.notna(row.get('UnitCost')) else None,
                    })
                
                summary_doc = self.create_inventory_summary(inventory_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} inventory documents + 1 summary")
            
            elif 'dimproduct' in table_lower or (table_lower == 'dimproduct'):
                # Process as product table (DimProduct specifically)
                products_info = []
                for idx, row in df.iterrows():
                    doc = self.process_product_row(row, table_name)
                    documents.append(doc)
                    products_info.append({
                        'ProductKey': row.get('ProductKey'),
                        'EnglishProductName': row.get('EnglishProductName'),
                        'ListPrice': float(row.get('ListPrice')) if pd.notna(row.get('ListPrice')) else None,
                        'Color': row.get('Color') if pd.notna(row.get('Color')) else None,
                        'ProductLine': row.get('ProductLine') if pd.notna(row.get('ProductLine')) else None,
                        'Class': row.get('Class') if pd.notna(row.get('Class')) else None,
                    })
                
                summary_doc = self.create_product_summary(products_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} product documents + 1 summary")
                
            elif 'customer' in table_lower:
                # Process as customer table
                customers_info = []
                for idx, row in df.iterrows():
                    doc = self.process_customer_row(row, table_name)
                    documents.append(doc)
                    customers_info.append({
                        'CustomerKey': row.get('CustomerKey'),
                        'FullName': doc['metadata'].get('FullName'),
                        'Gender': row.get('Gender') if pd.notna(row.get('Gender')) else None,
                        'MaritalStatus': row.get('MaritalStatus') if pd.notna(row.get('MaritalStatus')) else None,
                        'YearlyIncome': float(row.get('YearlyIncome')) if pd.notna(row.get('YearlyIncome')) else None,
                        'Occupation': row.get('EnglishOccupation') if pd.notna(row.get('EnglishOccupation')) else None,
                        'Education': row.get('EnglishEducation') if pd.notna(row.get('EnglishEducation')) else None,
                    })
                
                summary_doc = self.create_customer_summary(customers_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} customer documents + 1 summary")
            
            else:
                # Generic processing for unknown tables
                print(f"  Warning: Unknown table type '{table_name}', processing as generic")
                for idx, row in df.iterrows():
                    lines = [f"Record from {table_name}:"]
                    for col, val in row.items():
                        if pd.notna(val):
                            lines.append(f"{col}: {val}")
                    
                    documents.append({
                        'text': "\n".join(lines),
                        'metadata': {'table': table_name, 'type': 'generic', 'row_index': idx}
                    })
                print(f"  Created {len(df)} generic documents")
        
        print(f"\nTotal documents created: {len(documents)}")
        return documents
