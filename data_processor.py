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
    
    # ==================== COMPETITOR PRODUCT PROCESSING ====================
    
    def process_competitor_product_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a competitor product row into a rich, complete document"""
        
        product_key = row.get('ProductKey', 'Unknown')
        product_name = row.get('EnglishProductName', 'Unknown Product')
        company = row.get('Company', 'Unknown Company')
        
        lines = []
        
        # === PRODUCT IDENTITY ===
        lines.append(f"COMPETITOR PRODUCT: {product_name}")
        lines.append(f"Company: {company}")
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
            'type': 'competitor_product',
            'ProductKey': int(product_key) if pd.notna(product_key) else 0,
            'EnglishProductName': str(product_name),
            'Company': str(company),
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
    
    def create_competitor_product_summary(self, products: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise competitor product summary document"""
        
        total_products = len(products)
        
        # Group by company
        company_counts = {}
        company_products = {}
        for p in products:
            company = p.get('Company', 'Unknown')
            company_counts[company] = company_counts.get(company, 0) + 1
            if company not in company_products:
                company_products[company] = []
            company_products[company].append(p)
        
        prices = []
        for p in products:
            price = p.get('ListPrice')
            if price is not None:
                prices.append((price, p.get('EnglishProductName', 'Unknown'), p.get('Company', 'Unknown')))
        
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
            "COMPETITOR PRODUCT DATABASE STATISTICS",
            "=======================================",
            "",
            f"TOTAL COMPETITOR PRODUCTS: {total_products}",
            "",
        ]
        
        # Company breakdown
        if company_counts:
            lines.append("PRODUCTS BY COMPANY:")
            for company, count in sorted(company_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_products) * 100
                lines.append(f"- {company}: {count} products ({pct:.1f}%)")
            lines.append("")
        
        if prices:
            prices_sorted = sorted(prices, key=lambda x: x[0], reverse=True)
            avg_price = sum(p[0] for p in prices) / len(prices)
            
            lines.extend([
                "PRICING:",
                f"- Products with prices: {len(prices)}",
                f"- Average price: ${avg_price:.2f}",
                f"- Highest price: ${prices_sorted[0][0]:.2f} ({prices_sorted[0][1]} - {prices_sorted[0][2]})",
                f"- Lowest price: ${prices_sorted[-1][0]:.2f} ({prices_sorted[-1][1]} - {prices_sorted[-1][2]})",
                "",
                "TOP 10 MOST EXPENSIVE:",
            ])
            for price, name, company in prices_sorted[:10]:
                lines.append(f"  ${price:.2f} - {name} ({company})")
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
            'type': 'competitor_product_summary',
            'total_products': total_products,
        }
        
        if prices:
            metadata['avg_price'] = sum(p[0] for p in prices) / len(prices)
            metadata['max_price'] = max(p[0] for p in prices)
            metadata['min_price'] = min(p[0] for p in prices)
        
        if company_counts:
            metadata['companies'] = list(company_counts.keys())
            metadata['company_counts'] = company_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== COMPETITOR SALES PROCESSING ====================
    
    def process_competitor_sales_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a competitor sales row into a document"""
        
        product_key = row.get('ProductKey', 'Unknown')
        order_date_key = row.get('OrderDateKey', 'Unknown')
        company = row.get('Company', 'Unknown Company')
        
        lines = []
        
        lines.append(f"COMPETITOR SALES RECORD")
        lines.append(f"Company: {company}")
        lines.append(f"Product Key: {product_key}")
        lines.append(f"Order Date Key: {order_date_key}")
        
        if pd.notna(row.get('SalesTerritoryKey')):
            lines.append(f"Sales Territory Key: {row.get('SalesTerritoryKey')}")
        
        if pd.notna(row.get('OrderQuantity')):
            lines.append(f"Order Quantity: {float(row.get('OrderQuantity')):.3f}")
        
        if pd.notna(row.get('UnitPrice')):
            lines.append(f"Unit Price: ${float(row.get('UnitPrice')):.2f}")
        
        if pd.notna(row.get('StandardCost')):
            lines.append(f"Standard Cost: ${float(row.get('StandardCost')):.2f}")
        
        if pd.notna(row.get('TotalProductCost')):
            lines.append(f"Total Product Cost: ${float(row.get('TotalProductCost')):.2f}")
        
        if pd.notna(row.get('SalesAmount')):
            lines.append(f"Sales Amount: ${float(row.get('SalesAmount')):.2f}")
        
        # Calculate profit
        sales_amount = float(row.get('SalesAmount', 0)) if pd.notna(row.get('SalesAmount')) else 0
        total_cost = float(row.get('TotalProductCost', 0)) if pd.notna(row.get('TotalProductCost')) else 0
        profit = sales_amount - total_cost
        if sales_amount > 0 or total_cost > 0:
            lines.append(f"Profit: ${profit:.2f}")
            if sales_amount > 0:
                profit_margin = (profit / sales_amount) * 100
                lines.append(f"Profit Margin: {profit_margin:.2f}%")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'competitor_sales',
            'ProductKey': int(product_key) if pd.notna(product_key) else 0,
            'OrderDateKey': int(order_date_key) if pd.notna(order_date_key) else 0,
            'Company': str(company),
        }
        
        if pd.notna(row.get('SalesAmount')):
            metadata['SalesAmount'] = float(row.get('SalesAmount'))
        if pd.notna(row.get('OrderQuantity')):
            metadata['OrderQuantity'] = float(row.get('OrderQuantity'))
        if pd.notna(row.get('UnitPrice')):
            metadata['UnitPrice'] = float(row.get('UnitPrice'))
        if pd.notna(row.get('SalesTerritoryKey')):
            metadata['SalesTerritoryKey'] = int(row.get('SalesTerritoryKey'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_competitor_sales_summary(self, sales_records: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise competitor sales summary document"""
        
        total_records = len(sales_records)
        
        # Group by company
        company_counts = {}
        company_sales = {}
        for r in sales_records:
            company = r.get('Company', 'Unknown')
            company_counts[company] = company_counts.get(company, 0) + 1
            if company not in company_sales:
                company_sales[company] = []
            company_sales[company].append(r)
        
        # Unique products
        unique_products = set(r.get('ProductKey') for r in sales_records if r.get('ProductKey'))
        
        # Sales totals
        total_sales_amount = sum(r.get('SalesAmount', 0) or 0 for r in sales_records)
        total_order_quantity = sum(r.get('OrderQuantity', 0) or 0 for r in sales_records)
        total_product_cost = sum(r.get('TotalProductCost', 0) or 0 for r in sales_records if r.get('TotalProductCost'))
        total_profit = total_sales_amount - total_product_cost
        
        # Unit prices
        unit_prices = [r.get('UnitPrice') for r in sales_records if r.get('UnitPrice') is not None]
        
        lines = [
            "COMPETITOR SALES DATABASE STATISTICS",
            "====================================",
            "",
            f"TOTAL SALES RECORDS: {total_records}",
            f"UNIQUE PRODUCTS SOLD: {len(unique_products)}",
            "",
        ]
        
        # Company breakdown
        if company_counts:
            lines.append("SALES RECORDS BY COMPANY:")
            for company, count in sorted(company_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_records) * 100
                company_total_sales = sum(r.get('SalesAmount', 0) or 0 for r in company_sales[company])
                lines.append(f"- {company}: {count} records ({pct:.1f}%), Total Sales: ${company_total_sales:,.2f}")
            lines.append("")
        
        lines.extend([
            "SALES TOTALS:",
            f"- Total Sales Amount: ${total_sales_amount:,.2f}",
            f"- Total Order Quantity: {total_order_quantity:,.3f}",
            f"- Total Product Cost: ${total_product_cost:,.2f}",
            f"- Total Profit: ${total_profit:,.2f}",
            "",
        ])
        
        if total_sales_amount > 0:
            profit_margin = (total_profit / total_sales_amount) * 100
            lines.append(f"Overall Profit Margin: {profit_margin:.2f}%")
            lines.append("")
        
        if unit_prices:
            avg_price = sum(unit_prices) / len(unit_prices)
            max_price = max(unit_prices)
            min_price = min(unit_prices)
            lines.extend([
                "UNIT PRICE STATISTICS:",
                f"- Average unit price: ${avg_price:.2f}",
                f"- Highest unit price: ${max_price:.2f}",
                f"- Lowest unit price: ${min_price:.2f}",
                "",
            ])
        
        # Top products by sales amount
        product_sales = {}
        for r in sales_records:
            pk = r.get('ProductKey')
            sales_amt = r.get('SalesAmount', 0) or 0
            if pk:
                if pk not in product_sales:
                    product_sales[pk] = 0
                product_sales[pk] += sales_amt
        
        if product_sales:
            sorted_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)
            lines.append("TOP 10 PRODUCTS BY SALES AMOUNT:")
            for pk, sales_amt in sorted_products[:10]:
                lines.append(f"  Product {pk}: ${sales_amt:,.2f}")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'competitor_sales_summary',
            'total_records': total_records,
            'unique_products': len(unique_products),
            'total_sales_amount': total_sales_amount,
            'total_order_quantity': total_order_quantity,
            'total_profit': total_profit,
        }
        
        if unit_prices:
            metadata['avg_unit_price'] = sum(unit_prices) / len(unit_prices)
        
        if company_counts:
            metadata['companies'] = list(company_counts.keys())
            metadata['company_counts'] = company_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== ACCOUNT PROCESSING ====================
    
    def process_account_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert an account row into a rich, complete document"""
        
        account_key = row.get('AccountKey', 'Unknown')
        account_description = row.get('AccountDescription', 'Unknown Account')
        
        lines = []
        
        # === ACCOUNT IDENTITY ===
        lines.append(f"ACCOUNT: {account_description}")
        lines.append(f"Account Key: {account_key}")
        
        if pd.notna(row.get('AccountCodeAlternateKey')):
            lines.append(f"Account Code: {row.get('AccountCodeAlternateKey')}")
        
        if pd.notna(row.get('ParentAccountKey')):
            lines.append(f"Parent Account Key: {row.get('ParentAccountKey')}")
        
        if pd.notna(row.get('ParentAccountCodeAlternateKey')):
            lines.append(f"Parent Account Code: {row.get('ParentAccountCodeAlternateKey')}")
        
        # === ACCOUNT CLASSIFICATION ===
        if pd.notna(row.get('AccountType')):
            lines.append(f"Account Type: {row.get('AccountType')}")
        
        if pd.notna(row.get('ValueType')):
            lines.append(f"Value Type: {row.get('ValueType')}")
        
        if pd.notna(row.get('Operator')):
            lines.append(f"Operator: {row.get('Operator')}")
        
        # === CUSTOM MEMBERS ===
        if pd.notna(row.get('CustomMembers')):
            lines.append(f"Custom Members: {row.get('CustomMembers')}")
        
        if pd.notna(row.get('CustomMemberOptions')):
            lines.append(f"Custom Member Options: {row.get('CustomMemberOptions')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'account',
            'AccountKey': int(account_key) if pd.notna(account_key) else 0,
            'AccountDescription': str(account_description),
        }
        
        if pd.notna(row.get('AccountType')):
            metadata['AccountType'] = str(row.get('AccountType'))
        if pd.notna(row.get('ParentAccountKey')):
            metadata['ParentAccountKey'] = int(row.get('ParentAccountKey'))
        if pd.notna(row.get('ValueType')):
            metadata['ValueType'] = str(row.get('ValueType'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_account_summary(self, accounts: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise account summary document"""
        
        total_accounts = len(accounts)
        
        # Account type breakdown
        account_type_counts = {}
        for a in accounts:
            acc_type = a.get('AccountType')
            if acc_type:
                account_type_counts[acc_type] = account_type_counts.get(acc_type, 0) + 1
        
        # Value type breakdown
        value_type_counts = {}
        for a in accounts:
            val_type = a.get('ValueType')
            if val_type:
                value_type_counts[val_type] = value_type_counts.get(val_type, 0) + 1
        
        # Hierarchical structure
        parent_accounts = sum(1 for a in accounts if a.get('ParentAccountKey'))
        root_accounts = total_accounts - parent_accounts
        
        lines = [
            "ACCOUNT DATABASE STATISTICS",
            "===========================",
            "",
            f"TOTAL ACCOUNTS: {total_accounts}",
            f"- Root accounts (no parent): {root_accounts}",
            f"- Child accounts (with parent): {parent_accounts}",
            "",
        ]
        
        if account_type_counts:
            lines.append("ACCOUNT TYPES:")
            for acc_type, count in sorted(account_type_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_accounts) * 100
                lines.append(f"- {acc_type}: {count} ({pct:.1f}%)")
            lines.append("")
        
        if value_type_counts:
            lines.append("VALUE TYPES:")
            for val_type, count in sorted(value_type_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_accounts) * 100
                lines.append(f"- {val_type}: {count} ({pct:.1f}%)")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'account_summary',
            'total_accounts': total_accounts,
            'root_accounts': root_accounts,
            'child_accounts': parent_accounts,
        }
        
        if account_type_counts:
            metadata['account_types'] = account_type_counts
        if value_type_counts:
            metadata['value_types'] = value_type_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== CURRENCY PROCESSING ====================
    
    def process_currency_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a currency row into a document"""
        
        currency_key = row.get('CurrencyKey', 'Unknown')
        currency_name = row.get('CurrencyName', 'Unknown Currency')
        currency_code = row.get('CurrencyAlternateKey', 'Unknown')
        
        lines = []
        
        lines.append(f"CURRENCY: {currency_name}")
        lines.append(f"Currency Key: {currency_key}")
        lines.append(f"Currency Code: {currency_code}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'currency',
            'CurrencyKey': int(currency_key) if pd.notna(currency_key) else 0,
            'CurrencyName': str(currency_name),
            'CurrencyAlternateKey': str(currency_code),
        }
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_currency_summary(self, currencies: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise currency summary document"""
        
        total_currencies = len(currencies)
        
        lines = [
            "CURRENCY DATABASE STATISTICS",
            "============================",
            "",
            f"TOTAL CURRENCIES: {total_currencies}",
            "",
            "CURRENCIES:",
        ]
        
        for curr in currencies:
            name = curr.get('CurrencyName', 'Unknown')
            code = curr.get('CurrencyAlternateKey', 'Unknown')
            lines.append(f"- {name} ({code})")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'currency_summary',
            'total_currencies': total_currencies,
        }
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== DATE PROCESSING ====================
    
    def process_date_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a date row into a rich document"""
        
        date_key = row.get('DateKey', 'Unknown')
        full_date = row.get('FullDateAlternateKey', 'Unknown Date')
        
        lines = []
        
        lines.append(f"DATE: {full_date}")
        lines.append(f"Date Key: {date_key}")
        
        # === DAY INFORMATION ===
        if pd.notna(row.get('DayNumberOfWeek')):
            lines.append(f"Day Number of Week: {row.get('DayNumberOfWeek')}")
        
        if pd.notna(row.get('EnglishDayNameOfWeek')):
            lines.append(f"Day Name (English): {row.get('EnglishDayNameOfWeek')}")
        
        if pd.notna(row.get('SpanishDayNameOfWeek')):
            lines.append(f"Day Name (Spanish): {row.get('SpanishDayNameOfWeek')}")
        
        if pd.notna(row.get('FrenchDayNameOfWeek')):
            lines.append(f"Day Name (French): {row.get('FrenchDayNameOfWeek')}")
        
        if pd.notna(row.get('DayNumberOfMonth')):
            lines.append(f"Day of Month: {row.get('DayNumberOfMonth')}")
        
        if pd.notna(row.get('DayNumberOfYear')):
            lines.append(f"Day of Year: {row.get('DayNumberOfYear')}")
        
        # === WEEK INFORMATION ===
        if pd.notna(row.get('WeekNumberOfYear')):
            lines.append(f"Week Number of Year: {row.get('WeekNumberOfYear')}")
        
        # === MONTH INFORMATION ===
        if pd.notna(row.get('EnglishMonthName')):
            lines.append(f"Month Name (English): {row.get('EnglishMonthName')}")
        
        if pd.notna(row.get('SpanishMonthName')):
            lines.append(f"Month Name (Spanish): {row.get('SpanishMonthName')}")
        
        if pd.notna(row.get('FrenchMonthName')):
            lines.append(f"Month Name (French): {row.get('FrenchMonthName')}")
        
        if pd.notna(row.get('MonthNumberOfYear')):
            lines.append(f"Month Number: {row.get('MonthNumberOfYear')}")
        
        # === CALENDAR INFORMATION ===
        if pd.notna(row.get('CalendarQuarter')):
            lines.append(f"Calendar Quarter: Q{row.get('CalendarQuarter')}")
        
        if pd.notna(row.get('CalendarYear')):
            lines.append(f"Calendar Year: {row.get('CalendarYear')}")
        
        if pd.notna(row.get('CalendarSemester')):
            lines.append(f"Calendar Semester: {row.get('CalendarSemester')}")
        
        # === FISCAL INFORMATION ===
        if pd.notna(row.get('FiscalQuarter')):
            lines.append(f"Fiscal Quarter: Q{row.get('FiscalQuarter')}")
        
        if pd.notna(row.get('FiscalYear')):
            lines.append(f"Fiscal Year: {row.get('FiscalYear')}")
        
        if pd.notna(row.get('FiscalSemester')):
            lines.append(f"Fiscal Semester: {row.get('FiscalSemester')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'date',
            'DateKey': int(date_key) if pd.notna(date_key) else 0,
            'FullDateAlternateKey': str(full_date),
        }
        
        if pd.notna(row.get('CalendarYear')):
            metadata['CalendarYear'] = int(row.get('CalendarYear'))
        if pd.notna(row.get('CalendarQuarter')):
            metadata['CalendarQuarter'] = int(row.get('CalendarQuarter'))
        if pd.notna(row.get('FiscalYear')):
            metadata['FiscalYear'] = int(row.get('FiscalYear'))
        if pd.notna(row.get('FiscalQuarter')):
            metadata['FiscalQuarter'] = int(row.get('FiscalQuarter'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_date_summary(self, dates: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise date summary document"""
        
        total_dates = len(dates)
        
        # Year range
        calendar_years = [d.get('CalendarYear') for d in dates if d.get('CalendarYear')]
        fiscal_years = [d.get('FiscalYear') for d in dates if d.get('FiscalYear')]
        
        # Quarter distribution
        calendar_quarters = {}
        for d in dates:
            q = d.get('CalendarQuarter')
            if q:
                calendar_quarters[q] = calendar_quarters.get(q, 0) + 1
        
        fiscal_quarters = {}
        for d in dates:
            q = d.get('FiscalQuarter')
            if q:
                fiscal_quarters[q] = fiscal_quarters.get(q, 0) + 1
        
        lines = [
            "DATE DATABASE STATISTICS",
            "=========================",
            "",
            f"TOTAL DATES: {total_dates}",
            "",
        ]
        
        if calendar_years:
            min_year = min(calendar_years)
            max_year = max(calendar_years)
            lines.extend([
                "CALENDAR YEAR RANGE:",
                f"- From: {min_year}",
                f"- To: {max_year}",
                f"- Span: {max_year - min_year + 1} years",
                "",
            ])
        
        if fiscal_years:
            min_fiscal = min(fiscal_years)
            max_fiscal = max(fiscal_years)
            lines.extend([
                "FISCAL YEAR RANGE:",
                f"- From: {min_fiscal}",
                f"- To: {max_fiscal}",
                f"- Span: {max_fiscal - min_fiscal + 1} years",
                "",
            ])
        
        if calendar_quarters:
            lines.append("CALENDAR QUARTER DISTRIBUTION:")
            for q in sorted(calendar_quarters.keys()):
                count = calendar_quarters[q]
                pct = (count / total_dates) * 100
                lines.append(f"- Q{q}: {count} dates ({pct:.1f}%)")
            lines.append("")
        
        if fiscal_quarters:
            lines.append("FISCAL QUARTER DISTRIBUTION:")
            for q in sorted(fiscal_quarters.keys()):
                count = fiscal_quarters[q]
                pct = (count / total_dates) * 100
                lines.append(f"- Q{q}: {count} dates ({pct:.1f}%)")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'date_summary',
            'total_dates': total_dates,
        }
        
        if calendar_years:
            metadata['min_calendar_year'] = min(calendar_years)
            metadata['max_calendar_year'] = max(calendar_years)
        if fiscal_years:
            metadata['min_fiscal_year'] = min(fiscal_years)
            metadata['max_fiscal_year'] = max(fiscal_years)
        if calendar_quarters:
            metadata['calendar_quarters'] = calendar_quarters
        if fiscal_quarters:
            metadata['fiscal_quarters'] = fiscal_quarters
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== DEPARTMENT GROUP PROCESSING ====================
    
    def process_department_group_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a department group row into a document"""
        
        dept_group_key = row.get('DepartmentGroupKey', 'Unknown')
        dept_group_name = row.get('DepartmentGroupName', 'Unknown Department Group')
        
        lines = []
        
        lines.append(f"DEPARTMENT GROUP: {dept_group_name}")
        lines.append(f"Department Group Key: {dept_group_key}")
        
        if pd.notna(row.get('ParentDepartmentGroupKey')):
            lines.append(f"Parent Department Group Key: {row.get('ParentDepartmentGroupKey')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'department_group',
            'DepartmentGroupKey': int(dept_group_key) if pd.notna(dept_group_key) else 0,
            'DepartmentGroupName': str(dept_group_name),
        }
        
        if pd.notna(row.get('ParentDepartmentGroupKey')):
            metadata['ParentDepartmentGroupKey'] = int(row.get('ParentDepartmentGroupKey'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_department_group_summary(self, dept_groups: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise department group summary document"""
        
        total_groups = len(dept_groups)
        
        # Hierarchical structure
        parent_groups = sum(1 for d in dept_groups if d.get('ParentDepartmentGroupKey'))
        root_groups = total_groups - parent_groups
        
        lines = [
            "DEPARTMENT GROUP DATABASE STATISTICS",
            "====================================",
            "",
            f"TOTAL DEPARTMENT GROUPS: {total_groups}",
            f"- Root groups (no parent): {root_groups}",
            f"- Child groups (with parent): {parent_groups}",
            "",
            "DEPARTMENT GROUPS:",
        ]
        
        for dg in dept_groups:
            name = dg.get('DepartmentGroupName', 'Unknown')
            parent = dg.get('ParentDepartmentGroupKey')
            if parent:
                lines.append(f"- {name} (Parent: {parent})")
            else:
                lines.append(f"- {name} (Root)")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'department_group_summary',
            'total_groups': total_groups,
            'root_groups': root_groups,
            'child_groups': parent_groups,
        }
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== EMPLOYEE PROCESSING ====================
    
    def process_employee_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert an employee row into a rich, complete document"""
        
        employee_key = row.get('EmployeeKey', 'Unknown')
        
        # Build full name
        first_name = row.get('FirstName', '') if pd.notna(row.get('FirstName')) else ''
        middle_name = row.get('MiddleName', '') if pd.notna(row.get('MiddleName')) else ''
        last_name = row.get('LastName', '') if pd.notna(row.get('LastName')) else ''
        
        name_parts = [first_name, middle_name, last_name]
        full_name = ' '.join(p for p in name_parts if p).strip() or 'Unknown Employee'
        
        lines = []
        
        # === EMPLOYEE IDENTITY ===
        lines.append(f"EMPLOYEE: {full_name}")
        lines.append(f"Employee Key: {employee_key}")
        
        if pd.notna(row.get('EmployeeNationalIDAlternateKey')):
            lines.append(f"National ID: {row.get('EmployeeNationalIDAlternateKey')}")
        
        if pd.notna(row.get('ParentEmployeeKey')):
            lines.append(f"Manager/Parent Employee Key: {row.get('ParentEmployeeKey')}")
        
        if pd.notna(row.get('ParentEmployeeNationalIDAlternateKey')):
            lines.append(f"Manager National ID: {row.get('ParentEmployeeNationalIDAlternateKey')}")
        
        if pd.notna(row.get('Title')):
            lines.append(f"Title: {row.get('Title')}")
        
        # === DEPARTMENT & TERRITORY ===
        if pd.notna(row.get('DepartmentName')):
            lines.append(f"Department: {row.get('DepartmentName')}")
        
        if pd.notna(row.get('SalesTerritoryKey')):
            lines.append(f"Sales Territory Key: {row.get('SalesTerritoryKey')}")
        
        # === EMPLOYMENT STATUS ===
        if pd.notna(row.get('Status')):
            lines.append(f"Status: {row.get('Status')}")
        
        current_flag = row.get('CurrentFlag', False) if pd.notna(row.get('CurrentFlag')) else False
        lines.append(f"Current Employee: {'Yes' if current_flag else 'No'}")
        
        sales_person_flag = row.get('SalesPersonFlag', False) if pd.notna(row.get('SalesPersonFlag')) else False
        lines.append(f"Sales Person: {'Yes' if sales_person_flag else 'No'}")
        
        # === DATES ===
        if pd.notna(row.get('HireDate')):
            lines.append(f"Hire Date: {row.get('HireDate')}")
        
        if pd.notna(row.get('BirthDate')):
            lines.append(f"Birth Date: {row.get('BirthDate')}")
        
        if pd.notna(row.get('StartDate')):
            lines.append(f"Start Date: {row.get('StartDate')}")
        
        if pd.notna(row.get('EndDate')):
            lines.append(f"End Date: {row.get('EndDate')}")
        
        # === DEMOGRAPHICS ===
        if pd.notna(row.get('Gender')):
            gender = row.get('Gender')
            gender_display = {'M': 'Male', 'F': 'Female'}.get(gender, gender)
            lines.append(f"Gender: {gender_display}")
        
        if pd.notna(row.get('MaritalStatus')):
            status = row.get('MaritalStatus')
            status_display = {'M': 'Married', 'S': 'Single'}.get(status, status)
            lines.append(f"Marital Status: {status_display}")
        
        # === COMPENSATION ===
        if pd.notna(row.get('BaseRate')):
            lines.append(f"Base Rate: ${float(row.get('BaseRate')):,.4f}")
        
        if pd.notna(row.get('PayFrequency')):
            freq = row.get('PayFrequency')
            freq_names = {1: 'Monthly', 2: 'Bi-weekly', 3: 'Weekly', 4: 'Daily'}
            freq_display = freq_names.get(freq, f'Frequency {freq}')
            lines.append(f"Pay Frequency: {freq_display}")
        
        salaried_flag = row.get('SalariedFlag', False) if pd.notna(row.get('SalariedFlag')) else False
        lines.append(f"Salaried: {'Yes' if salaried_flag else 'No'}")
        
        # === TIME OFF ===
        if pd.notna(row.get('VacationHours')):
            lines.append(f"Vacation Hours: {row.get('VacationHours')}")
        
        if pd.notna(row.get('SickLeaveHours')):
            lines.append(f"Sick Leave Hours: {row.get('SickLeaveHours')}")
        
        # === CONTACT INFO ===
        if pd.notna(row.get('EmailAddress')):
            lines.append(f"Email: {row.get('EmailAddress')}")
        
        if pd.notna(row.get('Phone')):
            lines.append(f"Phone: {row.get('Phone')}")
        
        if pd.notna(row.get('LoginID')):
            lines.append(f"Login ID: {row.get('LoginID')}")
        
        # === EMERGENCY CONTACT ===
        if pd.notna(row.get('EmergencyContactName')):
            lines.append(f"Emergency Contact: {row.get('EmergencyContactName')}")
        
        if pd.notna(row.get('EmergencyContactPhone')):
            lines.append(f"Emergency Contact Phone: {row.get('EmergencyContactPhone')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'employee',
            'EmployeeKey': int(employee_key) if pd.notna(employee_key) else 0,
            'FullName': full_name,
        }
        
        if pd.notna(row.get('DepartmentName')):
            metadata['DepartmentName'] = str(row.get('DepartmentName'))
        if pd.notna(row.get('Gender')):
            metadata['Gender'] = str(row.get('Gender'))
        if pd.notna(row.get('Status')):
            metadata['Status'] = str(row.get('Status'))
        if pd.notna(row.get('SalesTerritoryKey')):
            metadata['SalesTerritoryKey'] = int(row.get('SalesTerritoryKey'))
        if pd.notna(row.get('ParentEmployeeKey')):
            metadata['ParentEmployeeKey'] = int(row.get('ParentEmployeeKey'))
        metadata['CurrentFlag'] = current_flag
        metadata['SalesPersonFlag'] = sales_person_flag
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_employee_summary(self, employees: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise employee summary document"""
        
        total_employees = len(employees)
        
        # Current vs former
        current_employees = sum(1 for e in employees if e.get('CurrentFlag', False))
        former_employees = total_employees - current_employees
        
        # Department breakdown
        department_counts = {}
        for e in employees:
            dept = e.get('DepartmentName')
            if dept:
                department_counts[dept] = department_counts.get(dept, 0) + 1
        
        # Gender breakdown
        gender_counts = {}
        for e in employees:
            gender = e.get('Gender')
            if gender:
                gender_display = {'M': 'Male', 'F': 'Female'}.get(gender, gender)
                gender_counts[gender_display] = gender_counts.get(gender_display, 0) + 1
        
        # Status breakdown
        status_counts = {}
        for e in employees:
            status = e.get('Status')
            if status:
                status_counts[status] = status_counts.get(status, 0) + 1
        
        # Sales people
        sales_people = sum(1 for e in employees if e.get('SalesPersonFlag', False))
        
        # Hierarchical structure
        managers = sum(1 for e in employees if e.get('ParentEmployeeKey'))
        
        lines = [
            "EMPLOYEE DATABASE STATISTICS",
            "============================",
            "",
            f"TOTAL EMPLOYEES: {total_employees}",
            f"- Current employees: {current_employees}",
            f"- Former employees: {former_employees}",
            f"- Sales people: {sales_people}",
            f"- Employees with managers: {managers}",
            "",
        ]
        
        if department_counts:
            lines.append("EMPLOYEES BY DEPARTMENT:")
            for dept, count in sorted(department_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_employees) * 100
                lines.append(f"- {dept}: {count} ({pct:.1f}%)")
            lines.append("")
        
        if gender_counts:
            lines.append("GENDER DISTRIBUTION:")
            for gender, count in sorted(gender_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_employees) * 100
                lines.append(f"- {gender}: {count} ({pct:.1f}%)")
            lines.append("")
        
        if status_counts:
            lines.append("STATUS DISTRIBUTION:")
            for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_employees) * 100
                lines.append(f"- {status}: {count} ({pct:.1f}%)")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'employee_summary',
            'total_employees': total_employees,
            'current_employees': current_employees,
            'former_employees': former_employees,
            'sales_people': sales_people,
        }
        
        if department_counts:
            metadata['departments'] = department_counts
        if gender_counts:
            metadata['gender_distribution'] = gender_counts
        if status_counts:
            metadata['status_distribution'] = status_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== GEOGRAPHY PROCESSING ====================
    
    def process_geography_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a geography row into a rich document"""
        
        geography_key = row.get('GeographyKey', 'Unknown')
        
        lines = []
        
        # Build location string
        location_parts = []
        if pd.notna(row.get('City')):
            location_parts.append(row.get('City'))
        if pd.notna(row.get('StateProvinceName')):
            location_parts.append(row.get('StateProvinceName'))
        if pd.notna(row.get('EnglishCountryRegionName')):
            location_parts.append(row.get('EnglishCountryRegionName'))
        
        location = ', '.join(location_parts) if location_parts else 'Unknown Location'
        
        lines.append(f"GEOGRAPHY: {location}")
        lines.append(f"Geography Key: {geography_key}")
        
        # === CITY ===
        if pd.notna(row.get('City')):
            lines.append(f"City: {row.get('City')}")
        
        # === STATE/PROVINCE ===
        if pd.notna(row.get('StateProvinceName')):
            lines.append(f"State/Province: {row.get('StateProvinceName')}")
        
        if pd.notna(row.get('StateProvinceCode')):
            lines.append(f"State/Province Code: {row.get('StateProvinceCode')}")
        
        # === COUNTRY ===
        if pd.notna(row.get('EnglishCountryRegionName')):
            lines.append(f"Country (English): {row.get('EnglishCountryRegionName')}")
        
        if pd.notna(row.get('SpanishCountryRegionName')):
            lines.append(f"Country (Spanish): {row.get('SpanishCountryRegionName')}")
        
        if pd.notna(row.get('FrenchCountryRegionName')):
            lines.append(f"Country (French): {row.get('FrenchCountryRegionName')}")
        
        if pd.notna(row.get('CountryRegionCode')):
            lines.append(f"Country Code: {row.get('CountryRegionCode')}")
        
        # === POSTAL CODE ===
        if pd.notna(row.get('PostalCode')):
            lines.append(f"Postal Code: {row.get('PostalCode')}")
        
        # === TERRITORY & IP ===
        if pd.notna(row.get('SalesTerritoryKey')):
            lines.append(f"Sales Territory Key: {row.get('SalesTerritoryKey')}")
        
        if pd.notna(row.get('IpAddressLocator')):
            lines.append(f"IP Address Locator: {row.get('IpAddressLocator')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'geography',
            'GeographyKey': int(geography_key) if pd.notna(geography_key) else 0,
            'Location': location,
        }
        
        if pd.notna(row.get('City')):
            metadata['City'] = str(row.get('City'))
        if pd.notna(row.get('StateProvinceName')):
            metadata['StateProvinceName'] = str(row.get('StateProvinceName'))
        if pd.notna(row.get('EnglishCountryRegionName')):
            metadata['Country'] = str(row.get('EnglishCountryRegionName'))
        if pd.notna(row.get('CountryRegionCode')):
            metadata['CountryCode'] = str(row.get('CountryRegionCode'))
        if pd.notna(row.get('SalesTerritoryKey')):
            metadata['SalesTerritoryKey'] = int(row.get('SalesTerritoryKey'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_geography_summary(self, geographies: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise geography summary document"""
        
        total_locations = len(geographies)
        
        # Country breakdown
        country_counts = {}
        for g in geographies:
            country = g.get('Country')
            if country:
                country_counts[country] = country_counts.get(country, 0) + 1
        
        # State breakdown
        state_counts = {}
        for g in geographies:
            state = g.get('StateProvinceName')
            if state:
                state_counts[state] = state_counts.get(state, 0) + 1
        
        # City breakdown
        city_counts = {}
        for g in geographies:
            city = g.get('City')
            if city:
                city_counts[city] = city_counts.get(city, 0) + 1
        
        lines = [
            "GEOGRAPHY DATABASE STATISTICS",
            "=============================",
            "",
            f"TOTAL LOCATIONS: {total_locations}",
            "",
        ]
        
        if country_counts:
            lines.append("LOCATIONS BY COUNTRY:")
            for country, count in sorted(country_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_locations) * 100
                lines.append(f"- {country}: {count} ({pct:.1f}%)")
            lines.append("")
        
        if state_counts:
            lines.append("TOP 10 STATES/PROVINCES:")
            for state, count in sorted(state_counts.items(), key=lambda x: -x[1])[:10]:
                pct = (count / total_locations) * 100
                lines.append(f"- {state}: {count} ({pct:.1f}%)")
            lines.append("")
        
        if city_counts:
            lines.append("TOP 10 CITIES:")
            for city, count in sorted(city_counts.items(), key=lambda x: -x[1])[:10]:
                pct = (count / total_locations) * 100
                lines.append(f"- {city}: {count} ({pct:.1f}%)")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'geography_summary',
            'total_locations': total_locations,
        }
        
        if country_counts:
            metadata['countries'] = country_counts
        if state_counts:
            metadata['states'] = dict(list(sorted(state_counts.items(), key=lambda x: -x[1])[:10]))
        if city_counts:
            metadata['cities'] = dict(list(sorted(city_counts.items(), key=lambda x: -x[1])[:10]))
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== ORGANIZATION PROCESSING ====================
    
    def process_organization_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert an organization row into a document"""
        
        org_key = row.get('OrganizationKey', 'Unknown')
        org_name = row.get('OrganizationName', 'Unknown Organization')
        
        lines = []
        
        lines.append(f"ORGANIZATION: {org_name}")
        lines.append(f"Organization Key: {org_key}")
        
        if pd.notna(row.get('ParentOrganizationKey')):
            lines.append(f"Parent Organization Key: {row.get('ParentOrganizationKey')}")
        
        if pd.notna(row.get('PercentageOfOwnership')):
            lines.append(f"Percentage of Ownership: {row.get('PercentageOfOwnership')}")
        
        if pd.notna(row.get('CurrencyKey')):
            lines.append(f"Currency Key: {row.get('CurrencyKey')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'organization',
            'OrganizationKey': int(org_key) if pd.notna(org_key) else 0,
            'OrganizationName': str(org_name),
        }
        
        if pd.notna(row.get('ParentOrganizationKey')):
            metadata['ParentOrganizationKey'] = int(row.get('ParentOrganizationKey'))
        if pd.notna(row.get('CurrencyKey')):
            metadata['CurrencyKey'] = int(row.get('CurrencyKey'))
        if pd.notna(row.get('PercentageOfOwnership')):
            metadata['PercentageOfOwnership'] = str(row.get('PercentageOfOwnership'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_organization_summary(self, organizations: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise organization summary document"""
        
        total_orgs = len(organizations)
        
        # Hierarchical structure
        parent_orgs = sum(1 for o in organizations if o.get('ParentOrganizationKey'))
        root_orgs = total_orgs - parent_orgs
        
        lines = [
            "ORGANIZATION DATABASE STATISTICS",
            "================================",
            "",
            f"TOTAL ORGANIZATIONS: {total_orgs}",
            f"- Root organizations (no parent): {root_orgs}",
            f"- Child organizations (with parent): {parent_orgs}",
            "",
            "ORGANIZATIONS:",
        ]
        
        for org in organizations:
            name = org.get('OrganizationName', 'Unknown')
            parent = org.get('ParentOrganizationKey')
            ownership = org.get('PercentageOfOwnership')
            if parent:
                parent_str = f"Parent: {parent}"
            else:
                parent_str = "Root"
            
            if ownership:
                lines.append(f"- {name} ({parent_str}, Ownership: {ownership})")
            else:
                lines.append(f"- {name} ({parent_str})")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'organization_summary',
            'total_organizations': total_orgs,
            'root_organizations': root_orgs,
            'child_organizations': parent_orgs,
        }
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== PRODUCT CATEGORY PROCESSING ====================
    
    def process_product_category_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a product category row into a document"""
        
        category_key = row.get('ProductCategoryKey', 'Unknown')
        category_name = row.get('EnglishProductCategoryName', 'Unknown Category')
        
        lines = []
        
        lines.append(f"PRODUCT CATEGORY: {category_name}")
        lines.append(f"Product Category Key: {category_key}")
        
        if pd.notna(row.get('ProductCategoryAlternateKey')):
            lines.append(f"Category Alternate Key: {row.get('ProductCategoryAlternateKey')}")
        
        # Multilingual names
        if pd.notna(row.get('SpanishProductCategoryName')):
            lines.append(f"Category Name (Spanish): {row.get('SpanishProductCategoryName')}")
        
        if pd.notna(row.get('FrenchProductCategoryName')):
            lines.append(f"Category Name (French): {row.get('FrenchProductCategoryName')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'product_category',
            'ProductCategoryKey': int(category_key) if pd.notna(category_key) else 0,
            'EnglishProductCategoryName': str(category_name),
        }
        
        if pd.notna(row.get('SpanishProductCategoryName')):
            metadata['SpanishProductCategoryName'] = str(row.get('SpanishProductCategoryName'))
        if pd.notna(row.get('FrenchProductCategoryName')):
            metadata['FrenchProductCategoryName'] = str(row.get('FrenchProductCategoryName'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_product_category_summary(self, categories: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise product category summary document"""
        
        total_categories = len(categories)
        
        lines = [
            "PRODUCT CATEGORY DATABASE STATISTICS",
            "====================================",
            "",
            f"TOTAL PRODUCT CATEGORIES: {total_categories}",
            "",
            "CATEGORIES:",
        ]
        
        for cat in categories:
            name = cat.get('EnglishProductCategoryName', 'Unknown')
            lines.append(f"- {name}")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'product_category_summary',
            'total_categories': total_categories,
        }
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== PRODUCT SUBCATEGORY PROCESSING ====================
    
    def process_product_subcategory_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a product subcategory row into a document"""
        
        subcategory_key = row.get('ProductSubcategoryKey', 'Unknown')
        subcategory_name = row.get('EnglishProductSubcategoryName', 'Unknown Subcategory')
        
        lines = []
        
        lines.append(f"PRODUCT SUBCATEGORY: {subcategory_name}")
        lines.append(f"Product Subcategory Key: {subcategory_key}")
        
        if pd.notna(row.get('ProductSubcategoryAlternateKey')):
            lines.append(f"Subcategory Alternate Key: {row.get('ProductSubcategoryAlternateKey')}")
        
        if pd.notna(row.get('ProductCategoryKey')):
            lines.append(f"Product Category Key: {row.get('ProductCategoryKey')}")
        
        # Multilingual names
        if pd.notna(row.get('SpanishProductSubcategoryName')):
            lines.append(f"Subcategory Name (Spanish): {row.get('SpanishProductSubcategoryName')}")
        
        if pd.notna(row.get('FrenchProductSubcategoryName')):
            lines.append(f"Subcategory Name (French): {row.get('FrenchProductSubcategoryName')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'product_subcategory',
            'ProductSubcategoryKey': int(subcategory_key) if pd.notna(subcategory_key) else 0,
            'EnglishProductSubcategoryName': str(subcategory_name),
        }
        
        if pd.notna(row.get('ProductCategoryKey')):
            metadata['ProductCategoryKey'] = int(row.get('ProductCategoryKey'))
        if pd.notna(row.get('SpanishProductSubcategoryName')):
            metadata['SpanishProductSubcategoryName'] = str(row.get('SpanishProductSubcategoryName'))
        if pd.notna(row.get('FrenchProductSubcategoryName')):
            metadata['FrenchProductSubcategoryName'] = str(row.get('FrenchProductSubcategoryName'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_product_subcategory_summary(self, subcategories: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise product subcategory summary document"""
        
        total_subcategories = len(subcategories)
        
        # Group by category
        category_counts = {}
        category_subcategories = {}
        for sc in subcategories:
            cat_key = sc.get('ProductCategoryKey')
            if cat_key:
                category_counts[cat_key] = category_counts.get(cat_key, 0) + 1
                if cat_key not in category_subcategories:
                    category_subcategories[cat_key] = []
                category_subcategories[cat_key].append(sc.get('EnglishProductSubcategoryName', 'Unknown'))
        
        lines = [
            "PRODUCT SUBCATEGORY DATABASE STATISTICS",
            "=======================================",
            "",
            f"TOTAL PRODUCT SUBCATEGORIES: {total_subcategories}",
            "",
        ]
        
        if category_counts:
            lines.append("SUBCATEGORIES BY CATEGORY:")
            for cat_key, count in sorted(category_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_subcategories) * 100
                subcats = category_subcategories.get(cat_key, [])
                lines.append(f"- Category {cat_key}: {count} subcategories ({pct:.1f}%)")
                for subcat in subcats[:5]:  # Show first 5 subcategories per category
                    lines.append(f"  • {subcat}")
                if len(subcats) > 5:
                    lines.append(f"  ... and {len(subcats) - 5} more")
            lines.append("")
        
        lines.append("ALL SUBCATEGORIES:")
        for sc in subcategories:
            name = sc.get('EnglishProductSubcategoryName', 'Unknown')
            cat_key = sc.get('ProductCategoryKey')
            if cat_key:
                lines.append(f"- {name} (Category: {cat_key})")
            else:
                lines.append(f"- {name}")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'product_subcategory_summary',
            'total_subcategories': total_subcategories,
        }
        
        if category_counts:
            metadata['category_counts'] = category_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== PROFILE PROCESSING ====================
    
    def process_profile_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a profile row into a document"""
        
        profile_key = row.get('ProfileKey', 'Unknown')
        profile_name = row.get('ProfileName', 'Unknown Profile')
        platform = row.get('Platform', 'Unknown Platform')
        is_own_brand = row.get('IsOwnBrand', False) if pd.notna(row.get('IsOwnBrand')) else False
        
        lines = []
        
        lines.append(f"PROFILE: {profile_name}")
        lines.append(f"Profile Key: {profile_key}")
        lines.append(f"Platform: {platform}")
        lines.append(f"Own Brand: {'Yes' if is_own_brand else 'No'}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'profile',
            'ProfileKey': int(profile_key) if pd.notna(profile_key) else 0,
            'ProfileName': str(profile_name),
            'Platform': str(platform),
            'IsOwnBrand': bool(is_own_brand),
        }
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_profile_summary(self, profiles: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise profile summary document"""
        
        total_profiles = len(profiles)
        
        # Platform breakdown
        platform_counts = {}
        for p in profiles:
            platform = p.get('Platform')
            if platform:
                platform_counts[platform] = platform_counts.get(platform, 0) + 1
        
        # Own brand vs competitor breakdown
        own_brand_count = sum(1 for p in profiles if p.get('IsOwnBrand', False))
        competitor_count = total_profiles - own_brand_count
        
        lines = [
            "PROFILE DATABASE STATISTICS",
            "===========================",
            "",
            f"TOTAL PROFILES: {total_profiles}",
            f"- Own Brand Profiles: {own_brand_count}",
            f"- Competitor Profiles: {competitor_count}",
            "",
        ]
        
        if platform_counts:
            lines.append("PROFILES BY PLATFORM:")
            for platform, count in sorted(platform_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_profiles) * 100
                lines.append(f"- {platform}: {count} ({pct:.1f}%)")
            lines.append("")
        
        lines.append("ALL PROFILES:")
        for p in profiles:
            name = p.get('ProfileName', 'Unknown')
            platform = p.get('Platform', 'Unknown')
            is_own = p.get('IsOwnBrand', False)
            brand_type = "Own Brand" if is_own else "Competitor"
            lines.append(f"- {name} ({platform}, {brand_type})")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'profile_summary',
            'total_profiles': total_profiles,
            'own_brand_count': own_brand_count,
            'competitor_count': competitor_count,
        }
        
        if platform_counts:
            metadata['platform_counts'] = platform_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== PROMOTION PROCESSING ====================
    
    def process_promotion_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a promotion row into a rich document"""
        
        promotion_key = row.get('PromotionKey', 'Unknown')
        promotion_name = row.get('EnglishPromotionName', 'Unknown Promotion')
        
        lines = []
        
        lines.append(f"PROMOTION: {promotion_name}")
        lines.append(f"Promotion Key: {promotion_key}")
        
        if pd.notna(row.get('PromotionAlternateKey')):
            lines.append(f"Alternate Key: {row.get('PromotionAlternateKey')}")
        
        # Multilingual names
        if pd.notna(row.get('SpanishPromotionName')):
            lines.append(f"Promotion Name (Spanish): {row.get('SpanishPromotionName')}")
        if pd.notna(row.get('FrenchPromotionName')):
            lines.append(f"Promotion Name (French): {row.get('FrenchPromotionName')}")
        
        # Discount
        if pd.notna(row.get('DiscountPct')):
            lines.append(f"Discount Percentage: {float(row.get('DiscountPct')):.2f}%")
        
        # Type
        if pd.notna(row.get('EnglishPromotionType')):
            lines.append(f"Promotion Type (English): {row.get('EnglishPromotionType')}")
        if pd.notna(row.get('SpanishPromotionType')):
            lines.append(f"Promotion Type (Spanish): {row.get('SpanishPromotionType')}")
        if pd.notna(row.get('FrenchPromotionType')):
            lines.append(f"Promotion Type (French): {row.get('FrenchPromotionType')}")
        
        # Category
        if pd.notna(row.get('EnglishPromotionCategory')):
            lines.append(f"Promotion Category (English): {row.get('EnglishPromotionCategory')}")
        
        # Dates
        if pd.notna(row.get('StartDate')):
            lines.append(f"Start Date: {row.get('StartDate')}")
        if pd.notna(row.get('EndDate')):
            lines.append(f"End Date: {row.get('EndDate')}")
        
        # Quantity limits
        if pd.notna(row.get('MinQty')):
            lines.append(f"Minimum Quantity: {row.get('MinQty')}")
        if pd.notna(row.get('MaxQty')):
            lines.append(f"Maximum Quantity: {row.get('MaxQty')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'promotion',
            'PromotionKey': int(promotion_key) if pd.notna(promotion_key) else 0,
            'EnglishPromotionName': str(promotion_name),
        }
        
        if pd.notna(row.get('DiscountPct')):
            metadata['DiscountPct'] = float(row.get('DiscountPct'))
        if pd.notna(row.get('StartDate')):
            metadata['StartDate'] = str(row.get('StartDate'))
        if pd.notna(row.get('EndDate')):
            metadata['EndDate'] = str(row.get('EndDate'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_promotion_summary(self, promotions: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise promotion summary document"""
        
        total_promotions = len(promotions)
        
        # Active vs inactive
        active_promotions = sum(1 for p in promotions if p.get('StartDate') and not p.get('EndDate'))
        
        # Discount statistics
        discounts = [p.get('DiscountPct') for p in promotions if p.get('DiscountPct') is not None]
        
        lines = [
            "PROMOTION DATABASE STATISTICS",
            "=============================",
            "",
            f"TOTAL PROMOTIONS: {total_promotions}",
            "",
        ]
        
        if discounts:
            avg_discount = sum(discounts) / len(discounts)
            max_discount = max(discounts)
            min_discount = min(discounts)
            lines.extend([
                "DISCOUNT STATISTICS:",
                f"- Average discount: {avg_discount:.2f}%",
                f"- Maximum discount: {max_discount:.2f}%",
                f"- Minimum discount: {min_discount:.2f}%",
                "",
            ])
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'promotion_summary',
            'total_promotions': total_promotions,
        }
        
        if discounts:
            metadata['avg_discount'] = sum(discounts) / len(discounts)
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== RESELLER PROCESSING ====================
    
    def process_reseller_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a reseller row into a rich document"""
        
        reseller_key = row.get('ResellerKey', 'Unknown')
        reseller_name = row.get('ResellerName', 'Unknown Reseller')
        business_type = row.get('BusinessType', 'Unknown')
        
        lines = []
        
        lines.append(f"RESELLER: {reseller_name}")
        lines.append(f"Reseller Key: {reseller_key}")
        lines.append(f"Business Type: {business_type}")
        
        if pd.notna(row.get('ResellerAlternateKey')):
            lines.append(f"Alternate Key: {row.get('ResellerAlternateKey')}")
        
        if pd.notna(row.get('GeographyKey')):
            lines.append(f"Geography Key: {row.get('GeographyKey')}")
        
        if pd.notna(row.get('Phone')):
            lines.append(f"Phone: {row.get('Phone')}")
        
        if pd.notna(row.get('NumberEmployees')):
            lines.append(f"Number of Employees: {row.get('NumberEmployees')}")
        
        if pd.notna(row.get('ProductLine')):
            lines.append(f"Product Line: {row.get('ProductLine')}")
        
        if pd.notna(row.get('AddressLine1')):
            lines.append(f"Address: {row.get('AddressLine1')}")
        if pd.notna(row.get('AddressLine2')):
            lines.append(f"Address Line 2: {row.get('AddressLine2')}")
        
        if pd.notna(row.get('AnnualSales')):
            lines.append(f"Annual Sales: ${float(row.get('AnnualSales')):,.2f}")
        
        if pd.notna(row.get('AnnualRevenue')):
            lines.append(f"Annual Revenue: ${float(row.get('AnnualRevenue')):,.2f}")
        
        if pd.notna(row.get('YearOpened')):
            lines.append(f"Year Opened: {row.get('YearOpened')}")
        
        if pd.notna(row.get('FirstOrderYear')):
            lines.append(f"First Order Year: {row.get('FirstOrderYear')}")
        if pd.notna(row.get('LastOrderYear')):
            lines.append(f"Last Order Year: {row.get('LastOrderYear')}")
        
        if pd.notna(row.get('OrderFrequency')):
            lines.append(f"Order Frequency: {row.get('OrderFrequency')}")
        
        if pd.notna(row.get('BankName')):
            lines.append(f"Bank Name: {row.get('BankName')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'reseller',
            'ResellerKey': int(reseller_key) if pd.notna(reseller_key) else 0,
            'ResellerName': str(reseller_name),
            'BusinessType': str(business_type),
        }
        
        if pd.notna(row.get('GeographyKey')):
            metadata['GeographyKey'] = int(row.get('GeographyKey'))
        if pd.notna(row.get('AnnualSales')):
            metadata['AnnualSales'] = float(row.get('AnnualSales'))
        if pd.notna(row.get('AnnualRevenue')):
            metadata['AnnualRevenue'] = float(row.get('AnnualRevenue'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_reseller_summary(self, resellers: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise reseller summary document"""
        
        total_resellers = len(resellers)
        
        # Business type breakdown
        business_type_counts = {}
        for r in resellers:
            bt = r.get('BusinessType')
            if bt:
                business_type_counts[bt] = business_type_counts.get(bt, 0) + 1
        
        # Sales statistics
        annual_sales = [r.get('AnnualSales') for r in resellers if r.get('AnnualSales') is not None]
        
        lines = [
            "RESELLER DATABASE STATISTICS",
            "=============================",
            "",
            f"TOTAL RESELLERS: {total_resellers}",
            "",
        ]
        
        if business_type_counts:
            lines.append("RESELLERS BY BUSINESS TYPE:")
            for bt, count in sorted(business_type_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_resellers) * 100
                lines.append(f"- {bt}: {count} ({pct:.1f}%)")
            lines.append("")
        
        if annual_sales:
            total_sales = sum(annual_sales)
            avg_sales = total_sales / len(annual_sales)
            lines.extend([
                "ANNUAL SALES STATISTICS:",
                f"- Total annual sales: ${total_sales:,.2f}",
                f"- Average annual sales: ${avg_sales:,.2f}",
                "",
            ])
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'reseller_summary',
            'total_resellers': total_resellers,
        }
        
        if business_type_counts:
            metadata['business_types'] = business_type_counts
        if annual_sales:
            metadata['total_annual_sales'] = sum(annual_sales)
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== SALES REASON PROCESSING ====================
    
    def process_sales_reason_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a sales reason row into a document"""
        
        reason_key = row.get('SalesReasonKey', 'Unknown')
        reason_name = row.get('SalesReasonName', 'Unknown Reason')
        reason_type = row.get('SalesReasonReasonType', 'Unknown Type')
        
        lines = []
        
        lines.append(f"SALES REASON: {reason_name}")
        lines.append(f"Sales Reason Key: {reason_key}")
        lines.append(f"Reason Type: {reason_type}")
        
        if pd.notna(row.get('SalesReasonAlternateKey')):
            lines.append(f"Alternate Key: {row.get('SalesReasonAlternateKey')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'sales_reason',
            'SalesReasonKey': int(reason_key) if pd.notna(reason_key) else 0,
            'SalesReasonName': str(reason_name),
            'SalesReasonReasonType': str(reason_type),
        }
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_sales_reason_summary(self, reasons: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise sales reason summary document"""
        
        total_reasons = len(reasons)
        
        # Type breakdown
        type_counts = {}
        for r in reasons:
            rt = r.get('SalesReasonReasonType')
            if rt:
                type_counts[rt] = type_counts.get(rt, 0) + 1
        
        lines = [
            "SALES REASON DATABASE STATISTICS",
            "================================",
            "",
            f"TOTAL SALES REASONS: {total_reasons}",
            "",
        ]
        
        if type_counts:
            lines.append("REASONS BY TYPE:")
            for rt, count in sorted(type_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_reasons) * 100
                lines.append(f"- {rt}: {count} ({pct:.1f}%)")
            lines.append("")
        
        lines.append("ALL SALES REASONS:")
        for r in reasons:
            name = r.get('SalesReasonName', 'Unknown')
            rt = r.get('SalesReasonReasonType', 'Unknown')
            lines.append(f"- {name} ({rt})")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'sales_reason_summary',
            'total_reasons': total_reasons,
        }
        
        if type_counts:
            metadata['reason_types'] = type_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== SALES TERRITORY PROCESSING ====================
    
    def process_sales_territory_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a sales territory row into a document"""
        
        territory_key = row.get('SalesTerritoryKey', 'Unknown')
        territory_region = row.get('SalesTerritoryRegion', 'Unknown Region')
        territory_country = row.get('SalesTerritoryCountry', 'Unknown Country')
        
        lines = []
        
        lines.append(f"SALES TERRITORY: {territory_region}, {territory_country}")
        lines.append(f"Sales Territory Key: {territory_key}")
        lines.append(f"Region: {territory_region}")
        lines.append(f"Country: {territory_country}")
        
        if pd.notna(row.get('SalesTerritoryAlternateKey')):
            lines.append(f"Alternate Key: {row.get('SalesTerritoryAlternateKey')}")
        
        if pd.notna(row.get('SalesTerritoryGroup')):
            lines.append(f"Territory Group: {row.get('SalesTerritoryGroup')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'sales_territory',
            'SalesTerritoryKey': int(territory_key) if pd.notna(territory_key) else 0,
            'SalesTerritoryRegion': str(territory_region),
            'SalesTerritoryCountry': str(territory_country),
        }
        
        if pd.notna(row.get('SalesTerritoryGroup')):
            metadata['SalesTerritoryGroup'] = str(row.get('SalesTerritoryGroup'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_sales_territory_summary(self, territories: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise sales territory summary document"""
        
        total_territories = len(territories)
        
        # Country breakdown
        country_counts = {}
        for t in territories:
            country = t.get('SalesTerritoryCountry')
            if country:
                country_counts[country] = country_counts.get(country, 0) + 1
        
        # Group breakdown
        group_counts = {}
        for t in territories:
            group = t.get('SalesTerritoryGroup')
            if group:
                group_counts[group] = group_counts.get(group, 0) + 1
        
        lines = [
            "SALES TERRITORY DATABASE STATISTICS",
            "====================================",
            "",
            f"TOTAL SALES TERRITORIES: {total_territories}",
            "",
        ]
        
        if country_counts:
            lines.append("TERRITORIES BY COUNTRY:")
            for country, count in sorted(country_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_territories) * 100
                lines.append(f"- {country}: {count} ({pct:.1f}%)")
            lines.append("")
        
        if group_counts:
            lines.append("TERRITORIES BY GROUP:")
            for group, count in sorted(group_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_territories) * 100
                lines.append(f"- {group}: {count} ({pct:.1f}%)")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'sales_territory_summary',
            'total_territories': total_territories,
        }
        
        if country_counts:
            metadata['countries'] = country_counts
        if group_counts:
            metadata['groups'] = group_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== SCENARIO PROCESSING ====================
    
    def process_scenario_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a scenario row into a document"""
        
        scenario_key = row.get('ScenarioKey', 'Unknown')
        scenario_name = row.get('ScenarioName', 'Unknown Scenario')
        
        lines = []
        
        lines.append(f"SCENARIO: {scenario_name}")
        lines.append(f"Scenario Key: {scenario_key}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'scenario',
            'ScenarioKey': int(scenario_key) if pd.notna(scenario_key) else 0,
            'ScenarioName': str(scenario_name),
        }
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_scenario_summary(self, scenarios: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise scenario summary document"""
        
        total_scenarios = len(scenarios)
        
        lines = [
            "SCENARIO DATABASE STATISTICS",
            "============================",
            "",
            f"TOTAL SCENARIOS: {total_scenarios}",
            "",
            "SCENARIOS:",
        ]
        
        for s in scenarios:
            name = s.get('ScenarioName', 'Unknown')
            lines.append(f"- {name}")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'scenario_summary',
            'total_scenarios': total_scenarios,
        }
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== SCRAPE RUN PROCESSING ====================
    
    def process_scrape_run_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a scrape run row into a document"""
        
        scrape_run_id = row.get('ScrapeRunID', 'Unknown')
        scrape_timestamp = row.get('ScrapeTimestamp', 'Unknown Timestamp')
        
        lines = []
        
        lines.append(f"SCRAPE RUN: {scrape_run_id}")
        lines.append(f"Scrape Timestamp: {scrape_timestamp}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'scrape_run',
            'ScrapeRunID': int(scrape_run_id) if pd.notna(scrape_run_id) else 0,
            'ScrapeTimestamp': str(scrape_timestamp),
        }
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_scrape_run_summary(self, scrape_runs: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise scrape run summary document"""
        
        total_runs = len(scrape_runs)
        
        lines = [
            "SCRAPE RUN DATABASE STATISTICS",
            "===============================",
            "",
            f"TOTAL SCRAPE RUNS: {total_runs}",
            "",
        ]
        
        if scrape_runs:
            # Get earliest and latest timestamps
            timestamps = [r.get('ScrapeTimestamp') for r in scrape_runs if r.get('ScrapeTimestamp')]
            if timestamps:
                lines.append(f"Earliest Run: {min(timestamps)}")
                lines.append(f"Latest Run: {max(timestamps)}")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'scrape_run_summary',
            'total_runs': total_runs,
        }
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== ADDITIONAL PRODUCT DESCRIPTION PROCESSING ====================
    
    def process_additional_product_description_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert an additional product description row into a document"""
        
        product_key = row.get('ProductKey', 'Unknown')
        culture_name = row.get('CultureName', 'Unknown Culture')
        description = row.get('ProductDescription', 'No description')
        
        lines = []
        
        lines.append(f"ADDITIONAL PRODUCT DESCRIPTION")
        lines.append(f"Product Key: {product_key}")
        lines.append(f"Culture: {culture_name}")
        lines.append(f"Description: {description}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'additional_product_description',
            'ProductKey': int(product_key) if pd.notna(product_key) else 0,
            'CultureName': str(culture_name),
        }
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_additional_product_description_summary(self, descriptions: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise additional product description summary document"""
        
        total_descriptions = len(descriptions)
        
        # Culture breakdown
        culture_counts = {}
        for d in descriptions:
            culture = d.get('CultureName')
            if culture:
                culture_counts[culture] = culture_counts.get(culture, 0) + 1
        
        # Unique products
        unique_products = set(d.get('ProductKey') for d in descriptions if d.get('ProductKey'))
        
        lines = [
            "ADDITIONAL PRODUCT DESCRIPTION STATISTICS",
            "==========================================",
            "",
            f"TOTAL DESCRIPTIONS: {total_descriptions}",
            f"UNIQUE PRODUCTS WITH DESCRIPTIONS: {len(unique_products)}",
            "",
        ]
        
        if culture_counts:
            lines.append("DESCRIPTIONS BY CULTURE:")
            for culture, count in sorted(culture_counts.items(), key=lambda x: -x[1]):
                pct = (count / total_descriptions) * 100
                lines.append(f"- {culture}: {count} ({pct:.1f}%)")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'additional_product_description_summary',
            'total_descriptions': total_descriptions,
            'unique_products': len(unique_products),
        }
        
        if culture_counts:
            metadata['cultures'] = culture_counts
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== CALL CENTER PROCESSING ====================
    
    def process_call_center_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a call center row into a document"""
        
        fact_id = row.get('FactCallCenterID', 'Unknown')
        date_key = row.get('DateKey', 'Unknown')
        
        lines = []
        
        lines.append(f"CALL CENTER RECORD")
        lines.append(f"Fact Call Center ID: {fact_id}")
        lines.append(f"Date Key: {date_key}")
        
        if pd.notna(row.get('Date')):
            lines.append(f"Date: {row.get('Date')}")
        
        if pd.notna(row.get('WageType')):
            lines.append(f"Wage Type: {row.get('WageType')}")
        
        if pd.notna(row.get('Shift')):
            lines.append(f"Shift: {row.get('Shift')}")
        
        if pd.notna(row.get('LevelOneOperators')):
            lines.append(f"Level One Operators: {row.get('LevelOneOperators')}")
        
        if pd.notna(row.get('LevelTwoOperators')):
            lines.append(f"Level Two Operators: {row.get('LevelTwoOperators')}")
        
        if pd.notna(row.get('TotalOperators')):
            lines.append(f"Total Operators: {row.get('TotalOperators')}")
        
        if pd.notna(row.get('Calls')):
            lines.append(f"Calls: {row.get('Calls')}")
        
        if pd.notna(row.get('AutomaticResponses')):
            lines.append(f"Automatic Responses: {row.get('AutomaticResponses')}")
        
        if pd.notna(row.get('Orders')):
            lines.append(f"Orders: {row.get('Orders')}")
        
        if pd.notna(row.get('IssuesRaised')):
            lines.append(f"Issues Raised: {row.get('IssuesRaised')}")
        
        if pd.notna(row.get('AverageTimePerIssue')):
            lines.append(f"Average Time Per Issue: {row.get('AverageTimePerIssue')} minutes")
        
        if pd.notna(row.get('ServiceGrade')):
            lines.append(f"Service Grade: {float(row.get('ServiceGrade')):.2f}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'call_center',
            'FactCallCenterID': int(fact_id) if pd.notna(fact_id) else 0,
            'DateKey': int(date_key) if pd.notna(date_key) else 0,
        }
        
        if pd.notna(row.get('Calls')):
            metadata['Calls'] = int(row.get('Calls'))
        if pd.notna(row.get('ServiceGrade')):
            metadata['ServiceGrade'] = float(row.get('ServiceGrade'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_call_center_summary(self, call_center_records: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise call center summary document"""
        
        total_records = len(call_center_records)
        
        # Totals
        total_calls = sum(r.get('Calls', 0) or 0 for r in call_center_records)
        total_orders = sum(r.get('Orders', 0) or 0 for r in call_center_records)
        total_issues = sum(r.get('IssuesRaised', 0) or 0 for r in call_center_records)
        
        # Service grades
        service_grades = [r.get('ServiceGrade') for r in call_center_records if r.get('ServiceGrade') is not None]
        
        lines = [
            "CALL CENTER DATABASE STATISTICS",
            "===============================",
            "",
            f"TOTAL RECORDS: {total_records}",
            "",
            "TOTALS:",
            f"- Total Calls: {total_calls:,}",
            f"- Total Orders: {total_orders:,}",
            f"- Total Issues Raised: {total_issues:,}",
            "",
        ]
        
        if service_grades:
            avg_grade = sum(service_grades) / len(service_grades)
            lines.append(f"Average Service Grade: {avg_grade:.2f}")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'call_center_summary',
            'total_records': total_records,
            'total_calls': total_calls,
            'total_orders': total_orders,
            'total_issues': total_issues,
        }
        
        if service_grades:
            metadata['avg_service_grade'] = sum(service_grades) / len(service_grades)
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== CURRENCY RATE PROCESSING ====================
    
    def process_currency_rate_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a currency rate row into a document"""
        
        currency_key = row.get('CurrencyKey', 'Unknown')
        date_key = row.get('DateKey', 'Unknown')
        
        lines = []
        
        lines.append(f"CURRENCY RATE RECORD")
        lines.append(f"Currency Key: {currency_key}")
        lines.append(f"Date Key: {date_key}")
        
        if pd.notna(row.get('Date')):
            lines.append(f"Date: {row.get('Date')}")
        
        if pd.notna(row.get('AverageRate')):
            lines.append(f"Average Rate: {float(row.get('AverageRate')):.6f}")
        
        if pd.notna(row.get('EndOfDayRate')):
            lines.append(f"End of Day Rate: {float(row.get('EndOfDayRate')):.6f}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'currency_rate',
            'CurrencyKey': int(currency_key) if pd.notna(currency_key) else 0,
            'DateKey': int(date_key) if pd.notna(date_key) else 0,
        }
        
        if pd.notna(row.get('AverageRate')):
            metadata['AverageRate'] = float(row.get('AverageRate'))
        if pd.notna(row.get('EndOfDayRate')):
            metadata['EndOfDayRate'] = float(row.get('EndOfDayRate'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_currency_rate_summary(self, currency_rates: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise currency rate summary document"""
        
        total_records = len(currency_rates)
        
        # Unique currencies
        unique_currencies = set(r.get('CurrencyKey') for r in currency_rates if r.get('CurrencyKey'))
        
        # Rate statistics
        avg_rates = [r.get('AverageRate') for r in currency_rates if r.get('AverageRate') is not None]
        
        lines = [
            "CURRENCY RATE DATABASE STATISTICS",
            "==================================",
            "",
            f"TOTAL RATE RECORDS: {total_records}",
            f"UNIQUE CURRENCIES: {len(unique_currencies)}",
            "",
        ]
        
        if avg_rates:
            avg_rate = sum(avg_rates) / len(avg_rates)
            max_rate = max(avg_rates)
            min_rate = min(avg_rates)
            lines.extend([
                "RATE STATISTICS:",
                f"- Average rate: {avg_rate:.6f}",
                f"- Maximum rate: {max_rate:.6f}",
                f"- Minimum rate: {min_rate:.6f}",
            ])
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'currency_rate_summary',
            'total_records': total_records,
            'unique_currencies': len(unique_currencies),
        }
        
        if avg_rates:
            metadata['avg_rate'] = sum(avg_rates) / len(avg_rates)
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== FINANCE PROCESSING ====================
    
    def process_finance_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert a finance row into a document"""
        
        finance_key = row.get('FinanceKey', 'Unknown')
        date_key = row.get('DateKey', 'Unknown')
        amount = row.get('Amount', 0)
        
        lines = []
        
        lines.append(f"FINANCE RECORD")
        lines.append(f"Finance Key: {finance_key}")
        lines.append(f"Date Key: {date_key}")
        lines.append(f"Amount: ${float(amount):,.2f}")
        
        if pd.notna(row.get('Date')):
            lines.append(f"Date: {row.get('Date')}")
        
        if pd.notna(row.get('OrganizationKey')):
            lines.append(f"Organization Key: {row.get('OrganizationKey')}")
        
        if pd.notna(row.get('DepartmentGroupKey')):
            lines.append(f"Department Group Key: {row.get('DepartmentGroupKey')}")
        
        if pd.notna(row.get('ScenarioKey')):
            lines.append(f"Scenario Key: {row.get('ScenarioKey')}")
        
        if pd.notna(row.get('AccountKey')):
            lines.append(f"Account Key: {row.get('AccountKey')}")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'finance',
            'FinanceKey': int(finance_key) if pd.notna(finance_key) else 0,
            'DateKey': int(date_key) if pd.notna(date_key) else 0,
            'Amount': float(amount) if pd.notna(amount) else 0.0,
        }
        
        if pd.notna(row.get('OrganizationKey')):
            metadata['OrganizationKey'] = int(row.get('OrganizationKey'))
        if pd.notna(row.get('AccountKey')):
            metadata['AccountKey'] = int(row.get('AccountKey'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_finance_summary(self, finance_records: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise finance summary document"""
        
        total_records = len(finance_records)
        
        # Total amount
        total_amount = sum(r.get('Amount', 0) or 0 for r in finance_records)
        
        # Organization breakdown
        org_amounts = {}
        for r in finance_records:
            org_key = r.get('OrganizationKey')
            amount = r.get('Amount', 0) or 0
            if org_key:
                if org_key not in org_amounts:
                    org_amounts[org_key] = 0
                org_amounts[org_key] += amount
        
        lines = [
            "FINANCE DATABASE STATISTICS",
            "===========================",
            "",
            f"TOTAL FINANCE RECORDS: {total_records}",
            f"TOTAL AMOUNT: ${total_amount:,.2f}",
            "",
        ]
        
        if org_amounts:
            sorted_orgs = sorted(org_amounts.items(), key=lambda x: x[1], reverse=True)
            lines.append("TOP 10 ORGANIZATIONS BY AMOUNT:")
            for org_key, amount in sorted_orgs[:10]:
                lines.append(f"  Organization {org_key}: ${amount:,.2f}")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'finance_summary',
            'total_records': total_records,
            'total_amount': total_amount,
        }
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== INTERNET SALES PROCESSING ====================
    
    def process_internet_sales_row(self, row: pd.Series, table_name: str) -> Dict[str, Any]:
        """Convert an internet sales row into a rich document"""
        
        product_key = row.get('ProductKey', 'Unknown')
        customer_key = row.get('CustomerKey', 'Unknown')
        sales_order_number = row.get('SalesOrderNumber', 'Unknown')
        
        lines = []
        
        lines.append(f"INTERNET SALES RECORD")
        lines.append(f"Sales Order Number: {sales_order_number}")
        lines.append(f"Product Key: {product_key}")
        lines.append(f"Customer Key: {customer_key}")
        
        if pd.notna(row.get('SalesOrderLineNumber')):
            lines.append(f"Order Line Number: {row.get('SalesOrderLineNumber')}")
        
        if pd.notna(row.get('OrderDateKey')):
            lines.append(f"Order Date Key: {row.get('OrderDateKey')}")
        
        if pd.notna(row.get('OrderDate')):
            lines.append(f"Order Date: {row.get('OrderDate')}")
        
        if pd.notna(row.get('OrderQuantity')):
            lines.append(f"Order Quantity: {row.get('OrderQuantity')}")
        
        if pd.notna(row.get('UnitPrice')):
            lines.append(f"Unit Price: ${float(row.get('UnitPrice')):,.2f}")
        
        if pd.notna(row.get('ExtendedAmount')):
            lines.append(f"Extended Amount: ${float(row.get('ExtendedAmount')):,.2f}")
        
        if pd.notna(row.get('DiscountAmount')):
            lines.append(f"Discount Amount: ${float(row.get('DiscountAmount')):,.2f}")
        
        if pd.notna(row.get('SalesAmount')):
            lines.append(f"Sales Amount: ${float(row.get('SalesAmount')):,.2f}")
        
        if pd.notna(row.get('TaxAmt')):
            lines.append(f"Tax Amount: ${float(row.get('TaxAmt')):,.2f}")
        
        if pd.notna(row.get('Freight')):
            lines.append(f"Freight: ${float(row.get('Freight')):,.2f}")
        
        if pd.notna(row.get('PromotionKey')):
            lines.append(f"Promotion Key: {row.get('PromotionKey')}")
        
        if pd.notna(row.get('SalesTerritoryKey')):
            lines.append(f"Sales Territory Key: {row.get('SalesTerritoryKey')}")
        
        if pd.notna(row.get('CarrierTrackingNumber')):
            lines.append(f"Carrier Tracking Number: {row.get('CarrierTrackingNumber')}")
        
        # Calculate profit
        sales_amount = float(row.get('SalesAmount', 0)) if pd.notna(row.get('SalesAmount')) else 0
        total_cost = float(row.get('TotalProductCost', 0)) if pd.notna(row.get('TotalProductCost')) else 0
        profit = sales_amount - total_cost
        if sales_amount > 0 or total_cost > 0:
            lines.append(f"Profit: ${profit:,.2f}")
            if sales_amount > 0:
                profit_margin = (profit / sales_amount) * 100
                lines.append(f"Profit Margin: {profit_margin:.2f}%")
        
        full_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'internet_sales',
            'ProductKey': int(product_key) if pd.notna(product_key) else 0,
            'CustomerKey': int(customer_key) if pd.notna(customer_key) else 0,
            'SalesOrderNumber': str(sales_order_number),
        }
        
        if pd.notna(row.get('SalesAmount')):
            metadata['SalesAmount'] = float(row.get('SalesAmount'))
        if pd.notna(row.get('OrderQuantity')):
            metadata['OrderQuantity'] = int(row.get('OrderQuantity'))
        if pd.notna(row.get('OrderDateKey')):
            metadata['OrderDateKey'] = int(row.get('OrderDateKey'))
        
        return {'text': full_text, 'metadata': metadata}
    
    def create_internet_sales_summary(self, sales_records: List[Dict], table_name: str) -> Dict[str, Any]:
        """Create a concise internet sales summary document"""
        
        total_records = len(sales_records)
        
        # Unique products and customers
        unique_products = set(r.get('ProductKey') for r in sales_records if r.get('ProductKey'))
        unique_customers = set(r.get('CustomerKey') for r in sales_records if r.get('CustomerKey'))
        
        # Sales totals
        total_sales_amount = sum(r.get('SalesAmount', 0) or 0 for r in sales_records)
        total_order_quantity = sum(r.get('OrderQuantity', 0) or 0 for r in sales_records)
        total_discount = sum(r.get('DiscountAmount', 0) or 0 for r in sales_records if r.get('DiscountAmount'))
        
        lines = [
            "INTERNET SALES DATABASE STATISTICS",
            "===================================",
            "",
            f"TOTAL SALES RECORDS: {total_records}",
            f"UNIQUE PRODUCTS SOLD: {len(unique_products)}",
            f"UNIQUE CUSTOMERS: {len(unique_customers)}",
            "",
            "SALES TOTALS:",
            f"- Total Sales Amount: ${total_sales_amount:,.2f}",
            f"- Total Order Quantity: {total_order_quantity:,}",
            f"- Total Discount Amount: ${total_discount:,.2f}",
            "",
        ]
        
        # Top products by sales
        product_sales = {}
        for r in sales_records:
            pk = r.get('ProductKey')
            sales_amt = r.get('SalesAmount', 0) or 0
            if pk:
                if pk not in product_sales:
                    product_sales[pk] = 0
                product_sales[pk] += sales_amt
        
        if product_sales:
            sorted_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)
            lines.append("TOP 10 PRODUCTS BY SALES AMOUNT:")
            for pk, sales_amt in sorted_products[:10]:
                lines.append(f"  Product {pk}: ${sales_amt:,.2f}")
        
        summary_text = "\n".join(lines)
        
        metadata = {
            'table': table_name,
            'type': 'internet_sales_summary',
            'total_records': total_records,
            'unique_products': len(unique_products),
            'unique_customers': len(unique_customers),
            'total_sales_amount': total_sales_amount,
            'total_order_quantity': total_order_quantity,
        }
        
        return {'text': summary_text, 'metadata': metadata}
    
    # ==================== MAIN PROCESSING ====================
    
    def process_tables(self, tables_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """Process all tables and create documents for embedding"""
        documents = []
        
        for table_name, df in tables_data.items():
            print(f"Processing table: {table_name} ({len(df)} rows)")
            
            # Determine table type based on name
            table_lower = table_name.lower()
            
            if 'competitorfactinternetsales' in table_lower:
                # Process as competitor sales table
                sales_info = []
                for idx, row in df.iterrows():
                    doc = self.process_competitor_sales_row(row, table_name)
                    documents.append(doc)
                    sales_info.append({
                        'ProductKey': row.get('ProductKey'),
                        'OrderDateKey': row.get('OrderDateKey'),
                        'SalesTerritoryKey': int(row.get('SalesTerritoryKey')) if pd.notna(row.get('SalesTerritoryKey')) else None,
                        'OrderQuantity': float(row.get('OrderQuantity')) if pd.notna(row.get('OrderQuantity')) else 0,
                        'UnitPrice': float(row.get('UnitPrice')) if pd.notna(row.get('UnitPrice')) else None,
                        'SalesAmount': float(row.get('SalesAmount')) if pd.notna(row.get('SalesAmount')) else 0,
                        'TotalProductCost': float(row.get('TotalProductCost')) if pd.notna(row.get('TotalProductCost')) else None,
                        'Company': row.get('Company') if pd.notna(row.get('Company')) else None,
                    })
                
                summary_doc = self.create_competitor_sales_summary(sales_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} competitor sales documents + 1 summary")
            
            elif 'inventory' in table_lower:
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
            
            elif 'competitordimproduct' in table_lower:
                # Process as competitor product table
                products_info = []
                for idx, row in df.iterrows():
                    doc = self.process_competitor_product_row(row, table_name)
                    documents.append(doc)
                    products_info.append({
                        'ProductKey': row.get('ProductKey'),
                        'EnglishProductName': row.get('EnglishProductName'),
                        'ListPrice': float(row.get('ListPrice')) if pd.notna(row.get('ListPrice')) else None,
                        'Color': row.get('Color') if pd.notna(row.get('Color')) else None,
                        'ProductLine': row.get('ProductLine') if pd.notna(row.get('ProductLine')) else None,
                        'Class': row.get('Class') if pd.notna(row.get('Class')) else None,
                        'Company': row.get('Company') if pd.notna(row.get('Company')) else None,
                    })
                
                summary_doc = self.create_competitor_product_summary(products_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} competitor product documents + 1 summary")
            
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
            
            elif 'dimaccount' in table_lower:
                # Process as account table
                accounts_info = []
                for idx, row in df.iterrows():
                    doc = self.process_account_row(row, table_name)
                    documents.append(doc)
                    accounts_info.append({
                        'AccountKey': row.get('AccountKey'),
                        'AccountDescription': row.get('AccountDescription'),
                        'AccountType': row.get('AccountType') if pd.notna(row.get('AccountType')) else None,
                        'ValueType': row.get('ValueType') if pd.notna(row.get('ValueType')) else None,
                        'ParentAccountKey': int(row.get('ParentAccountKey')) if pd.notna(row.get('ParentAccountKey')) else None,
                    })
                
                summary_doc = self.create_account_summary(accounts_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} account documents + 1 summary")
            
            elif 'dimcurrency' in table_lower:
                # Process as currency table
                currencies_info = []
                for idx, row in df.iterrows():
                    doc = self.process_currency_row(row, table_name)
                    documents.append(doc)
                    currencies_info.append({
                        'CurrencyKey': row.get('CurrencyKey'),
                        'CurrencyName': row.get('CurrencyName'),
                        'CurrencyAlternateKey': row.get('CurrencyAlternateKey'),
                    })
                
                summary_doc = self.create_currency_summary(currencies_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} currency documents + 1 summary")
            
            elif 'dimdate' in table_lower:
                # Process as date table
                dates_info = []
                for idx, row in df.iterrows():
                    doc = self.process_date_row(row, table_name)
                    documents.append(doc)
                    dates_info.append({
                        'DateKey': row.get('DateKey'),
                        'FullDateAlternateKey': row.get('FullDateAlternateKey'),
                        'CalendarYear': int(row.get('CalendarYear')) if pd.notna(row.get('CalendarYear')) else None,
                        'CalendarQuarter': int(row.get('CalendarQuarter')) if pd.notna(row.get('CalendarQuarter')) else None,
                        'FiscalYear': int(row.get('FiscalYear')) if pd.notna(row.get('FiscalYear')) else None,
                        'FiscalQuarter': int(row.get('FiscalQuarter')) if pd.notna(row.get('FiscalQuarter')) else None,
                    })
                
                summary_doc = self.create_date_summary(dates_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} date documents + 1 summary")
            
            elif 'dimdepartmentgroup' in table_lower:
                # Process as department group table
                dept_groups_info = []
                for idx, row in df.iterrows():
                    doc = self.process_department_group_row(row, table_name)
                    documents.append(doc)
                    dept_groups_info.append({
                        'DepartmentGroupKey': row.get('DepartmentGroupKey'),
                        'DepartmentGroupName': row.get('DepartmentGroupName'),
                        'ParentDepartmentGroupKey': int(row.get('ParentDepartmentGroupKey')) if pd.notna(row.get('ParentDepartmentGroupKey')) else None,
                    })
                
                summary_doc = self.create_department_group_summary(dept_groups_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} department group documents + 1 summary")
            
            elif 'dimemployee' in table_lower:
                # Process as employee table
                employees_info = []
                for idx, row in df.iterrows():
                    doc = self.process_employee_row(row, table_name)
                    documents.append(doc)
                    employees_info.append({
                        'EmployeeKey': row.get('EmployeeKey'),
                        'FullName': doc['metadata'].get('FullName'),
                        'DepartmentName': row.get('DepartmentName') if pd.notna(row.get('DepartmentName')) else None,
                        'Gender': row.get('Gender') if pd.notna(row.get('Gender')) else None,
                        'Status': row.get('Status') if pd.notna(row.get('Status')) else None,
                        'SalesTerritoryKey': int(row.get('SalesTerritoryKey')) if pd.notna(row.get('SalesTerritoryKey')) else None,
                        'ParentEmployeeKey': int(row.get('ParentEmployeeKey')) if pd.notna(row.get('ParentEmployeeKey')) else None,
                        'CurrentFlag': bool(row.get('CurrentFlag')) if pd.notna(row.get('CurrentFlag')) else False,
                        'SalesPersonFlag': bool(row.get('SalesPersonFlag')) if pd.notna(row.get('SalesPersonFlag')) else False,
                    })
                
                summary_doc = self.create_employee_summary(employees_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} employee documents + 1 summary")
            
            elif 'dimgeography' in table_lower:
                # Process as geography table
                geographies_info = []
                for idx, row in df.iterrows():
                    doc = self.process_geography_row(row, table_name)
                    documents.append(doc)
                    geographies_info.append({
                        'GeographyKey': row.get('GeographyKey'),
                        'Location': doc['metadata'].get('Location'),
                        'City': row.get('City') if pd.notna(row.get('City')) else None,
                        'StateProvinceName': row.get('StateProvinceName') if pd.notna(row.get('StateProvinceName')) else None,
                        'Country': row.get('EnglishCountryRegionName') if pd.notna(row.get('EnglishCountryRegionName')) else None,
                        'CountryCode': row.get('CountryRegionCode') if pd.notna(row.get('CountryRegionCode')) else None,
                        'SalesTerritoryKey': int(row.get('SalesTerritoryKey')) if pd.notna(row.get('SalesTerritoryKey')) else None,
                    })
                
                summary_doc = self.create_geography_summary(geographies_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} geography documents + 1 summary")
            
            elif 'dimorganization' in table_lower:
                # Process as organization table
                organizations_info = []
                for idx, row in df.iterrows():
                    doc = self.process_organization_row(row, table_name)
                    documents.append(doc)
                    organizations_info.append({
                        'OrganizationKey': row.get('OrganizationKey'),
                        'OrganizationName': row.get('OrganizationName'),
                        'ParentOrganizationKey': int(row.get('ParentOrganizationKey')) if pd.notna(row.get('ParentOrganizationKey')) else None,
                        'CurrencyKey': int(row.get('CurrencyKey')) if pd.notna(row.get('CurrencyKey')) else None,
                        'PercentageOfOwnership': row.get('PercentageOfOwnership') if pd.notna(row.get('PercentageOfOwnership')) else None,
                    })
                
                summary_doc = self.create_organization_summary(organizations_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} organization documents + 1 summary")
            
            elif 'dimproductcategory' in table_lower:
                # Process as product category table
                categories_info = []
                for idx, row in df.iterrows():
                    doc = self.process_product_category_row(row, table_name)
                    documents.append(doc)
                    categories_info.append({
                        'ProductCategoryKey': row.get('ProductCategoryKey'),
                        'EnglishProductCategoryName': row.get('EnglishProductCategoryName'),
                        'SpanishProductCategoryName': row.get('SpanishProductCategoryName') if pd.notna(row.get('SpanishProductCategoryName')) else None,
                        'FrenchProductCategoryName': row.get('FrenchProductCategoryName') if pd.notna(row.get('FrenchProductCategoryName')) else None,
                    })
                
                summary_doc = self.create_product_category_summary(categories_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} product category documents + 1 summary")
            
            elif 'dimproductsubcategory' in table_lower:
                # Process as product subcategory table
                subcategories_info = []
                for idx, row in df.iterrows():
                    doc = self.process_product_subcategory_row(row, table_name)
                    documents.append(doc)
                    subcategories_info.append({
                        'ProductSubcategoryKey': row.get('ProductSubcategoryKey'),
                        'EnglishProductSubcategoryName': row.get('EnglishProductSubcategoryName'),
                        'ProductCategoryKey': int(row.get('ProductCategoryKey')) if pd.notna(row.get('ProductCategoryKey')) else None,
                        'SpanishProductSubcategoryName': row.get('SpanishProductSubcategoryName') if pd.notna(row.get('SpanishProductSubcategoryName')) else None,
                        'FrenchProductSubcategoryName': row.get('FrenchProductSubcategoryName') if pd.notna(row.get('FrenchProductSubcategoryName')) else None,
                    })
                
                summary_doc = self.create_product_subcategory_summary(subcategories_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} product subcategory documents + 1 summary")
            
            elif 'dimprofile' in table_lower:
                # Process as profile table
                profiles_info = []
                for idx, row in df.iterrows():
                    doc = self.process_profile_row(row, table_name)
                    documents.append(doc)
                    profiles_info.append({
                        'ProfileKey': row.get('ProfileKey'),
                        'ProfileName': row.get('ProfileName'),
                        'Platform': row.get('Platform'),
                        'IsOwnBrand': bool(row.get('IsOwnBrand')) if pd.notna(row.get('IsOwnBrand')) else False,
                    })
                
                summary_doc = self.create_profile_summary(profiles_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} profile documents + 1 summary")
            
            elif 'dimpromotion' in table_lower:
                # Process as promotion table
                promotions_info = []
                for idx, row in df.iterrows():
                    doc = self.process_promotion_row(row, table_name)
                    documents.append(doc)
                    promotions_info.append({
                        'PromotionKey': row.get('PromotionKey'),
                        'EnglishPromotionName': row.get('EnglishPromotionName'),
                        'DiscountPct': float(row.get('DiscountPct')) if pd.notna(row.get('DiscountPct')) else None,
                        'StartDate': str(row.get('StartDate')) if pd.notna(row.get('StartDate')) else None,
                        'EndDate': str(row.get('EndDate')) if pd.notna(row.get('EndDate')) else None,
                    })
                
                summary_doc = self.create_promotion_summary(promotions_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} promotion documents + 1 summary")
            
            elif 'dimreseller' in table_lower:
                # Process as reseller table
                resellers_info = []
                for idx, row in df.iterrows():
                    doc = self.process_reseller_row(row, table_name)
                    documents.append(doc)
                    resellers_info.append({
                        'ResellerKey': row.get('ResellerKey'),
                        'ResellerName': row.get('ResellerName'),
                        'BusinessType': row.get('BusinessType'),
                        'GeographyKey': int(row.get('GeographyKey')) if pd.notna(row.get('GeographyKey')) else None,
                        'AnnualSales': float(row.get('AnnualSales')) if pd.notna(row.get('AnnualSales')) else None,
                        'AnnualRevenue': float(row.get('AnnualRevenue')) if pd.notna(row.get('AnnualRevenue')) else None,
                    })
                
                summary_doc = self.create_reseller_summary(resellers_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} reseller documents + 1 summary")
            
            elif 'dimsalesreason' in table_lower:
                # Process as sales reason table
                reasons_info = []
                for idx, row in df.iterrows():
                    doc = self.process_sales_reason_row(row, table_name)
                    documents.append(doc)
                    reasons_info.append({
                        'SalesReasonKey': row.get('SalesReasonKey'),
                        'SalesReasonName': row.get('SalesReasonName'),
                        'SalesReasonReasonType': row.get('SalesReasonReasonType'),
                    })
                
                summary_doc = self.create_sales_reason_summary(reasons_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} sales reason documents + 1 summary")
            
            elif 'dimsalesterritory' in table_lower:
                # Process as sales territory table
                territories_info = []
                for idx, row in df.iterrows():
                    doc = self.process_sales_territory_row(row, table_name)
                    documents.append(doc)
                    territories_info.append({
                        'SalesTerritoryKey': row.get('SalesTerritoryKey'),
                        'SalesTerritoryRegion': row.get('SalesTerritoryRegion'),
                        'SalesTerritoryCountry': row.get('SalesTerritoryCountry'),
                        'SalesTerritoryGroup': row.get('SalesTerritoryGroup') if pd.notna(row.get('SalesTerritoryGroup')) else None,
                    })
                
                summary_doc = self.create_sales_territory_summary(territories_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} sales territory documents + 1 summary")
            
            elif 'dimscenario' in table_lower:
                # Process as scenario table
                scenarios_info = []
                for idx, row in df.iterrows():
                    doc = self.process_scenario_row(row, table_name)
                    documents.append(doc)
                    scenarios_info.append({
                        'ScenarioKey': row.get('ScenarioKey'),
                        'ScenarioName': row.get('ScenarioName'),
                    })
                
                summary_doc = self.create_scenario_summary(scenarios_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} scenario documents + 1 summary")
            
            elif 'dimscraperun' in table_lower:
                # Process as scrape run table
                scrape_runs_info = []
                for idx, row in df.iterrows():
                    doc = self.process_scrape_run_row(row, table_name)
                    documents.append(doc)
                    scrape_runs_info.append({
                        'ScrapeRunID': row.get('ScrapeRunID'),
                        'ScrapeTimestamp': str(row.get('ScrapeTimestamp')) if pd.notna(row.get('ScrapeTimestamp')) else None,
                    })
                
                summary_doc = self.create_scrape_run_summary(scrape_runs_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} scrape run documents + 1 summary")
            
            elif 'factadditionalinternationalproductdescription' in table_lower:
                # Process as additional product description table
                descriptions_info = []
                for idx, row in df.iterrows():
                    doc = self.process_additional_product_description_row(row, table_name)
                    documents.append(doc)
                    descriptions_info.append({
                        'ProductKey': row.get('ProductKey'),
                        'CultureName': row.get('CultureName'),
                    })
                
                summary_doc = self.create_additional_product_description_summary(descriptions_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} additional product description documents + 1 summary")
            
            elif 'factcallcenter' in table_lower:
                # Process as call center table
                call_center_info = []
                for idx, row in df.iterrows():
                    doc = self.process_call_center_row(row, table_name)
                    documents.append(doc)
                    call_center_info.append({
                        'FactCallCenterID': row.get('FactCallCenterID'),
                        'DateKey': row.get('DateKey'),
                        'Calls': int(row.get('Calls')) if pd.notna(row.get('Calls')) else 0,
                        'Orders': int(row.get('Orders')) if pd.notna(row.get('Orders')) else 0,
                        'IssuesRaised': int(row.get('IssuesRaised')) if pd.notna(row.get('IssuesRaised')) else 0,
                        'ServiceGrade': float(row.get('ServiceGrade')) if pd.notna(row.get('ServiceGrade')) else None,
                    })
                
                summary_doc = self.create_call_center_summary(call_center_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} call center documents + 1 summary")
            
            elif 'factcurrencyrate' in table_lower:
                # Process as currency rate table
                currency_rates_info = []
                for idx, row in df.iterrows():
                    doc = self.process_currency_rate_row(row, table_name)
                    documents.append(doc)
                    currency_rates_info.append({
                        'CurrencyKey': row.get('CurrencyKey'),
                        'DateKey': row.get('DateKey'),
                        'AverageRate': float(row.get('AverageRate')) if pd.notna(row.get('AverageRate')) else None,
                        'EndOfDayRate': float(row.get('EndOfDayRate')) if pd.notna(row.get('EndOfDayRate')) else None,
                    })
                
                summary_doc = self.create_currency_rate_summary(currency_rates_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} currency rate documents + 1 summary")
            
            elif 'factfinance' in table_lower:
                # Process as finance table
                finance_info = []
                for idx, row in df.iterrows():
                    doc = self.process_finance_row(row, table_name)
                    documents.append(doc)
                    finance_info.append({
                        'FinanceKey': row.get('FinanceKey'),
                        'DateKey': row.get('DateKey'),
                        'Amount': float(row.get('Amount')) if pd.notna(row.get('Amount')) else 0,
                        'OrganizationKey': int(row.get('OrganizationKey')) if pd.notna(row.get('OrganizationKey')) else None,
                        'AccountKey': int(row.get('AccountKey')) if pd.notna(row.get('AccountKey')) else None,
                    })
                
                summary_doc = self.create_finance_summary(finance_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} finance documents + 1 summary")
            
            elif 'factinternetsales' in table_lower:
                # Process as internet sales table
                sales_info = []
                for idx, row in df.iterrows():
                    doc = self.process_internet_sales_row(row, table_name)
                    documents.append(doc)
                    sales_info.append({
                        'ProductKey': row.get('ProductKey'),
                        'CustomerKey': row.get('CustomerKey'),
                        'SalesOrderNumber': row.get('SalesOrderNumber'),
                        'OrderDateKey': int(row.get('OrderDateKey')) if pd.notna(row.get('OrderDateKey')) else None,
                        'OrderQuantity': int(row.get('OrderQuantity')) if pd.notna(row.get('OrderQuantity')) else 0,
                        'SalesAmount': float(row.get('SalesAmount')) if pd.notna(row.get('SalesAmount')) else 0,
                    })
                
                summary_doc = self.create_internet_sales_summary(sales_info, table_name)
                documents.append(summary_doc)
                print(f"  Created {len(df)} internet sales documents + 1 summary")
            
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
