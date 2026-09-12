"""
Synthetic Data Generator for Apache Spark E-Commerce Analytics Project
Generates realistic e-commerce transaction records in CSV format.
"""

import csv
import os
import random
from datetime import datetime, timedelta

def generate_ecommerce_data(output_file: str, num_records: int = 5000):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    categories = {
        "Electronics": ["Smartphone", "Laptop", "Wireless Headphones", "Smartwatch", "4K Monitor", "Bluetooth Speaker"],
        "Clothing": ["Denim Jeans", "Cotton T-Shirt", "Hooded Sweatshirt", "Running Shoes", "Winter Jacket", "Sneakers"],
        "Home & Kitchen": ["Air Fryer", "Espresso Machine", "Blender", "Cookware Set", "Vacuum Cleaner", "Toaster"],
        "Books": ["Sci-Fi Novel", "Data Engineering Handbook", "Biography", "Cookbook", "History of Computing", "Economics 101"],
        "Sports & Outdoors": ["Yoga Mat", "Dumbbell Set", "Camping Tent", "Bicycle Helmet", "Water Bottle", "Hiking Backpack"]
    }
    
    payment_methods = ["Credit Card", "PayPal", "Debit Card", "UPI / Digital Wallet", "Apple Pay"]
    cities = ["New York", "London", "Tokyo", "Berlin", "San Francisco", "Sydney", "Toronto", "Paris", "Singapore", "Mumbai"]
    statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", "RETURNED", "PENDING"]
    
    base_date = datetime(2025, 1, 1)
    
    fieldnames = [
        "order_id",
        "customer_id",
        "customer_name",
        "city",
        "category",
        "product_name",
        "unit_price",
        "quantity",
        "discount_percent",
        "payment_method",
        "order_status",
        "order_timestamp"
    ]
    
    customer_names = [
        "Alice Smith", "Bob Jones", "Charlie Brown", "Diana Prince", "Evan Wright",
        "Fiona Gallagher", "George Clark", "Hannah Abbott", "Ian Malcolm", "Julia Roberts",
        "Kevin Bacon", "Laura Croft", "Michael Scott", "Nora Jones", "Oscar Martinez",
        "Pam Beesly", "Quentin Tarantino", "Rachel Green", "Steve Rogers", "Tony Stark"
    ]
    
    print(f"Generating {num_records} transaction records to {output_file}...")
    
    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(1, num_records + 1):
            category = random.choice(list(categories.keys()))
            product = random.choice(categories[category])
            
            # Base pricing logic by category
            if category == "Electronics":
                price = round(random.uniform(50.0, 1200.0), 2)
            elif category == "Home & Kitchen":
                price = round(random.uniform(25.0, 350.0), 2)
            elif category == "Clothing":
                price = round(random.uniform(15.0, 150.0), 2)
            elif category == "Books":
                price = round(random.uniform(8.0, 45.0), 2)
            else:
                price = round(random.uniform(15.0, 250.0), 2)
            
            # Simulated edge cases / dirty data for Spark cleaning practice:
            # 1. Occasional null customer_name or city
            # 2. Occasional zero or negative quantities
            # 3. Occasional missing discount
            cust_idx = random.randint(0, len(customer_names) - 1)
            customer_name = customer_names[cust_idx]
            city = random.choice(cities)
            
            # Inject ~2% nulls for data quality demonstration
            if random.random() < 0.02:
                customer_name = ""
            if random.random() < 0.02:
                city = ""
                
            qty = random.choices([1, 2, 3, 4, 5, -1], weights=[50, 25, 12, 8, 4, 1])[0]
            discount = round(random.choice([0.0, 0.05, 0.10, 0.15, 0.20, 0.25]), 2)
            
            random_seconds = random.randint(0, 365 * 24 * 3600)
            order_time = base_date + timedelta(seconds=random_seconds)
            
            row = {
                "order_id": f"ORD-{100000 + i}",
                "customer_id": f"CUST-{1000 + cust_idx}",
                "customer_name": customer_name,
                "city": city,
                "category": category,
                "product_name": product,
                "unit_price": price,
                "quantity": qty,
                "discount_percent": discount,
                "payment_method": random.choice(payment_methods),
                "order_status": random.choice(statuses),
                "order_timestamp": order_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            writer.writerow(row)
            
    print(f"Successfully generated {num_records} records in '{output_file}'.")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    target_csv = os.path.join(current_dir, "raw_transactions.csv")
    generate_ecommerce_data(target_csv, 5000)
