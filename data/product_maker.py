# Generates a local JSON product database with 500+ products
# Output: product_db.json

import json

CATEGORIES = {
    "Electronics": [
        "Mobile Phone", "Laptop", "Tablet", "Smart Watch", "Headphones",
        "Bluetooth Speaker", "Power Bank", "Keyboard", "Mouse", "Monitor",
        "Printer", "Router", "Webcam", "External Hard Drive", "USB Flash Drive",
        "Gaming Console", "Graphics Card", "Motherboard", "RAM", "SSD",
        "Soundbar", "Projector", "VR Headset", "Drone", "Digital Camera"
    ],
    "Clothing": [
        "T-Shirt", "Shirt", "Jeans", "Trousers", "Jacket", "Sweatshirt",
        "Kurta", "Kurti", "Saree", "Lehenga", "Blazer", "Shorts",
        "Skirt", "Top", "Track Pants", "Hoodie", "Cap", "Socks",
        "Raincoat", "Sweater", "Innerwear", "Nightwear"
    ],
    "Home": [
        "Mixer Grinder", "Washing Machine", "Refrigerator", "Microwave Oven",
        "Induction Cooktop", "Electric Kettle", "Iron", "Water Purifier",
        "Vacuum Cleaner", "Air Cooler", "Table Fan", "Ceiling Fan",
        "Lamp", "Curtains", "Bedsheet", "Pillow", "Mattress",
        "Dining Table", "Chair", "Sofa", "Wardrobe", "Bookshelf",
        "Wall Clock", "Door Mat"
    ],
    "Books": [
        "Novel", "Textbook", "Biography", "Comics", "Magazine",
        "Exam Guide", "Question Bank", "Notebook", "Diary",
        "Self Help Book", "Story Book", "Children Book",
        "Engineering Book", "Medical Book", "Law Book",
        "Maths Book", "Science Book", "History Book"
    ],
    "Beauty": [
        "Face Wash", "Face Cream", "Moisturizer", "Sunscreen", "Serum",
        "Perfume", "Deodorant", "Lipstick", "Foundation", "Compact Powder",
        "Hair Oil", "Shampoo", "Conditioner", "Hair Mask",
        "Beard Oil", "Trimmer", "Face Razor", "Hair Dryer",
        "Straightener", "Body Lotion"
    ],
    "Sports": [
        "Cricket Bat", "Cricket Ball", "Football", "Volleyball",
        "Badminton Racket", "Shuttlecock", "Tennis Racket",
        "Basketball", "Yoga Mat", "Skipping Rope", "Dumbbells",
        "Resistance Band", "Treadmill", "Gym Gloves", "Sports Shoes",
        "Helmet", "Knee Guard", "Cycling Gloves"
    ]
}

# ---------- GENERATE 500+ PRODUCTS ----------
product_db = []
product_id_counter = 100000

for category, base_products in CATEGORIES.items():
    for base in base_products:
        # Create multiple variants per product to scale count
        for variant in range(1, 6):  # 5 variants per product
            product_db.append({
                "product_id": f"PROD{product_id_counter}",
                "product_name": f"{base} Variant {variant}",
                "category": category
            })
            product_id_counter += 1

# ---------- SAVE JSON ----------
with open("product_db.json", "w", encoding="utf-8") as f:
    json.dump(product_db, f, indent=2)

print("product_db.json created")
print(f"Total products generated: {len(product_db)}")
