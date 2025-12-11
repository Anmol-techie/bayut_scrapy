from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['bayut_production']
collection = db['sublocation_properties']

# Update all documents to add purpose field with value "for-sale"
result = collection.update_many(
    {},  # Empty filter to match all documents
    {'$set': {'purpose': 'for-sale'}}
)

print(f"Successfully updated {result.modified_count} documents")
print(f"Matched {result.matched_count} documents")

# Verify by checking a sample document
sample = collection.find_one()
if sample:
    print(f"\nSample document after update:")
    print(f"Purpose field: {sample.get('purpose', 'Not found')}")