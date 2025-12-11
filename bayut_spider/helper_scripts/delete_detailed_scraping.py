from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['bayut_production']
collection = db['sublocation_properties']

# Delete the detailed_scraping field from all documents
result = collection.update_many(
    {},  # Empty filter to match all documents
    {'$unset': {'detailed_scraping': ""}}  # $unset removes the field
)

print(f"Successfully removed detailed_scraping field from {result.modified_count} documents")
print(f"Matched {result.matched_count} documents")

# Verify by checking a sample document
sample = collection.find_one()
if sample:
    print(f"\nSample document after update:")
    print(f"detailed_scraping field: {sample.get('detailed_scraping', 'Not found')}")