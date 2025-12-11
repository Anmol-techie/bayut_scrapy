#!/usr/bin/env python3
"""
Rename details_url field to detailed_url in all MongoDB documents
"""

from pymongo import MongoClient, UpdateOne
from datetime import datetime

def rename_field():
    """Rename details_url to detailed_url in all documents"""
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bayut_production']
    collection = db['sublocation_properties']
    
    print(f"🔄 Starting field rename: details_url → detailed_url")
    print(f"⏰ Started at: {datetime.now()}")
    
    # Count documents with the old field
    count_with_old = collection.count_documents({'details_url': {'$exists': True}})
    print(f"📊 Documents with 'details_url': {count_with_old:,}")
    
    if count_with_old == 0:
        # Check if already renamed
        count_with_new = collection.count_documents({'detailed_url': {'$exists': True}})
        if count_with_new > 0:
            print(f"✅ Field already renamed! Found {count_with_new:,} documents with 'detailed_url'")
            return
        else:
            print("⚠️  No documents have 'details_url' field")
            return
    
    # Rename the field using $rename operator
    print(f"🚀 Renaming field in {count_with_old:,} documents...")
    
    result = collection.update_many(
        {'details_url': {'$exists': True}},
        {'$rename': {'details_url': 'detailed_url'}}
    )
    
    print(f"✅ Successfully renamed field in {result.modified_count:,} documents")
    print(f"📊 Matched {result.matched_count:,} documents")
    
    # Verify the rename
    count_with_new = collection.count_documents({'detailed_url': {'$exists': True}})
    count_with_old_after = collection.count_documents({'details_url': {'$exists': True}})
    
    print(f"\n🔍 Verification:")
    print(f"  - Documents with 'detailed_url': {count_with_new:,}")
    print(f"  - Documents with 'details_url': {count_with_old_after:,}")
    
    # Show a sample document
    sample = collection.find_one({'detailed_url': {'$exists': True}})
    if sample:
        print(f"\n📄 Sample document:")
        print(f"  - property_id: {sample.get('property_id')}")
        print(f"  - detailed_url: {sample.get('detailed_url')}")
    
    print(f"\n✅ Field rename complete!")
    print(f"⏰ Finished at: {datetime.now()}")

if __name__ == "__main__":
    rename_field()