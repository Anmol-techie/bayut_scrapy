#!/usr/bin/env python3
"""
MongoDB DateTime Conversion and Schema Validation
Permanent solution for Bayut scraper data structure
"""

from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime
import json

def parse_datetime(date_string):
    """Convert ISO datetime string to MongoDB DateTime object"""
    if not date_string:
        return None
    
    # If already a datetime object, return as-is
    if isinstance(date_string, datetime):
        return date_string
    
    if not isinstance(date_string, str):
        return None
    
    try:
        # Remove 'Z' and parse
        if date_string.endswith('Z'):
            date_string = date_string[:-1] + '+00:00'
        return datetime.fromisoformat(date_string)
    except:
        try:
            # Fallback parsing
            return datetime.strptime(date_string.replace('Z', ''), '%Y-%m-%dT%H:%M:%S.%f')
        except:
            try:
                # Another fallback without microseconds
                return datetime.strptime(date_string.replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
            except:
                return None

def convert_existing_datetime_fields():
    """Convert all existing datetime string fields to proper DateTime objects"""
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bayut_production']
    collection = db['sublocation_properties']
    
    print("🔄 Converting existing datetime fields to DateTime objects...")
    
    # Fields that should be datetime
    datetime_fields = ['created_at', 'last_seen', 'first_seen']
    
    # Count documents that need conversion
    sample_check = collection.find_one({'created_at': {'$type': 'string'}})
    if not sample_check:
        print("✅ All datetime fields are already converted!")
        return 0
    
    total_docs = collection.count_documents({})
    print(f"📊 Total documents in collection: {total_docs}")
    
    # Process in batches for performance
    batch_size = 1000
    updated_count = 0
    processed_count = 0
    
    cursor = collection.find({}, no_cursor_timeout=True).batch_size(batch_size)
    
    try:
        batch_updates = []
        
        for doc in cursor:
            processed_count += 1
            update_needed = False
            update_doc = {}
            
            # Check and convert main datetime fields
            for field in datetime_fields:
                if field in doc and isinstance(doc[field], str):
                    parsed_date = parse_datetime(doc[field])
                    if parsed_date:
                        update_doc[field] = parsed_date
                        update_needed = True
            
            # Check and convert appearances array datetime fields
            if 'appearances' in doc and isinstance(doc['appearances'], list):
                new_appearances = []
                appearances_changed = False
                
                for appearance in doc['appearances']:
                    new_appearance = appearance.copy()
                    if isinstance(appearance, dict) and 'scraped_at' in appearance:
                        if isinstance(appearance['scraped_at'], str):
                            parsed_date = parse_datetime(appearance['scraped_at'])
                            if parsed_date:
                                new_appearance['scraped_at'] = parsed_date
                                appearances_changed = True
                    new_appearances.append(new_appearance)
                
                if appearances_changed:
                    update_doc['appearances'] = new_appearances
                    update_needed = True
            
            if update_needed:
                batch_updates.append(
                    UpdateOne(
                        {'_id': doc['_id']},
                        {'$set': update_doc}
                    )
                )
            
            # Execute batch update
            if len(batch_updates) >= batch_size:
                result = collection.bulk_write(batch_updates, ordered=False)
                updated_count += result.modified_count
                print(f"  ✅ Processed {processed_count}/{total_docs} documents, updated {updated_count}...")
                batch_updates = []
        
        # Process remaining updates
        if batch_updates:
            result = collection.bulk_write(batch_updates, ordered=False)
            updated_count += result.modified_count
            
    finally:
        cursor.close()
    
    print(f"✅ Successfully converted datetime fields in {updated_count} documents")
    
    return updated_count

def create_collection_indexes():
    """Create indexes for better query performance"""
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bayut_production']
    collection = db['sublocation_properties']
    
    print("\n📑 Creating indexes...")
    
    indexes = [
        ([("property_id", ASCENDING)], {"unique": True, "background": True}),
        ([("created_at", ASCENDING)], {"background": True}),
        ([("last_seen", ASCENDING)], {"background": True}),
        ([("purpose", ASCENDING)], {"background": True}),
        ([("current_price", ASCENDING)], {"background": True}),
        ([("locations_seen", ASCENDING)], {"background": True}),
        ([("detail_scraped", ASCENDING)], {"background": True}),
    ]
    
    for index_spec, index_options in indexes:
        try:
            collection.create_index(index_spec, **index_options)
            field_name = index_spec[0][0]
            print(f"  ✅ Index created on: {field_name}")
        except Exception as e:
            if "already exists" in str(e):
                field_name = index_spec[0][0]
                print(f"  ℹ️  Index already exists on: {field_name}")
            else:
                print(f"  ⚠️  Error creating index: {e}")

def apply_schema_validation():
    """Apply schema validation to ensure data quality"""
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bayut_production']
    
    print("\n📋 Applying schema validation rules...")
    
    validation_rules = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["property_id", "purpose"],
            "properties": {
                "property_id": {
                    "bsonType": ["string", "int"],
                    "description": "Unique property identifier"
                },
                "purpose": {
                    "bsonType": "string",
                    "enum": ["for-sale", "for-rent"],
                    "description": "Property listing purpose"
                },
                "details_url": {
                    "bsonType": ["string", "null"],
                    "pattern": "^https?://",
                    "description": "URL to property details page"
                },
                "current_price": {
                    "bsonType": ["number", "int", "null"],
                    "minimum": 0,
                    "maximum": 1000000000,
                    "description": "Current property price in AED"
                },
                "last_seen": {
                    "bsonType": ["date", "null"],
                    "description": "Last time property was seen (DateTime)"
                },
                "first_seen": {
                    "bsonType": ["date", "null"],
                    "description": "First time property was seen (DateTime)"
                },
                "created_at": {
                    "bsonType": ["date", "null"],
                    "description": "Document creation timestamp (DateTime)"
                },
                "last_page": {
                    "bsonType": ["int", "null"],
                    "minimum": 1,
                    "maximum": 10000,
                    "description": "Last page number where property appeared"
                },
                "first_page": {
                    "bsonType": ["int", "null"],
                    "minimum": 1,
                    "maximum": 10000,
                    "description": "First page number where property appeared"
                },
                "last_position": {
                    "bsonType": ["int", "null"],
                    "minimum": 1,
                    "maximum": 100,
                    "description": "Last position on page"
                },
                "last_location": {
                    "bsonType": ["string", "null"],
                    "description": "Last location where property was listed"
                },
                "first_location": {
                    "bsonType": ["string", "null"],
                    "description": "First location where property was listed"
                },
                "locations_seen": {
                    "bsonType": "array",
                    "uniqueItems": True,
                    "items": {
                        "bsonType": "string"
                    },
                    "description": "All unique locations where property has been seen"
                },
                "appearances": {
                    "bsonType": "array",
                    "maxItems": 100,
                    "items": {
                        "bsonType": "object",
                        "required": ["page_number", "location", "scraped_at"],
                        "properties": {
                            "page_number": {
                                "bsonType": "int",
                                "minimum": 1
                            },
                            "position": {
                                "bsonType": ["int", "null"],
                                "minimum": 1,
                                "maximum": 100
                            },
                            "location": {
                                "bsonType": "string"
                            },
                            "price": {
                                "bsonType": ["number", "int", "null"],
                                "minimum": 0
                            },
                            "scraped_at": {
                                "bsonType": "date"
                            }
                        }
                    },
                    "description": "History of property appearances (max 100 entries)"
                },
                "detail_scraped": {
                    "bsonType": "bool",
                    "description": "Whether detail page has been scraped"
                },
                "last_raw_item": {
                    "bsonType": "object",
                    "description": "Raw data from last scrape"
                }
            }
        }
    }
    
    try:
        # First, remove any existing validation
        db.command({
            "collMod": "sublocation_properties",
            "validator": {},
            "validationLevel": "off"
        })
        
        # Apply new validation with moderate level (only validates new/updated docs)
        db.command({
            "collMod": "sublocation_properties",
            "validator": validation_rules,
            "validationLevel": "moderate",
            "validationAction": "warn"
        })
        
        print("✅ Schema validation applied successfully")
        print("   - Level: moderate (validates only new/updated documents)")
        print("   - Action: warn (logs warnings but doesn't reject invalid docs)")
        
    except Exception as e:
        print(f"⚠️  Error applying schema validation: {e}")

def verify_datetime_conversion():
    """Verify that datetime conversion worked correctly"""
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bayut_production']
    collection = db['sublocation_properties']
    
    print("\n🔍 Verifying datetime conversion...")
    
    # Check for any remaining string datetime fields
    string_dates = collection.count_documents({
        '$or': [
            {'created_at': {'$type': 'string'}},
            {'last_seen': {'$type': 'string'}},
            {'first_seen': {'$type': 'string'}}
        ]
    })
    
    if string_dates > 0:
        print(f"  ⚠️  Found {string_dates} documents with string datetime fields")
    else:
        print("  ✅ All datetime fields are proper DateTime objects")
    
    # Test date range query
    print("\n📊 Testing date range queries...")
    from datetime import timedelta
    now = datetime.utcnow()
    
    # Last 7 days
    week_ago = now - timedelta(days=7)
    week_count = collection.count_documents({
        'created_at': {'$gte': week_ago, '$lte': now}
    })
    print(f"  - Documents created in last 7 days: {week_count}")
    
    # Last 30 days
    month_ago = now - timedelta(days=30)
    month_count = collection.count_documents({
        'created_at': {'$gte': month_ago, '$lte': now}
    })
    print(f"  - Documents created in last 30 days: {month_count}")
    
    # Sample document check
    sample = collection.find_one()
    if sample:
        print("\n📄 Sample document datetime fields:")
        for field in ['created_at', 'last_seen', 'first_seen']:
            if field in sample:
                value = sample[field]
                value_type = type(value).__name__
                print(f"  - {field}: {value_type}")
        
        if 'appearances' in sample and sample['appearances']:
            first_app = sample['appearances'][0]
            if 'scraped_at' in first_app:
                value_type = type(first_app['scraped_at']).__name__
                print(f"  - appearances[0].scraped_at: {value_type}")

def main():
    print("🚀 MongoDB Data Structure Fix for Bayut Scraper")
    print("=" * 60)
    
    # Step 1: Convert existing datetime fields
    updated = convert_existing_datetime_fields()
    
    # Step 2: Create indexes for better performance
    create_collection_indexes()
    
    # Step 3: Apply schema validation
    apply_schema_validation()
    
    # Step 4: Verify conversion
    verify_datetime_conversion()
    
    print("\n" + "=" * 60)
    print("✅ Database optimization complete!")
    print("\nBenefits:")
    print("  ✓ DateTime fields are now proper MongoDB DateTime objects")
    print("  ✓ Date range queries will work in Metabase and other tools")
    print("  ✓ Schema validation ensures data quality for future inserts")
    print("  ✓ Indexes improve query performance")
    print("\n📝 Note: The insertion script needs to be updated to use DateTime objects")

if __name__ == "__main__":
    main()