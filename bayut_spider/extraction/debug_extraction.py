#!/usr/bin/env python3
"""
Debug the LD+JSON extraction to see what scripts are being found
"""

import requests
import json
from bs4 import BeautifulSoup

def debug_extraction(url):
    print(f"🔍 Debugging extraction for: {url}")
    
    try:
        res = requests.get(url)
        html = res.text
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all LD+JSON scripts
        json_scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
        print(f"📝 Found {len(json_scripts)} LD+JSON script tags")
        
        for i, script in enumerate(json_scripts):
            content = script.string or ""
            print(f"\n📄 Script {i+1}: {len(content)} characters")
            
            if content:
                try:
                    data = json.loads(content)
                    print(f"✅ Valid JSON")
                    
                    # Show structure
                    if isinstance(data, dict):
                        print(f"📊 Keys: {list(data.keys())}")
                        
                        # Check @type
                        if "@type" in data:
                            print(f"🏷️  @type: {data['@type']}")
                        
                        # Check for itemListElement (the property data we want)
                        if "itemListElement" in data:
                            items = data.get("itemListElement", [])
                            print(f"🎯 itemListElement with {len(items)} items")
                            
                            if items and len(items) > 0:
                                first_item = items[0]
                                if isinstance(first_item, dict):
                                    print(f"   First item keys: {list(first_item.keys())}")
                                    
                                    # Check for mainEntity (the property data)
                                    if "mainEntity" in first_item:
                                        main_entity = first_item["mainEntity"]
                                        if isinstance(main_entity, dict):
                                            print(f"   ✅ mainEntity keys: {list(main_entity.keys())}")
                                            print(f"   🎉 THIS IS THE PROPERTY DATA SCRIPT!")
                        
                        # Show a preview of the JSON
                        json_preview = json.dumps(data, indent=2)[:500]
                        print(f"📋 Preview:\n{json_preview}...")
                        
                except json.JSONDecodeError as e:
                    print(f"❌ Invalid JSON: {e}")
                    print(f"   Content preview: {content[:200]}...")
        
        # Now test the extraction function
        print(f"\n🔬 Testing extract_single_ldjson function:")
        
        # Import the extraction function
        import sys
        sys.path.append('.')
        from bayut_ldjson_to_mongo import extract_single_ldjson
        
        result = extract_single_ldjson(html)
        if result:
            print(f"✅ extract_single_ldjson returned data")
            if isinstance(result, dict):
                print(f"📊 Keys: {list(result.keys())}")
                if "itemListElement" in result:
                    items = result.get("itemListElement", [])
                    print(f"🎯 Found {len(items)} property items")
        else:
            print(f"❌ extract_single_ldjson returned None")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    # Test a URL that should work
    debug_extraction("https://www.bayut.com/for-sale/property/dubai/jumeirah-village-circle-jvc/page-2/?sort=date_desc")