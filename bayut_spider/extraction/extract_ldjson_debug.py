#!/usr/bin/env python3
"""
Extract and analyze the actual LD+JSON data
"""

import requests
import json
from bs4 import BeautifulSoup

def extract_and_analyze():
    url = "https://www.bayut.com/for-sale/property/dubai/dubai-silicon-oasis-dso/page-2/"
    
    print(f"🔍 Extracting LD+JSON from: {url}")
    
    try:
        res = requests.get(url)
        html = res.text
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all LD+JSON scripts
        json_scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
        print(f"📝 Found {len(json_scripts)} LD+JSON script tags")
        
        # Look for the large one (Script 3 from debug)
        for i, script in enumerate(json_scripts):
            content = script.string or ""
            print(f"\n📄 Script {i+1}: {len(content)} characters")
            
            if len(content) > 10000:  # This should be our target script
                print("🎯 This looks like the main data script!")
                
                try:
                    data = json.loads(content)
                    print(f"✅ Successfully parsed JSON")
                    
                    # Analyze the structure
                    if isinstance(data, dict):
                        print(f"📊 JSON keys: {list(data.keys())}")
                        
                        # Look for itemListElement
                        if "itemListElement" in data:
                            items = data["itemListElement"]
                            print(f"🏠 Found itemListElement with {len(items)} items")
                            
                            if items and len(items) > 0:
                                # Examine first item
                                first_item = items[0]
                                print(f"🔍 First item keys: {list(first_item.keys()) if isinstance(first_item, dict) else 'Not a dict'}")
                                
                                if isinstance(first_item, dict) and "mainEntity" in first_item:
                                    main_entity = first_item["mainEntity"]
                                    print(f"✅ Found mainEntity with keys: {list(main_entity.keys()) if isinstance(main_entity, dict) else 'Not a dict'}")
                                    
                                    # This is exactly what we need!
                                    print(f"🎉 SUCCESS: Found valid property data structure!")
                                    
                                    # Save a sample for analysis
                                    with open("sample_ldjson.json", "w") as f:
                                        json.dump(data, f, indent=2, ensure_ascii=False)
                                    print(f"💾 Saved sample data to sample_ldjson.json")
                                    
                                    return True
                        
                        # Check other possible structures
                        if "@type" in data:
                            print(f"📊 @type: {data['@type']}")
                        if "numberOfItems" in data:
                            print(f"📊 numberOfItems: {data['numberOfItems']}")
                    
                except json.JSONDecodeError as e:
                    print(f"❌ JSON parse error: {e}")
                    
        return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = extract_and_analyze()
    if success:
        print("\n🎉 The LD+JSON extraction works! The blocking detection was a false positive.")
    else:
        print("\n❌ Could not extract valid LD+JSON data.")