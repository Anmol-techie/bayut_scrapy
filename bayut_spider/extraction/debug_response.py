#!/usr/bin/env python3
"""
Debug the actual response content to understand the blocking
"""

import requests
from bs4 import BeautifulSoup

def debug_response():
    url = "https://www.bayut.com/for-sale/property/dubai/dubai-silicon-oasis-dso/page-2/"
    
    print(f"🔍 Debugging response from: {url}")
    
    try:
        res = requests.get(url)
        print(f"📊 Response: HTTP {res.status_code}, Size: {len(res.text)} bytes")
        
        html = res.text
        
        # Check what kind of blocking indicators we have
        blocking_indicators = {
            "captcha": "captcha" in html.lower(),
            "cloudflare": "cloudflare" in html.lower(),
            "challenge": "challenge" in html.lower(),
            "403": "403" in html,
            "blocked": "blocked" in html.lower(),
            "bot": "bot" in html.lower(),
            "robot": "robot" in html.lower(),
        }
        
        print("\n🚨 Blocking indicators found:")
        for indicator, found in blocking_indicators.items():
            if found:
                print(f"  ✅ {indicator}")
        
        # Look for the title to understand what page we got
        soup = BeautifulSoup(html, "html.parser")
        title = soup.find("title")
        if title:
            print(f"\n📄 Page title: {title.get_text().strip()}")
        
        # Look for specific error messages or indicators
        print(f"\n🔍 First 1000 characters of response:")
        print("-" * 50)
        print(html[:1000])
        print("-" * 50)
        
        # Look for script tags that might contain LD+JSON
        json_scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
        print(f"\n📝 Found {len(json_scripts)} LD+JSON script tags")
        
        if json_scripts:
            for i, script in enumerate(json_scripts[:3]):  # Show first 3
                content = script.string or ""
                print(f"  Script {i+1}: {len(content)} chars")
                if content:
                    print(f"    Preview: {content[:200]}...")
        
        # Check if we have the Bayut main content
        property_cards = soup.find_all("div", class_="ca2b9dd041")  # Common Bayut property card class
        print(f"\n🏠 Found {len(property_cards)} potential property cards")
        
        # Look for pagination or property listings indicators
        pagination = soup.find("nav", attrs={"aria-label": "pagination"})
        if pagination:
            print("✅ Found pagination navigation - this looks like a real listings page")
        
        # Check for meta tags that might indicate the page type
        meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description:
            print(f"📝 Meta description: {meta_description.get('content', '')[:200]}...")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    debug_response()