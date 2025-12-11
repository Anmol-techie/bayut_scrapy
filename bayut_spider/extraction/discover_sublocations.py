#!/usr/bin/env python3
"""
Bayut Sub-location Discovery Script

This script scrapes all sub-locations for each UAE city from Bayut's location pages.
It extracts the location links with property counts to build a comprehensive list
of all available sub-locations that can be scraped.

Output: bayut_sublocations_all_cities.csv
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv

# List of major cities in UAE (as used in Bayut URLs)
cities = [
    "dubai",
    "abu-dhabi",
    "sharjah",
    "ajman",
    "ras-al-khaimah",
    "fujairah",
    "umm-al-quwain"
]

base_url = "https://www.bayut.com"
results = []

for city in cities:
    print(f"Scraping: {city.title()}...")
    city_url = f"{base_url}/for-sale/property/{city}/"
    try:
        response = requests.get(city_url)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to load {city_url}: {e}")
        continue

    soup = BeautifulSoup(response.text, "html.parser")

    location_container = soup.find("div", attrs={"aria-label": "Location links"})
    if not location_container:
        print(f"❌ No 'Location links' found for {city}")
        continue

    location_links = location_container.find_all("a")

    for link in location_links:
        name = link.text.strip()
        href = link.get("href")
        full_url = urljoin(base_url, href)
        count_span = link.find_next_sibling("span")
        count = count_span.text.strip("()") if count_span else None

        results.append({
            "city": city.replace("-", " ").title(),
            "sublocation": name,
            "url": full_url,
            "listings": count
        })

print(f"\n✅ Done! {len(results)} locations scraped.\n")

# Save to CSV
filename = "bayut_sublocations_all_cities.csv"
with open(filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["city", "sublocation", "url", "listings"])
    writer.writeheader()
    writer.writerows(results)

print(f"📁 Saved to: {filename}")