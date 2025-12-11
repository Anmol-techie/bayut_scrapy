#!/usr/bin/env python3
"""
Bayut Property Complete Data Extractor
Extracts ALL possible data from Bayut property HTML files
"""

import re
import json
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional
import os
from datetime import datetime

class BayutPropertyExtractor:
    def __init__(self):
        """Initialize the Bayut property data extractor"""
        self.data = {}
        
    def extract_property_id(self, html_content: str, filename: str = None) -> Optional[str]:
        """Extract property ID from HTML or filename"""
        # Try to extract from filename first
        if filename:
            match = re.search(r'(\d{7,})', filename)
            if match:
                return match.group(1)
        
        # Try to extract from URL in HTML
        match = re.search(r'property/details-(\d+)\.html', html_content)
        if match:
            return match.group(1)
            
        # Try to extract from any reference to the property ID
        match = re.search(r'"propertyId":\s*"?(\d{7,})"?', html_content)
        if match:
            return match.group(1)
            
        return None
    
    def extract_json_ld_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract structured data from JSON-LD scripts"""
        json_data = {}
        
        # Find all JSON-LD script tags
        json_scripts = soup.find_all('script', type='application/ld+json')
        
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                
                # Extract based on @type
                if data.get('@type') == 'Apartment' or data.get('@type') == 'House':
                    json_data['property_type_schema'] = data.get('@type')
                    json_data['schema_name'] = data.get('name', '')
                    json_data['schema_url'] = data.get('url', '')
                    
                    # Extract geo coordinates
                    if 'geo' in data:
                        json_data['latitude'] = data['geo'].get('latitude')
                        json_data['longitude'] = data['geo'].get('longitude')
                    
                    # Extract floor size
                    if 'floorSize' in data:
                        json_data['floor_size'] = data['floorSize'].get('value')
                        json_data['floor_size_unit'] = data['floorSize'].get('unitText')
                    
                    # Extract rooms and bathrooms
                    if 'numberOfRooms' in data:
                        json_data['bedrooms'] = data['numberOfRooms'].get('value')
                    json_data['bathrooms'] = data.get('numberOfBathroomsTotal')
                    
                    # Extract address
                    if 'address' in data:
                        json_data['country'] = data['address'].get('addressCountry')
                        json_data['region'] = data['address'].get('addressRegion')
                        json_data['locality'] = data['address'].get('addressLocality')
                    
                    # Extract location info
                    if 'containedInPlace' in data:
                        json_data['area_name'] = data['containedInPlace'].get('name')
                        json_data['area_url'] = data['containedInPlace'].get('url')
                
                elif data.get('@type') == 'ItemPage':
                    if 'mainEntity' in data:
                        entity = data['mainEntity']
                        json_data['title'] = entity.get('name', '')
                        json_data['alternate_title'] = entity.get('alternateName', '')
                        json_data['description'] = entity.get('description', '')
                        json_data['main_image'] = entity.get('image', '')
                        
                        # Extract offer details
                        if 'offers' in entity and entity['offers']:
                            offer = entity['offers'][0]
                            json_data['currency'] = offer.get('priceCurrency')
                            
                            # Extract price
                            if 'priceSpecification' in offer:
                                json_data['price'] = offer['priceSpecification'].get('price')
                                json_data['price_currency'] = offer['priceSpecification'].get('priceCurrency')
                            
                            # Extract agent info
                            if 'offeredBy' in offer:
                                agent = offer['offeredBy']
                                json_data['agent_name'] = agent.get('name')
                                json_data['agent_image'] = agent.get('image')
                                
                                if 'parentOrganization' in agent:
                                    json_data['agency_name'] = agent['parentOrganization'].get('name')
                                    json_data['agency_url'] = agent['parentOrganization'].get('url')
                
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error parsing JSON-LD: {e}")
                continue
        
        return json_data
    
    def extract_property_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract property information from the property details section"""
        details = {}
        
        # Find property details list
        property_list = soup.find('ul', {'aria-label': 'Property details'})
        if property_list:
            items = property_list.find_all('li')
            for item in items:
                spans = item.find_all('span')
                if len(spans) >= 2:
                    key = spans[0].get_text(strip=True).replace(':', '')
                    value = spans[-1].get_text(strip=True)
                    
                    # Map to standardized keys
                    key_map = {
                        'Type': 'property_type',
                        'Purpose': 'purpose',
                        'Reference no.': 'reference_number',
                        'Completion': 'completion_status',
                        'Added on': 'added_date',
                        'Handover date': 'handover_date',
                        'Furnished': 'furnishing',
                        'Ownership': 'ownership',
                        'Build Year': 'build_year',
                        'Floors': 'floors',
                        'Parking Spaces': 'parking_spaces'
                    }
                    
                    standardized_key = key_map.get(key, key.lower().replace(' ', '_'))
                    details[standardized_key] = value
                    
                # Special handling for TruCheck
                if 'TruCheck' in item.get_text():
                    trucheck_span = item.find('span', {'aria-label': 'Trucheck date'})
                    if trucheck_span:
                        details['trucheck_date'] = trucheck_span.get_text(strip=True)
        
        return details
    
    def extract_amenities(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract all amenities and features"""
        amenities = {
            'all_amenities': [],
            'categorized_amenities': {}
        }
        
        # Method 1: Extract from visible amenities section
        amenities_section = soup.find('div', class_='db2d3ff3')
        if amenities_section:
            # Get visible amenities
            visible_items = amenities_section.find_all('span', class_='c0327f5b')
            for item in visible_items:
                amenity = item.get_text(strip=True)
                if amenity and amenity not in amenities['all_amenities']:
                    amenities['all_amenities'].append(amenity)
        
        # Method 2: Extract from amenity dialog (hidden but contains all)
        amenity_dialog = soup.find('div', id='property-amenity-dialog')
        if amenity_dialog:
            categories = amenity_dialog.find_all('div', class_='_791bcb34')
            for category in categories:
                category_name_elem = category.find('div', class_='_668d7c5b')
                if category_name_elem:
                    category_name = category_name_elem.get_text(strip=True)
                    amenities['categorized_amenities'][category_name] = []
                    
                    # Get all amenities in this category
                    items = category.find_all('span', class_='c0327f5b')
                    for item in items:
                        amenity = item.get_text(strip=True)
                        if amenity:
                            amenities['categorized_amenities'][category_name].append(amenity)
                            if amenity not in amenities['all_amenities']:
                                amenities['all_amenities'].append(amenity)
        
        # Method 3: Search for common amenities in text
        amenity_keywords = [
            'Swimming Pool', 'Gym', 'Parking', 'Security', 'Balcony', 'Garden',
            'Elevator', 'Central AC', 'Maid Room', 'Storage', 'Laundry',
            'Kids Play Area', 'BBQ Area', 'Pets Allowed', 'Study Room',
            'Private Pool', 'Private Garden', 'Sea View', 'Built in Wardrobes'
        ]
        
        full_text = soup.get_text()
        for keyword in amenity_keywords:
            if keyword.lower() in full_text.lower() and keyword not in amenities['all_amenities']:
                amenities['all_amenities'].append(keyword)
        
        return amenities
    
    def extract_contact_info(self, html_content: str, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract agent and contact information"""
        contact = {}
        
        # Extract phone numbers (UAE format)
        phone_patterns = [
            r'\+971[\s-]?\d{1,2}[\s-]?\d{3}[\s-]?\d{4}',  # +971 50 123 4567
            r'971\d{8,9}',  # 971501234567
            r'0[2-7,9]\d{7,8}',  # 0501234567
            r'\d{2}[\s-]?\d{3}[\s-]?\d{4}'  # 50 123 4567
        ]
        
        phones = set()
        for pattern in phone_patterns:
            matches = re.findall(pattern, html_content)
            for match in matches:
                # Clean and validate the number
                clean_number = re.sub(r'[\s-]', '', match)
                if len(clean_number) >= 9:  # Valid UAE number
                    phones.add(match)
        
        contact['phone_numbers'] = list(phones)
        
        # Extract WhatsApp number (usually same as mobile)
        whatsapp_pattern = r'whatsapp[^0-9]*([+0-9\s-]{10,})'
        whatsapp_match = re.search(whatsapp_pattern, html_content, re.IGNORECASE)
        if whatsapp_match:
            contact['whatsapp'] = whatsapp_match.group(1).strip()
        
        # Extract email if present
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails = re.findall(email_pattern, html_content)
        if emails:
            contact['emails'] = list(set(emails))
        
        # Extract agent details from structured data or HTML
        agent_elem = soup.find('div', class_='_5eac2e30')
        if agent_elem:
            agent_name_elem = agent_elem.find('span', class_='_3aa81812')
            if agent_name_elem:
                contact['agent_name_html'] = agent_name_elem.get_text(strip=True)
        
        # Extract broker information
        broker_pattern = r'BRN[:\s]*(\d+)'
        broker_match = re.search(broker_pattern, html_content)
        if broker_match:
            contact['broker_number'] = broker_match.group(1)
        
        # Extract RERA number
        rera_pattern = r'RERA[:\s]*(\d+)'
        rera_match = re.search(rera_pattern, html_content)
        if rera_match:
            contact['rera_number'] = rera_match.group(1)
        
        return contact
    
    def extract_description(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract property description and highlights"""
        description_data = {}
        
        # Find main description
        desc_elem = soup.find('div', {'aria-label': 'Property description'})
        if desc_elem:
            description_data['full_description'] = desc_elem.get_text(strip=True)
            
            # Extract key points from description
            desc_text = desc_elem.get_text()
            
            # Extract bullet points
            bullets = re.findall(r'[•▪◦]\s*([^•▪◦\n]+)', desc_text)
            if bullets:
                description_data['bullet_points'] = [b.strip() for b in bullets]
            
            # Extract sections with strong tags
            strong_elements = desc_elem.find_all('strong')
            description_data['highlighted_sections'] = [elem.get_text(strip=True) for elem in strong_elements]
        
        # Extract title/headline
        title_elem = soup.find('h1', class_='_4bbafa79')
        if title_elem:
            description_data['headline'] = title_elem.get_text(strip=True)
        
        return description_data
    
    def extract_location_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract detailed location information"""
        location = {}
        
        # Extract from breadcrumb or navigation
        breadcrumb = soup.find('nav', {'aria-label': 'Breadcrumb'})
        if breadcrumb:
            links = breadcrumb.find_all('a')
            location['breadcrumb'] = [link.get_text(strip=True) for link in links]
        
        # Extract nearby places
        nearby_section = soup.find('div', class_='_9e8a3c2f')
        if nearby_section:
            nearby_items = nearby_section.find_all('div', class_='_3d169545')
            location['nearby_places'] = []
            for item in nearby_items:
                place_name = item.find('span', class_='_9589c5c1')
                distance = item.find('span', class_='_2a90c1f7')
                if place_name and distance:
                    location['nearby_places'].append({
                        'name': place_name.get_text(strip=True),
                        'distance': distance.get_text(strip=True)
                    })
        
        return location
    
    def extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extract all image URLs"""
        images = []
        
        # Find all img tags
        img_tags = soup.find_all('img')
        for img in img_tags:
            src = img.get('src') or img.get('data-src')
            if src and 'bayut' in src:
                images.append(src)
        
        # Find images in style attributes
        style_pattern = r'url\(["\']?(https://[^"\']+)["\']?\)'
        styles = soup.find_all(style=True)
        for elem in styles:
            matches = re.findall(style_pattern, elem['style'])
            images.extend(matches)
        
        # Remove duplicates
        return list(set(images))
    
    def extract_pricing_details(self, soup: BeautifulSoup, html_content: str) -> Dict[str, Any]:
        """Extract detailed pricing information"""
        pricing = {}
        
        # Extract main price
        price_pattern = r'(?:AED|Dhs?\.?)\s*([0-9,]+(?:\.\d{2})?)'
        price_matches = re.findall(price_pattern, html_content)
        if price_matches:
            # Clean and convert prices
            prices = []
            for match in price_matches:
                clean_price = match.replace(',', '')
                try:
                    prices.append(float(clean_price))
                except ValueError:
                    continue
            
            if prices:
                pricing['price'] = max(prices)  # Usually the main price is the largest
                pricing['all_prices_found'] = list(set(prices))
        
        # Extract price per sqft
        price_sqft_pattern = r'([0-9,]+)\s*(?:AED|Dhs?\.?)?\s*/\s*(?:sq\.?\s*ft\.?|sqft)'
        sqft_match = re.search(price_sqft_pattern, html_content, re.IGNORECASE)
        if sqft_match:
            pricing['price_per_sqft'] = sqft_match.group(1).replace(',', '')
        
        # Extract payment plan details
        payment_pattern = r'(\d+)[/:](\d+)\s*payment\s*plan'
        payment_match = re.search(payment_pattern, html_content, re.IGNORECASE)
        if payment_match:
            pricing['payment_plan'] = f"{payment_match.group(1)}/{payment_match.group(2)}"
        
        # Extract down payment
        down_payment_pattern = r'down\s*payment[:\s]*(?:AED|Dhs?\.?)?\s*([0-9,]+)'
        down_match = re.search(down_payment_pattern, html_content, re.IGNORECASE)
        if down_match:
            pricing['down_payment'] = down_match.group(1).replace(',', '')
        
        return pricing
    
    def extract_developer_project_info(self, soup: BeautifulSoup, html_content: str) -> Dict[str, Any]:
        """Extract developer and project information"""
        project = {}
        
        # Extract developer name
        developer_patterns = [
            r'(?:developed?\s*by|developer)[:\s]*([A-Za-z0-9\s&]+?)(?:\.|,|\n|<)',
            r'([A-Za-z0-9\s&]+?)\s*(?:Developments?|Properties|Real Estate)',
        ]
        
        for pattern in developer_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                project['developer'] = match.group(1).strip()
                break
        
        # Extract project name
        project_patterns = [
            r'(?:project|residence|tower|building)[:\s]*([A-Za-z0-9\s]+?)(?:\.|,|\n|<)',
        ]
        
        for pattern in project_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                project['project_name'] = match.group(1).strip()
                break
        
        # Extract project timeline
        timeline_pattern = r'(?:completion|handover|delivery)[:\s]*([Q][1-4]\s*\d{4}|\d{4})'
        timeline_match = re.search(timeline_pattern, html_content, re.IGNORECASE)
        if timeline_match:
            project['completion_date'] = timeline_match.group(1)
        
        return project
    
    def extract_additional_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract any additional metadata"""
        metadata = {}
        
        # Extract meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name') or meta.get('property')
            content = meta.get('content')
            if name and content:
                metadata[f'meta_{name}'] = content
        
        # Extract data attributes
        elements_with_data = soup.find_all(attrs={'data-id': True})
        for elem in elements_with_data:
            data_id = elem.get('data-id')
            if data_id:
                metadata['data_id'] = data_id
                break
        
        return metadata
    
    def extract_all(self, html_content: str, filename: str = None) -> Dict[str, Any]:
        """Main method to extract all data from the HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialize result dictionary
        result = {
            'extraction_timestamp': datetime.now().isoformat(),
            'filename': filename
        }
        
        # Extract property ID
        result['property_id'] = self.extract_property_id(html_content, filename)
        
        # Extract from JSON-LD
        json_ld_data = self.extract_json_ld_data(soup)
        result.update(json_ld_data)
        
        # Extract property details
        property_details = self.extract_property_details(soup)
        result.update(property_details)
        
        # Extract amenities
        amenities = self.extract_amenities(soup)
        result['amenities'] = amenities['all_amenities']
        result['amenities_categorized'] = amenities['categorized_amenities']
        result['total_amenities_count'] = len(amenities['all_amenities'])
        
        # Extract contact information
        contact_info = self.extract_contact_info(html_content, soup)
        result['contact'] = contact_info
        
        # Extract description
        description = self.extract_description(soup)
        result.update(description)
        
        # Extract location data
        location = self.extract_location_data(soup)
        result['location_details'] = location
        
        # Extract images
        images = self.extract_images(soup)
        result['images'] = images
        result['image_count'] = len(images)
        
        # Extract pricing details
        pricing = self.extract_pricing_details(soup, html_content)
        result['pricing_details'] = pricing
        
        # Extract developer and project info
        project_info = self.extract_developer_project_info(soup, html_content)
        result['project_info'] = project_info
        
        # Extract additional metadata
        metadata = self.extract_additional_metadata(soup)
        result['metadata'] = metadata
        
        # Clean up None values
        result = self.clean_data(result)
        
        return result
    
    def clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize the extracted data"""
        if isinstance(data, dict):
            return {k: self.clean_data(v) for k, v in data.items() if v is not None and v != ''}
        elif isinstance(data, list):
            return [self.clean_data(item) for item in data if item is not None and item != '']
        else:
            return data
    
    def save_to_json(self, data: Dict[str, Any], output_file: str):
        """Save extracted data to JSON file"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {output_file}")
    
    def save_to_csv(self, data: Dict[str, Any], output_file: str):
        """Save extracted data to CSV file (flattened)"""
        import csv
        
        # Flatten nested dictionary
        flattened = self.flatten_dict(data)
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=flattened.keys())
            writer.writeheader()
            writer.writerow(flattened)
        print(f"Data saved to {output_file}")
    
    def flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, ', '.join(map(str, v))))
            else:
                items.append((new_key, v))
        return dict(items)


def main():
    """Main function to test the extractor"""
    # Example usage
    extractor = BayutPropertyExtractor()
    
    # Test with the sample file
    test_file = '/Users/apple/Desktop/dbs-sleek/property_4032341.html'
    
    if os.path.exists(test_file):
        print(f"Processing: {test_file}")
        
        # Read HTML content
        with open(test_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Extract all data
        extracted_data = extractor.extract_all(html_content, os.path.basename(test_file))
        
        # Save to JSON
        output_json = test_file.replace('.html', '_extracted.json')
        extractor.save_to_json(extracted_data, output_json)
        
        # Save to CSV
        output_csv = test_file.replace('.html', '_extracted.csv')
        extractor.save_to_csv(extracted_data, output_csv)
        
        # Print summary
        print("\n" + "="*50)
        print("EXTRACTION SUMMARY")
        print("="*50)
        print(f"Property ID: {extracted_data.get('property_id')}")
        print(f"Title: {extracted_data.get('headline', 'N/A')}")
        print(f"Price: AED {extracted_data.get('price', 'N/A')}")
        print(f"Location: {extracted_data.get('locality', 'N/A')}")
        print(f"Bedrooms: {extracted_data.get('bedrooms', 'N/A')}")
        print(f"Bathrooms: {extracted_data.get('bathrooms', 'N/A')}")
        print(f"Size: {extracted_data.get('floor_size', 'N/A')} {extracted_data.get('floor_size_unit', '')}")
        print(f"Total Amenities: {extracted_data.get('total_amenities_count', 0)}")
        print(f"Agent: {extracted_data.get('agent_name', 'N/A')}")
        print(f"Agency: {extracted_data.get('agency_name', 'N/A')}")
        
        # Print contact numbers if found
        if extracted_data.get('contact', {}).get('phone_numbers'):
            print(f"Contact Numbers: {', '.join(extracted_data['contact']['phone_numbers'])}")
        
        print("\n" + "="*50)
        print(f"Full data saved to: {output_json}")
        print(f"CSV data saved to: {output_csv}")
        
    else:
        print(f"File not found: {test_file}")


if __name__ == "__main__":
    main()