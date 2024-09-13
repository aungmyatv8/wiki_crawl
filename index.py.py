import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time
import glob
from pathlib import Path
import os
import re  # For sanitizing filenames

folder_name = 0
visited_urls = set()  # To avoid visiting the same URL multiple times
base_url = "https://my.wikipedia.org"

def sanitize_filename(url):
    """Convert a URL into a valid filename."""
    sanitized = re.sub(r'[\\/*?:"<>|]', "_", url)  # Replace invalid characters
    return sanitized

def save_to_file(filename, data):
    global folder_name
    folder_path = os.path.join("./files/", str(folder_name))

    # Count existing files in the folder to check if a new folder is needed
    files = glob.glob(f'./files/{folder_name}/*.txt')

    if len(files) > 500:
        # Increment the folder_name and create a new folder
        folder_name += 1
        folder_path = os.path.join("./files/", str(folder_name))
        Path(folder_path).mkdir(parents=True, exist_ok=True)

    file_path = os.path.join(folder_path, filename)
    
    # Convert data to string if it's a list
    if isinstance(data, list):
        data = '\n'.join(map(str, data))  # Join list elements into a single string
    
    with open(file_path, 'w', encoding='utf-8') as file:  # Use 'w' to create a new file
        file.write(data)

def get_links_from_page(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract all links that start with '/wiki/' and avoid administrative links (e.g., /wiki/File:, /wiki/Special:)
        links = [a['href'] for a in soup.find_all('a', href=True) 
                 if a['href'].startswith('/wiki/') and 
                 not any(prefix in a['href'] for prefix in [":", "/wiki/Help", "/wiki/Wikipedia", "/wiki/Talk"])]
        full_links = [f"{base_url}{link}" for link in links]
        
        return full_links
    
    except requests.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return []

def crawl_page(url):
    if url in visited_urls:
        return  # Avoid re-crawling the same URL
    
    print(f"Crawling: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract data as needed (e.g., paragraphs)
        paragraphs = soup.find_all('p')
        text = [para.text for para in paragraphs]
        
        # Generate a unique filename based on the URL
        filename = sanitize_filename(url.replace(base_url + "/wiki/", "")) + ".txt"
        
        # Save the page text to a unique file
        save_to_file(filename, text)
        
        # Mark the URL as visited
        visited_urls.add(url)
    
    except requests.RequestException as e:
        error_message = f"Request failed for {url}: {e}"
        filename = sanitize_filename(url.replace(base_url + "/wiki/", "")) + "_error.txt"
        save_to_file(filename, error_message)

def main(start_url, max_pages=100000):
    links_to_crawl = [start_url]
    crawled_urls = set()

    with ThreadPoolExecutor(max_workers=5) as executor:  # Using 5 threads to crawl concurrently
        while links_to_crawl and len(crawled_urls) < max_pages:
            url = links_to_crawl.pop(0)
            
            if url not in crawled_urls:
                # Crawl the page in a separate thread
                executor.submit(crawl_page, url)
                crawled_urls.add(url)
                
                # Get new links and add them to the list
                new_links = get_links_from_page(url)
                for link in new_links:
                    if link not in crawled_urls and link not in links_to_crawl:
                        links_to_crawl.append(link)
                        
                # Delay to avoid overwhelming the server
                time.sleep(0.5)  # Adjust the delay as needed
            
            print(f"Completed {len(crawled_urls)} pages out of {max_pages}")

# Example URL to start crawling
start_url = "https://my.wikipedia.org/wiki/ရန်ကုန်"
main(start_url)
