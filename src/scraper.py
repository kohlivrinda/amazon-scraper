import asyncio
import json
import random
import re
import time
from typing import Optional

import lxml
import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import TimeoutError, async_playwright

from dynamic import DynamicScraper
from static import StaticScraper


async def run_scraper(url):
        static_scraper = StaticScraper(url, use_proxy= False)
        #INITIALIZE WIRH PROXY PARAMS AND CREDENTIALS IF USING PROXY
        await static_scraper.initialize()
        
        static_scraped_data = await static_scraper.run_static_scraper()
        print(json.dumps(static_scraped_data, indent = 4))
        
        dynamic_scraper = DynamicScraper(url, product_data= static_scraped_data, use_proxy= False)
        #INITIALIZE WIRH PROXY PARAMS AND CREDENTIALS IF USING PROXY
        
        scraped_data = await dynamic_scraper.run_dynamic_scraper()
        print(json.dumps(scraped_data, indent = 4))
        
        with open('../outputs/outputs.json', 'a') as f:
            f.write(json.dumps(scraped_data, ensure_ascii=False) + '\n')

def correct_json_file(input_file, output_file):
        with open(input_file, 'r') as file:
            content = file.read().strip()
    
    # Use a regular expression to insert a comma before '{' when it is directly followed by '"product_name"', accounting for any type and amount of whitespace between '}' and '{'
        corrected_content = re.sub(r'}\s*(?=\s*{\s*"\s*product_name")', '},', content)
        
        # Wrap the whole content in square brackets to form a valid JSON array
        corrected_content = f'[{corrected_content}]'
        
        with open(output_file, 'w') as file:
            file.write(corrected_content)
    

async def main():
    data = pd.read_csv("../data/dedup_urls.csv")
    
    if 'scraping' not in data.columns:
        data.insert(loc = 1, column = "scraping", value = 0)
        
    # Filter the DataFrame based on the condition  that scraping has not been attempted
    filtered_data = data[data['scraping'] == 0]

    # Sample unscraped 100 rows from the filtered DataFrame
    selected_rows = filtered_data.sample(5)
    # selected_rows = data.sample(100)
    print(selected_rows)
    for idx, row in selected_rows.iterrows():
        url = row['product URL'] 
        try:
            await run_scraper(url)
            data.at[idx, 'scraping'] = 1
            # flag to indicate successful scaraping
        except Exception as e:
            print(f"Exception: {e}")
            with open('../outputs/failed_urls.txt', 'a') as f:
                f.write(f"{url}\n")
                # flag to indicate unsuccessful scraping
                data.at[idx, 'scraping'] = -1
                continue
    correct_json_file(input_file='../outputs/outputs.json', output_file= '../outputs/outputs.json' )
    
    data.to_csv('../outputs/marked_dedup_urls.csv')
    
        
if __name__ == "__main__":
    asyncio.run(main())
            
        
            
            
        

            
    
    