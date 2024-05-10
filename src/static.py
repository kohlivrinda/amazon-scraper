import asyncio
import json
import random
import re
import time
from collections import defaultdict
from typing import Optional

import lxml
import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import TimeoutError, async_playwright


class StaticScraper:
    def __init__(self, url:str, 
                    use_proxy: bool, 
                    proxy_ip: Optional[str] = None, 
                    username: Optional[str] = None, 
                    password: Optional[str] = None):
        self.url = url
        self.use_proxy = use_proxy
        
        if self.use_proxy:
            self.proxy_ip = proxy_ip
            self.password = password
            self.username = username
        
    async def initialize(self):
        self.soup = await self.get_soup() 
        
    
    async def fetch_content_with_retry(self):
        MAX_RETRIES = 5
        retry_count = 0
        
        while retry_count < MAX_RETRIES:
            try:
                async with async_playwright() as p:
                    if self.use_proxy:
                        proxy = {
                            'server': self.proxy_ip,
                            'username': self.username,
                            'password': self.password
                        }
                        browser = await p.firefox.launch(proxy=proxy, headless= True)
                    else:
                        browser = await p.firefox.launch(headless = True)
                    page = await browser.new_page()
                    await page.goto(self.url, timeout=100000)
                    content = await page.content()
                    await browser.close()
                    return content
            except Exception as e:
                print(f"An error occurred: {e}")
                retry_count += 1
                if retry_count == MAX_RETRIES:
                    print("Maximum retries reached.")
                    raise
                await asyncio.sleep(random.randint(1, 5))
                
    async def get_soup(self):
        try:
            response_content = await self.fetch_content_with_retry()
            if response_content:
                soup = BeautifulSoup(response_content, 'lxml')
                return soup
            
        except Exception as e:
            #TODO: add better logging
                print("An error occured:", e)
                return None
            
    def get_product_name(self):
        try:
            title = self.soup.select_one('#productTitle').get_text().strip() 
            return title
        except Exception as e:
            print(f"Failed to fetch title for {self.url}.\n Exception: {e}")
            #TODO: logging
            return None
    
    def get_about_product(self):
        #TODO: logging
        about_section = self.soup.select_one('#feature-bullets')
        if not about_section:
            about_section = self.soup.select_one('#visual-rich-product-description')
            if not about_section: 
                return None
            else: 
                extracted_texts = []
                elements = about_section.find_all(class_="a-size-small a-color-base visualRpdText")
                for element in elements:
                    extracted_texts.append(element.get_text())
                    return extracted_texts

        else:
            return [item.get_text().strip() for item in about_section.find_all('li', class_ = 'a-spacing-mini')] if about_section else []
                
        
    def get_price_details(self):
        #TODO: logging
        table = self.soup.find('table', class_='a-lineitem a-align-top')
        if not table:
            return None
        outer_span = table.find('span', class_='a-size-small a-color-price')
        first_span_text = outer_span.find(class_='a-offscreen').get_text(strip=True)
        outer_span_text = outer_span.contents[-1].strip()[:-1]
        unit_price = first_span_text + ' ' + outer_span_text

        price_element = table.find(class_='apexPriceToPay')
        price = price_element.find('span', class_='a-offscreen').get_text(strip=True)

        result = {'total price': price,
                'unit price': unit_price
                }
        return result
    
    def get_product_details(self) -> dict:
        
        pdt_details = self.soup.find('div', class_='a-section a-spacing-small a-spacing-top-small')

        if not pdt_details:
            return None

        
        table_values = {}
        for tr in pdt_details.find_all('tr'):
            key = tr.find('td', class_='a-span3').text.strip()
            value = tr.find('td', class_='a-span9').text.strip()
            table_values[key] = value
        #TODO: logging
        return table_values

    def get_amazon_details(self):
        details = {}
        details_section = self.soup.select_one('#detailBulletsWrapper_feature_div')
        if not details_section:
            return None
        
        for li in details_section.select_one('#detailBullets_feature_div').find_all('li'):
            key_element = li.find('span', class_='a-text-bold')
            key = re.sub(r'\s+', ' ', key_element.get_text(strip=True).replace('\u200f', '').replace('\u200e', '').strip())[:-2]
            value = key_element.find_next_sibling('span').get_text(strip=True).strip()
            details[key] = value

        rank_sec = details_section.find('span', string=" Best Sellers Rank: ")
        bestsellers_rank_text = rank_sec.parent.get_text(strip=True)
        bestsellers_rank_text = rank_sec.parent.get_text(strip=True)
        match = re.search(r'^(.*?)\(', bestsellers_rank_text)
        overall_rank = match.group(1)
        # overall_rank = bestsellers_rank_text.split('#')[1].split(' ')[0]
        subranks = [rank_sec.parent.find_all('span', class_ = 'a-list-item')[0].get_text()]


        num_rating_str = self.soup.select_one('#acrCustomerReviewText').get_text(strip=True)
        cleaned_rating_string = ''.join(filter(str.isdigit, num_rating_str))
        num_ratings = int(cleaned_rating_string)

        details['avg_rating']=  details_section.find('span', class_= 'a-size-base a-color-base').get_text(strip= True)
        details['num_ratings']= num_ratings
        details['overall rank'] = overall_rank
        details['subranks'] = subranks
        return details


    def get_ai_sentiments(self) -> dict:

        buttons = self.soup.find("div", class_="_cr-product-insights_style_aspect-button-group__nm_MR").find_all("button")

        kv_pairs = {}

        for button in buttons:
            name = button.find("span", class_="a-size-base").get_text(strip=True)
            aria_desc = button.get("aria-describedby")
            if aria_desc:
                value_parts = aria_desc.split("_")[-1].split() 
                value = value_parts[0] if value_parts else ""  
                kv_pairs[name] = value
            else:
                kv_pairs[name] = "No value"
        return kv_pairs


    def get_ai_summary(self) -> str:
        return self.soup.select_one('#product-summary').find('p').get_text()

    
    # def get_climate_pledge_badges(self):
    #     badges_area = self.soup.select_one('#climatePledgeFriendly')
    #     certifications = {}

    #     if badges_area:
    #         cert_area = badges_area.findAll('span', class_='a-size-base')
    #         certifications = {i.get_text(): None for i in cert_area}

    #         if 'Cradle to Cradle Certified' in certifications:
    #             level_element = badges_area.find('span', class_="a-size-small a-color-tertiary")
    #             if level_element:
    #                 level = level_element.get_text()
    #                 certifications['Cradle to Cradle Certified'] = level

    #     return certifications
    
    def get_climate_pledge_badges(self):
        badges_area = self.soup.select_one("#climatePledgeFriendly")
        cert_map = defaultdict(list)
        if badges_area:
            cert_area = badges_area.findAll("a", class_="a-size-base")
            text_area = badges_area.findAll("span", class_="a-size-base-plus a-text-bold")

            for i in range(len(text_area)):
                cert_map[f"{text_area[i].get_text()}"] = [f"{cert_area[i].get_text()}"]
                
            
            try:
                pop_element = self.soup.find("div", id = "a-popover-CPFBottomSheet-ATF")
                level_elements = pop_element.find_all("span", class_ = "a-size-small a-color-base")
                cert_levels = [el.get_text() for el in level_elements]
            except:
                pass
            
            c2c_count = 0
            for key, values in cert_map.items():
                for value in values:
                    if value == " Cradle to Cradle Certified ":
                        values.append(cert_levels[c2c_count])
                        c2c_count +=1
        return cert_map

    def check_amazon_choice(self):
        return True if self.soup.find('span', class_ = "ac-badge-rectangle") else False
    
    async def run_static_scraper(self):
        product_data = {}
        product_data['product_name'] = self.get_product_name()
        product_data['about_items'] = self.get_about_product()
        product_data['price'] = self.get_price_details()
        product_data['amazon choice'] = self.check_amazon_choice()
        product_data.update(self.get_product_details())
        product_data.update(self.get_amazon_details())
        
        if self.soup.select_one('#climatePledgeFriendly'):
            badges = self.get_climate_pledge_badges()
            product_data['Climate Pledge Badges'] = badges
            product_data['Number of Badges'] = len(badges)

        
        if self.soup.find('div', {"id": "cr-product-insights-cards"}):
            product_data['AI Sentiments'] = self.get_ai_sentiments()
            product_data['AI Summary'] = self.get_ai_summary() 
            product_data['Needs Reviews'] = False
        
        else:
            product_data['Needs Reviews'] = True
            #TODO: add flag logic to carry over into dynamic scraper
        
        #TODO: logging
        
        product_data['fully_scraped'] = False
        return product_data
    
    