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


class DynamicScraper():
    def __init__(self, url: str, 
                product_data:dict,
                use_proxy: bool, 
                proxy_ip: Optional[str] = None, 
                username: Optional[str] = None, 
                password: Optional[str] = None,
                country_code: Optional[str] = None):
        
        self.url = url
        self.product_data = product_data
        self.needs_review = self.product_data['Needs Reviews']
        self.asin = self.product_data['ASIN']
        
        self.use_proxy = use_proxy
        
        
        if self.use_proxy:
            self.proxy_ip = proxy_ip
            self.password = password
            self.username = username
            self.country_code = country_code
            
        # self.browser = None
        
    # Define request retry functionality with a maximum retry limit.
    async def perform_request_with_retry(self, page, url):
    # set max retries
        MAX_RETRIES = 5
        # initialize retry counter
        retry_count = 0
        # loop until maximum retries are reached
        while retry_count < MAX_RETRIES:
            try:
            # try to make a request to the URL using the page object and a timeout of 30 seconds.
                await page.goto(url, timeout = 80000)
            # break the while loop if the request was successful.
                break
            except:
            # if an exception occurs, increment the retry counter
                retry_count += 1
            # if maximum retries have been reached, raise an exception
                if retry_count == MAX_RETRIES:
                    raise Exception("Maximum retries reached")
            # wait for a random amount of time between 1 and 5 seconds before retrying
            await asyncio.sleep(random.randint(1, 5))
        
    def get_qa_url(self):
        return(f"https://www.amazon.com/ask/questions/asin/{self.asin}/")
    
    async def scrape_question_page(self, url):
        async with async_playwright() as pw:
            if self.use_proxy:
                proxy = {
                    'server': self.proxy_ip,
                    # 'username': f'{self.username}-cc-{self.country_code}',
                    'username': self.username,
                    'password': self.password
                }
                browser = await pw.firefox.launch(proxy=proxy, headless= True)
            else:
                browser = await pw.firefox.launch(headless = True)
            # browser = await pw.firefox.launch(headless=True)
            
            page = await browser.new_page()
            await self.perform_request_with_retry(page, url)
            answers = []

            answers_section = await page.query_selector('.a-section.a-spacing-large.askAnswersAndComments.askWrapText')
            answer_blocks = await answers_section.query_selector_all('.a-section.a-spacing-medium')
            for block in answer_blocks:
                answer_content_block = await block.query_selector('.askLongText')
                if not answer_content_block:
                    answer_content_block = await block.query_selector('span')
                answer_text = await answer_content_block.inner_text()
                answers.append(answer_text)
            print('answers added')

            next_button = await page.query_selector('.a-last')
            if next_button and await next_button.is_enabled():
                await next_button.click()
                print('question next button pressed')
                await asyncio.sleep(5)  # Ensure new page has loaded

                    
        await browser.close()
        return answers


        
    async def get_product_qa(self):
        qa_url = self.get_qa_url()
        async with async_playwright() as pw:
            if self.use_proxy:
                proxy = {
                    'server': self.proxy_ip,
                    # 'username': f'{self.username}-cc-{self.country_code}',
                    'username': self.username,
                    'password': self.password
                }
                browser = await pw.firefox.launch(proxy=proxy, headless= True)
            else:
                browser = await pw.firefox.launch(headless = True)
            browser = await pw.firefox.launch(headless=True)
            page = await browser.new_page()
            await self.perform_request_with_retry(page, qa_url)

            scrape_info = []
            unique_qa_ids = set()  # Set to store unique qa IDs
            qa_added = True

            while qa_added:
                qa_added = False
                qa_section = await page.query_selector('.a-section.askTeaserQuestions')
                selected_elements = await qa_section.query_selector_all('.a-fixed-left-grid.a-spacing-base')

                for element in selected_elements:
                    
                    question_element = await element.query_selector('.a-fixed-left-grid.a-spacing-small')

                    try:
                        answer_element = await element.query_selector('.a-fixed-left-grid.a-spacing-base .a-fixed-left-grid-col.a-col-right')
                    except:
                        pass

                    try:
                        q_id = await question_element.get_attribute('id')  # Get the ID attribute of the review div 
                        if q_id.startswith("question") and q_id not in unique_qa_ids:
                            unique_qa_ids.add(q_id)  # Add to the set of processed IDs
                            match = re.search(r'(?<=-).*', q_id)
                            qid_str = match.group(0)
                            print(qid_str)

                            qa_added = True
                            
                            qa_details = {}

                            # Scrape top answer
                            if answer_element:
                                answer_element_cont = await answer_element.query_selector('.askLongText')

                                if not answer_element_cont:
                                    answer_element_cont = await answer_element.query_selector('span')

                                if answer_element_cont:
                                    qa_details['answer'] = await answer_element_cont.inner_text()  
    


                            # Scrape question text                
                            question_text_element = await question_element.query_selector('.a-declarative')
                            if question_element:
                                qa_details['question'] = await question_text_element.inner_text()

                            
                            # Scrape votes for question
                            vote_element = await element. query_selector('.count')
                            if vote_element:
                                qa_details['votes'] = await vote_element.inner_text()  


                            try:
                            # Scrape multiple answers if present
                                all_answers_div_cont = await answer_element.query_selector(f'#askSeeAllAnswersLink-{qid_str}')
                                all_answers_div = await all_answers_div_cont.query_selector('.a-link-normal')
                                if all_answers_div:

                                    
                                    all_answers_url = await all_answers_div.get_attribute('href')
                                    print(all_answers_url)
                                    answers = await self.scrape_question_page(f'https://www.amazon.com{all_answers_url}')
                                    qa_details['all_answers'] = answers
                            except:
                                pass

                    except:
                        pass
                        scrape_info.append(qa_details)

                        
                        # BREAK LOOP FOR TESTING PURPOSES

                        # print(f"scrape len:{len(scrape_info)}")
                        # if len(scrape_info) == 8:
                        #     return scrape_info
                
                if not qa_added:
                    break

                next_button = await page.query_selector('.a-last')
                if next_button and await next_button.is_enabled():
                    await next_button.click()
                    await asyncio.sleep(5)  # Ensure new page has loaded
                else:
                    break
                    
        await browser.close()
        return scrape_info
    
    def dedup_qa(self, data:list)->list:
        """_summary_

        Args:
            data (list): QA list

        Returns:
            dedup_data(list): deduplicated QA
        """
        ques = set()
        dedup_data = [i for i in data if i['question'] not in ques and not ques.add(i['question'])]
        return dedup_data
        

    async def get_product_reviews(self):
        async with async_playwright() as pw:
            if self.use_proxy:
                proxy = {
                    'server': self.proxy_ip,
                    # 'username': f'{self.username}-cc-{self.country_code}',
                    'username': self.username,
                    'password': self.password
                }
                browser = await pw.firefox.launch(proxy=proxy, headless= True)
            else:
                browser = await pw.firefox.launch(headless = True)
            # browser = await pw.firefox.launch(headless=True)
            page = await browser.new_page()
            await self.perform_request_with_retry(page, self.url)
            scrape_info = []
            unique_review_ids = set()  # Set to store unique review IDs
            reviews_added = True

            first_button = await page.query_selector('.a-link-emphasis.a-text-bold')
            if first_button:
                await first_button.click()
                await asyncio.sleep(5)  

            while reviews_added:
                reviews_added = False
                selected_elements = await page.query_selector_all('.a-section.review.aok-relative')
                for element in selected_elements:
                    review_id = await element.get_attribute('id')  # Get the ID attribute of the review div 
                    
                    # Check if the review ID starts with "R" and is not already processed
                    if review_id.startswith("R") and review_id not in unique_review_ids:
                        unique_review_ids.add(review_id)  # Add to the set of processed IDs
                        reviews_added = True
                        
                        # Scrape review details
                        review_details = {}

                        review_title_element = await element.query_selector('.a-size-base.review-title.a-text-bold:not(span.a-icon-alt)')
                        review_text_element = await element.query_selector('.a-size-base.review-text.review-text-content')
                        review_rating_element = await element.query_selector(".a-icon-alt")
                        review_meta_element = await element.query_selector(".a-size-base.a-color-secondary.review-date")
                        
                        try:
                            review_helpfulness_element = await element.query_selector(".a-size-base.a-color-tertiary.cr-vote-text")
                            if review_helpfulness_element:
                                review_details['helpful'] = await review_helpfulness_element.inner_text()
                        except: 
                            pass
                        
                        title_text_total = await review_title_element.inner_text()
                        match = re.search(r'\n(.+)', title_text_total)
                        review_details['title'] = match.group(1)
                        
                        review_details['text'] = await review_text_element.inner_text()
                        rating_text = await review_rating_element.inner_text()
                        review_details['ratings'] = str(rating_text)[:3]
                        review_details['meta'] = await review_meta_element.inner_text()

                        scrape_info.append(review_details)
                        # print(len(scrape_info))
                        # if len(scrape_info) == 50:
                        #     return scrape_info
                
                if not reviews_added:
                    break

                next_button = await page.query_selector('.a-last')
                if next_button and await next_button.is_enabled():
                    await next_button.click()
                    await asyncio.sleep(5)  # Ensure new page has loaded
                else:
                    break
                    
        await browser.close()
        return scrape_info

    async def set_browser(self) -> None:
        async with async_playwright() as pw:
            if self.use_proxy:
                    proxy = {
                        'server': self.proxy_ip,
                        # 'username': f'{self.username}-cc-{self.country_code}',
                        'username': self.username,
                        'password': self.password
                    }
                    browser = await pw.firefox.launch(proxy=proxy, headless= True)
            else:
                browser = await pw.firefox.launch(headless = True)
            
            browser = browser
                
                
    async def run_dynamic_scraper(self):
        #TODO: account for no reviews/ no qa cases
        
        try:
            self.product_data['Reviews'] = await self.get_product_reviews()
        except:
            try:
                self.use_proxy = not self.use_proxy
                self.product_data['Reviews'] = await self.get_product_reviews()
            except Exception as e:
                print (f"{e} at reviews")
                pass
            
        try:
            qa = await self.get_product_qa()
            self.product_data['QA'] = self.dedup_qa(qa)
        except :
            try:
                self.use_proxy = not self.use_proxy
                qa = await self.get_product_qa()
                self.product_data['QA'] = self.dedup_qa(qa)
            except Exception as e:
                
                #TODO: SWITCH
                print (f"{e} at qa")
                pass
        
        self.product_data['fully_scraped'] = True
        # try:
        #     await browser.close()
        # except Exception as e:
        #     print (f"{e} at closing")
        
        return self.product_data
        
        
        