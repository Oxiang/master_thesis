import os
import pandas as pd
from bs4 import BeautifulSoup as bs
from selenium import webdriver
import re
from datetime import date, datetime
from tqdm import tqdm
from redshift import redshift_helper
import time
import numpy as np
import logging
import psutil
from selenium.webdriver.chrome.options import Options

MAX_SCRAPE = 200
TIMEOUT = 30
RATE_LIMIT = 1

rsh = redshift_helper.RedShiftHelper()
unscraped_urls = rsh.get_unscraped_glassdoor_urls(MAX_SCRAPE)
rsh.redshift_quit()
unscraped_urls_dic = {str(i[0]) : str(i[1]) for i in unscraped_urls}
unscraped_list = [str(url[1]) for url in unscraped_urls]

today = date.today().strftime("%Y-%m-%d")

# Extract standardized 
full_jd_dataset = []

driver = webdriver.Chrome(r"C:\chromedriver.exe")
driver.set_page_load_timeout(TIMEOUT)

for k,url in tqdm(unscraped_urls_dic.items()):
    try:
        job_id = str(k)
        base_url = url
        driver.get(base_url)
        new_soup = bs(driver.page_source, 'html.parser')
        
        # Title
        try:
            job_title = new_soup.find("div", class_="css-17x2pwl e11nt52q6").text
        except:
            job_title = ''
            
        # Company
        try:
            company = new_soup.find("div", class_="css-16nw49e e11nt52q1").text
        except:
            company = ''
            
        # salary
        try:
            salary = new_soup.find("span", class_="small css-10zcshf e1v3ed7e1").text
        except:
            salary = ''
            
        # Description
        try:
            description = new_soup.find("div", id="JobDescriptionContainer").text.replace("'", "\"")
        except:
            description = ''
            
        # Location
        try:
            location = new_soup.find("div", class_="css-1v5elnn e11nt52q2").text
        except:
            location = ''
            
        try:
            industry = new_soup.find("label", string="Industry").next_sibling.text
        except:
            industry = ''
        try:
            size_of_company = new_soup.find("label", string="Size").next_sibling.text
        except:
            size_of_company = ''
            
        # Post elapsed - glassdoor have no elapsed period
        elapsed_period = ''
        # job function - glassdoor have no job function
        job_function = ''
        # employment type - glassdoor have no employment type
        employment_type = ''
        # seniority of role -  seniority of role
        seniority = ''
        
        jd_data = [job_id,
                   base_url, #url
                   job_title, #jobtitle
                   salary, #payrange
                   location, #location
                   seniority,
                   industry,
                   employment_type, 
                   job_function,
                   size_of_company,
                   elapsed_period,
                   description, #jobdescription
                   today] #scrapedate] #time elapsed after posting
        
        full_jd_dataset.append(jd_data)
    except Exception as e:
        print("Error occur on {} with {}".format(job_id, e))
driver.quit()

# TODO: add filtering to discard empty job title and job description

# database operations
try:
    print('Pushing scraped data to redshift linkedin_scraped_data')
    rsh = redshift_helper.RedShiftHelper()
    rsh.mapper_glassdoor_scraped_data(full_jd_dataset)
    
    print('Deleting scraped URLS from redshift glassdoor_scraped_data')
    rsh.delete_unscraped_urls_glassdoor(unscraped_list)
    
    print('Glassdoor Scraping done')
    total_unscraped = rsh.get_all_unscraped_glassdoor_urls_keys()
    total_scraped = rsh.get_all_scraped_glassdoor_urls()
    print('Total unscraped glassdoor url = {}'.format(len(total_unscraped)))
    print('Total scraped glassdoor url = {}'.format(len(total_scraped)))
    
    rsh.redshift_quit()   
    
except Exception as e:
    jd_df = pd.DataFrame(full_jd_dataset, columns=['jobid', 
                                                   'url',
                                                   'title',
                                                   'salary',
                                                   'location',
                                                   'seniority',
                                                   'industry',
                                                   'employmet_type',
                                                   'job_function',
                                                   'size_of_company',
                                                   'elapsed_period',
                                                   'description',
                                                   'today'])
    jd_df.to_csv('tmp/glassdoor_scraping_{}.csv'.format(today))
    print('Scraped {} glassdoor links'.format(jd_df.shape[0]))
    print('Error in glassdoor scraping due to {} on {}. Saving in tmp folder'.format(e, today))