import pandas as pd
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from datetime import date
from tqdm import tqdm
from redshift import redshift_helper
import numpy as np
import time
import logging
import random
from selenium.webdriver.chrome.options import Options

MAX_SCRAPE = 200
TIMEOUT = 30
PAGE_WAIT = 2.5
BASE_URL = "https://mycareersfuture.gov.sg"

rsh = redshift_helper.RedShiftHelper()
unscraped_urls = rsh.get_unscraped_mcf_urls(MAX_SCRAPE)
unscraped_urls_dic = {str(i[0]) : str(i[1]) for i in unscraped_urls}
unscraped_list = [str(url[1]) for url in unscraped_urls]

today = date.today().strftime("%Y-%m-%d")

# Extract standardized 
full_jd_dataset = []
new_urls = set()

# TODO: Put to headless
driver = webdriver.Chrome(r"C:\chromedriver.exe")
driver.set_page_load_timeout(TIMEOUT)

for k,url in tqdm(unscraped_urls_dic.items()):
    try:
        job_id = str(k)
        base_url = url
        driver.get(base_url)
        new_soup = bs(driver.page_source, 'html.parser')
        time.sleep(PAGE_WAIT)
        
        # Title
        try:
            job_title = new_soup.find("h1", id='job_title').text
        except:
            job_title = ''
            
        # Company
        try:
            company = new_soup.find("p", class_="f6 fw6 mv0 black-80 mr2 di ttu").text
        except:
            company = ''
            
        # salary
        try:
            salary = new_soup.find("div", class_="salary tr-l").text
        except:
            salary = ''
            
        # Description
        try:
            description = new_soup.find("div", id="description-content").text.replace("'", "\"")
        except:
            description = ''
            
        # Location
        try:
            location = new_soup.find("a", class_="link dark-pink underline-hover").text
        except:
            location = ''
            
        try:
            industry = new_soup.find("p", id="job-categories").text
        except:
            industry = ''

        # mycareersfuture has no size of company
        size_of_company = ''
            
        # Post elapsed - glassdoor have no elapsed period
        try:
            elapsed_period = new_soup.find("span", id="last_posted_date").text
        except:
            elapsed_period = ''
            
        try:
            job_function = new_soup.find("p", id="job-categories").text
        except:
            job_function = ''

        try:
            employment_type = new_soup.find("p", id="employment_type").text
        except:           
            employment_type = ''
            
        try:
            seniority = new_soup.find("p", id="seniority").text
        except:  
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
        
        # Other urls
        try:
            other_url_list = new_soup.findAll("div", {"data-cy": "suggested-jobs"})
            print(len(other_url_list))
            for card in other_url_list[0].find_all(href=True):
                job_link = card['href'].split("?")[0]
                if 'job' in job_link:
                    new_urls.add(BASE_URL + job_link)
        except:
            pass
        
    except Exception as e:
        print("Error occur on {} with {}".format(job_id, e))
driver.quit()

# Clean data
clean_df = pd.DataFrame(full_jd_dataset, columns=['jobid',
                                                  'base_url',
                                                  'job_title',
                                                  'salary',
                                                  'location',
                                                  'senioritylevel',
                                                  'industry',
                                                  'employmenttype',
                                                  'jobfuction',
                                                  'sizeofcompany',
                                                  'elapsed_period',
                                                  'description',
                                                  'today'])
parsed_scraped_data = clean_df.copy()
parsed_scraped_data.replace("'", '', regex=True, inplace=True)
parsed_scraped_data.replace("", np.nan, regex=True, inplace=True)
# parsed_scraped_data = parsed_scraped_data[parsed_scraped_data['job_title'].notnull()]
parsed_scraped_data.dropna(subset=['job_title','description'],inplace=True)
scraped_data_list = parsed_scraped_data.to_numpy().tolist()

# TODO -> check if the website is down and notify, stop scraping

# database operations
try:
    print('Pushing scraped data to redshift mycareersfuture_scraped_data')
    rsh = redshift_helper.RedShiftHelper()
    rsh.mapper_mcf_scraped_data(scraped_data_list)
    
    print('Deleting scraped URLS from redshift mycareersfuture_scraped_data')
    rsh.delete_unscraped_mcf_urls(unscraped_list)
    
    print('mycareersfuture Scraping done')
    
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
    jd_df.to_csv('tmp/mycareersfuture_scraping_{}.csv'.format(today))
    print('Scraped {} mycareersfuture links'.format(jd_df.shape[0]))
    print('Error in mycareersfuture scraping due to {} on {}. Saving in tmp folder'.format(e, today))
    
# saving other links
# TODO: since this is the same as seeding procedure, we can create a function for it
seed_list = list(new_urls)
random.shuffle(seed_list)

existing_data = rsh.get_all_scraped_mcf_urls_key()
unscraped_data = rsh.get_all_unscraped_mcf_urls_keys()
    
# turn to set
existing_unscraped_key_set = set([str(k[0]) for k in unscraped_data])
existing_scrape_key_set = set([str(k[0]) for k in existing_data])

# remove duplicates and remove url with more than 256 characters
duplicate_removal = {}
for n_url in seed_list:
    key = n_url.split("?")[0].split("/")[-1]
    cut_url = n_url.split("&")[0]
    if key not in duplicate_removal and key not in existing_scrape_key_set and key not in exisiting_unscraped_key_set:
        if len(cut_url) <= 256 and len(key) <= 256:
            duplicate_removal[key] = cut_url
            
# push to redshift
print('Scraped {} secondary links from mycareersfuture'.format(len(duplicate_removal)))
try:
    rsh.mapper_unscraped_linkedin_urls(duplicate_removal)
    rsh.redshift_quit()
    print('Successfully pushed mycareersfuture secondary links to redshift')
except Exception as e:
    today = date.today().strftime("%Y-%m-%d")
    seed_df = pd.DataFrame([[k,v] for k,v in duplicate_removal.items()], columns = ['jd_key', 'url'])
    seed_df.to_csv('tmp/mycareersfuture_secondary_links_{}.csv'.format(today))
    print('Error in mycareersfuture secondary links due to {} on {}. Saving in tmp folder'.format(e, today))