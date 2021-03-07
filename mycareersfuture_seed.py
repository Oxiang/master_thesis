from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
from tqdm import tqdm
import random
import re
from redshift import redshift_helper
from datetime import date

'''
    int MAX_PAGE => the amount of pages to traverse through for a single job title search
    float RATE_LIMIT => amount of time in seconds to wait per page tranversal
'''
MAX_PAGE = 4
PAGE_WAIT = 10
RATE_LIMIT = 3
TIMEOUT = 30

# Search Url
search_url = 'https://www.mycareersfuture.gov.sg/'

# Load all known job titles
known_job_titles_table = pd.read_csv('known_job_titles.csv')
known_job_titles_list = known_job_titles_table['job_title'].tolist()

# Seed list
seed_set = set()

driver = webdriver.Chrome(r"C:\chromedriver.exe")
driver.set_page_load_timeout(TIMEOUT)

for title in tqdm(known_job_titles_list):
    try:
        driver.get(search_url)
        job_search_bar = driver.find_element_by_xpath('//*[@id="search-text"]')
        job_search_bar.clear()
        job_search_bar.send_keys(title)
        submit_search_job = driver.find_element_by_xpath('//*[@id="search-button"]')
        submit_search_job.click()
        
        time.sleep(PAGE_WAIT)
        list_of_jobs_ul = driver.find_element_by_xpath('//*[@id="search-results"]/div[3]')
        list_of_jobs = list_of_jobs_ul.find_elements_by_tag_name('div')
        for job in list_of_jobs:
            try:
                anchor = job.find_elements_by_tag_name('a')[0]
                link = anchor.get_attribute("href")
                seed_set.add(link)
            except:
                pass
        print("completed {}, current seed size {}".format(title,len(seed_set)))
    except Exception as e:
        print('Error occurred when searching for {}, {}'.format(title,e))
driver.quit()

# Randomize
seed_list = list(seed_set)
random.shuffle(seed_list)

rsh = redshift_helper.RedShiftHelper()
existing_scrape_key = rsh.get_all_scraped_mcf_urls_key()
exisiting_unscraped_key = rsh.get_all_unscraped_mcf_urls_keys()

# turn to set
exisiting_unscraped_key_set = set([str(k[0]) for k in exisiting_unscraped_key])
existing_scrape_key_set = set([str(k[0]) for k in existing_scrape_key])

# Duplicate filter before saving
duplicate_removal = {}
for n_url in seed_list:
    key = n_url.split("/")[-1]
    if key not in duplicate_removal and key not in existing_scrape_key_set and key not in exisiting_unscraped_key_set:
        duplicate_removal[key] = n_url

# Push into redshift
try:
    rsh.mapper_unscraped_mcf_urls(duplicate_removal)
    rsh.redshift_quit()
except Exception as e:
    today = date.today().strftime("%Y-%m-%d")
    seed_df = pd.DataFrame([[k,v] for k,v in duplicate_removal.items()], columns = ['jd_key', 'url'])
    seed_df.to_csv('tmp/mycareersfuture_seed_{}.csv'.format(today))
    print('Scraped {} seed links from mycareerfuture'.format(len(duplicate_removal)))
    print('Error in mycareersfuture seeding due to {} on {}. Saving in tmp folder'.format(e, today))
    