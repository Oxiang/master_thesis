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

MAX_SCRAPE = 500
TIMEOUT = 30
RATE_LIMIT = 1
SKIP_RATE = 3

'''
Set Logging
'''
logging.basicConfig(filename='logs/linkedin_scrape.log', filemode='w',format='%(asctime)s - %(message)s', level=logging.INFO)
now = datetime.now()
logging.info('Starting script on {}'.format(now.strftime("%m/%d/%Y, %H:%M:%S")))

starttime = time.time()

logging.info('Pulling data from linkedin_unscraped_urls')
print('Pulling data from linkedin_unscraped_urls')

'''
Load unscraped url from redshift
'''

try:
    rsh = redshift_helper.RedShiftHelper()
    unscraped_urls = rsh.get_unscraped_linkedin_urls(MAX_SCRAPE)
    unscraped_list = [str(url[1]) for url in unscraped_urls]
except Exception as e:
    logging.exception("Error with {} | CPU: {} | RAM: {}".format(e, psutil.cpu_percent(), psutil.virtual_memory().percent))
    raise Exception("Error with {}".format(e))

today = date.today().strftime("%Y-%m-%d")

# Extract standardized 
full_jd_dataset = []
new_urls = set()

logging.info('Start scraping')
print('Start scraping')

try:
    options = Options()
    options.add_argument('--headless')
    if os.name == 'nt':
        driver = webdriver.Chrome(r"C:\chromedriver.exe")
        logging.info('Using Windows')
    else:
        driver = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=options)
        logging.info('Using Linux, healdless. OS name: {}'.format(os.name))
    driver.set_page_load_timeout(TIMEOUT)
except Exception as e:
    logging.exception("Error with {} | CPU: {} | RAM: {}".format(e, psutil.cpu_percent(), psutil.virtual_memory().percent))
    raise Exception("Error with {}".format(e))


for url in tqdm(unscraped_list):
    try:
        time.sleep(RATE_LIMIT)
        base_url = url
        driver.get(base_url)
        new_soup = bs(driver.page_source, 'html.parser')
        
        # To stop duplication
        try:
            base_data = base_url.split("?")[0].split("/")[-1]
        except:
            base_data = ''
        
        # Title
        try:
            job_title = new_soup.find("h1", class_='topcard__title').text
        except:
            job_title = ''
            
        # Company
        try:
            company = new_soup.find("span", class_="topcard__flavor").text
        except:
            company = ''
            
        # salary
        try:
            salary = new_soup.find("div", class_="salary").text
        except:
            salary = ''
            
        # Description
        try:
            description = new_soup.find("div",class_="description__text").text
        except:
            description = ''
            
        # Location
        try:
            location = new_soup.find("span", class_="topcard__flavor topcard__flavor--bullet").text
        except:
            location = ''
            
        # Post elapsed
        try:
            post_elapsed = new_soup.find("span", class_="topcard__flavor--metadata posted-time-ago__text").text
        except:
            post_elapsed = ''
            
        # Grouped data - Seniority level, employment type, job function, industries
        bundle_data = ['', '', '', '']
        try:    
            grouped_data = new_soup.find("ul", class_="job-criteria__list")
            for i,ind_data in enumerate(grouped_data):
                idx_data = ind_data.find("span", class_="job-criteria__text job-criteria__text--criteria").text
                bundle_data[i] = idx_data
        except:
            pass
        
        jd_data = [base_url, #url
                   job_title, #jobtitle
                   description, #jobdescription
                   salary, #payrange
                   location, #location
                   bundle_data[0], #senioritylevel
                   bundle_data[3], #industry
                   bundle_data[1], #employmenttype
                   bundle_data[2], #jobfunction
                   '', #sizeofcompany
                   today, #scrapedate
                   post_elapsed] #time elapsed after posting
                
        full_jd_dataset.append(jd_data)
        
        # Extract other urls
        try:
            similar_jobs = new_soup.find('h2', class_="similar-jobs__header")
            for anchor in new_soup.find_all(href=True):
                job_link = anchor['href']
                # Ensure job only url
                if re.search("linkedin.com/jobs/", job_link):
                    # Ensure it is not the same url
                    if base_data:
                        try:
                            job_data = job_link.split("?")[0].split("/")[-1]
                        except:
                            job_data = ''
                        if base_data != job_data:
                            new_urls.add(job_link)
        except:
            pass
    except Exception as e:
        logging.exception("Error with {} | CPU: {} | RAM: {}".format(e, psutil.cpu_percent(), psutil.virtual_memory().percent))
        # raise Exception("Error with {}".format(e))
        # If an error occur, move to next page after SKIP_RATE seconds
        time.sleep(SKIP_RATE)
    
driver.quit()

logging.info('Ended Scrape')
print('Ended Scrape')

logging.info('Removing empty scraped data')
print('Removing empty scraped data')

# Remove expired jd - i.e. description and job title are empty
legit_jd_dataset = []
for jd in full_jd_dataset:
    title = jd[1]
    description = jd[2]
    if (len(title) == 0) or (len(description) == 0):
        pass
    else:
        legit_jd_dataset.append(jd)
        
logging.info('Preparing scraped data to be passed to redshift')
print('Preparing scraped data to be passed to redshift')

# Prepare data Export data
export_data = pd.DataFrame(legit_jd_dataset, columns=['url',
                                                     'jobtitle',
                                                     'jobdescription',
                                                     'payrange',
                                                     'location',
                                                     'senioritylevel',
                                                     'industry',
                                                     'employmenttype',
                                                     'jobfuction',
                                                     'sizeofcompany',
                                                     'scrapeddate',
                                                     'elapsedpostingdate'])

# Prepare data - List format, remove first column index, shorten urls, change Nan to empty string, remove single apostrophe
parsed_scraped_data = export_data.copy()
parsed_scraped_data['url'] = parsed_scraped_data['url'].apply(lambda x: x.split("?")[0])
parsed_scraped_data.replace("'", '', regex=True, inplace=True)
parsed_scraped_data.replace(np.nan, '', regex=True, inplace=True)
scraped_data_list = parsed_scraped_data.to_numpy().tolist()
        
logging.info('Pushing scraped data to redshift linkedin_scraped_data. New data {}'.format(len(scraped_data_list)))
print('Pushing scraped data to redshift linkedin_scraped_data. New data {}'.format(len(scraped_data_list)))

# Push everything to redshift under unscraped_linkedin_urls table
try:
    rsh = redshift_helper.RedShiftHelper()
    rsh.mapper_linkedin_scraped_data(scraped_data_list)
    rsh.redshift_quit()
except Exception as e:
    logging.exception("Error with {} | CPU: {} | RAM: {}".format(e, psutil.cpu_percent(), psutil.virtual_memory().percent))
    raise Exception("Error with {}".format(e))

logging.info('Removing duplicates from scraped data based on linkedin_scraped_data')
print('Removing duplicates from scraped data based on linkedin_scraped_data')

# Check if the data already exists in database
try:
    rsh = redshift_helper.RedShiftHelper()
    existing_data = rsh.get_scraped_linkedin_urls()
    unscraped_data = rsh.get_all_unscraped_linkedin_urls()
    rsh.redshift_quit()
except Exception as e:
    logging.exception("Error with {} | CPU: {} | RAM: {}".format(e, psutil.cpu_percent(), psutil.virtual_memory().percent))
    raise Exception("Error with {}".format(e))

existing_url_list = [str(url[0]) for url in existing_data]

# Create existing urls hashmap - for scraped urls
existing_url_map = {}
for url in existing_url_list:
    key = url.split('/')[-1]
    existing_url_map[key] = 1
    
# update existing urls hashmap for unscraped urls
for url in unscraped_data:
    existing_url_map[str(url[0])] = 1

# Remove duplicated urls
new_unscraped_url = {}
for url in new_urls:
    key = url.split("?")[0].split("/")[-1]
    short_url = url.split("?")[0]
    if key not in existing_url_map:
        new_unscraped_url[key] = short_url

logging.info('Deleting scraped urls from linkedin_unscraped_data')
print('Deleting scraped urls from linkedin_unscraped_data')

try:
    rsh = redshift_helper.RedShiftHelper()
    rsh.delete_unscraped_urls(unscraped_list)
    rsh.redshift_quit()
except Exception as e:
    logging.exception("Error with {} | CPU: {} | RAM: {}".format(e, psutil.cpu_percent(), psutil.virtual_memory().percent))
    raise Exception("Error with {}".format(e))

logging.info('Pusing new unscraped urls to linkedin_unscraped_data')
print('Pusing new unscraped urls to linkedin_unscraped_data')

try:
    rsh = redshift_helper.RedShiftHelper()
    rsh.mapper_unscraped_linkedin_urls(new_unscraped_url)
    rsh.redshift_quit()
except Exception as e:
    logging.exception("Error with {} | CPU: {} | RAM: {}".format(e, psutil.cpu_percent(), psutil.virtual_memory().percent))
    raise Exception("Error with {}".format(e))

endtime = time.time()
elasped = endtime - starttime
logging.info('Time taken for scraping {} data | Timeout : {} | Rate limit: {} is {}'.format(MAX_SCRAPE, TIMEOUT, RATE_LIMIT, elasped))