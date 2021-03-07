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
RATE_LIMIT = 0.5

# Search Url
search_url = 'https://www.glassdoor.sg/Job/index.htm'

# Load all known job titles
known_job_titles_table = pd.read_csv('known_job_titles.csv')
known_job_titles_list = known_job_titles_table['job_title'].tolist()

# Seed list
seed_set = set()

for title in known_job_titles_list:
    try:
        driver = webdriver.Chrome(r"C:\chromedriver.exe")
        driver.get(search_url)
        job_search_bar = driver.find_element_by_xpath('//*[@id="KeywordSearch"]')
        job_search_bar.clear()
        job_search_bar.send_keys(title)
        submit_search_job = driver.find_element_by_xpath('//*[@id="HeroSearchButton"]')
        submit_search_job.click()
        
        current_url = driver.current_url
        for i in range(2,MAX_PAGE+1):
            try:
                time.sleep(RATE_LIMIT)
                list_of_jobs_ul = driver.find_element_by_xpath('//*[@id="MainCol"]/div[1]/ul')
                list_of_jobs = list_of_jobs_ul.find_elements_by_tag_name('li')
                for job in list_of_jobs:
                    anchor = job.find_elements_by_tag_name('a')[0]
                    link = anchor.get_attribute("href")
                    seed_set.add(link)
                next_url = current_url[:-4] + "_IP{}".format(i) + current_url[-4:]
                driver.get(next_url)
            except Exception as e:
                print('Page occurred when searching for {}, page {}'.format(title,i))
    except Exception as e:
        print('Error occurred when searching for {}, {}'.format(title,e))
    driver.quit()

# Randomize
seed_list = list(seed_set)
random.shuffle(seed_list)

# Duplicate filter before saving

duplicate_removal = {}
max_size = 0
for n_url in seed_list:
    # restructure the url as some of them are very long
    key = re.findall("jobListingId=\d*", n_url)[0].split("=")[-1]
    pos = re.findall("pos=\d*", n_url)[0]
    ao = re.findall("ao=\d*", n_url)[0]
    s = re.findall("s=\d*", n_url)[0]
    guid = re.findall("guid=.+?(?=&)", n_url)[0]
    src = re.findall("src=.+?(?=&)", n_url)[0]
    t = re.findall("t=.+?(?=&)", n_url)[0]
    vt = re.findall("vt=.+?(?=&)", n_url)[0]
    cs = re.findall("cs=.+?(?=&)", n_url)[0]
    cb = re.findall("cb=.+?(?=&)", n_url)[0]
    joblistingid = re.findall("jobListingId=\d*", n_url)[0]
    full_set = [pos, ao, s, guid, src, t, vt, cs, cb , joblistingid]
    new_url = "https://www.glassdoor.sg/partner/jobListing.htm?" + "&".join(full_set)
    duplicate_removal[key] = new_url
    max_size = max(max_size, len(new_url))
print("max restructured url is", max_size)

try:
    rsh = redshift_helper.RedShiftHelper()
    rsh.mapper_unscraped_glassdoor_urls(duplicate_removal)
    rsh.redshift_quit()
except Exception as e:
    today = date.today().strftime("%Y-%m-%d")
    seed_df = pd.DataFrame([[k,v] for k,v in duplicate_removal.items()], columns = ['jd_key', 'url'])
    seed_df.to_csv('tmp/glassdoor_seed_{}.csv'.format(today))
    print('Scraped {} seed links from glassdoor'.format(len(duplicate_removal)))
    print('Error in glassdoor seeding due to {} on {}. Saving in tmp folder'.format(e, today))