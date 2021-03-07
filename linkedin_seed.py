from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
from tqdm import tqdm
import random
from redshift import redshift_helper

# Search Url
search_url = 'https://www.linkedin.com/jobs'

# Load all known job titles
known_job_titles_table = pd.read_csv('known_job_titles.csv')
known_job_titles_list = known_job_titles_table['job_title'].tolist()

# Seed list
seed_set = set()

driver = webdriver.Chrome(r"C:\chromedriver.exe")

for title in tqdm(known_job_titles_list):
    try:
        # Initiate search
        driver.get(search_url)
        job_search_bar = driver.find_element_by_xpath('//*[@id="JOBS"]/section[1]/input')
        job_search_bar.clear()
        job_search_bar.send_keys(title)
        location_search_bar = driver.find_element_by_xpath('//*[@id="JOBS"]/section[2]/input')
        location_search_bar.clear()
        location_search_bar.send_keys("Singapore")
        submit_search_job = driver.find_element_by_xpath('/html/body/main/section[1]/section/div[2]/button[2]')
        submit_search_job.click()
        
        # Extract URLs and save
        random_selector = driver.find_element_by_xpath('/html/body/header/nav/section/section[2]/form/section[1]/input')
        # Scrolls down to reveal more job description - more you scroll down, the longer the list
        for i in range(15):
            random_selector.send_keys(Keys.PAGE_DOWN)
            time.sleep(0.2)
        job_result = driver.find_element_by_xpath('//*[@id="main-content"]/div/section/ul')
        job_result_list = job_result.find_elements_by_tag_name('li')
        for idx in range(1, len(job_result_list)+1):
            anchor = driver.find_element_by_xpath('//*[@id="main-content"]/div/section/ul/li[{}]/a'.format(idx))
            link = anchor.get_attribute("href")
            seed_set.add(link)
        driver.quit()
    except Exception as e:
        print('Error occurred when searching for {}, {}'.format(title,e))

driver.quit()
# Randomize
seed_list = list(seed_set)
random.shuffle(seed_list)

rsh = redshift_helper.RedShiftHelper()
existing_scrape_key = rsh.get_scraped_linkedin_urls()
exisiting_unscraped_key = rsh.get_all_unscraped_linkedin_urls()

# turn to set
exisiting_unscraped_key_set = set([str(k[0]) for k in exisiting_unscraped_key])
existing_scrape_key_set = set([str(k[0]) for k in existing_scrape_key])

# remove duplicates and remove url with more than 256 characters
duplicate_removal = {}
for n_url in seed_list:
    key = n_url.split("?")[0].split("/")[-1]
    cut_url = n_url.split("&")[0]
    if key not in duplicate_removal and key not in existing_scrape_key_set and key not in exisiting_unscraped_key_set:
        if len(cut_url) <= 256 and len(key) <= 256:
            duplicate_removal[key] = cut_url

# push to redshift
print('Scraped {} seed links from linkedin'.format(len(duplicate_removal)))
try:
    rsh.mapper_unscraped_linkedin_urls(duplicate_removal)
    rsh.redshift_quit()
    print('Successfully pushed linkedin seed links to redshift')
except Exception as e:
    today = date.today().strftime("%Y-%m-%d")
    seed_df = pd.DataFrame([[k,v] for k,v in duplicate_removal.items()], columns = ['jd_key', 'url'])
    seed_df.to_csv('tmp/linkedin_seed_{}.csv'.format(today))
    print('Error in linkedin seeding due to {} on {}. Saving in tmp folder'.format(e, today))
    