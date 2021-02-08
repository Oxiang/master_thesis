from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
from tqdm import tqdm
import random

# Search Url
search_url = 'https://www.linkedin.com/jobs'

# Load all known job titles
known_job_titles_table = pd.read_csv('known_job_titles.csv')
known_job_titles_list = known_job_titles_table['job_title'].tolist()

# Seed list
seed_set = set()

for title in tqdm(known_job_titles_list):
    try:
        # Initiate search
        driver = webdriver.Chrome(r"C:\chromedriver.exe")
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

# Duplicate filter before saving
# We can filter out string after linkedin.com/jobs/view/ THIS THIS THIS ? ...
# E.g. We can extract scientist-3-month-contract-at-a-star-agency-for-science-technology-and-research-2335113979
# from https://sg.linkedin.com/jobs/view/scientist-3-month-contract-at-a-star-agency-for-science-technology-and-research-2335113979?refId=44ec646e-345f-4c85-a339-df8830cc0d9c&trackingId=%2FeOErzvp7C05gKb%2BSeu%2BcQ%3D%3D&position=4&pageNum=0&trk=public_jobs_job-result-card_result-card_full-click
duplicate_removal = {}
for n_url in seed_list:
    key = n_url.split("?")[0].split("/")[-1]
    if key not in duplicate_removal:
        duplicate_removal[key] = n_url

no_duplicate_new_urls = list(duplicate_removal.values())

# Export to csv
export_data = pd.DataFrame(no_duplicate_new_urls, columns=['urls'])
export_data.to_csv('unscrapped_linkedin_url.csv')