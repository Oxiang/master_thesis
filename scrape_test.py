import time
import re
import os
from datetime import date
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

BASE_URL = 'https://sg.linkedin.com/jobs/view/technical-solutions-specialist-collaboration-contact-center-at-cisco-2392372728'
TIMEOUT = 30
RATE_LIMIT = 0.3
today = date.today().strftime("%Y-%m-%d")

options = Options()
options.add_argument('--headless')
if os.name == 'nt':
    driver = webdriver.Chrome(r"C:\chromedriver.exe")
else:
    driver = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=options)
driver.set_page_load_timeout(TIMEOUT)

full_jd_dataset = []
new_urls = set()

try:
    time.sleep(RATE_LIMIT)
    base_url = BASE_URL
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
    print("Error with {}".format(e))
driver.quit()
    
print(full_jd_dataset)
print(new_urls)