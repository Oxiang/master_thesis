# Testing importing modules
import pandas as pd
from bs4 import BeautifulSoup as bs
from selenium import webdriver
import re
from datetime import date
from tqdm import tqdm
from redshift import redshift_helper
import time
import numpy as np
import psutil
print('Running network test')
print('Imported all modules')

# Test connecting to database
rsh = redshift_helper.RedShiftHelper()
print('Connected to redshift')

unscraped_urls = rsh.get_all_unscraped_linkedin_urls()
print('Pulled linkedin unscraped urls:', len(unscraped_urls))

existing_data = rsh.get_scraped_linkedin_urls()
print('Pulled linkedin scraped urls:', len(existing_data))

existing_data = rsh.get_all_unscraped_glassdoor_urls_keys()
print('Pulled glassdoor unscraped urls:', len(existing_data))

existing_data = rsh.get_all_scraped_glassdoor_urls()
print('Pulled glassdoor scraped urls:', len(existing_data))

existing_data = rsh.get_all_unscraped_mcf_urls_keys()
print('Pulled mycareersfuture unscraped urls:', len(existing_data))

existing_data = rsh.get_all_scraped_mcf_urls_key()
print('Pulled mycareersfuture scraped urls:', len(existing_data))

# Check PSUTIL
print("CPU: {}".format(psutil.cpu_percent()))
print("RAM: {}".format(psutil.virtual_memory().percent))

rsh.redshift_quit()