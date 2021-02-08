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
print('Imported all modules')

MAX_SCRAPE = 1000

# Test connecting to database

rsh = redshift_helper.RedShiftHelper()
print('Conencted to redshift')

# Test pulling unscraped data

unscraped_urls = rsh.get_unscraped_linkedin_urls(MAX_SCRAPE)
print('Pulled unscraped urls', len(unscraped_urls))

# Test pulling scraped data

existing_data = rsh.get_scraped_linkedin_urls()
print('Pulled scraped urls', len(existing_data))


# Check PSUTIL
print("CPU: {}".format(psutil.cpu_percent()))
print("RAM: {}".format(psutil.virtual_memory().percent))