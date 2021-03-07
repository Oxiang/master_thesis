import psycopg2
import json
import numpy as np
from datetime import date, datetime

# read cred
with open(r'./credentials.json') as f:
    creds = json.load(f)
    
DBNAME = creds['DBNAME']
HOST = creds['HOST']
PORT = creds['PORT']
USER = creds['USER']
PASSWORD = creds['PASSWORD']

RETRIES = 3

class RedShiftHelper:
    
    def __init__(self):
        self.__connection()
        
    def __connection(self):
        retry_counter = 0
        while retry_counter < RETRIES:
            try:
                self.con = psycopg2.connect(dbname= DBNAME, host= HOST, port= PORT, user= USER, password= PASSWORD)
                print("Reconnection successful")
                return
            except:
                print('Unsuccessful reconnection : {} tries '.format(retry_counter + 1))
                retry_counter += 1
        print('Fail to establish database connection after {} tries'.format(RETRIES))
        
    def __check_status(self):
        if not self.con.status:
            print('re-establishing connection')
            self.__connection()

        
    def redshift_quit(self):
        self.con.close()
        
    def getAll(self):
        cur = self.con.cursor()
        cur.execute("SELECT * FROM linkedin_scraped_data;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_all_scraped_mcf_urls_key(self):
        cur = self.con.cursor()
        cur.execute("SELECT jdid FROM mycareersfuture_scraped_data;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_all_unscraped_linkedin_urls(self):
        self.__check_status()
        cur = self.con.cursor()
        cur.execute("SELECT * FROM linkedin_unscraped_urls;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_all_unscraped_mcf_urls_keys(self):
        self.__check_status()
        cur = self.con.cursor()
        cur.execute("SELECT jd_key FROM mycareersfuture_unscraped_urls;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_all_unscraped_glassdoor_urls_keys(self):
        self.__check_status()
        cur = self.con.cursor()
        cur.execute("SELECT jd_key FROM glassdoor_unscraped_urls;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_unscraped_linkedin_urls(self, limit: int):
        self.__check_status()
        cur = self.con.cursor()
        cur.execute("SELECT * FROM linkedin_unscraped_urls ORDER BY jd_key DESC LIMIT {}".format(limit))
        data = np.array(cur.fetchall())
        cur.close()
        return data

    def get_unscraped_mcf_urls(self, limit: int):
        self.__check_status()
        cur = self.con.cursor()
        cur.execute("SELECT * FROM mycareersfuture_unscraped_urls ORDER BY jd_key DESC LIMIT {}".format(limit))
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_unscraped_glassdoor_urls(self, limit: int):
        self.__check_status()
        cur = self.con.cursor()
        cur.execute("SELECT * FROM glassdoor_unscraped_urls limit {};".format(limit))
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_scraped_linkedin_urls(self):
        self.__check_status()
        cur = self.con.cursor()
        cur.execute("SELECT Url FROM linkedin_scraped_data;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_all_scraped_glassdoor_urls(self):
        self.__check_status()
        cur = self.con.cursor()
        cur.execute("SELECT Url FROM glassdoor_scraped_data;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
        
    def mapper_unscraped_linkedin_urls(self, jd_set: dict):
        self.__check_status()
        if len(jd_set) == 0:
            print('no unscraped url available')
            return
        now = datetime.now()
        formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
        query_list = []
        query_header = 'INSERT INTO linkedin_unscraped_urls ( jd_key, url, entry_date ) VALUES'
        query_list.append(query_header)
        for k,v in jd_set.items():
            jd_query = "( '{}', '{}', '{}' ),".format(k,v, formatted_date)
            query_list.append(jd_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def mapper_unscraped_mcf_urls(self, jd_set: dict):
        self.__check_status()
        if len(jd_set) == 0:
            print('no unscraped url available')
            return
        now = datetime.now()
        formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
        query_list = []
        query_header = 'INSERT INTO mycareersfuture_unscraped_urls ( jd_key, url, entry_date ) VALUES'
        query_list.append(query_header)
        for k,v in jd_set.items():
            jd_query = "( '{}', '{}', '{}' ),".format(k,v, formatted_date)
            query_list.append(jd_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def mapper_unscraped_glassdoor_urls(self, jd_set: dict):
        self.__check_status()
        if len(jd_set) == 0:
            print('no unscraped url available')
            return
        query_list = []
        query_header = 'INSERT INTO glassdoor_unscraped_urls ( jd_key, url ) VALUES'
        query_list.append(query_header)
        for k,v in jd_set.items():
            jd_query = "( '{}', '{}' ),".format(k,v)
            query_list.append(jd_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def mapper_linkedin_scraped_data(self, dataset: list):
        self.__check_status()
        if len(dataset) == 0:
            print('no scraped data available')
            return
        query_list = []
        query_header = 'INSERT INTO linkedin_scraped_data ( url, jobtitle, jobdescription, payrange, location, senioritylevel, industry, employmenttype, jobfunction, sizeofcompany, scrapeddate, elaspedperiod ) VALUES'
        query_list.append(query_header)
        for row in dataset:
            data_query = "( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11])
            query_list.append(data_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def mapper_glassdoor_scraped_data(self, dataset: list):
        self.__check_status()
        if len(dataset) == 0:
            print('no scraped data available')
            return
        query_list = []
        query_header = 'INSERT INTO glassdoor_scraped_data ( jdid, url, jobtitle, payrange, location, senioritylevel, industry, employmenttype, jobfunction, sizeofcompany, elaspedperiod, jobdescription, scrapeddate ) VALUES'
        query_list.append(query_header)
        for row in dataset:
            data_query = "( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12])
            query_list.append(data_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def mapper_mcf_scraped_data(self, dataset: list):
        self.__check_status()
        if len(dataset) == 0:
            print('no scraped data available')
            return
        query_list = []
        query_header = 'INSERT INTO mycareersfuture_scraped_data ( jdid, url, jobtitle, payrange, location, senioritylevel, industry, employmenttype, jobfunction, sizeofcompany, elaspedperiod, jobdescription, scrapeddate ) VALUES'
        query_list.append(query_header)
        for row in dataset:
            data_query = "( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12])
            query_list.append(data_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
    
    def delete_unscraped_urls_glassdoor(self, url_list: list):
        self.__check_status()
        if len(url_list) == 0:
            print('suppose to raise an error here. but delete list is empty')
            return
        compile_query = []
        for url in url_list:
            compile_query.append("'{}'".format(url))
        delete_query = 'DELETE FROM glassdoor_unscraped_urls WHERE Url in ({})'.format(",".join(compile_query))
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()   
        
    def delete_unscraped_urls(self, url_list: list):
        self.__check_status()
        compile_query = []
        for url in url_list:
            compile_query.append("'{}'".format(url))
        delete_query = 'DELETE FROM linkedin_unscraped_urls WHERE Url in ({})'.format(",".join(compile_query))
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def delete_unscraped_mcf_urls(self, url_list: list):
        self.__check_status()
        if len(url_list) == 0:
            print('suppose to raise an error here. but delete list is empty')
            return
        compile_query = []
        for url in url_list:
            compile_query.append("'{}'".format(url))
        delete_query = 'DELETE FROM mycareersfuture_unscraped_urls WHERE Url in ({})'.format(",".join(compile_query))
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()   
        
    def delete_known_job_titles(self):
        self.__check_status()
        delete_query = 'DELETE FROM known_job_titles'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()     
        
    def mapper_known_job_titles(self, job_title_list: list):
        self.__check_status()
        if len(job_title_list) == 0:
            print('no job title available')
            return
        query_list = []
        query_header = 'INSERT INTO known_job_titles ( job_title ) VALUES'
        query_list.append(query_header)
        for title in job_title_list:
            title_query = "( '{}' ),".format(title)
            query_list.append(title_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def delete_known_skills(self):
        self.__check_status()
        delete_query = 'DELETE FROM known_skills'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def mapper_known_skills(self, known_skills_list: list):
        self.__check_status()
        if len(known_skills_list) == 0:
            print('no skills available')
            return
        query_list = []
        query_header = 'INSERT INTO known_skills ( keywords ) VALUES'
        query_list.append(query_header)
        for row in known_skills_list:
            title_query = "( '{}' ),".format(row[0])
            query_list.append(title_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def delete_ml_job_skill_score(self):
        self.__check_status()
        delete_query = 'DELETE FROM ml_job_skill_score'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def mapper_ml_job_skill_score(self, ml_job_skill_score: list):
        self.__check_status()
        if len(known_skills_list) == 0:
            print('no ml job skills score available')
            return
        query_list = []
        query_header = 'INSERT INTO ml_job_skill_score ( skill, job_title, relevance_score ) VALUES'
        query_list.append(query_header)
        for row in ml_job_skill_score:
            title_query = "( '{}', '{}', '{}' ),".format(row[0], row[1], row[2])
            query_list.append(title_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def delete_ml_job_score(self):
        self.__check_status()
        delete_query = 'DELETE FROM ml_job_score'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def mapper_ml_job_score(self, ml_job_score: list):
        self.__check_status()
        if len(ml_job_score) == 0:
            print('no ml job score available')
            return
        query_list = []
        query_header = 'INSERT INTO ml_job_score ( job_title, total_score ) VALUES'
        query_list.append(query_header)
        for row in ml_job_score:
            title_query = "( '{}', '{}'),".format(row[0], row[1])
            query_list.append(title_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()
        
    def delete_ml_job_info(self):
        self.__check_status()
        delete_query = 'DELETE FROM ml_job_info'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def mapper_ml_job_info(self, ml_job_info: list):
        self.__check_status()
        if len(ml_job_info) == 0:
            print('no ml job info available')
            return
        query_list = []
        query_header = 'INSERT INTO ml_job_info ( job_title, pay, opportunity, pay_score, opportunity_score ) VALUES'
        query_list.append(query_header)
        for row in ml_job_info:
            title_query = "( '{}', '{}', '{}', '{}', '{}'),".format(row[0], row[1], row[2], row[3], row[4])
            query_list.append(title_query)
        complete_query = " ".join(query_list)
        complete_query = complete_query[:-1]
        cur = self.con.cursor()
        cur.execute(complete_query)
        self.con.commit()
        cur.close()