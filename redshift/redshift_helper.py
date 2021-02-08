import psycopg2
import json
import numpy as np

# read cred
with open(r'./credentials.json') as f:
    creds = json.load(f)
    
DBNAME = creds['DBNAME']
HOST = creds['HOST']
PORT = creds['PORT']
USER = creds['USER']
PASSWORD = creds['PASSWORD']

class RedShiftHelper:
    
    def __init__(self):
        self.con = psycopg2.connect(dbname= DBNAME, host= HOST, port= PORT, user= USER, password= PASSWORD)
        
    def redshift_quit(self):
        self.con.close()
        
    def getAll(self):
        cur = self.con.cursor()
        cur.execute("SELECT * FROM linkedin_scraped_data;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_all_unscraped_linkedin_urls(self):
        cur = self.con.cursor()
        cur.execute("SELECT * FROM linkedin_unscraped_urls;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def delete_all_unscraped_linked_urls(self):
        cur = self.con.cursor()
        cur.execute("TRUNCATE linkedin_unscraped_urls;")
        cur.close()
    
    def get_unscraped_linkedin_urls(self, limit: int):
        cur = self.con.cursor()
        cur.execute("SELECT * FROM linkedin_unscraped_urls limit {};".format(limit))
        data = np.array(cur.fetchall())
        cur.close()
        return data
    
    def get_scraped_linkedin_urls(self):
        cur = self.con.cursor()
        cur.execute("SELECT Url FROM linkedin_scraped_data;")
        data = np.array(cur.fetchall())
        cur.close()
        return data
        
    def mapper_unscraped_linkedin_urls(self, jd_set: dict):
        query_list = []
        query_header = 'INSERT INTO linkedin_unscraped_urls ( jd_key, url ) VALUES'
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
        
    def delete_unscraped_urls(self, url_list: list):
        compile_query = []
        for url in url_list:
            compile_query.append("'{}'".format(url))
        delete_query = 'DELETE FROM linkedin_unscraped_urls WHERE Url in ({})'.format(",".join(compile_query))
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()    
        
    def delete_known_job_titles(self):
        delete_query = 'DELETE FROM known_job_titles'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()     
        
    def mapper_known_job_titles(self, job_title_list: list):
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
        delete_query = 'DELETE FROM known_skills'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def mapper_known_skills(self, known_skills_list: list):
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
        delete_query = 'DELETE FROM ml_job_skill_score'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def mapper_ml_job_skill_score(self, ml_job_skill_score: list):
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
        delete_query = 'DELETE FROM ml_job_score'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def mapper_ml_job_score(self, ml_job_score: list):
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
        delete_query = 'DELETE FROM ml_job_info'
        cur = self.con.cursor()
        cur.execute(delete_query)
        self.con.commit()
        cur.close()
        
    def mapper_ml_job_info(self, ml_job_info: list):
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