import lxml
import pandas
import html5lib
import exrex
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import psycopg2
import re
from var import *

conn = psycopg2.connect(host=dbhost, dbname=database, user=dbuser, password=dbpassword)
cursor = conn.cursor()
engine = create_engine('postgresql://'+dbuser+'@'+dbhost+':5432/'+database)

query="CREATE TABLE IF NOT EXISTS intensive_options (id SERIAL PRIMARY KEY, description VARCHAR(100));"
print query
cursor.execute(query)
query="CREATE TABLE IF NOT EXISTS courses (id BIGSERIAL PRIMARY KEY, code VARCHAR(10) NOT NULL, description VARCHAR(100) NULL, period text[], study_type text[], intensive INTEGER REFERENCES intensive_options, exam BOOL);"
print query
cursor.execute(query)
query="CREATE TABLE IF NOT EXISTS prereq_staging (code VARCHAR(100), description text);"
print query
cursor.execute(query)
query="CREATE TABLE IF NOT EXISTS prereq (id BIGSERIAL PRIMARY KEY, course_id INTEGER REFERENCES courses, description text);"
print query
cursor.execute(query)
print "Generate URL"
urls=list(exrex.generate('https://my\.une\.edu\.au/courses/'+str(year)+'/units/atoz/[A-Z]\?export=true&page=')) #iterates through A-Z
i=0
while i < len(urls):
    empty=False
    l= 1
    while empty==False:
        url=urls[i]+str(l)
        print url
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        if len(page.content)>2000: #excludes "mostly empty pages"
            df = pandas.read_html(url)
            df[0] = df[0].iloc[1:] #drop 0th row
            df[0].columns = ['code', 'description', 'trimester', 'study', 'intensive', 'exam']
            df[0].to_sql("courses_staging", engine, if_exists = "append")
            l+=1
        else:
          empty=True
          #For testing and DDOS prevention
    i+=1
query="DELETE FROM courses_staging WHERE trimester IS NULL;"
print query
cursor.execute(query)
query="INSERT INTO intensive_options (description) SELECT DISTINCT intensive FROM courses_staging;"
print query
cursor.execute(query)
query="INSERT INTO courses (code, description, period, study_type, intensive, exam) SELECT code, courses_staging.description, regexp_split_to_array(trimester, ',\s+'), regexp_split_to_array(study, ',\s+'), intensive_options.id, CASE WHEN exam='Yes' THEN TRUE ELSE FALSE END FROM courses_staging INNER JOIN intensive_options on intensive_options.description=courses_staging.intensive"
print query
cursor.execute(query)
cursor.close()
conn.commit()

cursor = conn.cursor()
cursor.execute('SELECT code from courses')
code=cursor.fetchall()

i=0
prereq=(())
while i < len(code):
    url='https://my.une.edu.au/courses/'+str(year)+'/units/'+''.join(code[i])
    page = requests.get(url)
    content = page.content.replace('\n', ' ').replace('\r', '')
    text=re.findall(r'Pre-requisites</td> <td> (.*?) </td>',content)
    cursor.execute("INSERT INTO prereq_staging (code, description) VALUES (%s, %s)", (''.join(code[i]),text[0]))
    print  "{}, {} - {} of {}".format(''.join(code[i]),text[0],i, len(code))
    i+=1

query="INSERT INTO prereq (course_id, description) SELECT DISTINCT courses.id, CASE WHEN prereq_staging.description='None' THEN '' ELSE prereq_staging.description END FROM prereq_staging INNER JOIN courses ON courses.code=prereq_staging.code;"
print query
cursor.execute(query)

query="DROP TABLE courses_staging; DROP TABLE prereq_staging;"
print query
cursor.execute(query)

cursor.close()
conn.commit()
