import mysql.connector
import json
import re
from datetime import datetime, timedelta
import calendar

def predicate_equals(r, mail):
   field_index = {"From":1, "To":2, "Subject":3, "Date":4}
   if mail[field_index[r['field']]].lower()==r['value'].lower():
       return 1
   return 0

def predicate_contains(r, mail):
    field_index = {"From":1, "To":2, "Subject":3, "Date":4}    
    x = re.findall(r['value'].lower(), mail[field_index[r['field']]].lower())
    if x:
        return 1       
    return 0

def predicate_less_than(r, mail):
    field_index = {"From":1, "To":2, "Subject":3, "Date":4}
    months = dict((month, index) for index, month in enumerate(calendar.month_abbr) if month)
    
    d = mail[4].split()
    try:
        mail_date = datetime(int(d[3]), months[d[2]], int(d[1]))
    except:
        mail_date = datetime(int(d[2]), months[d[1]], int(d[0]))
    today_date = datetime.now()
    diff = today_date - mail_date
    if diff < timedelta(days=int(r['value'])):
        return 1
    return 0

db = mysql.connector.connect(host="localhost",user="root",password="Krishna3101",database="emailDB",auth_plugin='mysql_native_password',charset='utf8mb4')
cur = db.cursor()

with open("rules.json","r") as f:
    rules = json.load(f)

cur.execute("SELECT * FROM mails")
result = cur.fetchall()

for mail in result:
    l=[]
    for r in rules['rules']:
        if r['predicate']=="equals":
            l.append(predicate_equals(r, mail))
        if r['predicate']=="contains":
            l.append(predicate_contains(r, mail))
        if r['predicate']=="less_than":
            l.append(predicate_less_than(r, mail))
    if l.count(1)==len(l) and rules['predicate']=='all':
        print(mail)
    elif l.count(1)>=1 and rules['predicate']=='any':
        print(mail)
