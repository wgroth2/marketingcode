#
# https://github.com/simple-salesforce/simple-salesforce/blob/master/README.rst
# 
# Code by bill.roth@secondfront.com, bill.roth@gmail.com 

from simple_salesforce import Salesforce
import requests
import sys
import mariadb

def loginsf():
   session = requests.Session()
   # manipulate the session instance (optional)
   try:
      sf = Salesforce(username='bill.roth@secondfront.com', password='FOOO', security_token='WOOBA', session=session)
   except Exception as e:
      print(e)
      sys.exit(1)
   return sf

def leads(sf:Salesforce) -> float:
   query = "Select count() from Lead where Email <> '' or Phone <> ''"
   try:
      leadcount = sf.query(query)
   except Exception as e:
      print(e)
      sys.exit(1)
   return float(leadcount['totalSize'])


def insert_into_db(sf: Salesforce,conn: mariadb, tag: str, val: float):

   # Get Cursor
   cur = conn.cursor()

   try:
      query = f"INSERT into `metrics` (`ts`, `ID`, `val`, `campaign`) VALUES (current_timestamp(), NULL, {val}, '{tag}');" 
      cur.execute(query)
   except mariadb.Error as e: 
        print(f"Error: {e}")
        conn.close()
        sys.exit(1)

   return

def logindb():
   try:
      conn = mariadb.connect(user="db_user",password="Zowie",host="host",port=3306,database="db")
   except mariadb.Error as e:
      print(f"Error connecting to MariaDB Platform: {e}")
      sys.exit(1)
   return conn

def main():
    sf = loginsf()
    conn = logindb()

    insert_into_db(sf,conn, "leads",leads(sf))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
