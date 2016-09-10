#! /usr/bin/python

import MySQLdb

db = MySQLdb.connect("localhost","root","linux","mysql")
cursor = db.cursor()
sql = """select * from test"""

try:
  cursor.execute(sql)
  data = cursor.fetchall()
  for row in data:
      name =  row[0]
      age = row[1]
      print name,age
  db.commit()
except:
  db.rollback()

db.close()
