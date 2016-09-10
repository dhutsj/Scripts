#! /usr/bin/python

import MySQLdb

db = MySQLdb.connect("localhost","root","linux","mysql")
cursor = db.cursor()
sql = """select * from test"""

try:
  cursor.execute(sql)
  data = cursor.fetchone()
  print data
  db.commit()
except:
  db.rollback()

db.close()
