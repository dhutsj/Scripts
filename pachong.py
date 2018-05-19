#coding=utf-8
import urllib2
import re
from bs4 import BeautifulSoup

request = urllib2.Request("http://www.baidu.com")
response =urllib2.urlopen(request)
html=response.read()
reg=re.compile(r'<a href="(.+?\.com)"')
soup = BeautifulSoup(html,"html.parser")
for k in soup.find_all("a"):
    print(k)
#print re.findall(reg, html)
urllists = []
for urllist in re.findall(reg, html):
    urllists.append(urllist)

print urllists[1]


