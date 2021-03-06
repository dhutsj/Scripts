# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import re
import json
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def getHTMLText(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return ""

def getStockList(lst, stockURL):
    html = getHTMLText(stockURL)
    soup = BeautifulSoup(html, 'html.parser')
    a = soup.find_all('a')
    for i in a:
        try:
            href = i.attrs['href']
            lst.append(re.findall(r"[s][hz]\d{6}", href)[0])
        except:
            continue

def getStockInfo(lst, stockURL, fpath):
    count = 0
    for stock in lst:
        url = stockURL + stock + ".html"
        html = getHTMLText(url)
        try:
            if html == "":
                continue
            infoDict = {}
            soup = BeautifulSoup(html, 'html.parser')
            stockInfo = soup.find('div', attrs={'class': 'stock-bets'})

            name = stockInfo.find_all(attrs={'class': 'bets-name'})[0]
            #print name.text
            infoDict.update({'股票名称': name.text})

            keyList = stockInfo.find_all('dt')
            valueList = stockInfo.find_all('dd')
            for i in range(len(keyList)):
                key = keyList[i].text
                #print u'%s' % key
                val = valueList[i].text
                #print val
                infoDict[key] = val

            with open(fpath, 'a') as f:
                str = json.dumps(infoDict, encoding="UTF-8", ensure_ascii=False)
                print str
                #f.write(str(infoDict) + '\n')
                f.write(str + '\n')
                count = count + 1
                print("\r当前进度: {:.8f}%".format(count * 100 / len(lst)))
        except Exception as e:
                print e
                count = count + 1
                print("\r当前进度: {:.8f}%".format(count * 100 / len(lst)))
                continue

def main():
        stock_list_url = 'http://quote.eastmoney.com/stocklist.html'
        stock_info_url = 'https://gupiao.baidu.com/stock/'
        output_file = 'C:/test/BaiduStockInfo.txt'
        slist = []
        getStockList(slist, stock_list_url)
        print slist
        getStockInfo(slist, stock_info_url, output_file)


if __name__ == '__main__':
    main()