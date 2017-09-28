# -*- coding:utf-8 -*-
import re
import rsa
import time
import json
import base64
import logging
import binascii
import requests
import urllib


class WeiBoLogin(object):
    """
    class of WeiBoLogin, to login weibo.com
    """

    def __init__(self):
        """
        constructor
        """
        self.user_name = None
        self.pass_word = None
        self.user_uniqueid = None
        self.user_nick = None

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0"})
        self.session.get("http://weibo.com/login.php")
        return

    def login(self, user_name, pass_word):
        """
        login weibo.com, return True or False
        """
        self.user_name = user_name
        self.pass_word = pass_word
        self.user_uniqueid = None
        self.user_nick = None

        # get json data
        s_user_name = self.get_username()
        json_data = self.get_json_data(su_value=s_user_name)
        if not json_data:
            return False
        s_pass_word = self.get_password(json_data["servertime"], json_data["nonce"], json_data["pubkey"])

        # make post_data
        post_data = {
            "entry": "weibo",
            "gateway": "1",
            "from": "",
            "savestate": "7",
            "userticket": "1",
            "vsnf": "1",
            "service": "miniblog",
            "encoding": "UTF-8",
            "pwencode": "rsa2",
            "sr": "1280*800",
            "prelt": "529",
            "url": "http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack",
            "rsakv": json_data["rsakv"],
            "servertime": json_data["servertime"],
            "nonce": json_data["nonce"],
            "su": s_user_name,
            "sp": s_pass_word,
            "returntype": "TEXT",
        }

        # get captcha code
        if json_data["showpin"] == 1:
            url = "http://login.sina.com.cn/cgi/pin.php?r=%d&s=0&p=%s" % (int(time.time()), json_data["pcid"])
            with open("captcha.jpeg", "wb") as file_out:
                file_out.write(self.session.get(url).content)
            code = input("请输入验证码:")
            post_data["pcid"] = json_data["pcid"]
            post_data["door"] = code

        # login weibo.com
        login_url_1 = "http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.18)&_=%d" % int(time.time())
        json_data_1 = self.session.post(login_url_1, data=post_data).json()
        print json_data_1
        if json_data_1["retcode"] == "0":
            params = {
                "callback": "sinaSSOController.callbackLoginStatus",
                "client": "ssologin.js(v1.4.18)",
                "ticket": json_data_1["ticket"],
                "ssosavestate": int(time.time()),
                "_": int(time.time()*1000),
            }
            response = self.session.get("https://passport.weibo.com/wbsso/login", params=params)
            global json_data_2
            json_data_2 = json.loads(re.search(r"\((?P<result>.*)\)", response.text).group("result"))

            if json_data_2["result"] is True:
                self.user_uniqueid = json_data_2["userinfo"]["uniqueid"]
                self.user_nick = json_data_2["userinfo"]["displayname"]
                logging.warning("WeiBoLogin succeed: %s", json_data_2)

            else:
                logging.warning("WeiBoLogin failed: %s", json_data_2)
        else:
            logging.warning("WeiBoLogin failed: %s", json_data_1)
        return True if self.user_uniqueid and self.user_nick else False

    def get_username(self):
        """
        get legal username
        """
        username_quote = urllib.pathname2url(self.user_name)
        username_base64 = base64.b64encode(username_quote.encode("utf-8"))
        return username_base64.decode("utf-8")

    def get_json_data(self, su_value):
        global json_data
        """
        get the value of "servertime", "nonce", "pubkey", "rsakv" and "showpin", etc
        """
        params = {
            "entry": "weibo",
            "callback": "sinaSSOController.preloginCallBack",
            "rsakt": "mod",
            "checkpin": "1",
            "client": "ssologin.js(v1.4.18)",
            "su": su_value,
            "_": int(time.time()*1000),
        }
        try:
            response = self.session.get("http://login.sina.com.cn/sso/prelogin.php", params=params)
            json_data = json.loads(re.search(r"\((?P<data>.*)\)", response.text).group("data"))
        except Exception as excep:
            json_data = {}
            logging.error("WeiBoLogin get_json_data error: %s", excep)

        logging.debug("WeiBoLogin get_json_data: %s", json_data)
        return json_data

    def get_password(self, servertime, nonce, pubkey):
        """
        get legal password
        """
        string = (str(servertime) + "\t" + str(nonce) + "\n" + str(self.pass_word)).encode("utf-8")
        public_key = rsa.PublicKey(int(pubkey, 16), int("10001", 16))
        password = rsa.encrypt(string, public_key)
        password = binascii.b2a_hex(password)
        return password.decode()
    def send_weibo(self):
        global json_data_2
        url = 'https://weibo.com/aj/mblog/add?_wv=5&__rnd=' + str(int(time.time()*1000))
        post_data = {
                "text": "Hi, this weibo is sent by Python",
                "pic_id": "",
                "rank": 0,
                "rankid": "",
                "_surl": "",
                "hottopicid": "",
                "location": "home",
                "module": "stissue",
                "_t": 0,
        }
        headers = {}
        headers['Referer'] = 'https://weibo.com/u/' + json_data_2["userinfo"]["uniqueid"] + '?topnav=1&wvr=5'
        #headers['Cookie'] = 'TC-Ugrow-G0=5e22903358df63c5e3fd2c757419b456; TC-V5-G0=7975b0b5ccf92b43930889e90d938495; _s_tentry=weibo.com; Apache=5607745287556.127.1506493712261; SINAGLOBAL=5607745287556.127.1506493712261; TC-Page-G0=1bbd8b9d418fd852a6ba73de929b3d0c; ULV=1506493712292:1:1:1:5607745287556.127.1506493712261:; __gads=ID=47a7ac76059fd18f:T=1506494461:S=ALNI_MYqXOo8NHd149Wio4BzEaQ9wwrQ0A; login_sid_t=0660265c5db10c1b54dc5f3b7399e05f; WBtopGlobal_register_version=1844f177002b1566; SSOLoginState=1506501772; SCF=AlTP9uOHqxKP9Y9S4fo6KCVBa_c35EJnzNwCiWe1dy_iw7rqvgMFS-GT3-2lf9YIVU6D9hdkcLE23wU4I5OPJsI.; SUB=_2A250zxTdDeRhGedJ6FUU8S7LzzyIHXVXvQEVrDV8PUNbmtBeLRDQkW9noXTI7plhlO98V44S6g1OKf93-g..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWSxcr8WO573_IaBlDKq.jd5JpX5K2hUgL.Fo2Ne0MfeK5NSh52dJLoI0qLxKqL1KzL1KzLxK-L1KeL1hnLxKqL12zL1h.LxKML1-2L1hBLxK-L12qLBoqLxK-LB-BL1K5t; SUHB=0YhgOfXC8vZ0qe; ALF=1538037770; un=jszxtsj@163.com; wvr=6; wb_cusLike_1737510740=N; UOR=,,www.baidu.com; wb_timefeed_1737510740=1'
        #headers['Referer'] = {'Referer': 'http://weibo.com/%(username)s?wvr=5&wvr=5&lf=reg' % {'username': self.user_name}}
        #response = requests.post(url, headers=headers,data=post_data)
        response = self.session.post(url, headers=headers,data=post_data)
        print response.status_code

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s\t%(levelname)s\t%(message)s")
    weibo = WeiBoLogin()
    weibo.login("jszxtsj@163.com", "DHUtsj1234")
    weibo.send_weibo()