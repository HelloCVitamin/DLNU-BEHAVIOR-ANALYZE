# -*- coding: utf-8 -*-
import re
from hashlib import md5
from urlparse import urljoin
from multiprocessing.dummy import Pool
import requests
from bs4 import BeautifulSoup
from database import connect_db, account_list
from settings import *


class Drcom(object):
    def __init__(self, username, password):

        self.username = username
        self.password = password if len(password) == 32 else md5(password).hexdigest()

        self.login_url = urljoin(ISP_URL, 'Self/LoginAction.action')
        self.random_url = urljoin(ISP_URL, 'Self/RandomCodeAction.action')
        self.login_history = urljoin(ISP_URL, '/Self/UserLoginLogAction.action')

        self.headers = {
            'Origin': 'http://210.30.1.114:8089',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:33.0) Gecko/20100101 Firefox/33.0',
            'Accept': 'image/webp,*/*;q=0.8',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Accept-Encoding': ' gzip,deflate,sdch',
            'Referer': 'http://210.30.1.114:8089/Self/LogoutAction.action',
        }

        self.s = requests.Session()

    def login(self):
        try:
            req = self.s.get(self.login_url, timeout=TIME_OUT)
            if req.status_code == 200:

                check_code = re.findall('var checkcode="(\d*)', req.text, re.S)
                self.s.get(self.random_url)
                post_data = {
                    'account': self.username,
                    'password': self.password,
                    'code': '',
                    'checkcode': check_code[0],
                    'Submit': '%E7%99%BB+%E5%BD%95',
                }

                re_status = self.s.post(self.login_url, data=post_data)
                return 0 if u'温馨提示' in re_status.text else 2
            else:
                return -2

        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            return -1

        except requests.exceptions.HTTPError:
            return -2

    def get_records(self):
        history = []
        post_data = {
            'type': 4,
            'month': 'CHECKER.TBLUSERLOGIN201611',
            'startDate': '2016-01-01',
            'endDate': '2017-12-31',
        }
        req = self.s.post(self.login_history, data=post_data)
        soup = BeautifulSoup(req.text, 'lxml')
        raw_table = soup.find('table', class_="table4")
        history_list = raw_table.find('tbody').find_all('tr')
        for each_ in history_list:
            each_info = each_.find_all('td')
            history.append({
                'uid': self.username,
                'start_time': each_info[0].text.strip(),
                'end_time': each_info[1].text.strip(),
                'usage_duration': each_info[2].text.strip(),
                'usage_flow': each_info[3].text.strip(),
                'source_ip': each_info[9].text.strip()
            })
        return history


def insert_history(uid, pwd, column_id):
    conn_l = connect_db(PG_DATABASE_LOCALHOST)
    cursor_l = conn_l.cursor()
    cursor_l.execute("SELECT * FROM USER_LOGIN_HISTORY WHERE uid = %(uid)s LIMIT 1", {'uid': uid})
    if not cursor_l.fetchone():
        this = Drcom(uid, pwd)
        status_code = this.login()
        if status_code == 0:
            history = this.get_records()
            for each_ in history:
                cursor_l.execute(
                    "INSERT INTO USER_LOGIN_HISTORY "
                    "(UID, LOGIN_TIME, LOGOUT_TIME, DURATION, USAGE_FLOW, SOURCE_IP) "
                    "VALUES (%(uid)s, %(start_time)s, %(end_time)s, %(usage_duration)s, %(usage_flow)s, %(source_ip)s)",
                    each_
                )
            if len(history) == 0:
                cursor_l.execute("DELETE FROM UID_PASSWORD WHERE UID = %(uid)s", {'uid': uid})
                print u'ID={}, 0 条记录，删除此账户'.format(uid)
            else:
                print u'ID={}, {} 条记录'.format(uid, len(history))
            conn_l.commit()
        elif status_code == 2:
            cursor_l.execute("DELETE FROM UID_PASSWORD WHERE ID = %(id)s", {'id': column_id})
            conn_l.commit()
            print u'ID={}, 密码错误删除此账户'.format(uid)
        else:
            print u'ID={}, 状态码={}'.format(uid, status_code)
    else:
        print u'ID={}, 此账户已录入数据'.format(uid)
    conn_l.close()


def insert_history_by_multiprocess(account_info):
    insert_history(*account_info)


if __name__ == '__main__':
    lists = account_list()
    print len(lists)

    pool = Pool(16)
    pool.map(insert_history_by_multiprocess, lists)
    pool.close()
    pool.join()
