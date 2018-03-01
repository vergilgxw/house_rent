# coding: utf-8

import urllib2
import requests
from datetime import datetime, date
from bs4 import BeautifulSoup
import re
import time
import pdb
import pandas as pd
import sys
import socket, ssl

from os.path import isfile

import sqlite3 

SLEEP_TIME = 5 

def date_parser(s):
    return pd.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

    

def create_items_table(conn):
    conn.execute(
        "create table if not exists items("
        "id integer primary key autoincrement, "
        "time timestamp, "
        "title text, "
        "link text unique, "
        "status text default 'unread', "
        "city text)")
    conn.execute("create index if not exists idx on items (time)")
    conn.commit()

def create_param_table(conn):
    conn.execute(
        "create table if not exists sp_param("
        "name text unique, value text)")
    conn.commit()


class RentCrowl():
    def __init__(self, db_file, link_file, delay_sec=1.0):
        """ 
        df_file: file store data
        link_file: file store accessed link
        delay_sec: seconds to delay between two html access
        """

        self.db_file = db_file
        self.link_file = link_file

        self.delay_sec = delay_sec;

        self._read_db()
        self._last_open_time = None

    def crawl_items(self, urlbase_list):

        """
        n_page: page number for each douban group
        batch_size: scanning post number between two updating 
        """

        while (1):
            self._get_sp_params()

            if self.status == 0:
                time.sleep(SLEEP_TIME)
                continue 

            elif self.status == 1:
                self.db.execute("insert or replace into sp_params (name, value) values ('status', '0')")
                self.db.commit()

            if self.keywords is None:
                time.sleep(SLEEP_TIME)
                continue 

            for k, v in urlbase_list.items():
                self.city = k
                print "scan for {}..".format(k)
                self._scan_list(v)

            time.sleep(SLEEP_TIME)

    def _read_db(self):
        """
        read data from sqlite db, if not exist create one
        """
        conn = sqlite3.connect(self.db_file)
        create_items_table(conn)
        create_param_table(conn)

        print "connect to db successfully"

        conn.row_factory = sqlite3.Row
        self.db = conn

    def _get_sp_params(self):

        cur = self.db.execute("select value from sp_params where name='status'")
        status = cur.fetchone()
        if status is None:
            status = 0; 
        else:
            status = int(status[0])


        cur = self.db.execute("select value from sp_params where name='keywords'")
        kws_str = cur.fetchone()
        if kws_str is None:
            kws = None; 
        else:
            kws = kws_str[0].split()

        cur = self.db.execute("select value from sp_params where name='ndays'")
        ndays = cur.fetchone()
        if ndays is None:
            ndays = 1; 
        else:
            ndays = max(int(ndays[0]), 0)


        self.keywords = kws 
        self.ndays = ndays 
        self.status = status

    def _scan_list(self, group_list):

        keywords = self.keywords
        ndays = self.ndays
        status = self.status


        url_base = "https://www.douban.com/group/search"

        params = {'start': '0', 'cat': '1013', 'group': '279962', 'q': u'上地', 'sort': 'time'}

        cnt = 0
        start_time = datetime.now()

        for group in group_list:
            item_batch = []
            print 'scan group {}'.format(group)
            params['group'] = group
            inner_cnt = 0
            for kw in keywords:
                params['q'] = kw
                max_days = 0
                i = -1
                while (max_days <= ndays):
                    i += 1
                    params['start'] = str(50 * i)
                    r = requests.get(url_base, params=params)
                    r = self._open_url(url_base, params=params)
                    try: 
                        soup = BeautifulSoup(r.text, 'lxml')
                        list_table = soup.find_all(name='table', class_='olt')[0].find_all(name='tr')
                        small_batch = [self._extract_info(x) for x in list_table]
                        batch_times = [x['time'] for x in small_batch]
                        max_days = (date.today() - min(batch_times).date()).days
                        item_batch.extend(small_batch)
                        print 'o',
                    except: 
                        print 'x',

                    cnt += 1
                    inner_cnt += 1
                    
                    sys.stdout.flush()
            print ''
            self._insert_items(item_batch)

            print "scan {} pages ".format(cnt),
            end_time = datetime.now()
            start_time, timediff = end_time, (end_time-start_time).total_seconds()
            print end_time
            print "averaged_time: {}s".format(timediff/inner_cnt)

        return 

    def _check_link(self, link):
        cur = self.db.execute("select link from links where link = (?)", [link])
        if len(cur.fetchall()) == 0:
            return True
        else:
            return False

    def _link_accessed(self, link):
        try:
            cur = self.db.execute("insert  or ignore into links (link) values (?)", [link])
        except:
            pass
        self.db.commit()

    def _insert_items(self, x):
        self.db.executemany("insert or ignore into items (time, title, link, city) values "
                "(:time, :title, :link, :city)", x)
        self.db.commit()

        item_num = self.db.execute("select count(*) from items").fetchone()[0]
        print "{} items in the database".format(item_num)


    def _open_url(self, url, params=None):
        """
        try 10 times 
        if fail, return none
        """

        err_cnt = 0
        while True:
            try:
                if self._last_open_time is not None:
                    timediff = (datetime.now() - self._last_open_time).total_seconds()
                    if self.delay_sec > timediff:
                        time.sleep(self.delay_sec - timediff)

                self._last_open_time = datetime.now()
                r = requests.get(url, params, timeout=10)
                r.raise_for_status()
                return r
            except (requests.exceptions.RequestException, socket.timeout, ssl.SSLError) as e:
                print "{}: try to open".format(err_cnt+1), e
                if err_cnt < 10:
                    err_cnt += 1
                    print "sleep for 5 second and try again"
                    time.sleep(SLEEP_TIME)
                elif self._internet_on():
                    print "> 10 failures, skip"
                    return None
                else:
                    while True:
                        print "internet disconected, press any key when ok"
                        raw_input()
                        if self._internet_on():
                            print "internet connected, continue"
                            err_cnt = 0
                            break


    def _extract_info(self, x):
        """ 将一个帖子标题等内容解析并存储 """
        info = dict()
        title_info = x.find(name='td', class_='td-subject')
        info['title'] = title_info.a['title'].replace('\n', ' ').replace('\r', ' ')
        info['link'] = title_info.a['href']

        time_text = x.find(name='td', class_='td-time')['title']
        info['time'] = datetime.strptime(time_text, '%Y-%m-%d %H:%M:%S')
        info['city'] = self.city;

        return info


    def _internet_on(self):
        try:
            urllib2.urlopen('http://www.baidu.com', timeout=10)
            return True
        except urllib2.URLError as err: 
            return False

    
if __name__ == "__main__":

    data_file = 'data.db'
    link_file = 'links'

    beijing_list = ['279962', '35417', '252218', '257523', '26926', 
            '276176', 'sweethome', 'opking', 'xiaotanzi', 'zhufang', 
            '550436','bjfangchan']


    urlbase_list = {'beijing': beijing_list}#, 'shenzhen': shenzhen_list}

    rc = RentCrowl(data_file, link_file, delay_sec=SLEEP_TIME) 
    rc.crawl_items(urlbase_list)



