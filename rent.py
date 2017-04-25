# coding: utf-8

import urllib2
from datetime import datetime
from bs4 import BeautifulSoup
import re
import time
import pdb
import pandas as pd
import sys
import socket, ssl

from os.path import isfile

import sqlite3 

def date_parser(s):
    return pd.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

    

def create_items_table(conn):
    print "item table not exist, will create one"
    conn.execute(
        "create table items("
        "id integer primary key autoincrement, "
        "time timestamp, "
        "title text, "
        "link text unique, "
        "author text, "
        "author_link text, "
        "status text default 'unread', "
        "city text)")
    conn.execute("create index idx on items (time)")
    conn.commit()

def create_links_table(conn):
    print "link table not exist, will create one"
    conn.execute(
        "create table links("
        "link text unique)")
    conn.commit()


class RentCrowl():
    def __init__(self, db_file, link_file, delay_sec=1.0):
        """ 
        df_file: file store data
        link_file: file store accessed link
        delay_sec: seconds to delay between two html access
        """

        self.valide_days = 20
        self.items_page = 25
        # self.df_file = df_file 
        self.db_file = db_file
        self.link_file = link_file

        # self.headers = ['title', 'link', 'author', 'author_link']
        self.delay_sec = delay_sec;

        self.db_constructor = {"items": create_items_table, 
                "links": create_links_table}

        self._read_db()
        self._last_open_time = None

    def crawl_items(self, urlbase_list, n_page=10, batch_size=40):

        """
        n_page: page number for each douban group
        batch_size: scanning post number between two updating 
        """
        for k, v in urlbase_list.items():
            self.city = k
            print "scan for {}..".format(k)
            allthings = self._scan_list(v, n_page)
            self._scan_items(allthings, batch_size)

    def _read_db(self):
        """
        read data from sqlite db, if not exist create one
        """
        conn = sqlite3.connect(self.db_file)
        cur = conn.execute("select name from sqlite_master where type='table'")
        tables = [x[0] for x in cur.fetchall()]
        for tbl in ["items", "links"]:
            if tbl not in tables:
                self.db_constructor[tbl](conn)

        print "load data successfully"

        conn.row_factory = sqlite3.Row
        self.conn = conn


    def _scan_list(self, urlbase_list, n_page=10):

        allthings = []
        cnt = 0
        n_total_pages = len(urlbase_list) * n_page
        print "scanning item list........"

        
        start_time = datetime.now()
        for urlbase in urlbase_list:
            print "scan urlbase {}..".format(urlbase)
            for i in xrange(n_page):
                plain_text = self._open_url(urlbase+str(i*self.items_page), "in reading item list: ")
                cnt += 1
                if plain_text is not None: 
                    print 'o',
                else:
                    print 'x',

                sys.stdout.flush()
                if cnt % 10 == 0:
                    print ""
                    print "scan {}/{} pages ".format(cnt, n_total_pages),
                    end_time = datetime.now()
                    start_time, timediff = end_time, (end_time-start_time).total_seconds()
                    print end_time
                    print "averaged_time: {}s".format(timediff/10)

                if plain_text is not None:
                    soup = BeautifulSoup(plain_text, 'lxml')
                    try:
                        list_table = soup.find_all(name='table', class_='olt')[0].find_all(name='tr')[2:]      
                    except:
                        break
                    for x in list_table:
                        infos = self._extract_info(x)
                        if (datetime.now()-infos['re_time']).days < self.valide_days:
                            allthings.append(infos)
        
        print '' 
        print 'finish, scanning {} pages '.format(n_total_pages),
        print datetime.now()
        return allthings

    def _check_link(self, link):
        cur = self.conn.cursor()
        cur.execute("select link from links where link = (?)", [link])
        if len(cur.fetchall()) == 0:
            return True
        else:
            return False

    def _link_accessed(self, link):
        cur = self.conn.cursor()
        try:
            cur.execute("insert  or ignore into links (link) values (?)", [link])
        except:
            pass
        self.conn.commit()

    def _insert_items(self, x):
        cur = self.conn.cursor()
        cur.executemany("insert or ignore into items (time, title, link, author, author_link, city) values "
                "(:time, :title, :link, :author, :author_link, :city)", x)
        self.conn.commit()

        item_num = cur.execute("select count(*) from items").fetchone()[0]
        print "{} items in the database".format(item_num)


    def _scan_items(self, allthings, batch_size):

        print 'start scanning time info....'
        unfetched_list = [x for x in allthings if self._check_link(x['link'])]
        print '{}/{}'.format(len(unfetched_list), len(allthings))

        len_list = len(unfetched_list)
        print "total {} new items".format(len_list)
        cnt = 0
        start_time = datetime.now()

        small_batch = []
        for x in unfetched_list:
            x['time'] = self._get_timestamp(x['link'])
            if x['time'] is not False and x['time'] is not None:
                print 'o',
            else:
                print 'x',

            sys.stdout.flush()
            cnt += 1

            # if open url failed
            if x['time'] is not False:
                self._link_accessed(x['link'])
                # if no time information
                if x['time'] is not None and (datetime.now()-x['time']).days < self.valide_days: 
                    x['city'] = self.city;
                    small_batch.append(x)

            if cnt % batch_size == 0:
                print ""
                print "scan {}/{} result ".format(cnt, len_list),
                end_time = datetime.now()
                start_time, timediff = end_time, (end_time-start_time).total_seconds()
                print end_time
                print "averaged_time: {}s".format(timediff/batch_size)

                self._insert_items(small_batch)
                small_batch = []


        if len(small_batch) != 0:
            self._insert_items(small_batch)


        print 'finish get time information ',
        print datetime.now()

    def _open_url(self, url, text=""):
        """
        try 10 times 
        if fail, return none
        """
        url_text = None
        err_cnt = 0
        while True:
            try:
                if self._last_open_time is not None:
                    timediff = (datetime.now() - self._last_open_time).total_seconds()
                    if self.delay_sec > timediff:
                        time.sleep(self.delay_sec - timediff)

                self._last_open_time = datetime.now()
                f = urllib2.urlopen(url, timeout=15)
                url_text = str(f.read())
                return url_text
            except (urllib2.HTTPError, urllib2.URLError, socket.timeout, ssl.SSLError) as e:
                print text
                print "{}: try to open {}".format(err_cnt+1, url), e
                if err_cnt < 10:
                    err_cnt += 1
                    print "sleep for 5 second and try again"
                    time.sleep(5)
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


    def _get_timestamp(self, href):
        """ 
        get time stamp
        
        return False if open url failed
        return None if no timestamp information
        """
        
        text = self._open_url(href, text="in getting timestamp: ")
        if text is None:
            return False 
        
        soup = BeautifulSoup(text, 'lxml')
        time_info = soup.find(name='div', class_='topic-doc')
        try:
            ts = datetime.strptime(time_info.find(name='span', class_='color-green').string, '%Y-%m-%d %H:%M:%S')
            return ts
        except:
            return None 

    def _extract_info(self, x):
        """ 将一个帖子标题等内容解析并存储 """
        info = dict()
        title_info = x.find(name='td', class_='title')
        info['title'] = title_info.a['title'].replace('\n', ' ').replace('\r', ' ')
        info['link'] = title_info.a['href']
        author_info = x.find_all(name='td', nowrap='nowrap')[0]
        info['author_link'] = author_info.a['href']        
        info['author'] = author_info.getText()
        time_text = x.find(name='td', class_='time').getText()
        try:
            info['re_time'] = datetime.strptime(time_text, '%m-%d %H:%M')
            info['re_time'] = info['re_time'].replace(year=datetime.now().year)
        except:
            info['re_time'] = datetime.strptime(time_text, '%Y-%m-%d')


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

    beijing_list = ['https://www.douban.com/group/252218/discussion?start=',
            'https://www.douban.com/group/257523/discussion?start=', 
            'https://www.douban.com/group/26926/discussion?start=',
            'https://www.douban.com/group/276176/discussion?start=',
            'https://www.douban.com/group/279962/discussion?start=', 
            'https://www.douban.com/group/sweethome/discussion?start=',
            'https://www.douban.com/group/beijingzufang/discussion?start=',
            'https://www.douban.com/group/opking/discussion?start=',
            'https://www.douban.com/group/xiaotanzi/discussion?start=',
            'https://www.douban.com/group/zhufang/discussion?start='] 

    shenzhen_list = ['https://www.douban.com/group/106955/discussion?start=', 
            'https://www.douban.com/group/nanshanzufang/discussion?start=',
            'https://www.douban.com/group/szsh/discussion?start=',
            'https://www.douban.com/group/futianzufang/discussion?start=',
            'https://www.douban.com/group/551176/discussion?start=',
            'https://www.douban.com/group/luohuzufang/discussion?start=',
            'https://www.douban.com/group/longhuazufang/discussion?start=',
            'https://www.douban.com/group/498004/discussion?start=',
            'https://www.douban.com/group/longgangzufang/discussion?start=', 
            'https://www.douban.com/group/SZhouse/discussion?start=',
            'https://www.douban.com/group/559626/discussion?start=', 
            'https://www.douban.com/group/528184/discussion?start=', 
            'https://www.douban.com/group/592828/discussion?start=',
            'https://www.douban.com/group/huanzhongxian/discussion?start=', 
            'https://www.douban.com/group/591624/discussion?start=', 
            'https://www.douban.com/group/luobao1haoxian/discussion?start=']

    urlbase_list = {'beijing': beijing_list, 'shenzhen': shenzhen_list}

    n_page = 10
    batch_size = 10

    rc = RentCrowl(data_file, link_file, delay_sec=4) 
    while(1):
        rc.crawl_items(urlbase_list, n_page, batch_size)



