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

def date_parser(s):
    return pd.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

class RentCrowl():
    def __init__(self, df_file, link_file, displayer=None, delay_sec=1.0):
        """ 
        df_file: file store data
        link_file: file store accessed link
        displayer: a class to handle the format information and display
        delay_sec: seconds to delay between two html access
        """

        self.items_page = 25
        self.df_file = df_file 
        self.link_file = link_file
        self.displayer = displayer

        self.headers = ['title', 'link', 'author', 'author_link']
        self.delay_sec = delay_sec;

        self._read_df()
        self._read_link()
        self._last_open_time = None

    def crawl_items(self, urlbase_list, n_page=10, batch_size=40):

        """
        n_page: page number for each douban group
        batch_size: scanning post number between two updating 
        """
        allthings = self._scan_list(urlbase_list, n_page)
        self._scan_items(allthings, batch_size)

    def _read_df(self):
        """
        read data file, if no file, create one
        """
        print "read data file..." 
        if isfile(self.df_file):
            self.df = pd.read_csv(self.df_file, sep=',', header = 0, index_col = 0, 
                     parse_dates = ['time'], date_parser = date_parser, quotechar = '"', encoding = 'utf-8')
        
            self.df = self.df[self.headers]

            if self.df.empty:
                print "success, data is empty" 
                self.links = set()
                self.author = set()
            else:
                self.links = set(self.df['link'])
                self.author = set(self.df['author_link'])
                print "success"
        else:
            print "data file does not exist, will create one"
            self.df = None

            self.links = set()
            self.author = set()

    def _read_link(self):
        print "read link file..."
        if isfile(self.link_file):
            with open(self.link_file, 'rb+') as f: 
                contents = [l.strip() for l in f]

            self.links |= set(contents)
            print "success"
        else:
            print "link file does not exist, will create one"

    def _scan_list(self, urlbase_list, n_page=10):

        allthings = []
        cnt = 0
        n_total_pages = len(urlbase_list) * n_page
        print "scanning item list........"

        
        start_time = datetime.now()
        for urlbase in urlbase_list:
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
                    list_table = soup.find_all(name='table', class_='olt')[0].find_all(name='tr')[2:]      
                    allthings += [self._extract_info(x) for x in list_table]
        
        print '' 
        print 'finish, scanning {} pages '.format(n_total_pages),
        print datetime.now()
        return allthings

    def _scan_items(self, allthings, batch_size):

        print 'start scanning time info....'
        unfetched_list = [x for x in allthings if x['link'] not in self.links]

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
                self.links.add(x['link'])
                # if no time information
                if x['time'] is not None:
                    small_batch.append(x)

            if cnt % batch_size == 0:
                print ""
                print "scan {}/{} result ".format(cnt, len_list),
                end_time = datetime.now()
                start_time, timediff = end_time, (end_time-start_time).total_seconds()
                print end_time
                print "averaged_time: {}s".format(timediff/batch_size)

                self._update_df(small_batch, write=True)
                self._update_links()
                small_batch = []

                if self.displayer is not None:
                    self.displayer.display()

        self._update_df(small_batch, write=True)
        self._update_links()

        if self.displayer is not None:
            self.displayer.display()

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
                    # print "timediff = {}".format(timediff)
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


    def _update_df(self, new_items, write=True):
        # filter_items = [x for x in new_items if x['time']]
        if len(new_items) == 0:
            return

        df_new = pd.DataFrame(new_items)
        df_new = df_new.set_index('time')
        df_new = df_new[self.headers]
        if self.df is None:
            df_update = df_new
        else: 
            df_update = self.df.append(df_new)

        len_1 = len(df_update)
        df_update = df_update.drop_duplicates().sort_index(ascending=False)
        len_2 = len(df_update)

        print "drop {} duplicated items".format(len_1-len_2)

        if write:
            df_update.to_csv(self.df_file, sep = ',', quotechar='"', encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')

        self.df = df_update
        self.author = set(self.df['author_link'])

        print "update the database, it has now {} items".format(len(df_update))

    def _update_links(self):
        with open(self.link_file, 'wb+') as f:
            f.write('\n'.join(self.links))

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

        return info


    def _internet_on(self):
        try:
            urllib2.urlopen('http://www.baidu.com', timeout=10)
            return True
        except urllib2.URLError as err: 
            return False

if __name__ == "__main__":

    data_file = 'rent.csv'
    link_file = 'links'

    urlbase_list = ['https://www.douban.com/group/beijingzufang/discussion?start=',
               'https://www.douban.com/group/26926/discussion?start=',
               'https://www.douban.com/group/zhufang/discussion?start=',
               'https://www.douban.com/group/257523/discussion?start=', 
                   'https://www.douban.com/group/279962/discussion?start=', 
                   'https://www.douban.com/group/sweethome/discussion?start=',
                   'https://www.douban.com/group/opking/discussion?start=',
                   'https://www.douban.com/group/276176/discussion?start=']

    n_page = 5
    batch_size = 20

    rc = RentCrowl(data_file, link_file, delay_sec=4) 
    while(1):
        rc.crawl_items(urlbase_list, n_page, batch_size)



