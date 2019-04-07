""" a rent scriper """
# coding: utf-8

from datetime import datetime, date
import time
import socket
import ssl
import sys
import sqlite3
from bs4 import BeautifulSoup
import requests


SLEEP_TIME = 1


def _create_items_table(conn):
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

def _create_param_table(conn):
    conn.execute(
        "create table if not exists sp_param("
        "name text unique, value text)")
    conn.commit()


class RentCrowl:
    #pylint: disable=too-few-public-methods
    """ Rent Class"""
    def __init__(self, db_file, link_file, delay_sec=1.0):
        """
        df_file: file store data
        link_file: file store accessed link
        delay_sec: seconds to delay between two html access
        """

        self.db_file = db_file
        self.link_file = link_file

        self.delay_sec = delay_sec

        self._read_db()
        self._last_open_time = None

        self.city = None
        self.database = None

        self.keywords = None
        self.ndays = None
        self.status = None

    def crawl_items(self, urlbase_list):

        """
        n_page: page number for each douban group
        batch_size: scanning post number between two updating
        """

        while True:
            self._get_sp_params()

            if self.status == 0:
                time.sleep(SLEEP_TIME)
                continue

            elif self.status == 1:
                self.database.execute(
                    "insert or replace into sp_params (name, value) "
                    "values ('status', '0')")
                self.database.commit()

            if self.keywords is None:
                time.sleep(SLEEP_TIME)
                continue

            for key, val in urlbase_list.items():
                self.city = key
                print(f"scan for {key}..")
                self._scan_list(val)

            time.sleep(SLEEP_TIME)

    def _read_db(self):
        """
        read data from sqlite db, if not exist create one
        """
        conn = sqlite3.connect(self.db_file)
        _create_items_table(conn)
        _create_param_table(conn)

        print("connect to db successfully")

        conn.row_factory = sqlite3.Row
        self.database = conn

    def _get_sp_params(self):

        cur = self.database.execute(
            "select value from sp_params where name='status'")
        status = cur.fetchone()
        if status is None:
            status = 0
        else:
            status = int(status[0])


        cur = self.database.execute(
            "select value from sp_params where name='keywords'")
        kws_str = cur.fetchone()
        if kws_str is None:
            kws = None
        else:
            kws = kws_str[0].split()

        cur = self.database.execute(
            "select value from sp_params where name='ndays'")
        ndays = cur.fetchone()
        if ndays is None:
            ndays = 1
        else:
            ndays = max(int(ndays[0]), 0)


        self.keywords = kws
        self.ndays = ndays
        self.status = status

    def _scan_list(self, group_list):

        keywords = self.keywords
        ndays = self.ndays

        url_base = "https://www.douban.com/group/search"

        params = {'start': '0', 'cat': '1013', 'group': '279962',
                  'q': u'上地', 'sort': 'time'}

        cnt = 0
        start_time = datetime.now()

        for group in group_list:
            item_batch = []
            print(f'scan group {group}')
            params['group'] = group
            inner_cnt = 0
            for keyword in keywords:
                params['q'] = keyword
                max_days = 0
                i = -1
                while max_days <= ndays:
                    i += 1
                    params['start'] = str(50 * i)
                    ret = requests.get(url_base, params=params)
                    ret = self._open_url(url_base, params=params)
                    try:
                        soup = BeautifulSoup(ret.text, 'lxml')
                        list_table = soup.find_all(
                            name='table', class_='olt')[0].find_all(name='tr')
                        small_batch = [self._extract_info(x) for x in list_table]
                        batch_times = [x['time'] for x in small_batch]
                        max_days = (date.today() - min(batch_times).date()).days
                        item_batch.extend(small_batch)
                        print('o', )
                    except:
                        print('x', )

                    cnt += 1
                    inner_cnt += 1

                    sys.stdout.flush()
            print('')
            self._insert_items(item_batch)

            print(f"scan {cnt} pages ")
            end_time = datetime.now()
            start_time, timediff = end_time, (end_time-start_time).total_seconds()
            print(end_time)
            print(f"averaged_time: {timediff/inner_cnt}s")

    def _check_link(self, link):
        cur = self.database.execute(
            "select link from links where link = (?)", [link])
        if not cur.fetchall():
            return True
        return False

    def _link_accessed(self, link):
        try:
            self.database.execute(
                "insert  or ignore into links (link) values (?)", [link])
        except sqlite3.DatabaseError:
            pass
        self.database.commit()

    def _insert_items(self, item):
        self.database.executemany(
            "insert or ignore into items (time, title, link, city) values "
            "(:time, :title, :link, :city)", item)
        self.database.commit()

        item_num = self.database.execute(
            "select count(*) from items").fetchone()[0]
        print(f"{item_num} items in the database")

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
                result = requests.get(url, params, timeout=10)
                result.raise_for_status()
                return result
            except (requests.exceptions.RequestException, socket.timeout,
                    ssl.SSLError) as err:
                print(f"{err_cnt+1}: try to open", err)
                if err_cnt < 10:
                    err_cnt += 1
                    print("sleep for 5 second and try again")
                    time.sleep(SLEEP_TIME)
                elif _internet_on():
                    print("> 10 failures, skip")
                    return None
                else:
                    while True:
                        print("internet disconected, press any key when ok")
                        input()
                        if _internet_on():
                            print("internet connected, continue")
                            err_cnt = 0
                            break

    def _extract_info(self, page):
        """ 将一个帖子标题等内容解析并存储 """
        info = dict()
        title_info = page.find(name='td', class_='td-subject')
        info['title'] = title_info.a['title'].replace('\n', ' ').replace('\r', ' ')
        info['link'] = title_info.a['href']

        time_text = page.find(name='td', class_='td-time')['title']
        info['time'] = datetime.strptime(time_text, '%Y-%m-%d %H:%M:%S')
        info['city'] = self.city

        return info

def _internet_on():
    remote_server = "www.baidu.com"
    try:
        host = socket.gethostbyname(remote_server)
        socket.create_connection((host, 80), 2)
        return True
    except socket.herror:
        pass
    return False


def main():
    """main function"""
    rcl = RentCrowl(DATA_FILE, LINK_FILE, delay_sec=SLEEP_TIME)
    rcl.crawl_items(URLBASE_LIST)

DATA_FILE = 'data.db'
LINK_FILE = 'links'

BEIJING_LIST = [
    '279962', '35417', '252218', '257523', '26926', '276176',
    'sweethome', 'opking', 'xiaotanzi', 'zhufang', '550436', 'bjfangchan']


URLBASE_LIST = {'beijing': BEIJING_LIST}#, 'shenzhen': shenzhen_list}

if __name__ == "__main__":
    main()
