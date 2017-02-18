
# coding: utf-8

import urllib2
from datetime import datetime
from bs4 import BeautifulSoup
import re
import time
import pdb
import pandas as pd

from os.path import isfile


def date_parser(s):
    """ 解析表示时间的字符串成datetime格式"""
    return pd.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

def get_timestamp(href):
    """ 获得帖子里的时间戳 """
    # 打开url有可能失败，因此需要处理异常
    try:
        f = urllib2.urlopen(href)  
        # 睡眠1s，
        time.sleep(5)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print 'in get timestamp: ',  e
        return False    
    
    # 读取url后的内容，转成字符串送给Beautifulsoup解析
    soup = BeautifulSoup(str(f.read()), 'lxml')
    # 找帖子抬头信息块
    time_info = soup.find(name='div', class_='topic-doc')
    try:
        # 找对应的时间，并解析
        ts = datetime.strptime(time_info.find(name='span', class_='color-green').string, '%Y-%m-%d %H:%M:%S')
        return ts
    except:
        False


def extract_info(x):
    """ 将一个帖子标题等内容解析并存储 """
    # info是个dict数据结构
    info = dict()
    # 标题
    title_info = x.find(name='td', class_='title')
    # 标题内容, 替换里面可能出现的换行以免后面出现显示格式问题
    info['title'] = title_info.a['title'].replace('\n', ' ').replace('\r', ' ')
    # 帖子的链接
    info['link'] = title_info.a['href']
    # 帖子的作者信息
    author_info = x.find_all(name='td', nowrap='nowrap')[0]
    # 作者的主页链接
    info['author_link'] = author_info.a['href']        
    # 作者的名字
    info['author'] = author_info.getText()
    
    return info



class RentCrowl():
    """ 爬虫类，爬虫的实现 """
    def __init__(self, df_file, displayer, delay_sec=1.0):

        """ 初始化函数， df_file是存储爬下来数据的文件"""
        # 豆瓣小组每页25个帖子
        self.items_page = 25
        # 存数据的文件
        self.df_file = df_file 
        # 存帖子链接的文件
        self.link_file = link_file
        # 用于显示的对象
        self.displayer = displayer
        # 列名
        self.headers = ['title', 'link', 'author', 'author_link']

        self.delay_sec = delay_sec;

    def read_df(self):
        if isfile(self.df_file):
            # 读取存储数据的csv文件， 文件的项为time,   
            self.df = pd.read_csv(self.df_file, sep=',', header = 0, index_col = 0, 
                     parse_dates = ['time'], date_parser = date_parser, quotechar = '"', encoding = 'utf-8')

            # 列顺序调整
            self.df = self.df[self.headers]

            if self.df.empty:
                self.links = set()
                self.author = set()
            else:
                self.links = set(self.df['link'])
                self.author = set(self.df['author_link'])

        else:
            self.df = None

            self.links = set()
            self.author = set()

    def read_link(self):
        if isfile(self.link_file):
            # 读取链接文件
            with open(self.link_file, 'rb+') as f: 
                contents = [l.strip() for l in f]

            self.links |= set(contents)

    def update_df(self, new_items, write = True):
        """ 更新数据文件，并存储"""
        # 有些帖子访问会失败得不到时间戳，所以这里只放得到时间戳的
        filter_items = [x for x in new_items if x['time']]
        if len(filter_items) == 0:
            return

        # 建立新的DataFrame
        df_new = pd.DataFrame(filter_items)
        df_new = df_new.set_index('time')
        df_new = df_new[self.headers]
        if self.df is None:
            df_update = df_new
        else: 
            df_update = self.df.append(df_new)

        # 按index也就是时间排序
        df_update = df_update.drop_duplicates().sort_index(ascending=False)


        if write:
            df_update.to_csv(self.df_file, sep = ',', quotechar='"', encoding='utf-8', data_format = '%Y-%m-%d %H:%M:%S')

        # 更新
        self.df = df_update
        self.author = set(self.df['author_link'])

        print "update the database, it has now {} items".format(len(df_update))

    def update_links(self):
        """ 更新链接文件 """
        with open(self.link_file, 'wb+') as f:
            f.write('\n'.join(self.links))


    def crawl_items(self, urlbase_list, n_page = 10, batch_size = 40):

        allthings = []
        cnt = 0
        err_cnt = 0
        n_total_pages = len(urlbase_list) * n_page
        print "Scanning item list........"
        for urlbase in urlbase_list:
            for i in xrange(n_page):
                # 获取某页
                try:
                    f = urllib2.urlopen(urlbase+str(i*self.items_page))
                except (urllib2.HTTPError, urllib2.URLError) as e:
                    print 'In read item list: ',  e
                    if err_cnt < 10:
                        err_cnt += 1
                        print "sleep for 10 second and try again"
                        time.sleep(10)
                    else:
                        print "> 10 failures, stop"
                        return
        
                cnt += 1
                if cnt % 10 == 0:
                    print "scan {}/{} pages".format(cnt, n_total_pages)

                time.sleep(self.delay_sec)
                
                # 读取html source text
                plain_text = str(f.read())
                # 解析
                soup = BeautifulSoup(plain_text, 'lxml')
                # 定位到帖子列表
                list_table = soup.find_all(name='table', class_='olt')[0].find_all(name='tr')[2:]      
                # 对帖子列表里的所有帖子抽取信息并加入 allthings
                allthings += [extract_info(x) for x in list_table]
        
        print 'finish, scanning {} pages'.format(n_total_pages)

        print 'start scanning time....'

        # 只进一步读取没访问过的帖子
        new_li = [x for x in allthings if x['link'] not in self.links]

        len_list = len(new_li)
        print "total {} new items".format(len_list)
        cnt = 0

        small_batch = []
        for x in new_li:
            # 进一步读取帖子内部信息，获取帖子发表的时间
            self.links.add(x['link'])

            x['time'] = get_timestamp(x['link'])

            small_batch.append(x)

            cnt += 1
            if cnt % batch_size == 0:
                # 每访问40个帖子，作一次更新，并存储对应文件
                print "scan {}/{} result".format(cnt, len_list)
                self.update_df(small_batch, write = True)
                self.update_links()
                small_batch = []

                if self.displayer:
                    # 显示
                    self.displayer.display()


        # 全部扫完后，再更新
        self.update_df(small_batch, write = True)
        self.update_links()

        if self.displayer:
            self.displayer.display()

        print 'finish get time information'



class DataDisplayer():
    """ 显示类的实现，把数据写进md格式并显示 """
    def __init__(self, csv_file, md_file, n_dp_days):
        self.csv_file = csv_file
        self.md_file = md_file
        self.in_set = set()
        self.out_set = set()
        self.zjlist = set()
        self.n_dp_days = n_dp_days

    
    def read_file(self):
        def date_parser(s):
            return pd.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

        self.df = pd.read_csv(self.csv_file, sep=',', header = 0, index_col = 0, 
                 parse_dates = ['time'], date_parser = date_parser, quotechar = '"', encoding = 'utf-8')

        if not self.df.empty:
            self.links = set(self.df['link'])
            self.author = set(self.df['author_link'])
        else:
            self.links = set()
            self.author = set()

    def set_in_set(self, in_set):
        self.in_set = in_set

    def set_out_set(self, out_set):
        self.out_set = out_set

    def set_zjlist(self, zjlist_file):
        if isfile(zjlist_file):
            with open('zhongjie_list', 'rb') as f:
                self.ban_authors = set([x.strip() for x in f])
        else:
            self.ban_authors = set()

    def filter_info(self, day_offset):
        def sel(x, in_set, out_set):
            result = False
            for l in in_set:
                if l in x:
                    result = True
                    break

            for l in out_set:
                if l in x:
                    return False
            return result

        selector = [sel(x['title'], self.in_set, self.out_set) for i, x in self.df.iterrows()]

        df_sel = self.df.iloc[selector].sort_index(ascending = False)
        df_sel = df_sel[[x['author_link'] not in self.ban_authors for i, x in df_sel.iterrows()]] 

        if not df_sel.empty:
            df_sel = df_sel[df_sel.index >= (df_sel.index[0] - pd.DateOffset(day_offset))]
        
        self.display_df = df_sel

    def display(self, clear = False):

        if not isfile(self.csv_file):
            with open(self.md_file, 'wb') as f:
                pass
            return 

        self.read_file()

        if self.df.empty:
            with open(self.md_file, 'wb') as f:
                pass
            return 

        self.filter_info(self.n_dp_days)

        link_set = set()
        with open(self.md_file, 'rb') as f:   
            pattern = re.compile(r'- \[.*?\]\((.*?)\)')
            for line in f:  
                if clear or '【new!】' not in line:
                    result = re.search(pattern, line)
                    link_set.add(result.group(1))

        with open(self.md_file, 'wb') as f:
            for index, x in self.display_df.iterrows():
                if x['link'] in link_set:
                    f.write('- [{}]({})        作者：[{}]({})   {}\n'.format(
                        x['title'].encode('utf-8'), x['link'], 
                        x['author'].encode('utf-8'), x['author_link'],  
                        index.strftime("%Y-%m-%d %H:%M:%S")))
                else:
                    f.write('- [【new!】{}]({})        作者：[{}]({})   {}\n'.format(
                        x['title'].encode('utf-8'), x['link'], x['author'].encode('utf-8'), 
                        x['author_link'],  index.strftime("%Y-%m-%d %H:%M:%S"))) 




data_file = 'rent.csv'
link_file = 'links'
md_file = 'data.md'

in_set = set([u'13号', u'回龙观', u'上地', u'西二旗', u'五道口', u'龙泽', u'知春路', u'大钟寺', u'北京体育大学',
             u'北体', u'百度', u'中国地质大学', u'当代城市家园', u'上地西里', u'宣海', u'霍营', u'美和园', u'立水桥'
             , u'上地东里'])
out_set = set([u'限女', u'求租', u'芍药居'])

zjlist_file = 'zhongjie_list'



display_days = 5


dp = DataDisplayer(data_file, md_file, display_days)
dp.set_in_set(in_set)
dp.set_out_set(out_set)
dp.set_zjlist(zjlist_file)



urlbase_list = ['https://www.douban.com/group/beijingzufang/discussion?start=',
           'https://www.douban.com/group/26926/discussion?start=',
           'https://www.douban.com/group/zhufang/discussion?start=',
           'https://www.douban.com/group/257523/discussion?start=', 
               'https://www.douban.com/group/279962/discussion?start=', 
               'https://www.douban.com/group/sweethome/discussion?start=',
               'https://www.douban.com/group/opking/discussion?start=',
               'https://www.douban.com/group/276176/discussion?start=']


n_page = 10
batch_size = 40


# clear
dp.display(clear = True)

rc = RentCrowl(data_file, dp, delay_sec=5) 
rc.read_df()
rc.read_link()

# while(1):
rc.crawl_items(urlbase_list, n_page, batch_size)
        # time.sleep(600)



