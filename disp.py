# coding: utf-8
import re
import pandas as pd
import time
from os.path import isfile

import difflib
import pdb
import numpy as np
import sqlite3



class DataDisplayer():
    """ 
    select data and write it into md file
    """
    def __init__(self, db_file, md_file, n_dp_days):
        """
        n_dp_days: search how many days
        """
        self.db_file   = db_file
        self.md_file   = md_file
        self.in_set    = set()
        self.out_set   = set()
        self.zjlist    = set()
        self.n_dp_days = n_dp_days
        
        print "connect database"
        self._connect_db()
        self._init_read_links()
        self._init_show_links()



    def _init_read_links(self):

        self.conn.execute(
            "create table if not exists read_links "
            "(link text unique,"
            "read boolean default false)")
        self.conn.commit()

    def _init_show_links(self):
        self.conn.execute("create table if not exists show_links "
                "(link text unique)")


    def display(self, clear=False, fold=True):
        """
        clear: clear "new" logo if True
        """
        new_logo_str = '【new!】'

        # search for existing link in md file
        if not isfile(self.md_file):
            print "md file not exist, will create one"
            with open(self.md_file, 'wb') as f:
                pass

        if clear:
            self._save_show_links()
    
        print  "read information"
        items = self.conn.execute("select items.*, "
                "case when read_links.link is null then 0 else 1 end as read "
                "from items left join read_links "
                "on items.link = read_links.link "
                "where datetime(time) > datetime('now', ?) "
                "order by datetime(time) desc", ['-{} days'.format(int(self.n_dp_days))])

        
        print "filter information"
        items = self._filter_info(items)

        print "folding repeat posts"
        items = self._folding(items)

        self._add_show_links(items)

        # write things to md file
        print "write to file"
        with open(self.md_file, 'wb') as f:
            for  x in items:
                if x['read'] == 1:
                    f.write(self._format_item_str(x))
                else:
                    f.write(self._format_item_str(x, new_logo_str))
        print "success"


    def _format_item_str(self, x, prefix=''):
        return '- [{}{}]({})        作者：[{}]({})  重复: {}  {}\n'.format(prefix, 
                        x['title'].encode('utf-8'), x['link'], x['author'].encode('utf-8'), 
                        x['author_link'],  x['fold_num'], x['time'].strftime("%Y-%m-%d %H:%M:%S"))

    

    def _connect_db(self):
        self.conn = sqlite3.connect(self.db_file, detect_types=sqlite3.PARSE_DECLTYPES) 
        self.conn.row_factory = sqlite3.Row


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

    def _filter_info(self, items):
        """
        filter information
        """
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

        items = [x for x in items if sel(x['title'], self.in_set, self.out_set) 
                and x['author_link'] not in self.ban_authors]

        return items

    def _folding(self, items):
        """
        folding the overlapped titles
        """

        fold_list = len(items) * [False]
        fold_num = len(items) * [1]

        ret = []
        for i, x in enumerate(items):
            if  fold_list[i]:
                continue

            t = {key: x[key] for key in x.keys()}
            t['fold_num'] = 1
            ret.append(t)

            for j, y in enumerate(items[i+1:]):
                if x['author_link'] != y['author_link']:
                    continue
                
                s = difflib.SequenceMatcher(None, x['title'], y['title'])  
                _, _, overlapped_len = s.find_longest_match(0, len(x['title']), 0, len(y['title'])) 
                if overlapped_len/float(len(x['title'])) > 0.5:
                    t['fold_num'] += 1
                    fold_list[i+j+1] = True
                    if y['read'] == 1:
                        self.conn.execute("insert or ignore into read_links (link) "
                                "values (?)", x['link'])
                        t['read'] == 1

        return ret 

    def _save_show_links(self):
        self.conn.execute("insert or ignore into read_links (link) "
                "select link from show_links")

        self.conn.execute("drop table show_links")
        self.conn.execute("create table show_links "
                "(link text unique)")
        self.conn.commit()
    
    def _add_show_links(self, items):
        links = [(x['link'],) for x in items]
        self.conn.executemany("insert or ignore into show_links (link) "
                "values (?)", links)
        self.conn.commit()



if __name__ == '__main__':

    data_file    = 'data.db'
    # link_file    = 'links'
    zjlist_file  = 'zhongjie_list'
    md_file      = './data.md'
    display_days = 5

    in_set  = set([u'13号', u'回龙观', u'上地', u'西二旗', u'五道口', u'龙泽', u'知春路', u'大钟寺', u'北京体育大学',
                     u'北体', u'中国地质大学', u'当代城市家园', u'上地西里', u'宣海', u'霍营', u'美和园', u'立水桥'])

    out_set = set([u'限女', u'求租', u'芍药居'])

    dp = DataDisplayer(data_file, md_file, display_days)
    dp.set_in_set(in_set)
    dp.set_out_set(out_set)
    dp.set_zjlist(zjlist_file)

    # while True:
    dp.display(clear=True)
        # time.sleep(5)


