# coding: utf-8
import re
import pandas as pd
import time
from os.path import isfile

import difflib
import pdb
import numpy as np


class DataDisplayer():
    """ 
    select data and write it into md file
    """
    def __init__(self, csv_file, md_file, n_dp_days):
        """
        n_dp_days: search how many days
        """
        self.csv_file  = csv_file
        self.md_file   = md_file
        self.in_set    = set()
        self.out_set   = set()
        self.zjlist    = set()
        self.n_dp_days = n_dp_days

    def display(self, clear=False, fold=True):
        """
        clear: clear "new" logo if True
        """

        if not isfile(self.csv_file):
            with open(self.md_file, 'wb') as f:
                pass
            return 

        print "read file"
        self._read_file()

        if self.df.empty:
            with open(self.md_file, 'wb') as f:
                pass
            return 
        
        new_logo_str = '【new!】'

        print "filter infomation"
        self._filter_info(self.n_dp_days)

        link_set = set()
        # search for existing link in md file
        if not isfile(self.md_file):
            print "md file not exist, will create on"
            with open(self.md_file, 'wb') as f:
                pass

        with open(self.md_file, 'rb') as f:   
            pattern = re.compile(r'- \[.*?\]\((.*?)\)')
            for line in f:  
                if clear or new_logo_str not in line:
                    result = re.search(pattern, line)
                    link_set.add(result.group(1))

        print "folding repeat posts"
        self._folding(link_set)

        # write things to md file
        print "write to file"
        with open(self.md_file, 'wb') as f:
            for index, x in self.display_df.iterrows():
                if x['link'] in link_set:
                    f.write(self._format_item_str(x, index))
                else:
                    f.write(self._format_item_str(x, index, new_logo_str))
        print "success"


    def _format_item_str(self, x, time_str, prefix=''):
        return '- [{}{}]({})        作者：[{}]({})  重复: {}  {}\n'.format(prefix, 
                        x['title'].encode('utf-8'), x['link'], x['author'].encode('utf-8'), 
                        x['author_link'],  x['fold_num'], time_str.strftime("%Y-%m-%d %H:%M:%S"))

    
    def _read_file(self):
        date_parser = lambda s: pd.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

        self.df = pd.read_csv(self.csv_file, sep=',', header=0, index_col=0, 
                 parse_dates=['time'], date_parser=date_parser, quotechar='"', encoding='utf-8')

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

    def _filter_info(self, day_offset):
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

        if not self.df.empty:
            df_sel = self.df[self.df.index >= (self.df.index[0] - pd.DateOffset(day_offset))]

        selector = [sel(x['title'], self.in_set, self.out_set) for i, x in df_sel.iterrows()]
        df_sel = df_sel.iloc[selector].sort_index(ascending = False)
        df_sel = df_sel[[x['author_link'] not in self.ban_authors for i, x in df_sel.iterrows()]] 

        
        self.display_df = df_sel

    def _folding(self, link_set):
        """
        folding the overlapped titles
        """

        fold_list = np.array(len(self.display_df) * [False])
        fold_num = np.array(len(self.display_df) * [1], dtype=int)

        for i, (index, x) in enumerate(self.display_df.iterrows()):
            if  fold_list[i]:
                continue

            for j, (idx2, y) in enumerate(self.display_df.iloc[i+1:].iterrows()):
                if x['author_link'] != y['author_link']:
                    continue
                
                s = difflib.SequenceMatcher(None, x['title'], y['title'])  
                _, _, overlapped_len = s.find_longest_match(0, len(x['title']), 0, len(y['title'])) 
                if overlapped_len/float(len(x['title'])) > 0.5:
                    fold_num[i] += 1
                    fold_list[i+j+1] = True
                    if y['link'] in link_set:
                        link_set.add(x['link'])

        self.display_df['fold_num'] = fold_num
        self.display_df = self.display_df[~fold_list]



if __name__ == '__main__':

    data_file    = 'rent.csv'
    link_file    = 'links'
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
    dp.display(clear=False)
        # time.sleep(5)


