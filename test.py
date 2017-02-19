# coding: utf-8
from rent import DataDisplayer, RentCrowl


zjlist_file = 'test_zhongjie_list'
data_file = 'test_rent.csv'
link_file = 'test_links'
md_file = 'test_data.md'
display_days = 5


in_set = set([u'13号', u'回龙观', u'上地', u'西二旗', u'五道口', u'龙泽', u'知春路', u'大钟寺', u'北京体育大学',
             u'北体', u'百度', u'中国地质大学', u'当代城市家园', u'上地西里', u'宣海', u'霍营', u'美和园', u'立水桥'
             , u'上地东里'])
out_set = set([u'限女', u'求租', u'芍药居'])

dp = DataDisplayer(data_file, md_file, display_days)
dp.set_in_set(in_set)
dp.set_out_set(out_set)
dp.set_zjlist(zjlist_file)

n_page = 1
batch_size = 5

urlbase_list = ['https://www.douban.com/group/beijingzufang/discussion?start=',
           'https://www.douban.com/group/26926/discussion?start=']

dp.display(clear=True)

rc = RentCrowl(data_file, link_file, dp, delay_sec=3) 
cnt = 0
while cnt < 3:
    cnt += 1 
    rc.crawl_items(urlbase_list, n_page, batch_size)




