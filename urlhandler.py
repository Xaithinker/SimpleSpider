#/usr/bin env python 
# -*- coding:utf-8 -*-

import requests
from lxml import etree
from random import randrange
from itertools import repeat, starmap

from log import logger                          # class. logger is with module.__name__,so init in module
logger = logger()                               # init .py logger
from config import BASE_URL, USER_AGENT         # str
from db import process_sql, closed, connection  # function, function, Instance of connect()

# 1: 使用python标准库处理URL

class UrlHandler(object):
    '''
    Init the table : province, city and so on
    '''
    def __init__(self):
        self.base_url = BASE_URL
        self.user_agent = USER_AGENT[randrange(0, len(USER_AGENT))]

    def web_init(self):
        '''
        1. parse the HTML
        2. Insert the Province, City to DB
        3. The next step's url is 'url' column in Table City
        '''
        logger.info("Start web_init()...")
        url = self.base_url
        headers = {
            'Host': 'www.lianjia.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': self.user_agent,
        }

        session = requests.Session()
        with session.get(url, headers=headers, hooks=dict(response=UrlHandler.parse)) as s:
            if s.status_code is 200:
                logger.info('GET successful to process the content...')
            #print(s.headers)

    @staticmethod
    def parse(r, *args, **kwargs): # r-> response
        '''
        for hooks in web_init()
        '''
        root_node = etree.HTML(r.content.decode(r.encoding))

        # 1 parse the all province in www.lianjia.com/city/
        # This is the number of city in different provinces ordered by the way of xpath.
        li_city_num = [5, 1, 1, 5, 10, 1, 4, 1, 6, 4, 10, 14, 5, 1, 9, 2, 5, 3, 1, 1, 1, 8, 7, 4, 2, 1, 3, 8] # User_Defined Not using xpath.
        
        province_generator = root_node.xpath(r'//div[@class="city_list_tit c_b"]') # a list of Province Generator
        logger.info("Success to parse Provinces!")
        li_pro_name = [x.text for x in province_generator]
        # ------------------------------------------------------------------------------------
        # 2 Get all citys and the entrance of city (:urls likes 'https://sh.lianjia.com')
        citys_generator = root_node.xpath(r'//div[@class="city_province"]/ul/li/a') 
        li_city_name = [x.text for x in citys_generator]
        logger.info("Success to parse Citys!")
        city_urls_generator = root_node.xpath(r'//div[@class="city_province"]/ul/li/a/@href')
        li_city_urls = [x for x in city_urls_generator]
        logger.info("Success to parse URLs in different Citys!")
        # ------------------------------------------------------------------------------------
        # 3 : Insert into Table of province
        '''
        pro_id, pro_name, city_num
        '''
        param_pro_generator = (x for x in zip(range(10000, 10000+len(li_pro_name)), li_pro_name, li_city_num) if len(li_pro_name)==len(li_city_num))
        # 3.1 Insert Into
        print(''' 
INSERT_INTO_ONE->list
----------------------------
|pos| SQL Without Parameter|
----------------------------
| 0 | INSERT_INTO_PROVINCE |
| 1 | INSERT_INTO_AREA     |
| 2 | INSERT_INTO_CITY     |
| 3 | INSERT_INTO_RENT     | 
----------------------------
''')
        logger.info("Starting INSERT INTO Province...")
        while True:
            try:
                param = next(param_pro_generator)
                process_sql(connection, param=param, pos=0, sql='SELECT * FROM Province ORDER BY pro_id DESC LIMIT 1' )  #  Error related with SQL  has detected within function
            except: # StopInteation
                break
        
        # --------------------------------------------------------------------------------------
        # 4 : Insert into table of city
        '''
        city_id, city_name, url, pro_id(same as pro_id in Province table)
        '''
        # Trouble 1: li_pro_id has duplicate values in Table City.
        # So how to use two lists to repeat elements in one list while the other act as the repeating number
        ''' VERSION 0.1 Using map()
        li_pro_id = range(10000, 10000+len(li_pro_name)) # li_pro_id's len is equal to li_city_num
        li_pro_idToStr_temp = list(map(str, li_pro_id))  # For Easy to process the next func
        li_pro_idToStr_temp.reverse() # reverse for OUTPUTing of the element without using Index in _f(item) function when duplicate with ((li_pro_id_temp.pop(),) * item)

        def _f(item):
            return (li_pro_idToStr_temp.pop(),) * item # index started with 0; duplicate the same pro_id in Table City.
        
        li_pro_idInCity = list(map(int, [y for x in list(map(_f,li_city_num)) for y in x]))
        # Duplicate has finished and get li_pro_inInCity then writting in pro_id In Table City.
        '''
        # Trouble 1 @ UPDATE: repeat elemnets in list function:
        '''
        VERSION 0.2 Using itertools.repeat, itertools.starmap
        '''
        li_pro_id = range(10000, 10000+len(li_pro_name)) # li_pro_id's len is equal to li_city_num
        temp = starmap(repeat, list(zip(li_pro_id, li_city_num))) # [(li_pro_id_elem, li_city_num_elem), ...]
        li_pro_idInCity = [x for y in temp for x in y] # list comprehension

        param_city_generator = (x for x in zip(range(20000, 20000+len(li_city_name)), li_city_name, li_city_urls , li_pro_idInCity) if len(li_city_name)==len(li_pro_idInCity)==len(li_city_urls))
        logger.info("Starting INSERT INTO City...")
        while True:
            try:
                param = next(param_city_generator) # eg. (20089, '上海', 'https://sh.lianjia.com/', 10020)
                process_sql(connection,  pos=2, param=param, sql='SELECT * FROM City ORDER BY city_id DESC LIMIT 1')
            except:
                break
        
        
   
## TODO: 数据库插入City，Province表的参数param已经构造完成。然后就是 插入数据库
# 然后，City里有url列，再次解析，以上海为例，进行租房信息的解析，构造参数，插入数据库
        


