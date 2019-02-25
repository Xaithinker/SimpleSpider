#/usr/bin env python 
# -*- coding:utf-8 -*-

import uuid
from queue import Queue
import threading
import requests
import time
from random import randrange, random
from lxml import etree
from itertools import repeat, starmap

from log import logger                          # class. logger is with module.__name__,so init in module
logger = logger()                               # init .py logger
from config import USER_AGENT, QUEUE_MAXSIZE                   # str， int
from db import process_sql, closed, connect  #connection # function, function,class ,Instance of connect() 
import parseRent

try:
    import pymysql
    from pymysql import ProgrammingError, OperationalError, Warning
except ImportError as e:
    logger.error(r"ImportError! Check 'pymysql' installed or not: {0}".format(e))


# global Queue to store the url of '/zufang/'
URL_QUEUE = Queue(QUEUE_MAXSIZE)
CONDITION = threading.Condition()

# flag for stop the thread!!!
T_FLAG = True  # When urlinCity()'s generator raise StopIneration, the thread stops ans set T_FLAG to false, then ParseCity() thread should stop at once.
# ------------------------------------------------------------------------------------------
connection = connect() # one thread one connection

class ParseCity_MultiThreading(threading.Thread):
    '''
    基于队列的多线程处理
    '''
    def __init__(self):
        threading.Thread.__init__(self) # thread control

        self.headers = {'User-Agent': USER_AGENT[randrange(0, len(USER_AGENT))]}
        self.session = requests.Session()

    def run(self):
        #parse_city
        while True:
            # thread control
            if T_FLAG:
                CONDITION.acquire()
            if URL_QUEUE.empty() and T_FLAG: #当put线程结束时，队列仍有元素，仍继续运行至空
                logger.warning("URL_QUEUE is empty. Waiting for urlinCity() put urls...")
                CONDITION.wait()
                logger.info("UrlinCity() has input some urls...")
            if URL_QUEUE.empty() and not T_FLAG: #当put线程结束且队列空时，才退出
                logger.info("Thread ParseCity() exits sucessfully !!!")
                break
            url = URL_QUEUE.get() # eg. 'https://sh.lianjia.com/zufang/'
            # thread control 
            if T_FLAG: # 有线程的情况下才通知
                CONDITION.notify()
                CONDITION.release() # when get url form queue then release the pubilc resourse

            # main func
            with self.session.get(url, headers=self.headers, hooks=dict(response=ParseCity_MultiThreading.parse_Area)) as s:
                #Insert url into queue from parseRent for iter every city's less than 100 rent house
                parseRent.CITY_URL_QUEUE.put(url)
                
                if s.status_code is 200:
                    logger.info("Finishing Got url form Queue. Successful to parse the Area Information...")

    @staticmethod
    def parse_Area(r, *args, **kwargs):
        '''
        1. parse the Area information and Insert into the Area
        '''
        root_node = etree.HTML(r.content.decode(r.encoding))
        area_name_generator = root_node.xpath(r'//ul[@data-target="area"]/li[position()!=1]/a')
        # ------------------Table Area : area_name ------------------------------
        li_area_mame = [x.text for x in area_name_generator ] # area name in list
        # ------------------Table Area : city_id -------------------------------------

        # sql to fetch the URL and city_id(len == 1)
        #connection1 = connect()
        with connection.cursor() as cursor:
            cursor.execute("SELECT city_id FROM city WHERE url = '{0}'".format(r.url[:-7]))
            id_tuple = cursor.fetchone()

        city_id = [x for x in id_tuple] # len == 1, list[int]
        li_city_id = [x for y in starmap(repeat, list(zip(city_id, [len(li_area_mame)]))) for x in y] # repeat list[int, int, int...]
        # ------------------Table Area : area_id -------------------------------------
        li_area_id = []
        for _ in range(len(li_area_mame)):
            li_area_id.append(uuid.uuid1().time_low)
        #li_area_id = range(30000, 30000+len(li_area_mame))
        # --------INSERT INT AREA-----------------------------------------------------
        logger.info("Starting INSERT INTO Area...of City_id {0}".format(city_id))
        param_area_generator = (x for x in zip(li_area_id, li_area_mame, li_city_id))
        while True:
            try:
                param = next(param_area_generator) # eg. (area_id, area_name, city_id)
                #print(param)
                process_sql(connection,  pos=1, param=param, sql='SELECT * FROM Area ORDER BY area_id DESC LIMIT 1')
            except:
                break
    
    
