#/usr/bin env python 
# -*- coding:utf-8 -*-

import time

from log import logger                          # class. logger is with module.__name__,so init in module
logger = logger()                               # init .py logger

from urlhandler import UrlHandler
from urlInCity import ParseUrl
from parseCity import ParseCity_MultiThreading
from parseRent import ParseRent, Proudcer

class Spider():

    table_pro_city_init = UrlHandler().web_init
    thread_1_put_zufangUrl = ParseUrl()
    thread_2_put_AreaUrl_info = ParseCity_MultiThreading()
    parse_rent_info_consumer = ParseRent()
    parse_rent_info_producer = Proudcer()

    def __init__(self):
        self.task_1 = Spider.table_pro_city_init

        self.task_2_1 = Spider.thread_1_put_zufangUrl
        self.task_2_2 = Spider.thread_2_put_AreaUrl_info

        self.task_3_2 = Spider.parse_rent_info_consumer
        self.task_3_1 = Spider.parse_rent_info_producer

    def task1(self):
        '''
        Initialization. Parse tables of Province, City and insert the information into the DB.
        '''
        logger.info("Task1 Running...")
        self.task_1() 
        logger.info("Task1 Finish!")

    def task2(self):
        '''
        1. Using City_url from DB to get the url containing a string of 'zufang', then put into the global Queue.
        2. Geting url form Queue and Parse the Area information into the DB.
        '''
        logger.info("Task2 Running...")
        self.task_2_1.start() # Thread1: Stop when raise StopInteration form generator
        self.task_2_2.start() # Thread2: Stop when T_FLAG changes by Thread1
        self.task_2_2.join()
        self.task_2_1.join()
        logger.info("Task2 Finish!")
    
    def task3(self):
        '''
        1. City_Url(in parseRent.py when it ends then) Put into Queue for iter every rent information of City
        2. Insert the Rent information into the DB.
        '''
        logger.info("Task3 Running...")
        self.task_3_1.start() # Thread1: Stop when CITY_URL_QUEUE is empty
        self.task_3_2.start() # Thread2: Stop when T_FLAG changes by Thread1
        self.task_3_2.join()
        self.task_3_1.join()
        logger.info("Task3 Finish!")
    
    def spider(self):
        logger.info('-'*60)
        #self.task1()
        logger.info('-'*60)
        #self.task2()
        logger.info('-'*60)
        self.task3()

# from parseRent import CITY_URL_QUEUE
# from db import process_sql, connect
# connection = connect()
# with connection.cursor() as cursor:
#     cursor.execute("SELECT url FROM city")
#     url_tuple = cursor.fetchall()

# url_li = [x for y in url_tuple for x in y] # len == 1, list[int]
# for url in url_li:
#     #print(url)
#     CITY_URL_QUEUE.put(url+'/zufang/')

if __name__ == "__main__":
    myspider = Spider()
    start_time = time.time()
    myspider.spider()
    end_time = time.time()
    logger.info("Running {} seconds".format(start_time - end_time))