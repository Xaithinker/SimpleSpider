#/usr/bin env python 
# -*- coding:utf-8 -*-

import time
import uuid
from queue import Queue
import threading
import requests
from lxml import etree
from random import randrange
from requests.exceptions import RequestException, RequestsWarning

from log import logger                          # class. logger is with module.__name__,so init in module
logger = logger()                               # init .py logger
from config import USER_AGENT, QUEUE_SIZE                   # str， int
from db import process_sql, closed, connect #connection  # function, function, class, Instance of connect() 

try:
    import pymysql
    from pymysql import ProgrammingError, OperationalError, Warning
except ImportError as e:
    logger.error(r"ImportError! Check 'pymysql' installed or not: {0}".format(e))

# ------------------------------------------------------------------------------------------
# global Queue to store the url of next page of Tenting Information
NEXT_URL_QUEUE = Queue(QUEUE_SIZE)
RENT_CONDITION = threading.Condition()

# Queue for store Every City_url in when the end of parseCity.py
CITY_URL_QUEUE = Queue()

# flag for stop the thread!!!
T_FLAG = True  # When Proudcer()'s 'for' stops, the thread stops ans set T_FLAG to false, then ParseRent() thread should stop at once.

connection = connect()
# ----------------------------------------------------------------------------------
class Proudcer(threading.Thread):
    '''
    URLs for RentHouse INPUT TO QUQUE
    '''
    def __init__(self): # eg 'https://sh.lianjia.com/zufang/'
        threading.Thread.__init__(self) # thread control

        self.headers = {'User-Agent': USER_AGENT[randrange(0, len(USER_AGENT))]}
        self.session = requests.Session()

    def run(self):
        while not CITY_URL_QUEUE.empty(): # Ends when City_url is all parsed!
            city_url = CITY_URL_QUEUE.get()
            #time.sleep(10)
            #logger.info('-'*60+'\nWaiting for ten seconds')
            
            for i in range(1, 100): 
                 # thread Control
                RENT_CONDITION.acquire() # 获得锁
                if NEXT_URL_QUEUE.full():
                    logger.warning("NEXT_URL_QUEUE is full.Blocked and Waiting for parseCity()...")
                    RENT_CONDITION.wait()

                #for i in range(1, 100): # the max page of rent_house
                url = city_url + 'pg{0}/#contentList'.format(i)
                NEXT_URL_QUEUE.put(url)

                #thread Control
                RENT_CONDITION.notify()
                RENT_CONDITION.release() # put then notify the parse func

                # 检验是否和以前重复内容!!若有重复内容，则判断之后均是重复的，可直接break,
                # 若非重复内容退出。则在探测下一个url时会抛出异常，此时应退出
                try:
                    with self.session.get(city_url+'pg{0}/#contentList'.format(i), headers=self.headers) as s1:
                        root_node_1 = etree.HTML(s1.content.decode(s1.encoding))
                    with self.session.get(city_url+'pg{0}/#contentList'.format(i+1), headers=self.headers) as s2:
                        root_node_2 = etree.HTML(s2.content.decode(s2.encoding))
                    
                    processding = r"//div[@class='content__list']/div[1]"
                    rent_title_generator_1 = root_node_1.xpath(processding + r"//p[@class='content__list--item--title twoline']/a/text()")
                    rent_title_generator_2 = root_node_2.xpath(processding + r"//p[@class='content__list--item--title twoline']/a/text()")
                    li_title1 = [s.strip() for s in rent_title_generator_1]
                    li_title2 = [s.strip() for s in rent_title_generator_2]
                    if li_title1 == li_title2: # end there 1
                        #print(i) check how many items in city
                        logger.warning("There is duplicate content, so drop url behind it.")
                        break # 检验重复内容，则立即退出 for cluse!
                except: # end there 2
                    logger.error("Maybe get the end of the Url of Area!")
                    break
                logger.info("{0} has put into the Queue!".format(url))

        if CITY_URL_QUEUE.empty():
            global T_FLAG
            T_FLAG = False


class ParseRent(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self) # thread control

        self.headers = {'User-Agent': USER_AGENT[randrange(0, len(USER_AGENT))]}
        self.session = requests.Session()

    def run(self):
        '''
        2. parse the Rent information and Insert into the Rent
        rent_id, title, area, sub_area, size, room, brand, price, time, tag, area_id
        '''
        while True:
            # thread control
            if T_FLAG:
                RENT_CONDITION.acquire()
            if NEXT_URL_QUEUE.empty() and T_FLAG:
                logger.warning("NEXT_URL_QUEUE is empty. Waiting for urlinCity() put urls...")
                RENT_CONDITION.wait()
                logger.info("UrlinCity() has input some urls...")
            
            if NEXT_URL_QUEUE.empty() and not T_FLAG:
                logger.info("ParseRent() Thread exits sucessfully !!!")
                break
            url = NEXT_URL_QUEUE.get() # eg. 'https://sh.lianjia.com/zufang/pg1/#contentList'
            logger.info("Get url:{0} from queue".format(url))
            # thread control 
            if T_FLAG:    
                RENT_CONDITION.notify()
                RENT_CONDITION.release() # when get url form queue then release the pubilc resourse

            # main func ---------------------------------------------------------------------------
            try:
                #r = requests.get(url, headers=self.headers, hooks=dict(response=ParseRent.parse_now_page))
                with self.session.get(url, headers=self.headers, hooks=dict(response=ParseRent.parse_now_page)) as s:
                # parse_Rent() DONE!
                    if s.status_code is 200:
                        logger.info("Successful to parse the Area Information...")
                
            except RequestException as e:
                logger.error("Caught RequestException... \n{0}".format(e))
                continue
            except RequestsWarning as w:
                logger.warning("Caught, RequestsWarning...\n{0}".format(w))
                continue
            except Exception as ex:
                logger.error("While process GET something errors caught...\n{0}".format(ex))
                continue

    @staticmethod
    def parse_now_page(r, *args, **kwargs):
            root_node = etree.HTML(r.content.decode(r.encoding))
            #鉴于解析过程的丢失信息，数据不能单独匹配在一起，故以'单元'为操作对象，该单元包含单一租房的所有信息

            #------------as a single ELEMENT------------------
            for i in range(1, 10000): # just represent a Large Num.
                processding = r"//div[@class='content__list']/div[{0}]".format(i)
                #---------------------title---------------------------------------------------------
                rent_title_generator = root_node.xpath(processding + r"//p[@class='content__list--item--title twoline']/a/text()") 
                if len(rent_title_generator) is 0: # stop when counting 30 probably
                    logger.info("OnePage has parsed successfully !!!")
                    break
                li_title = [s.strip() for s in rent_title_generator] # li[str,]
                #-----------------area-----sub_area--------------------------------------------------
                rent_area_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/a[1]/text()")
                rent_sub_area_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/a[2]/text()")
                li_area = [s.strip() for s in rent_area_generator] # li[str,]
                if len(li_area) is 0:
                    li_area.append('NULL')
                
                li_sub_area = [s.strip() for s in rent_sub_area_generator] # li[str,]
                if len(li_sub_area) is 0:
                    li_sub_area.append('NULL')
                # ----------------size----room----------------------------------------------------
                if li_area[0] is not 'NULL':
                    rent_size_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/text()[4]")
                    rent_room_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/text()[6]")
                    if len([s.strip() for s in rent_room_generator]) is 0:
                       rent_room_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/text()[5]")
                else:
                    rent_size_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/text()[3]")
                    rent_room_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/text()[5]")
                    if len([s.strip() for s in rent_room_generator]) is 0:
                        rent_room_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/text()[4]")


                if len([s.strip() for s in rent_room_generator]) is 0:
                    rent_size_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/text()[1]")
                    rent_room_generator = root_node.xpath(processding + r"//p[@class='content__list--item--des']/text()[3]")
                li_size = [s.strip() for s in rent_size_generator] # li[str,] 48m2
                li_room = [s.strip() for s in rent_room_generator] # li[str,] 2室1厅1卫      
                #----------------------brand-----------------------------------------------------------
                rent_brand_generator = root_node.xpath(processding + r"//p[@class='content__list--item--brand oneline']/text()")
                li_brand = [s.strip() for s in rent_brand_generator] # li[str,] 链家
                if len(li_brand) is 0:
                    li_brand.append('NULL')
                #----------------------price-------------------------------------------------------------
                rent_price_generator = root_node.xpath(processding + r"//span[@class='content__list--item-price']/em/text()")
                li_price = [s.strip() for s in rent_price_generator] # li[str,] 5200
                #----------------------time---------------------------------------------------------------
                rent_time_generator = root_node.xpath(processding + r"//p[@class='content__list--item--time oneline']/text()")
                li_time = [s.strip() for s in rent_time_generator] # li[str,] 
                #----------------------tag----------------------------------------------------------------
                rent_tag_generator = root_node.xpath(processding + r"//p[@class='content__list--item--bottom oneline']/i/text()")
                li_tag_temp = [s.strip() for s in rent_tag_generator] # li[str,] ['独栋公寓', '月租', '拎包入住', '免中介费', '押一付一', '新上']
                if len(li_tag_temp) is not 0:    
                    str_tag = ' '.join(li_tag_temp) # '独栋公寓 月租 拎包入住 免中介费 押一付一 新上'
                    li_tag = [] 
                    li_tag.append(str_tag) # ['独栋公寓 月租 拎包入住 免中介费 押一付一 新上',]
                else:
                    li_tag = ['NULL']
                #---------------------rent_id--------------------------------------------------------------
                li_rent_id = [uuid.uuid1().time_low]
                # ---------------------------area_id--------------------------------------------------------
                if li_area[0] is not 'NULL':
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT area_id FROM Area WHERE area_name = '{0}'".format(li_area[0]))
                        area_id_tuple = cursor.fetchone()
                        li_area_id = [x for x in area_id_tuple] # len == 1, list[int]
                else:
                    li_area_id = [0,]

                # -------------------parameter for sql ------------------------------------------------------
                param_generator = (x for x in zip(li_rent_id, li_title, li_area, li_sub_area, li_size, li_room, li_brand, li_price, li_time, li_tag, li_area_id)) # just oen element
                #print(next(param_generator))
                # -------------Insert Into DB-----------------------------------------------------------------
                process_sql(connection,  pos=3, param=next(param_generator), sql='SELECT * FROM Rent ORDER BY Rent_id DESC LIMIT 1')

