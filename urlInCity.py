#/usr/bin env python 
# -*- coding:utf-8 -*-
"""
Parse the right URL in different citys

"""
import threading
import time
import requests
from random import randrange
from lxml import etree
from requests.exceptions import RequestException, RequestsWarning

from log import logger                          # class. logger is with module.__name__,so init in module
logger = logger()                               # init .py logger
from config import USER_AGENT          # str
from db import process_sql, closed, connect #connection  # function, function, class, Instance of connect() 
from parseCity import URL_QUEUE, CONDITION                # Queue(), threading.Condition()
import parseCity

try:
    import pymysql
    from pymysql import ProgrammingError, OperationalError, Warning
except ImportError as e:
    logger.error(r"ImportError! Check 'pymysql' installed or not: {0}".format(e))

# ---------------------------------------

CITY_ENTRANCE_URL = ''
connection = connect()

# decorator
def coroutine(consumer_f):
     def inf():
         f = consumer_f()
         next(f)
         return f
     return inf

class ParseUrl(threading.Thread):
    
    def __init__(self):
        threading.Thread.__init__(self) # thread control

        self.user_agent = USER_AGENT[randrange(0, len(USER_AGENT))]

    # --------------------------MAKE generator to generate SQL -------------------------------------------------------------
    @staticmethod
    def inCitys():
        try:
            with connection.cursor() as cursor:
                cursor.execute('SELECT city_name FROM City')
                li_citys = [x for y in cursor.fetchall() for x in y] #  all city list
                '''
                The ralation between renting house of city and province should filter with SQL not Python,So Just list all citys with right renting information.
                '''
                return li_citys
        except ProgrammingError as p:
            logger.error('ProgrammingError has detected: {0}'.format(p))
        except OperationalError as o:
            logger.error('OperationalError has detected: {0}'.format(o))
        except Warning as w:
            logger.warning('Warning has detected: {0}'.format(w))
        # finally:
        #     return li_citys

    @staticmethod
    def generator_sql(li):
        for i in li:
            (yield "SELECT url FROM City WHERE city_name='{0}'".format(i)) # Using city_name get '安庆'url: ----get--->'https://aq.lianjia.com/')
        
    # ------------------------------------ MAIN FUNCTION --------------------------------------------
    def run(self):
        '''
        web_init
        The Main Function
        1. inCitys() and generator_sql() to Define the Iterator to get the sql which get the entrance url
        2. hooks = entrance_of_cityUrl() to filter the 'zufang' Url and Store into Global List "city_entrance_url"
        3. Insert the url to QUEUE execute multithreading
        '''
        logger.info("Starting web_init()...")
        headers = {'Referer': 'https://www.lianjia.com/city/','Upgrade-Insecure-Requests': '1','User-Agent': self.user_agent}
        session = requests.Session()
        sql_generator = ParseUrl.generator_sql(ParseUrl.inCitys()) # Using two method to Define the Iterator to get the sql which get the entrance url
        # step:2  parse the rent information about the url
        # 使用基于生成器的协程 ，当web_init()向全局city_entrance_url写入一个url时，发送它交给 parse_Rent_info_consumer() 解析。
        c = ParseUrl.parse_Rent_info_consumer()

        while True:
            # main func
            try:
                sql = next(sql_generator) # finally all city has iterated
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    city_url = cursor.fetchone()[0]
                # then get the 'zufang' url entrance
                logger.info("Getting the Entrance url of Citys...")
                try:
                    with session.get(city_url, headers=headers, hooks=dict(response=ParseUrl.entrance_of_cityUrl)) as s:
                        # step:1  hooks to get the global list of all 'zufang' url    
                        if s.status_code is 200:
                            #logger.info('Successful to get the entrance url of City...')
                            if CITY_ENTRANCE_URL is not '':
                                c.send(CITY_ENTRANCE_URL)
                                logger.info("Url that has been sent : {}".format(CITY_ENTRANCE_URL))
                            else: # None no need to send
                                logger.warning("Url is None, Just ignore it...")
                            
                except RequestException as e:
                    logger.error("Caught RequestException... \n{0}".format(e))
                    continue
                except RequestsWarning as w:
                    logger.warning("Caught, RequestsWarning...\n{0}".format(w))
                    continue
                except Exception as ex:
                    logger.error("While process GET something errors caught...\n{0}".format(ex))
                    continue

            except ProgrammingError as p:
                logger.error('ProgrammingError has detected: {0}'.format(p))
            except OperationalError as o:
                logger.error('OperationalError has detected: {0}'.format(o))
            except Warning as w:
                logger.warning('Warning has detected: {0}'.format(w))
            except KeyboardInterrupt as k:
                logger.error('KeyBoardInterrupt has detected {0}'.format(k))
            except: # stopIneraton And Ending the 'send' and 'receive'
                logger.info("Send and Receive has finish !")
                parseCity.T_FLAG = False  # When generator raise StopIneration, the thread stops, and ParseCity() thread should stop at once.
                break # 有sql_generator生成器的结束来控制此线程的结束
    
    # ---------------------------------hooks of web_init() ------------------------------------
    @staticmethod
    def entrance_of_cityUrl(r, *args, **kwargs):
        # 0 Get the Entrance of City with Urls stored in li_area_city
        root_node = etree.HTML(r.content.decode(r.encoding))
        inCity_url_generator = root_node.xpath(r"//div[@class='ti-hover']/../ul/li/a/@href") 
        # A list of URLs such as that. BUT What we want is something likes .../zufang/. so consider all the probability.
        # first get the list of urls, then MATCH what we want
        '''[
        'https://sh.lianjia.com/ershoufang/',
        'https://sh.fang.lianjia.com/',
        'https://sh.lianjia.com/zufang/',
        'https://us.lianjia.com',
        'https://shang.lianjia.com/sh',
        'https://sh.lianjia.com/xiaoqu/',
        'https://sh.lianjia.com/jingjiren/',
        'https://sh.lianjia.com/wenda/',
        'https://sh.lianjia.com/tool.html',
        'https://sh.lianjia.com/yezhu/]
        '''
        # all urls matching '/zufang/
        li_url = [x for x in inCity_url_generator if 'zufang' in x]  # len(li_url) = 1
        # CITY_ENTRANCE_URL is a global variable to Store the url which then send to the 'parse_Rent_info_consumer'
        global CITY_ENTRANCE_URL
        if li_url: # not None
            CITY_ENTRANCE_URL = li_url[0]  # eg ['https://sh.lianjia.com/zufang/',]
            logger.info("Successful to stored the 'zufang' url into the CITY_ENTRANCE_URL...")
        else:
            CITY_ENTRANCE_URL = '' # if none, set it to none and pass it
        
    # -----------Second main function Insert Into The QUEUE FROM 'parseCity.py'-----------------------

    @staticmethod
    @coroutine
    def parse_Rent_info_consumer():
        '''
        To parse the URL send by web_init()
        '''
        while True:
            #time.sleep(randrange(3, 4))  # slow down the speed to parse
            url = (yield) # eg 'https://sh.lianjia.com/zufang/'
            logger.info('-'*50)
            logger.info("Url that has been received : {0}".format(url))
            
             # thread Control
            CONDITION.acquire() # 获得锁
            if URL_QUEUE.full():
                logger.warning("URL_QUEUE is full.Blocked and Waiting for parseCity()...")
                CONDITION.wait()
                logger.info("parseCity() has parsed some urls...")
            URL_QUEUE.put(url)
            logger.info("Url has put into the Queue!")
            # Into The QUEUE FROM 'parseCity.py'
            #thread Control
            CONDITION.notify()
            CONDITION.release() # put then notify the parse func
