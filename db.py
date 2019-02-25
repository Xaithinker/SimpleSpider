#/usr/bin env python
# -*- encoding:utf-8 -*-

from log import logger
logger = logger() # init .py logger

from config import (
    HOST, DBNAME, USER, PASSWORD, PORT, 
    QUERY_TABLE_PROVINCE, 
    TABLE_NAME, 
    INSERT_INTO_ONE) #  INSERT_INTO_ONE[INSERT_INTO_PROVINCE, INSERT_INTO_AREA, INSERT_INTO_CITY, INSERT_INTO_RENT]

try:
    import pymysql
    from pymysql import ProgrammingError, OperationalError, Warning
except ImportError as e:
    logger.error(r"ImportError! Check 'pymysql' installed or not: {0}".format(e))

def connect():
    try:
        cxn = pymysql.connect(host=HOST, user=USER, password=PASSWORD, port=PORT, db=DBNAME)
        logger.info('Successful connect to {0}'.format(DBNAME))
        return cxn  # 返回connect对象
    except:
        logger.error("Failed to connect {0}".format(DBNAME))

def closed(cxn):
    try:
        cxn.close()
        logger.info("Connection has closed successfully!")
    except Exception as e:
        logger.error("Some errors occered! : {0}".format(e))

def create(cxn): #cursor
    '''
    Create Table:'Province','City', 'Area', 'Rent'
    '''
    try:
        with cxn.cursor() as cursor:  # open cursor
            for query in QUERY_TABLE_PROVINCE:
                cursor.execute(query)
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        cxn.commit()
        logger.info("{}, {}, {}, {} has Created".format(*TABLE_NAME))
        closed(cxn)
    except Exception as e:
        logger.error(e)
        with cxn.cursor() as cursor: # when DROP, must obey the opposite way of REFERENCES
            cursor.execute("DROP TABLE rent") # the DROP order is strict!!
            cursor.execute("DROP TABLE area")
            cursor.execute("DROP TABLE city")
            cursor.execute("DROP TABLE province")
            logger.info("Table has DROP, Auto trying to execute again!")
            create(cxn)

def _execute_handler(cxn, op, param=None): # sql -> list[str]
    '''
    handle the input sql
    '''
    try:
        with cxn.cursor() as cursor:
            if param is not None:
                cursor.execute(op, param) # op is a  sql with parameters or not.
            else:
                cursor.execute(op) # Using when no parameters like ('', '', '')
                res = cursor.fetchall()
                logger.info("the output is : {0}".format(res[-1])) # the SQL with parameters defaultly to haveing no output.
        cxn.commit()
        logger.info("sql has executed successfully")
    except ProgrammingError as p:
        logger.error('ProgrammingError has detected: {0}'.format(p))
    except OperationalError as o:
        logger.error('OperationalError has detected: {0}'.format(o))
    except Warning as w:
        logger.warning('Warning has detected: {0}'.format(w))
    finally:
        logger.info("execute_handler() has finished!")

def insert(cxn, pos, param): # param->tuple pos->int!
    try:
        _execute_handler(cxn, INSERT_INTO_ONE[pos], param=param) # connection, str, tuple_parameter
        logger.info("Insertion Successful!")
    except IndexError as i:
        logger.error("Out of Index error: {0}".format(i))
    except Exception as e:
        logger.error("Some Exception raised: {0}".format(e))
    finally:
            logger.info("insert() function exits successfully!")


def outPutData(cxn, sql):
    '''
    output has so many different ways, so user needs to write SQL for directly executing.
    '''
    _execute_handler(cxn, sql, param=None)
    logger.info("outPudData() has finished!")


def process_sql(cxn, param=None, pos=None, sql=None):
    '''
    Process the input sql
    '''
    if param is not None and pos is not None: # param and pos synchronicity
        if type(param) is not tuple:
            logger.error('The type of param is TUPLE!')
            raise TypeError
        else: # out of index error has processed in insert()
            insert(cxn, pos, param)
    if sql is not None:
        outPutData(cxn, sql)
        #closed(cxn)  # Close Connection should call by user when user's all task is done.
    else:
        logger.error("sql and param,index can't happened the same time.And param and pos obey synchronicity")
        raise RuntimeError

connection = connect()
