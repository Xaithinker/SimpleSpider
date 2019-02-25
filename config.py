VERSION = '1.0.00'
BASE_URL = 'https://www.lianjia.com/city/'
USER_AGENT = (
    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36",
    )

# DBMS
DBNAME = 'spider'
HOST = '127.0.0.1'  # local 
USER = 'user'       # your db usrname
PASSWORD = '11113333' # set your password
PORT = 3306         # port
## Create table City, Province, Area, Rent
TABLE_NAME = ('Province','City', 'Area', 'Rent')
QUERY_TABLE_PROVINCE = [
'''
CREATE TABLE `City`
(
  city_id            int         NOT NULL   PRIMARY KEY AUTO_INCREMENT,
  city_name          varchar(50) CHARACTER SET UTF8MB4 NOT NULL , 
  url                varchar(50) CHARACTER SET UTF8MB4 NOT NULL , 
  pro_id             int         NOT NULL   
)
''',

'''
CREATE TABLE `Province`
(
  pro_id             int           NOT NULL   PRIMARY KEY  AUTO_INCREMENT,
  pro_name           varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  city_num           int           NOT NULL    DEFAULT 1
)
''', 

'''
CREATE TABLE `Area`
(
  area_id             BIGINT           NOT NULL   PRIMARY KEY  AUTO_INCREMENT,
  area_name           varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  city_id             int           NOT NULL
)
''',
'''
CREATE TABLE `Rent`
(
  rent_id             BIGINT           NOT NULL   PRIMARY KEY  AUTO_INCREMENT,
  title               varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  area                varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  sub_area            varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  size                varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  room                varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  brand               varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  price               int ,
  time                varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  tag                 varchar(50)   CHARACTER SET UTF8MB4 NOT NULL,
  area_id             BIGINT           NOT NULL
)
''',

'ALTER TABLE City ADD CONSTRAINT FK_City_Province FOREIGN KEY (pro_id) REFERENCES Province (pro_id)',
'ALTER TABLE Area ADD CONSTRAINT FK_Area_City FOREIGN KEY (city_id) REFERENCES City (city_id)'
#'ALTER TABLE Rent ADD CONSTRAINT FK_Rent_Area FOREIGN KEY (area_id) REFERENCES Area (area_id)' # sometimes, lost the area_id, so remove the foreign key
]

# three columns type: tuple 
# eg. ('1001', 'province1', '1100') with EXECUTE(operation [, parameters])
# OR list of tuples like [(), ()] with EXECUTEMANY(...)
INSERT_INTO_PROVINCE = 'INSERT INTO `Province`(pro_id, pro_name, city_num)    VALUES(%s, %s, %s)'  
INSERT_INTO_CITY     = 'INSERT INTO `City`    (city_id, city_name, url, pro_id)   VALUES(%s, %s, %s, %s)'
INSERT_INTO_RENT     = 'INSERT INTO `RENT`    (rent_id, title, area, sub_area, size, room, brand, price, time, tag, area_id)    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
INSERT_INTO_AREA     = 'INSERT INTO `AREA`    (area_id, area_name, city_id)  VALUES(%s, %s, %s)'
INSERT_INTO_ONE      = [INSERT_INTO_PROVINCE, INSERT_INTO_AREA, INSERT_INTO_CITY, INSERT_INTO_RENT]


# parseCity.py

QUEUE_MAXSIZE = 11  # 队列缓存最大容量

# parseRent.py
QUEUE_SIZE = 11 #

# for deleting Fout Tables
'''
DELETE FROM Rent WHERE rent_id >= 0;
DELETE FROM Area WHERE area_id >= 0;
DELETE FROM City WHERE city_id >= 0;
DELETE FROM Province WHERE pro_id >= 0;
'''