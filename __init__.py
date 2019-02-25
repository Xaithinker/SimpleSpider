#/usr/bin env python
# -*- encoding:utf-8 -*-

from config import VERSION                               # config related
from db import connection, create

# [DB INIT]
create(connection) # init db with table : 'Province','City', 'Area', 'Rent' then connection closed!!!

# config
__version__ = VERSION
