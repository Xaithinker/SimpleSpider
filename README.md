## Python Spider
> 爬取[链家](https://www.lianjia.com)所有省份城市的租房信息

- 日志记录：`log.py`
  - 默认
  
   `formats = '%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s'`
  - 自定义formats可覆盖默认格式。
  - 以每个模块 `__name__` 作为名称:
  
  `logger = logging.getLogger(__name__)`

- 数据库存储 : `db.py`
  - 以本地数据库服务作为示例
  - 简单封装一些数据库操作，带参数或不带参数。
  - `process_sql`

- `config.py`
  - 一些初始化参数或全局队列、变量

- `urlhandler.py`
  - 请求、解析数据并存储至表
- `urlinCity.py` and `parseCity.py`
  - 使用基于内置`yield`的协程
  - 多线程使用共享队列并发执行
- `parseRent`
  - 多线程使用共享队列并发执行
  
- `spider.py`
  - 主函数，完成自动化爬取并存储过程