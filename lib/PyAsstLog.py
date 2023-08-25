# 导入包
import logging
from logging.handlers import RotatingFileHandler
import configparser

# 读取配置文件
cf = configparser.ConfigParser()
cf.read('d:/PyAsst/conf/config.ini', encoding='utf-8')

# 定义日志路径+文件名
log_file = cf.get('Path', "logPath") + 'logfile.log'
# 定义日志格式
log_format = logging.Formatter('%(asctime)s thread:%(thread)d process:%(process)d  message:%(message)s')

# 创建并配置logger日志器
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 定义handler处理器 输出文件.文件按照天拆分
file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when='D')
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)

# 定义handler处理器 输出控制台
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)


def info(msg):
    # 执行
    logging.info(msg)






