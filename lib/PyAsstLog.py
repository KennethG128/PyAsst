# 导入包
import conf.Config as cf
import logging
from logging.handlers import RotatingFileHandler


# 定义日志路径+文件名
log_file = cf.Log_Path + 'logfile.log'
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






