

# 添加包路径
import sys
sys.path.append('d:/PyAsst')

# 导入包
import lib.PyAsstLog as log
from apscheduler.schedulers.background import BackgroundScheduler
import os



# 路径定义
root_path = 'd:/AutoTask/'
waiting_path = root_path + 'waiting/'
archive_path = root_path + 'archive/'
ontime_path = root_path + 'ontime/'
running_path = root_path + 'running/'

# 方法定义


# 主程序
if __name__ == '__main__':

    # 主程序启动检查
    pathCheck_list = [root_path, waiting_path, archive_path, ontime_path, running_path]
    for path in pathCheck_list:
        if os.path.exists(path) == False:
            log.info('启动检查失败,文件{}不存在. 自动退出程序'.format(path))
            sys.exit()
    log.info('启动检查正常')



    print('ok')



    pass