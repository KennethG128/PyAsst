
# 添加项目路径
import sys
import logging
sys.path.append('d:/PyAsst')


# 导入包
import lib.PyAsstLog as log
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import os
import subprocess
import datetime
import shutil
import croniter
import xml.etree.ElementTree as et



# 路径定义
root_path = 'd:/PyAsst/autoTask/'
waiting_path = root_path + 'waiting/'
archive_path = root_path + 'archive/'
running_path = root_path + 'running/'


# 方法定义
def checkXml(xml_file):

    # 拆分文件名,后缀,路径,
    (path, file) = os.path.split(xml_file)
    (filename, ext) = os.path.splitext(file)

    # 判断是否xml文件
    if (ext == '.xml'):

        # 尝试打开xml格式是否正确
        xml_check = et.parse(os.path.join(path, file))

        # 如果的报错字符串,表示格式有误
        if isinstance(xml_check, str):
            return 'false'
        else:
            return 'true'
    else:
        return 'false'

def checkXmlCron(xml_file):
    # 拆分文件名,后缀,路径,
    (path, file) = os.path.split(xml_file)
    (filename, ext) = os.path.splitext(file)

    # 拆分日期时间
    dt = file.split('_')[0]
    try:
        if datetime.datetime.strptime(dt, "%Y-%m-%d %H#%M#%S") < datetime.datetime.now() :
            return 'true'
        else:
            return 'false'
    except Exception as err:
        return 'None'

def checkRunningTask():
    for file in os.listdir(running_path):
        if checkXml(os.path.join(running_path, file)) == 'true':
            log.info('正在检查上次未执行完成的xml任务')
            shutil.move(os.path.join(running_path, file), os.path.join(root_path, file))
            log.info('%s 已移动至%s' % (file, root_path))

def loadTask():
    for task_file in os.listdir(root_path):

        # 合并文件路径与文件名
        task = root_path + task_file

        # 如果是文件夹跳出本次for循环
        if os.path.isdir(task):
            continue

        # 判断XML格式是否有问题
        if not checkXml(task):
            log.info('XML格式校验无效,跳过本次执行')
            continue

        # 移动task至running
        shutil.move(root_path + task_file, running_path)

        # 加载task
        scheduler.add_job(taskJob, args=[running_path + task_file], next_run_time=datetime.datetime.now())


def taskJob(task):

    # 拆分文件名,后缀,路径,
    (path, file) = os.path.split(task)
    (filename, ext) = os.path.splitext(file)

    # 格式化当前时间
    timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

    # 读取XML
    et_tree = et.parse(task)
    rootnode = et_tree.getroot()
    nodes_list = rootnode.findall('script')

    # 执行task
    try:
        # 遍历全部script节点
        for node in nodes_list:

            # 参数1=XML文件路径,  参数2=script对应的scriptId
            res = subprocess.Popen('python {} {} {}'.format(node.attrib['path'], task, node.attrib['sid']), stdout=subprocess.PIPE, encoding='utf-8')
            log.info(res.stdout.read())
    except Exception as err:
        log.info('sid:{} {} 运行报错: {}'.format(node.attrib['sid'], node.attrib['path'], err))

    # task执行完成后处理
    if rootnode.attrib['type'] == 'Repeat':

        # 计算下一次执行时间
        now = datetime.datetime.now()
        cron = croniter.croniter(rootnode.attrib['cron'], now)
        cronNextTime = [cron.get_next(datetime.datetime).strftime('%Y-%m-%d %H#%M#%S')][0]
        shutil.move(task, waiting_path + cronNextTime + '_' + filename + ext)

    elif rootnode.attrib['type'] == 'Once':
        # shutil.move(task, archive_path + filename + '_' + timestamp + ext)
        shutil.move(task, archive_path + filename + ext)
    else:
        log.info('无法识别Type')
        shutil.move(task, archive_path + filename + '_' + timestamp + ext)
        pass

    log.info('执行完成 %s' % task)

def waitingTaskMove():
    for file in os.listdir(waiting_path):

        # 如果是文件夹跳出本次for循环
        if os.path.isdir(os.path.join(waiting_path, file)):
            continue
        if checkXml(os.path.join(waiting_path, file)) == 'true' and checkXmlCron(os.path.join(waiting_path, file)) == 'true':
            log.info('%s XML校验有效' % file)
            log.info('%s Cron已到期' % file)
            shutil.move(os.path.join(waiting_path, file), os.path.join(root_path, file.split('_')[1]))
            log.info('%s 已移动至%s' % (file, root_path))

# 主程序
if __name__ == '__main__':

    # 主程序启动检查
    pathCheck_list = [root_path, waiting_path, archive_path, running_path]
    for path in pathCheck_list:
        if not os.path.exists(path):
            log.info('启动检查失败,文件{}不存在. 自动退出程序'.format(path))
            sys.exit()
    log.info('启动检查正常')


    # apscheduler配置
    scheduler = BackgroundScheduler()
    logging.getLogger('apscheduler.executors.default').propagate = False
    executors = {
        'default': ThreadPoolExecutor(20),
        'processpool': ProcessPoolExecutor(5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 10
    }


    # 检查上次未完成Task
    checkRunningTask()

    # 扫描wating里已到期的xml任务
    scheduler.add_job(waitingTaskMove, trigger='interval', seconds=25)

    # 加载任务
    scheduler.add_job(loadTask, trigger='interval', seconds=30)

    # 执行任务
    scheduler.start()

    # 一直运行
    while True:
        pass


