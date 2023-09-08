# 添加项目路径
import sys
sys.path.append('d:/PyAsst')
import lib.PyAsstLog as log
import xml.etree.ElementTree as et
import os
import pandas as pd
import re
import time
import requests
import shutil
import datetime



if __name__ == '__main__':
    # 读取XML
    # xml_file = 'D:/PyAsst/autoTask/waiting/3023-09-04 19#00#00_response执行.xml'
    # xml_sid = '1'
    xml_file = sys.argv[1]
    xml_sid = sys.argv[2]



    # 定位路径
    cur_path = os.path.abspath(os.path.dirname(__file__))
    data_path = cur_path[:cur_path.find("PyAsst")] + 'PyAsst/data/ttxResponse/'
    waiting_path = data_path + 'waiting/'
    archive_path = data_path + 'archive/'
    header_path = data_path + 'header/'


    # 遍历waiting文件夹
    for file_xlsx in os.listdir(waiting_path):

        log.info('正在执行 %s' % file_xlsx)

        # 拆分文件名,后缀,路径,
        (path, file) = os.path.split(file_xlsx)
        (filename, ext) = os.path.splitext(file)

        # 格式化当前时间
        timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

        # 读取excel
        file_info_df = pd.read_excel(waiting_path + file_xlsx, sheet_name='info')
        file_value_df = pd.read_excel(waiting_path + file_xlsx, sheet_name='value', keep_default_na=False, dtype=str)

        # 获取info信息
        url = file_info_df.loc[0, 'url']
        header = file_info_df.loc[0, 'header']
        responsetype = file_info_df.loc[0, 'responsetype']
        form = file_info_df.loc[0, 'form']

        # 整理header
        headers = {}
        f = open(header_path + header + '.properties', encoding='utf-8')
        for line in f.read().split('\n'):
            # 拆分键与值,仅1次
            key, value = line.split(':', 1)
            headers[key] = value
        f.close()

        # 检查字段数量是否一到
        if len(file_value_df.columns) != len(re.findall('{columns}', form)):
            log.info('字段数量不一致,中止执行')
            sys.exit()

        # form_list生成
        form_list = []

        # 遍历excel行
        for row in range(file_value_df.shape[0]):

            # 查询form需要替换的字段
            refind = re.findall('{columns}', form)

            # 备份原form格式
            form_re = form
            for column in range(len(file_value_df.columns)):

                # 变量替换
                form_re = re.sub('{columns}', str(file_value_df.loc[row][column]), form_re, count=1)
                refind = re.findall('{columns}', form_re)

            # 存储form至list
            form_list.append(form_re)

        # 提交请求
        log.info('提交请求共%s条' % len(form_list))

        # 计算器
        count_n = 1

        # 遍历form_list并执行
        for form_str in form_list:
            log.info('正在提交请求：%s / %s' % (count_n, len(form_list)))
            form_str = form_str.encode('UTF-8').decode('latin1')
            count_n = count_n + 1

            if responsetype == 'post':
                try:
                    res = requests.post(url, data=form_str, headers=headers)
                    log.info(res.text)
                except Exception as err:
                    log.info('form执行失败 %s' % form_str)
                    log.info('报错信息: %s' % err)
            elif responsetype == 'put':
                try:
                    pass
                    res = requests.put(url, data=form_str, headers=headers)
                    log.info(res.text)
                except Exception as err:
                    log.info('form执行失败 %s' % form_str)
                    log.info('报错信息: %s' % err)
            else:
                log.info('无效的responsetype配置,中止运行')
                sys.exit()
            time.sleep(1)

        # 执行完成备份excel
        shutil.move(waiting_path + file_xlsx, archive_path + filename + '_' + timestamp + ext)
        log.info('执行完成 %s' % file_xlsx)





