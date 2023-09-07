# 添加项目路径
import sys
sys.path.append('d:/PyAsst')
import lib.PyAsstLog as log
import lib.PyAsstDb as db
import lib.PyAsstmail
import xml.etree.ElementTree as et
from dateutil.relativedelta import relativedelta
import datetime
import os
import pandas as pd



def datetimeSplit(startdate, enddate, unit, step):
    """
        输入日期时间范围切片
        :param startdate: 开始时间 字符
        :param enddate: 结束时间
        :param unit: 切片单位 seconds,minutes,hours,days,months,years
        :param step: 步长
        :return:  返回结果DF
    """
    startdate_list = []
    enddate_list = []

    if unit == 'minutes':
        datestep = relativedelta(minutes=step)
    elif unit == 'hours':
        datestep = relativedelta(hours=step)
    elif unit == 'days':
        datestep = relativedelta(days=step)
    elif unit == 'months':
        datestep = relativedelta(months=step)
    elif unit == 'years':
        datestep = relativedelta(years=step)


    # 时间切片
    while startdate < enddate:
        i_dt = startdate + datestep
        # 判断超出边界
        if i_dt > enddate:
            i_dt = enddate

        startdate_list.append(datetime.datetime.strftime(startdate, '%Y-%m-%d %H:%M:%S'))
        enddate_list.append(datetime.datetime.strftime(i_dt, '%Y-%m-%d %H:%M:%S'))

        startdate = startdate + datestep

    # 结果整理DF
    datetime_split_df = pd.DataFrame(
        {
            'startdate': startdate_list,
            'enddate': enddate_list
        }
    )
    return datetime_split_df


if __name__ == '__main__':
    # 读取XML
    xml_file = 'D:/PyAsst/autoTask/archive/sql测试.xml'
    xml_sid = '1'
    # xml_file = sys.argv[1]
    # xml_sid = sys.argv[2]
    et_tree = et.parse(xml_file)
    sql = et_tree.find('.//script[@sid="' + xml_sid + '"]/sql').text
    fieldtype = et_tree.find('.//script[@sid="' + xml_sid + '"]/field').get('type')
    column = et_tree.find('.//script[@sid="' + xml_sid + '"]/field/column').text
    instance = et_tree.find('.//script[@sid="' + xml_sid + '"]/instance').text
    outputtype = et_tree.find('.//script[@sid="' + xml_sid + '"]/output').get('type')



    # 时间戳
    timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

    # 判断field等于datetime
    if fieldtype == 'datetime':

        startdatetype = et_tree.find('.//script[@sid="' + xml_sid + '"]/field/startDate').get('type')
        enddatetype = et_tree.find('.//script[@sid="' + xml_sid + '"]/field/endDate').get('type')
        startdate_str = et_tree.find('.//script[@sid="' + xml_sid + '"]/field/startDate').text
        enddate_str = et_tree.find('.//script[@sid="' + xml_sid + '"]/field/endDate').text
        unit = et_tree.find('.//script[@sid="' + xml_sid + '"]/field/unit').text
        step = int(et_tree.find('.//script[@sid="' + xml_sid + '"]/field/step').text)

        # 创建mysql对象
        mysqldb = db.mySqlDb()

        # 获取instance配置
        engine_dtsql = mysqldb.getConnect(mysqldb.getdbconfig(instance))

        if startdatetype == 'auto':

            # 查询SQL表达式
            startdate_str = mysqldb.execQuery('select ' + startdate_str, engine_dtsql).iloc[0, 0]
        elif startdatetype == 'fixed':
            pass
        else:
            log.info('无效的startdatetype')

        if enddatetype == 'auto':

            # 查询SQL表达式
            enddate_str = mysqldb.execQuery('select ' + enddate_str, engine_dtsql).iloc[0, 0]
        elif enddatetype == 'fixed':
            pass
        else:
            log.info('enddatetype')

        # 日期转换为日期类型
        startdate_dt = datetime.datetime.strptime(startdate_str, '%Y-%m-%d %H:%M:%S')
        enddate_dt = datetime.datetime.strptime(enddate_str, '%Y-%m-%d %H:%M:%S')

        # 关闭mysql对象
        mysqldb.closeConnect(engine_dtsql)

    # 判断field等于指定字段
    if fieldtype == 'string':
        value = et_tree.find('.//script[@sid="' + xml_sid + '"]/field/value').text

    # 连接查询mysql
    mysqldb = db.mySqlDb()

    # 获取instance配置
    engine_query = mysqldb.getConnect(mysqldb.getdbconfig(instance))


    # 判断输出类型
    if outputtype == 'excel':
        filepath = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/filePath').text

        # 检查是否有相同文件, 并清除
        if os.path.exists(filepath):
            log.info('已存在文件 %s' % filepath)
            os.remove(filepath)
            log.info('已删除文件 %s' % filepath)

        # 按日期切片结果遍历执行
        if fieldtype == 'datetime':

            # 获取时间切片
            datedatetime_split_df = datetimeSplit(startdate_dt, enddate_dt, unit, step)

            # 遍历时间片执行
            for index, row in datedatetime_split_df.iterrows():
                query_sql = sql.replace('{WHERE}', 'and ' + column + '>=' + '\'' + row['startdate'] + '\'' + '\n' + 'and ' + column + '<' + '\'' + row['enddate'] + '\'')
                result_df = mysqldb.execQuery(query_sql, engine_query)

                if result_df.empty:
                    log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s,跳过本次循环' % (xml_file, xml_sid, index + 1, datedatetime_split_df.shape[0], result_df.shape[0]))
                    continue
                if not os.path.exists(filepath):
                    result_df.to_excel(filepath, sheet_name='Sheet1', index=False, header=True)
                else:
                    with pd.ExcelWriter(filepath, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
                        result_df.to_excel(writer, sheet_name='Sheet1', index=False)
                log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s' % (xml_file, xml_sid, index + 1, datedatetime_split_df.shape[0], result_df.shape[0]))

            log.info('XML: %s sid: %s 结果已输出至：%s' % (xml_file, xml_sid, filepath))

    elif outputtype == 'mysql':
        dbinstance = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/dbInstance').text
        dbtable = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/dbTable').text

        # 连接插入mysql
        mysqldb = db.mySqlDb()

        # 获取instance配置
        engine_insert = mysqldb.getConnect(mysqldb.getdbconfig(dbinstance))

        # 按日期切片结果遍历执行
        if fieldtype == 'datetime':

            # 获取时间切片
            datedatetime_split_df = datetimeSplit(startdate_dt, enddate_dt, unit, step)

            # 遍历时间片执行
            for index, row in datedatetime_split_df.iterrows():
                query_sql = sql.replace('{WHERE}', 'and ' + column + '>=' + '\'' + row['startdate'] + '\'' + '\n' + 'and ' + column + '<' + '\'' + row['enddate'] + '\'')
                result_df = mysqldb.execQuery(query_sql, engine_query)
                if result_df.empty:
                    log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：0,跳过本次循环' % (xml_file, xml_sid, index + 1, datedatetime_split_df.shape[0]))
                    continue
                result_df.to_sql(name=dbtable, con=engine_insert, if_exists='append', index=False, chunksize=500)
                log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s' % (xml_file, xml_sid, index + 1, datedatetime_split_df.shape[0], result_df.shape[0]))

            # 关闭mysql对象
            mysqldb.closeConnect(engine_insert)

            log.info('XML: %s sid: %s 结果已插入到实例: %s 表: %s' % (xml_file, xml_sid, dbinstance, dbtable))

    elif outputtype == 'email':

        mailsender = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/mailSender').text

        # 收件人获取并转换成list
        mailreceitvers = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/mailReceitvers').text.split(',')

        mailtitle = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/mailTitle').text
        mailtext = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/mailText').text
        filepath = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/filePath').text

        # 处理输出文件名
        (path, file) = os.path.split(filepath)
        (filename, ext) = os.path.splitext(file)
        filepath = path + '/' + filename + '_' + timestamp + '_' + ext


        # 检查是否有相同文件, 并清除
        if os.path.exists(filepath):
            log.info('已存在文件 %s' % filepath)
            os.remove(filepath)
            log.info('已删除文件 %s' % filepath)

        # 按日期切片结果遍历执行
        if fieldtype == 'datetime':

            # 获取时间切片
            datedatetime_split_df = datetimeSplit(startdate_dt, enddate_dt, unit, step)

            # 遍历时间片执行
            for index, row in datedatetime_split_df.iterrows():
                query_sql = sql.replace('{WHERE}', 'and ' + column + '>=' + '\'' + row[
                    'startdate'] + '\'' + '\n' + 'and ' + column + '<' + '\'' + row['enddate'] + '\'')
                result_df = mysqldb.execQuery(query_sql, engine_query)

                if result_df.empty:
                    log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s,跳过本次循环' % (
                    xml_file, xml_sid, index + 1, datedatetime_split_df.shape[0], result_df.shape[0]))
                    continue
                if not os.path.exists(filepath):
                    result_df.to_excel(filepath, sheet_name='Sheet1', index=False, header=True)
                else:
                    with pd.ExcelWriter(filepath, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
                        result_df.to_excel(writer, sheet_name='Sheet1', index=False)
                log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s' % (xml_file, xml_sid, index + 1, datedatetime_split_df.shape[0], result_df.shape[0]))

            log.info('XML: %s sid: %s 结果已输出至：%s' % (xml_file, xml_sid, filepath))

            # 添加邮件对象
            sandmail = lib.PyAsstmail.mail(mailsender)

            # 发邮件
            sandmail.sandMail(mailreceitvers, filepath, mailtitle, mailtext)


    # 关闭mysql对象
    mysqldb.closeConnect(engine_query)
















