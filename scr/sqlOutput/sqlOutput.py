# 添加项目路径
import sys

sys.path.append('d:/PyAsst')
import lib.PyAsstLog as log
import lib.PyAsstDb as db
import lib.PyAsstmail
import xml.etree.ElementTree as et
from dateutil.relativedelta import relativedelta
import datetime
import itertools
import os
import pandas as pd



def datetimeSplit(startdate, enddate, unit, step, column):
    """
        输入日期时间范围切片
        :param startdate: 开始时间 字符
        :param enddate: 结束时间
        :param unit: 切片单位 seconds,minutes,hours,days,months,years
        :param step: 步长
        :param column: 字段名
        :return:  返回结果list
    """
    startdate_list = []
    enddate_list = []
    datetime_list = []

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

        # 判断是否超出边界
        if i_dt > enddate:
            i_dt = enddate

        # 拼装字符
        datetime_string = 'and {}>='.format(column) + '\'' + datetime.datetime.strftime(startdate, '%Y-%m-%d %H:%M:%S') + '\'' + ' and {}<'.format(column) + '\'' + datetime.datetime.strftime(i_dt, '%Y-%m-%d %H:%M:%S') + '\''
        datetime_list.append(datetime_string)

        # 加步长
        startdate = startdate + datestep

    return datetime_list



if __name__ == '__main__':
    # 读取XML
    # xml_file = 'D:/PyAsst/autoTask/archive/GW统计表抽数.xml'
    # xml_sid = '入库'
    xml_file = sys.argv[1]
    xml_sid = sys.argv[2]

    # 判断xml格式是否有误
    try:
        et_tree = et.parse(xml_file)
    except Exception as err:
        log.info('xml格式有误, 终止运行 报错信息{}'.format(err))
        sys.exit()

    # 获取instance
    instance = et_tree.find('.//script[@sid="' + xml_sid + '"]/instance').text

    # 组装SQL list
    sql = et_tree.find('.//script[@sid="' + xml_sid + '"]/sql').text

    # 定位fields节点
    fields = et_tree.find('.//script[@sid="' + xml_sid + '"]/fields')

    # 定义fields_list
    fields_list = []

    # 遍历fields节点
    for field in fields.findall('field'):

        # 处理datetime类型
        if field.get('type') == 'datetime':

            startdatetype = field.find('startDate').get('type')
            enddatetype = field.find('endDate').get('type')
            startdate_str = field.find('startDate').text
            enddate_str = field.find('endDate').text
            unit = field.find('unit').text
            step = int(field.find('step').text)
            column = field.get('column')


            # 创建mysql对象
            mysqldb = db.mySqlDb()
            # 获取instance配置
            engine_dtsql = mysqldb.getConnect(mysqldb.getdbconfig(instance))

            # 处理开始日期
            if startdatetype == 'sql':
                startdate_str = mysqldb.execQuery('select ' + startdate_str, engine_dtsql).iloc[0, 0]
            elif startdatetype == 'fixed':
                pass
            else:
                log.info('无效的startdatetype')

            # 处理结束日期
            if enddatetype == 'sql':
                # 查询SQL表达式
                enddate_str = mysqldb.execQuery('select ' + enddate_str, engine_dtsql).iloc[0, 0]
            elif enddatetype == 'fixed':
                pass
            else:
                log.info('enddatetype')

            # 关闭mysql对象
            mysqldb.closeConnect(engine_dtsql)

            # 日期转换为日期类型
            startdate_dt = datetime.datetime.strptime(startdate_str, '%Y-%m-%d %H:%M:%S')
            enddate_dt = datetime.datetime.strptime(enddate_str, '%Y-%m-%d %H:%M:%S')

            # 获取时间切片
            datetime_list = datetimeSplit(startdate_dt, enddate_dt, unit, step, column)

            # 结果追加到fields_list
            fields_list.append(datetime_list)

        # 处理list类型
        elif field.get('type') == 'list':

            column = field.get('column')
            temp_list = field.find('value').text.split(',')
            value_list = ['and {}='.format(column) + '\'' + value + '\'' for value in temp_list]


            # 结果追加到fields_list
            fields_list.append(value_list)

        # 处理sql类型
        elif field.get('type') == 'sql':

            sql_str = field.find('value').text
            column = field.get('column')

            # 创建mysql对象
            mysqldb = db.mySqlDb()
            # 获取instance配置
            engine_sql = mysqldb.getConnect(mysqldb.getdbconfig(instance))

            # 查询SQL表达式
            result_str = mysqldb.execQuery(sql_str, engine_sql)

            # 关闭mysql对象
            mysqldb.closeConnect(engine_sql)

            # 取第一列转换为list
            result_list = list(result_str.iloc[:, 0])

            # 处理reslut_list
            value_list = ['and {}='.format(column) + '\'' + value + '\'' for value in result_list]

            # 结果追加到fields_list
            fields_list.append(value_list)

    # 将N个list 笛卡尔积
    merged_list = [' '.join(elements) for elements in itertools.product(*fields_list)]

    # 生成sql_list
    sql_list = [sql.replace('{WHERE}', elements) for elements in merged_list]

    # 定义时间戳
    timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

    # 连接查询mysql
    mysqldb = db.mySqlDb()

    # 获取instance配置
    engine_query = mysqldb.getConnect(mysqldb.getdbconfig(instance))

    # 获取输出类型
    outputtype = et_tree.find('.//script[@sid="' + xml_sid + '"]/output').get('type')

    if outputtype == 'excel':

        filepath = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/filePath').text

        # 拆分文件名,后缀,路径,
        (path, file) = os.path.split(filepath)
        (filename, ext) = os.path.splitext(file)


        # 定义导出文件名
        file_csv = path + filename + '_' + timestamp + '.csv'
        file_excel = path + filename + '_' + timestamp + '.xlsx'

        # 循环执行SQL
        for index, sql_str in enumerate(sql_list):

            result_df = mysqldb.execQuery(sql_str, engine_query)


            # 判断本次是否为空
            if result_df.empty:
                log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s,跳过本次循环' % (xml_file, xml_sid, index + 1, len(sql_list), result_df.shape[0]))
                continue

            if not os.path.exists(file_csv):
                result_df.to_csv(file_csv, index=0, header=True, mode='a')
            else:
                result_df.to_csv(file_csv, index=0, header=False, mode='a')
            log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s' % (xml_file, xml_sid, index + 1, len(sql_list), result_df.shape[0]))


        # 判断临时文件是否有结果
        try:
            a = pd.read_csv(file_csv)
        except Exception as err:
            log.info('本次查询没有结果,结束运行')
            sys.exit()

        # csv转excel
        write = pd.ExcelWriter(file_excel, engine='openpyxl')
        temp_df = pd.read_csv(file_csv, encoding='UTF-8', header=None, low_memory=False)
        temp_df.to_excel(write, index=False, header=None, sheet_name='Sheet1')
        write.close()
        log.info('结果已输出至：{}'.format(file_excel))
        os.remove(file_csv)

    elif outputtype == 'mysql':
        dbinstance = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/dbInstance').text
        dbtable = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/dbTable').text

        # 连接插入mysql
        mysqldb = db.mySqlDb()

        # 获取instance配置
        engine_insert = mysqldb.getConnect(mysqldb.getdbconfig(dbinstance))

        # 执行SQL
        for index, sql_str in enumerate(sql_list):
            result_df = mysqldb.execQuery(sql_str, engine_query)

            # 判断本次是否为空
            if result_df.empty:
                log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s,跳过本次循环' % (xml_file, xml_sid, index + 1, len(sql_list), result_df.shape[0]))
                continue

            result_df.to_sql(name=dbtable, con=engine_insert, if_exists='append', index=False, chunksize=500)
            log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s' % (xml_file, xml_sid, index + 1, len(sql_list), result_df.shape[0]))

        log.info('XML: %s sid: %s 结果已插入到实例: %s 表: %s' % (xml_file, xml_sid, dbinstance, dbtable))

        # 关闭mysql对象
        mysqldb.closeConnect(engine_insert)

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


        # 定义导出文件名
        file_csv = path + filename + '_' + timestamp + '.csv'
        file_excel = path + filename + '_' + timestamp + '.xlsx'

        # 循环执行SQL
        for index, sql_str in enumerate(sql_list):

            result_df = mysqldb.execQuery(sql_str, engine_query)

            # 判断本次是否为空
            if result_df.empty:
                log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s,跳过本次循环' % (
                xml_file, xml_sid, index + 1, len(sql_list), result_df.shape[0]))
                continue

            if not os.path.exists(file_csv):
                result_df.to_csv(file_csv, index=0, header=True, mode='a')
            else:
                result_df.to_csv(file_csv, index=0, header=False, mode='a')
            log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s' % (
            xml_file, xml_sid, index + 1, len(sql_list), result_df.shape[0]))

        # 判断临时文件是否有结果
        try:
            a = pd.read_csv(file_csv)
        except Exception as err:
            log.info('本次查询没有结果,结束运行')
            sys.exit()

        # csv转excel
        write = pd.ExcelWriter(file_excel, engine='openpyxl')
        temp_df = pd.read_csv(file_csv, encoding='UTF-8', header=None, low_memory=False)
        temp_df.to_excel(write, index=False, header=None, sheet_name='Sheet1')
        write.close()
        log.info('结果已输出至：{}'.format(file_excel))
        os.remove(file_csv)

        # 添加邮件对象
        sandmail = lib.PyAsstmail.mail(mailsender)

        # 发邮件
        sandmail.sandMail(mailreceitvers, file_excel, mailtitle, mailtext)

    elif outputtype == 'postfile':

        header = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/header').text
        responsetype = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/responsetype').text
        url = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/url').text
        form = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/form').text

        # 处理url
        if header == 'Post_LD':
            url = url.replace('{server}', 'ldoms.buycoor.net')
        elif header == 'Post_CPD':
            url = url.replace('{server}', 'cpdoms.buycoor.net')
        elif header == 'Post_LRLCPD':
            url = url.replace('{server}', 'lrloms.buycoor.net')
        elif header == 'Post_GW':
            url = url.replace('{server}', 'eoms.buycoor.net')
        else:
            log.info('无效的header,终止运行')
            sys.exit()

        # 创建info sheet
        info_excel = pd.DataFrame()
        info_excel['url'] = [url]
        info_excel['header'] = [header]
        info_excel['responsetype'] = [responsetype]
        info_excel['form'] = [form]

        # 定义post file路径
        filepath = et_tree.find('.//script[@sid="' + xml_sid + '"]/output/filePath').text

        # 拆分文件名,后缀,路径,
        (path, file) = os.path.split(filepath)
        (filename, ext) = os.path.splitext(file)

        # 定义导出文件名
        file_csv = path + '/' + filename + '_' + timestamp + '.csv'
        file_excel = path + '/' + filename + '_' + timestamp + '.xlsx'



        # 循环执行SQL
        for index, sql_str in enumerate(sql_list):

            result_df = mysqldb.execQuery(sql_str, engine_query)

            # 判断本次是否为空
            if result_df.empty:
                log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s,跳过本次循环' % (xml_file, xml_sid, index + 1, len(sql_list), result_df.shape[0]))
                continue

            if not os.path.exists(file_csv):
                result_df.to_csv(file_csv, index=0, header=True, mode='a')
            else:
                result_df.to_csv(file_csv, index=0, header=False, mode='a')
            log.info('XML: %s sid: %s 执行进度: %s / %s 结果行数：%s' % (xml_file, xml_sid, index + 1, len(sql_list), result_df.shape[0]))

            # 判断临时文件是否有结果
            try:
                a = pd.read_csv(file_csv)
            except Exception as err:
                log.info('本次查询没有结果,结束运行')
                sys.exit()

        # csv转excel
        write = pd.ExcelWriter(file_excel, engine='openpyxl')
        temp_df = pd.read_csv(file_csv, encoding='UTF-8', header=None, low_memory=False)
        temp_df.to_excel(write, index=False, header=None, sheet_name='value')
        write.close()
        log.info('结果已输出至：{}'.format(file_excel))
        os.remove(file_csv)


        # 插入post excel info
        if os.path.exists(file_excel):
            with pd.ExcelWriter(file_excel, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
                info_excel.to_excel(writer, sheet_name='info', index=False)
                log.info('post info已追加至{}'.format(file_excel))
        else:
            log.info('本次执行没有结果')


    # 关闭mysql对象
    mysqldb.closeConnect(engine_dtsql)








