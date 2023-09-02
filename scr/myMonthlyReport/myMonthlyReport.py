# 添加项目路径
import sys
sys.path.append('d:/PyAsst')
import pandas as pd
import lib.PyAsstLog as log
import lib.PyAsstDb
import os
import datetime

def insertdb(table, pd_file):
    try:
        # myDB连接
        db = lib.PyAsstDb.mySqlDb()
        engine = db.getConnect(db.getdbconfig('myDB'))

        # 插入db
        pd_file.to_sql(name=table, con=engine, if_exists='append', index=False, chunksize=500)
        log.info('数据插入完成')
        db.closeConnect(engine)
        return 0

    except Exception as e:
        log.info(' 插入db失败,报错信息: %s' % e)
        db.closeConnect(engine)
        return 1


if __name__ == '__main__':

    # 读取XML
    # xml_file = 'D:/PyAsst/autoTask/TTXPOST测试.xml
    # xml_file = sys.argv[1]
    # xml_sid = sys.argv[2]

    # 定位文件
    excel_file = 'D:/KennethG/个人月报/个人月报.xlsx'
    temp_file = 'D:/KennethG/个人月报/temp.xlsx'

    # 定义变量
    created_date = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    yearmonth = '2023-08'
    # 获取年
    year = yearmonth[:4]



    # 读取excel并导入db
    excel_sheet_list = ['f_account_detail', 'f_cfs_detail']
    for excel_sheet in excel_sheet_list:
        if excel_sheet == 'f_account_detail':
            log.info('准备执行 %s' % excel_sheet)
            excel_sheet_pd = pd.read_excel(excel_file, sheet_name=excel_sheet)
            excel_sheet_pd['created'] = created_date
            del excel_sheet_pd['billingamount']

            # 执行插入db
            iserror = insertdb('f_account_detail', excel_sheet_pd)
            log.info('%s 导入完成' % excel_sheet)

        elif excel_sheet == 'f_cfs_detail':
            log.info('准备执行 %s' % excel_sheet)
            excel_sheet_pd = pd.read_excel(excel_file, sheet_name=excel_sheet)
            excel_sheet_pd['created'] = created_date
            del excel_sheet_pd['status']

            # 执行插入db
            iserror = insertdb('f_cfs_detail', excel_sheet_pd)
            log.info('%s 导入完成' % excel_sheet)

        else:
            log.info('无效的sheet')




    # 输出'收支表汇总-视图'

    # 定义SQL
    sql = ('''
                SELECT
a.code AS code,
a.name AS name,
ifnull(b.amount, 0) AS 本月发生额,
ifnull(c.本年累计, 0) AS 本年累计
FROM f_cfs_code a
LEFT JOIN (
SELECT
date AS date,
code AS code,
name AS name,
amount AS amount
FROM f_cfs_detail
WHERE 1
and date REGEXP '@ym'
) b ON a.code = b.code
LEFT JOIN (
SELECT
fcd.code,
fcd.name,
sum(fcd.amount) as '本年累计'
FROM
	f_cfs_detail fcd 
WHERE
	1 
	AND date REGEXP '%y'
group by fcd.code,fcd.name
) c ON a.code = c.code
                ''').replace('@ym', yearmonth).replace('%y', year)

    # 创建数据库对象
    db = lib.PyAsstDb.mySqlDb()
    engine = db.getConnect(db.getdbconfig('myDB'))

    # 查询数据库
    try:
        result_df = db.execQuery(sql, engine)
    except Exception as err:
        log.info('SQL查询失败: %s ' % err)

    # 清除temp文件
    if os.path.exists(temp_file):
        os.remove(temp_file)

    # 输出到temp
    result_df.to_excel(temp_file, index=False, header=True, sheet_name='收支表汇总-视图')
    log.info('数据导出成功 %s' % temp_file)

    # 关闭数据库
    db.closeConnect(engine)


