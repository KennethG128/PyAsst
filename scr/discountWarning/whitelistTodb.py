# 添加项目路径
import sys
sys.path.append('d:/PyAsst')
import lib.PyAsstLog as log
import lib.PyAsstDb as db
import pandas as pd
import datetime
import xml.etree.ElementTree as et



if __name__ == '__main__':
    # 读取XML
    # xml_file = 'D:/PyAsst/autoTask/archive/XML模版/whitelistTodb模版.xml'
    # xml_sid = '1'
    xml_file = sys.argv[1]
    xml_sid = sys.argv[2]
    et_tree = et.parse(xml_file)


    # 初始化创建时间
    created_date = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

    # onedrive目录定义
    data_path = et_tree.find('.//script[@sid="' + xml_sid + '"]/dataPath').text



    try:
        excel_pd1 = pd.read_excel(data_path + 'Makeup链接ID白名单.xlsx', sheet_name='Makeup Link ID', converters={"sourceItemId":  str})
        excel_pd2 = pd.read_excel(data_path + 'ACD&PPD链接ID白名单.xlsx', sheet_name='ACD&PPD Link ID', converters={"sourceItemId": str})
        excel_pd3 = pd.read_excel(data_path + 'LDs链接ID白名单.xlsx', sheet_name='LDs Link ID', converters={"sourceItemId": str})
        excel_pd4 = pd.read_excel(data_path + 'CPD&LID链接ID白名单.xlsx', sheet_name='CPD&LID  Link ID', converters={"sourceItemId": str})

        excel_pd1a = pd.read_excel(data_path + 'Makeup链接ID白名单.xlsx', sheet_name='Makeup Policy Code', converters={"PolicyCode": str})
        excel_pd2a = pd.read_excel(data_path + 'ACD&PPD链接ID白名单.xlsx', sheet_name='ACD&PPD Policy Code', converters={"PolicyCode": str})
        excel_pd3a = pd.read_excel(data_path + 'LDs链接ID白名单.xlsx', sheet_name='LDs Policy Code', converters={"PolicyCode": str})
        excel_pd4a = pd.read_excel(data_path + 'CPD&LID链接ID白名单.xlsx', sheet_name='CPD&LID Policy Code', converters={"PolicyCode": str})


    except Exception as e:
        log.info('打开excel失败: %s' % e)
        sys.exit()

    # 合并df
    excel_pdAll = pd.DataFrame()
    excel_pdAll = pd.concat([excel_pdAll, excel_pd1, excel_pd2, excel_pd3, excel_pd4])

    excel_pdAll2 = pd.DataFrame()
    excel_pdAll2 = pd.concat([excel_pdAll2, excel_pd1a, excel_pd2a, excel_pd3a, excel_pd4a])

    # 添加创建日期字段
    excel_pdAll['created'] = created_date
    excel_pdAll2['created'] = created_date




    try:
        # 创建mysql对象
        mysqldb = db.mySqlDb()

        # 获取instance配置
        engine = mysqldb.getConnect(mysqldb.getdbconfig('localomsdb_xomsdb'))
    except Exception as err:
        log.info(err)



    try:
        # 插入db
        excel_pdAll.to_sql(name='oms_discount_exclude', con=engine, if_exists='append', index=False, chunksize=500)
        excel_pdAll2.to_sql(name='oms_discount_exclude2', con=engine, if_exists='append', index=False, chunksize=500)
        log.info('导入完成')
        pd.read_sql('delete from oms_discount_exclude where created < "' + created_date + '"', engine, chunksize=100)
        pd.read_sql('delete from oms_discount_exclude2 where created < "' + created_date + '"', engine, chunksize=100)
        log.info('清空表历史完成')
    except Exception as err:
        log.info(err)