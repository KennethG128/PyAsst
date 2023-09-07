# 导入包
from sqlalchemy import create_engine
import lib.PyAsstLog as log
from lib.PyAsstAes import PrpCrypt
import configparser
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import os




class mySqlDb:

    def getdbconfig(self, instance):

        # 获取db.ini文件
        cur_path = os.path.abspath(os.path.dirname(__file__))
        config_path = cur_path[:cur_path.find("PyAsst")] + 'PyAsst/conf/'
        cf = configparser.ConfigParser()
        cf.read(config_path + 'db.ini', encoding='utf-8')

        host = cf.get(instance, "host")
        port = cf.get(instance, "port")
        user = cf.get(instance, "user")
        db = cf.get(instance, "db")

        # password解密
        pc = PrpCrypt('1', '2')
        password = pc.decrypt(cf.get(instance, "password"))

        if 'ssh' in instance:
            ssh_host = cf.get(instance, "ssh_host")
            ssh_port = cf.get(instance, "ssh_port")
            ssh_user = cf.get(instance, "ssh_user")

            # password解密
            ssh_password = pc.decrypt(cf.get(instance, "ssh_password"))

            # SSH配置与打开
            tunnel = SSHTunnelForwarder(
                (ssh_host, int(ssh_port)),
                ssh_username=ssh_user,
                ssh_password=ssh_password,
                remote_bind_address=(host, int(port))
            )
            tunnel.start()
            port = str(tunnel.local_bind_port)
            EngineArr = 'mysql+mysqlconnector://{}:{}@{}:{}/{}?charset = utf8'.format(user, password, '127.0.0.1', port, db)
        else:
            EngineArr = '''mysql+mysqlconnector://''' + user + ''':''' + password + '''@''' + host + ''':''' + port + '''/''' + db
        return EngineArr

    def getConnect(self, EngineArr):
        try:
            engin = create_engine(EngineArr, connect_args={'auth_plugin': 'mysql_native_password'})
            log.info('数据库连接成功')
        except Exception as err:
            log.info('数据库连接失败,%s' % err)
        else:
            return engin

    def getConnectGW(self, EngineArr):
        try:
            connect_args = {"ssl_disabled": "false"}
            engin = create_engine(EngineArr, connect_args=connect_args)
            log.info('数据库连接成功')
        except Exception as err:
            log.info('数据库连接失败,%s' % err)
        else:
            return engin

    def execQuery(self, sql, engine):
        try:
            df = pd.read_sql_query(sql, engine)
        except Exception as err:
            log.info("查询失败 %s" % err)
        else:
            return df

    def updateQuery(self, sql, engine):
        try:
            pd.read_sql(sql, engine, chunksize=500)
            log.info("更新数据库成功")
        except Exception as err:
            log.info("更新数据库失败 %s" % err)


    def closeConnect(self, engine):
         try:
             engine.dispose()
         except Exception as err:
             log.info('没有可关闭的连接 %s' % err)
         else:
             log.info('关闭数据库成功')


# ----------------------------------------------
# if __name__ == '__main__':
#     sql ='select * from alipay_settle_fee limit 100'
#
#     # 创建数据库对象
#     db = mySqlDb()
#
#     # 连接数据库
#     engine = db.getConnect(db.getdbconfig('localomsdb'))
#
#     # 查询SQL
#     df = db.execQuery(sql, engine)
#     print(df)
#
#     # 关闭数据库
#     db.closeConnect(engine)
# ----------------------------------------------