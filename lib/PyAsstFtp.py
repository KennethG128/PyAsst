import configparser
from lib.PyAsstAes import PrpCrypt
import os
import paramiko
import stat
import lib.PyAsstLog as log


class sftp:

    def __init__(self, host_dict):

        # 获取ftp.ini文件
        cur_path = os.path.abspath(os.path.dirname(__file__))
        config_path = cur_path[:cur_path.find("PyAsst")] + 'PyAsst/conf/'
        cf = configparser.ConfigParser()
        cf.read(config_path + 'ftp.ini', encoding='utf-8')

        self.host = cf.get(host_dict, "host")
        self.port = int(cf.get(host_dict, "port"))
        self.user = cf.get(host_dict, "user")

        # password解密
        pc = PrpCrypt('1', '2')
        self.password = pc.decrypt(cf.get(host_dict, "password"))

    def sftpConnect(self):

        transport = paramiko.Transport((self.host, self.port))
        transport.connect(username=self.user, password=self.password)
        log.info('FTP连接成功')
        self.__transport = transport

    def sftpClose(self):
        self.__transport.close()
        log.info('FTP关闭成功')

    def sftpGetDir(self, sftp_path, is_folder):
        """
           :param sftp_path: FTP路径
           :param is_folder: 判断是否只查文件夹
           :return reslut_list:  列出每个文件名，清单格式
        """
        reslut_list = []
        try:
            sftp = paramiko.SFTPClient.from_transport(self.__transport)
            filesAttr = sftp.listdir_attr(sftp_path)
            for fileAttr in filesAttr:
                if is_folder == 1:
                    if stat.S_ISDIR(fileAttr.st_mode):  # 判断是否文件夹
                        reslut_list.append(fileAttr.filename)
                else:
                    reslut_list.append(fileAttr.filename)

            return reslut_list

        except Exception as err:
            log.info(err)



    def sftpDownloadAll(self, sftp_path, local_path, file_pre='', is_remove=0):
        """
           :param file_pre: 默认空,添加在已下载文件名前缀
           :param is_remove: 默认不删除文件, 0不删除,1删除
        """

        sftp = paramiko.SFTPClient.from_transport(self.__transport)

        try:
            # 获取sftp指定目录内所有文件信息
            listfile = sftp.listdir(sftp_path)
            for file in listfile:
                if os.path.exists(local_path + file_pre + file):
                    log.info('文件已存在: ' + local_path + file_pre + file)
                else:
                    sftp.get(sftp_path + file, local_path + file_pre + file)
                    log.info('sftp文件已下载: ' + sftp_path + file + ' =====> ' + local_path + file_pre + file)

                    #  删除文件
                    if is_remove == 1:
                        sftp.remove(sftp_path + file)
                        log.info('sftp文件已删除: ' + sftp_path + file)
            else:
                log.info('sftp文件不存在')
        except Exception as err:
            log.info(err)
            log.info('sftp没有找到目录: %s' % sftp_path)

# ----------------------------------------------
#
# if __name__ == '__main__':
#
#     # 配置路径
#     Download_path = 'd:/PyAsst/'
#     ftp_path = "/SAP/ALIPAY/"
#
#     # 配置ftp名
#     ftp_host_dict = 'SAP_OMS'
#
#     # 创建ftp对象
#     sftp_class = sftp(ftp_host_dict)
#
#     # 连接ftp
#     sftp_class.sftpConnect()
#
#     # 下载整个文件夹的文件
#     sftp_class.sftpDownloadAll(ftp_path, Download_path)
#
#     # 关闭ftp连接
#     sftp_class.sftpClose()
# ----------------------------------------------
