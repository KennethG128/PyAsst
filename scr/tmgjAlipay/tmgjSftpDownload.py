# 添加项目路径
import sys
sys.path.append('d:/PyAsst')
import lib.PyAsstLog as log
import xml.etree.ElementTree as et
import lib.PyAsstFtp




if __name__ == '__main__':
    # 读取XML
    xml_file = 'D:/PyAsst/autoTask/archive/XML模版/myaccountFTP下载模版.xml'
    xml_sid = '1'
    # xml_file = sys.argv[1]
    # xml_sid = sys.argv[2]
    et_tree = et.parse(xml_file)


    # 获取店铺id并转换成list
    store_list = et_tree.find('.//script[@sid="' + xml_sid + '"]/storeId').text.split(',')

    # 获取下载文件类型
    filetype = et_tree.find('.//script[@sid="' + xml_sid + '"]/fileType').text

    # 获取文件路径
    backup_path = et_tree.find('.//script[@sid="' + xml_sid + '"]/backupPath').text
    waiting_path = et_tree.find('.//script[@sid="' + xml_sid + '"]/backupPath').text

    # 定义访问的SFTP
    ftp_host_dict = 'TMGJ_SFTP'

    # 遍历店铺ID文件夹
    for store_id in store_list:

        ftp_path = '/home/alipay/download/' + filetype + '/'

        #  配置已下载文件的前缀
        file_pre = filetype + '_'

        #  定义SFTP目录+店铺ID
        ftp_store_path = ftp_path + store_id + "/"

        # 判断备份文件存放位置
        if store_id == '2088041556225206':
            backupfile_path = backup_path + 'DCL_139485_2088041556225206/'
        elif store_id == '2088831954826763':
            backupfile_path = backup_path + 'KS_138013_2088831954826763/'
        elif store_id == '2088831957170360':
            backupfile_path = backup_path + 'LP_138009_2088831957170360/'
        elif store_id == '2088731614630247':
            backupfile_path = backup_path + 'MM_137399_2088731614630247/'
        elif store_id == '2088531448646779':
            backupfile_path = backup_path + 'UD_137082_2088531448646779/'
        elif store_id == '2088141395154502':
            backupfile_path = backup_path + 'TKM_139960_2088141395154502/'
        elif store_id == '2088341494441194':
            backupfile_path = backup_path + 'LRP_141103_2088341494441194/'
        elif store_id == '2088241468759127':
            backupfile_path = backup_path + 'VHY_141040_2088241468759127/'
        elif store_id == '2088241573137346':
            backupfile_path = backup_path + 'LRP_1411031_2088241573137346/'
        elif store_id == '2088531242254103':
            backupfile_path = backup_path + 'NYX_142210_2088531242254103/'
        elif store_id == '2088341192979696':
            backupfile_path = backup_path + 'ITC_142084_2088341192979696/'
        elif store_id == '2088341570348632':
            backupfile_path = backup_path + 'SKC_143297_2088341570348632/'
        else:
            log.info('未能识别出店铺ID')

        # 创建SFTP对象并连接
        myaccount_sftp = lib.PyAsstFtp.sftp(ftp_host_dict)
        myaccount_sftp.sftpConnect()

        # 下载至备份
        myaccount_sftp.sftpDownloadAll(ftp_store_path, backupfile_path, file_pre, 0)

        # 下载至待处理并删除SFTP上原文件,注意is_remove 参数0=不删除, 1=删除
        myaccount_sftp.sftpDownloadAll(ftp_store_path, waiting_path, filetype + '_', is_remove = 0)

        # 关闭SFTP
        myaccount_sftp.sftpClose()

