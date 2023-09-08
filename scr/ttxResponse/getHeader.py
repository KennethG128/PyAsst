# 添加项目路径
import sys
sys.path.append('d:/PyAsst')
import lib.PyAsstLog as log
import shutil
import xml.etree.ElementTree as et
import sys
import os



if __name__ == '__main__':

    # 读取XML
    # xml_file = 'D:/PyAsst/autoTask/waiting/2023-09-04 18#00#00_获取250header.xml'
    # xml_sid = '1'
    xml_file = sys.argv[1]
    xml_sid = sys.argv[2]
    et_tree = et.parse(xml_file)
    sourceheader_path = et_tree.find('.//script[@sid="' + xml_sid + '"]/sourceHeader').text


    # 定位路径
    cur_path = os.path.abspath(os.path.dirname(__file__))
    data_path = cur_path[:cur_path.find("PyAsst")] + 'PyAsst/data/ttxResponse/'
    header_path = data_path + 'header/'

    # 获取header
    try:
        shutil.copy(sourceheader_path + 'Post_LD.properties', header_path + 'Post_LD.properties')
        log.info('已获取250post LD header文件')
        shutil.copy(sourceheader_path + 'Post_GW.properties', header_path + 'Post_GW.properties')
        log.info('已获取250post GW header文件')
        shutil.copy(sourceheader_path + 'Post_CPD.properties', header_path + 'Post_CPD.properties')
        log.info('已获取250post CPD header文件')
        shutil.copy(sourceheader_path + 'Post_LRLCPD.properties', header_path + 'Post_LRLCPD.properties')
        log.info('已获取250post LRL header文件')
    except Exception as err:
        log.info(err)