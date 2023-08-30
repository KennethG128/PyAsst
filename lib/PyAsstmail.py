import lib.PyAsstLog as log
from lib.PyAsstAes import PrpCrypt
import os
import configparser
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import email
import imaplib
from email.header import decode_header
from email.header import Header



class mail:

    def __init__(self, mail_dict):
        # 获取mail.ini文件
        cur_path = os.path.abspath(os.path.dirname(__file__))
        config_path = cur_path[:cur_path.find("PyAsst")] + 'PyAsst/conf/'
        cf = configparser.ConfigParser()
        cf.read(config_path + 'ftp.ini', encoding='utf-8')
        mail_file = configparser.ConfigParser()
        mail_file.read(os.path.join(config_path, 'mail.ini'), encoding='utf-8')
        self.mail = mail_dict
        # password解密
        pc = PrpCrypt('1', '2')
        self.password = pc.decrypt(mail_file.get(mail_dict, 'password'))
        self.smtp_host = mail_file.get(mail_dict, 'smtp_host')
        self.smtp_port = mail_file.get(mail_dict, 'smtp_port')
        self.imap_host = mail_file.get(mail_dict, 'imap_host')
        self.imap_port = mail_file.get(mail_dict, 'imap_port')


    def sandMail(self, mail_receivers, mail_attachment, mail_send_title, mail_send_text):

        # 邮件内容配置
        message = MIMEMultipart()
        message['From'] = self.mail
        message['To'] = ','.join(mail_receivers)
        message['Subject'] = Header(mail_send_title, 'utf-8')

        # 邮件正文内容
        message.attach(MIMEText(mail_send_text, 'plain', 'utf-8'))

        # 附件配置----------------
        (path, file) = os.path.split(mail_attachment)
        with open(mail_attachment, 'rb') as f:
            content = f.read()

        ma = MIMEText(content, 'base64', 'utf-8')
        ma['Content-Disposition'] = 'attachment;filename="'+file+'"'
        ma.add_header('Content-Disposition', 'attachment', filename=file)
        # 添加附件
        message.attach(ma)


        # 发送邮件
        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(self.smtp_host, self.smtp_port)
            log.info("连接 {0}:{1} 成功".format(self.smtp_host, self.smtp_port))
            smtpObj.login(self.mail, self.password)
            log.info("登录 {0} 成功".format(self.mail))
            smtpObj.sendmail(self.mail, message['To'].split(','), message.as_string())
            log.info('附件 %s 发送成功' % mail_attachment)
            smtpObj.quit()
        except smtplib.SMTPException as err:
            log.info('邮件发送失败 %s' % err)

    def imapLogin(self):
        try:
            self.imap_conn = imaplib.IMAP4(self.imap_host,self.imap_port)
            log.info("连接 {0}:{1} 成功".format(self.imap_host, self.imap_port))
            self.imap_conn.login(self.mail, self.password)
            log.info("登录 {0} 成功".format(self.mail))
        except BaseException as e:
            log.info("连接 {0}:{1} 失败 %s".format(self.mail, self.imap_port) % e)

    def getMailList(self, mail_path, mail_flag):
        """
           输入邮件文件夹路径和邮件标记(UNSEEN\ALL\SEEN)遍历邮件,返回邮件ID list
        """
        self.imap_conn.select(mail_path)
        mail_list_count = len(self.imap_conn.search(None, mail_flag)[1][0].split())
        mail_list = self.imap_conn.search(None, mail_flag)[1][0].split()

        # mail_list 转码成ID
        mail_list_id = []
        for row in mail_list:
            mail_list_id.append(row.decode('utf-8'))

        return mail_list_id, mail_list_count

    def mailTitle(self, mail_id):
        """
           输入邮件ID,返回标题字符串
        """
        typ, email_content = self.imap_conn.fetch(mail_id.encode(), '(RFC822)')
        mail_text = email_content[0][1]
        msg = email.message_from_bytes(mail_text)
        subject = msg['Subject']
        subdecode = decode_header(subject)
        mail_title = subdecode[0][0]
        try:
            mail_title = mail_title.decode(subdecode[0][1],errors='ignore')
        except Exception as err:
            log.info(err)
        return mail_title

    def mailFromAdd(self, mail_id):
        """
           输入邮件ID,返回发件邮箱地址
        """
        typ, email_content = self.imap_conn.fetch(f'{mail_id}'.encode(), '(RFC822)')
        mail_text = email_content[0][1]
        msg = email.message_from_bytes(mail_text)
        email_from = msg['From']
        from_decode = decode_header(email_from)
        mail_from_add = from_decode[0][0]
        return mail_from_add

    def mailAttSave(self, mail_id, download_path, att_id):
        """
           输入邮件ID和保存附件路径,下载邮件的全部附件
        """
        type_, data = self.imap_conn.fetch(mail_id, '(RFC822)')
        msg = email.message_from_string(data[0][1].decode('utf-8'))
        for part in msg.walk():
            file_name = part.get_filename()
            if file_name:
                h = email.header.Header(file_name)
                dh = email.header.decode_header(h)
                filename = dh[0][0]
                if dh[0][1]:
                    # 字符编码转换
                    value, charset = decode_header(str(filename, dh[0][1]))[0]
                    if charset:
                        filename = value.decode(charset)
                data = part.get_payload(decode=True)

                # 判断filename是否为字符串类型
                if (isinstance(filename, str)):
                    filename = att_id + '_' + filename
                    pass
                else:
                    filename = att_id + '_' + str(filename, encoding="utf-8")

                filepath = os.path.join(download_path, filename)

                try:
                    att_file = open(filepath, 'wb')
                    log.info('附件保存成功 ' + filepath)
                    att_file.write(data)
                    att_file.close()
                except Exception as err:
                    log.info('附件保存失败')
                    log.info(err)

    def mailSeen(self, mail_id):
        self.imap_conn.store(mail_id, '+FLAGS', '\\Seen')

    def mailMove(self, mail_id, mail_folder):
        # 复制到指定邮箱文件夹
        self.imap_conn.copy(mail_id, mail_folder)
        # 移动到已删除文件夹
        self.imap_conn.store(mail_id, '+FLAGS', '\\Deleted')
        log.info('邮件已移动至 %s' %mail_folder)

    def mailclose(self):
        self.imap_conn.close()
        self.imap_conn.logout()
        log.info('关闭 %s 成功' % self.mail)


# ----------------------------------------------
# if __name__ == '__main__':

    # mail_dict = 'oms.it@buycoor.com'
    # 收件人清单
    # mail_receivers = ['jian.gao@loreal.com']
    # mail_attachment = 'd:/PyAsst/附件1.txt'
    # mail_title = '测试01'
    # mail_text = '正文01'

    # 添加对象 输入发送邮件地
    # m = mail(mail_dict)
    # 发送邮件
    # m.sandMail(mail_receivers, mail_attachment, mail_title, mail_text)

    # # 登录imap
    # m.imapLogin()
    # mail_list_id, mail_list_count = m.getMailList('INBOX', 'UNSEEN')
    # for i in mail_list_id:
    #     mail_title = m.mailTitle(i)
    # # 获取tital
    # mail_title = m.mailTitle(mail_list_id[0])
    # # 获取fromadd
    # mail_fromadd = m.mailFromAdd(mail_list_id[0])
    # # 下载附件
    # m.mailAttSave(mail_list_id[0], 'd:/PyAsst/')
    # # 移动邮件
    # m.mailMove(mail_list_id[0], 'INBOX/archive')
    # log.info(mail_title)
    # log.info(mail_fromadd)
    # # 关闭邮箱连接
    # m.mailclose()

# ----------------------------------------------