import requests, MySQLdb
from lxml import etree
import datetime

'''
create table sams_factdata.dm_nature_index_journals (
    name varchar(500) NOT NULL COMMENT '期刊名称',
    db_create_time varchar(500) datetime '数据库插入时间，请勿修改',
    db_modify_time varchar(500) datetime '数据库更新时间，请勿修改',
)
'''

def email_alert(err):
    import smtplib
    from email.mime.text import MIMEText
    from email.utils import formataddr

    msg = MIMEText(err, 'plain', 'utf-8')
    msg['From'] = formataddr(["管理员",'koljahuang@126.com'])     #显示发件人信息
    msg['To'] = formataddr(["kolja",'koljahuang@sjtu.edu.cn'])      #显示收件人信息
    msg['Subject'] = "datas sync err！"      #定义邮件主题
    try:
        server = smtplib.SMTP("smtp.126.com", 25)
        #set_debuglevel(1)可以打印出和SMTP服务器交互的所有信息
        server.set_debuglevel(1)
        #login()方法用来登录SMTP服务器
        server.login('koljahuang@126.com','131438hugo')
        #sendmail()方法就是发邮件，由于可以一次发给多个人，所以传入一个list，邮件正文是一个str，as_string()把MIMEText对象变成str
        server.sendmail('koljahuang@126.com', ['koljahuang@sjtu.edu.cn',], msg.as_string())
        print("邮件发送成功!")
        server.quit()
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")


def insert2mysql(lines):
    with MySQLdb.connect(host='sadb.idc.sjtu.edu.cn', port=13136, passwd='sakyglztSJK114',user='sams', db='sams_factdata') as conn:
        with conn.cursor() as cursor:
            sql = '''insert into dm_nature_index_journals(name) values(%s)'''
            cursor.executemany(sql, lines)
            conn.commit()



if __name__ == '__main__':

    print("今天是：", datetime.datetime.today())

    try:
        resp = requests.get('https://www.nature.com/nature-index/faq')

        content = resp.content.decode()

        html = etree.HTML(content)

        raw_list = html.xpath("//ul[@class='c-faq-journals']//li/text()")

        journals_list = list(filter(lambda x: x.strip(), map(lambda x: x.strip(), raw_list)))

        journals_list = tuple(map(lambda x: (x,), journals_list))

        print(journals_list)

        insert2mysql(journals_list)
    except Exception as e:
        print(e)
        email_alert(str(e))
    else:
        raw_list=[]
        if not raw_list:
            email_alert('err: no journals list crawled')

