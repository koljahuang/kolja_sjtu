'''
采用datax批量同步，crontab进行同步作业的调度

项目大致流程：
sa生产数据 -> 动态生成目标表 -> sa查询目标表建表ddl --> bi创建目标表(存在会先删) --> 子进程datax同步 --> check --> 删除sa的目标表
以上环境出现异常，邮件告警
'''

import MySQLdb
import subprocess, os, re, datetime,logging
from config import settings

module_logger = logging.getLogger('comparative_analysis')


sa_host = settings.SA_HOST
sa_port = settings.SA_PORT
sa_user = settings.SA_USER
sa_pwd = settings.SA_PWD
bi_host = settings.BI_HOST
bi_port = settings.BI_PORT
bi_user = settings.BI_USER
bi_pwd = settings.BI_PWD


current_absurl = os.path.abspath(__file__)
current_dir = os.path.dirname(current_absurl)

week_list = ["星期一","星期二","星期三","星期四","星期五","星期六","星期日"]
weekday = week_list[datetime.datetime.today().weekday()]

today = datetime.datetime.now().strftime("%Y%m%d")
# today = '20221101'

def sa_query(db, sql):
    conn = MySQLdb.connect(sa_host, port=sa_port, user=sa_user, passwd=sa_pwd, db=db, connect_timeout=5)
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def email_alert(err):
    import smtplib
    from email.mime.text import MIMEText
    from email.utils import formataddr

    msg = MIMEText(err, 'plain', 'utf-8')
    msg['From'] = formataddr(["管理员",f'{settings.SEND_MAIL}'])     #显示发件人信息
    msg['To'] = formataddr(["kolja",'koljahuang@sjtu.edu.cn'])          #显示收件人信息
    msg['Subject'] = "datas sync err！"      #定义邮件主题
    try:
        server = smtplib.SMTP("smtp.126.com", 25)
        #set_debuglevel(1)可以打印出和SMTP服务器交互的所有信息
        server.set_debuglevel(1)
        #login()方法用来登录SMTP服务器
        server.login(f'{settings.SEND_MAIL}',f'{settings.SEND_PWD}')
        #sendmail()方法就是发邮件，由于可以一次发给多个人，所以传入一个list，邮件正文是一个str，as_string()把MIMEText对象变成str
        server.sendmail(f'{settings.SEND_MAIL}', [f'koljahuang@sjtu.edu.cn',], msg.as_string())
        module_logger.info("邮件发送成功!")
        server.quit()
    except smtplib.SMTPException:
        module_logger.error("Error: 无法发送邮件")


def bi_query(db, sql):
    conn = MySQLdb.connect(bi_host, port=bi_port, user=bi_user, passwd=bi_pwd, db=db, connect_timeout=5)
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def run_command(cmd, tbl):
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            # module_logger.info(output)
            err = re.findall(r'ERROR', output)
            if err:
                excpetion = re.findall(r'Exception when.*?\n', output)[0]
                raise Exception(f'{tbl} sync failed cause {excpetion}')
    rc = process.poll()
    return rc


def sync(tbl:str):

    cmd = f'python /opt/modules/datax/bin/datax.py \
        -p "-Dtbl={tbl} \
        -Dsa_host={sa_host} -Dsa_port={sa_port} -Dsa_user={sa_user} -Dsa_pwd={sa_pwd} \
        -Dbi_host={bi_host} -Dbi_port={bi_port} -Dbi_user={bi_user} -Dbi_pwd={bi_pwd}" \
        {current_dir}/datax_rw.json'
    module_logger.info(cmd)
    run_command(cmd, tbl)


def create_table(tbl:str):
    try:
        ddl_create = sa_query('sams_bak', f"show create table {tbl}")[0][1]
    except Exception as e:
        raise e
    else:
        module_logger.info(ddl_create)
        bi_query('sams_bak', f'drop table if exists {tbl}')
        bi_query('sams_bak', ddl_create)


def bi_check(tbl:str) -> bool:
    record_cnt = bi_query('sams_bak', f'select count(1) from {tbl}')[0][0]
    if record_cnt > 0:
        module_logger.info(f"checked: there is {record_cnt} records synced to {tbl} in bi")
        return True
    else:
        raise Exception(f'checked: sync failed cause there is no records synced to {tbl} in bi')


def sa_check(tbl:str) -> bool:
    record_cnt = sa_query('sams_bak', f'select count(1) from {tbl}')[0][0]
    if record_cnt > 0:
        module_logger.info(f"checked: there is {record_cnt} records in sa {tbl}")
        return True


def sa_produce_data():
    module_logger.info('starting sa data producing')
    sa_query('sams_bak', 'call P_Comparative_analysis()')


def is_data_ready(tables: list) -> bool:
    tbls = tables.copy()
    for tbl in tables:
        is_succ = sa_check(tbl)
        if is_succ:
            tbls.remove(tbl)
    if not tbls:
        return True
    else:
        module_logger.warning(f'sa produce data of {tables} with unkonwn problem')


def create_tbl2(tbl, tbl2):
    bi_query('sams_bak', f'drop table if exists {tbl2}')
    sql = f'create table {tbl2} as select * from {tbl}'
    module_logger.info(sql)
    bi_query('sams_bak', sql)


def start_sync():
    # tables limit
    tables = [
                'comparative_analysis_xm_res',
                'comparative_analysis_qklw_res',
                'comparative_analysis_ks_res',
                'comparative_analysis_dc_res',
                'comparative_analysis_bysj_res',
            ]
    tables = list(map(lambda x: x + '_' + f'{today}', tables))
    # execute day limit
    # if weekday in ["星期一","星期二","星期三","星期四","星期五","星期六","星期日"]:
    if weekday in ["星期五"]:
        sa_produce_data()
        if is_data_ready(tables):
            for tbl in tables:
                tbl2 = tbl.replace(f'_{today}', '')
                try:
                    create_table(tbl)
                    sync(tbl)
                    create_tbl2(tbl, tbl2)
                    is_succ = bi_check(tbl2)
                    if is_succ:
                        sa_query('sams_bak', f'drop table if exists {tbl}')
                except Exception as e:
                    module_logger.error(e)
                    email_alert(str(e.args))
                else:
                    module_logger.info(f'{tbl} sync succ')
        else:
            email_alert('sa produce data with unkonwn problem。pls check sams.T_Record_Logs in sa')
    else:
        module_logger.info(f'今日{weekday}, 无事，退朝！')