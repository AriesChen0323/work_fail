import pandas as pd
from sqlalchemy import create_engine
import requests as rs
import datetime as dt
import logging
import feather
import os
import configparser
import psutil
import time

os.chdir(os.path.split(os.path.realpath(__file__))[0])  # 設定工作目錄為此檔案位置
config = configparser.ConfigParser()
config.read('setting.ini')

status = config['DEFAULT']['STATUS']
slack_report_channel = config[status]['slack_report_channel']
slack_log_channel = config[status]['slack_log_channel']

# 基礎設定
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    handlers=[logging.FileHandler('log/my.log', 'a', 'utf-8')])

# 定義 handler 輸出 sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# 設定輸出格式
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# handler 設定輸出格式
console.setFormatter(formatter)
# 加入 hander 到 root logger
logging.getLogger('').addHandler(console)

# 定義logger
logger_dumpTable = logging.getLogger('dumpTable')
logger_to_slack = logging.getLogger('slack')
logger_utils = logging.getLogger('utils')


def get_logger(tag):
    return logging.getLogger(tag)


def getConfig(section, key):
    config = configparser.ConfigParser()
    path = os.path.split(os.path.realpath(__file__))[0] + '/config.ini'
    config.read(path)
    return config.get(section, key)


def get_data(table_name, path='share'):
    logger_dumpTable.info('GET: ' + table_name + ' start.')
    df = pd.DataFrame()
    for f in os.listdir(path):
        if table_name in f:
            df = df.append(feather.read_dataframe(path + '/' + f))
    df = df.reset_index(drop=True)
    logger_dumpTable.info('GET: ' + table_name + ' finish.')
    return df


def get_memory():
    values = psutil.virtual_memory()
    mem = '\n 記憶體：使用{}GB，OS可用{}GB，APP可用{}GB'.format(values.used >> 30, values.free >> 30, values.available >> 30)
    return mem


def log2slack(msg, to=slack_log_channel, img_url=""):
    mem = get_memory()
    logger_to_slack.info(msg + mem)
    url = ""
    if to == 'log':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'andrew':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'aries':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'may':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'report':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'report_18':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'test':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'report_dev':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'log_dev':
        url = 'https://hooks.slack.com/services/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'
    elif to == 'report_lab':
        url = 'https://hooks.slack.com/services/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'

    rs.post(url,
            data='''{{
        "attachments": [
            {{
                "fallback": "Required plain-text summary of the attachment.",
                "color": "#36a64f",
                "pretext": '{}',
                "image_url": "{}",
            }}
        ]
    }}'''.format(msg.replace("'", '"'), img_url).encode('utf-8'))


def log2telegram(msg, img_url=""):
    import telegram
    logger_to_slack.info(msg)

    bot = telegram.Bot(token='751804411:AAGdUcpwmoOYjCOTa6t8gxnw3JBvSBfLoOQ')
    chat_id = -296752573
    try:
        bot.send_message(chat_id=chat_id, text=msg, timeout=100)
    except telegram.error.TimedOut:
        time.sleep(5)
        bot.send_message(chat_id=chat_id, text=msg, timeout=100)

    if img_url:
        try:
            bot.send_photo(chat_id=chat_id, photo=img_url, timeout=100)
        except telegram.error.TimedOut:
            time.sleep(5)
            bot.send_photo(chat_id=chat_id, photo=img_url, timeout=100)


def get_db(server, db):
    connection_string = {
        'New': 'mssql+pymssql://stake@dbstakeanalyze:Qwer1234@dbstakeanalyze.database.windows.net:1433/{}',
        'Old': 'mssql+pymssql://dataScience@18azuredb:qwer1234azureDB@18azuredb.database.windows.net:1433/{}',
        'IDC': 'mssql+pymssql://dataScience:qwer1234@172.17.0.9:1433/{}',
        'Campaign': 'mssql+pymssql://dataScience@dbs-production:qwer1234!@#5566@dbs-production.database.windows.net:1433/{}',
        'DS': 'mysql+pymysql://root:1qaz2wsx@172.17.0.80:8882/{}?charset=utf8mb4',
        'Log': 'mssql+pymssql://dataScience:qwer12345566@172.17.0.12:1433/{}',
        'Red': 'mssql+pymssql://selectman@db-production-hk:qwer1234!@#@db-production-hk.database.windows.net:1433/{}',
        'Matomo': 'mysql+pymysql://root:EDCrfvTGB@172.17.0.90:3306/{}?charset=utf8',
        'IDC_ALL': 'mssql+pymssql://selectman:Happyicsgame!@#@172.17.0.8:1433/{}',
        'New_F': 'mssql+pymssql://stake@dbstakeread:1qaz@WSX@dbstakeread.database.windows.net:1433/{}',
    }
    engine = create_engine(connection_string[server].format(db), encoding='utf-8')
    return engine


def list_tables(server, db):
    engine = get_db(server, db)
    from sqlalchemy import inspect
    inspector = inspect(engine)
    result = inspector.get_table_names()
    return result


def dump_table_query(server, db, table_name, query, n=1000000, path='share/', prefix=""):
    logger_dumpTable.info('GET: ' + table_name + ' start.')
    engine = get_db(server, db)
    reader = pd.read_sql(query, engine, chunksize=n)
    df = pd.DataFrame()
    for i, chunk in enumerate(reader):
        logger_dumpTable.info(i)
        df = df.append(chunk)

    df.reset_index(drop=True).to_feather('{}/{}{}.ft'.format(path, prefix, table_name))
    print('GET: ', table_name, ' finish.')
    logger_dumpTable.info('GET: ' + table_name + ' finish.')


def dump_table(server, db, table_name, n=1000000, path='share/', prefix="", where=None):
    logger_dumpTable.info('GET: ' + table_name + ' start.')
    engine = get_db(server, db)
    query = "SELECT * FROM [dbo].[{}] ".format(table_name)
    if where:
        query = query + where
    reader = pd.read_sql(query, engine, chunksize=n)
    for i, chunk in enumerate(reader):
        logger_dumpTable.info(i)
        chunk.to_feather('{}/{}{}_{}.ft'.format(path, prefix, table_name, i))
    print('GET: ', table_name, ' finish.')
    logger_dumpTable.info('GET: ' + table_name + ' finish.')


def read_sql(server, db, query):
    engine = get_db(server, db)
    return pd.read_sql(query, engine)


def read_sql_chunk(server, db, query, n=100000):
    engine = get_db(server, db)
    reader = pd.read_sql(query, engine, chunksize=n)
    df = pd.DataFrame()
    for i, chunk in enumerate(reader):
        logger_dumpTable.info(i)
        df = df.append(chunk)
    return df


def get_yesterday(offset=-1):
    date = dt.date.today()
    yesterday = (date + dt.timedelta(days=offset)).strftime("%Y-%m-%d")
    return yesterday


def dtu_launch(action):
    api = {
        'OPEN': "https://s3events.azure-automation.net/webhooks?token=s%2bdHvn%2feWYCVSTWpPoa8j8FlLk2ml7XBjk950ThzSW8%3d",
        'CLOSE': "https://s3events.azure-automation.net/webhooks?token=NZDS4Mreddv8oGo5WRiUaT4TYDSJWgqQgzSo7ot83Dc%3d",
        'S2': "https://s3events.azure-automation.net/webhooks?token=t8bc2xtlxVCAql7MDox16L1KiFK4ql%2f4G%2fnvlsXPPkg%3d",
        'S3': "https://s3events.azure-automation.net/webhooks?token=NZDS4Mreddv8oGo5WRiUaT4TYDSJWgqQgzSo7ot83Dc%3d",
        'S6': "https://s3events.azure-automation.net/webhooks?token=f515bEWmThJZU4sF%2bRlJFBKMBWZv5pWe4w5viHSqoqk%3d",
        'S9': "https://s3events.azure-automation.net/webhooks?token=s%2bdHvn%2feWYCVSTWpPoa8j8FlLk2ml7XBjk950ThzSW8%3d"
    }
    log2slack('DTU API {} 回應 - {}'.format(action, rs.post(api[action]).status_code))


def dtu_check():
    from selenium import webdriver

    options = webdriver.ChromeOptions()
    # 若重開機時要重新檢查Selenium Container的連線IP，進入該Container之後以ifconfig查看
    browser = webdriver.Remote('http://172.18.0.3:4444/wd/hub', options.to_capabilities())
    browser.get(
        'https://portal.azure.com/#@18luck.com/resource/subscriptions/80adb128-7b85-4c27-9849-af04ebec1984/resourceGroups/rg-stake/providers/Microsoft.Sql/servers/dbstakeanalyze/databases/dbFetchStakeMain/overview')
    time.sleep(5)

    browser.find_element_by_name('loginfmt').send_keys('andrew@icsgame.com')
    browser.find_element_by_css_selector('[type="submit"]').click()
    time.sleep(5)

    browser.find_element_by_name('passwd').send_keys('#EDC4rfv%TGB')
    browser.find_element_by_css_selector('[type="submit"]').click()
    time.sleep(5)

    if 'Update your password' in browser.page_source:
        log2slack('取得新流水ft檔 - Azure密碼到期需更換')
        time.sleep(9999)
        return '取得新流水ft檔 - Azure密碼到期需更換'

    browser.find_element_by_css_selector('#idBtn_Back').click()
    time.sleep(5)
    while True:
        if 'Pricing tier' in browser.page_source:
            dtu_status = browser.find_element_by_xpath('//label[text()="Pricing tier"]/../following-sibling::div').text
            if 'DTU' in dtu_status:
                break

    browser.quit()
    return dtu_status
