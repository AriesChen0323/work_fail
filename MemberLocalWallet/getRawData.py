import feather as ft
import pandas as pd
from sqlalchemy import create_engine
import utils
import warnings
import requests as rs
warnings.filterwarnings('ignore')

def log2rocket(msg, to="#tofu_report", img_url=""):
    msg = msg.replace("\t","\\t").replace("\n","\\n")
    rs.post('https://rc.18luck.com/api/v1/chat.postMessage',
            headers={"X-Auth-Token": '4E4T0wLYiqs5lnyiWWpBXrlByhsRpGXTwnp2Dblvnxu',
                     "X-User-Id": 'iJRRFYwdWPNusRRb7',
                     "Content-type": "application/json"},
            data=f'{{ "channel": "{to}", "text": "{msg}"}}'.encode(
                'utf-8'),
            verify=False)

logChannel = '@數據-Aries'    

yesterday = '2020-02-20'
today = '2020-03-07'

query = ''' SELECT [Id]
      ,[MemberId]
      ,[ProductId]
      ,[CreateTime]
      ,[UpdateTime]
      ,[Balance]
  FROM [LckTempDB].[dbo].[MemberWallets] with(nolock)
  where CreateTime between '{}' and '{}' '''.format(yesterday, today)
try:
    log2rocket('撈取地方錢包資料：開始', to=logChannel)
    df = utils.read_sql('IDC_ALL', 'LckTempDB', query)
    log2rocket('撈取地方錢包資料：成功', to=logChannel)
    # 已知錯誤
    log2rocket('修正已知資料錯誤', to=logChannel)
    # 地方錢包金額錯誤
    df.loc[(df.MemberId == 452500) & (df.ProductId == 22), 'Balance'] = 24390
    # 事前下注、事後和局
    df.loc[(df.MemberId == 156895) & (df.ProductId == 7), 'Balance'] += 4400
    # 賭神賽獎金
    df.loc[(df.MemberId == 214590) & (df.ProductId == 7), 'Balance'] += 20000
    # KG個人中彩
    df.loc[(df.MemberId == 185729) & (df.ProductId == 14), 'Balance'] += 40122
    # 輸出
    try:
        path = '../../share/data_prod/MemberWallets/'
        log2rocket('地方錢包資料落檔：開始', to=logChannel)
        df.drop_duplicates('Id').reset_index(drop=True).to_feather(path + 'rawMemberWallets.ft')
        log2rocket('地方錢包資料落檔：成功 \n 排程結束', to=logChannel)
    except Exception as e:
        log2rocket('撈取地方錢包資料：失敗，{}'.format(e), to=logChannel)
except Exception as e:
    log2rocket('撈取地方錢包資料：失敗，{}'.format(e), to=logChannel)



    


