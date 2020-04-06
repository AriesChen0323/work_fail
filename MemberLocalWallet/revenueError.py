import feather as ft
import os
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


nextDate = pd.datetime.strftime(pd.datetime.today().date(), '%Y-%m-%d')
date = pd.datetime.strftime(pd.datetime.strptime(nextDate, '%Y-%m-%d') - pd.Timedelta('1 days'), '%Y-%m-%d')

# 所需資料 ----------------------------------------------------------------------

path = '/home/interview/share/data_prod/'
pathMemberWallet = '/home/interview/share/data_prod/MemberWallets/'
pathMemberAccountLog = '/home/interview/share/data_prod/MemberAccountLog/'

# 地方錢包金額
MemberWallets = ft.read_dataframe(pathMemberWallet + 'MemberWalletsFlow' + nextDate.replace('-', '') + '.ft')

# 中心錢包金額
MemberAccount = pd.DataFrame()
MemberAccountList = os.listdir(pathMemberAccountLog)
for name in MemberAccountList:
    if '.ft' in name:        
        # 2020年02月後才需要，地方錢包金額是從 2020/2/19 之後開始打
        if (int(name[-5:-3]) > 1) & (int(name[-9:-5]) >= 2020):
            MemberAccount = MemberAccount.append(ft.read_dataframe(pathMemberAccountLog + name))

# 當天中心錢包變化資料
df = MemberAccount.query('AddTime >= @date & AddTime < @nextDate')
df.Type = df.Type.str.lower()            

# 流水
gameSummary = ft.read_dataframe(path + 'game_daily_summary.ft')
member_profile = ft.read_dataframe(path + 'member_profile.ft')

# 函數 ----------------------------------------------------------------------

def renameFuc(df, colOldName, colNewName):
    return df.rename(columns={colOldName:colNewName})

# 各種金額統計 ----------------------------------------------------------------------

# 存
MemberDepWitFlow = df.query('Type.isin(["thirdpayment", "payment-adjustment", "payment to deposit"]) & Amount > 0').groupby('MemberId').Amount.sum().reset_index()
MemberDepWitFlow = renameFuc(MemberDepWitFlow, 'Amount', 'Deposit')
# 提
MemberDepWitFlow = MemberDepWitFlow.merge(df.query('Type.isin(["payment-withdrawal", "payment-adjustment"]) & Amount < 0').groupby('MemberId').Amount.sum().reset_index(), on = 'MemberId', how = 'outer')
MemberDepWitFlow = renameFuc(MemberDepWitFlow, 'Amount', 'Withdrawal')
# 其他
MemberDepWitFlow = MemberDepWitFlow.merge(df.query('Type.isin(["affiliatewallettrans", "payment-batchadjuest"])').groupby('MemberId').Amount.sum().reset_index(), on = 'MemberId', how = 'outer')
MemberDepWitFlow = renameFuc(MemberDepWitFlow, 'Amount', 'batchAdjuest')
# 地方錢包金額
localSum = MemberWallets.query('Balance > 0').groupby('MemberId').Balance.sum().reset_index()
localSum = renameFuc(localSum, 'Balance', 'localSum')
MemberDepWitFlow = MemberDepWitFlow.merge(localSum, on = 'MemberId', how = 'outer')
# 輸贏
winLoss = gameSummary.query('date == @date').groupby('member_id').gross_revenue_payout_time.sum().reset_index()
winLoss = renameFuc(winLoss, 'gross_revenue_payout_time', 'winLoss')
winLoss = renameFuc(winLoss, 'member_id', 'MemberId')
MemberDepWitFlow = MemberDepWitFlow.merge(winLoss, on = 'MemberId', how = 'outer')
# 原始中心錢包金額
# 中心錢包變動紀錄
MemberAccount['targetDate'] = pd.datetime.strptime(date, '%Y-%m-%d')
MemberAccount['diff'] = abs(MemberAccount['targetDate'] - MemberAccount['AddTime'])
MemberAccount['中心錢包餘額'] = MemberAccount['OldBalance']
MemberAccount.loc[MemberAccount.AddTime < date, '中心錢包餘額'] = MemberAccount.loc[MemberAccount.AddTime < date, 'CurrentBalance']
closestLog = MemberAccount.groupby('MemberId')['diff'].min().reset_index()
closestLog['isClosest'] = 1
MemberAccount = MemberAccount.merge(closestLog, on = ['MemberId', 'diff'], how = 'left')
startCenterAccount = MemberAccount.query('isClosest == 1').groupby('MemberId')['中心錢包餘額'].min().reset_index()
MemberDepWitFlow = MemberDepWitFlow.merge(startCenterAccount, on = 'MemberId', how = 'outer')

# 整理 ----------------------------------------------------------------------

MemberDepWitFlow = MemberDepWitFlow.fillna(0)
MemberDepWitFlow['depWitDiff'] = MemberDepWitFlow.Deposit + MemberDepWitFlow.Withdrawal 
MemberDepWitFlow['dwWinLossDiff'] = MemberDepWitFlow.depWitDiff - MemberDepWitFlow.winLoss
member_profile['MemberId'] = member_profile['member_id']
# Top100
MemberDepWitFlow = MemberDepWitFlow.sort_values(by = 'dwWinLossDiff').head(100).merge(member_profile[['MemberId', 'member_name']], on = 'MemberId', how = 'left').reset_index(drop=True)
# rename
MemberDepWitFlow = MemberDepWitFlow[['MemberId', 'member_name', 'Deposit', 'Withdrawal', 'batchAdjuest', 'localSum',
       'winLoss', '中心錢包餘額', 'depWitDiff', 'dwWinLossDiff']].rename(columns={
    'MemberId':'用戶ID', 'member_name':'用戶名稱', 'Deposit':'存款', 'Withdrawal':'提款', 
    'batchAdjuest':'彩金＋返水', 'localSum':'地方錢包餘額',
       'winLoss':'輸贏（公司）', '中心錢包餘額':'中心錢包原有額度', 'depWitDiff':'存提差', 'dwWinLossDiff':'輸贏與存提差'
})

MemberDepWitFlow['日期'] = pd.datetime.strptime(date, '%Y-%m-%d')

# 輸出
revenueError = ft.read_dataframe(path + 'revenueError.ft')
MemberDepWitFlow.append(revenueError).drop_duplicates().reset_index(drop = True).to_feather(path + 'revenueError.ft')
