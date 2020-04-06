import feather as ft
import os
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
import datetime as dt
import utils
import warnings
warnings.filterwarnings('ignore')
# sns.set(style = "darkgrid")



# 指定日期
# SpecifyDate = '2020-03-10'
SpecifyDate = pd.datetime.strftime(pd.datetime.today().date() - pd.Timedelta('1 days'), '%Y-%m-%d')

# 新註冊用戶必須補上昨日註冊時間
# 前一天日期，字串，格式：%Y-%m-%d
lastOneDay = pd.datetime.strftime(pd.datetime.strptime(SpecifyDate, '%Y-%m-%d') - pd.Timedelta('1 days'), '%Y-%m-%d')
# 本應是前一天，但怕這樣會誤導不知是何時的錢包餘額，所以改成前兩天的 23:59:59
lastTwoDay = pd.datetime.strftime(pd.datetime.strptime(SpecifyDate, '%Y-%m-%d') - pd.Timedelta('2 days'), '%Y-%m-%d')

path = '/home/interview/share/data_prod/MemberWallets/'
MemberWallets = ft.read_dataframe(path + 'MemberWalletsFlow' + lastOneDay.replace('-', '') + '.ft')

query = ''' SELECT ProductId, Name
            FROM [LckBaseDB].[dbo].[Product] with(nolock) '''
productId = utils.read_sql('IDC_ALL', 'LckBaseDB', query)
productId = productId.query('~(ProductId.isin([4, 6, 8, 10, 11, 13, 16, 18, 24, 30]))')

path = '/home/interview/share/data_prod/'
gameSummary = ft.read_dataframe(path + 'game_daily_summary.ft')
member_profile = ft.read_dataframe(path + 'member_profile.ft')




def rawDataAppendNewRegMember(SpecifyDate, lastOneDay, member_profile, MemberWallets, productId):
    
    #  要補的用戶名單
    allMember = list(MemberWallets.MemberId.unique())
    lossMember = set(member_profile.query('reg_date >= @lastOneDay & reg_date < @SpecifyDate').member_id) - set(allMember)
    
    # 補上所有 ProductId 的資料
    appendDf = pd.DataFrame()
    allProductId = set(productId.ProductId) - set([19])
    allProductIdLen = len(allProductId)
    for ProductId in allProductId:
        appendDf = appendDf.append(
            pd.DataFrame({'MemberId':list(lossMember),
                  'ProductId':np.repeat(ProductId, len(lossMember)),
                  'CreateTime':np.repeat(pd.datetime.strptime(lastOneDay + " 00:00:00", '%Y-%m-%d %H:%M:%S'), len(lossMember)),
                  'UpdateTime':np.repeat(pd.datetime.strptime(lastOneDay + " 00:00:00", '%Y-%m-%d %H:%M:%S'), len(lossMember)),
                  'Balance':np.repeat(0, len(lossMember))
                 })
        )
        
    # 合併資料
    MemberWallets = MemberWallets.append(appendDf)
    
    return MemberWallets.reset_index(drop=True)


def calLocalWalletChangeBySummaryStake(SpecifyDate, lastOneDay, lastTwoDay, MemberWallets, productId, gameSummary):

    # MemberAccount Mon
    MemberAccountYM = pd.datetime.strftime(pd.datetime.strptime(SpecifyDate, '%Y-%m-%d') - pd.Timedelta('1 days'), '%Y-%m-%d')[:7].replace('-', '')
    
    BalanceUpDateTime = pd.datetime.strptime(lastTwoDay + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    MemberWallets.BalanceUpDateTime = MemberWallets.BalanceUpDateTime.fillna(BalanceUpDateTime)
    # 去除不必要的欄位
    MemberWallets = MemberWallets[['Balance2', 'BalanceUpDateTime', 'CreateTime', 'MemberId', 'ProductId']]
    # 新註冊用戶必須補上地方錢包餘額：0
    MemberWallets.Balance2 = MemberWallets.Balance2.fillna(0)
    MemberWallets = MemberWallets.rename(columns = {'Balance2':'Balance'})
    # 挑選需要的 MemberAccountLog
    name = 'MemberAccountLog' + MemberAccountYM

    MemberAccount = ft.read_dataframe('../../share/data_prod/MemberAccountLog/' + name +'.ft')
    
    MemberAccount = MemberAccount.query('AddTime >= @lastOneDay & AddTime < @SpecifyDate')
    MemberAccount['isMainTo'] = MemberAccount['Type'].apply(lambda x:x.replace('main to ', '') if 'main to' in x else 0)
    MemberAccount['isToMain'] = MemberAccount['Type'].apply(lambda x:x.replace(' to main', '') if ' to main' in x else 0)
    
    productId['lowerName'] = productId.Name.apply(lambda x:x.lower() if x != 'PG' else x)
    productId.loc[productId.lowerName == '188casino', 'lowerName'] = 'grandsuite'
    productId.loc[productId.lowerName == 'lbkeno', 'lowerName'] = 'keno'
    productId.loc[productId.lowerName == 'mscasino', 'lowerName'] = 'msclub'
    productId.loc[productId.lowerName == '188sport', 'lowerName'] = 'sportsbook'
    productId = productId.query('~(Name == "IMESport")')
    
    mapToGameSummary = {
        'aggame':['AsiaGameAgin', 'AsiaGameHunter', 'AsiaGameXin', 'AsiaGameYoplay'],
        'gdcasino':['GoldDeluxCasino'], 
        'immwgfish':['IMMegaWinFish'], 
        'keno':['LibiancKeno'],
        'grandsuite':['NewCasino188'],
        'sportsbook':['NewSport188'], 
        'owsport':['OWSportNumberGame', 'OWVirtualSport', 'OWSport'],
        'PG':['PGSlot'],
        'blcard':['BLCard'],
        'bwesport':['BWESport'],
        'vrlotto':['VRLotto'],
        'tcglotto':['TCGLotto'],
        'ptslot':['PTSlot'],
        'ppslot':['PPSlot'],
        'msclub':['MSCasino'],
        'keno':['LibiancKeno'],
        'kgkeno':['KGKeno'],
        'imsport':['IMSport', 'IMESport'],
        'imsglotto':['IMSGLotto'],
        'imsgcard':['IMSGCard'],
        'cqslot':['CQSlot'],
        'ebetcasino':['EBETCasino'],
        'hlcard':['HLCard'],
        'imcasino':['IMCasino'],
        'imkycard':['IMKYCard'],
        'imlycard':['IMLYCard'],
        'immtcard':['IMMTCard']
    }
    
    # 避免有新的遊戲
    while len(mapToGameSummary.keys()) < productId.shape[0]:
        for gameName in set(productId.lowerName) - set(mapToGameSummary.keys()):
            mapToGameSummary[gameName] = list(productId.loc[productId.lowerName == gameName, 'Name'])
    # 反對應        
    invMapToGameSummary = {}
    for key, values in mapToGameSummary.items():
        for value in values:
            invMapToGameSummary[value] = key
            
    # 把 dict 改成 List
    gameTypeName = []
    gameProductType = []
    for game in mapToGameSummary.keys():
        for productType in mapToGameSummary[game]:
            gameTypeName.append(game)
            gameProductType.append(productType)

    productMatch = pd.DataFrame({'lowerName':gameTypeName, 'matchName':gameProductType})
    productMatch = productMatch.merge(productId[['ProductId', 'lowerName']], on = 'lowerName', how = 'left')        
    
    gameSummary = gameSummary.query('date >= @lastOneDay & date < @SpecifyDate')
    gameSummary = gameSummary.merge(productMatch[['matchName', 'ProductId']], left_on = 'product_type', right_on = 'matchName', how = 'left')
    gameSummary = gameSummary.rename(columns={'member_id':'MemberId'})
    # IMESport 與 IMSport 共用錢包
    where = gameSummary['ProductId'] == 19
    gameSummary.loc[where, 'matchName'] = 'IMSport'
    gameSummary.loc[where, 'ProductId'] = 17
    
    # 開始計算變化
    gameSummary.gross_revenue_payout_time = gameSummary.gross_revenue_payout_time * -1
    gameChange = gameSummary.groupby(['ProductId', 'MemberId']).gross_revenue_payout_time.sum().reset_index()
    
    # 
    MemberAccount = MemberAccount.query('isMainTo != 0 | isToMain != 0')
    MemberAccount = MemberAccount.merge(productId[['lowerName', 'ProductId']].rename(columns={'lowerName':'isMainTo', 'ProductId':'isMainToProductId'}),
                          on = 'isMainTo', how = 'left')
    MemberAccount = MemberAccount.merge(productId[['lowerName', 'ProductId']].rename(columns={'lowerName':'isToMain', 'ProductId':'isToMainProductId'}),
                          on = 'isToMain', how = 'left')
    MemberWallets = MemberWallets.merge(productId[['lowerName', 'ProductId']],
                                        on = 'ProductId', how = 'left')
    
    # 
    def choseNotNa(x):
        if x[0]:
            return x[0]
        else:
            return x[1]

    MemberAccount['lowerName'] = MemberAccount[['isToMain', 'isMainTo']].apply(choseNotNa, axis=1)

    toProduct = MemberAccount.query('isMainTo != 0').groupby(['isMainToProductId', 'MemberId'])['Amount'].sum().reset_index()
    toProduct.Amount = toProduct.Amount * -1
    toMain = MemberAccount.query('isToMain != 0').groupby(['isToMainProductId', 'MemberId'])['Amount'].sum().reset_index()
    toMain.Amount = toMain.Amount * -1

    gameChange = gameChange.merge(toProduct, left_on = ['ProductId', 'MemberId'], right_on = ['isMainToProductId', 'MemberId'], how = 'outer')
    gameChange = gameChange.merge(toMain, left_on = ['ProductId', 'MemberId'], right_on = ['isToMainProductId', 'MemberId'], how = 'outer')
    gameChange = gameChange.fillna(0)
    gameChange['changeTotal'] = gameChange.gross_revenue_payout_time + gameChange.Amount_x + gameChange.Amount_y


    def choseNotZero(x):
        if x[0] != 0:
            return x[0]
        elif x[1] != 0:
            return x[1]
        else:
            pass

    gameChange.loc[gameChange.ProductId == 0, 'ProductId'] = gameChange[gameChange.ProductId == 0][['isMainToProductId', 'isToMainProductId']].apply(choseNotZero, axis=1)
    gameChange = gameChange.groupby(['ProductId', 'MemberId'])['gross_revenue_payout_time', 'isMainToProductId', 'Amount_x',
                                                               'isToMainProductId', 'Amount_y', 'changeTotal'].agg({
                                                               'gross_revenue_payout_time':'sum', 'isMainToProductId':'min', 'Amount_x':'sum',
                                                               'isToMainProductId':'min', 'Amount_y':'sum', 'changeTotal':'sum'}).reset_index()    
    
    MemberWallets = MemberWallets.merge(gameChange[['MemberId', 'changeTotal', 'ProductId']], on = ['MemberId', 'ProductId'], how = 'left')
    # 處理有些沒有異動的地方錢包
    MemberWallets['changeTotal'] = MemberWallets['changeTotal'].fillna(0)
    MemberWallets['Balance2'] = MemberWallets['Balance'] + MemberWallets['changeTotal']
    MemberWallets['BalanceUpDateTime'] = pd.datetime.strptime(lastOneDay + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
    
    MemberWallets.reset_index(drop=True).to_feather('../../share/data_prod/MemberWallets/' + 'MemberWalletsFlow' + SpecifyDate.replace('-', '') + '.ft')
    
    
MemberWallets = rawDataAppendNewRegMember(SpecifyDate, lastOneDay, member_profile, MemberWallets, productId)
calLocalWalletChangeBySummaryStake(SpecifyDate, lastOneDay, lastTwoDay, MemberWallets, productId, gameSummary)