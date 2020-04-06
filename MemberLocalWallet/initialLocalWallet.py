import feather as ft
import os
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
import datetime as dt
import re
import utils
import warnings
warnings.filterwarnings('ignore')

# 重跑到哪天為止（不含）
# 必須為 2020-03-07 或之後，因為地方錢包補打是在 2020-03-06 結束
SpecifyDate = '2020-03-07'


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


pathProd = '../../share/data_prod/'
pathMemberWallet = '../../share/data_prod/MemberWallets/'
pathMemberAccountLog = '../../share/data_prod/MemberAccountLog/'

member_profile = ft.read_dataframe(pathProd + 'member_profile.ft')
# 所有遊戲流水（為了處理『獲取地方錢包金額前下注，且於獲取地方錢包後派彩的問題』）
gameAll = ft.read_dataframe(pathProd + 'game_all_in_one_begin20191101.ft').query('PayoutTime >= "2020-02-15" & PayoutTime < @SpecifyDate')
# 玩家地方錢包資料（已處理新註冊用戶的資料）
MemberWallets = ft.read_dataframe(pathMemberWallet + 'rawMemberWallets.ft')

# 中心錢包變動紀錄
MemberAccount = pd.DataFrame()
MemberAccountList = os.listdir(pathMemberAccountLog)
for name in MemberAccountList:
    if '.ft' in name:        
        # 2020年02月後才需要，地方錢包金額是從 2020/2/19 之後開始打
        if (int(name[-5:-3]) > 1) & (int(name[-9:-5]) >= 2020):
            MemberAccount = MemberAccount.append(ft.read_dataframe(pathMemberAccountLog + name))
MemberAccount = MemberAccount.query('AddTime < @SpecifyDate')
            
    
# 遊戲代號資料
query = ''' SELECT ProductId, Name
            FROM [LckBaseDB].[dbo].[Product] with(nolock) '''
productId = utils.read_sql('IDC_ALL', 'LckBaseDB', query)
productId = productId.query('~(ProductId.isin([4, 6, 8, 10, 11, 13, 16, 18, 24, 30]))')
# 技術債，過去的命名方式有與遊戲名稱並不統一
productId['lowerName'] = productId.Name.apply(lambda x:x.lower() if x != 'PG' else x)
productId.loc[productId.lowerName == '188casino', 'lowerName'] = 'grandsuite'
productId.loc[productId.lowerName == 'lbkeno', 'lowerName'] = 'keno'
productId.loc[productId.lowerName == 'mscasino', 'lowerName'] = 'msclub'
productId.loc[productId.lowerName == '188sport', 'lowerName'] = 'sportsbook'

def rawDataAppendNewRegMember(member_profile, MemberWallets, productId, SpecifyDate):
    
    #  要補的用戶名單
    allMember = list(MemberWallets.MemberId.unique())
    lossMember = set(member_profile.query('reg_date >= "2020-02-15" & reg_date < @SpecifyDate').member_id) - set(allMember)
    
    # 補上所有 ProductId 的資料
    appendDf = pd.DataFrame()
    allProductId = set(productId.ProductId) - set([19])
    allProductIdLen = len(allProductId)
    for ProductId in allProductId:
        appendDf = appendDf.append(
            pd.DataFrame({'MemberId':list(lossMember),
                  'ProductId':np.repeat(ProductId, len(lossMember)),
                  'CreateTime':np.repeat(pd.datetime.strptime("2020-02-15 00:00:00", '%Y-%m-%d %H:%M:%S'), len(lossMember)),
                  'UpdateTime':np.repeat(pd.datetime.strptime("2020-02-15 00:00:00", '%Y-%m-%d %H:%M:%S'), len(lossMember)),
                  'Balance':np.repeat(0, len(lossMember))
                 })
        )
        
    # 合併資料
    MemberWallets = MemberWallets.append(appendDf)
    
    return MemberWallets.reset_index(drop=True)

def adjustIMSportWallet(SpecifyDate):
    
    member_profile = ft.read_dataframe('../../share/data_prod/' + 'member_profile.ft')
    MemberWallets = ft.read_dataframe('../../share/data_prod/MemberWallets/' + 'MemberWalletsFlow' + SpecifyDate.replace('-', '') + '.ft')

    # 撈取 IM 資料    
    IMBetRecord = pd.DataFrame()

    startList = ['2020-02-15', '2020-02-18', '2020-02-21', '2020-02-24', '2020-02-28', '2020-03-02', '2020-03-06']
    endList = ['2020-02-18', '2020-02-21', '2020-02-24', '2020-02-28', '2020-03-02', '2020-03-06', '2020-03-07']

    for start, end in zip(startList, endList):

        query = ''' SELECT [BetTime]
                          ,[PlayerName]
                          ,[BetAmount]
                          ,[NetAmount]
                          ,[PayoutTime]
                      FROM [dbo].[IMSportBetRecord] with(nolock)
                      where BetTime between '{}' and '{}'
                        and NetAmount = 0
                        and IsSettled = 1  '''.format(start, end)
        IMBetRecord = IMBetRecord.append(utils.read_sql('New', 'dbFetchStakeMain', query))


    # 需要 MemberId 來與 MemberWallets 合併，取得 地方錢包餘額 的呼叫時間
    IMBetRecord['lowerName'] = IMBetRecord.PlayerName.str.lower()
    IMBetRecord = IMBetRecord.merge(member_profile[['member_name', 'member_id']].rename(columns={'member_name':'lowerName', 'member_id':'MemberId'}), on = 'lowerName', how = 'left')
    IMBetRecord = IMBetRecord.merge(MemberWallets.query('ProductId == 17')[['MemberId', 'CreateTime']], on = 'MemberId', how = 'left')
    
    # 有些用戶並沒有在名單內（1年未上線回鍋不在要計算地方錢包餘額的名單內）
    IMBetRecord = IMBetRecord[~IMBetRecord.CreateTime.isna()]
    
    # 調整時差至 UTC+8
    IMBetRecord['BetTime'] += pd.Timedelta('8 hours')
    IMBetRecord['PayoutTime'] += pd.Timedelta('8 hours')
    
    # 需要的資料（取地方錢包餘額前用戶下注，取完後和局或取消局退回流水的資料）
    IMBetRecordNeedAdd = IMBetRecord.query('CreateTime >= BetTime & CreateTime < PayoutTime').groupby('MemberId').BetAmount.sum().reset_index()
    
    # 更新地方錢包金額
    for index in IMBetRecordNeedAdd.index:
        member = IMBetRecordNeedAdd.loc[index, 'MemberId']
        amount = IMBetRecordNeedAdd.loc[index, 'BetAmount']
        MemberWallets.loc[(MemberWallets.MemberId == member) & (MemberWallets.ProductId == 17), 'Balance'] += amount
        
    # 輸出更新資料
    MemberWallets.reset_index(drop=True).to_feather('../../share/data_prod/MemberWallets/' + 'MemberWalletsFlow' + SpecifyDate.replace('-', '') + '.ft')

def calLocalWalletChangeByRawStake(SpecifyDate, pathMemberWallet, MemberWallets, MemberAccount, productId, gameAll):
    
    lastOneDay = pd.datetime.strftime(pd.datetime.strptime(SpecifyDate, '%Y-%m-%d') - pd.Timedelta('1 days'), '%Y-%m-%d')
    
    def choseNotNa(x):
        if x[0]:
            return x[0]
        else:
            return x[1]

    # product 命名 - 遊戲名稱  對照
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

    # 如果新增遊戲，則用假設 product 命名的小寫即微遊戲名稱
    while len(mapToGameSummary.keys()) < productId.shape[0]:
        for gameName in set(productId.lowerName) - set(mapToGameSummary.keys()):
            mapToGameSummary[gameName] = list(productId.loc[productId.lowerName == gameName, 'Name'])

    # 反對照也需要
    invMapToGameSummary = {}
    for key, values in mapToGameSummary.items():
        for value in values:
            invMapToGameSummary[value] = key

    # 遊戲代碼從 dict 調整成 list
    gameTypeName = []
    gameProductType = []
    for game in mapToGameSummary.keys():
        for productType in mapToGameSummary[game]:
            gameTypeName.append(game)
            gameProductType.append(productType)

    productMatch = pd.DataFrame({'lowerName':gameTypeName, 'matchName':gameProductType})
    productMatch = productMatch.merge(productId[['ProductId', 'lowerName']], on = 'lowerName', how = 'left')



    # 調整錢包餘額-------------------------------------------------------------------------------------------
    # 19：IMESport，與 IMSport 共通
    wallet = MemberWallets.query('ProductId != 19')




    # 中心至地方
    MemberAccount['isMainTo'] = MemberAccount['Type'].apply(lambda x:x.replace('main to ', '') if 'main to' in x else 0)
    # 地方至中心
    MemberAccount['isToMain'] = MemberAccount['Type'].apply(lambda x:x.replace(' to main', '') if ' to main' in x else 0)

    # 計算派彩-------------------------------------------------------------------------------------------
    gameAllTarget = gameAll.merge(member_profile[['member_name', 'member_id']].rename(columns={'member_name':'PlayerName', 'member_id':'MemberId'}), on = 'PlayerName', how = 'left')      

    gameAllTarget['lowerName'] = gameAllTarget.ProductType.apply(lambda x:invMapToGameSummary[x])

    productMatchOne = productMatch.groupby(['lowerName', 'ProductId'])['matchName'].count().reset_index()
    del productMatchOne['matchName']
    gameAllTarget = gameAllTarget.merge(productMatchOne, on = 'lowerName', how = 'left')

    where = gameAllTarget['ProductId'] == 19
    gameAllTarget.loc[where, 'lowerName'] = 'imsport'
    gameAllTarget.loc[where, 'ProductId'] = 17


    gameAllTarget = gameAllTarget.merge(wallet[['CreateTime', 'MemberId', 'ProductId']], on = ['MemberId', 'ProductId'], how = 'left')
    # # 用戶贏錢會讓地方錢包增加，因此與 NetAmount 相同，不用轉換

    # type1 事前下注，事後派彩 不論輸贏（流水＋輸贏）（有輸一半的，所以不能定 NetAmount 為 0）
    gameChange = gameAllTarget[~gameAllTarget.CreateTime.isna()].query('(BetTime <= CreateTime & PayoutTime >= CreateTime)').groupby(['ProductId', 'MemberId'])['NetAmount', 'BetAmount'].sum().reset_index()
    gameChange['BetBeforeAndWin'] = gameChange['NetAmount'] + gameChange['BetAmount']
    del gameChange['NetAmount']
    del gameChange['BetAmount']
    # type2 事後下注，事後派彩（輸贏）
    gameChange = gameChange.merge(gameAllTarget[~gameAllTarget.CreateTime.isna()].query('(BetTime >= CreateTime)').groupby(['ProductId', 'MemberId']).NetAmount.sum().reset_index(), on = ['ProductId', 'MemberId'], how = 'outer')
    gameChange.BetBeforeAndWin = gameChange.BetBeforeAndWin.fillna(0)
    gameChange.NetAmount = gameChange.NetAmount.fillna(0)
    gameChange['gross_revenue_bet_time'] = gameChange['BetBeforeAndWin'] + gameChange['NetAmount']

    # 中心、地方轉帳-------------------------------------------------------------------------------------------

    # 只需考慮中心與地方的進出金額
    MemberAccount = MemberAccount.query('isMainTo != 0 | isToMain != 0')
    MemberAccount = MemberAccount.merge(productId[['lowerName', 'ProductId']].rename(columns={'lowerName':'isMainTo', 'ProductId':'isMainToProductId'}),
                          on = 'isMainTo', how = 'left')
    MemberAccount = MemberAccount.merge(productId[['lowerName', 'ProductId']].rename(columns={'lowerName':'isToMain', 'ProductId':'isToMainProductId'}),
                          on = 'isToMain', how = 'left')
    wallet = wallet.merge(productId[['lowerName', 'ProductId']],
                          on = 'ProductId', how = 'left')

    # 創出一行 isToMain + isMainTo 非 NA ，以方便合併 CreateTime
    MemberAccount['lowerName'] = MemberAccount[['isToMain', 'isMainTo']].apply(choseNotNa, axis=1)
    MemberAccount = MemberAccount.merge(wallet[['MemberId', 'lowerName', 'CreateTime']],
                          on = ['MemberId', 'lowerName'], how = 'left')
    MemberAccount = MemberAccount.query('AddTime >= CreateTime')

    # 分別計算 中心轉地方金額、地方轉中心金額
    toProduct = MemberAccount.query('isMainTo != 0').groupby(['isMainToProductId', 'MemberId'])['Amount'].sum().reset_index()
    toProduct.Amount = toProduct.Amount * -1
    toMain = MemberAccount.query('isToMain != 0').groupby(['isToMainProductId', 'MemberId'])['Amount'].sum().reset_index()
    toMain.Amount = toMain.Amount * -1

    # 合併 輸贏＋地方錢包進出的變化
    gameChange = gameChange.merge(toProduct, left_on = ['ProductId', 'MemberId'], right_on = ['isMainToProductId', 'MemberId'], how = 'outer')
    gameChange = gameChange.merge(toMain, left_on = ['ProductId', 'MemberId'], right_on = ['isToMainProductId', 'MemberId'], how = 'outer')
    gameChange = gameChange.fillna(0)
    gameChange['changeTotal'] = gameChange.gross_revenue_bet_time + gameChange.Amount_x + gameChange.Amount_y

    # 會有存款但沒有投注的問題，故要補上 ProductId
    def choseNotZero(x):
        if x[0] != 0:
            return x[0]
        elif x[1] != 0:
            return x[1]
        else:
            pass

    gameChange.loc[gameChange.ProductId == 0, 'ProductId'] = gameChange[gameChange.ProductId == 0][['isMainToProductId', 'isToMainProductId']].apply(choseNotZero, axis=1)
    gameChange = gameChange.groupby(['ProductId', 'MemberId'])['BetBeforeAndWin', 'NetAmount',
                                                               'gross_revenue_bet_time', 'isMainToProductId', 'Amount_x',
                                                               'isToMainProductId', 'Amount_y', 'changeTotal'].agg({'BetBeforeAndWin':'sum', 'NetAmount':'sum',
                                                               'gross_revenue_bet_time':'sum', 'isMainToProductId':'min', 'Amount_x':'sum',
                                                               'isToMainProductId':'min', 'Amount_y':'sum', 'changeTotal':'sum'}).reset_index()
    # 最後的計算處理
    wallet = wallet.merge(gameChange[['MemberId', 'changeTotal', 'ProductId']], on = ['MemberId', 'ProductId'], how = 'left')
    # 處理有些沒有異動的地方錢包
    wallet['changeTotal'] = wallet['changeTotal'].fillna(0)
    wallet['Balance2'] = wallet['Balance'] + wallet['changeTotal']
    wallet['BalanceUpDateTime'] = pd.datetime.strptime(lastOneDay + ' 23:59:59', '%Y-%m-%d %H:%M:%S')

    wallet.reset_index(drop=True).to_feather(pathMemberWallet + 'MemberWalletsFlow' + SpecifyDate.replace('-', '') + '.ft')
    

MemberWallets = rawDataAppendNewRegMember(member_profile, MemberWallets, productId, SpecifyDate)
calLocalWalletChangeByRawStake(SpecifyDate, pathMemberWallet, MemberWallets, MemberAccount, productId, gameAll)
adjustIMSportWallet(SpecifyDate)
