'''
Build you own database with python and python Tushare-toolkit
@ Frank 2019.02.22
DataBase version: Mysql
'''

import pandas as pd
import numpy as np
import tushare as ts
from sqlalchemy import create_engine
import datetime as dt
import jqdatasdk as jq
import time


global TOKEN

TOKEN = '5014a3a9fe4aa0d8ce0189c8090b553ffb66ddfd16f4417d219f7804'

# income_sheet_items = [var[0] for var in pd.read_csv('D:\pythons\MyDataBase\income_sheet.txt').values]
# balance_sheet_items = [var[0] for var in pd.read_csv('D:\pythons\MyDataBase\balance_sheet.txt').values]
# cashflow_sheet_items = [var[0] for var in pd.read_csv('D:\pythons\MyDataBase\cashflow_sheet.txt').values]


class my_database(object):
    '''
    DataBase Connection Object
    '''

    def __init__(self):
        return

    def activate(self, database='chinaasharestock'):
        connect = create_engine("mysql+pymysql://root:{0}@127.0.0.1:3306/".format('N_surrender')+database)
        return connect

def base_data_panel():
    '''
    Update basedate: open,high,low,close,adjustfactor... etc
    :return: Updated database
    '''

    #initialize database
    connection = my_database().activate()
    # token
    api = ts.pro_api(TOKEN)
    # request calendar and store it
    today = dt.datetime.today().strftime("%Y%m%d")
    calendar = api.query('trade_cal', start_date='20050101', end_date=today)
    trade_day = calendar[calendar['is_open']==1]['cal_date']
    trade_day.to_sql('calendar', connection, index=False, if_exists='append')

    # update Basic info
    ## read Log
    update_start_date = pd.read_sql('select UpdateLog.update from UpdateLog', con=connection)
    update_start_date = int(update_start_date.iloc[-1])
    #update trading
    on_list = api.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,list_date,delist_date,list_status,is_hs')
    on_list.to_sql('on_list_info', connection, if_exists='append')
    # update basic
    loc_start = np.where(np.array([int(ivars) for ivars in trade_day.values])>update_start_date)[0][0]
    loc_end = np.where(np.array([int(ivars) for ivars in trade_day.values])>=int(today))[0][0]
    problem = {'basic':[], 'adj_factor':[], 'basic_info':[]}
    for idate in trade_day.values[loc_start:loc_end+1]:
        try:
            df = api.daily(trade_date=str(idate))
            df.to_sql('Basic', connection, if_exists='append')
            print('Basic Table Updated at date {0}'.format(idate))
        except Exception as e:
            print("{0} is in valid".format(idate))
            problem['basic'].append(idate)
            continue
    # update adj_factor
    for idate in trade_day.values[loc_start:loc_end+1]:
        try:
            df = api.adj_factor(ts_code='', trade_date=str(idate))
            df.to_sql('AdjustFactor', connection, if_exists='append')
            print('AdjustFactor Factor Updated at date {0}'.format(idate))
        except Exception as e:
            print("{0} is in valid".format(idate))
            problem['adj_factor'].append(idate)
            continue
    #update basic info
    for idate in trade_day.values[loc_start:loc_end+1]:
        try:
            df = api.daily_basic(ts_code='', trade_date=str(idate),
                                 fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,free_share,total_mv,circ_mv')
            df.to_sql('Basic_Indicator', connection, if_exists='append')
            print('Basic Indicator Table Updated at date {0}'.format(idate))
        except Exception as e:
            print("{0} is in valid".format(idate))
            problem['basic_info'].append(idate)
            continue

    # update log
    update_info = pd.DataFrame({'update': today}, index=[int(today)])
    update_info.to_sql('UpdateLog', connection, if_exists='append')

def rehability():
    '''
    this is an old version, too slow
    :return:
    '''
    connection = my_database().activate()
    code_list = np.unique(pd.read_sql('select ts_code from chinaasharestock.basic', con=connection).values)#whatever you have at time t
    update_start_date = pd.read_sql('select UpdateLog.update from UpdateLog', con=connection)
    update_start_date = int(update_start_date.iloc[-1])
    out = pd.DataFrame([])
    for icode in code_list:
        return_arr = pd.read_sql('select trade_date,ts_code,close from chinaasharestock.basic where chinaasharestock.basic.ts_code='+ "\'" + icode + "\'" + ' and chinaasharestock.basic.trade_date>='+str(update_start_date), con=connection)
        adj_factor = pd.read_sql('select trade_date,ts_code,adj_factor from chinaasharestock.adjustfactor where chinaasharestock.adjustfactor.ts_code='+ "\'" + icode + "\'" +  ' and chinaasharestock.adjustfactor.trade_date>='+str(update_start_date),con=connection)
        df = pd.merge(return_arr[['trade_date', 'ts_code', 'close']], adj_factor[['trade_date', 'adj_factor']], how='inner', on='trade_date')
        df['adj_close'] = df['close']*df['adj_factor']
        df = df[['ts_code', 'trade_date', 'adj_close']].pivot(index='trade_date', columns='ts_code')
        df.columns = [icode]
        df = df[icode].pct_change().dropna()
        out = pd.concat([out, df], axis=1)
        print('Finish {0}'.format(icode))

def Rehability():
    connection = my_database().activate()
    #update_start_date = pd.read_sql('select UpdateLog.update from UpdateLog', con=connection)
    #update_start_date = int(update_start_date.iloc[-1])
    # tushare adjust factor has some problem
    close_arr = pd.read_sql(
        'select trade_date,ts_code,close from chinaasharestock.basic', con=connection)
    adj_factor = pd.read_sql(
        'select trade_date,ts_code,adj_factor from chinaasharestock.adjustfactor', con=connection)
    temp_arr = close_arr.pivot(index='trade_date', columns='ts_code', values='close')
    adj_temp_arr = adj_factor.pivot(index='trade_date', columns='ts_code', values='adj_factor')
    update_info = list(set(adj_temp_arr.columns).intersection(set(temp_arr.columns)))
    ret_arr = (temp_arr[update_info]*adj_temp_arr[update_info]).fillna(method='ffill').pct_change()
    np.save('return.npy', ret_arr)
    return None

def triple_sheet_initialize():
    '''
    TODO: build better way to deal with 80 quota limitation
    :return:
    '''
    #initialize database
    connection = my_database().activate()
    # token
    api = ts.pro_api(TOKEN)
    # request calendar and store it
    today = dt.datetime.today().strftime("%Y%m%d")
    calendar = api.query('trade_cal', start_date='20050101', end_date=today)
    trade_day = calendar[calendar['is_open']==1]['cal_date']
    trade_day.to_sql('calendar', connection, index=False, if_exists='append')

    # update Basic info
    ## read Log
    update_start_date = pd.read_sql('select UpdateLog.update from UpdateLog', con=connection)
    update_start_date = int(update_start_date.iloc[-1])
    #update trading
    on_list = api.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,list_date,delist_date,list_status,is_hs')
    on_list.to_sql('on_list_info', connection, if_exists='append')
    # update basic
    loc_start = np.where(np.array([int(ivars) for ivars in trade_day.values])>update_start_date)[0][0]
    loc_end = np.where(np.array([int(ivars) for ivars in trade_day.values])>=int(today))[0][0]

    wrong_code = {'balancesheet':[], 'cashflow':[]}
    for itable in ['balancesheet', 'cashflow']:
        count = 0
        for icode in on_list['ts_code']:
            if count <80:
                try:
                    df = api.query(itable, ts_code=icode, start_date='20040101', end_date=today)
                    df.to_sql("_".join([itable, 'sheet']), connection, index=False, if_exists='append')
                    print("[INFO] Successfully append data from {0} table with code {1}".format(itable, icode))
                    time.sleep(0.1)
                    count += 1
                except Exception as e:
                    count = 0
                    wrong_code[itable].append(icode)
                    print("[Error] Failure to capture data for " + icode)
                    print('[SLEEP] Sleeping a while')
                    time.sleep(60)
                    continue
            else:
                time.sleep(30)
                print('[Exceeded 80, Sleeping a while]')
                count = 0

    wrong_code2 = {'balancesheet':[], 'cashflow':[]}
    for itable in ['balancesheet', 'cashflow']:
        count = 0
        for icode in wrong_code[itable]:
            if count <=80:
                try:
                    df = api.query(itable, ts_code=icode, start_date='20040101', end_date=today)
                    df.to_sql("_".join([itable, 'sheet']), connection, index=False, if_exists='append')
                    print("[INFO] Successfully append data from {0} table with code {1}".format(itable, icode))
                    time.sleep(0.1)
                    count += 1
                except Exception as e:
                    count = 0
                    wrong_code2[itable].append(icode)
                    print("[Error] Failure to capture data for " + icode)
                    time.sleep(60)
                    print('[Sleeping a while]')
                    continue
            else:
                time.sleep(30)
                print('[Exceeded 80, Sleeping a while]')
                count = 0

    wrong_code3 = {'balancesheet':[], 'cashflow':[]}
    for itable in ['balancesheet', 'cashflow']:
        count = 0
        for icode in wrong_code2[itable]:
            if count <=80:
                try:
                    df = api.query(itable, ts_code=icode, start_date='20040101', end_date=today)
                    df.to_sql("_".join([itable, 'sheet']), connection, index=False, if_exists='append')
                    print("[INFO] Successfully append data from {0} table with code {1}".format(itable, icode))
                    time.sleep(0.1)
                    count += 1
                except Exception as e:
                    count = 0
                    wrong_code3[itable].append(icode)
                    print("[Error] Failure to capture data for " + icode)
                    time.sleep(60)
                    print('[Sleeping a while]')
                    continue
            else:
                time.sleep(30)
                print('[Exceeded 80, Sleeping a while]')
                count = 0

    wrong_code4 = {'balancesheet':[], 'cashflow':[]}
    for itable in ['balancesheet', 'cashflow']:
        count = 0
        for icode in wrong_code3[itable]:
            if count <80:
                try:
                    df = api.query(itable, ts_code=icode, start_date='20040101', end_date=today)
                    df.to_sql("_".join([itable, 'sheet']), connection, index=False, if_exists='append')
                    print("[INFO] Successfully append data from {0} table with code {1}".format(itable, icode))
                    time.sleep(0.1)
                    count += 1
                except Exception as e:
                    count = 0
                    wrong_code4[itable].append(icode)
                    print("[Error] Failure to capture data for " + icode)
                    time.sleep(60)
                    print('[Sleeping a while]')
                    continue
            else:
                time.sleep(30)
                print('[Exceeded 80, Sleeping a while]')
                count = 0
    wrong_info_final = wrong_code4
    return wrong_info_final

def triple_sheet_update():
    '''
    Financial data is totally differently frequented you should use a new Log items in UpdateLog table
    :return:
    '''
    pass

def save_ret_to_bin(vars, tables, save_ticker_date=False, save_path=None):
    '''
    TODO: give a choice that if the user want different kinds of frequented data: daily, monthly or quarterly basically
    Saving your data from DataBase to np array
    :param vars: variable name and the name of of npy file, str
    :param tables: table name, str
    :param save_ticker_date: save dates and tickers or not, bool
    :param save_path: path you wanna save your data, default None
    :return: None
    '''
    if isinstance(vars, str) and isinstance(tables, str):
        connection = my_database().activate()
        out_arr = pd.read_sql('select trade_date,ts_code,{0} from chinaasharestock.{1}'.format(vars, tables),
                                   con=connection)
        out_arr = out_arr.pivot(index='trade_date', columns='ts_code', values=vars)

        if save_ticker_date == True:
            np.save(vars+'dates.npy', out_arr.index)
            np.save(vars+'tickers.npy', out_arr.columns)
        else:
            pass
        if save_path is None:
            np.save(vars+'.npy', out_arr)
        else:
            np.save('{0}/{1}.npy'.format(save_path, vars), out_arr)
    else:
        print('[ERROR] Wrong variable and table type !')
        raise TypeError
    return

'''###TO DO###'''
# Really need a document to guide people to use it
# separate to code to at least three parts:
    # * initialize: the first run of this database bulding process (with a simple command)
    # * updating: unify the updating setting of different datasets finacial and basid data
    # * data output: out put data with cmd line tools
# for easier use, build a xml file that for people to set some default parameters including:
    # * date range, update tables, which data to output in npy format etc.
