import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
from product_to_sql import rrProduct_to_sql,renrProduct_to_sql,xyProduct_to_sql
import re
date=datetime.today()
date=date.strftime('%Y-%m-%d')
config_path=glv.get('config_path')
def dailydata_getting(data_type,product_code):
    if data_type=='stock':
        inputpath=glv.get('output_stock')
    elif data_type=='future':
        inputpath=glv.get('output_future')
    else:
        inputpath=glv.get('output_prodinfo')
    inputpath=str(inputpath)+ f" Where product_code='{product_code}' And valuation_date='{date}' "
    df=gt.data_getting(inputpath,config_path)
    if len(df)!=0:
        status='exist'
    else:
        status='not_exist'
    return status
def temptable_manage(data_type,product_code):
    if data_type=='stock':
        inputpath=glv.get('output_stock_temp')
    elif data_type=='future':
        inputpath=glv.get('output_future_temp')
    else:
        inputpath=glv.get('output_prodinfo_temp')
    inputpath=str(inputpath)+ f" Where product_code='{product_code}' And valuation_date='{date}' "
    df=gt.data_getting(inputpath,config_path)
    if len(df)!=0:
        status='exist'
    else:
        status='not_exist'
    return status
def rr_running_main():
    current_time = datetime.now().time()
    product_code=['SSS044','SNY426']
    for product in product_code:
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute <= 15):
            status_stock = temptable_manage(product, 'stock')
            status_future = temptable_manage(product, 'future')
            stuatus_prodinfo = temptable_manage(product, 'prodinfo')
            if status_stock=='not_exist':
                gt.table_manager(config_path,'stockholding_temp')
            if status_future=='not_exist':
                gt.table_manager(config_path,'futureholding_temp')
            if stuatus_prodinfo =='not_exist':
                gt.table_manager(config_path,'productinfo_temp')
        tsql=rrProduct_to_sql(product,False)
        tsql.rr_sql_saving_main()
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute >= 30):
            status_stock=dailydata_getting(product,'stock')
            status_future=dailydata_getting(product,'future')
            stuatus_prodinfo=dailydata_getting(product,'prodinfo')
            if status_stock=='not_exist':
                tsql2=rrProduct_to_sql(product,True)
                tsql2.stockHolding_saving()
            if status_future=='not_exist':
                tsql3=rrProduct_to_sql(product,True)
                tsql3.futureHolding_saving()
            if stuatus_prodinfo=='not_exist':
                tsql4=rrProduct_to_sql(product,True)
                tsql4.InfoHolding_saving()
def xy_running_main():
    current_time = datetime.now().time()
    product_code=['SGS958']
    for product in product_code:
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute <= 15):
            status_stock = temptable_manage(product, 'stock')
            status_future = temptable_manage(product, 'future')
            stuatus_prodinfo = temptable_manage(product, 'prodinfo')
            if status_stock=='not_exist':
                gt.table_manager(config_path,'stockholding_temp')
            if status_future=='not_exist':
                gt.table_manager(config_path,'futureholding_temp')
            if stuatus_prodinfo =='not_exist':
                gt.table_manager(config_path,'productinfo_temp')
        tsql=xyProduct_to_sql(False)
        tsql.xy_sql_saving_main()
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute >= 30):
            status_stock=dailydata_getting(product,'stock')
            status_future=dailydata_getting(product,'future')
            stuatus_prodinfo=dailydata_getting(product,'prodinfo')
            if status_stock=='not_exist':
                tsql2=xyProduct_to_sql(True)
                tsql2.stockHolding_saving()
            if status_future=='not_exist':
                tsql3=xyProduct_to_sql(True)
                tsql3.futureHolding_saving()
            if stuatus_prodinfo=='not_exist':
                tsql4=xyProduct_to_sql(True)
                tsql4.InfoHolding_saving()
def renr_running_main():
    current_time = datetime.now().time()
    product_code=['SLA626']
    for product in product_code:
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute <= 15):
            status_stock = temptable_manage(product, 'stock')
            status_future = temptable_manage(product, 'future')
            stuatus_prodinfo = temptable_manage(product, 'prodinfo')
            if status_stock=='not_exist':
                gt.table_manager(config_path,'stockholding_temp')
            if status_future=='not_exist':
                gt.table_manager(config_path,'futureholding_temp')
            if stuatus_prodinfo =='not_exist':
                gt.table_manager(config_path,'productinfo_temp')
        tsql=renrProduct_to_sql(False)
        tsql.renrui_sql_saving_main()
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute >= 30):
            status_stock=dailydata_getting(product,'stock')
            stuatus_prodinfo=dailydata_getting(product,'prodinfo')
            if status_stock=='not_exist':
                tsql2=renrProduct_to_sql(True)
                tsql2.stockHolding_saving()
            if stuatus_prodinfo=='not_exist':
                tsql4=renrProduct_to_sql(True)
                tsql4.InfoHolding_saving()
if __name__ == '__main__':
    renr_running_main()
