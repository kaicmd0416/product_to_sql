import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from product_to_sql import rrProduct_to_sql,renrProduct_to_sql,xyProduct_to_sql
import re

date=datetime.today()
date=date.strftime('%Y-%m-%d')
config_path=glv.get('config_path')

def dailydata_getting(data_type, product_code):
    """
    检查每日数据是否存在
    
    该函数检查指定产品代码和数据类型在数据库中的每日数据是否存在。
    
    Args:
        data_type (str): 数据类型，可选值：'stock'（股票）, 'future'（期货）, 'prodinfo'（产品信息）
        product_code (str): 产品代码
        
    Returns:
        str: 返回状态，'exist'表示数据存在，'not_exist'表示数据不存在
    """
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

def temptable_manage(data_type, product_code):
    """
    检查临时表数据是否存在
    
    该函数检查指定产品代码和数据类型在数据库中的临时表数据是否存在。
    
    Args:
        data_type (str): 数据类型，可选值：'stock'（股票）, 'future'（期货）, 'prodinfo'（产品信息）
        product_code (str): 产品代码
        
    Returns:
        str: 返回状态，'exist'表示数据存在，'not_exist'表示数据不存在
    """
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
    """
    瑞锐产品数据运行主函数
    
    该函数处理瑞锐产品的数据入库流程，包括：
    1. 在交易时间前（9:15前）检查并创建临时表
    2. 执行数据保存操作
    3. 在交易时间后（15:30后）检查并保存每日数据
    
    支持的产品代码：
    - SSS044: 量化中证500
    - SNY426: 金砖1号
    """
    current_time = datetime.now().time()
    product_code=['SSS044','SNY426']
    for product in product_code:
        # 交易时间前（9:15前）的处理
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute <= 15):
            status_stock = temptable_manage('stock', product)
            status_future = temptable_manage('future', product)
            stuatus_prodinfo = temptable_manage('prodinfo', product)
            # 如果临时表不存在，则创建相应的临时表
            if status_stock=='not_exist':
                gt.table_manager(config_path,'stockholding_temp')
            if status_future=='not_exist':
                gt.table_manager(config_path,'futureholding_temp')
            if stuatus_prodinfo =='not_exist':
                gt.table_manager(config_path,'productinfo_temp')
        
        # 执行数据保存操作
        tsql=rrProduct_to_sql(product,False)
        tsql.rr_sql_saving_main()
        
        # 交易时间后（15:30后）的处理
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute >= 30):
            status_stock=dailydata_getting('stock', product)
            status_future=dailydata_getting('future', product)
            stuatus_prodinfo=dailydata_getting('prodinfo', product)
            # 如果每日数据不存在，则保存到正式表
            if status_stock=='not_exist':
                tsql2=rrProduct_to_sql(product,True)
                tsql2.stockHolding_saving()
            if status_future=='not_exist':
                tsql3=rrProduct_to_sql(product,True)
                tsql3.futureHolding_saving()
            if stuatus_prodinfo=='not_exist':
                tsql4=rrProduct_to_sql(product,True)
                tsql4.InfoHolding_saving()

def xy_future_running_main():
    """
    兴业期货数据运行主函数
    
    该函数处理兴业产品的期货数据入库流程，包括：
    1. 在交易时间前（9:15前）检查并创建期货临时表
    2. 执行期货数据保存操作
    3. 在交易时间后（15:30后）检查并保存每日期货数据
    
    支持的产品代码：
    - SGS958: 兴业产品
    """
    current_time = datetime.now().time()
    product_code = ['SGS958']
    for product in product_code:
        # 交易时间前（9:15前）的处理
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute <= 15):
            status_future = temptable_manage('future', product)
            if status_future == 'not_exist':
                gt.table_manager(config_path, 'futureholding_temp')
        
        # 执行期货数据保存操作
        tsql = xyProduct_to_sql(False)
        tsql.futureHolding_saving()
        
        # 交易时间后（15:30后）的处理
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute >= 30):
            status_future = dailydata_getting('future', product)
            if status_future == 'not_exist':
                tsql3 = xyProduct_to_sql(True)
                tsql3.futureHolding_saving()

def renr_future_running_main():
    """
    仁瑞期货数据运行主函数
    
    该函数处理仁瑞产品的期货数据入库流程，包括：
    1. 在交易时间前（9:15前）检查并创建期货临时表
    2. 执行期货数据保存操作
    3. 在交易时间后（15:30后）检查并保存每日期货数据
    
    支持的产品代码：
    - SLA626: 仁瑞产品
    """
    current_time = datetime.now().time()
    product_code = ['SLA626']
    for product in product_code:
        # 交易时间前（9:15前）的处理
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute <= 15):
            status_future = temptable_manage('future', product)
            if status_future == 'not_exist':
                gt.table_manager(config_path, 'futureholding_temp')
        
        # 执行期货数据保存操作
        tsql = renrProduct_to_sql(False)
        tsql.futureHolding_saving()
        
        # 交易时间后（15:30后）的处理
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute >= 30):
            status_future = dailydata_getting('future', product)
            if status_future == 'not_exist':
                tsql3 = renrProduct_to_sql(True)
                tsql3.futureHolding_saving()

def xy_running_main():
    """
    兴业产品数据运行主函数
    
    该函数处理兴业产品的股票和产品信息数据入库流程，包括：
    1. 在交易时间前（9:15前）检查并创建临时表
    2. 执行数据保存操作
    3. 在交易时间后（15:30后）检查并保存每日数据
    
    支持的产品代码：
    - SGS958: 兴业产品
    """
    current_time = datetime.now().time()
    product_code=['SGS958']
    for product in product_code:
        # 交易时间前（9:15前）的处理
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute <= 15):
            status_stock = temptable_manage('stock', product)
            stuatus_prodinfo = temptable_manage('prodinfo', product)
            # 如果临时表不存在，则创建相应的临时表
            if status_stock=='not_exist':
                gt.table_manager(config_path,'stockholding_temp')
            if stuatus_prodinfo =='not_exist':
                gt.table_manager(config_path,'productinfo_temp')
        
        # 执行数据保存操作
        tsql=xyProduct_to_sql(False)
        tsql.xy_sql_saving_main()
        
        # 交易时间后（15:30后）的处理
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute >= 30):
            status_stock=dailydata_getting('stock', product)
            stuatus_prodinfo=dailydata_getting('prodinfo', product)
            print(status_stock,stuatus_prodinfo)
            # 如果每日数据不存在，则保存到正式表
            if status_stock=='not_exist':
                tsql2=xyProduct_to_sql(True)
                tsql2.stockHolding_saving()
            if stuatus_prodinfo=='not_exist':
                tsql4=xyProduct_to_sql(True)
                tsql4.InfoHolding_saving()

def renr_running_main():
    """
    仁瑞产品数据运行主函数
    
    该函数处理仁瑞产品的股票和产品信息数据入库流程，包括：
    1. 在交易时间前（9:15前）检查并创建临时表
    2. 执行数据保存操作
    3. 在交易时间后（15:30后）检查并保存每日数据
    
    支持的产品代码：
    - SLA626: 仁瑞产品
    """
    current_time = datetime.now().time()
    product_code=['SLA626']
    for product in product_code:
        # 交易时间前（9:15前）的处理
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute <= 15):
            status_stock = temptable_manage('stock', product)
            stuatus_prodinfo = temptable_manage('prodinfo', product)
            # 如果临时表不存在，则创建相应的临时表
            if status_stock=='not_exist':
                gt.table_manager(config_path,'stockholding_temp')
            if stuatus_prodinfo =='not_exist':
                gt.table_manager(config_path,'productinfo_temp')
        
        # 执行数据保存操作
        tsql=renrProduct_to_sql(False)
        tsql.renrui_sql_saving_main()
        
        # 交易时间后（15:30后）的处理
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute >= 30):
            status_stock=dailydata_getting('stock', product)
            stuatus_prodinfo=dailydata_getting('prodinfo', product)
            # 如果每日数据不存在，则保存到正式表
            if status_stock=='not_exist':
                tsql2=renrProduct_to_sql(True)
                tsql2.stockHolding_saving()
            if stuatus_prodinfo=='not_exist':
                tsql4=renrProduct_to_sql(True)
                tsql4.InfoHolding_saving()

if __name__ == '__main__':
    # 主程序入口，默认执行兴业产品数据处理
    xy_running_main()
    # 以下代码被注释，可根据需要启用
    # renr_future_running_main()
    # print('000000000000000000000000000000000000000')
    # xy_future_running_main()
    # print('000000000000000000000000000000000000000')
    # rr_running_main()
