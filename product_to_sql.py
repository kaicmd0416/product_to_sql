import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import re

def standardize_column_names_stock(df):
    """
    标准化股票持仓数据的列名
    
    该函数将股票持仓数据中的中文列名映射为标准的英文列名，确保数据格式的一致性。
    处理过程包括：列名转小写、替换空格为下划线、移除特殊字符、应用映射规则。
    
    Args:
        df (pandas.DataFrame): 包含股票持仓数据的DataFrame，列名为中文
        
    Returns:
        pandas.DataFrame: 标准化后的DataFrame，包含以下列：
            - code: 证券代码
            - price: 最新价
            - pct_chg: 当日涨幅
            - mkt_value: 市值
            - mkt_name: 市场名称
            - chi_name: 证券名称
            - quantity: 当前拥股
            - unit_cost: 成本价
            - cost: 当前成本
            - profit: 盈亏
            - valid_quantity: 可用余额
            - pre_quantity: 昨夜拥股
    """
    # 创建列名映射字典
    column_mapping = {
        # 代码相关
        '证券代码': 'code',
        '市场名称': 'mkt_name',
        '证券名称': 'chi_name',
        '当前拥股' : 'quantity',
        '成本价': 'unit_cost',
        '当前成本': 'cost',
        '市值' : 'mkt_value',
        '盈亏' : 'profit',
        '可用余额' : 'valid_quantity',
        '昨夜拥股': 'pre_quantity',
        '最新价' : 'price',
        '当日涨幅': 'pct_chg'
    }
    # 处理列名：先转小写
    df.columns = df.columns.str.lower()
    # 处理列名：替换空格为下划线
    df.columns = df.columns.str.replace(' ', '_')
    # 处理列名：移除特殊字符
    df.columns = df.columns.str.replace('[^\w\s]', '')
    # 创建小写的映射字典
    lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
    # 应用标准化映射
    renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
    df = df.rename(columns=renamed_columns)
    # 获取所有标准化后的列名
    standardized_columns = set(column_mapping.values())
    # 只保留在映射字典中定义的列
    columns_to_keep = [col for col in df.columns if col in standardized_columns]
    df = df[columns_to_keep]
    # 定义固定的列顺序
    fixed_columns = ['code','price','pct_chg','mkt_value','mkt_name','chi_name','quantity','unit_cost','cost','profit','valid_quantity','pre_quantity']
    # 只选择实际存在的列，并按固定顺序排列
    existing_columns = [col for col in fixed_columns if col in df.columns]
    df = df[existing_columns]
    return df

def standardize_column_names_future(df):
    """
    标准化期货持仓数据的列名
    
    该函数将期货持仓数据中的中文列名映射为标准的英文列名，确保数据格式的一致性。
    处理过程包括：列名转小写、替换空格为下划线、移除特殊字符、应用映射规则。
    
    Args:
        df (pandas.DataFrame): 包含期货持仓数据的DataFrame，列名为中文
        
    Returns:
        pandas.DataFrame: 标准化后的DataFrame，包含以下列：
            - code: 合约代码
            - price: 最新价
            - pct_chg: 当日涨幅
            - mkt_value: 合约价值
            - mkt_code: 市场代码
            - direction: 多空方向
            - chi_name: 合约名称
            - quantity: 总持仓
            - pre_quantity: 昨仓
            - today_quantity: 今仓
            - unit_cost: 持仓均价
            - cost: 持仓成本
            - profit: 持仓盈亏
    """
    # 创建列名映射字典
    column_mapping = {
        # 代码相关
        '合约代码': 'code',
        'InstrumentID': 'code',
        '合约':'code',
        '市场代码': 'mkt_code',
        '合约名称': 'chi_name',
        '多空' : 'direction',
        '买卖':'direction',
        'PosiDirection':'direction',
        '总持仓' : 'quantity',
        'Position':'quantity',
        '昨仓' : 'pre_quantity',
        'YdPosition':'pre_quantity',
        '今仓' : 'today_quantity',
        'TodayPosition' :'today_quantity',
        '持仓均价': 'unit_cost',
        '持仓成本': 'cost',
        'OpenCost':'cost',
        '合约价值' : 'mkt_value',
        '持仓盈亏' : 'profit',
        'PositionProfit':'profit',
        '最新价' : 'price',
        'SettlementPrice':'price',
        '当日涨幅': 'pct_chg'
    }
    # 处理列名：先转小写
    df.columns = df.columns.str.lower()
    # 处理列名：替换空格为下划线
    df.columns = df.columns.str.replace(' ', '_')
    # 处理列名：移除特殊字符
    df.columns = df.columns.str.replace('[^\w\s]', '')
    # 创建小写的映射字典
    lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
    # 应用标准化映射
    renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
    df = df.rename(columns=renamed_columns)
    # 获取所有标准化后的列名
    standardized_columns = set(column_mapping.values())
    # 只保留在映射字典中定义的列
    columns_to_keep = [col for col in df.columns if col in standardized_columns]
    df = df[columns_to_keep]
    # 定义固定的列顺序
    fixed_columns = ['code','price','pct_chg','mkt_value','mkt_code','direction','chi_name','quantity','pre_quantity','today_quantity','unit_cost','cost','profit']
    # 只选择实际存在的列，并按固定顺序排列
    existing_columns = [col for col in fixed_columns if col in df.columns]
    df = df[existing_columns]
    return df

def standardize_column_names_info(df):
    """
    标准化产品信息数据的列名
    
    该函数将产品信息数据中的中文列名映射为标准的英文列名，确保数据格式的一致性。
    处理过程包括：列名转小写、替换空格为下划线、移除特殊字符、应用映射规则。
    
    Args:
        df (pandas.DataFrame): 包含产品信息数据的DataFrame，列名为中文
        
    Returns:
        pandas.DataFrame: 标准化后的DataFrame，包含以下列：
            - account: 资金账号
            - asset_value: 总资产
            - mkt_value: 总市值
            - stock_mkt_value: 股票总市值
            - bond_mkt_value: 债券总市值
            - fund_mkt_value: 基金总市值
            - repurchase_mkt_value: 回购总市值
            - net_value: 净资产
            - profit: 今日账号盈亏
    """
    # 创建列名映射字典
    column_mapping = {
        # 代码相关
        '资金账号': 'account',
        '总资产': 'asset_value',
        '总市值': 'mkt_value',
        '股票总市值':'stock_mkt_value',
        '债券总市值':'bond_mkt_value',
        '基金总市值': 'fund_mkt_value',
        '回购总市值':'repurchase_mkt_value',
        '净资产': 'net_value',
        '今日账号盈亏': 'profit'
    }
    # 处理列名：先转小写
    df.columns = df.columns.str.lower()
    # 处理列名：替换空格为下划线
    df.columns = df.columns.str.replace(' ', '_')
    # 处理列名：移除特殊字符
    df.columns = df.columns.str.replace('[^\w\s]', '')
    # 创建小写的映射字典
    lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
    # 应用标准化映射
    renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
    df = df.rename(columns=renamed_columns)
    # 获取所有标准化后的列名
    standardized_columns = set(column_mapping.values())
    # 只保留在映射字典中定义的列
    columns_to_keep = [col for col in df.columns if col in standardized_columns]
    df = df[columns_to_keep]
    # 定义固定的列顺序
    fixed_columns = ['account','asset_value','mkt_value','stock_mkt_value','bond_mkt_value','fund_mkt_value','repurchase_mkt_value','net_value','profit']
    # 只选择实际存在的列，并按固定顺序排列
    existing_columns = [col for col in fixed_columns if col in df.columns]
    df = df[existing_columns]
    return df

class rrProduct_to_sql:
    """
    瑞锐产品数据转SQL类
    
    该类负责处理瑞锐产品的股票持仓、期货持仓和产品信息数据，
    将数据标准化后保存到数据库中。
    
    Attributes:
        product_type (str): 产品代码，如'SSS044'（量化中证500）或'SNY426'（金砖1号）
        is_daily (bool): 是否为每日数据，True表示保存到正式表，False表示保存到临时表
    """
    
    def __init__(self, product_type, is_daily):
        """
        初始化瑞锐产品数据转SQL对象
        
        Args:
            product_type (str): 产品代码
            is_daily (bool): 是否为每日数据
        """
        self.product_type = product_type
        self.is_daily = is_daily
        
    def stockHolding_saving(self):
        """
        保存股票持仓数据到数据库
        
        该方法读取瑞锐产品的股票持仓数据文件，进行数据标准化处理，
        然后保存到数据库的股票持仓表中。
        
        Raises:
            ValueError: 当文件日期与当前日期不匹配时抛出异常
        """
        inputpath = glv.get('stock_rr')
        input_list = os.listdir(inputpath)
        if self.product_type=='SSS044': #rr500
            input_list = [i for i in input_list if 'PositionDetail(量化中证500)' in i]
        elif self.product_type=='SNY426':
            input_list = [i for i in input_list if 'PositionDetail(金砖1号)' in i]
        input_list.sort()
        input_name = input_list[-1]
        file_date_list = re.findall(r'\d{8}', input_name)
        date1 = file_date_list[-1]
        date1 = gt.strdate_transfer(date1)
        today = datetime.today()
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('positionDetail最新更新日期为:' + str(date1))
            raise ValueError
        inputpath_holding = os.path.join(inputpath, input_name)
        df = gt.readcsv(inputpath_holding)
        df=standardize_column_names_stock(df)
        df['valuation_date']=today
        df['product_code']=self.product_type
        df['update_time']=current_time
        df=df[['valuation_date','product_code','update_time']+df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily==True:
             sm = gt.sqlSaving_main(inputpath_configsql, 'stock',delete=True)
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'stock_temp',delete=True)
        sm.df_to_sql(df,'product_code',self.product_type)
        
    def futureHolding_saving(self):
        """
        保存期货持仓数据到数据库
        
        该方法读取瑞锐产品的期货持仓数据文件，进行数据标准化处理，
        然后保存到数据库的期货持仓表中。
        
        Raises:
            ValueError: 当文件日期与当前日期不匹配时抛出异常
        """
        inputpath=glv.get('future_rr')
        input_list = os.listdir(inputpath)
        if self.product_type=='SSS044': #rr500
            input_list = [i for i in input_list if 'PositionDetail(量化500增强2)' in i]
        elif self.product_type=='SNY426':
            input_list = [i for i in input_list if 'PositionDetail(金砖1号)' in i]
        input_list.sort()
        input_name = input_list[-1]
        file_date_list = re.findall(r'\d{8}', input_name)
        date1 = file_date_list[-1]
        date1 = gt.strdate_transfer(date1)
        today = datetime.today()
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('positionDetail最新更新日期为:' + str(date1))
            raise ValueError
        inputpath_holding = os.path.join(inputpath, input_name)
        df = gt.readcsv(inputpath_holding)
        df=standardize_column_names_future(df)
        df['valuation_date']=today
        df['product_code']=self.product_type
        df['update_time']=current_time
        df=df[['valuation_date','product_code','update_time']+df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily==True:
             sm = gt.sqlSaving_main(inputpath_configsql, 'future',delete=True)
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'future_temp',delete=True)
        sm.df_to_sql(df,'product_code',self.product_type)
        
    def InfoHolding_saving(self):
        """
        保存产品信息数据到数据库
        
        该方法读取瑞锐产品的账户信息数据文件，进行数据标准化处理，
        然后保存到数据库的产品信息表中。
        
        Raises:
            ValueError: 当文件日期与当前日期不匹配时抛出异常
        """
        inputpath=glv.get('stock_rr')
        input_list = os.listdir(inputpath)
        if self.product_type=='SSS044': #rr500
            input_list = [i for i in input_list if 'Account(量化中证500)' in i]
        elif self.product_type=='SNY426':
            input_list = [i for i in input_list if 'Account(金砖1号)' in i]
        input_list.sort()
        input_name = input_list[-1]
        file_date_list = re.findall(r'\d{8}', input_name)
        date1 = file_date_list[-1]
        date1 = gt.strdate_transfer(date1)
        today = datetime.today()
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('Account最新更新日期为:' + str(date1))
            raise ValueError
        inputpath_holding = os.path.join(inputpath, input_name)
        df = gt.readcsv(inputpath_holding)
        df=standardize_column_names_info(df)
        df['valuation_date']=today
        df['product_code']=self.product_type
        df['update_time']=current_time
        df=df[['valuation_date','product_code','update_time']+df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily==True:
             sm = gt.sqlSaving_main(inputpath_configsql, 'info')
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'info_temp')
        sm.df_to_sql(df)
        
    def rr_sql_saving_main(self):
        """
        瑞锐产品数据保存主函数
        
        该方法依次调用股票持仓、期货持仓和产品信息的保存方法，
        如果某个步骤出现异常，会打印错误信息但继续执行其他步骤。
        """
        try:
            self.stockHolding_saving()
        except:
            print(self.product_type+str('股票holding入库有问题'))
        try:
            self.futureHolding_saving()
        except:
            print(self.product_type+str('期货holding入库有问题'))
        try:
            self.InfoHolding_saving()
        except:
            print(self.product_type+str('产品Info入库有问题'))

class xyProduct_to_sql:
    """
    兴业产品数据转SQL类
    
    该类负责处理兴业产品的股票持仓、期货持仓和产品信息数据，
    将数据标准化后保存到数据库中。
    
    Attributes:
        product_type (str): 产品代码，固定为'SGS958'
        is_daily (bool): 是否为每日数据，True表示保存到正式表，False表示保存到临时表
    """
    
    def __init__(self, is_daily):
        """
        初始化兴业产品数据转SQL对象
        
        Args:
            is_daily (bool): 是否为每日数据
        """
        self.product_type = 'SGS958'
        self.is_daily = is_daily

    def read_csv_file(self, file_name):
        """
        读取特殊格式的CSV文件
        
        该方法读取格式为"key=value"的CSV文件，将第一行作为列名，
        其余行作为数据。
        
        Args:
            file_name (str): CSV文件路径
            
        Returns:
            pandas.DataFrame: 解析后的DataFrame
        """
        df = pd.read_csv(file_name, header=None)
        df.columns = df.iloc[0, :].map(lambda x: x.split('=')[0])
        df = df.map(lambda x: x.split('=')[1])
        return df

    def stockHolding_saving(self):
        """
        保存股票持仓数据到数据库
        
        该方法读取兴业产品的股票持仓数据文件，进行数据标准化处理，
        然后保存到数据库的股票持仓表中。
        
        Raises:
            ValueError: 当文件日期与当前日期不匹配时抛出异常
        """
        inputpath = glv.get('stock_xy')
        input_list = os.listdir(inputpath)
        input_list = [i for i in input_list if 'PositionDetail' in i]
        input_list.sort()
        input_name = input_list[-1]
        file_date_list = re.findall(r'\d{8}', input_name)
        date1 = file_date_list[-1]
        date1 = gt.strdate_transfer(date1)
        today = datetime.today()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('positionDetail最新更新日期为:' + str(date1))
            raise ValueError
        inputpath_holding = os.path.join(inputpath, input_name)
        df = gt.readcsv(inputpath_holding)
        df = standardize_column_names_stock(df)
        df['valuation_date'] = today
        df['product_code'] = self.product_type
        df['update_time'] = current_time
        df['pct_chg'] = df['pct_chg'].apply(lambda x: str(x)[:-1])
        df['pct_chg'] = df['pct_chg'].astype(float) / 100
        df = df[['valuation_date', 'product_code', 'update_time'] + df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily == True:
            sm = gt.sqlSaving_main(inputpath_configsql, 'stock', delete=True)
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'stock_temp', delete=True)
        sm.df_to_sql(df, 'product_code', self.product_type)

    def futureHolding_saving(self):
        """
        保存期货持仓数据到数据库
        
        该方法读取兴业产品的期货持仓数据文件，进行数据标准化处理，
        然后保存到数据库的期货持仓表中。
        """
        inputpath = glv.get('future_xy')
        today = datetime.today()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.intdate_transfer(today)
        inputpath = os.path.join(inputpath, today)
        inputpath_holding = os.path.join(inputpath, 'GLDH5000602925.position.dat')
        df = self.read_csv_file(inputpath_holding)
        df['当日涨幅'] = df['SettlementPrice'].astype(float) / df['PreSettlementPrice'].astype(float) - 1
        df = standardize_column_names_future(df)
        def direction_transfer(x):
            """
            转换多空方向编码
            
            Args:
                x (str): 方向编码，'2'表示多头，其他表示空头
                
            Returns:
                str: 中文方向描述
            """
            if x == '2':
                return '多'
            else:
                return '空'

        df['direction'] = df['direction'].apply(lambda x: direction_transfer(x))
        df['valuation_date'] = gt.strdate_transfer(today)
        df['product_code'] = self.product_type
        df['update_time'] = current_time
        df = df[['valuation_date', 'product_code', 'update_time'] + df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily == True:
            sm = gt.sqlSaving_main(inputpath_configsql, 'future', delete=True)
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'future_temp', delete=True)
        sm.df_to_sql(df, 'product_code', self.product_type)

    def InfoHolding_saving(self):
        """
        保存产品信息数据到数据库
        
        该方法读取兴业产品的账户信息数据文件，进行数据标准化处理，
        然后保存到数据库的产品信息表中。
        
        Raises:
            ValueError: 当文件日期与当前日期不匹配时抛出异常
        """
        inputpath = glv.get('stock_xy')
        input_list = os.listdir(inputpath)
        input_list = [i for i in input_list if 'Account' in i]
        input_list.sort()
        input_name = input_list[-1]
        file_date_list = re.findall(r'\d{8}', input_name)
        date1 = file_date_list[-1]
        date1 = gt.strdate_transfer(date1)
        today = datetime.today()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('Account最新更新日期为:' + str(date1))
            raise ValueError
        inputpath_holding = os.path.join(inputpath, input_name)
        df = gt.readcsv(inputpath_holding)
        df = standardize_column_names_info(df)
        df['valuation_date'] = today
        df['product_code'] = self.product_type
        df['update_time'] = current_time
        df = df[['valuation_date', 'product_code', 'update_time'] + df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily == True:
            sm = gt.sqlSaving_main(inputpath_configsql, 'info')
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'info_temp')
        sm.df_to_sql(df)

    def xy_sql_saving_main(self):
        """
        兴业产品数据保存主函数
        
        该方法依次调用股票持仓和产品信息的保存方法，
        如果某个步骤出现异常，会打印错误信息但继续执行其他步骤。
        """
        try:
            self.stockHolding_saving()
        except:
            print(self.product_type + str('股票holding入库有问题'))
        try:
            self.InfoHolding_saving()
        except:
            print(self.product_type + str('产品Info入库有问题'))

class renrProduct_to_sql:
    """
    仁瑞产品数据转SQL类
    
    该类负责处理仁瑞产品的股票持仓、期货持仓和产品信息数据，
    将数据标准化后保存到数据库中。
    
    Attributes:
        product_type (str): 产品代码，固定为'SLA626'
        is_daily (bool): 是否为每日数据，True表示保存到正式表，False表示保存到临时表
    """
    
    def __init__(self, is_daily):
        """
        初始化仁瑞产品数据转SQL对象
        
        Args:
            is_daily (bool): 是否为每日数据
        """
        self.product_type='SLA626'
        self.is_daily=is_daily

    def read_csv_file(self, file_name):
        """
        读取特殊格式的CSV文件
        
        该方法读取格式为"key=value"的CSV文件，将第一行作为列名，
        其余行作为数据。
        
        Args:
            file_name (str): CSV文件路径
            
        Returns:
            pandas.DataFrame: 解析后的DataFrame
        """
        df = pd.read_csv(file_name, header=None)
        df.columns = df.iloc[0, :].map(lambda x: x.split('=')[0])
        df = df.map(lambda x: x.split('=')[1])
        return df
        
    def stockHolding_saving(self):
        """
        保存股票持仓数据到数据库
        
        该方法读取仁瑞产品的股票持仓数据文件，进行数据标准化处理，
        然后保存到数据库的股票持仓表中。
        
        Raises:
            ValueError: 当文件日期与当前日期不匹配时抛出异常
        """
        inputpath=glv.get('stock_renrui')
        # print(inputpath)
        input_list = os.listdir(inputpath)
        input_list = [i for i in input_list if 'PositionStatics' in i]
        input_list.sort()
        input_name = input_list[-1]
        file_date_list = re.findall(r'\d{8}', input_name)
        date1 = file_date_list[-1]
        date1 = gt.strdate_transfer(date1)
        today = datetime.today()
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('positionDetail最新更新日期为:' + str(date1))
            raise ValueError
        inputpath_holding = os.path.join(inputpath, input_name)
        df = gt.readcsv(inputpath_holding)
        df=standardize_column_names_stock(df)
        df['valuation_date']=today
        df['product_code']=self.product_type
        df['update_time']=current_time
        df['pct_chg'] = df['pct_chg'].apply(lambda x: str(x)[:-1])
        df['pct_chg'] = df['pct_chg'].astype(float) / 100
        df=df[['valuation_date','product_code','update_time']+df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily == True:
            sm = gt.sqlSaving_main(inputpath_configsql, 'stock', delete=True)
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'stock_temp', delete=True)
        sm.df_to_sql(df, 'product_code', self.product_type)
        
    def InfoHolding_saving(self):
        """
        保存产品信息数据到数据库
        
        该方法读取仁瑞产品的账户信息数据文件，进行数据标准化处理，
        然后保存到数据库的产品信息表中。
        
        Raises:
            ValueError: 当文件日期与当前日期不匹配时抛出异常
        """
        inputpath=glv.get('stock_renrui')
        input_list = os.listdir(inputpath)
        input_list = [i for i in input_list if 'Account' in i]
        input_list.sort()
        input_name = input_list[-1]
        file_date_list = re.findall(r'\d{8}', input_name)
        date1 = file_date_list[-1]
        date1 = gt.strdate_transfer(date1)
        today = datetime.today()
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('Account最新更新日期为:' + str(date1))
            raise ValueError
        inputpath_holding = os.path.join(inputpath, input_name)
        df = gt.readcsv(inputpath_holding)
        df=standardize_column_names_info(df)
        df['valuation_date']=today
        df['product_code']=self.product_type
        df['update_time']=current_time
        df=df[['valuation_date','product_code','update_time']+df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily==True:
             sm = gt.sqlSaving_main(inputpath_configsql, 'info')
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'info_temp')
        sm.df_to_sql(df)
        
    def renrui_sql_saving_main(self):
        """
        仁瑞产品数据保存主函数
        
        该方法依次调用股票持仓和产品信息的保存方法，
        如果某个步骤出现异常，会打印错误信息但继续执行其他步骤。
        """
        try:
            self.stockHolding_saving()
        except Exception as e:
            print(self.product_type+str('股票holding入库有问题'))
            print(e)
        try:
            self.InfoHolding_saving()
        except Exception as e:
            print(self.product_type+str('产品Info入库有问题'))
            print(e)

    def futureHolding_saving(self):
        """
        保存期货持仓数据到数据库
        
        该方法读取仁瑞产品的期货持仓数据文件，进行数据标准化处理，
        然后保存到数据库的期货持仓表中。
        """
        inputpath = glv.get('future_renrui')
        today = datetime.today()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        today = gt.intdate_transfer(today)
        inputpath = os.path.join(inputpath, today)
        inputpath_holding = os.path.join(inputpath, 'GLDH5000603056.position.dat')
        df = self.read_csv_file(inputpath_holding)
        df['当日涨幅'] = df['SettlementPrice'].astype(float) / df['PreSettlementPrice'].astype(float) - 1
        df = standardize_column_names_future(df)
        def direction_transfer(x):
            """
            转换多空方向编码
            
            Args:
                x (str): 方向编码，'2'表示多头，其他表示空头
                
            Returns:
                str: 中文方向描述
            """
            if x == '2':
                return '多'
            else:
                return '空'
        df['direction'] = df['direction'].apply(lambda x: direction_transfer(x))
        df['valuation_date'] = gt.strdate_transfer(today)
        df['product_code'] = self.product_type
        df['update_time'] = current_time
        df = df[['valuation_date', 'product_code', 'update_time'] + df.columns.tolist()[:-3]]
        inputpath_configsql = glv.get('config_sql')
        if self.is_daily == True:
            sm = gt.sqlSaving_main(inputpath_configsql, 'future', delete=True)
        else:
            sm = gt.sqlSaving_main(inputpath_configsql, 'future_temp', delete=True)
        sm.df_to_sql(df, 'product_code', self.product_type)

if __name__ == '__main__':
    rts=renrProduct_to_sql(False)
    rts.renrui_sql_saving_main()
