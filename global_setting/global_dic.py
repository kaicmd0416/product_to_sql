"""
全局路径配置模块

这个模块负责管理和配置项目中使用的所有文件路径，主要功能包括：
1. 读取路径配置文件（JSON格式）
2. 构建完整的文件路径
3. 提供全局路径访问接口

配置文件结构 (tools_path_config.json):
1. main_folder:
   - folder_type: 文件夹类型标识
   - path: 基础路径
   - disk: 磁盘路径（可选）
2. sub_folder:
   - data_type: 数据类型标识
   - folder_name: 文件夹名称
   - folder_type: 文件夹类型标识
3. components:
   - data_source: 数据源配置
     - mode: 数据源模式 (local/sql)

主要依赖：
- json：配置文件处理
- os：路径操作
- pathlib：路径处理
- pandas：数据处理
"""

import json
import os
from pathlib import Path
import pandas as pd
import pymysql
from datetime import datetime

# 全局字典
global_dic = {}

def init():
    """
    初始化全局字典，从配置文件加载设置
    """
    global global_dic
    
    try:
        # 获取当前文件所在目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 构建配置文件的完整路径
        config_path = os.path.join(current_dir, 'productInfo_path_config.json')
        
        # 读取配置文件
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
            
        # 更新全局字典
        global_dic.update(config_data)
        return True
        
    except Exception as e:
        print(f"Error loading configuration: {str(e)}")
        global_dic = {}
        return False

def get(key):
    """
    获取全局变量值，支持本地文件系统和SQL数据库两种模式
    
    Args:
        key (str): 要获取的键名
    
    Returns:
        any: 对应的值，可能是文件路径或SQL查询语句
    """
    global global_dic
    
    # 如果字典为空，尝试重新初始化
    if not global_dic:
        if not init():
            return None
    
    # 特殊处理source参数
    if key == 'source':
        return global_dic.get('components', {}).get('data_source', {}).get('mode', 'local')
    
    # 特殊处理config_path参数
    if key == 'config_path':
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, 'productInfo_path_config.json')
    
    # 获取配置信息
    config = None
    for item in global_dic.get('sub_folder', []):
        if item.get('data_type') == key:
            config = item
            break
    
    if not config:
        return None
    
    # 检查数据源模式
    data_source = global_dic.get('components', {}).get('data_source', {})
    mode = data_source.get('mode', 'local')
    
    # 如果没有folder_type配置，返回SQL语句
    if 'folder_type' not in config:
        if config.get('sql_sheet'):
            table_name = config['sql_sheet']
            return f"SELECT * FROM {table_name}"
        return None
    
    # SQL模式且有sql_sheet配置，返回查询语句
    if mode == 'sql' and config.get('sql_sheet'):
        table_name = config['sql_sheet']
        return f"SELECT * FROM {table_name}"
    else:
        # 本地文件模式
        if 'folder_name' not in config:
            return None
            
        # 获取主文件夹路径
        main_folder = global_dic.get('main_folder', [])
        folder_type = config.get('folder_type', 'input_folder')
        
        # 查找对应的主文件夹
        base_path = None
        for folder in main_folder:
            if folder.get('folder_type') == folder_type:
                base_path = folder.get('path')
                # 尝试获取disk配置，如果没有则使用path中的磁盘路径
                disk = folder.get('disk', '')
                if disk:
                    # 确保disk以冒号结尾
                    if not disk.endswith(':'):
                        disk += ':'
                    # 确保base_path不以斜杠开头
                    base_path = base_path.lstrip('\\').lstrip('/')
                    # 构建完整路径
                    full_path = os.path.join(disk + os.sep, base_path, config['folder_name'])
                    # 标准化路径分隔符
                    return os.path.normpath(full_path)
                break
                
        if not base_path:
            return None
        
        # 如果没有disk配置，直接使用base_path
        full_path = os.path.join(base_path, config['folder_name'])
        full_path = os.path.normpath(full_path)
        return full_path

def set(key, value):
    """
    设置全局变量值
    
    Args:
        key (str): 要设置的键名
        value (any): 要设置的值
    """
    global global_dic
    global_dic[key] = value

# 初始化全局字典
init()

# 确保get函数可以被导出
__all__ = ['get', 'set', 'init']