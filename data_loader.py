import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta
import pytz
import ccxt
import logging
from typing import List, Dict, Union, Optional
import requests
import asyncio

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("binance_data_updater.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("BinanceDataUpdater")

class BinanceDailyDataUpdater:
    def __init__(self, 
                 original_data_path: str = 'data/all_data_1d.parquet',
                 new_data_path: str = 'data/all_data_1d_2023.parquet',
                 start_date: str = '2021-01-01',
                 api_key: str = None,
                 api_secret: str = None):
        """
        初始化每日数据更新器
        
        Args:
            original_data_path: 原始数据文件路径
            new_data_path: 新数据保存路径
            start_date: 开始日期，格式 'YYYY-MM-DD'
            api_key: Binance API密钥
            api_secret: Binance API密钥
        """
        self.original_data_path = original_data_path
        self.data_path = new_data_path
        self.start_date = pd.to_datetime(start_date)
        self.api_key = api_key
        self.api_secret = api_secret
        
        # 从原始数据文件获取交易对列表
        self.symbols = self._get_symbols_from_original_data()
        
        logger.info(f"初始化完成，将从原始数据中的 {len(self.symbols)} 个交易对获取从 {start_date} 开始的数据")
    
    def _get_symbols_from_original_data(self) -> List[str]:
        """从原始数据文件中获取交易对列表"""
        try:
            df = pd.read_parquet(self.original_data_path)
            symbols = df['symbol'].unique().tolist()
            logger.info(f"从原始数据文件中获取到 {len(symbols)} 个交易对")
            return symbols
        except Exception as e:
            logger.error(f"读取原始数据文件失败: {e}")
            return ['BTC/USDT', 'ETH/USDT']  # 默认交易对
    
    def _get_start_timestamp(self) -> int:
        """获取开始时间戳"""
        return int(self.start_date.timestamp() * 1000)
    
    async def fetch_daily_klines(self, symbol: str, start_timestamp: Optional[int] = None) -> pd.DataFrame:
        """获取单个交易对的日线数据
        
        Args:
            symbol: 交易对名称
            start_timestamp: 开始时间戳(毫秒)，如果为None则使用默认起始日期
        """
        try:
            # 使用传入的起始时间戳或默认值
            start_time = start_timestamp if start_timestamp is not None else self._get_start_timestamp()
            all_klines = []
            max_retries = 2  # 最大重试次数
            timeout = 10     # 超时时间（秒）
            
            while True:
                binance_symbol = symbol.replace('/', '')
                url = "https://fapi.binance.com/fapi/v1/klines"
                params = {
                    'symbol': binance_symbol,
                    'interval': '1d',
                    'startTime': start_time,
                    'limit': 1500
                }
                
                # 添加重试循环
                for retry in range(max_retries):
                    try:
                        headers = {'X-MBX-APIKEY': self.api_key} if self.api_key else {}
                        response = requests.get(
                            url, 
                            params=params, 
                            headers=headers,
                            timeout=timeout  # 添加超时设置
                        )
                        
                        if response.status_code == 429:  # 频率限制
                            wait_time = 60 * (retry + 1)  # 递增等待时间
                            logger.warning(f"{symbol}: 触发频率限制，等待 {wait_time} 秒")
                            await asyncio.sleep(wait_time)
                            continue
                            
                        if response.status_code == 418:  # IP 封禁
                            wait_time = 300 * (retry + 1)  # 递增等待时间
                            logger.warning(f"{symbol}: IP 被临时封禁，等待 {wait_time} 秒")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        response.raise_for_status()
                        klines = response.json()
                        break  # 成功获取数据，跳出重试循环
                        
                    except requests.exceptions.Timeout:
                        if retry < max_retries - 1:
                            wait_time = 5 * (retry + 1)  # 递增等待时间
                            logger.warning(f"{symbol}: 请求超时，第 {retry + 1} 次重试，等待 {wait_time} 秒")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"{symbol}: 达到最大重试次数，跳过该交易对")
                            return pd.DataFrame()
                            
                    except requests.exceptions.RequestException as e:
                        if retry < max_retries - 1:
                            wait_time = 5 * (retry + 1)
                            logger.warning(f"{symbol}: 请求失败 ({str(e)})，第 {retry + 1} 次重试，等待 {wait_time} 秒")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"{symbol}: 达到最大重试次数，跳过该交易对")
                            return pd.DataFrame()
                
                if not klines:
                    break
                    
                all_klines.extend(klines)
                
                # 更新start_time为最后一条数据的时间+1
                start_time = int(klines[-1][0]) + 1
                
                if len(klines) < 1500:
                    break
                    
                await asyncio.sleep(0.2)
                
            if not all_klines:
                logger.info(f"没有新的日线数据 - {symbol}")
                return pd.DataFrame()
            
            # 修改 DataFrame 的创建部分
            df = pd.DataFrame(all_klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'count', 'taker_buy_volume',  # 将 'trades' 改为 'count'
                'taker_buy_quote_volume', 'ignore'
            ])
            
            # 删除 'ignore' 列
            df = df.drop('ignore', axis=1)
            
            # 转换类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 
                        'taker_buy_volume', 'taker_buy_quote_volume', 'count']:  # 更新数值列列表
                df[col] = pd.to_numeric(df[col])
            
            # 转换时间戳
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            
            # 添加交易对信息
            df['symbol'] = symbol
            
            # 移除获取资金费率的部分
            logger.info(f"获取到 {symbol} 的 {len(df)} 条日线数据")
            return df
            
        except Exception as e:
            logger.error(f"获取 {symbol} 日线数据时出错: {str(e)}")
            return pd.DataFrame()

    async def fetch_funding_rates(self, symbol: str, start_time: pd.Timestamp) -> pd.DataFrame:
        """获取资金费率历史数据"""
        try:
            # 格式化交易对
            binance_symbol = symbol.replace('/', '')
            
            # 直接使用完整的URL
            url = f"https://fapi.binance.com/fapi/v1/fundingRate"
            
            # 构建请求参数
            params = {
                'symbol': binance_symbol,
                'startTime': int(start_time.timestamp() * 1000),
                'limit': 1000
            }
            
            # 发送请求
            headers = {'X-MBX-APIKEY': self.api_key} if self.api_key else {}
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            # 处理响应
            data = response.json()
            
            if not data:
                logger.warning(f"没有 {symbol} 的资金费率数据")
                return pd.DataFrame()
            
            # 转换为DataFrame
            df = pd.DataFrame(data)
            
            # 转换时间戳
            df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
            df = df.rename(columns={'fundingTime': 'timestamp', 'fundingRate': 'funding_rate'})
            
            # 转换数据类型
            df['funding_rate'] = pd.to_numeric(df['funding_rate'])
            
            # 添加交易对信息
            df['symbol'] = symbol
            
            logger.info(f"获取到 {symbol} 的 {len(df)} 条资金费率记录")
            return df
            
        except Exception as e:
            logger.error(f"获取 {symbol} 资金费率数据时出错: {str(e)}")
            return pd.DataFrame()
    def _get_update_start_date(self, update_mode: str = 'incremental') -> pd.Timestamp:
        """根据更新模式确定开始日期
        
        Args:
            update_mode: 'full'表示完整更新，'incremental'表示增量更新
            
        Returns:
            开始日期的时间戳
        """
        if update_mode == 'full':
            return self.start_date
        
        # 增量更新模式，检查文件是否存在
        if os.path.exists(self.data_path):
            try:
                # 读取现有数据
                df = pd.read_parquet(self.data_path)
                
                if 'open_time' in df.columns:
                    # 找到最近的数据日期
                    if isinstance(df['open_time'].iloc[0], np.integer):
                        # 如果是数值类型（毫秒时间戳），转换为datetime
                        latest_date = pd.to_datetime(df['open_time'].max(), unit='ms')
                    else:
                        # 如果已经是datetime
                        latest_date = df['open_time'].max()
                        
                    # 设置为最新日期的下一天
                    next_date = latest_date + timedelta(days=1)
                    logger.info(f"增量更新模式：从 {next_date.strftime('%Y-%m-%d')} 开始获取新数据")
                    return next_date
                
            except Exception as e:
                logger.warning(f"读取现有数据文件失败: {e}，将使用默认起始日期")
        
        # 如果文件不存在或出现异常，使用默认起始日期
        logger.info(f"使用默认起始日期: {self.start_date.strftime('%Y-%m-%d')}")
        return self.start_date
    
    def check_trading_status(self, symbol: str) -> bool:
        """检查交易对是否可以交易
        
        Args:
            symbol: 交易对名称 (如 'BTC/USDT')
            
        Returns:
            bool: 是否可以交易
        """
        try:
            binance_symbol = symbol.replace('/', '')
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = requests.get(url)
            response.raise_for_status()
            
            exchange_info = response.json()
        
            # 检查交易对状态
            for symbol_info in exchange_info['symbols']:
                if symbol_info['symbol'] == binance_symbol:
                    # 对于期货，我们主要关注：
                    # 1. status 是否为 TRADING
                    # 2. contractType 是否为 PERPETUAL（永续合约）
                    # 3. 是否在交易时间内
                    is_trading = symbol_info['status'] == 'TRADING'
                    is_perpetual = symbol_info['contractType'] == 'PERPETUAL'
                    
                    if not is_trading:
                        logger.warning(f"{symbol} 期货合约状态为: {symbol_info['status']}")
                        return False
                        
                    if not is_perpetual:
                        logger.warning(f"{symbol} 不是永续合约")
                        return False
                    
                    return True
                    
            logger.warning(f"在期货市场中未找到交易对 {symbol}")
            return False
    
        except Exception as e:
            logger.error(f"检查交易对 {symbol} 状态时出错: {e}")
            return False
    
    async def update_all_data(self, update_mode: str = 'incremental'):
        """更新所有交易对的数据
    
        Args:
            update_mode: 'full'表示完整更新，'incremental'表示增量更新
        """
        # 记录开始时间
        start_time = time.time()
        # 根据更新模式确定起始日期
        update_start_date = self._get_update_start_date(update_mode)
        
        # 根据更新模式设置等待时间
        if update_mode == 'incremental':
            group_wait_time = 0.05  # 从0.1减少到0.05秒
            symbol_wait_time = 0.005  # 从0.01减少到0.005秒
        else:
            group_wait_time = 2  # 从10减少到2秒
            symbol_wait_time = 0.2  # 从1减少到0.2秒
        
        all_new_data = []
        
        # # 在获取数据前检查交易对状态
        # active_symbols = []
        # for symbol in self.symbols:
        #     if self.check_trading_status(symbol):
        #         active_symbols.append(symbol)
        #     else:
        #         logger.warning(f"交易对 {symbol} 已停止交易或不可用，将被排除")
        active_symbols = self.symbols
        # 将symbols分组，增量更新时可以使用更大的组大小
        group_size = 100 if update_mode == 'incremental' else 50  # 之前是50和20
        symbol_groups = [active_symbols[i:i + group_size] for i in range(0, len(active_symbols), group_size)]
        
        for group in symbol_groups:
            group_data = []
            for symbol in group:
                try:
                    logger.info(f"开始更新 {symbol} 的数据，起始日期: {update_start_date.strftime('%Y-%m-%d')}...")
                    # 使用新的起始日期
                    klines = await self.fetch_daily_klines(symbol, start_timestamp=int(update_start_date.timestamp() * 1000))
                    
                    if not klines.empty:
                        group_data.append(klines)
                    
                    # 避免API限制
                    # 根据更新模式使用不同的等待时间
                    await asyncio.sleep(symbol_wait_time)
                    
                except Exception as e:
                    logger.error(f"处理 {symbol} 时出错: {e}")
                    continue
            
            if group_data:
                all_new_data.extend(group_data)
            
            # 每组处理完后等待一段时间
            # 根据更新模式使用不同的等待时间
            await asyncio.sleep(group_wait_time)
        
        # 如果没有新数据
        if not all_new_data:
            logger.info("没有找到任何新数据")
            return
        
        # 合并所有新数据
        combined_data = pd.concat(all_new_data, ignore_index=True)
        
        # 增量更新模式：将新数据与现有数据合并
        if update_mode == 'incremental' and os.path.exists(self.data_path):
            try:
                # 读取现有数据
                existing_data = pd.read_parquet(self.data_path)
                
                # 转换时间格式，确保可以正确合并
                if 'open_time' in existing_data.columns and isinstance(existing_data['open_time'].iloc[0], np.integer):
                    existing_data['open_time'] = pd.to_datetime(existing_data['open_time'], unit='ms')
                    
                if 'close_time' in existing_data.columns and isinstance(existing_data['close_time'].iloc[0], np.integer):
                    existing_data['close_time'] = pd.to_datetime(existing_data['close_time'], unit='ms')
                
                # 合并数据，移除可能的重复项
                combined_data = pd.concat([existing_data, combined_data], ignore_index=True)
                combined_data = combined_data.drop_duplicates(subset=['symbol', 'open_time'], keep='last')
                
                logger.info(f"合并新旧数据，总记录数: {len(combined_data)}")
            except Exception as e:
                logger.error(f"合并数据时出错: {e}")
                logger.warning("将只保存新数据")
            
        try:
            # 确保数据目录存在
            os.makedirs(os.path.dirname(self.data_path), exist_ok=True)
            
            # 确保时间戳列以正确的方式存储
            data_copy = combined_data.copy()
            
            # 将时间戳列转换为整数表示（毫秒)
            if 'open_time' in data_copy.columns:
                data_copy['open_time'] = data_copy['open_time'].astype('int64') // 10**6
                
            if 'close_time' in data_copy.columns:
                data_copy['close_time'] = data_copy['close_time'].astype('int64') // 10**6
            
            # 保存为parquet格式，只保存需要的列
            columns_to_save = [
                'open_time', 'close_time', 'open', 'high', 'low', 'close',
                'volume', 'quote_volume', 'count', 'taker_buy_volume',
                'taker_buy_quote_volume', 'symbol'
            ]
            data_copy = data_copy[columns_to_save]
            
            data_copy.to_parquet(self.data_path, index=False)
            
            # # 创建备份
            # backup_path = f"{self.data_path}.{datetime.now().strftime('%Y%m%d')}.bak"
            # data_copy.to_parquet(backup_path, index=False)
            
            # 计算时间统计
            end_time = time.time()
            total_time = end_time - start_time
            
            # 计算数据跨度
            min_date = pd.to_datetime(combined_data['open_time'].min())
            max_date = pd.to_datetime(combined_data['open_time'].max())
            total_days = (max_date - min_date).days + 1
            
            # 计算每天平均时间
            time_per_day = total_time / total_days if total_days > 0 else 0
            
            # 打印统计信息
            logger.info(f"数据获取统计:")
            logger.info(f"总运行时间: {total_time:.2f} 秒")
            logger.info(f"数据时间范围: {min_date.date()} 到 {max_date.date()} ({total_days} 天)")
            logger.info(f"平均每天数据获取时间: {time_per_day:.2f} 秒")
            logger.info(f"获取的交易对数量: {len(self.symbols)}")
            logger.info(f"总数据条数: {len(combined_data)}")
            logger.info(f"平均每个交易对每天的数据条数: {len(combined_data)/(len(self.symbols)*total_days):.2f}")
            
            logger.info(f"数据已保存到 {self.data_path}，共 {len(data_copy)} 条记录")
            # logger.info(f"备份已创建: {backup_path}")
            
        except Exception as e:
            logger.error(f"保存数据时出错: {e}")
            
            # 尝试使用CSV作为备用选项
            emergency_dir = os.path.dirname(self.data_path)
            emergency_name = os.path.basename(self.data_path).replace('.parquet', '_emergency.csv')
            emergency_path = os.path.join(emergency_dir, emergency_name)
            
            try:
                os.makedirs(emergency_dir, exist_ok=True)
                combined_data.to_csv(emergency_path, index=False)
                logger.info(f"数据已紧急保存为CSV: {emergency_path}")
            except Exception as csv_error:
                logger.critical(f"紧急保存也失败: {csv_error}")
                raise

    def update(self, update_mode: str = 'incremental') -> None:
        """
        运行更新操作（同步版本）
        
        Args:
            update_mode: 'full'表示完整更新，'incremental'表示增量更新
        """
        # 在同步环境中运行异步函数
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.update_all_data(update_mode=update_mode))

# 主程序部分修改
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='更新Binance期货日频数据')
    parser.add_argument('--original_data', type=str, default='data/all_data_1d.parquet',
                      help='原始数据文件路径')
    parser.add_argument('--new_data', type=str, default='data/all_data_1d_2023.parquet',
                      help='新数据保存路径')
    parser.add_argument('--start_date', type=str, default='2021-01-01',
                      help='开始日期，格式 YYYY-MM-DD')
    parser.add_argument('--api_key', type=str, help='Binance API密钥')
    parser.add_argument('--api_secret', type=str, help='Binance API密钥')
    
    args = parser.parse_args()
    
    # 更新模式: 'full'=完整更新，'incremental'=增量更新(只获取最新数据)
    update_mode = 'full'  # 默认使用增量更新，只需要在需要全量更新时改为'full'
    
    # 从环境变量获取API凭证（如果命令行参数未提供）
    api_key = args.api_key or os.environ.get('BINANCE_API_KEY')
    api_secret = args.api_secret or os.environ.get('BINANCE_API_SECRET')
    
    # 创建更新器实例
    updater = BinanceDailyDataUpdater(
        original_data_path=args.original_data,
        new_data_path=args.new_data,
        start_date=args.start_date,
        api_key="vC2wEreUVtnbzSrZ4F7D8J4sbJiP9KTSpgcmCmDwNNBqnxf4W0NGplX7VJKLLrhH",
        api_secret="iSdVXh4iV0XYNYpX1RvVLwnIux6J9HqULyNOcMwU5GicUEBHbxpefZp8ELqHdkQ0"
    )
    
    # 运行更新，使用指定的更新模式
    updater.update(update_mode=update_mode)
    
    logger.info(f"数据{update_mode}更新成功完成")