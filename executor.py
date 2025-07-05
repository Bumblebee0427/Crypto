import ccxt
import pandas as pd
import numpy as np
import time
import asyncio
import os
import re
import logging
import math
from typing import Dict, List, Tuple
from datetime import datetime, timedelta

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_executor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TradingExecutor")

class LogSignalReader:
    """读取交易日志文件的类"""
    
    def __init__(self, log_dir: str = "trading_logs"):
        self.log_dir = log_dir
    
    def get_latest_log_file(self) -> str:
        """获取最新的日志文件"""
        try:
            # 检查日志目录是否存在
            if not os.path.exists(self.log_dir):
                logger.error(f"日志目录 {self.log_dir} 不存在")
                return None
                
            # 获取所有日志文件
            log_files = [f for f in os.listdir(self.log_dir) if f.startswith("trading_signals_") and f.endswith(".log")]
            
            if not log_files:
                logger.error("没有找到日志文件")
                return None
                
            # 按修改时间排序，获取最新的
            latest_file = max(log_files, key=lambda f: os.path.getmtime(os.path.join(self.log_dir, f)))
            return os.path.join(self.log_dir, latest_file)
        
        except Exception as e:
            logger.error(f"获取最新日志文件时出错: {e}")
            return None
    
    def parse_log_file(self, log_file: str) -> Tuple[Dict[str, float], str]:
        """解析日志文件，提取最新的交易信号"""
        try:
            # 检查文件是否存在
            if not os.path.exists(log_file):
                logger.error(f"日志文件 {log_file} 不存在")
                return {}, ""
            
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 提取所有回测时间段
            backtest_blocks = re.split(r'={80}', content)
            
            # 如果没有找到有效的回测块
            if len(backtest_blocks) <= 1:
                logger.warning("在日志文件中未找到有效的回测块")
                return {}, ""
            
            # 获取最后一个回测块（最新的）
            latest_block = backtest_blocks[-2]  # -2是因为split后最后一个通常是空字符串
            
            # 提取回测时间
            timestamp_match = re.search(r'回测时间: (\d{4}-\d{2}-\d{2})', latest_block)
            if not timestamp_match:
                logger.warning("未能从最新块中提取回测时间")
                return {}, ""
            
            backtest_date = timestamp_match.group(1)
            logger.info(f"解析的回测日期: {backtest_date}")
            
            # 提取多头和空头持仓
            long_positions = {}
            short_positions = {}
            
            # 多头持仓解析
            long_section = re.search(r'多头持仓 \(\d+\):(.*?)空头持仓', latest_block, re.DOTALL)
            if long_section:
                long_text = long_section.group(1)
                # 使用正则提取symbol和position size
                long_matches = re.findall(r'(\w+USDT)\s+[\d.]+\s+([\d.-]+)', long_text)
                for symbol, size in long_matches:
                    long_positions[symbol] = float(size)
            
            # 空头持仓解析
            short_section = re.search(r'空头持仓 \(\d+\):(.*?)持仓总结', latest_block, re.DOTALL)
            if short_section:
                short_text = short_section.group(1)
                # 使用正则提取symbol和position size，注意空头的size应该是负数
                short_matches = re.findall(r'(\w+USDT)\s+[\d.]+\s+([\d.-]+)', short_text)
                for symbol, size in short_matches:
                    # 确保空头持仓是负数
                    size_float = float(size)
                    if size_float > 0:
                        size_float = -size_float
                    short_positions[symbol] = size_float
            
            # 合并多头和空头持仓
            all_positions = {**long_positions, **short_positions}
            
            logger.info(f"从日志中解析出 {len(all_positions)} 个持仓信号")
            logger.info(f"持仓信号： {all_positions} ")
            return all_positions, backtest_date
            
        except Exception as e:
            logger.error(f"解析日志文件时出错: {e}")
            return {}, ""

class BinanceFuturesExecutor:
    def __init__(self, api_key: str, api_secret: str, is_test: bool = True):
        """
        初始化交易执行器
        
        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            is_test: 是否使用测试网络
        """
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True,
                'testnet': is_test
            }
        })
        
        # 设置为对冲模式
        try:
            # 使用标准的 CCXT 方法设置对冲模式
            self.exchange.fapiPrivatePostPositionSideDual({'dualSidePosition': 'true'})
            logger.info("成功设置为对冲模式")
        except Exception as e:
            logger.warning(f"设置对冲模式失败（可能已经是对冲模式）: {e}")
            
        self.positions = {}  # 当前持仓
        self.is_test = is_test
        logger.info(f"{'测试网络' if is_test else '实盘'} 交易执行器初始化完成")

    def get_account_balance(self) -> float:
        """获取账户余额"""
        try:
            balance = self.exchange.fetch_balance()
            usdt_balance = balance['USDT']['free']
            logger.info(f"当前账户可用USDT余额: {usdt_balance}")
            return usdt_balance
        except Exception as e:
            logger.error(f"获取账户余额失败: {e}")
            return 0.0

    def get_current_positions(self) -> Dict[str, float]:
        """获取当前持仓"""
        try:
            positions = self.exchange.fetch_positions()
            current_positions = {}
            
            for pos in positions:
                if abs(float(pos['contracts'])) > 0:
                    symbol = pos['symbol']
                    contracts = float(pos['contracts'])
                    
                    # 调整符号表示，确保多空方向正确
                    if pos['side'] == 'short':
                        contracts = -abs(contracts)
                    
                    current_positions[symbol] = contracts
                    
            logger.info(f"当前持有 {len(current_positions)} 个持仓")
            return current_positions
            
        except Exception as e:
            logger.error(f"获取当前持仓失败: {e}")
            return {}
    
    # def format_symbol_for_binance(self, symbol: str) -> str:
    #     """格式化交易对名称，适应Binance API要求"""
    #     # 如果symbol已经是Binance格式（如BTCUSDT），则直接返回
    #     if '/' not in symbol:
    #         return symbol
    #     # 如果是标准格式（如BTC/USDT），则转换为Binance格式
    #     return symbol.replace('/', '')
    def format_symbol_for_binance(self, symbol: str) -> str:
        """格式化交易对名称，适应Binance API要求"""
        # 移除 :USDT 后缀（如果存在）
        symbol = symbol.split(':')[0]
        
        # 移除 / 符号
        symbol = symbol.replace('/', '')
        
        # 确保只有一个 USDT 后缀
        if symbol.endswith('USDT'):
            symbol = symbol[:-4]  # 移除 USDT
        
        # 添加 USDT 后缀
        return symbol + 'USDT'

    def place_order(self, symbol: str, side: str, quantity: float, params: dict = None):
        """下单"""
        try:
            # 格式化符号
            formatted_symbol = self.format_symbol_for_binance(symbol)
            logger.info(f"格式化后的交易对: {formatted_symbol}")
            
            #4.29
            # 获取当前市场价格
            ticker = self.exchange.fetch_ticker(formatted_symbol)
            current_price = ticker['last']
            
            # 计算订单的名义价值
            notional_value = abs(quantity) * current_price
            
            # 如果名义价值小于5 USDT且不是减仓单，调整数量
            if notional_value < 5 and (not params or not params.get('reduceOnly', False)):
                original_quantity = quantity
                while notional_value < 5:
                    if quantity > 0:
                        quantity += 1
                    else:
                        quantity -= 1
                    notional_value = abs(quantity) * current_price
                logger.info(f"{formatted_symbol} 订单名义价值 ({notional_value:.2f} USDT) 小于5 USDT, 调整数量从 {original_quantity} 到 {quantity}")
            
            
            
            # 获取当前持仓信息
            positions = self.exchange.fetch_positions([formatted_symbol])
            current_position = None
            for pos in positions:
                if pos['symbol'] == formatted_symbol and abs(float(pos['contracts'])) > 0:
                    current_position = pos
                    break
                
            # 如果没有提供params，使用空字典
            if params is None:
                params = {}
                
            # 如果没有在参数中指定positionSide，则自动判断
            if 'positionSide' not in params:
                if side == 'buy':
                    if current_position and current_position['side'] == 'short':
                        params['positionSide'] = 'SHORT'
                    else:
                        params['positionSide'] = 'LONG'
                else:  # side == 'sell'
                    if current_position and current_position['side'] == 'long':
                        params['positionSide'] = 'LONG'
                    else:
                        params['positionSide'] = 'SHORT'

            logger.info(f"下单参数: {formatted_symbol} {side} {quantity} positionSide={params['positionSide']}")
            
            
            # 获取市场信息
            market = self.exchange.market(formatted_symbol)
            quantity = int(abs(quantity))
            logger.info(f"调整后的下单数量: {quantity}")

            # 检查最小交易量
            min_amount = market.get('limits', {}).get('amount', {}).get('min', 0)
            if quantity < min_amount:
                logger.warning(f"{formatted_symbol} 订单数量 {quantity} 小于最小限制 {min_amount}，跳过")
                return None
        
            # 确保数量是整数
            try:
                # 尝试创建订单
                order = self.exchange.create_order(
                    symbol=formatted_symbol,
                    type='MARKET',
                    side=side,
                    amount=quantity,
                    params=params  # 包含持仓方向
                )
                
                logger.info(f"订单执行成功: {formatted_symbol} {side} {quantity}")
                return order
            except Exception as e:
                if "'float' object cannot be interpreted as an integer" in str(e):
                    # 如果是浮点数错误，尝试使用整数
                    int_quantity = int(quantity)
                    logger.info(f"尝试使用整数数量下单: {int_quantity}")
                    order = self.exchange.create_order(
                        symbol=formatted_symbol,
                        type='MARKET',
                        side=side,
                        amount=int_quantity,
                        params=params  # 包含持仓方向
                    )
                    logger.info(f"整数数量下单成功: {formatted_symbol} {side} {int_quantity}")
                    return order
                else:
                    # 如果是其他错误，重新抛出
                    raise
        
        except Exception as e:
            logger.error(f"下单失败 {formatted_symbol} {side} {quantity}: {e}")
            return None
        
    # def set_leverage(self, symbol: str, leverage: int = 1):
    #     """设置特定交易对的杠杆倍数"""
    #     try:
    #         formatted_symbol = self.format_symbol_for_binance(symbol)
    #         response = self.exchange.fapiPrivate_post_leverage({
    #             'symbol': formatted_symbol,
    #             'leverage': leverage
    #         })
    #         logger.info(f"为 {symbol} 设置杠杆倍数为 {leverage}x 成功")
    #         return response
    #     except Exception as e:
    #         logger.error(f"为 {symbol} 设置杠杆倍数失败: {e}")
    #         return None
    def set_leverage(self, symbol: str, leverage: int = 1):
        """设置特定交易对的杠杆倍数"""
        try:
            formatted_symbol = self.format_symbol_for_binance(symbol)
            # 使用 ccxt 标准方法设置杠杆
            response = self.exchange.set_leverage(leverage, formatted_symbol)
            logger.info(f"为 {symbol} 设置杠杆倍数为 {leverage}x 成功")
            return response
        except Exception as e:
            logger.error(f"为 {symbol} 设置杠杆倍数失败: {e}")
            # 如果设置杠杆失败，记录错误但不中断交易流程
            return None
        
    # def execute_trades(self, target_positions: Dict[str, float], leverage: int = 1):
    #     """
    #     执行交易
        
    #     Args:
    #         target_positions: 目标持仓量 {symbol: quantity}
    #         leverage: 杠杆倍数,默认为1倍
    #     """
    #     try:
    #         # 获取当前持仓
    #         current_positions = self.get_current_positions()
            
    #         # 创建交易计划
    #         trade_plan = []
            
    #         # 对于目标持仓中不存在但当前持仓存在的品种，平仓
    #         for symbol, current_qty in current_positions.items():
    #             if symbol not in target_positions:
    #                 # 确定平仓方向
    #                 side = 'sell' if current_qty > 0 else 'buy'
                    
    #                 # 格式化符号（确保使用正确的符号格式）
    #                 formatted_symbol = self.format_symbol_for_binance(symbol)
                    
    #                 # 添加到交易计划
    #                 trade_plan.append({
    #                     'symbol': formatted_symbol,  # 使用格式化后的符号
    #                     'side': side,
    #                     'quantity': abs(current_qty),
    #                     'current': current_qty,
    #                     'target': 0
    #                 })
                    
    #         for symbol, target_qty in target_positions.items():
    #             # 获取当前持仓量
    #             current_qty = current_positions.get(symbol, 0.0)
                
    #             # 计算需要交易的数量
    #             trade_qty = target_qty - current_qty
    #             # trade_qty = round(trade_qty, 2)
    #             trade_qty = trade_qty
    #             # 如果差异很小，忽略
    #             if abs(trade_qty) < 0.001:
    #                 logger.info(f"{symbol} 持仓差异很小，忽略调整: 当前 {current_qty}, 目标 {target_qty}")
    #                 continue
                
    #             # 确定交易方向
    #             side = 'buy' if trade_qty > 0 else 'sell'
                
    #             # 添加到交易计划
    #             trade_plan.append({
    #                 'symbol': symbol,
    #                 'side': side,
    #                 'quantity': abs(trade_qty),
    #                 'current': current_qty,
    #                 'target': target_qty
    #             })
            
    #         # 执行交易计划
    #         logger.info(f"交易计划包含 {len(trade_plan)} 个订单")
            
    #         default_leverage = 1
    #         for trade in trade_plan:
    #             symbol = trade['symbol']
    #             logger.info(f"准备执行: {symbol} {trade['side']} {trade['quantity']} (当前: {trade['current']}, 目标: {trade['target']})")
                
    #             # # # 设置杠杆倍数
    #             # self.set_leverage(symbol, leverage = 1)
    #             # 设置杠杆倍数
    #             try:
    #                 self.set_leverage(symbol, leverage=1)
    #             except Exception as e:
    #                 logger.warning(f"设置杠杆失败，继续使用默认杠杆: {e}")
    #             # 调整数量以适应杠杆变化
    #             original_qty = trade['quantity']
    #             adjusted_qty = original_qty * (leverage / default_leverage)
    #             # 记录调整信息
    #             logger.info(f"{symbol} 将下单数量从 {original_qty} 调整为 {adjusted_qty} (杠杆: {leverage}x)")
    #             # 更新实际下单数量
    #             trade['quantity'] = adjusted_qty
                
    #             # 执行订单
    #             result = self.place_order(
    #                 symbol=trade['symbol'],
    #                 side=trade['side'],
    #                 quantity=trade['quantity']
    #             )
                
    #             # 避免触发API限制
    #             time.sleep(0.5)
            
    #         logger.info("所有交易执行完成")
            
    #     except Exception as e:
    #         logger.error(f"执行交易失败: {e}")
    def execute_trades(self, target_positions: Dict[str, float], leverage: int = 1):
        """
        执行交易
        
        Args:
            target_positions: 目标持仓量 {symbol: quantity}
            leverage: 杠杆倍数,默认为1倍
        """
        try:
            # 获取当前持仓
            positions = self.exchange.fetch_positions()
            current_positions = {}
            
            # 整理当前持仓信息，分别记录多空仓位
            for pos in positions:
                if abs(float(pos['contracts'])) > 0:
                    # symbol = pos['symbol']
                    symbol = self.format_symbol_for_binance(pos['symbol'])
                    if symbol not in current_positions:
                        current_positions[symbol] = {'LONG': 0, 'SHORT': 0}
                    position_side = pos['info']['positionSide']  # 获取具体的持仓方向
                    current_positions[symbol][position_side] = float(pos['contracts'])
            
            # 创建交易计划
            trade_plan = []
            
            # 1. 首先处理需要清仓的币种
            for symbol, positions_info in current_positions.items():
                if symbol not in target_positions:
                    # 需要清仓，检查多空仓位
                    if positions_info['LONG'] > 0:
                        # 平多仓
                        trade_plan.append({
                            'symbol': symbol,
                            'side': 'sell',
                            'quantity': positions_info['LONG'],
                            'positionSide': 'LONG',
                            'action': 'close'
                        })
                    
                    if positions_info['SHORT'] > 0:
                        # 平空仓
                        trade_plan.append({
                            'symbol': symbol,
                            'side': 'buy',
                            'quantity': positions_info['SHORT'],
                            'positionSide': 'SHORT',
                            'action': 'close'
                        })
            
            # 2. 处理需要调整的仓位
            for symbol, target_qty in target_positions.items():
                formatted_symbol = self.format_symbol_for_binance(symbol)
                current_info = current_positions.get(formatted_symbol, {'LONG': 0, 'SHORT': 0})
                
                if target_qty > 0:  # 目标是多仓
                    # 如果有空仓，先平掉
                    if current_info['SHORT'] > 0:
                        trade_plan.append({
                            'symbol': symbol,
                            'side': 'buy',
                            'quantity': current_info['SHORT'],
                            'positionSide': 'SHORT',
                            'action': 'close'
                        })
                    
                    # 调整多仓数量
                    qty_diff = target_qty - current_info['LONG']
                    logger.info(f"调整多仓数量: {symbol} diff: {qty_diff}")
                    if abs(qty_diff) > 1:
                        trade_plan.append({
                            'symbol': symbol,
                            'side': 'buy' if qty_diff > 0 else 'sell',
                            'quantity': abs(qty_diff),
                            'positionSide': 'LONG',
                            'action': 'adjust'
                        })
                        
                elif target_qty < 0:  # 目标是空仓
                    # 如果有多仓，先平掉
                    if current_info['LONG'] > 0:
                        trade_plan.append({
                            'symbol': symbol,
                            'side': 'sell',
                            'quantity': current_info['LONG'],
                            'positionSide': 'LONG',
                            'action': 'close'
                        })
                    
                    # 调整空仓数量
                    qty_diff = abs(target_qty) - current_info['SHORT']
                    logger.info(f"调整空仓数量: {symbol} diff: {qty_diff}")
                    if abs(qty_diff) > 1:
                        trade_plan.append({
                            'symbol': symbol,
                            'side': 'sell' if qty_diff > 0 else 'buy',
                            'quantity': abs(qty_diff),
                            'positionSide': 'SHORT',
                            'action': 'adjust'
                        })
                
                else:  # target_qty == 0，需要清仓
                    if current_info['LONG'] > 0:
                        trade_plan.append({
                            'symbol': symbol,
                            'side': 'sell',
                            'quantity': current_info['LONG'],
                            'positionSide': 'LONG',
                            'action': 'close'
                        })
                    if current_info['SHORT'] > 0:
                        trade_plan.append({
                            'symbol': symbol,
                            'side': 'buy',
                            
                            'quantity': current_info['SHORT'],
                            'positionSide': 'SHORT',
                            'action': 'close'
                        })
            
            # 执行交易计划
            logger.info(f"交易计划包含 {len(trade_plan)} 个订单")
            
            for trade in trade_plan:
                symbol = trade['symbol']
                logger.info(f"准备执行: {symbol} {trade['action']} {trade['side']} {trade['quantity']} ({trade['positionSide']})")
                
                try:
                    self.set_leverage(symbol, leverage=1)
                except Exception as e:
                    logger.warning(f"设置杠杆失败，继续使用默认杠杆: {e}")
                
                # 执行订单
                result = self.place_order(
                    symbol=symbol,
                    side=trade['side'],
                    quantity=trade['quantity'],
                    params={'positionSide': trade['positionSide']}
                )
                
                # 避免触发API限制
                time.sleep(0.5)
            
            logger.info("所有交易执行完成")
            
        except Exception as e:
            logger.error(f"执行交易失败: {e}")
        
def run_daily_trade():
    """执行每日交易"""
    try:
        logger.info("=== 开始每日交易执行 ===")
        
        # 1. 读取最新交易信号
        signal_reader = LogSignalReader()
        latest_log = signal_reader.get_latest_log_file()
        
        if not latest_log:
            logger.error("未找到日志文件，无法执行交易")
            return
        
        target_positions, backtest_date = signal_reader.parse_log_file(latest_log)
        
        if not target_positions:
            logger.error("未能解析出有效的交易信号，无法执行交易")
            return
            
        # 检查回测日期是否是最近的
        today = datetime.now().date()
        backtest_date_obj = datetime.strptime(backtest_date, "%Y-%m-%d").date()
        days_diff = (today - backtest_date_obj).days
        
        if days_diff > 3:  # 如果回测日期超过3天，发出警告
            logger.warning(f"注意: 最新交易信号日期 ({backtest_date}) 距今已有 {days_diff} 天")
        
        # 2. 初始化交易执行器
        # 从环境变量获取API密钥
        # api_key = os.environ.get('BINANCE_API_KEY')
        # api_secret = os.environ.get('BINANCE_API_SECRET')
        # # 真实账号
        api_key = "vC2wEreUVtnbzSrZ4F7D8J4sbJiP9KTSpgcmCmDwNNBqnxf4W0NGplX7VJKLLrhH"
        api_secret = "iSdVXh4iV0XYNYpX1RvVLwnIux6J9HqULyNOcMwU5GicUEBHbxpefZp8ELqHdkQ0"
        # 模拟账号
        # 于思曼
        # api_key = "e9d4ab837f3b68a79c459e0164854093479a2962de4c517f14a7b1974f3a21ad"
        # api_secret = "5916c15b5549fe4be34c9add71e1dbb43cf75433e44a7c429f7cb37f018ad853"
        # 赵晨迪
        # api_key = "4c5d0bb35091e990b544e413ac56681179b69f8bbc45ea057ac2559a641ef771"
        # api_secret = "75cc50aaccfd3f6d70103541d1c77575ee06d461bedaae8ebee0954972410437"
        # 蒋文祺
        # api_key = "17ea79381d9a0aaa5d0009067c9298a58ca7009610d0df2bb6afada83434640"
        # api_secret = "d42a65060f853c5655a86d1a910c8b8524a8cf9822ae18d0c2234b46311818e4"
        
        if not api_key or not api_secret:
            logger.error("未设置Binance API密钥,请设置环境变量 BINANCE_API_KEY 和 BINANCE_API_SECRET")
            return
        
        # 创建执行器实例
        executor = BinanceFuturesExecutor(
            api_key=api_key,
            api_secret=api_secret,
            is_test=False  # 不使用测试网络
        )
        
        # 3. 获取账户余额
        balance = executor.get_account_balance()
        if balance <= 0:
            logger.error(f"账户余额不足: {balance} USDT")
            return
        
        # 4. 执行交易
        executor.execute_trades(target_positions)
        
        # 5. 打印交易后的持仓情况
        final_positions = executor.get_current_positions()
        logger.info("=== 交易后持仓情况 ===")
        for symbol, qty in final_positions.items():
            logger.info(f"{symbol}: {qty}")
        
        logger.info("=== 每日交易执行完成 ===")
        
    except Exception as e:
        logger.error(f"执行每日交易时发生错误: {e}")

if __name__ == "__main__":
    run_daily_trade()