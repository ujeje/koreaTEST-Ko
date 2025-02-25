import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from src.common.base_trader import BaseTrader
from src.korean.kis_kr_api import KISKRAPIManager
import time

class KRTrader(BaseTrader):
    """한국 주식 트레이더"""
    
    def __init__(self, config_path: str):
        """
        Args:
            config_path (str): 설정 파일 경로
        """
        super().__init__(config_path, "KOR")
        self.kr_api = KISKRAPIManager(config_path)
        self.load_settings()
        self.last_api_call = 0
        self.api_call_interval = 0.2  # API 호출 간격 (초)
        self.max_retries = 3  # 최대 재시도 횟수
    
    def _wait_for_api_call(self):
        """API 호출 간격을 제어합니다."""
        current_time = time.time()
        elapsed = current_time - self.last_api_call
        if elapsed < self.api_call_interval:
            time.sleep(self.api_call_interval - elapsed)
        self.last_api_call = time.time()

    def _retry_api_call(self, func, *args, **kwargs):
        """API 호출을 재시도합니다."""
        for attempt in range(self.max_retries):
            try:
                self._wait_for_api_call()
                result = func(*args, **kwargs)
                if result is not None:
                    return result
            except Exception as e:
                if "초당 거래건수를 초과" in str(e):
                    wait_time = (attempt + 1) * self.api_call_interval
                    self.logger.warning(f"API 호출 제한 도달. {wait_time}초 대기 후 재시도 ({attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)
                    continue
                raise
        return None
    
    def load_settings(self) -> None:
        """구글 스프레드시트에서 설정을 로드합니다."""
        try:
            self.settings = self.google_sheet.get_settings(market_type="KOR")
            self.individual_stocks = self.google_sheet.get_individual_stocks(market_type="KOR")
            self.pool_stocks = self.google_sheet.get_pool_stocks(market_type="KOR")
            
            # 설정값이 없는 경우 기본값 설정
            if 'stop_loss' not in self.settings:
                self.settings['stop_loss'] = -5.0  # 기본값 5%
            if 'trailing_start' not in self.settings:
                self.settings['trailing_start'] = 5.0  # 기본값 5%
            if 'trailing_stop' not in self.settings:
                self.settings['trailing_stop'] = -3.0  # 기본값 3%
            
            self.logger.info(f"{self.market_type} 설정을 성공적으로 로드했습니다.")
        except Exception as e:
            self.logger.error(f"{self.market_type} 설정 로드 실패: {str(e)}")
            raise
    
    def check_market_condition(self) -> bool:
        """한국 시장 상태를 체크합니다."""
        now = datetime.now()
        current_time = now.strftime("%H%M")
        
        # 장 운영 시간 체크 (09:00 ~ 15:30)
        return self.config['trading']['kor_market_start'] <= current_time <= self.config['trading']['kor_market_end']
    
    def _is_sell_time(self) -> bool:
        """매도 시점인지 확인합니다. (9:30 ± 5분)"""
        current_time = datetime.now().strftime('%H%M')
        return "0925" <= current_time <= "0935"
    
    def _is_buy_time(self) -> bool:
        """매수 시점인지 확인합니다. (13:20 ± 5분)"""
        current_time = datetime.now().strftime('%H%M')
        return "1315" <= current_time <= "1325"
    
    def calculate_ma(self, stock_code: str, period: int = 20) -> Optional[float]:
        """이동평균을 계산합니다."""
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=period*2)).strftime("%Y%m%d")
            
            df = self.kr_api.get_daily_price(stock_code, start_date, end_date)
            if df is None or len(df) < period:
                return None
            
            if 'stck_clpr' in df.columns:
                df['종가'] = df['stck_clpr'].astype(float)
            
            ma = df['종가'].rolling(window=period).mean().iloc[-2]  # 전일 종가의 이동평균값값
            return ma
        except Exception as e:
            self.logger.error(f"{period}일 이동평균 계산 실패 ({stock_code}): {str(e)}")
            return None
    
    def check_buy_condition(self, stock_code: str, ma_period: int, prev_close: float) -> tuple[bool, Optional[float]]:
        """매수 조건을 확인합니다."""
        ma = self.calculate_ma(stock_code, ma_period)
        if ma is None:
            return False, None
        return bool(prev_close > ma), ma
    
    def check_sell_condition(self, stock_code: str, ma_period: int, prev_close: float) -> tuple[bool, Optional[float]]:
        """매도 조건을 확인합니다."""
        ma = self.calculate_ma(stock_code, ma_period)
        if ma is None:
            return False, None
        return bool(prev_close < ma), ma
    
    def _is_rebalancing_day(self) -> bool:
        """리밸런싱 실행 여부를 확인합니다."""
        try:
            # 현재 시간 확인
            now = datetime.now()
            current_time = now.strftime("%H%M")
            
            # 구글 시트에서 리밸런싱 일자 가져오기
            rebalancing_date = str(self.settings.get('rebalancing_date', ''))
            if not rebalancing_date:
                return False
            
            # 구분자로 분리된 경우 (예: 3/15, 3-15, 3.15)
            for sep in ['/', '-', '.']:
                if sep in rebalancing_date:
                    year, month, day = map(int, str(rebalancing_date).split(sep))
                    target_day = str(day).zfill(2)
                    if str(now.day).zfill(2) == target_day:
                        # 리밸런싱 시간 확인 (09:00 ~ 09:10)
                        return "0900" <= current_time <= "0910"
                    return False
            
            # 숫자만 있는 경우 (예: "15")
            if str(rebalancing_date).isdigit():
                target_day = str(int(rebalancing_date)).zfill(2)
                if str(now.day).zfill(2) == target_day:
                    # 리밸런싱 시간 확인 (09:00 ~ 09:10)
                    return "0900" <= current_time <= "0910"
            
            return False
            
        except Exception as e:
            self.logger.error(f"리밸런싱 일자 확인 중 오류 발생: {str(e)}")
            return False

    def _rebalance_portfolio(self, balance: Dict):
        """포트폴리오 리밸런싱을 실행합니다."""
        try:
            self.logger.info("포트폴리오 리밸런싱을 시작합니다.")
            
            # 총 평가금액 계산
            total_balance = float(balance['output2'][0]['tot_evlu_amt'])
            
            # 구글 시트에서 리밸런싱 비율 가져오기
            rebalancing_ratio = float(self.settings.get('rebalancing_ratio', 0))
            if not rebalancing_ratio:
                self.logger.error("리밸런싱 비율이 설정되지 않았습니다.")
                return
            
            # 현금 보유 비율 계산
            cash_ratio = (1 - rebalancing_ratio/100)
            target_cash = total_balance * cash_ratio
            
            # 보유 종목별 현재 비율 계산
            holdings = {}
            for holding in balance['output1']:
                if int(holding.get('hldg_qty', 0)) > 0:
                    stock_code = holding['pdno']
                    current_price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                    if current_price_data is None:
                        continue
                        
                    current_price = float(current_price_data['output']['stck_prpr'])
                    quantity = int(holding['hldg_qty'])
                    current_value = current_price * quantity
                    current_ratio = current_value / total_balance * 100
                    
                    # 목표 비율 찾기
                    target_ratio = 0
                    stock_info = None
                    for _, row in self.individual_stocks.iterrows():
                        if row['종목코드'] == stock_code:
                            target_ratio = float(row['배분비율'])
                            stock_info = row
                            break
                    if not stock_info:
                        for _, row in self.pool_stocks.iterrows():
                            if row['종목코드'] == stock_code:
                                target_ratio = float(row['배분비율'])
                                stock_info = row
                                break
                    
                    if target_ratio > 0:
                        # 전체 리밸런싱 비율에 맞춰 목표 비율 조정
                        adjusted_target_ratio = target_ratio * (rebalancing_ratio/100)
                        holdings[stock_code] = {
                            'name': holding['prdt_name'],
                            'current_price': current_price,
                            'quantity': quantity,
                            'current_ratio': current_ratio,
                            'target_ratio': adjusted_target_ratio,
                            'current_value': current_value
                        }
            
            # 리밸런싱 실행
            for stock_code, info in holdings.items():
                ratio_diff = info['current_ratio'] - info['target_ratio']
                
                # 비율 차이가 1% 이상일 때만 리밸런싱 실행
                if abs(ratio_diff) >= 1.0:
                    target_value = total_balance * (info['target_ratio'] / 100)
                    value_diff = info['current_value'] - target_value
                    quantity_diff = int(abs(value_diff) / info['current_price'])
                    
                    if quantity_diff > 0:
                        order_type = "SELL" if ratio_diff > 0 else "BUY"
                        result = self._retry_api_call(
                            self.kr_api.order_stock,
                            stock_code,
                            order_type,
                            quantity_diff,
                            info['current_price']
                        )
                        
                        if result:
                            msg = f"리밸런싱 {order_type} 실행: {info['name']}({stock_code})"
                            msg += f"\n- 현재 비중: {info['current_ratio']:.1f}% → 목표 비중: {info['target_ratio']:.1f}%"
                            msg += f"\n- 거래: {quantity_diff}주 {'매도' if order_type == 'SELL' else '매수'}"
                            msg += f"\n- 금액: {value_diff:,.0f}원 (단가: {info['current_price']:,}원)"
                            msg += f"\n- 목표 현금 비중: {cash_ratio*100:.1f}%"
                            self.logger.info(msg)
                            
                            if order_type == "SELL":
                                self.add_daily_sold_stock(stock_code)
            
            self.logger.info("포트폴리오 리밸런싱이 완료되었습니다.")
            
        except Exception as e:
            error_msg = f"리밸런싱 실행 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            self.send_discord_message(error_msg, error=True)

    def execute_trade(self) -> None:
        """매매를 실행합니다."""
        try:
            # 현재 날짜 및 시간 확인
            now = datetime.now()
            current_time = now.strftime("%H%M")
            
            # 당일 최초 실행 여부 확인 및 초기화
            if self.execution_date != now.strftime("%Y-%m-%d"):
                self.execution_date = now.strftime("%Y-%m-%d")
                self.market_open_executed = False
                self.market_close_executed = False
                self.is_first_execution = True
                self.logger.info(f"=== {self.execution_date} 매매 시작 ===")
            
            # 설정 로드 (최초 실행 시에만)
            if self.is_first_execution:
                self.load_settings()
                self.is_first_execution = False
            
            # 매수 시간 확인
            buy_time = self.settings.get('buy_time', '0900')
            # 매도 시간 확인
            sell_time = self.settings.get('sell_time', '1500')
            
            # 매수 시간에 매수 실행 (아직 실행되지 않은 경우)
            if current_time >= buy_time and not self.market_open_executed:
                self.logger.info(f"매수 시간 도달: {buy_time}")
                self._execute_buy_orders()
                self.market_open_executed = True
                
            # 매도 시간에 매도 실행 (아직 실행되지 않은 경우)
            if current_time >= sell_time and not self.market_close_executed:
                self.logger.info(f"매도 시간 도달: {sell_time}")
                self._execute_sell_orders()
                self.market_close_executed = True
                
            # 스탑로스/트레일링 스탑 체크 (장중 계속)
            self._check_stop_conditions()
            
        except Exception as e:
            error_msg = f"매매 실행 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            raise
    
    def _execute_buy_orders(self) -> None:
        """매수 주문을 실행합니다."""
        try:
            # 계좌 잔고 조회
            balance = self._retry_api_call(self.kr_api.get_account_balance)
            if not balance:
                self.logger.error("계좌 잔고 조회 실패")
                return
                
            # 현금 잔고 확인
            cash = float(balance['output2'][0]['dnca_tot_amt'])
            self.logger.info(f"현재 현금 잔고: {cash:,.0f}원")
            
            # 보유 종목 확인
            holdings = {}
            for holding in balance['output1']:
                holdings[holding['pdno']] = {
                    'quantity': int(holding['hldg_qty']),
                    'name': holding['prdt_name'],
                    'current_price': float(holding['prpr'])
                }
            
            # 개별 종목 매수 조건 체크
            buy_candidates = []
            
            for _, row in self.individual_stocks.iterrows():
                stock_code = row['종목코드']
                stock_name = row['종목명']
                ma_period = int(row['매매기준'])
                allocation_ratio = float(row['배분비율']) / 100
                
                # 이미 보유 중인 종목은 스킵
                if stock_code in holdings:
                    self.logger.info(f"{stock_name}({stock_code}) - 이미 보유 중")
                    continue
                    
                # 당일 매도한 종목은 스킵
                if self.is_sold_today(stock_code):
                    self.logger.info(f"{stock_name}({stock_code}) - 당일 매도 종목")
                    continue
                
                # 매수 기간 체크 (설정된 경우)
                if 'buy_start_date' in row and 'buy_end_date' in row:
                    if not self._is_within_buy_period(row):
                        self.logger.info(f"{stock_name}({stock_code}) - 매수 기간이 아님")
                        continue
                
                # 현재가 조회
                price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                if not price_data:
                    self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
                    continue
                
                current_price = float(price_data['output']['stck_prpr'])
                prev_close = float(price_data['output']['stck_sdpr'])
                
                # 매수 조건 체크 (전일 종가가 이동평균선 위에 있는지)
                is_buy, ma_value = self.check_buy_condition(stock_code, ma_period, prev_close)
                
                if is_buy:
                    self.logger.info(f"{stock_name}({stock_code}) - 매수 조건 충족 (전일종가: {prev_close:,.0f}, {ma_period}일선: {ma_value:,.0f})")
                    
                    # 매수 금액 계산 (현금 * 배분비율)
                    buy_amount = cash * allocation_ratio
                    
                    # 매수 수량 계산 (매수금액 / 현재가)
                    buy_quantity = int(buy_amount / current_price)
                    
                    if buy_quantity > 0:
                        buy_candidates.append({
                            'code': stock_code,
                            'name': stock_name,
                            'quantity': buy_quantity,
                            'price': current_price,
                            'amount': buy_quantity * current_price,
                            'allocation_ratio': allocation_ratio
                        })
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 매수 수량이 0")
                else:
                    if ma_value:
                        self.logger.info(f"{stock_name}({stock_code}) - 매수 조건 미충족 (전일종가: {prev_close:,.0f}, {ma_period}일선: {ma_value:,.0f})")
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 이동평균 계산 실패")
            
            # POOL 종목 매수 조건 체크
            for _, row in self.pool_stocks.iterrows():
                stock_code = row['종목코드']
                stock_name = row['종목명']
                ma_period = int(row['매매기준'])
                allocation_ratio = float(row['배분비율']) / 100
                
                # 이미 보유 중인 종목은 스킵
                if stock_code in holdings:
                    self.logger.info(f"{stock_name}({stock_code}) - 이미 보유 중")
                    continue
                    
                # 당일 매도한 종목은 스킵
                if self.is_sold_today(stock_code):
                    self.logger.info(f"{stock_name}({stock_code}) - 당일 매도 종목")
                    continue
                
                # 현재가 조회
                price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                if not price_data:
                    self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
                    continue
                
                current_price = float(price_data['output']['stck_prpr'])
                prev_close = float(price_data['output']['stck_sdpr'])
                
                # 매수 조건 체크 (전일 종가가 이동평균선 위에 있는지)
                is_buy, ma_value = self.check_buy_condition(stock_code, ma_period, prev_close)
                
                if is_buy:
                    self.logger.info(f"{stock_name}({stock_code}) - 매수 조건 충족 (전일종가: {prev_close:,.0f}, {ma_period}일선: {ma_value:,.0f})")
                    
                    # 매수 금액 계산 (현금 * 배분비율)
                    buy_amount = cash * allocation_ratio
                    
                    # 매수 수량 계산 (매수금액 / 현재가)
                    buy_quantity = int(buy_amount / current_price)
                    
                    if buy_quantity > 0:
                        buy_candidates.append({
                            'code': stock_code,
                            'name': stock_name,
                            'quantity': buy_quantity,
                            'price': current_price,
                            'amount': buy_quantity * current_price,
                            'allocation_ratio': allocation_ratio
                        })
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 매수 수량이 0")
                else:
                    if ma_value:
                        self.logger.info(f"{stock_name}({stock_code}) - 매수 조건 미충족 (전일종가: {prev_close:,.0f}, {ma_period}일선: {ma_value:,.0f})")
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 이동평균 계산 실패")
            
            # 매수 후보가 없으면 종료
            if not buy_candidates:
                self.logger.info("매수 조건을 충족하는 종목이 없습니다.")
                return
            
            # 매수 후보 정렬 (배분 비율이 높은 순)
            buy_candidates.sort(key=lambda x: x['allocation_ratio'], reverse=True)
            
            # 최대 종목 수 제한
            max_individual = self.settings['max_individual_stocks']
            max_pool = self.settings['max_pool_stocks']
            
            # 현재 보유 종목 수 확인
            current_individual = sum(1 for code in holdings if code in self.individual_stocks['종목코드'].values)
            current_pool = sum(1 for code in holdings if code in self.pool_stocks['종목코드'].values)
            
            # 매수 가능 종목 수 계산
            available_individual = max(0, max_individual - current_individual)
            available_pool = max(0, max_pool - current_pool)
            
            # 매수 실행
            for candidate in buy_candidates:
                stock_code = candidate['code']
                stock_name = candidate['name']
                quantity = candidate['quantity']
                price = candidate['price']
                
                # 종목 유형 확인 (개별/POOL)
                is_individual = stock_code in self.individual_stocks['종목코드'].values
                
                # 최대 종목 수 체크
                if is_individual and available_individual <= 0:
                    self.logger.info(f"{stock_name}({stock_code}) - 최대 개별 종목 수 초과")
                    continue
                elif not is_individual and available_pool <= 0:
                    self.logger.info(f"{stock_name}({stock_code}) - 최대 POOL 종목 수 초과")
                    continue
                
                # 매수 주문 실행
                self.logger.info(f"{stock_name}({stock_code}) - 매수 주문: {quantity}주 @ {price:,.0f}원")
                
                order_result = self._retry_api_call(
                    self.kr_api.order_stock,
                    stock_code,
                    "BUY",
                    quantity
                )
                
                if order_result and order_result['rt_cd'] == '0':
                    self.logger.info(f"{stock_name}({stock_code}) - 매수 주문 성공: 주문번호 {order_result['output']['ODNO']}")
                    
                    # 종목 유형에 따라 가용 종목 수 감소
                    if is_individual:
                        available_individual -= 1
                    else:
                        available_pool -= 1
                else:
                    self.logger.error(f"{stock_name}({stock_code}) - 매수 주문 실패")
            
        except Exception as e:
            self.logger.error(f"매수 주문 실행 중 오류 발생: {str(e)}")
            raise
    
    def _execute_sell_orders(self) -> None:
        """매도 주문을 실행합니다."""
        try:
            # 계좌 잔고 조회
            balance = self._retry_api_call(self.kr_api.get_account_balance)
            if not balance:
                self.logger.error("계좌 잔고 조회 실패")
                return
            
            # 보유 종목이 없으면 종료
            if not balance['output1']:
                self.logger.info("보유 종목이 없습니다.")
                return
            
            # 보유 종목 매도 조건 체크
            for holding in balance['output1']:
                stock_code = holding['pdno']
                stock_name = holding['prdt_name']
                quantity = int(holding['hldg_qty'])
                
                if quantity <= 0:
                    continue
                
                # 개별 종목 또는 POOL 종목 확인
                is_individual = stock_code in self.individual_stocks['종목코드'].values
                is_pool = stock_code in self.pool_stocks['종목코드'].values
                
                if not (is_individual or is_pool):
                    self.logger.info(f"{stock_name}({stock_code}) - 매매 대상 아님")
                    continue
                
                # 매매 기준 확인
                if is_individual:
                    ma_period = int(self.individual_stocks[self.individual_stocks['종목코드'] == stock_code]['매매기준'].values[0])
                else:
                    ma_period = int(self.pool_stocks[self.pool_stocks['종목코드'] == stock_code]['매매기준'].values[0])
                
                # 현재가 조회
                price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                if not price_data:
                    self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
                    continue
                
                current_price = float(price_data['output']['stck_prpr'])
                prev_close = float(price_data['output']['stck_sdpr'])
                
                # 매도 조건 체크 (전일 종가가 이동평균선 아래에 있는지)
                is_sell, ma_value = self.check_sell_condition(stock_code, ma_period, prev_close)
                
                if is_sell:
                    self.logger.info(f"{stock_name}({stock_code}) - 매도 조건 충족 (전일종가: {prev_close:,.0f}, {ma_period}일선: {ma_value:,.0f})")
                    
                    # 매도 주문 실행
                    self.logger.info(f"{stock_name}({stock_code}) - 매도 주문: {quantity}주 @ {current_price:,.0f}원")
                    
                    order_result = self._retry_api_call(
                        self.kr_api.order_stock,
                        stock_code,
                        "SELL",
                        quantity
                    )
                    
                    if order_result and order_result['rt_cd'] == '0':
                        self.logger.info(f"{stock_name}({stock_code}) - 매도 주문 성공: 주문번호 {order_result['output']['ODNO']}")
                        # 당일 매도 종목에 추가
                        self.add_daily_sold_stock(stock_code)
                    else:
                        self.logger.error(f"{stock_name}({stock_code}) - 매도 주문 실패")
                else:
                    if ma_value:
                        self.logger.info(f"{stock_name}({stock_code}) - 매도 조건 미충족 (전일종가: {prev_close:,.0f}, {ma_period}일선: {ma_value:,.0f})")
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 이동평균 계산 실패")
            
        except Exception as e:
            self.logger.error(f"매도 주문 실행 중 오류 발생: {str(e)}")
            raise

    def _check_stop_conditions(self):
        """스탑로스와 트레일링 스탑 조건을 체크합니다."""
        try:
            balance = self.kr_api.get_account_balance()
            if balance is None:
                return
            
            for holding in balance['output1']:
                stock_code = holding['pdno']
                current_price_data = self.kr_api.get_stock_price(stock_code)
                if current_price_data is None:
                    continue
                
                current_price = float(current_price_data['output']['stck_prpr'])
                self._check_stop_conditions_for_stock(holding, current_price)
                
        except Exception as e:
            self.logger.error(f"스탑 조건 체크 중 오류 발생: {str(e)}")
    
    def _check_stop_conditions_for_stock(self, holding: Dict, current_price: float) -> bool:
        """개별 종목의 스탑로스와 트레일링 스탑 조건을 체크합니다."""
        try:
            stock_code = holding['pdno']
            entry_price = float(holding.get('pchs_avg_pric', 0)) if holding.get('pchs_avg_pric') and str(holding.get('pchs_avg_pric')).strip() != '' else 0
            quantity = int(holding.get('hldg_qty', 0)) if holding.get('hldg_qty') and str(holding.get('hldg_qty')).strip() != '' else 0
            name = holding.get('prdt_name', stock_code)
            
            # 보유 수량이 없는 경우는 정상적인 상황이므로 조용히 리턴
            if quantity <= 0:
                return False
            
            # 매수 평균가가 유효하지 않은 경우에만 경고
            if entry_price <= 0:
                self.logger.warning(f"매수 평균가({entry_price:,}원)가 유효하지 않습니다: {name}")
                return False
            
            # 스탑로스 체크
            loss_pct = (current_price - entry_price) / entry_price * 100
            if loss_pct <= self.settings['stop_loss']:
                trade_msg = f"스탑로스 조건 성립 - {name}({stock_code}): 손실률 {loss_pct:.2f}% <= {self.settings['stop_loss']}%"
                self.logger.info(trade_msg)
                
                # 스탑로스 매도
                result = self._retry_api_call(self.kr_api.order_stock, stock_code, "SELL", quantity)
                if result:
                    # 잔고 재조회
                    new_balance = self.kr_api.get_account_balance()
                    total_balance = float(new_balance['output2'][0]['tot_evlu_amt'])
                    d2_deposit = float(new_balance['output2'][0]['dnca_tot_amt'])
                    
                    msg = f"스탑로스 매도 실행: {name} {quantity}주 (시장가)"
                    msg += f"\n- 매도 사유: 손실률 {loss_pct:.2f}% (스탑로스 {self.settings['stop_loss']}% 도달)"
                    msg += f"\n- 매도 금액: {current_price * quantity:,.0f}원 (현재가 {current_price:,}원)"
                    msg += f"\n- 매수 정보: 매수단가 {entry_price:,}원 / 평가손익 {(current_price - entry_price) * quantity:,.0f}원"
                    msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원 / D+2예수금 {d2_deposit:,.0f}원"
                    self.logger.info(msg)
                    self.add_daily_sold_stock(stock_code)  # 당일 매도 종목에 추가
                return True
            
            # 트레일링 스탑 체크
            highest_price = float(holding.get('highest_price', entry_price)) if holding.get('highest_price') and str(holding.get('highest_price')).strip() != '' else entry_price
            if highest_price <= 0:
                highest_price = entry_price
            
            # 현재가가 신고가인 경우 업데이트
            if current_price > highest_price:
                # 이전 신고가 대비 상승률 계산
                price_change_pct = (current_price - highest_price) / highest_price * 100
                profit_pct = (current_price - entry_price) / entry_price * 100
                holding['highest_price'] = current_price
                
                # 목표가 초과 시에만 메시지 출력
                if profit_pct >= self.settings['trailing_start']:
                    if price_change_pct >= 1.0:  # 1% 이상 상승 시
                        msg = f"신고가 갱신 - {name}({stock_code})"
                        msg += f"\n- 현재 수익률: +{profit_pct:.1f}% (목표가 {self.settings['trailing_start']}% 초과)"
                        msg += f"\n- 고점 대비 상승: +{price_change_pct:.1f}% (이전 고점 {highest_price:,}원 → 현재가 {current_price:,}원)"
                        msg += f"\n- 트레일링 스탑: 현재가 기준 {abs(self.settings['trailing_stop']):.1f}% 하락 시 매도"
                        self.logger.info(msg)
            else:
                # 목표가(trailing_start) 초과 여부 확인
                profit_pct = (highest_price - entry_price) / entry_price * 100
                if profit_pct >= self.settings['trailing_start']:  # 목표가 초과 시에만 트레일링 스탑 체크
                    drop_pct = (current_price - highest_price) / highest_price * 100
                    
                    # 1% 이상 하락 시 메시지 출력
                    if drop_pct <= -1.0:
                        msg = f"고점 대비 하락 - {name}({stock_code})"
                        msg += f"\n- 현재 수익률: +{((current_price - entry_price) / entry_price * 100):.1f}%"
                        msg += f"\n- 고점 대비 하락: {drop_pct:.1f}% (고점 {highest_price:,}원 → 현재가 {current_price:,}원)"
                        msg += f"\n- 트레일링 스탑: {abs(self.settings['trailing_stop'] - drop_pct):.1f}% 더 하락하면 매도"
                        self.logger.info(msg)
                    
                    if drop_pct <= self.settings['trailing_stop']:
                        trade_msg = f"트레일링 스탑 조건 성립 - {name}({stock_code}): 고점대비 하락률 {drop_pct:.2f}% <= {self.settings['trailing_stop']}%"
                        self.logger.info(trade_msg)
                        
                        result = self._retry_api_call(self.kr_api.order_stock, stock_code, "SELL", quantity)
                        if result:
                            # 잔고 재조회
                            new_balance = self.kr_api.get_account_balance()
                            total_balance = float(new_balance['output2'][0]['tot_evlu_amt'])
                            d2_deposit = float(new_balance['output2'][0]['dnca_tot_amt'])
                            
                            msg = f"트레일링 스탑 매도 실행: {name} {quantity}주 (시장가)"
                            msg += f"\n- 매도 사유: 고점 대비 하락률 {drop_pct:.2f}% (트레일링 스탑 {self.settings['trailing_stop']}% 도달)"
                            msg += f"\n- 매도 금액: {current_price * quantity:,.0f}원 (현재가 {current_price:,}원)"
                            msg += f"\n- 매수 정보: 매수단가 {entry_price:,}원 / 평가손익 {(current_price - entry_price) * quantity:,.0f}원"
                            msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원 / D+2예수금 {d2_deposit:,.0f}원"
                            self.logger.info(msg)
                            self.add_daily_sold_stock(stock_code)  # 당일 매도 종목에 추가
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"개별 종목 스탑 조건 체크 중 오류 발생: {str(e)}")
            return False 

    def update_stock_report(self) -> None:
        """국내 주식 현황을 구글 스프레드시트에 업데이트합니다."""
        try:
            # 계좌 잔고 조회
            balance = self.kr_api.get_account_balance()
            if balance is None:
                raise Exception("계좌 잔고 조회 실패")
            
            # 보유 종목 데이터 생성
            holdings_data = []
            for holding in balance['output1']:
                if int(holding.get('hldg_qty', 0)) <= 0:
                    continue
                
                stock_code = holding['pdno']
                current_price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                
                if current_price_data:
                    holdings_data.append([
                        stock_code,                                           # 종목코드
                        holding['prdt_name'],                                # 종목명
                        float(current_price_data['output']['stck_prpr']),   # 현재가
                        '',                                                  # 구분
                        float(current_price_data['output']['prdy_ctrt']),   # 등락률
                        float(holding['pchs_avg_pric']),                    # 평단가
                        float(holding['evlu_pfls_rt']),                     # 수익률
                        int(holding['hldg_qty']),                           # 보유량
                        float(holding['evlu_pfls_amt']),                    # 평가손익
                        float(holding['pchs_amt']),                         # 매입금액
                        float(holding['evlu_amt'])                          # 평가금액
                    ])
            
            # 주식현황 시트 업데이트
            holdings_sheet = self._get_holdings_sheet()
            self._update_holdings_sheet(holdings_data, holdings_sheet)
            
        except Exception as e:
            self.logger.error(f"국내 주식 현황 업데이트 실패: {str(e)}")
            raise 