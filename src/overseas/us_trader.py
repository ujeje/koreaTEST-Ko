import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import pytz
from src.common.base_trader import BaseTrader
from src.overseas.kis_us_api import KISUSAPIManager
from src.utils.trade_history_manager import TradeHistoryManager
import time
import exchange_calendars as xcals

class USTrader(BaseTrader):
    """미국 주식 트레이더"""
    
    def __init__(self, config_path: str):
        """
        Args:
            config_path (str): 설정 파일 경로
        """
        super().__init__(config_path, "USA")
        self.us_api = KISUSAPIManager(config_path)
        self.trade_history = TradeHistoryManager("USA")
        self.load_settings()
        self.us_timezone = pytz.timezone("America/New_York")
        self.last_api_call = 0
        self.api_call_interval = 1.0  # API 호출 간격 (초)
        self.max_retries = 3  # 최대 재시도 횟수
        
        # 최고가 캐시 관련 변수 추가
        self.highest_price_cache = {}  # 종목별 최고가 캐시
        self.highest_price_cache_date = None  # 최고가 캐시 갱신 날짜
        
        self.logger.info(f"미국 시장 시간 설정: {self.config['trading']['usa_market_start']} ~ {self.config['trading']['usa_market_end']}")
    
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
            self.settings = self.google_sheet.get_settings(market_type="USA")
            self.individual_stocks = self.google_sheet.get_individual_stocks(market_type="USA")
            self.pool_stocks = self.google_sheet.get_pool_stocks(market_type="USA")
            
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
        """현재 시장 상태를 확인합니다."""
        current_time = datetime.now(self.us_timezone)
        
        # exchange_calendars 라이브러리를 사용하여 휴장일 확인
        try:
            # XNYS: 뉴욕 증권거래소 (NYSE) 캘린더 사용
            nyse_calendar = xcals.get_calendar("XNYS")
            current_date = current_time.strftime('%Y-%m-%d')
            
            # 오늘이 거래일인지 확인
            if not nyse_calendar.is_session(current_date):
                self.logger.info(f"오늘({current_date})은 미국 증시 휴장일입니다.")
                return False
            
            # 장 시작 시간과 종료 시간 체크
            current_time_str = current_time.strftime('%H%M')
            if not (self.config['trading']['usa_market_start'] <= current_time_str <= self.config['trading']['usa_market_end']):
                self.logger.info("현재 미국 장 운영 시간이 아닙니다.")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"시장 상태 확인 중 오류 발생: {str(e)}")
            
            # 오류 발생 시 기존 방식으로 체크 (폴백)
            # 주말 체크
            if current_time.weekday() >= 5:  # 5: 토요일, 6: 일요일
                self.logger.info("주말은 거래가 불가능합니다.")
                return False
                
            # 장 시작 시간과 종료 시간 체크
            current_time_str = current_time.strftime('%H%M')
            if not (self.config['trading']['usa_market_start'] <= current_time_str <= self.config['trading']['usa_market_end']):
                self.logger.info("현재 미국 장 운영 시간이 아닙니다.")
                return False
                
            return True
    
    def _is_market_open_time(self) -> bool:
        """시가 매수 시점인지 확인합니다."""
        # 미국 현지 시간으로 확인
        current_time = datetime.now(self.us_timezone).strftime('%H%M')
        start_time = self.config['trading']['usa_market_start']
        
        # 장 시작 후 10분 이내
        return start_time <= current_time <= str(int(start_time) + 10).zfill(4)
        
    def _is_market_close_time(self) -> bool:
        """종가 매수 시점인지 확인합니다."""
        # 미국 현지 시간으로 확인
        current_time = datetime.now(self.us_timezone).strftime('%H%M')
        end_time = self.config['trading']['usa_market_end']

        # 장 마감 30분 전부터 10분 동안
        close_start = str(int(end_time) - 30).zfill(4)
        close_end = str(int(end_time) - 20).zfill(4)
        return close_start <= current_time <= close_end
        
    def calculate_ma(self, stock_code: str, period: int = 20) -> Optional[float]:
        """이동평균선을 계산합니다."""
        try:
            end_date = datetime.now(self.us_timezone).strftime("%Y%m%d")
            # 이동평균 계산을 위해 필요한 데이터 기간 (기본 2배로 설정)
            required_days = period * 2
            start_date = (datetime.now(self.us_timezone) - timedelta(days=required_days)).strftime("%Y%m%d")
            
            # API 제한(100일)을 고려하여 데이터 조회
            all_data = []
            current_end_date = datetime.now(self.us_timezone)
            start_datetime = datetime.now(self.us_timezone) - timedelta(days=required_days)
            
            # 필요한 기간이 100일 이하인 경우 한 번에 조회
            if required_days <= 100:
                df = self.us_api.get_daily_price(stock_code, start_date, end_date)
                if df is None or len(df) < period:
                    return None
                    
                ma = df['clos'].rolling(window=period).mean().iloc[-2]  # 전일 종가의 이동평균값
                return ma
            
            # 필요한 기간이 100일 초과인 경우 분할 조회
            while current_end_date.replace(tzinfo=None) >= start_datetime.replace(tzinfo=None):
                # 현재 조회 기간의 시작일 계산 (최대 100일)
                current_start_date = current_end_date - timedelta(days=99)
                
                # 시작일보다 이전으로 가지 않도록 조정
                if current_start_date.replace(tzinfo=None) < start_datetime.replace(tzinfo=None):
                    current_start_date = start_datetime
                
                # 날짜 형식 변환
                current_start_date_str = current_start_date.strftime("%Y%m%d")
                current_end_date_str = current_end_date.strftime("%Y%m%d")
                
                self.logger.debug(f"이동평균 계산을 위한 일별 시세 조회: {stock_code}, {current_start_date_str} ~ {current_end_date_str}")
                
                # API 호출하여 데이터 조회
                df = self.us_api.get_daily_price(stock_code, current_start_date_str, current_end_date_str)
                if df is not None and len(df) > 0:
                    all_data.append(df)
                
                # 다음 조회 기간 설정 (하루 겹치지 않게)
                current_end_date = current_start_date - timedelta(days=1)
                
                # 시작일에 도달하면 종료
                if current_end_date.replace(tzinfo=None) < start_datetime.replace(tzinfo=None):
                    break
            
            # 조회된 데이터가 없는 경우
            if not all_data:
                return None
            
            # 모든 데이터 합치기
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 중복 제거 (날짜 기준)
            if 'xymd' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['xymd'])
                combined_df = combined_df.sort_values('xymd', ascending=True).reset_index(drop=True)
            
            # 데이터가 충분한지 확인
            if len(combined_df) < period:
                self.logger.warning(f"{stock_code}: 이동평균 계산을 위한 데이터가 부족합니다. (필요: {period}일, 실제: {len(combined_df)}일)")
                return None
            
            # 이동평균 계산
            ma = combined_df['clos'].astype(float).rolling(window=period).mean().iloc[-2]  # 전일 종가의 이동평균값
            return ma
        except Exception as e:
            self.logger.error(f"{period}일 이동평균 계산 실패 ({stock_code}): {str(e)}")
            return None
    
    def get_highest_price_since_first_buy(self, stock_code: str) -> float:
        """최초 매수일 이후부터 어제까지의 최고가를 조회합니다."""
        try:
            # 현재 날짜 확인
            current_date = datetime.now(self.us_timezone).strftime("%Y-%m-%d")
            
            # 캐시가 오늘 날짜의 데이터이고, 해당 종목의 최고가가 캐시에 있으면 캐시된 값 반환
            if (self.highest_price_cache_date == current_date and 
                stock_code in self.highest_price_cache):
                self.logger.debug(f"캐시된 최고가 사용: {stock_code}, {self.highest_price_cache[stock_code]}")
                return self.highest_price_cache[stock_code]
            
            # 날짜가 변경되었으면 캐시 초기화
            if self.highest_price_cache_date != current_date:
                self.highest_price_cache = {}
                self.highest_price_cache_date = current_date
                self.logger.info(f"최고가 캐시 초기화 (날짜 변경: {current_date})")
            
            # 최초 매수일 조회
            first_buy_date = self.trade_history.get_first_buy_date(stock_code)
            if not first_buy_date:
                return 0
            
            # 최초 매수일부터 어제까지의 일별 시세 조회
            first_date = datetime.strptime(first_buy_date, "%Y-%m-%d")
            # 어제 날짜 계산 (오늘 날짜에서 하루 빼기)
            end_date = datetime.now(self.us_timezone) - timedelta(days=1)
            
            # 최초 매수일이 어제보다 늦은 경우(즉, 오늘 처음 매수한 경우) 최고가는 매수가로 설정
            if first_date > end_date.replace(tzinfo=None):
                self.logger.debug(f"{stock_code}: 최초 매수일({first_date.strftime('%Y-%m-%d')})이 어제보다 늦어 최고가 계산 불가")
                return 0
            
            # API 제한(100일)을 고려하여 데이터 조회
            all_data = []
            current_end_date = end_date
            
            # 날짜 비교 시 타임존 정보가 없는 경우 오류가 발생할 수 있으므로 타임존 정보 제거
            while current_end_date.replace(tzinfo=None) >= first_date:
                # 현재 조회 기간의 시작일 계산 (최대 100일)
                current_start_date = current_end_date - timedelta(days=99)
                
                # 최초 매수일보다 이전으로 가지 않도록 조정
                if current_start_date.replace(tzinfo=None) < first_date:
                    current_start_date = first_date
                
                # 날짜 형식 변환
                start_date_str = current_start_date.strftime("%Y%m%d")
                end_date_str = current_end_date.strftime("%Y%m%d")
                
                self.logger.debug(f"일별 시세 조회: {stock_code}, {start_date_str} ~ {end_date_str}")
                
                # API 호출하여 데이터 조회
                df = self.us_api.get_daily_price(stock_code, start_date_str, end_date_str)
                if df is not None and len(df) > 0:
                    all_data.append(df)
                
                # 다음 조회 기간 설정 (하루 겹치지 않게)
                current_end_date = current_start_date - timedelta(days=1)
                
                # 최초 매수일에 도달하면 종료
                if current_end_date.replace(tzinfo=None) < first_date:
                    break
            
            # 조회된 데이터가 없는 경우
            if not all_data:
                return 0
            
            # 모든 데이터 합치기
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 중복 제거 (날짜 기준)
            combined_df = combined_df.drop_duplicates(subset=['xymd'])
            
            # 최초 매수일부터 어제까지의 데이터만 필터링
            combined_df = combined_df[(combined_df['xymd'] >= pd.to_datetime(first_buy_date, format='%Y-%m-%d')) & 
                                     (combined_df['xymd'] <= pd.to_datetime(end_date.strftime('%Y-%m-%d')))]
            
            # 최고가 계산
            highest_price = combined_df['high'].astype(float).max()
            
            # 캐시에 저장
            self.highest_price_cache[stock_code] = highest_price
            self.logger.debug(f"최고가 캐시 업데이트: {stock_code}, {highest_price}")
            
            return highest_price
            
        except Exception as e:
            self.logger.error(f"최고가 조회 실패 ({stock_code}): {str(e)}")
            return 0
        
    def check_buy_condition(self, stock_code: str, ma_period: int, prev_close: float) -> tuple[bool, Optional[float]]:
        """매수 조건을 확인합니다."""
        try:
            # 5일과 지정된 기간의 이동평균선 계산
            ma5 = self.calculate_ma(stock_code, 5)
            ma_target = self.calculate_ma(stock_code, ma_period)
            
            if ma5 is None or ma_target is None:
                return False, None
                
            # 전일 데이터 조회
            end_date = datetime.now(self.us_timezone).strftime("%Y%m%d")
            start_date = (datetime.now(self.us_timezone) - timedelta(days=2)).strftime("%Y%m%d")
            
            df = self.us_api.get_daily_price(stock_code, start_date, end_date)
            if df is None or len(df) < 2:
                return False, None
                
            # 전일과 전전일의 5일 이동평균선
            ma5_prev = df['clos'].rolling(window=5).mean().iloc[-2]  # 전일
            ma5_prev2 = df['clos'].rolling(window=5).mean().iloc[-3]  # 전전일
            
            # 전일과 전전일의 지정된 이동평균선
            ma_target_prev = df['clos'].rolling(window=ma_period).mean().iloc[-2]  # 전일
            ma_target_prev2 = df['clos'].rolling(window=ma_period).mean().iloc[-3]  # 전전일
            
            # 골든크로스 조건 확인
            # 전전일: 5일선 < 지정된 이평선
            # 전일: 5일선 > 지정된 이평선
            golden_cross = (ma5_prev2 < ma_target_prev2) and (ma5_prev > ma_target_prev)
            
            if golden_cross:
                self.logger.info(f"골든크로스 발생: {stock_code}")
                self.logger.info(f"- 전전일: 5일선(${ma5_prev2:.2f}) < {ma_period}일선(${ma_target_prev2:.2f})")
                self.logger.info(f"- 전일: 5일선(${ma5_prev:.2f}) > {ma_period}일선(${ma_target_prev:.2f})")
            
            return golden_cross, ma_target
            
        except Exception as e:
            self.logger.error(f"매수 조건 확인 중 오류 발생 ({stock_code}): {str(e)}")
            return False, None
        
    def check_sell_condition(self, stock_code: str, ma_period: int, prev_close: float) -> tuple[bool, Optional[float]]:
        """매도 조건을 확인합니다."""
        ma = self.calculate_ma(stock_code, ma_period)
        if ma is None:
            return False, None
        return bool(prev_close < ma), ma
        
    def get_today_sold_stocks(self) -> List[str]:
        """API를 통해 당일 매도한 종목 코드 목록을 조회합니다.
        
        Returns:
            List[str]: 당일 매도한 종목 코드 목록
        """
        sold_stocks = []
        try:
            # 당일 체결 내역 조회
            executed_orders = self._retry_api_call(self.us_api.get_today_executed_orders)
            
            if executed_orders and 'output' in executed_orders:
                for order in executed_orders['output']:
                    # 매도 주문만 필터링 (01: 매도)
                    if order['sll_buy_dvsn_cd'] == '01':
                        stock_code = order['pdno']
                        # 체결 수량이 있는 경우만 추가
                        if int(order['ft_ccld_qty']) > 0:
                            if stock_code not in sold_stocks:
                                sold_stocks.append(stock_code)
                                self.logger.debug(f"당일 매도 종목 확인: {order['prdt_name']}({stock_code})")
            
            return sold_stocks
        except Exception as e:
            self.logger.error(f"당일 매도 종목 조회 중 오류 발생: {str(e)}")
            # 오류 발생 시 파일에 저장된 정보 반환
            return super().get_today_sold_stocks()
        
    def get_trailing_stop_sell_price(self, stock_code: str) -> Optional[float]:
        """트레일링 스탑으로 매도된 종목의 매도 가격을 조회합니다.
        
        Args:
            stock_code (str): 종목 코드
            
        Returns:
            Optional[float]: 트레일링 스탑 매도 가격, 없으면 None
        """
        try:
            # 거래 내역에서 해당 종목의 트레일링 스탑 매도 내역 조회
            trailing_stop_trades = self.trade_history.get_trades_by_type_and_code("TRAILING_STOP", stock_code)
            
            if trailing_stop_trades and len(trailing_stop_trades) > 0:
                # 가장 최근 트레일링 스탑 매도 가격 반환
                latest_trade = trailing_stop_trades[-1]
                return float(latest_trade.get("price", 0))
            
            return None
        except Exception as e:
            self.logger.error(f"트레일링 스탑 매도 가격 조회 중 오류 발생 ({stock_code}): {str(e)}")
            return None
        
    def _is_rebalancing_day(self) -> bool:
        """리밸런싱 실행 여부를 확인합니다.
        
        리밸런싱 날짜 형식:
        1. 년/월/일 (예: 2023/12/15) - 해당 년월일에 리밸런싱
        2. 월/일 (예: 12/15) - 매년 해당 월일에 리밸런싱
        3. 일 (예: 15) - 매월 해당 일자에 리밸런싱
        """
        try:
            # 리밸런싱 일자 확인
            rebalancing_date = self.settings.get('rebalancing_date', '')
            if not rebalancing_date:
                return False
            
            # 현재 날짜/시간 확인 (미국 시간 기준)
            now = datetime.now(self.us_timezone)
            
            # 리밸런싱 일자 파싱
            rebalancing_date_str = str(rebalancing_date).strip()
            
            # 구분자로 분리된 경우 (년/월/일 또는 월/일)
            for sep in ['/', '-', '.']:
                if sep in rebalancing_date_str:
                    parts = rebalancing_date_str.split(sep)
                    
                    # 년/월/일 형식 (예: 2023/12/15)
                    if len(parts) == 3:
                        year, month, day = map(int, parts)
                        if now.year == year and now.month == month and now.day == day:
                            self.logger.info(f"리밸런싱 날짜 도달: {year}/{month}/{day}")
                            return True
                        return False
                    
                    # 월/일 형식 (예: 12/15)
                    elif len(parts) == 2:
                        month, day = map(int, parts)
                        if now.month == month and now.day == day:
                            self.logger.info(f"리밸런싱 날짜 도달: 매년 {month}/{day}")
                            return True
                        return False
            
            # 숫자만 있는 경우 (일자만 지정, 예: "15")
            if rebalancing_date_str.isdigit():
                day = int(rebalancing_date_str)
                if now.day == day:
                    self.logger.info(f"리밸런싱 날짜 도달: 매월 {day}일")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"리밸런싱 일자 확인 중 오류 발생: {str(e)}")
            return False

    def _rebalance_portfolio(self, balance: Dict):
        """포트폴리오 리밸런싱을 실행합니다."""
        try:
            self.logger.info("포트폴리오 리밸런싱을 시작합니다.")
            
            # 총 평가금액 계산
            total_balance = self._retry_api_call(self.us_api.get_total_balance)
            if total_balance is None:
                return
            
            # USD 통화 정보 찾기
            usd_balance = next(x for x in total_balance['output2'] if x['crcy_cd'] == 'USD')
            
            # 총자산금액을 환율로 나누어 달러로 환산
            total_assets = float(total_balance['output3']['tot_asst_amt']) / float(usd_balance['frst_bltn_exrt'])
            
            # 구글 시트에서 리밸런싱 비율 가져오기
            rebalancing_ratio = float(self.settings.get('rebalancing_ratio', 0))
            if not rebalancing_ratio:
                self.logger.error("리밸런싱 비율이 설정되지 않았습니다.")
                return
            
            # 보유 종목별 현재 비율 계산
            holdings = {}
            for holding in balance['output1']:
                if int(holding.get('ord_psbl_qty', 0)) > 0:
                    stock_code = holding['ovrs_pdno']
                    exchange = holding.get('ovrs_excg_cd', '')  # NASD, NYSE, AMEX
                    full_stock_code = f"{stock_code}.{exchange}"
                    
                    current_price_data = self._retry_api_call(self.us_api.get_stock_price, full_stock_code)
                    if current_price_data is None:
                        continue
                        
                    current_price = float(current_price_data['output']['last'])
                    quantity = int(holding['ord_psbl_qty'])
                    current_value = current_price * quantity
                    current_ratio = current_value / total_assets * 100
                    
                    # 개별 종목에서 찾기
                    individual_match = self.individual_stocks[self.individual_stocks['종목코드'] == stock_code]
                    if not individual_match.empty:
                        stock_info = individual_match.iloc[0]
                        target_ratio = float(stock_info['배분비율'])
                    else:
                        # POOL 종목에서 찾기
                        pool_match = self.pool_stocks[self.pool_stocks['종목코드'] == stock_code]
                        if not pool_match.empty:
                            stock_info = pool_match.iloc[0]
                            target_ratio = float(stock_info['배분비율'])
                    
                    if stock_info is not None:
                        # 리밸런싱 비율 적용 (rebalancing_ratio가 1일 때 100%를 의미)
                        # 배분비율과 리밸런싱 비율을 곱하여 최종 목표 비율 계산
                        adjusted_target_ratio = target_ratio * rebalancing_ratio
                        holdings[full_stock_code] = {
                            'name': stock_info['종목명'],
                            'current_price': current_price,
                            'quantity': quantity,
                            'current_value': current_value,
                            'current_ratio': current_ratio,
                            'target_ratio': adjusted_target_ratio
                        }
            
            # 리밸런싱 실행
            for stock_code, info in holdings.items():
                ratio_diff = info['target_ratio'] - info['current_ratio']
                
                # 최소 리밸런싱 비율 차이 (1% 이상)
                if abs(ratio_diff) >= 0.0:
                    target_value = total_assets * (info['target_ratio'] / 100)
                    value_diff = target_value - info['current_value']
                    quantity_diff = int(value_diff / info['current_price'])
                    
                    if quantity_diff > 0:  # 매수
                        # 매수 시 지정가의 1% 높게 설정하여 시장가처럼 거래
                        buy_price = info['current_price'] * 1.01
                        
                        result = self._retry_api_call(
                            self.us_api.order_stock,
                            stock_code,
                            "BUY",
                            quantity_diff,
                            buy_price
                        )
                        if result:
                            msg = f"리밸런싱 매수: {info['name']}({stock_code}) {quantity_diff}주"
                            msg += f"\n- 현재 비중: {info['current_ratio']:.1f}% → 목표 비중: {info['target_ratio']:.1f}%"
                            msg += f"\n- 현재가: ${info['current_price']:.2f}"
                            msg += f"\n- 매수 금액: ${value_diff:,.2f}"
                            self.logger.info(msg)
                            
                            # 거래 내역 저장
                            trade_data = {
                                "trade_type": "REBALANCE_BUY",
                                "trade_action": "BUY",
                                "stock_code": stock_code,
                                "stock_name": info['name'],
                                "quantity": quantity_diff,
                                "price": info['current_price'],
                                "total_amount": abs(value_diff),
                                "reason": f"리밸런싱 매수 (현재 비중 {info['current_ratio']:.1f}% → 목표 비중 {info['target_ratio']:.1f}%)",
                                "order_type": "BUY"
                            }
                            self.trade_history.add_trade(trade_data)
                            
                    elif quantity_diff < 0:  # 매도
                        # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                        sell_price = info['current_price'] * 0.99
                        
                        result = self._retry_api_call(
                            self.us_api.order_stock,
                            stock_code,
                            "SELL",
                            abs(quantity_diff),
                            sell_price
                        )
                        if result:
                            msg = f"리밸런싱 매도: {info['name']}({stock_code}) {abs(quantity_diff)}주"
                            msg += f"\n- 현재 비중: {info['current_ratio']:.1f}% → 목표 비중: {info['target_ratio']:.1f}%"
                            msg += f"\n- 현재가: ${info['current_price']:.2f}"
                            msg += f"\n- 매도 금액: ${abs(value_diff):,.2f}"
                            self.logger.info(msg)
                            
                            # 거래 내역 저장
                            trade_data = {
                                "trade_type": "REBALANCE_SELL",
                                "trade_action": "SELL",
                                "stock_code": stock_code,
                                "stock_name": info['name'],
                                "quantity": abs(quantity_diff),
                                "price": info['current_price'],
                                "total_amount": abs(value_diff),
                                "reason": f"리밸런싱 매도 (현재 비중 {info['current_ratio']:.1f}% → 목표 비중 {info['target_ratio']:.1f}%)",
                                "profit_loss": (info['current_price'] - info['current_price']) * abs(quantity_diff),
                                "profit_loss_pct": 0.0
                            }
                            self.trade_history.add_trade(trade_data)
                            
                            # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                            self.sold_stocks_cache_time = 0
            
            self.logger.info("포트폴리오 리밸런싱이 완료되었습니다.")
            
        except Exception as e:
            self.logger.error(f"포트폴리오 리밸런싱 중 오류 발생: {str(e)}")

    def execute_trade(self):
        """매매를 실행합니다."""
        try:
            now = datetime.now(self.us_timezone)
            current_date = now.date()
            
            # 당일 최초 실행 여부 확인 및 초기화
            if self.execution_date != current_date:
                self.execution_date = current_date
                self.market_open_executed = False
                self.market_close_executed = False
                self.sold_stocks_cache = []  # 당일 매도 종목 캐시 초기화
                self.sold_stocks_cache_time = 0  # 캐시 시간 초기화
                self.logger.info(f"=== {self.execution_date} 매매 시작 ===")
            
            # 장 운영 시간이 아니면 스킵
            if not self.check_market_condition():
                return
            
            # 계좌 잔고 조회
            balance = self._retry_api_call(self.us_api.get_account_balance)
            if balance is None:
                self.logger.error("계좌 잔고 조회 실패")
                return
            
            # 0. 스탑로스/트레일링 스탑 체크 (최우선 실행)
            self._check_stop_conditions()
            
            # 장 시작 매매 조건
            is_market_open = self._is_market_open_time() and not self.market_open_executed
            # 종가 매수 조건
            is_market_close = self._is_market_close_time() and not self.market_close_executed
            
            # 1. 장 시작 시점에 매도 조건 체크 및 실행
            if is_market_open:
                self.logger.info(f"[미국 시장] 장 시작 매매 시점 도달")
                self.logger.info(f"1. 매도 조건 체크 시작")
                
                # 매도 조건 체크 및 실행
                self._process_sell_conditions(balance)
                
                # 매도 후 잔고 다시 조회
                balance = self._retry_api_call(self.us_api.get_account_balance)
                if balance is None:
                    self.logger.error("계좌 잔고 조회 실패")
                    return
                
                # 2. 리밸런싱 체크 및 실행 (매도 이후에 실행)
                if self._is_rebalancing_day():
                    self.logger.info(f"2. 리밸런싱 실행")
                    self._rebalance_portfolio(balance)
                    
                    # 리밸런싱 후 잔고 다시 조회
                    balance = self._retry_api_call(self.us_api.get_account_balance)
                    if balance is None:
                        self.logger.error("계좌 잔고 조회 실패")
                        return
                
                # 3. 시가 매수 실행
                self.logger.info(f"3. 시가 매수 실행")
                self.logger.info(f"미국 시장은 매수 종목의 {self.settings['market_open_ratio']*100:.0f}%를 장 시작 시점에 매수합니다.")
                self._process_buy_conditions(balance, is_market_open=True, is_market_close=False)
                
                self.market_open_executed = True
                self.logger.info("장 시작 매매 실행 완료")
            
            # 4. 종가 매수 실행
            elif is_market_close:
                self.logger.info(f"[미국 시장] 장 종료 매매 시점 도달")
                self.logger.info(f"4. 종가 매수 실행")
                self.logger.info(f"미국 시장은 매수 종목의 {self.settings['market_close_ratio']*100:.0f}%를 장 종료 시점에 매수합니다.")
                
                # 종가 매수 실행
                self._process_buy_conditions(balance, is_market_open=False, is_market_close=True)
                
                self.market_close_executed = True
                self.logger.info("종가 매매 실행 완료")
            
        except Exception as e:
            error_msg = f"매매 실행 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
    
    def _process_sell_conditions(self, balance: Dict):
        """매도 조건 처리"""
        try:
            # 보유 종목 확인
            for holding in balance['output1']:
                # 거래 가능 수량이 있는 경우만 처리
                quantity = int(holding.get('ord_psbl_qty', 0))
                if quantity <= 0:
                    continue
                    
                # 거래소와 종목코드 결합
                exchange = holding.get('ovrs_excg_cd', '')  # NASD, NYSE, AMEX
                stock_code = f"{holding['ovrs_pdno']}.{exchange}"       # ????
                stock_name = holding['ovrs_item_name']
                
                # 매도 조건 확인
                ma_period = 0
                for _, row in self.individual_stocks.iterrows():
                    if row['종목코드'] == holding['ovrs_pdno']:
                        ma_period = int(row['매매기준'])
                        break
                
                if ma_period == 0:
                    for _, row in self.pool_stocks.iterrows():
                        if row['종목코드'] == holding['ovrs_pdno']:
                            ma_period = int(row['매매기준'])
                            break
                
                if ma_period == 0:
                    self.logger.warning(f"{stock_name}({stock_code})의 매매기준을 찾을 수 없습니다.")
                    continue
                
                # 현재가 조회
                current_price_data = self._retry_api_call(self.us_api.get_stock_price, stock_code)
                if current_price_data is None:
                    self.logger.warning(f"{stock_name}({stock_code})의 현재가를 조회할 수 없습니다.")
                    continue
                    
                current_price = float(current_price_data['output']['last'])
                prev_close = float(current_price_data['output']['base'])
                
                # 매도 조건 확인 - 전일 종가를 기준으로 판단
                sell_condition, ma = self.check_sell_condition(stock_code, ma_period, prev_close)
                
                if ma is None:
                    self.logger.warning(f"{stock_name}({stock_code})의 이동평균을 계산할 수 없습니다.")
                    continue
                    
                if sell_condition:
                    self.logger.info(f"{stock_name}({stock_code}) - 매도 조건 충족: 전일 종가 ${prev_close:.2f} < MA{ma_period} ${ma:.2f}")
                    
                    # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                    sell_price = current_price * 0.99
                    
                    # 매도 주문 실행
                    result = self._retry_api_call(self.us_api.order_stock, stock_code, "SELL", quantity, sell_price)
                    
                    if result:
                        msg = f"매도 주문 실행: {stock_name} {quantity}주 (지정가)"
                        msg += f"\n- 매도 사유: {ma_period}일선 매도 조건 충족 (전일 종가 ${prev_close:.2f} < MA ${ma:.2f})"
                        msg += f"\n- 매도 금액: ${current_price * quantity:,.2f} (현재가 ${current_price:.2f})"
                        
                        # 매수 평균가 가져오기
                        avg_price = float(holding.get('pchs_avg_pric', 0))
                        if avg_price <= 0:
                            avg_price = prev_close  # 매수 평균가가 없으면 전일 종가 사용
                            
                        msg += f"\n- 매수 정보: 매수단가 ${avg_price:.2f} / 평가손익 ${(current_price - avg_price) * quantity:,.2f}"
                        msg += f"\n- 매도 수익률: {((current_price - avg_price) / avg_price * 100):.2f}% (매수가 ${avg_price:,.2f})"
                        self.logger.info(msg)
                        
                        # 거래 내역 저장
                        trade_data = {
                            "trade_type": "SELL",
                            "trade_action": "SELL",
                            "stock_code": stock_code,
                            "stock_name": stock_name,
                            "quantity": quantity,
                            "price": current_price,
                            "total_amount": quantity * current_price,
                            "ma_period": ma_period,
                            "ma_value": ma,
                            "reason": f"{ma_period}일선 매도 조건 충족 (전일 종가 ${prev_close:.2f} < MA ${ma:.2f})",
                            "profit_loss": (current_price - avg_price) * quantity,
                            "profit_loss_pct": (current_price - avg_price) / avg_price * 100
                        }
                        self.trade_history.add_trade(trade_data)
                        
                        # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                        self.sold_stocks_cache_time = 0
        
        except Exception as e:
            self.logger.error(f"매도 조건 처리 중 오류 발생: {str(e)}")
    
    def _process_buy_conditions(self, balance: Dict, is_market_open: bool, is_market_close: bool):
        """매수 조건을 체크하고 실행합니다."""
        try:
            # 개별 종목 매수
            for _, row in self.individual_stocks.iterrows():
                if row['거래소'] != "KOR":  # 미국 주식만 처리
                    self._process_single_stock_buy(row, balance, is_market_open, is_market_close)
            
            # POOL 종목 매수
            for _, row in self.pool_stocks.iterrows():
                if row['거래소'] != "KOR":  # 미국 주식만 처리
                    self._process_single_stock_buy(row, balance, is_market_open, is_market_close)
        
        except Exception as e:
            self.logger.error(f"매수 조건 체크 중 오류 발생: {str(e)}")
    
    def _process_single_stock_buy(self, row: pd.Series, balance: Dict, is_market_open: bool, is_market_close: bool):
        """단일 종목의 매수를 처리합니다."""
        try:
            # 거래소와 종목코드 결합
            stock_code = f"{row['종목코드']}.{row['거래소']}"
            ma_period = int(row['매매기준']) if row['매매기준'] and str(row['매매기준']).strip() != '' else 20
            allocation_ratio = float(row['배분비율']) / 100 if row['배분비율'] and str(row['배분비율']).strip() != '' else 0.1
            
            # 현재가 조회 (재시도 로직 적용)
            current_price_data = self._retry_api_call(self.us_api.get_stock_price, stock_code)
            if current_price_data is None:
                return
            
            current_price = float(current_price_data['output']['last'])
            prev_close = float(current_price_data['output']['base'])
            
            # 보유 종목 확인
            holdings = [h for h in balance['output1'] if h['ovrs_pdno'] == stock_code.split('.')[0]]
            is_holding = len(holdings) > 0
            
            # 시가 매수 처리
            if is_market_open:
                # 이미 보유 중이면 매수하지 않음
                if is_holding:
                    return
                
                # 당일 매도한 종목은 스킵
                if self.is_sold_today(stock_code):
                    self.logger.info(f"{row['종목명']}({stock_code}) - 당일 매도 종목 재매수 제한")
                    return
                
                # 트레일링 스탑으로 매도된 종목 체크
                trailing_stop_price = self.get_trailing_stop_sell_price(stock_code.split('.')[0])
                if trailing_stop_price is not None:
                    if current_price < trailing_stop_price:
                        msg = f"트레일링 스탑 매도 종목 재매수 제한 - {row['종목명']}({stock_code})"
                        msg += f"\n- 현재가(${current_price:.2f})가 트레일링 스탑 매도가(${trailing_stop_price:.2f}) 미만"
                        self.logger.info(msg)
                        return
                    else:
                        msg = f"트레일링 스탑 매도 종목 재매수 가능 - {row['종목명']}({stock_code})"
                        msg += f"\n- 현재가(${current_price:.2f})가 트레일링 스탑 매도가(${trailing_stop_price:.2f}) 이상"
                        self.logger.info(msg)

                should_buy, ma = self.check_buy_condition(stock_code, ma_period, prev_close)
                if should_buy and ma is not None:
                    # 당일 매도 종목 체크
                    if self.is_sold_today(stock_code):
                        msg = f"당일 매도 종목 재매수 제한 - {row['종목명']}({stock_code})"
                        self.logger.info(msg)
                        return
                    
                    trade_msg = f"매수 조건 성립 - {row['종목명']}({stock_code}): 5일선이 {ma_period}일선을 상향돌파"
                    self.logger.info(trade_msg)
                    
                    # 최대 보유 종목 수 체크 (개별 종목과 POOL 종목 각각 체크)
                    total_individual_holdings = len([h for h in balance['output1'] if int(h.get('ord_psbl_qty', 0)) > 0 and any(s['종목코드'] == h['ovrs_pdno'].split('.')[0] for _, s in self.individual_stocks.iterrows())])
                    total_pool_holdings = len([h for h in balance['output1'] if int(h.get('ord_psbl_qty', 0)) > 0 and any(s['종목코드'] == h['ovrs_pdno'].split('.')[0] for _, s in self.pool_stocks.iterrows())])
                    
                    # 현재 종목이 개별 종목인지 POOL 종목인지 확인
                    is_individual = any(s['종목코드'] == stock_code.split('.')[0] for _, s in self.individual_stocks.iterrows())
                    max_stocks = self.settings['max_individual_stocks'] if is_individual else self.settings['max_pool_stocks']
                    current_holdings = total_individual_holdings if is_individual else total_pool_holdings
                    
                    if current_holdings >= max_stocks:
                        msg = f"최대 보유 종목 수({max_stocks}개) 초과로 매수 보류: {row['종목명']}"
                        self.logger.info(msg)
                        return
                    
                    buyable_data = self._retry_api_call(self.us_api.get_psbl_amt, stock_code)
                    if buyable_data is None:
                        return
                    
                    # 주문가능금액 확인
                    available_cash = float(buyable_data['output']['frcr_ord_psbl_amt1'])     #주문가능금액 - 외화인경우 "ord_psbl_frcr_amt" / 원화인경우 "frcr_ord_psbl_amt1"
                    total_balance = self._retry_api_call(self.us_api.get_total_balance)
                    if total_balance is None:
                        return
                    # 총자산금액을 환율로 나누어 달러로 환산
                    total_assets = float(total_balance['output3']['tot_asst_amt']) / float([x['frst_bltn_exrt'] for x in total_balance['output2'] if x['crcy_cd'] == 'USD'][0])
                    
                    # 매수 가능 금액 계산
                    buy_amount = total_assets * allocation_ratio
                    total_quantity = int(buy_amount / current_price)
                    
                    if total_quantity <= 0:
                        msg = f"매수 자금 부족 - {row['종목명']}({stock_code})"
                        msg += f"\n[시가 매수] 필요자금: ${current_price:.2f}/주 | 가용자금: ${buy_amount*self.settings['market_open_ratio']:.2f}"
                        msg += f"\n[종가 매수] 필요자금: ${current_price:.2f}/주 | 가용자금: ${buy_amount*self.settings['market_close_ratio']:.2f}"
                        self.logger.info(msg)
                        return
                    
                    # 현금 부족 시 POOL 종목 매도 로직
                    market_quantity = int(total_quantity * self.settings['market_open_ratio'])
                    required_cash = market_quantity * current_price
                    
                    if required_cash > available_cash and is_individual:
                        self.logger.info(f"현금 부족: 필요 금액 ${required_cash:.2f}, 가용 금액 ${available_cash:.2f}")
                        self.logger.info(f"POOL 종목 매도를 통한 현금 확보 시도")
                        
                        # POOL 종목 보유 현황 확인
                        pool_holdings = []
                        for holding in balance['output1']:
                            if int(holding.get('ord_psbl_qty', 0)) > 0:
                                stock_code_only = holding['ovrs_pdno']
                                # POOL 종목인지 확인
                                pool_match = self.pool_stocks[self.pool_stocks['종목코드'] == stock_code_only]
                                if not pool_match.empty:
                                    # 현재가 조회
                                    exchange = holding.get('ovrs_excg_cd', '')
                                    full_code = f"{stock_code_only}.{exchange}"
                                    price_data = self._retry_api_call(self.us_api.get_stock_price, full_code)
                                    if price_data is not None:
                                        current_price_pool = float(price_data['output']['last'])
                                        quantity_pool = int(holding['ord_psbl_qty'])
                                        value = current_price_pool * quantity_pool
                                        
                                        pool_holdings.append({
                                            'code': full_code,
                                            'name': holding['ovrs_item_name'],
                                            'quantity': quantity_pool,
                                            'price': current_price_pool,
                                            'value': value
                                        })
                        
                        # 구글 스프레드시트 순서의 역순으로 정렬 (마지막에 추가된 종목부터 매도)
                        pool_codes = self.pool_stocks['종목코드'].tolist()
                        pool_holdings.sort(key=lambda x: pool_codes.index(x['code'].split('.')[0]) if x['code'].split('.')[0] in pool_codes else float('inf'), reverse=True)
                        
                        cash_to_secure = required_cash - available_cash
                        secured_cash = 0
                        sold_stocks = []
                        
                        # 필요한 현금을 확보할 때까지 POOL 종목 매도
                        for pool_stock in pool_holdings:
                            if secured_cash >= cash_to_secure:
                                break
                                
                            sell_quantity = pool_stock['quantity']
                            expected_cash = sell_quantity * pool_stock['price']
                            
                            # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                            sell_price = pool_stock['price'] * 0.99
                            
                            # 매도 주문 실행
                            result = self._retry_api_call(
                                self.us_api.order_stock,
                                pool_stock['code'],
                                "SELL",
                                sell_quantity,
                                sell_price
                            )
                            
                            if result:
                                secured_cash += expected_cash
                                sold_stocks.append(f"{pool_stock['name']}({pool_stock['code']}) {sell_quantity}주 (${expected_cash:.2f})")
                                self.logger.info(f"현금 확보를 위한 POOL 종목 매도: {pool_stock['name']}({pool_stock['code']}) {sell_quantity}주 (${expected_cash:.2f})")
                                
                                # 거래 내역 저장
                                trade_data = {
                                    "trade_type": "SELL",
                                    "trade_action": "SELL",
                                    "stock_code": pool_stock['code'],
                                    "stock_name": pool_stock['name'],
                                    "quantity": sell_quantity,
                                    "price": pool_stock['price'],
                                    "total_amount": expected_cash,
                                    "reason": f"현금 확보를 위한 POOL 종목 매도 (개별 종목 {row['종목명']} 매수 자금 확보)"
                                }
                                self.trade_history.add_trade(trade_data)
                                
                                # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                                self.sold_stocks_cache_time = 0
                        
                        if secured_cash >= cash_to_secure:
                            self.logger.info(f"현금 확보 성공: ${secured_cash:.2f} (필요 금액: ${cash_to_secure:.2f})")
                            self.logger.info(f"매도한 POOL 종목: {', '.join(sold_stocks)}")
                            
                            # 매도 후 충분한 시간 대기 (주문 체결 시간 고려)
                            self.logger.info("매도 주문 체결 대기 중... (5초)")
                            time.sleep(5)  # 매도 주문 체결을 위해 5초 대기
                            
                            # 주문가능금액 다시 확인
                            buyable_data = self._retry_api_call(self.us_api.get_psbl_amt, stock_code)
                            if buyable_data is None:
                                return
                            available_cash = float(buyable_data['output']['frcr_ord_psbl_amt1'])
                        else:
                            self.logger.info(f"현금 확보 실패: ${secured_cash:.2f} (필요 금액: ${cash_to_secure:.2f})")
                            # 현금 확보 실패 시에도 계속 진행 (남은 현금으로 최대한 매수)
                            self.logger.info(f"남은 현금으로 최대한 매수 시도")
                    
                    # 현금 부족 시에도 가능한 최대 수량 계산
                    if available_cash < required_cash:
                        # 시가 매수 수량 재계산 (가용 현금 기준)
                        market_quantity = int(available_cash / current_price)
                        self.logger.info(f"현금 부족으로 매수 수량 조정: {total_quantity} -> {market_quantity}주 (시가 매수)")
                    
                    if market_quantity > 0:
                        # 매수 시 지정가의 1% 높게 설정하여 시장가처럼 거래
                        buy_price = current_price * 1.01
                        
                        result = self._retry_api_call(self.us_api.order_stock, stock_code, "BUY", market_quantity, buy_price)
                        if result:
                            msg = f"매수 주문 실행: {row['종목명']}({stock_code}) {market_quantity}주 (지정가: ${current_price:,.2f})"
                            msg += f"\n- 매수 사유: 5일선이 {ma_period}일선을 상향돌파"
                            msg += f"\n- 매수 정보: 주문가 ${current_price:,.2f} / 총금액 ${current_price * market_quantity:,.2f}"
                            msg += f"\n- 배분비율: {allocation_ratio*100}% (총자산 ${total_assets:,.2f} 중 ${buy_amount:,.2f})"
                            msg += f"\n- 이동평균: {ma_period}일선 ${ma:.2f}"
                            msg += f"\n- 계좌 상태: 총평가금액 ${total_assets:,.2f}"
                            msg += f"\n- 종가 매수 예정: {total_quantity - market_quantity}주 (전체 목표 수량의 {self.settings['market_close_ratio']*100:.0f}%)"
                            self.logger.info(msg)
                            
                            # 거래 내역 저장
                            trade_data = {
                                "trade_type": "BUY",
                                "trade_action": "BUY",
                                "stock_code": stock_code,
                                "stock_name": row['종목명'],
                                "quantity": market_quantity,
                                "price": current_price,
                                "total_amount": current_price * market_quantity,
                                "ma_period": ma_period,
                                "ma_value": ma,
                                "reason": f"5일선이 {ma_period}일선을 상향돌파",
                                "allocation_ratio": allocation_ratio
                            }
                            self.trade_history.add_trade(trade_data)
            
            # 종가 매수 처리
            elif is_market_close:
                # 당일 매수 종목인지 확인 (API를 통해 당일 체결 내역 조회)
                executed_orders = self._retry_api_call(self.us_api.get_today_executed_orders, stock_code)
                if executed_orders is None or 'output' not in executed_orders or not executed_orders['output']:
                    return
                
                # 당일 매수 체결 내역 확인 - 현재 종목에 대한 매수 내역만 필터링
                stock_code_only = stock_code.split('.')[0]  # 거래소 코드 제외한 종목코드만 추출
                
                buy_orders = [order for order in executed_orders['output'] 
                             if order['sll_buy_dvsn_cd'] == '02'  # 매수 주문만 필터링
                             and int(order['ft_ccld_qty']) > 0    # 체결된 주문만 필터링
                             and order['pdno'] == stock_code_only]  # 현재 종목에 대한 주문만 필터링
                
                if not buy_orders:
                    # 당일 시가에 매수한 내역이 없으면 종가 매수 불필요
                    #self.logger.info(f"종가 매수 불필요: {row['종목명']}({stock_code}) - 당일 시가 매수 내역 없음")
                    return
                
                # 이평선 조건 충족 여부 확인
                should_buy, ma = self.check_buy_condition(stock_code, ma_period, prev_close)
                if not should_buy:
                    # 이평선 조건을 충족하지 않으면 종가 매수 불필요
                    self.logger.info(f"종가 매수 불필요: {row['종목명']}({stock_code}) - 골든크로스 조건 미충족")
                    return
                
                # 당일 매수 수량 합계 계산
                total_bought = sum(int(order['ft_ccld_qty']) for order in buy_orders)
                
                # 종목의 총 목표 수량 계산
                buyable_data = self._retry_api_call(self.us_api.get_psbl_amt, stock_code)
                if buyable_data is None:
                    return
                
                total_balance = self._retry_api_call(self.us_api.get_total_balance)
                if total_balance is None:
                    return
                
                # 총자산금액을 환율로 나누어 달러로 환산
                total_assets = float(total_balance['output3']['tot_asst_amt']) / float([x['frst_bltn_exrt'] for x in total_balance['output2'] if x['crcy_cd'] == 'USD'][0])
                
                # 매수 가능 금액 계산
                buy_amount = total_assets * allocation_ratio
                total_target_quantity = int(buy_amount / current_price)
                
                # 종가 매수 목표 수량 계산 (전체 목표 수량의 종가 매수 비율)
                market_close_quantity = int(total_target_quantity * self.settings['market_close_ratio'])
                
                # 추가 매수 수량 계산 (종가 매수 목표 수량)
                additional_quantity = market_close_quantity
                
                if additional_quantity <= 0:
                    return
                
                # 현금 확인 및 필요 시 POOL 종목 매도
                required_cash = additional_quantity * current_price
                available_cash = float(buyable_data['output']['frcr_ord_psbl_amt1'])
                
                if required_cash > available_cash:
                    self.logger.info(f"종가 매수 - 현금 부족: 필요 금액 ${required_cash:.2f}, 가용 금액 ${available_cash:.2f}")
                    self.logger.info(f"종가 매수 - POOL 종목 매도를 통한 현금 확보 시도")
                    
                    # POOL 종목 보유 현황 확인
                    pool_holdings = []
                    for holding in balance['output1']:
                        if int(holding.get('ord_psbl_qty', 0)) > 0:
                            stock_code_only = holding['ovrs_pdno']
                            # POOL 종목인지 확인
                            pool_match = self.pool_stocks[self.pool_stocks['종목코드'] == stock_code_only]
                            if not pool_match.empty:
                                # 현재가 조회
                                exchange = holding.get('ovrs_excg_cd', '')
                                full_code = f"{stock_code_only}.{exchange}"
                                price_data = self._retry_api_call(self.us_api.get_stock_price, full_code)
                                if price_data is not None:
                                    current_price_pool = float(price_data['output']['last'])
                                    quantity_pool = int(holding['ord_psbl_qty'])
                                    value = current_price_pool * quantity_pool
                                    
                                    pool_holdings.append({
                                        'code': full_code,
                                        'name': holding['ovrs_item_name'],
                                        'quantity': quantity_pool,
                                        'price': current_price_pool,
                                        'value': value
                                    })
                    
                    # 구글 스프레드시트 순서의 역순으로 정렬 (마지막에 추가된 종목부터 매도)
                    pool_codes = self.pool_stocks['종목코드'].tolist()
                    pool_holdings.sort(key=lambda x: pool_codes.index(x['code'].split('.')[0]) if x['code'].split('.')[0] in pool_codes else float('inf'), reverse=True)
                    
                    cash_to_secure = required_cash - available_cash
                    secured_cash = 0
                    sold_stocks = []
                    
                    # 필요한 현금을 확보할 때까지 POOL 종목 매도
                    for pool_stock in pool_holdings:
                        if secured_cash >= cash_to_secure:
                            break
                            
                        sell_quantity = pool_stock['quantity']
                        expected_cash = sell_quantity * pool_stock['price']
                        
                        # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                        sell_price = pool_stock['price'] * 0.99
                        
                        # 매도 주문 실행
                        result = self._retry_api_call(
                            self.us_api.order_stock,
                            pool_stock['code'],
                            "SELL",
                            sell_quantity,
                            sell_price
                        )
                        
                        if result:
                            secured_cash += expected_cash
                            sold_stocks.append(f"{pool_stock['name']}({pool_stock['code']}) {sell_quantity}주 (${expected_cash:.2f})")
                            self.logger.info(f"종가 매수 - 현금 확보를 위한 POOL 종목 매도: {pool_stock['name']}({pool_stock['code']}) {sell_quantity}주 (${expected_cash:.2f})")
                            
                            # 거래 내역 저장
                            trade_data = {
                                "trade_type": "SELL",
                                "trade_action": "SELL",
                                "stock_code": pool_stock['code'],
                                "stock_name": pool_stock['name'],
                                "quantity": sell_quantity,
                                "price": pool_stock['price'],
                                "total_amount": expected_cash,
                                "reason": f"종가 매수 - 현금 확보를 위한 POOL 종목 매도 (개별 종목 {row['종목명']} 매수 자금 확보)"
                            }
                            self.trade_history.add_trade(trade_data)
                        
                    # 충분한 현금을 확보하지 못한 경우에도 가능한 최대 수량으로 매수 진행
                    if secured_cash < cash_to_secure:
                        self.logger.info(f"종가 매수 - 충분한 현금 확보 실패 (필요: ${cash_to_secure:.2f}, 확보: ${secured_cash:.2f})")
                        self.logger.info(f"종가 매수 - 남은 현금으로 최대한 매수 시도")
                        
                        # 주문가능금액 다시 확인
                        buyable_data = self._retry_api_call(self.us_api.get_psbl_amt, stock_code)
                        if buyable_data is None:
                            return
                        available_cash = float(buyable_data['output']['frcr_ord_psbl_amt1'])
                        
                        # 가능한 최대 수량 재계산
                        additional_quantity = int(available_cash / current_price)
                        if additional_quantity <= 0:
                            self.logger.info(f"종가 매수 취소: {row['종목명']}({stock_code}) - 매수 가능 수량 없음")
                            return
                    else:
                        self.logger.info(f"종가 매수 - 현금 확보 성공: ${secured_cash:.2f} (필요: ${cash_to_secure:.2f})")
                        self.logger.info(f"종가 매수 - 매도한 POOL 종목: {', '.join(sold_stocks)}")
                        
                        # 매도 후 충분한 시간 대기 (주문 체결 시간 고려)
                        self.logger.info("종가 매수 - 매도 주문 체결 대기 중... (5초)")
                        time.sleep(5)  # 매도 주문 체결을 위해 5초 대기
                        
                        # 주문가능금액 다시 확인
                        buyable_data = self._retry_api_call(self.us_api.get_psbl_amt, stock_code)
                        if buyable_data is None:
                            return
                        available_cash = float(buyable_data['output']['frcr_ord_psbl_amt1'])
                
                # 현금 부족 시에도 가능한 최대 수량 계산
                if available_cash < required_cash:
                    # 종가 매수 수량 재계산 (가용 현금 기준)
                    additional_quantity = int(available_cash / current_price)
                    self.logger.info(f"종가 매수 - 현금 부족으로 매수 수량 조정: {market_close_quantity} -> {additional_quantity}주")
                    
                    if additional_quantity <= 0:
                        self.logger.info(f"종가 매수 취소: {row['종목명']}({stock_code}) - 매수 가능 수량 없음")
                        return
                
                # 종가 매수 실행
                # 매수 시 지정가의 1% 높게 설정하여 시장가처럼 거래
                buy_price = current_price * 1.01
                
                result = self._retry_api_call(self.us_api.order_stock, stock_code, "BUY", additional_quantity, buy_price)
                if result:
                    msg = f"종가 매수 주문 실행: {row['종목명']}({stock_code}) {additional_quantity}주 (지정가: ${current_price:.2f})"
                    msg += f"\n- 매수 사유: 5일선이 {ma_period}일선을 상향돌파 종가 매수 (전체 목표의 {self.settings['market_close_ratio']*100:.0f}%)"
                    msg += f"\n- 기존 매수: {total_bought}주 / 추가 매수: {additional_quantity}주 / 총 목표: {total_target_quantity}주"
                    msg += f"\n- 매수 정보: 매수단가 ${current_price:,.2f} / 총금액 ${current_price * additional_quantity:,.2f}"
                    self.logger.info(msg)
                    
                    # 거래 내역 저장
                    trade_data = {
                        "trade_type": "BUY",
                        "trade_action": "BUY",
                        "stock_code": stock_code,
                        "stock_name": row['종목명'],
                        "quantity": additional_quantity,
                        "price": current_price,
                        "total_amount": current_price * additional_quantity,
                        "ma_period": ma_period,
                        "ma_value": ma,
                        "reason": f"5일선이 {ma_period}일선을 상향돌파 종가 매수",
                        "allocation_ratio": allocation_ratio
                    }
                    self.trade_history.add_trade(trade_data)
        
        except Exception as e:
            self.logger.error(f"개별 종목 매수 처리 중 오류 발생: {str(e)}")
    
    def _check_stop_conditions(self):
        """스탑로스와 트레일링 스탑 조건을 체크합니다."""
        try:
            balance = self._retry_api_call(self.us_api.get_account_balance)
            if balance is None:
                return
            
            for holding in balance['output1']:
                # 거래소와 종목코드 결합
                exchange = holding.get('ovrs_excg_cd', '')  # NASD, NYSE, AMEX
                stock_code = f"{holding['ovrs_pdno']}.{exchange}"
                current_price_data = self._retry_api_call(self.us_api.get_stock_price, stock_code)
                if current_price_data is None:
                    continue
                
                current_price = float(current_price_data['output']['last'])
                self._check_stop_conditions_for_stock(holding, current_price)
                
        except Exception as e:
            self.logger.error(f"스탑 조건 체크 중 오류 발생: {str(e)}")
    
    def _check_stop_conditions_for_stock(self, holding: Dict, current_price: float) -> bool:
        """개별 종목의 스탑로스와 트레일링 스탑 조건을 체크합니다."""
        try:
            exchange = holding.get('ovrs_excg_cd', '')  # NASD, NYSE, AMEX
            stock_code = f"{holding['ovrs_pdno']}.{exchange}"
            entry_price = float(holding.get('pchs_avg_pric', 0)) if holding.get('pchs_avg_pric') and str(holding.get('pchs_avg_pric')).strip() != '' else 0
            quantity = int(holding.get('ovrs_cblc_qty', 0)) if holding.get('ovrs_cblc_qty') and str(holding.get('ovrs_cblc_qty')).strip() != '' else 0
            name = holding.get('ovrs_item_name', stock_code)
            
            # 보유 수량이 없는 경우는 정상적인 상황이므로 조용히 리턴
            if quantity <= 0:
                return False
            
            # 매수 평균가가 유효하지 않은 경우에만 경고
            if entry_price <= 0:
                self.logger.warning(f"매수 평균가(${entry_price:.2f})가 유효하지 않습니다: {name}")
                return False
            
            # 계좌 잔고 조회
            total_balance = self._retry_api_call(self.us_api.get_total_balance)
            if total_balance is None:
                return False
            
            # USD 통화 정보 찾기
            usd_balance = next(x for x in total_balance['output2'] if x['crcy_cd'] == 'USD')
            
            # 총자산금액을 환율로 나누어 달러로 환산
            total_assets = float(total_balance['output3']['tot_asst_amt']) / float(usd_balance['frst_bltn_exrt'])
            d2_deposit = float(usd_balance['frcr_dncl_amt_2'])
            
            # 스탑로스 체크
            loss_pct = (current_price - entry_price) / entry_price * 100
            if loss_pct <= self.settings['stop_loss']:
                trade_msg = f"스탑로스 조건 성립 - {name}({stock_code}): 손실률 {loss_pct:.2f}% <= {self.settings['stop_loss']}%"
                self.logger.info(trade_msg)
                
                # 스탑로스 매도
                result = self._retry_api_call(self.us_api.order_stock, stock_code, "SELL", quantity)
                if result:
                    msg = f"스탑로스 매도 실행: {name} {quantity}주 (지정가)"
                    msg += f"\n- 매도 사유: 손실률 {loss_pct:.2f}% (스탑로스 {self.settings['stop_loss']}% 도달)"
                    msg += f"\n- 매도 금액: ${current_price * quantity:,.2f} (현재가 ${current_price:,.2f})"
                    msg += f"\n- 매수 정보: 매수단가 ${entry_price:,.2f} / 평가손익 ${(current_price - entry_price) * quantity:,.2f}"
                    msg += f"\n- 계좌 상태: 총평가금액 ${total_assets:,.2f} / D+2예수금 ${d2_deposit:,.2f}"
                    self.logger.info(msg)
                    
                    # 거래 내역 저장
                    trade_data = {
                        "trade_type": "STOP_LOSS",
                        "trade_action": "SELL",
                        "stock_code": stock_code,
                        "stock_name": name,
                        "quantity": quantity,
                        "price": current_price,
                        "total_amount": quantity * current_price,
                        "reason": f"스탑로스 조건 충족 (손실률 {loss_pct:.2f}% <= {self.settings['stop_loss']}%)",
                        "profit_loss": (current_price - entry_price) * quantity,
                        "profit_loss_pct": loss_pct
                    }
                    self.trade_history.add_trade(trade_data)
                    
                    # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                    self.sold_stocks_cache_time = 0
                return True
            
            # 트레일링 스탑 체크
            # 최고가 조회
            highest_price = self.get_highest_price_since_first_buy(stock_code)
            if highest_price <= 0:
                highest_price = entry_price
            
            # 현재가가 신고가인 경우 업데이트
            if current_price > highest_price:
                # 이전 신고가 대비 상승률 계산
                price_change_pct = (current_price - highest_price) / highest_price * 100
                profit_pct = (current_price - entry_price) / entry_price * 100
                
                # 목표가 초과 시에만 메시지 출력
                if profit_pct >= self.settings['trailing_start']:
                    if price_change_pct >= 1.0:  # 1% 이상 상승 시
                        msg = f"신고가 갱신 - {name}({stock_code})"
                        msg += f"\n- 현재 수익률: +{profit_pct:.1f}% (목표가 {self.settings['trailing_start']}% 초과)"
                        msg += f"\n- 고점 대비 상승: +{price_change_pct:.1f}% (이전 고점 ${highest_price:.2f} → 현재가 ${current_price:.2f})"
                        msg += f"\n- 트레일링 스탑: 현재가 기준 {abs(self.settings['trailing_stop']):.1f}% 하락 시 매도"
                        self.logger.info(msg)
                
                # 현재가를 새로운 최고가로 사용
                highest_price = current_price
                
                # 최고가 캐시 업데이트 (당일 최고가 유지를 위해)
                current_date = datetime.now(self.us_timezone).strftime("%Y-%m-%d")
                if self.highest_price_cache_date != current_date:
                    self.highest_price_cache = {}
                    self.highest_price_cache_date = current_date
                
                self.highest_price_cache[stock_code] = current_price
                self.logger.debug(f"최고가 캐시 업데이트: {stock_code}, ${current_price:.2f}")
            else:
                # 목표가(trailing_start) 초과 여부 확인
                profit_pct = (highest_price - entry_price) / entry_price * 100
                if profit_pct >= self.settings['trailing_start']:  # 목표가 초과 시에만 트레일링 스탑 체크
                    drop_pct = (current_price - highest_price) / highest_price * 100
                    
                    # 1% 이상 하락 시 메시지 출력
                    if drop_pct <= -1.0:
                        msg = f"고점 대비 하락 - {name}({stock_code})"
                        msg += f"\n- 현재 수익률: +{((current_price - entry_price) / entry_price * 100):.1f}%"
                        msg += f"\n- 고점 대비 하락: {drop_pct:.1f}% (고점 ${highest_price:,.2f} → 현재가 ${current_price:,.2f})"
                        msg += f"\n- 트레일링 스탑까지: {abs(self.settings['trailing_stop'] - drop_pct):.1f}% 더 하락하면 매도"
                        self.logger.info(msg)
                    
                    if drop_pct <= self.settings['trailing_stop']:
                        # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                        sell_price = current_price * 0.99
                        
                        result = self._retry_api_call(self.us_api.order_stock, stock_code, "SELL", quantity)
                        if result:
                            msg = f"트레일링 스탑 매도 실행: {name} {quantity}주 (지정가)"
                            msg += f"\n- 매도 사유: 고점 대비 하락률 {drop_pct:.2f}% (트레일링 스탑 {self.settings['trailing_stop']}% 도달)"
                            msg += f"\n- 매도 금액: ${current_price * quantity:,.2f} (현재가 ${current_price:,.2f})"
                            msg += f"\n- 매수 정보: 매수단가 ${entry_price:,.2f} / 평가손익 ${(current_price - entry_price) * quantity:,.2f}"
                            msg += f"\n- 계좌 상태: 총평가금액 ${total_assets:,.2f} / D+2예수금 ${d2_deposit:,.2f}"
                            self.logger.info(msg)
                            
                            # 거래 내역 저장
                            trade_data = {
                                "trade_type": "TRAILING_STOP",
                                "trade_action": "SELL",
                                "stock_code": stock_code,
                                "stock_name": name,
                                "quantity": quantity,
                                "price": current_price,
                                "total_amount": quantity * current_price,
                                "reason": f"트레일링 스탑 조건 충족 (고점 ${highest_price:.2f} 대비 하락률 {drop_pct:.2f}% <= {self.settings['trailing_stop']}%)",
                                "profit_loss": (current_price - entry_price) * quantity,
                                "profit_loss_pct": (current_price - entry_price) / entry_price * 100
                            }
                            self.trade_history.add_trade(trade_data)
                            
                            # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                            self.sold_stocks_cache_time = 0
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"스탑 조건 체크 중 오류 발생 ({stock_code}): {str(e)}")
            return False 

    def update_stock_report(self) -> None:
        """미국 주식 현황을 구글 스프레드시트에 업데이트합니다."""
        try:
            # 계좌 잔고 조회 (inquire-present-balance API 사용)
            balance = self.us_api.get_total_balance()
            if balance is None:
                raise Exception("계좌 잔고 조회 실패")
            
            # 기존 get_account_balance API로 보유 종목 데이터 가져오기
            account_balance = self.us_api.get_account_balance()
            if account_balance is None:
                raise Exception("계좌 잔고 조회 실패")
            
            # 보유 종목 데이터 생성
            holdings_data = []
            for holding in account_balance['output1']:
                if int(holding.get('ovrs_cblc_qty', 0)) <= 0:
                    continue
                
                full_stock_code = f"{holding['ovrs_pdno']}.{holding['ovrs_excg_cd']}"
                current_price_data = self._retry_api_call(self.us_api.get_stock_price, full_stock_code)
                
                if current_price_data:
                    holdings_data.append([
                        holding['ovrs_pdno'],                                           # 종목코드
                        holding['ovrs_item_name'],                           # 종목명
                        round(float(current_price_data['output']['last']), 2),         # 현재가
                        '',                                                  # 구분
                        round(float(current_price_data['output']['rate']), 2),         # 등락률
                        round(float(holding['pchs_avg_pric']), 2),                     # 평단가
                        round(float(holding['evlu_pfls_rt']), 2),                      # 수익률
                        int(holding['ovrs_cblc_qty']),                       # 보유량
                        round(float(holding['frcr_evlu_pfls_amt']), 2),               # 평가손익
                        round(float(holding['frcr_pchs_amt1']), 2),                   # 매입금액
                        round(float(holding['ovrs_stck_evlu_amt']), 2)                # 평가금액
                    ])
            
            # 주식현황 시트 업데이트
            holdings_sheet = self._get_holdings_sheet()
            self._update_holdings_sheet(holdings_data, holdings_sheet)
            
            # 환율 정보 가져오기
            exchange_rate = 1.0  # 기본값
            
            # output2에서 USD 통화에 대한 환율 정보 찾기
            if 'output2' in balance and balance['output2']:
                for currency_info in balance['output2']:
                    if currency_info.get('crcy_cd') == 'USD':
                        exchange_rate = float(currency_info.get('frst_bltn_exrt', 1.0))
                        #self.logger.info(f"현재 환율: 1 USD = {exchange_rate} KRW")
                        break
            
           
            # inquire-present-balance API에서 제공하는 요약 정보 가져오기
            output3 = balance.get('output3', [{}])
            
            # 매입금액합계금액 (pchs_amt_smtl) - 원화를 달러로 변환
            total_purchase_amount = round(float(output3.get('pchs_amt_smtl', 0)) / exchange_rate, 2)
            
            # 평가금액합계금액 (evlu_amt_smtl) - 원화를 달러로 변환
            total_eval_amount = round(float(output3.get('evlu_amt_smtl', 0)) / exchange_rate, 2)
            
            # 총평가손익금액 (tot_evlu_pfls_amt) - 원화를 달러로 변환
            total_eval_profit_loss = round(float(output3.get('tot_evlu_pfls_amt', 0)) / exchange_rate, 2)
            
            # 총자산금액 (tot_asst_amt) - 원화를 달러로 변환
            total_asset_amount = round(float(output3.get('tot_asst_amt', 0)) / exchange_rate, 2)
            
            # 총수익률 계산 (evlu_erng_rt1) - 퍼센트 값이므로 변환 불필요
            total_profit_rate = round(float(output3.get('evlu_erng_rt1', 0)), 2)
            
            # 요약 정보 업데이트
            # 평가손익금액은 F6, 수익률은 G6에 출력
            self.google_sheet.update_range(f"{holdings_sheet}!F6", [[total_eval_profit_loss]])
            self.google_sheet.update_range(f"{holdings_sheet}!G6", [[total_profit_rate]])
            
            # 나머지 정보는 K5:K7에 출력
            summary_data = [
                [total_purchase_amount],  # 매입금액합계금액 (달러)
                [total_eval_amount],      # 평가금액합계금액 (달러)
                [total_asset_amount]      # 총자산금액 (달러)               
            ]
            
            summary_range = f"{holdings_sheet}!K5:K7"
            self.google_sheet.update_range(summary_range, summary_data)
            self.logger.info("미국 주식 요약 정보 업데이트 완료")
            
        except Exception as e:
            self.logger.error(f"미국 주식 현황 업데이트 실패: {str(e)}")
            raise
            
    def _get_holdings_sheet(self) -> str:
        """주식현황 시트 이름을 반환합니다."""
        return self.google_sheet.sheets['holdings_us']  # 주식현황[USA]
        
    def _update_holdings_sheet(self, holdings_data: list, holdings_sheet: str) -> None:
        """주식현황 시트를 업데이트합니다."""
        try:
            # 마지막 업데이트 시간 갱신
            now = datetime.now(self.us_timezone)
            update_time = now.strftime("%Y-%m-%d %H:%M:%S")
            self.google_sheet.update_last_update_time(update_time, holdings_sheet)
            
            # 에러 메시지 초기화
            self.google_sheet.update_error_message("", holdings_sheet)
            
            # 보유 종목 리스트 업데이트 (기존 데이터 초기화 후 새로운 데이터 추가)
            self.logger.info(f"미국 주식 현황 데이터 초기화 및 업데이트 시작 (총 {len(holdings_data)}개 종목)")
            self.google_sheet.update_holdings(holdings_data, holdings_sheet)
            
            self.logger.info(f"미국 주식 현황 업데이트 완료 ({update_time})")
            
        except Exception as e:
            error_msg = f"주식현황 시트 업데이트 실패: {str(e)}"
            self.logger.error(error_msg)
            self.google_sheet.update_error_message(error_msg, holdings_sheet)
            raise 