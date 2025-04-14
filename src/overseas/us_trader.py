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
import logging

class USTrader(BaseTrader):
    """미국 주식 트레이더"""
    
    def __init__(self, config_path: str):
        """미국 주식 트레이더를 초기화합니다."""
        super().__init__(config_path, "USA")
        self.config_path = config_path
            
        # API 클라이언트 초기화
        self.us_api = KISUSAPIManager(config_path)
        self.us_timezone = pytz.timezone("America/New_York")
        
        # 마지막 API 호출 시간
        self.last_api_call_time = 0
        
        # 거래 내역 관리자 초기화
        self.trade_history = TradeHistoryManager("USA")
        
        # 로깅 설정 - BaseTrader에서 상속받은 logger를 사용
        # self.logger = logging.getLogger("USTrader")  # 이 줄을 제거
        
        # 매매 설정 로드
        self.settings = {}
        self.portfolio = None
        self.load_settings()
        
        # 오늘 매도된 종목 캐싱
        self.sold_stocks_cache = set()
        self.sold_stocks_cache_time = 0
        
        # 장 시작 메시지 표시 여부
        self.market_open_executed = False
        self.last_market_date = None
        
        self.logger.info(f"미국 시장 시간 설정: {self.config['trading']['usa_market_start']} ~ {self.config['trading']['usa_market_end']}")
    
    def _wait_for_api_call(self):
        """API 호출 간격을 제어합니다."""
        current_time = time.time()
        elapsed = current_time - self.last_api_call_time
        if elapsed < self.api_call_interval:
            time.sleep(self.api_call_interval - elapsed)
        self.last_api_call_time = time.time()

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
        current_date = current_time.strftime('%Y-%m-%d')
        current_time_str = current_time.strftime('%H%M')
        
        # 날짜가 변경되었으면 market_open_executed 초기화
        if self.last_market_date != current_date:
            self.market_open_executed = False
            self.last_market_date = current_date
        
        # exchange_calendars 라이브러리를 사용하여 휴장일 확인 (API 기반 확인)
        try:
            # XNYS: 뉴욕 증권거래소 (NYSE) 캘린더 사용
            nyse_calendar = xcals.get_calendar("XNYS")
            
            # 오늘이 거래일인지 확인 (API 기반)
            is_session = nyse_calendar.is_session(current_date)
            if not is_session:
                self.logger.info(f"오늘({current_date})은 미국 증시 휴장일입니다.")
                return False
            else:
                if not self.market_open_executed:
                    self.logger.info(f"오늘({current_date})은 미국 증시 개장일입니다.")
            
            # 장 시작 시간과 종료 시간 체크 (config 설정값 사용)
            if not (self.config['trading']['usa_market_start'] <= current_time_str <= self.config['trading']['usa_market_end']):
                self.logger.info(f"현재 미국 장 운영 시간이 아닙니다. (현재시간: {current_time_str}, 장 운영시간: {self.config['trading']['usa_market_start']}~{self.config['trading']['usa_market_end']})")
                # 장 시간이 지나면 다음날을 위해 초기화
                if current_time_str > self.config['trading']['usa_market_end']:
                    self.market_open_executed = False
                return False
            else:
                if not self.market_open_executed:
                    self.logger.info(f"현재 미국 장 운영 시간입니다. (현재시간: {current_time_str}, 장 운영시간: {self.config['trading']['usa_market_start']}~{self.config['trading']['usa_market_end']})")
                    self.market_open_executed = True
                
            return True
            
        except Exception as e:
            self.logger.error(f"시장 상태 확인 중 오류 발생: {str(e)}")
            
            # 오류 발생 시 기존 방식으로 체크 (폴백)
            # 주말 체크
            if current_time.weekday() >= 5:  # 5: 토요일, 6: 일요일
                self.logger.info("주말은 거래가 불가능합니다.")
                return False
                
            # 장 시작 시간과 종료 시간 체크 (config 설정값 사용)
            if not (self.config['trading']['usa_market_start'] <= current_time_str <= self.config['trading']['usa_market_end']):
                self.logger.info(f"현재 미국 장 운영 시간이 아닙니다. (현재시간: {current_time_str}, 장 운영시간: {self.config['trading']['usa_market_start']}~{self.config['trading']['usa_market_end']})")
                # 장 시간이 지나면 다음날을 위해 초기화
                if current_time_str > self.config['trading']['usa_market_end']:
                    self.market_open_executed = False
                return False
            else:
                if not self.market_open_executed:
                    self.logger.info(f"현재 미국 장 운영 시간입니다. (현재시간: {current_time_str}, 장 운영시간: {self.config['trading']['usa_market_start']}~{self.config['trading']['usa_market_end']})")
                    self.market_open_executed = True
                
            return True
    
    def _is_market_open_time(self) -> bool:
        """시가 매수 시점인지 확인합니다."""
        # 미국 현지 시간으로 확인
        current_time = datetime.now(self.us_timezone).strftime('%H%M')
        start_time = self.config['trading']['usa_market_start']
        
        # 장 시작 후 10분 이내
        return start_time <= current_time <= str(int(start_time) + 10).zfill(4)
        
    def _is_market_close_time(self) -> bool:
        """장 마감 시간인지 확인합니다."""
        return False  # 종가 매수 로직 사용하지 않음
        
    def calculate_ma(self, stock_code: str, period: int = 20, period_div_code: str = "D") -> Optional[tuple]:
        """이동평균선을 계산합니다.
        
        Args:
            stock_code (str): 종목코드
            period (int): 이동평균 기간
            period_div_code (str): 기간 구분 코드 (D: 일봉, W: 주봉)
        
        Returns:
            Optional[tuple]: (전전일/전전주 이동평균값, 전일/전주 이동평균값) 또는 None
        """
        try:
            end_date = datetime.now(self.us_timezone).strftime("%Y%m%d")
            
            # 필요한 기간을 계산
            if period_div_code == "D": # 일별 데이터
                # 최소 필요 데이터 포인트 수 (period + 2개 포인트가 필요: MA 계산 + 전전일, 전일)
                min_data_points = period + 2
                # 추가 여유를 위해 주말, 공휴일을 고려하여 30% 추가 (캘린더 일수)
                required_days = int(min_data_points * 1.3)
                
            else: # 주간 데이터
                # 주간 데이터는 캘린더 일수가 아닌 주 단위로 제공됨
                # 필요한 주 수 (period) + 2개 (전전주, 전주)
                required_weeks = period + 2
                # 주 단위를 일수로 변환 (한 주는 최대 7일)
                required_days = required_weeks * 7
                
                # API가 주간 데이터를 특정 시점(ex: 금요일)에 집계할 수 있으므로
                # 현재 요일에 따라 필요한 날짜를 추가 보정
                current_weekday = datetime.now(self.us_timezone).weekday()  # 0=월요일, 6=일요일
                if current_weekday < 5:  # 월~금요일인 경우
                    # 금요일까지 도달하지 않았으므로 이번 주는 아직 데이터가 없을 수 있음
                    # 한 주 더 추가 (7일)
                    required_days += 7
                
            start_date = (datetime.now(self.us_timezone) - timedelta(days=required_days)).strftime("%Y%m%d")
            
            # API 제한(100일)을 고려한 효율적인 데이터 조회
            if period_div_code == "W":
                # 주간 데이터는 데이터 포인트가 적을 수 있지만, 긴 기간의 경우 100주를 초과할 수 있음
                # API 제한인 100건을 고려하여 처리
                
                # 필요한 기간이 100주 이하인 경우 먼저 한 번에 조회 시도
                if required_weeks <= 100:
                    try:
                        hist_data = self._retry_api_call(
                            self.us_api.get_daily_price,
                            stock_code,
                            start_date,
                            end_date,
                            period_div_code
                        )
                        
                        if hist_data is not None and len(hist_data) >= period + 2:
                            # DataFrame으로 변환 (이미 DataFrame인 경우 그대로 사용)
                            df = hist_data if isinstance(hist_data, pd.DataFrame) else pd.DataFrame(hist_data)
                            # 정렬
                            df['xymd'] = pd.to_datetime(df['xymd'])
                            df = df.sort_values('xymd', ascending=True).reset_index(drop=True)
                            
                            # 이동평균 계산
                            df['clos'] = df['clos'].astype(float)
                            ma_series = df['clos'].rolling(window=period).mean()
                            
                            # 데이터가 충분한지 확인
                            if len(df) < period + 2:
                                self.logger.warning(f"{stock_code}: 주간 데이터 부족, 계산 불가 (현재: {len(df)}개, 필요: {period+2}개)")
                                return None
                            
                            # 전전주, 전주 이동평균값 반환
                            ma_prev2 = ma_series.iloc[-3]  # 전전주
                            ma_prev = ma_series.iloc[-2]   # 전주
                            return (ma_prev2, ma_prev)
                        
                        # 데이터가 부족하면 더 긴 기간으로 재시도 (아래 코드로 진행)
                        self.logger.debug(f"{stock_code}: 주간 데이터 부족, 더 긴 기간 조회 시도 (현재: {len(hist_data) if hist_data is not None else 0}개, 필요: {period+2}개)")
                    except Exception as e:
                        self.logger.error(f"{stock_code}: 주간 데이터 첫 조회 중 오류 발생 - {str(e)}")
                
                # 100건 초과 데이터 필요 또는 첫 번째 시도 실패 시 분할 조회
                try:
                    # 분할 조회를 위한 설정
                    all_data = []
                    current_end_date = datetime.now(self.us_timezone)
                    start_datetime = datetime.now(self.us_timezone) - timedelta(days=required_days * 2)  # 넉넉히 2배 기간으로 설정
                    
                    while current_end_date.replace(tzinfo=None) >= start_datetime.replace(tzinfo=None):
                        # 현재 조회 기간의 시작일 계산 (최대 100주에 해당하는 700일)
                        current_start_date = current_end_date - timedelta(days=700)  # 100주 = 700일
                        
                        # 시작일보다 이전으로 가지 않도록 조정
                        if current_start_date.replace(tzinfo=None) < start_datetime.replace(tzinfo=None):
                            current_start_date = start_datetime
                        
                        # 날짜 형식 변환
                        current_start_date_str = current_start_date.strftime("%Y%m%d")
                        current_end_date_str = current_end_date.strftime("%Y%m%d")
                        
                        self.logger.debug(f"주간 데이터 분할 조회: {stock_code}, {current_start_date_str} ~ {current_end_date_str}")
                        
                        # API 호출하여 데이터 조회
                        hist_data = self._retry_api_call(
                            self.us_api.get_daily_price,
                            stock_code,
                            current_start_date_str,
                            current_end_date_str,
                            period_div_code
                        )
                        
                        if hist_data is not None and len(hist_data) > 0:
                            all_data.append(hist_data)
                        
                        # 다음 조회 기간 설정 (1일 겹치지 않게)
                        current_end_date = current_start_date - timedelta(days=1)
                        
                        # 시작일에 도달하면 종료
                        if current_end_date.replace(tzinfo=None) < start_datetime.replace(tzinfo=None):
                            break
                    
                    # 조회된 데이터가 없는 경우
                    if not all_data:
                        self.logger.warning(f"{stock_code}: 주간 데이터 조회 실패, 데이터가 없습니다.")
                        return None
                    
                    # 모든 데이터 합치기
                    combined_df = pd.concat(all_data, ignore_index=True)
                    
                    # 중복 제거 (날짜 기준)
                    combined_df['xymd'] = pd.to_datetime(combined_df['xymd'])
                    combined_df = combined_df.drop_duplicates(subset=['xymd'])
                    combined_df = combined_df.sort_values('xymd', ascending=True).reset_index(drop=True)
                    
                    # 데이터가 충분한지 확인
                    if len(combined_df) < period + 2:
                        self.logger.warning(f"{stock_code}: 주간 데이터 부족, 계산 불가 (현재: {len(combined_df)}개, 필요: {period+2}개)")
                        return None
                    
                    # 이동평균 계산
                    combined_df['clos'] = combined_df['clos'].astype(float)
                    ma_series = combined_df['clos'].rolling(window=period).mean()
                    
                    # 전전주, 전주 이동평균값 반환
                    ma_prev2 = ma_series.iloc[-3]  # 전전주
                    ma_prev = ma_series.iloc[-2]   # 전주
                    return (ma_prev2, ma_prev)
                    
                except Exception as e:
                    self.logger.error(f"{stock_code}: 주간 데이터 분할 조회 중 오류 발생 - {str(e)}")
                    return None
                    
            else:  # 일별 데이터 처리
                # API 제한(100일)을 고려하여 데이터 조회
                all_data = []
                current_end_date = datetime.now(self.us_timezone)
                start_datetime = datetime.now(self.us_timezone) - timedelta(days=required_days)
                
                # 필요한 기간이 100일 이하인 경우 한 번에 조회
                if required_days <= 100:
                    hist_data = self._retry_api_call(
                        self.us_api.get_daily_price,
                        stock_code,
                        start_date,
                        end_date,
                        period_div_code
                    )
                    
                    if hist_data is None or len(hist_data) < period + 2:
                        self.logger.warning(f"{stock_code}: 이동평균 계산을 위한 데이터가 부족합니다. (필요: {period+2}개, 실제: {len(hist_data) if hist_data is not None else 0}개)")
                        return None
                    
                    # DataFrame으로 변환 (이미 DataFrame인 경우 그대로 사용)   
                    df = hist_data if isinstance(hist_data, pd.DataFrame) else pd.DataFrame(hist_data)
                    # 정렬
                    df['xymd'] = pd.to_datetime(df['xymd'])
                    df = df.sort_values('xymd', ascending=True).reset_index(drop=True)
                    # 이동평균 계산
                    df['clos'] = df['clos'].astype(float)
                    ma_series = df['clos'].rolling(window=period).mean()
                    # 전전일, 전일 이동평균값 반환
                    ma_prev2 = ma_series.iloc[-3]  # 전전일
                    ma_prev = ma_series.iloc[-2]   # 전일
                    return (ma_prev2, ma_prev)
                
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
                    
                    self.logger.debug(f"이동평균 계산을 위한 시세 조회: {stock_code}, {current_start_date_str} ~ {current_end_date_str}, 주기: {period_div_code}")
                    
                    # API 호출하여 데이터 조회
                    hist_data = self._retry_api_call(
                        self.us_api.get_daily_price,
                        stock_code,
                        current_start_date_str,
                        current_end_date_str,
                        period_div_code
                    )
                    
                    if hist_data is not None and len(hist_data) > 0:
                        all_data.append(hist_data)
                    
                    # 다음 조회 기간 설정 (하루 겹치지 않게)
                    current_end_date = current_start_date - timedelta(days=1)
                    
                    # 시작일에 도달하면 종료
                    if current_end_date.replace(tzinfo=None) < start_datetime.replace(tzinfo=None):
                        break
                
                # 조회된 데이터가 없는 경우
                if not all_data:
                    self.logger.warning(f"{stock_code}: 이동평균 계산을 위한 데이터를 조회할 수 없습니다.")
                    return None
                
                # 모든 데이터 합치기
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # 중복 제거 (날짜 기준)
                if 'xymd' in combined_df.columns:
                    combined_df['xymd'] = pd.to_datetime(combined_df['xymd'])
                    combined_df = combined_df.drop_duplicates(subset=['xymd'])
                    combined_df = combined_df.sort_values('xymd', ascending=True).reset_index(drop=True)
                
                # 데이터가 충분한지 확인
                if len(combined_df) < period + 2:  # 최소한 period+2개의 데이터가 필요
                    self.logger.warning(f"{stock_code}: 이동평균 계산을 위한 데이터가 부족합니다. (필요: {period+2}개, 실제: {len(combined_df)}개)")
                    return None
                
                # 이동평균 계산
                combined_df['clos'] = combined_df['clos'].astype(float)
                ma_series = combined_df['clos'].rolling(window=period).mean()
                # 전전일, 전일 이동평균값 반환
                ma_prev2 = ma_series.iloc[-3]  # 전전일
                ma_prev = ma_series.iloc[-2]   # 전일
                return (ma_prev2, ma_prev)
            
        except Exception as e:
            self.logger.error(f"{period}{period_div_code} 이동평균 계산 실패 ({stock_code}): {str(e)}")
            return None
    
    def get_highest_price_since_first_buy(self, stock_code: str) -> float:
        """최초 매수일 이후부터 어제까지의 최고가를 조회합니다."""
        try:
            # 현재 날짜 확인
            current_date = datetime.now(self.us_timezone).strftime("%Y-%m-%d")
            
            # 먼저 데이터베이스에서 저장된 최고가 확인
            db_highest_price = self.trade_history.get_highest_price(stock_code)
            
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
                df = self.us_api.get_daily_price(stock_code, start_date_str, end_date_str, "D")
                if df is not None and len(df) > 0:
                    all_data.append(df)
                
                # 다음 조회 기간 설정 (하루 겹치지 않게)
                current_end_date = current_start_date - timedelta(days=1)
                
                # 최초 매수일에 도달하면 종료
                if current_end_date.replace(tzinfo=None) < first_date:
                    break
            
            # 조회된 데이터가 없는 경우 데이터베이스에 저장된 최고가 반환
            if not all_data:
                return db_highest_price
            
            # 모든 데이터 합치기
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 중복 제거 (날짜 기준)
            combined_df = combined_df.drop_duplicates(subset=['xymd'])
            
            # 최초 매수일부터 어제까지의 데이터만 필터링
            combined_df = combined_df[(combined_df['xymd'] >= pd.to_datetime(first_buy_date, format='%Y-%m-%d')) & 
                                     (combined_df['xymd'] <= pd.to_datetime(end_date.strftime('%Y-%m-%d')))]
            
            # 최고가 계산
            api_highest_price = combined_df['high'].astype(float).max()
            
            # 데이터베이스의 최고가와 API에서 조회한 최고가 중 더 높은 값을 사용
            highest_price = max(db_highest_price, api_highest_price)
            
            # 데이터베이스 최고가 업데이트
            if highest_price > db_highest_price:
                self.trade_history.update_highest_price(stock_code, highest_price)
                self.logger.debug(f"최고가 데이터베이스 업데이트: {stock_code}, {highest_price}")
            
            return highest_price
            
        except Exception as e:
            self.logger.error(f"최고가 조회 실패 ({stock_code}): {str(e)}")
            return 0
    
    def check_buy_condition(self, stock_code: str, ma_period: int, prev_close: float, 
                           ma_condition: str = "종가", period_div_code: str = "D") -> tuple[bool, Optional[float]]:
        """매수 조건을 확인합니다.
        
        Args:
            stock_code (str): 종목코드
            ma_period (int): 이동평균 기간
            prev_close (float): 전일 종가
            ma_condition (str): 매수 조건 ("종가" 또는 이평선 기간)
            period_div_code (str): 기간 구분 코드 (D: 일봉, W: 주봉)
        
        Returns:
            tuple[bool, Optional[float]]: (매수 조건 충족 여부, 이동평균값)
        """
        try:
            # 이동평균 값 계산
            ma_target_values = self.calculate_ma(stock_code, ma_period, period_div_code)
            
            if ma_target_values is None:
                return False, None
            
            # 전전일과 전일 이동평균값 추출
            ma_target_prev2, ma_target_prev = ma_target_values
            
            # "종가" 조건: 전일 종가가 이동평균선 상향 돌파
            if ma_condition == "종가":
                # 전일 종가가 이동평균선을 상향 돌파했는지 확인
                # 즉, prev_close > ma_target_prev
                is_buy = prev_close > ma_target_prev
                if is_buy:
                    # 일/주 구분
                    period_unit = "일" if period_div_code == "D" else "주"
                    self.logger.info(f"매수 조건 충족: {stock_code} - 전일 종가(${prev_close:.2f})가 {ma_period}{period_unit}선(${ma_target_prev:.2f})을 상향돌파")
                return is_buy, ma_target_prev
            
            # 골든크로스 조건: ma_condition에 지정된 이평선이 기준 이평선을 상향 돌파
            else:
                try:
                    # 조건으로 지정된 이평선 기간을 정수로 변환
                    condition_period = int(ma_condition)
                    # 조건 이평선 값 계산
                    ma_condition_values = self.calculate_ma(stock_code, condition_period, period_div_code)
                    
                    if ma_condition_values is None:
                        return False, ma_target_prev
                    
                    # 전전일과 전일 이동평균값 추출
                    ma_condition_prev2, ma_condition_prev = ma_condition_values
                    
                    # 골든크로스 조건 확인
                    # 전전일: 조건 이평선 < 기준 이평선
                    # 전일: 조건 이평선 > 기준 이평선
                    golden_cross = (ma_condition_prev2 < ma_target_prev2) and (ma_condition_prev > ma_target_prev)
                    
                    if golden_cross:
                        # 일/주 구분
                        period_unit = "일" if period_div_code == "D" else "주"
                        self.logger.info(f"골든크로스 발생: {stock_code}")
                        self.logger.info(f"- 전전{period_unit}: {condition_period}{period_unit}선(${ma_condition_prev2:.2f}) < {ma_period}{period_unit}선(${ma_target_prev2:.2f})")
                        self.logger.info(f"- 전{period_unit}: {condition_period}{period_unit}선(${ma_condition_prev:.2f}) > {ma_period}{period_unit}선(${ma_target_prev:.2f})")
                    
                    return golden_cross, ma_target_prev
                    
                except (ValueError, TypeError):
                    self.logger.error(f"매수 조건 확인 중 오류: {ma_condition}이 유효한 이평선 기간이 아닙니다.")
                    return False, ma_target_prev
            
        except Exception as e:
            self.logger.error(f"매수 조건 확인 중 오류 발생 ({stock_code}): {str(e)}")
            return False, None
    
    def check_sell_condition(self, stock_code: str, ma_period: int, prev_close: float, 
                            ma_condition: str = "종가", period_div_code: str = "D") -> tuple[bool, Optional[float]]:
        """매도 조건을 확인합니다.
        
        Args:
            stock_code (str): 종목코드
            ma_period (int): 이동평균 기간
            prev_close (float): 전일 종가
            ma_condition (str): 매도 조건 ("종가" 또는 이평선 기간)
            period_div_code (str): 기간 구분 코드 (D: 일봉, W: 주봉)
        
        Returns:
            tuple[bool, Optional[float]]: (매도 조건 충족 여부, 이동평균값)
        """
        try:
            # 이동평균 값 계산
            ma_target_values = self.calculate_ma(stock_code, ma_period, period_div_code)
            
            if ma_target_values is None:
                return False, None
            
            # 전전일과 전일 이동평균값 추출
            ma_target_prev2, ma_target_prev = ma_target_values
            
            # "종가" 조건: 전일 종가가 이동평균선 하향 돌파
            if ma_condition == "종가":
                # 전일 종가가 이동평균선 아래에 있는지 확인
                # 즉, prev_close < ma_target_prev
                is_sell = prev_close < ma_target_prev
                if is_sell:
                    period_unit = "일" if period_div_code == "D" else "주"
                    self.logger.info(f"매도 조건 충족: {stock_code} - 전일 종가(${prev_close:.2f})가 {ma_period}{period_unit}선(${ma_target_prev:.2f}) 아래로 하향돌파")
                return is_sell, ma_target_prev
            
            # 데드크로스 조건: ma_condition에 지정된 이평선이 기준 이평선을 하향 돌파
            else:
                try:
                    # 조건으로 지정된 이평선 기간을 정수로 변환
                    condition_period = int(ma_condition)
                    # 조건 이평선 값 계산
                    ma_condition_values = self.calculate_ma(stock_code, condition_period, period_div_code)
                    
                    if ma_condition_values is None:
                        return False, ma_target_prev
                    
                    # 전전일과 전일 이동평균값 추출
                    ma_condition_prev2, ma_condition_prev = ma_condition_values
                    
                    # 데드크로스 조건 확인
                    # 전전일: 조건 이평선 > 기준 이평선
                    # 전일: 조건 이평선 < 기준 이평선
                    dead_cross = (ma_condition_prev2 > ma_target_prev2) and (ma_condition_prev < ma_target_prev)
                    
                    if dead_cross:
                        # 일/주 구분
                        period_unit = "일" if period_div_code == "D" else "주"
                        self.logger.info(f"데드크로스 발생: {stock_code}")
                        self.logger.info(f"- 전전{period_unit}: {condition_period}{period_unit}선(${ma_condition_prev2:.2f}) > {ma_period}{period_unit}선(${ma_target_prev2:.2f})")
                        self.logger.info(f"- 전{period_unit}: {condition_period}{period_unit}선(${ma_condition_prev:.2f}) < {ma_period}{period_unit}선(${ma_target_prev:.2f})")
                    
                    return dead_cross, ma_target_prev
                    
                except (ValueError, TypeError):
                    self.logger.error(f"매도 조건 확인 중 오류: {ma_condition}이 유효한 이평선 기간이 아닙니다.")
                    return False, ma_target_prev
                
        except Exception as e:
            self.logger.error(f"매도 조건 확인 중 오류 발생 ({stock_code}): {str(e)}")
            return False, None
    
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
            # 거래 내역에서 해당 종목의 모든 매도 내역 조회
            all_trades = self.trade_history.get_trades_by_code(stock_code)
            
            if not all_trades or len(all_trades) == 0:
                return None
                
            # 가장 최근 거래 확인
            latest_trade = all_trades[-1]
            
            # 마지막 거래가 정상 매도인 경우 None 반환
            if latest_trade.get("trade_type") != "TRAILING_STOP":
                return None
                
            # 마지막 거래가 트레일링 스탑 매도인 경우 가격 반환
            return float(latest_trade.get("price", 0))
            
        except Exception as e:
            self.logger.error(f"트레일링 스탑 매도 가격 조회 중 오류 발생 ({stock_code}): {str(e)}")
            return None

    def get_last_normal_sell_price(self, stock_code: str) -> Optional[float]:
        """정상적인 매도 조건으로 매도된 종목의 마지막 매도 가격을 조회합니다.
        스탑로스나 트레일링 스탑으로 인한 매도는 제외하고, 사용자가 설정한 정상 매도일 경우만 반환합니다.
        
        Args:
            stock_code (str): 종목 코드
            
        Returns:
            Optional[float]: 정상 매도 가격, 없으면 None
        """
        try:
            # 거래 내역에서 해당 종목의 모든 매도 내역 조회
            all_trades = self.trade_history.get_trades_by_code(stock_code)
            
            if not all_trades or len(all_trades) == 0:
                return None
                
            # 가장 최근 거래 확인
            latest_trade = all_trades[-1]
            
            # 마지막 거래가 정상 매도인 경우 가격 반환
            if latest_trade.get("trade_type") == "SELL":
                return float(latest_trade.get("price", 0))
            
            return None
            
        except Exception as e:
            self.logger.error(f"정상 매도 가격 조회 중 오류 발생 ({stock_code}): {str(e)}")
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
                        holdings[full_stock_code] = {
                            'name': stock_info['종목명'],
                            'current_price': current_price,
                            'quantity': quantity,
                            'current_value': current_value,
                            'current_ratio': current_ratio,
                            'target_ratio': target_ratio
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
                                "reason": f"리밸런싱 매수 (현재 비중 {info['current_ratio']:.1f}% → 목표 비중: {info['target_ratio']:.1f}%)",
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
                                "reason": f"리밸런싱 매도 (현재 비중 {info['current_ratio']:.1f}% → 목표 비중: {info['target_ratio']:.1f}%)",
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
            # 현재 날짜 및 시간 확인 (미국 시간 기준)
            now = datetime.now(self.us_timezone)
            
            # 당일 최초 실행 여부 확인 및 초기화
            if self.execution_date != now.strftime("%Y-%m-%d"):
                self.execution_date = now.strftime("%Y-%m-%d")
                self.market_open_executed = False
                self.sold_stocks_cache = []  # 당일 매도 종목 캐시 초기화
                self.sold_stocks_cache_time = 0  # 캐시 시간 초기화
                self.logger.info(f"=== {self.execution_date} 미국 시간 기준 매매 시작 ===")
            
            # 1. 스탑로스/트레일링 스탑 체크 (매 루프마다 실행)
            self._check_stop_conditions()
            
            # 2. 장 상태 체크
            is_market_open = self._is_market_open_time()
            
            # 3. 장 시작 시 매매 실행 (아직 실행되지 않은 경우)
            if is_market_open and not self.market_open_executed:
                self.logger.info(f"장 시작 매매 실행")
                
                # 계좌 잔고 조회
                balance = self._retry_api_call(self.us_api.get_account_balance)
                if balance is None:
                    self.logger.error("계좌 잔고 조회 실패")
                    return
                
                # 3-1. 매도 조건 처리
                self._process_sell_conditions(balance)
                
                # 3-2. 리밸런싱 체크 및 실행
                if self._is_rebalancing_day():
                    self.logger.info("리밸런싱 실행")
                    self._rebalance_portfolio(balance)
                
                # 3-3. 매수 조건 처리
                self._process_buy_conditions(balance)
                
                self.market_open_executed = True
                self.logger.info("장 시작 매매 실행 완료")
            
        except Exception as e:
            error_msg = f"매매 실행 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            raise
    
    def _process_sell_conditions(self, balance: Dict):
        """매도 조건 처리"""
        try:
            # 구글 스프레드시트에 있는 종목 코드 리스트 생성
            sheet_stock_codes = set()
            # 개별 종목에서 종목 코드 추가
            for _, row in self.individual_stocks.iterrows():
                sheet_stock_codes.add(row['종목코드'])
            # POOL 종목에서 종목 코드 추가
            for _, row in self.pool_stocks.iterrows():
                sheet_stock_codes.add(row['종목코드'])
            
            # 보유 종목 확인
            for holding in balance['output1']:
                # 거래 가능 수량이 있는 경우만 처리
                quantity = int(holding.get('ord_psbl_qty', 0))
                if quantity <= 0:
                    continue
                    
                # 거래소와 종목코드 결합
                exchange = holding.get('ovrs_excg_cd', '')  # NASD, NYSE, AMEX
                stock_code_only = holding['ovrs_pdno']
                stock_code = f"{stock_code_only}.{exchange}"
                stock_name = holding['ovrs_item_name']
                
                # 구글 스프레드시트에서 삭제된 종목 체크
                if stock_code_only not in sheet_stock_codes:
                    # 현재가 조회
                    current_price_data = self._retry_api_call(self.us_api.get_stock_price, stock_code)
                    if current_price_data is None:
                        self.logger.warning(f"{stock_name}({stock_code})의 현재가를 조회할 수 없습니다.")
                        continue
                        
                    current_price = float(current_price_data['output']['last'])
                    
                    self.logger.info(f"{stock_name}({stock_code}) - 구글 스프레드시트에서 삭제된 종목이므로 매도합니다.")
                    
                    # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                    sell_price = current_price * 0.99
                    
                    # 매도 주문 실행
                    result = self._retry_api_call(self.us_api.order_stock, stock_code, "SELL", quantity, sell_price)
                    
                    if result:
                        # 매수 평균가 가져오기
                        avg_price = float(holding.get('pchs_avg_pric', 0))
                        if avg_price <= 0:
                            avg_price = current_price  # 매수 평균가가 없으면 현재가 사용
                        
                        msg = f"매도 주문 실행: {stock_name} {quantity}주 (지정가)"
                        msg += f"\n- 매도 사유: 구글 스프레드시트에서 종목이 삭제됨"
                        msg += f"\n- 매도 금액: ${current_price * quantity:,.2f} (현재가 ${current_price:.2f})"
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
                            "ma_period": 0,
                            "ma_value": 0,
                            "ma_condition": "삭제됨",
                            "period_div_code": "",
                            "reason": "구글 스프레드시트에서 종목이 삭제됨",
                            "profit_loss": (current_price - avg_price) * quantity,
                            "profit_loss_pct": (current_price - avg_price) / avg_price * 100
                        }
                        self.trade_history.add_trade(trade_data)
                        
                        # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                        self.sold_stocks_cache_time = 0
                    continue
                
                # 매도 조건 확인
                ma_period = 0
                ma_condition = "종가"  # 기본값
                period_div_code = "D"  # 기본값
                
                # 개별 종목에서 찾기
                for _, row in self.individual_stocks.iterrows():
                    if row['종목코드'] == holding['ovrs_pdno']:
                        ma_period = int(row['매도기준'])
                        ma_condition = row.get('매도조건', '종가')
                        period_div_code = row.get('매도기준2', '일')
                        period_div_code = "D" if period_div_code == "일" else "W"
                        break
                
                # 개별 종목에서 찾지 못한 경우 POOL 종목에서 찾기
                if ma_period == 0:
                    for _, row in self.pool_stocks.iterrows():
                        if row['종목코드'] == holding['ovrs_pdno']:
                            ma_period = int(row['매도기준'])
                            ma_condition = row.get('매도조건', '종가')
                            period_div_code = row.get('매도기준2', '주')
                            period_div_code = "D" if period_div_code == "일" else "W"
                            break
                
                if ma_period == 0:
                    self.logger.warning(f"{stock_name}({stock_code})의 매도기준을 찾을 수 없습니다.")
                    continue
                
                # 현재가 조회
                current_price_data = self._retry_api_call(self.us_api.get_stock_price, stock_code)
                if current_price_data is None:
                    self.logger.warning(f"{stock_name}({stock_code})의 현재가를 조회할 수 없습니다.")
                    continue
                    
                current_price = float(current_price_data['output']['last'])
                prev_close = float(current_price_data['output']['base'])
                
                # 매도 조건 확인 - 전일 종가를 기준으로 판단
                sell_condition, ma = self.check_sell_condition(stock_code, ma_period, prev_close, ma_condition, period_div_code)
                
                if ma is None:
                    self.logger.warning(f"{stock_name}({stock_code})의 이동평균을 계산할 수 없습니다.")
                    continue
                    
                if sell_condition:
                    period_unit = "일선" if period_div_code == "D" else "주선"
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
                            "ma_condition": ma_condition,
                            "period_div_code": period_div_code,
                            "reason": f"{period_div_code}봉 기준 {ma_condition} {ma_period}{period_unit} 매도 조건 충족 (전일 종가 ${prev_close:.2f} < MA ${ma:.2f})",
                            "profit_loss": (current_price - avg_price) * quantity,
                            "profit_loss_pct": (current_price - avg_price) / avg_price * 100
                        }
                        self.trade_history.add_trade(trade_data)
                        
                        # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                        self.sold_stocks_cache_time = 0
        
        except Exception as e:
            self.logger.error(f"매도 조건 처리 중 오류 발생: {str(e)}")
    
    def _process_buy_conditions(self, balance: Dict):
        """매수 조건을 체크하고 실행합니다."""
        try:
            # 개별 종목 매수
            for _, row in self.individual_stocks.iterrows():
                if row['거래소'] != "KOR":  # 미국 주식만 처리
                    self._process_single_stock_buy(row, balance)
            
            # POOL 종목 매수
            for _, row in self.pool_stocks.iterrows():
                if row['거래소'] != "KOR":  # 미국 주식만 처리
                    self._process_single_stock_buy(row, balance)
        
        except Exception as e:
            self.logger.error(f"매수 조건 체크 중 오류 발생: {str(e)}")
    
    def _process_single_stock_buy(self, row: pd.Series, balance: Dict):
        """단일 종목의 매수를 처리합니다."""
        try:
            # 거래소와 종목코드 결합
            stock_code = f"{row['종목코드']}.{row['거래소']}"
            ma_period = int(row['매수기준']) if row['매수기준'] and str(row['매수기준']).strip() != '' else 20
            ma_condition = row.get('매수조건', '종가')  # 기본값은 '종가'
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
            
            # 장 시작 시 매수 처리
            if is_holding:
                return
            
            # 당일 매도한 종목은 스킵
            if self.is_sold_today(stock_code):
                self.logger.info(f"{row['종목명']}({stock_code}) - 당일 매도 종목 재매수 제한")
                return
            
            # 트레일링 스탑으로 매도된 종목 체크
            trailing_stop_price = self.get_trailing_stop_sell_price(stock_code.split('.')[0])
            if trailing_stop_price is not None:
                # 마지막 TS 매도 날짜 조회
                ts_sell_date = self.trade_history.get_last_ts_sell_date(stock_code.split('.')[0])
                if ts_sell_date is None:
                    self.logger.error(f"{row['종목명']}({stock_code}) - TS 매도 날짜 조회 실패")
                    return
                
                self.logger.info(f"{row['종목명']}({stock_code}) - 마지막 TS 매도 날짜: {ts_sell_date}")
                
                # 종목 유형에 따라 일간/주간 데이터 사용
                is_individual = any(s['종목코드'] == stock_code.split('.')[0] for _, s in self.individual_stocks.iterrows())
                # 매수기준2의 값에 따라 일봉/주봉 결정
                if is_individual:
                    # 개별 종목인 경우 해당 종목 찾기
                    individual_match = self.individual_stocks[self.individual_stocks['종목코드'] == stock_code.split('.')[0]]
                    if not individual_match.empty:
                        period_div_code_raw = individual_match.iloc[0].get('매수기준2', '일')
                        period_div_code = "D" if period_div_code_raw == "일" else "W"
                        period_unit = "일" if period_div_code == "D" else "주"
                    else:
                        period_div_code = "D"  # 찾지 못한 경우 기본값
                        period_unit = "일"
                else:
                    # POOL 종목인 경우 해당 종목 찾기
                    pool_match = self.pool_stocks[self.pool_stocks['종목코드'] == stock_code.split('.')[0]]
                    if not pool_match.empty:
                        period_div_code_raw = pool_match.iloc[0].get('매수기준2', '주')
                        period_div_code = "D" if period_div_code_raw == "일" else "W"
                        period_unit = "일" if period_div_code == "D" else "주"
                    else:
                        period_div_code = "D"  # 찾지 못한 경우 기본값
                        period_unit = "일"
                
                # 매수 조건 체크를 통해 정확한 이평선 값 얻기
                should_buy, ma_value = self.check_buy_condition(stock_code, ma_period, prev_close, ma_condition, period_div_code)
                
                if ma_value is None:
                    self.logger.error(f"{row['종목명']}({stock_code}) - {ma_period}{period_unit} 계산 실패")
                    return
                
                # 종가와 이평선 비교 (정확한 이평선 값 사용)
                is_above_ma = prev_close > ma_value
                
                if is_above_ma:
                    # 이평선 위에 있는 경우 추가 조건 체크
                    # 조건 1: 전일 종가가 TS 매도가보다 큰지 확인
                    is_above_ts_price = prev_close > trailing_stop_price
                    if is_above_ts_price:
                        self.logger.info(f"{row['종목명']}({stock_code}) - 전일 종가(${prev_close:.2f})가 TS 매도가(${trailing_stop_price:.2f})보다 크므로 매수조건 충족 여부와 관계없이 즉시 재매수")
                        
                        # 즉시 재매수 추가
                        buy_amount = total_assets * allocation_ratio
                        buy_quantity = int(buy_amount / current_price)
                        
                        if buy_quantity > 0:
                            # 매수 주문 실행
                            result = self._retry_api_call(
                                self.us_api.order_stock,
                                stock_code,
                                "BUY",
                                buy_quantity,
                                current_price
                            )
                            
                            if result:
                                msg = f"매수 주문 실행: {row['종목명']}({stock_code}) {buy_quantity}주"
                                msg += f"\n- 매수 사유: 트레일링 스탑 매도 후 재매수 (전일 종가 > TS 매도가)"
                                msg += f"\n- 매수 금액: ${buy_quantity * current_price:.2f}"
                                self.logger.info(msg)
                                
                                # 거래 내역 저장
                                trade_data = {
                                    "trade_type": "BUY",
                                    "trade_action": "BUY",
                                    "stock_code": stock_code.split('.')[0],
                                    "stock_name": row['종목명'],
                                    "quantity": buy_quantity,
                                    "price": current_price,
                                    "total_amount": buy_quantity * current_price,
                                    "ma_period": ma_period,
                                    "ma_value": ma_value,
                                    "ma_condition": "트레일링스탑매도후재매수",
                                    "period_div_code": period_div_code,
                                    "reason": f"트레일링 스탑 매도 후 재매수 조건 충족 (전일 종가 ${prev_close:.2f} > TS 매도가 ${trailing_stop_price:.2f})"
                                }
                                self.trade_history.add_trade(trade_data)
                            
                            # 즉시 처리 후 반환
                            return
                    else:
                        # 조건 2: TS 매도 이후 이평선을 한 번이라도 이탈했는지 확인
                        has_crossed_below = self._check_ma_cross_below_since_ts_sell(stock_code.split('.')[0], ts_sell_date, ma_period, period_div_code)
                        
                        if not has_crossed_below:
                            # 두 조건 모두 충족하지 않으면 재매수 제한
                            self.logger.info(f"{row['종목명']}({stock_code}) - TS 매도 이후 {ma_period}{period_unit}선 이탈 이력 없고, 전일 종가가 TS 매도가보다 작아 재매수 제한")
                            return
                        else:
                            # 이탈 이력이 있으면 매수 조건 확인 후 재매수 가능 (다음 단계로 진행)
                            self.logger.info(f"{row['종목명']}({stock_code}) - TS 매도 이후 {ma_period}{period_unit}선 이탈 이력 있어 매수 조건 확인 후 재매수 가능")
                else:
                    # 현재 이평선 아래에 있으면 매수 조건에 따라 결정 (다음 단계로 넘김)
                    self.logger.info(f"{row['종목명']}({stock_code}) - 현재 {ma_period}{period_unit}선 아래에 있음, 매수 조건에 따라 결정")

            # 정상 매도된 종목 체크 (정상매도된 종목 재매수 조건)
            last_normal_sell_price = self.get_last_normal_sell_price(stock_code.split('.')[0])
            if last_normal_sell_price is not None:
                # 매수 조건 체크를 통해 정확한 이평선 값 얻기
                should_buy, ma_value = self.check_buy_condition(stock_code, ma_period, prev_close, ma_condition, period_div_code)
                
                # 수정된 조건: 전일 종가가 이평선 위에 있고 직전 정상 매도가보다 높으면 즉시 재매수
                if ma_value is not None:
                    above_ma = prev_close > ma_value
                    higher_than_last_sell = prev_close > last_normal_sell_price
                    
                    if above_ma and higher_than_last_sell:
                        msg = f"{row['종목명']}({stock_code}) - 정상 매도 후 재매수 조건 충족"
                        msg += f"\n- 전일 종가(${prev_close:.2f})가 {ma_period}{period_unit}(${ma_value:.2f}) 위에 있고,"
                        msg += f"\n- 전일 종가(${prev_close:.2f})가 직전 정상 매도가(${last_normal_sell_price:.2f})보다 높음"
                        self.logger.info(msg)
                        
                        # 매수 금액 계산 (총자산 * 배분비율)
                        buy_amount = total_assets * allocation_ratio
                        buy_quantity = int(buy_amount / current_price)
                        
                        if buy_quantity > 0:
                            # 매수 주문 실행
                            result = self._retry_api_call(
                                self.us_api.order_stock,
                                stock_code,
                                "BUY",
                                buy_quantity,
                                current_price
                            )
                            
                            if result:
                                msg = f"매수 주문 실행: {row['종목명']}({stock_code}) {buy_quantity}주"
                                msg += f"\n- 매수 사유: 정상 매도 후 재매수 (전일 종가 > 이평선 && 전일 종가 > 정상 매도가)"
                                msg += f"\n- 매수 금액: ${buy_quantity * current_price:.2f}"
                                self.logger.info(msg)
                                
                                # 거래 내역 저장
                                trade_data = {
                                    "trade_type": "BUY",
                                    "trade_action": "BUY",
                                    "stock_code": stock_code.split('.')[0],
                                    "stock_name": row['종목명'],
                                    "quantity": buy_quantity,
                                    "price": current_price,
                                    "total_amount": buy_quantity * current_price,
                                    "ma_period": ma_period,
                                    "ma_value": ma_value,
                                    "ma_condition": "정상매도후재매수",
                                    "period_div_code": period_div_code,
                                    "reason": f"정상 매도 후 재매수 조건 충족 (전일 종가 ${prev_close:.2f} > MA {ma_period}{period_unit} ${ma_value:.2f} && 전일 종가 > 정상 매도가 ${last_normal_sell_price:.2f})"
                                }
                                self.trade_history.add_trade(trade_data)
                                
                                self.logger.info(f"{row['종목명']}({stock_code}) - 정상 매도 후 재매수 완료 (매수조건 충족 여부와 관계없이)")
                            
                            # 다른 매수 조건 확인 스킵
                            return
                        else:
                            self.logger.info(f"{row['종목명']}({stock_code}) - 추가 매수 수량이 0 또는 음수")
            
            # 종목 유형에 따라 일간/주간 데이터 사용
            is_individual = any(s['종목코드'] == stock_code.split('.')[0] for _, s in self.individual_stocks.iterrows())
            # 매수기준2의 값에 따라 일봉/주봉 결정
            if is_individual:
                # 개별 종목인 경우 해당 종목 찾기
                individual_match = self.individual_stocks[self.individual_stocks['종목코드'] == stock_code.split('.')[0]]
                if not individual_match.empty:
                    period_div_code = individual_match.iloc[0].get('매수기준2', '일')
                    period_div_code = "D" if period_div_code == "일" else "W"
                else:
                    period_div_code = "D"  # 찾지 못한 경우 기본값
            else:
                # POOL 종목인 경우 해당 종목 찾기
                pool_match = self.pool_stocks[self.pool_stocks['종목코드'] == stock_code.split('.')[0]]
                if not pool_match.empty:
                    period_div_code = pool_match.iloc[0].get('매수기준2', '주')
                    period_div_code = "D" if period_div_code == "일" else "W"
                else:
                    period_div_code = "W"  # 찾지 못한 경우 기본값
            
            # 매수 조건 체크
            should_buy, ma = self.check_buy_condition(stock_code, ma_period, prev_close, ma_condition, period_div_code)
            
            if should_buy and ma is not None:
                # 당일 매도 종목 체크
                if self.is_sold_today(stock_code):
                    msg = f"당일 매도 종목 재매수 제한 - {row['종목명']}({stock_code})"
                    self.logger.info(msg)
                    return
                
                buy_msg = f"매수 조건 성립 - {row['종목명']}({stock_code})"
                period_unit = "일선" if period_div_code == "D" else "주선"
                
                if ma_condition == "종가":
                    buy_msg += f": 전일 종가(${prev_close:.2f})가 {ma_period}{period_unit}(${ma:.2f})을 상향돌파"
                else:
                    buy_msg += f": {ma_condition}{period_unit}이 {ma_period}{period_unit}을 골든크로스"
                self.logger.info(buy_msg)
                
                # 최대 보유 종목 수 체크 (개별 종목과 POOL 종목 각각 체크)
                total_individual_holdings = len([h for h in balance['output1'] if int(h.get('ord_psbl_qty', 0)) > 0 and any(s['종목코드'] == h['ovrs_pdno'].split('.')[0] for _, s in self.individual_stocks.iterrows())])
                total_pool_holdings = len([h for h in balance['output1'] if int(h.get('ord_psbl_qty', 0)) > 0 and any(s['종목코드'] == h['ovrs_pdno'].split('.')[0] for _, s in self.pool_stocks.iterrows())])
                
                # 현재 종목이 개별 종목인지 POOL 종목인지 확인
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
                
                # 매수 금액 계산 (총자산 * 배분비율)
                buy_amount = total_assets * allocation_ratio
                total_quantity = int(buy_amount / current_price)
                
                if total_quantity <= 0:
                    msg = f"매수 자금 부족 - {row['종목명']}({stock_code})"
                    msg += f"\n필요자금: ${current_price:.2f}/주 | 가용자금: ${buy_amount:.2f}"
                    self.logger.info(msg)
                    return
                
                # 현금 부족 시 POOL 종목 매도 로직
                required_cash = total_quantity * current_price
                
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
                    # 매수 수량 재계산 (가용 현금 기준)
                    buy_quantity = int(available_cash / current_price)
                    self.logger.info(f"현금 부족으로 매수 수량 조정: {total_quantity} -> {buy_quantity}주")
                else:
                    # 전체 수량 매수
                    buy_quantity = total_quantity
                
                if buy_quantity <= 0:
                    self.logger.info(f"{row['종목명']}({stock_code}) - 매수 가능 수량이 0")
                    return
                
                # 매수 주문 실행
                buy_price = current_price * 1.01  # 지정가의 1% 높게 설정하여 시장가처럼 거래
                result = self._retry_api_call(
                    self.us_api.order_stock,
                    stock_code,
                    "BUY",
                    buy_quantity,
                    buy_price
                )
                
                if result:
                    msg = f"매수 주문 실행: {row['종목명']}({stock_code}) {buy_quantity}주"
                    period_unit = "일" if period_div_code == "D" else "주"
                    msg += f"\n- 매수 사유: 이동평균 상향돌파 (전일종가: ${prev_close:.2f} > {ma_period}{period_unit}선: ${ma:.2f})"
                    msg += f"\n- 매수 금액: ${buy_quantity * current_price:.2f}"
                    msg += f"\n- 배분 비율: {allocation_ratio*100:.1f}%"
                    self.logger.info(msg)
                    
                    # 거래 내역 저장
                    trade_data = {
                        "trade_type": "BUY",
                        "trade_action": "BUY",
                        "stock_code": stock_code,
                        "stock_name": row['종목명'],
                        "quantity": buy_quantity,
                        "price": current_price,
                        "total_amount": buy_quantity * current_price,
                        "ma_period": ma_period,
                        "ma_value": ma,
                        "ma_condition": ma_condition,
                        "period_div_code": period_div_code,
                        "reason": f"{period_div_code}봉 기준 {ma_condition} {ma_period}{period_unit}선 매수 조건 충족",
                        "order_type": "BUY"
                    }
                    self.trade_history.add_trade(trade_data)
                    
                    self.logger.info(f"{row['종목명']}({stock_code}) - 매수 주문 성공")
                else:
                    self.logger.error(f"{row['종목명']}({stock_code}) - 매수 주문 실패")
        
        except Exception as e:
            self.logger.error(f"종목 매수 처리 중 오류 발생: {str(e)}")
    
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
                
                # 현재가를 새로운 최고가로 사용하고 데이터베이스에 저장
                highest_price = current_price
                self.trade_history.update_highest_price(stock_code, current_price)
                self.logger.debug(f"최고가 데이터베이스 업데이트: {stock_code}, ${current_price:.2f}")
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
                    # 현재가
                    current_price = round(float(current_price_data['output']['last']), 2)
                    stock_code = holding['ovrs_pdno']
                    stock_name = holding['ovrs_item_name']
                    
                    # 구글 스프레드시트에 없는 종목이더라도 stock_history에 정보 업데이트
                    # trade_history에 stock_history 데이터 추가
                    trade_data = {
                        "trade_type": "USER",
                        "trade_action": "BUY",
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "quantity": int(holding['ovrs_cblc_qty']),
                        "price": current_price,
                        "total_amount": current_price * int(holding['ovrs_cblc_qty']),
                        "reason": "주식현황 업데이트"
                    }
                    self.trade_history.add_trade(trade_data)
                    
                    holdings_data.append([
                        stock_code,                                           # 종목코드
                        stock_name,                                          # 종목명
                        current_price,                                       # 현재가
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

    def _check_ma_cross_below_since_ts_sell(self, stock_code: str, ts_sell_date: str, ma_period: int, period_div_code: str) -> bool:
        """TS 매도 이후 종목이 이평선을 한 번이라도 이탈했는지 확인합니다.
        
        Args:
            stock_code (str): 종목 코드
            ts_sell_date (str): TS 매도 날짜 (YYYY-MM-DD 형식)
            ma_period (int): 이동평균 기간
            period_div_code (str): 기간 구분 코드 (D: 일봉, W: 주봉)
            
        Returns:
            bool: 이평선 이탈 이력 여부 (True: 이탈했음, False: 이탈하지 않음)
        """
        try:
            # TS 매도 이후의 시세 데이터 조회
            end_date = datetime.now(self.us_timezone).strftime("%Y%m%d")
            sell_date_obj = datetime.strptime(ts_sell_date, "%Y-%m-%d")
            start_date = sell_date_obj.strftime("%Y%m%d")
            
            # 미국 주식의 경우 거래소 코드 필요
            full_code = stock_code
            if '.' not in stock_code:
                # 거래소 코드 조회 시도
                for _, row in self.individual_stocks.iterrows():
                    if row['종목코드'] == stock_code:
                        full_code = f"{stock_code}.{row['거래소']}"
                        break
                
                if '.' not in full_code:
                    for _, row in self.pool_stocks.iterrows():
                        if row['종목코드'] == stock_code:
                            full_code = f"{stock_code}.{row['거래소']}"
                            break
            
            # 일봉/주봉 데이터 조회
            hist_data = self._retry_api_call(
                self.us_api.get_daily_price,
                full_code,
                start_date,
                end_date,
                period_div_code
            )
            
            if hist_data is None or len(hist_data) < 2:
                self.logger.error(f"{stock_code} - TS 매도 이후 시세 데이터 조회 실패")
                return False
            
            # 날짜 기준으로 정렬 (오래된 순)
            df = hist_data if isinstance(hist_data, pd.DataFrame) else pd.DataFrame(hist_data)
            df['xymd'] = pd.to_datetime(df['xymd'])
            df = df.sort_values('xymd', ascending=True)
            
            # TS 매도일 이후 데이터만 필터링 (매도일 제외)
            sell_date_formatted = pd.to_datetime(sell_date_obj)
            df_after_sell = df[df['xymd'] > sell_date_formatted]
            
            if len(df_after_sell) == 0:
                self.logger.info(f"{stock_code} - TS 매도 이후 시세 데이터가 없습니다")
                return False
            
            # 각 날짜에 대해 이동평균 계산 및 종가와 비교
            for _, row_data in df_after_sell.iterrows():
                date = row_data['xymd']
                close = float(row_data['clos'])
                
                # 해당 날짜까지의 이동평균 계산
                # 이동평균 계산을 위한 데이터 조회
                ma_end_date = date.strftime("%Y%m%d") if isinstance(date, pd.Timestamp) else date
                ma_start_date = (pd.to_datetime(ma_end_date) - timedelta(days=ma_period*2)).strftime("%Y%m%d")
                
                ma_hist_data = self._retry_api_call(
                    self.us_api.get_daily_price,
                    full_code,
                    ma_start_date,
                    ma_end_date,
                    period_div_code
                )
                
                if ma_hist_data is None or len(ma_hist_data) < ma_period:
                    continue
                
                # 날짜 정렬
                ma_df = ma_hist_data if isinstance(ma_hist_data, pd.DataFrame) else pd.DataFrame(ma_hist_data)
                ma_df['xymd'] = pd.to_datetime(ma_df['xymd'])
                ma_df = ma_df.sort_values('xymd', ascending=True)
                
                # 이동평균 계산
                ma_df['clos'] = ma_df['clos'].astype(float)
                ma_series = ma_df['clos'].rolling(window=ma_period).mean()
                
                if len(ma_series) < 1 or pd.isna(ma_series.iloc[-1]):
                    continue
                
                ma_value = ma_series.iloc[-1]
                
                # 종가가 이동평균보다 낮으면 이탈 확인
                period_unit = "일" if period_div_code == "D" else "주"
                if close < ma_value:
                    self.logger.info(f"{stock_code} - {date.strftime('%Y%m%d') if isinstance(date, pd.Timestamp) else date} 종가(${close:.2f})가 {ma_period}{period_unit}선(${ma_value:.2f}) 아래로 이탈 확인")
                    return True
            
            # 이탈 이력 없음
            self.logger.info(f"{stock_code} - TS 매도 이후 이평선 이탈 이력 없음")
            return False
            
        except Exception as e:
            self.logger.error(f"{stock_code} - 이평선 이탈 확인 중 오류 발생: {str(e)}")
            return False