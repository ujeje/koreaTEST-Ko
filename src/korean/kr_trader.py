import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from src.common.base_trader import BaseTrader
from src.korean.kis_kr_api import KISKRAPIManager
from src.utils.trade_history_manager import TradeHistoryManager
import time
import pytz  # 시간대 처리를 위한 pytz 추가

class KRTrader(BaseTrader):
    """한국 주식 트레이더"""
    
    def __init__(self, config_path: str):
        """
        Args:
            config_path (str): 설정 파일 경로
        """
        super().__init__(config_path, "KOR")
        self.kr_api = KISKRAPIManager(config_path)
        self.trade_history = TradeHistoryManager("KOR")
        self.load_settings()
        self.last_api_call = 0
        self.api_call_interval = 0.2  # API 호출 간격 (초)
        self.max_retries = 3  # 최대 재시도 횟수
        
        # 한국 시간대 설정
        self.kr_timezone = pytz.timezone("Asia/Seoul")
        
        # 최고가 캐시 관련 변수 추가
        self.highest_price_cache = {}  # 종목별 최고가 캐시
        self.highest_price_cache_date = None  # 최고가 캐시 갱신 날짜
        
        # 휴장일 캐시 관련 변수 추가
        self.holiday_cache = {}  # 날짜별 휴장일 캐시
        self.holiday_cache_date = None  # 휴장일 캐시 갱신 날짜
        
        # 장 시작 메시지 표시 여부
        self.market_open_executed = False
        self.last_market_date = None
    
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
        now = datetime.now(self.kr_timezone)
        current_date = now.strftime("%Y%m%d")
        current_time = now.strftime("%H%M")
        
        # 주말 체크
        if now.weekday() >= 5:  # 5: 토요일, 6: 일요일
            self.logger.info("주말은 거래일이 아닙니다.")
            return False
        
        # 장 운영 시간 체크 - config 설정값 사용
        if not (self.config['trading']['kor_market_start'] <= current_time <= self.config['trading']['kor_market_end']):
            self.logger.info(f"현재 장 운영 시간이 아닙니다. (현재시간: {current_time}, 장 운영시간: {self.config['trading']['kor_market_start']}~{self.config['trading']['kor_market_end']})")
            # 장 시간이 지나면 다음날을 위해 초기화
            if current_time > self.config['trading']['kor_market_end']:
                self.market_open_executed = False
            return False
        
        # 날짜가 변경되었으면 market_open_executed 초기화
        if self.last_market_date != current_date:
            self.market_open_executed = False
            self.last_market_date = current_date
        
        # 모의투자일 경우 휴장일 체크 없이 주말 아니면 개장일로 간주
        if self.is_paper_trading:
            # 메시지는 하루에 한 번만 출력
            if not self.market_open_executed:
                self.logger.info(f"모의투자 모드: 휴장일 체크 생략. 주말이 아니므로 개장일로 간주합니다.")
                self.logger.info(f"오늘({current_date})은 개장일입니다. 장 운영 시간: {self.config['trading']['kor_market_start']}~{self.config['trading']['kor_market_end']}")
                self.market_open_executed = True
            return True
        
        # 실전투자일 경우에만 휴장일 API를 사용하여 확인
        # 휴장일 정보는 API를 통해 확인
        # 휴장일 캐시가 오늘 날짜의 것인지 확인
        if self.holiday_cache_date != current_date:
            # 오늘 날짜의 휴장일 정보가 캐시에 없으면 API 호출
            self.logger.info(f"{current_date} 휴장일 정보를 API를 통해 조회합니다.")
            holiday_info = self.kr_api.check_holiday(current_date)
            if holiday_info:
                self.holiday_cache[current_date] = holiday_info
                self.holiday_cache_date = current_date
                self.logger.info(f"휴장일 정보 캐시 업데이트 완료: {holiday_info}")
            else:
                self.logger.error("휴장일 정보를 가져오는데 실패했습니다.")
                return False
        else:
            # 캐시에서 정보 가져오기
            holiday_info = self.holiday_cache.get(current_date)
            if not holiday_info:
                self.logger.error("캐시된 휴장일 정보가 없습니다.")
                return False
        
        # 개장일 여부 확인 (opnd_yn: 'Y'인 경우 개장일)
        is_open = holiday_info.get('opnd_yn') == 'Y'
        if not is_open:
            self.logger.info(f"오늘({current_date})은 개장일이 아닙니다. (휴장)")
        else:
            # 메시지는 하루에 한 번만 출력
            if not self.market_open_executed:
                self.logger.info(f"오늘({current_date})은 개장일입니다. 장 운영 시간: {self.config['trading']['kor_market_start']}~{self.config['trading']['kor_market_end']}")
                self.market_open_executed = True
        
        return is_open
    
    def calculate_ma(self, stock_code, period, period_div_code):
        """
        주어진 종목의 이동평균선 계산
        Args:
            stock_code: 종목코드
            period: 이동평균 기간
            period_div_code: 일/주 구분 코드
        Returns:
            (전전봉 이동평균, 전봉 이동평균)
        """
        today = datetime.now()
        end_date = today.strftime("%Y%m%d")
        
        # 필요한 데이터 기간 계산
        required_days = period * 2
        required_weeks = period * 2
        
        if period_div_code == "D":
            # API 제한(100일)을 고려한 효율적인 데이터 조회
            if required_days <= 100:
                # 100일 이내의 데이터는 한 번에 조회 가능
                start_date = (today - timedelta(days=required_days)).strftime("%Y%m%d")
                
                try:
                    hist_data = self._retry_api_call(
                        self.kr_api.get_daily_price,
                        stock_code,
                        start_date,
                        end_date,
                        period_div_code
                    )
                    
                    if hist_data is not None and len(hist_data) >= period + 2:
                        # DataFrame으로 변환 (이미 DataFrame으로 반환될 경우 변환 생략)
                        df = hist_data if isinstance(hist_data, pd.DataFrame) else pd.DataFrame(hist_data)
                        # 정렬
                        df['stck_bsop_date'] = pd.to_datetime(df['stck_bsop_date'])
                        df = df.sort_values('stck_bsop_date', ascending=True).reset_index(drop=True)
                        # 이동평균 계산
                        df['stck_clpr'] = df['stck_clpr'].astype(float)
                        ma_series = df['stck_clpr'].rolling(window=period).mean()
                        # 전전일, 전일 이동평균값 반환
                        ma_prev2 = ma_series.iloc[-3]  # 전전일
                        ma_prev = ma_series.iloc[-2]   # 전일
                        return (ma_prev2, ma_prev)
                    
                    self.logger.warning(f"{stock_code}: 일간 데이터 부족, 계산 불가 (현재: {len(hist_data) if hist_data is not None else 0}개, 필요: {period+2}개)")
                    return None
                    
                except Exception as e:
                    self.logger.error(f"{stock_code}: 일간 데이터 조회 중 오류 발생 - {str(e)}")
                    return None
            else:
                # 100일 초과 데이터가 필요한 경우 분할 조회
                try:
                    # 분할 조회를 위한 설정
                    all_data = []
                    current_end_date = datetime.now()
                    # 필요한 기간의 2배로 여유있게 설정 (필요한 경우 조정 가능)
                    start_datetime = datetime.now() - timedelta(days=required_days * 2)
                    
                    while current_end_date >= start_datetime:
                        # 현재 조회 기간의 시작일 계산 (최대 100일)
                        current_start_date = current_end_date - timedelta(days=100)
                        
                        # 시작일보다 이전으로 가지 않도록 조정
                        if current_start_date < start_datetime:
                            current_start_date = start_datetime
                        
                        # 날짜 형식 변환
                        current_start_date_str = current_start_date.strftime("%Y%m%d")
                        current_end_date_str = current_end_date.strftime("%Y%m%d")
                        
                        self.logger.debug(f"일간 데이터 분할 조회: {stock_code}, {current_start_date_str} ~ {current_end_date_str}")
                        
                        # API 호출하여 데이터 조회
                        hist_data = self._retry_api_call(
                            self.kr_api.get_daily_price,
                            stock_code,
                            current_start_date_str,
                            current_end_date_str,
                            period_div_code
                        )
                        
                        if hist_data is not None and len(hist_data) > 0:
                            all_data.append(hist_data)
                        
                        # 다음 조회 기간 설정 (1일 겹치지 않게)
                        current_end_date = current_start_date - timedelta(days=1)
                        
                        # 필요한 데이터를 모두 조회했거나 시작일에 도달하면 종료
                        if current_end_date < start_datetime:
                            break
                    
                    # 조회된 데이터가 없는 경우
                    if not all_data:
                        self.logger.warning(f"{stock_code}: 일간 데이터 조회 실패, 데이터가 없습니다.")
                        return None
                    
                    # 모든 데이터 합치기
                    combined_df = pd.concat(all_data, ignore_index=True)
                    
                    # 중복 제거 (날짜 기준)
                    combined_df['stck_bsop_date'] = pd.to_datetime(combined_df['stck_bsop_date'])
                    combined_df = combined_df.drop_duplicates(subset=['stck_bsop_date'])
                    combined_df = combined_df.sort_values('stck_bsop_date', ascending=True).reset_index(drop=True)
                    
                    # 데이터가 충분한지 확인
                    if len(combined_df) < period + 2:
                        self.logger.warning(f"{stock_code}: 일간 데이터 부족, 계산 불가 (현재: {len(combined_df)}개, 필요: {period+2}개)")
                        return None
                    
                    # 이동평균 계산
                    combined_df['stck_clpr'] = combined_df['stck_clpr'].astype(float)
                    ma_series = combined_df['stck_clpr'].rolling(window=period).mean()
                    
                    # 전전일, 전일 이동평균값 반환
                    ma_prev2 = ma_series.iloc[-3]  # 전전일
                    ma_prev = ma_series.iloc[-2]   # 전일
                    return (ma_prev2, ma_prev)
                    
                except Exception as e:
                    self.logger.error(f"{stock_code}: 일간 데이터 분할 조회 중 오류 발생 - {str(e)}")
                    return None
                
        elif period_div_code == "W":
            # API 제한(100개)을 고려한 효율적인 데이터 조회
            # 주간 데이터는 데이터 포인트가 적을 수 있지만, 긴 기간의 경우 100주를 초과할 수 있음
            
            # 필요한 기간이 100주 이하인 경우 먼저 한 번에 조회 시도
            if required_weeks <= 100:
                # 100주 이내의 데이터 조회 시도
                start_date = (today - timedelta(days=required_weeks * 7)).strftime("%Y%m%d")
                
                try:
                    hist_data = self._retry_api_call(
                        self.kr_api.get_daily_price,
                        stock_code,
                        start_date,
                        end_date,
                        period_div_code
                    )
                    
                    if hist_data is not None and len(hist_data) >= period + 2:
                        # 주간 데이터로 변환
                        df = hist_data if isinstance(hist_data, pd.DataFrame) else pd.DataFrame(hist_data)
                        # 정렬
                        df['stck_bsop_date'] = pd.to_datetime(df['stck_bsop_date'])
                        df = df.sort_values('stck_bsop_date', ascending=True).reset_index(drop=True)
                        
                        # 이동평균 계산
                        df['stck_clpr'] = df['stck_clpr'].astype(float)
                        ma_series = df['stck_clpr'].rolling(window=period).mean()
                        
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
                current_end_date = datetime.now()
                # 주간 데이터는 7배 길어진다
                start_datetime = datetime.now() - timedelta(days=required_weeks * 7 * 2)  # 넉넉히 2배 기간으로 설정
                
                while current_end_date >= start_datetime:
                    # 현재 조회 기간의 시작일 계산 (최대 100주에 해당하는 700일)
                    current_start_date = current_end_date - timedelta(days=700)  # 100주 = 700일
                    
                    # 시작일보다 이전으로 가지 않도록 조정
                    if current_start_date < start_datetime:
                        current_start_date = start_datetime
                    
                    # 날짜 형식 변환
                    current_start_date_str = current_start_date.strftime("%Y%m%d")
                    current_end_date_str = current_end_date.strftime("%Y%m%d")
                    
                    self.logger.debug(f"주간 데이터 분할 조회: {stock_code}, {current_start_date_str} ~ {current_end_date_str}")
                    
                    # API 호출하여 데이터 조회
                    hist_data = self._retry_api_call(
                        self.kr_api.get_daily_price,
                        stock_code,
                        current_start_date_str,
                        current_end_date_str,
                        period_div_code
                    )
                    
                    if hist_data is not None and len(hist_data) > 0:
                        all_data.append(hist_data)
                    
                    # 다음 조회 기간 설정 (1일 겹치지 않게)
                    current_end_date = current_start_date - timedelta(days=1)
                    
                    # 필요한 데이터를 모두 조회했거나 시작일에 도달하면 종료
                    if current_end_date < start_datetime:
                        break
                
                # 조회된 데이터가 없는 경우
                if not all_data:
                    self.logger.warning(f"{stock_code}: 주간 데이터 조회 실패, 데이터가 없습니다.")
                    return None
                
                # 모든 데이터 합치기
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # 중복 제거 (날짜 기준)
                combined_df['stck_bsop_date'] = pd.to_datetime(combined_df['stck_bsop_date'])
                combined_df = combined_df.drop_duplicates(subset=['stck_bsop_date'])
                combined_df = combined_df.sort_values('stck_bsop_date', ascending=True).reset_index(drop=True)
                
                # 데이터가 충분한지 확인
                if len(combined_df) < period + 2:
                    self.logger.warning(f"{stock_code}: 주간 데이터 부족, 계산 불가 (현재: {len(combined_df)}개, 필요: {period+2}개)")
                    return None
                
                # 이동평균 계산
                combined_df['stck_clpr'] = combined_df['stck_clpr'].astype(float)
                ma_series = combined_df['stck_clpr'].rolling(window=period).mean()
                
                # 전전주, 전주 이동평균값 반환
                ma_prev2 = ma_series.iloc[-3]  # 전전주
                ma_prev = ma_series.iloc[-2]   # 전주
                return (ma_prev2, ma_prev)
            
            except Exception as e:
                self.logger.error(f"{stock_code}: 주간 데이터 분할 조회 중 오류 발생 - {str(e)}")
                return None
                
        # 유효하지 않은 period_div_code
        self.logger.error(f"유효하지 않은 기간 구분 코드: {period_div_code}")
        return None
    
    def get_highest_price_since_first_buy(self, stock_code: str) -> float:
        """최초 매수일 이후부터 어제까지의 최고가를 조회합니다."""
        try:
            # 현재 날짜 확인
            current_date = datetime.now().strftime("%Y-%m-%d")
            
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
            yesterday = datetime.now() - timedelta(days=1)
            
            # 최초 매수일이 어제보다 늦은 경우(즉, 오늘 처음 매수한 경우) 최고가는 0으로 설정
            if first_date > yesterday:
                self.logger.debug(f"{stock_code}: 최초 매수일({first_date.strftime('%Y-%m-%d')})이 어제보다 늦어 최고가 계산 불가")
                return 0
            
            # API 제한(100일)을 고려하여 데이터 조회
            all_data = []
            current_end_date = yesterday
            
            while current_end_date >= first_date:
                # 현재 조회 기간의 시작일 계산 (최대 100일)
                current_start_date = current_end_date - timedelta(days=99)
                
                # 최초 매수일보다 이전으로 가지 않도록 조정
                if current_start_date < first_date:
                    current_start_date = first_date
                
                # 날짜 형식 변환
                start_date_str = current_start_date.strftime("%Y%m%d")
                end_date_str = current_end_date.strftime("%Y%m%d")
                
                self.logger.debug(f"일별 시세 조회: {stock_code}, {start_date_str} ~ {end_date_str}")
                
                # API 호출하여 데이터 조회
                df = self.kr_api.get_daily_price(stock_code, start_date_str, end_date_str, "D")
                if df is not None and len(df) > 0:
                    all_data.append(df)
                
                # 다음 조회 기간 설정 (하루 겹치지 않게)
                current_end_date = current_start_date - timedelta(days=1)
                
                # 최초 매수일에 도달하면 종료
                if current_end_date < first_date:
                    break
            
            # 조회된 데이터가 없는 경우
            if not all_data:
                return 0
            
            # 모든 데이터 합치기
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 중복 제거 (날짜 기준)
            if 'stck_bsop_date' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['stck_bsop_date'])
            
            # 최고가 계산
            highest_price = 0
            if 'stck_hgpr' in combined_df.columns:
                highest_price = combined_df['stck_hgpr'].astype(float).max()
            
            # 캐시에 저장
            self.highest_price_cache[stock_code] = highest_price
            self.logger.debug(f"최고가 캐시 업데이트: {stock_code}, {highest_price}")
            
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
            
            # 기간 단위 설정
            period_unit = "일" if period_div_code == "D" else "주"
            
            # "종가" 조건: 전일 종가가 이동평균선 상향 돌파
            if ma_condition == "종가":
                # 전일 종가가 이동평균선을 상향 돌파했는지 확인
                # 즉, prev_close > ma_target_prev
                is_buy = prev_close > ma_target_prev
                if is_buy:
                    self.logger.info(f"매수 조건 충족: {stock_code} - 전일 종가({prev_close:.2f})가 {ma_period}{period_unit}선({ma_target_prev:.2f})을 상향돌파")
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
                        self.logger.info(f"골든크로스 발생: {stock_code}")
                        self.logger.info(f"- 전전{period_unit}: {condition_period}{period_unit}선(₩{ma_condition_prev2:.2f}) < {ma_period}{period_unit}선(₩{ma_target_prev2:.2f})")
                        self.logger.info(f"- 전{period_unit}: {condition_period}{period_unit}선(₩{ma_condition_prev:.2f}) > {ma_period}{period_unit}선(₩{ma_target_prev:.2f})")
                    
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
            
            # 기간 단위 설정
            period_unit = "일" if period_div_code == "D" else "주"
            
            # "종가" 조건: 전일 종가가 이동평균선 하향 돌파
            if ma_condition == "종가":
                # 전일 종가가 이동평균선 아래에 있는지 확인
                # 즉, prev_close < ma_target_prev
                is_sell = prev_close < ma_target_prev
                if is_sell:
                    self.logger.info(f"매도 조건 충족: {stock_code} - 전일 종가({prev_close:.2f})가 {ma_period}{period_unit}선({ma_target_prev:.2f}) 아래로 하향돌파")
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
                        self.logger.info(f"데드크로스 발생: {stock_code}")
                        self.logger.info(f"- 전전{period_unit}: {condition_period}{period_unit}선(₩{ma_condition_prev2:.2f}) > {ma_period}{period_unit}선(₩{ma_target_prev2:.2f})")
                        self.logger.info(f"- 전{period_unit}: {condition_period}{period_unit}선(₩{ma_condition_prev:.2f}) < {ma_period}{period_unit}선(₩{ma_target_prev:.2f})")
                    
                    return dead_cross, ma_target_prev
                    
                except (ValueError, TypeError):
                    self.logger.error(f"매도 조건 확인 중 오류: {ma_condition}이 유효한 이평선 기간이 아닙니다.")
                    return False, ma_target_prev
                
        except Exception as e:
            self.logger.error(f"매도 조건 확인 중 오류 발생 ({stock_code}): {str(e)}")
            return False, None
    
    def _is_rebalancing_day(self) -> bool:
        """리밸런싱 실행 여부를 확인합니다.
        
        리밸런싱 날짜 형식:
        1. 년/월/일 (예: 2023/12/15) - 해당 년월일에 리밸런싱
        2. 월/일 (예: 12/15) - 매년 해당 월일에 리밸런싱
        3. 일 (예: 15) - 매월 해당 일자에 리밸런싱
        """
        try:
            # 현재 시간 확인
            now = datetime.now()
            
            # 구글 시트에서 리밸런싱 일자 가져오기
            rebalancing_date = str(self.settings.get('rebalancing_date', ''))
            if not rebalancing_date:
                return False
                    
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
            total_balance = float(balance['output2'][0]['tot_evlu_amt'])
            self.logger.info(f"총 평가금액: {total_balance:,.0f}원")
            
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
                    
                    if target_ratio > 0:
                        holdings[stock_code] = {
                            'name': holding['prdt_name'],
                            'current_price': current_price,
                            'quantity': quantity,
                            'current_ratio': current_ratio,
                            'target_ratio': target_ratio,
                            'current_value': current_value
                        }
            
            # 리밸런싱 실행
            for stock_code, info in holdings.items():
                # 목표 비율과 현재 비율의 차이 계산 (미국 시장과 동일하게 목표 - 현재)
                ratio_diff = info['target_ratio'] - info['current_ratio']
                
                # 최소 리밸런싱 비율 차이 (0.5% 이상)
                if abs(ratio_diff) >= 0.5:
                    target_value = total_balance * (info['target_ratio'] / 100)
                    value_diff = target_value - info['current_value']
                    quantity_diff = int(abs(value_diff) / info['current_price'])
                    
                    if quantity_diff > 0:
                        if ratio_diff > 0:  # 매수 필요
                            # 매수 시 지정가의 1% 높게 설정하여 시장가처럼 거래
                            buy_price = info['current_price']
                            
                            result = self._retry_api_call(
                                self.kr_api.order_stock,
                                stock_code,
                                "BUY",
                                quantity_diff
                            )
                            
                            if result:
                                msg = f"리밸런싱 매수: {info['name']}({stock_code}) {quantity_diff}주"
                                msg += f"\n- 현재 비중: {info['current_ratio']:.1f}% → 목표 비중: {info['target_ratio']:.1f}%"
                                msg += f"\n- 현재가: {info['current_price']:,}원"
                                msg += f"\n- 매수 금액: {value_diff:,.0f}원"
                                self.logger.info(msg)
                                
                                # 거래 내역 저장
                                trade_data = {
                                    "trade_type": "REBALANCE",
                                    "trade_action": "BUY",
                                    "stock_code": stock_code,
                                    "stock_name": info['name'],
                                    "quantity": quantity_diff,
                                    "price": buy_price,
                                    "total_amount": abs(value_diff),
                                    "reason": f"리밸런싱 매수 (현재 비중 {info['current_ratio']:.1f}% → 목표 비중 {info['target_ratio']:.1f}%)",
                                    "order_type": "BUY"
                                }
                                self.trade_history.add_trade(trade_data)
                                
                        else:  # 매도 필요
                            # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                            sell_price = info['current_price']
                            
                            result = self._retry_api_call(
                                self.kr_api.order_stock,
                                stock_code,
                                "SELL",
                                quantity_diff
                            )
                            
                            if result:
                                msg = f"리밸런싱 매도: {info['name']}({stock_code}) {quantity_diff}주"
                                msg += f"\n- 현재 비중: {info['current_ratio']:.1f}% → 목표 비중: {info['target_ratio']:.1f}%"
                                msg += f"\n- 현재가: {info['current_price']:,}원"
                                msg += f"\n- 매도 금액: {abs(value_diff):,.0f}원"
                                self.logger.info(msg)
                                
                                # 당일 매도 종목 캐시에 추가
                                self.sold_stocks_cache.append(stock_code)
                                
                                # 거래 내역 저장
                                trade_data = {
                                    "trade_type": "REBALANCE",
                                    "trade_action": "SELL",
                                    "stock_code": stock_code,
                                    "stock_name": info['name'],
                                    "quantity": quantity_diff,
                                    "price": sell_price,
                                    "total_amount": abs(value_diff),
                                    "reason": f"리밸런싱 매도 (현재 비중 {info['current_ratio']:.1f}% → 목표 비중 {info['target_ratio']:.1f}%)",
                                    "order_type": "SELL"
                                }
                                self.trade_history.add_trade(trade_data)
            
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
                self.sold_stocks_cache = []  # 당일 매도 종목 캐시 초기화
                self.sold_stocks_cache_time = 0  # 캐시 시간 초기화
                self.logger.info(f"=== {self.execution_date} 매매 시작 ===")
            
            # 1. 스탑로스/트레일링 스탑 체크 (매 루프마다 실행)
            self._check_stop_conditions()
            
            # 장 시작 시간 체크 (09:00)
            market_open_time = self.config['trading']['kor_market_start']
            market_open_end_time = f"{int(market_open_time) + 10:04d}"  # 10분 이내
            
            # 장 시작 시간에 매매 실행 (아직 실행되지 않은 경우, 장 시작 시간부터 10분간만 허용)
            if current_time >= market_open_time and current_time <= market_open_end_time and not self.market_open_executed:
                self.logger.info(f"장 시작 시간 도달: {market_open_time[:2]}:{market_open_time[2:]}")
                
                # 1. 시가 매도 실행
                self.logger.info("1. 시가 매도 실행")
                self._execute_sell_orders()
                
                # 2. 리밸런싱 체크 및 실행
                if self._is_rebalancing_day():
                    self.logger.info("2. 리밸런싱 실행")
                    
                    # 계좌 잔고 조회
                    balance = self._retry_api_call(self.kr_api.get_account_balance)
                    if balance is not None:
                        self._rebalance_portfolio(balance)
                    else:
                        self.logger.error("계좌 잔고 조회 실패로 리밸런싱을 실행할 수 없습니다.")
                
                # 3. 시가 매수 실행
                self.logger.info("3. 시가 매수 실행")
                self._execute_buy_orders()
                
                self.market_open_executed = True
                self.logger.info("장 시작 매매 실행 완료")
                
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
            cash = float(balance['output2'][0]['tot_evlu_amt'])
            self.logger.info(f"현재 총 평가금액: {cash:,.0f}원")
            
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
            individual_candidates = []
            pool_candidates = []
            
            self.logger.info("매수 조건 체크 시작")
            
            # 개별 종목 매수 조건 체크
            for _, row in self.individual_stocks.iterrows():
                candidates = self._check_stock_buy_condition(row, holdings, cash, 'individual')
                if candidates:
                    individual_candidates.extend(candidates)
            
            # POOL 종목 매수 조건 체크
            for _, row in self.pool_stocks.iterrows():
                candidates = self._check_stock_buy_condition(row, holdings, cash, 'pool')
                if candidates:
                    pool_candidates.extend(candidates)
            
            # 개별 종목을 우선 처리하고, 그 다음 POOL 종목 처리
            buy_candidates = individual_candidates + pool_candidates
            
            # 매수 후보가 없으면 종료
            if not buy_candidates:
                self.logger.info("매수 조건을 충족하는 종목이 없습니다.")
                return
            
            # 매수 후보 정렬 (구글 스프레드시트 순서대로)
            buy_candidates.sort(key=lambda x: (
                self.individual_stocks[self.individual_stocks['종목코드'] == x['code']].index.min() 
                if x['type'] == 'individual'
                else self.pool_stocks[self.pool_stocks['종목코드'] == x['code']].index.min()
            ))
            
            # 최대 종목 수 제한
            max_individual = self.settings['max_individual_stocks']
            max_pool = self.settings['max_pool_stocks']
            
            # 현재 보유 종목 수 확인
            current_individual = sum(1 for code in holdings if code in self.individual_stocks['종목코드'].values)
            current_pool = sum(1 for code in holdings if code in self.pool_stocks['종목코드'].values)
            
            # 매수 가능 종목 수 계산
            available_individual = max(0, max_individual - current_individual)
            available_pool = max(0, max_pool - current_pool)
            
            self.logger.info(f"[국내 시장] 매수 후보 종목 수: {len(buy_candidates)}개")
            self.logger.info(f"[국내 시장] 매수 가능 개별 종목 수: {available_individual}개, POOL 종목 수: {available_pool}개")
            
            # 매수 실행
            for candidate in buy_candidates:
                stock_code = candidate['code']
                stock_name = candidate['name']
                quantity = candidate['quantity']
                price = candidate['price']
                ma_period = candidate['ma_period']
                ma_value = candidate['ma_value']
                prev_close = candidate['prev_close']
                
                # 종목 유형 확인 (개별/POOL)
                is_individual = candidate['type'] == 'individual'
                
                # 최대 종목 수 체크
                if is_individual and available_individual <= 0:
                    self.logger.info(f"{stock_name}({stock_code}) - 최대 개별 종목 수 초과")
                    continue
                elif not is_individual and available_pool <= 0:
                    self.logger.info(f"{stock_name}({stock_code}) - 최대 POOL 종목 수 초과")
                    continue
                
                # 현금 확인
                required_cash = quantity * price
                
                # 개별 종목이고 현금이 부족한 경우 POOL 종목 매도 시도
                if is_individual and required_cash > cash:
                    self.logger.info(f"현금 부족: 필요 금액 {required_cash:,.0f}원, 가용 금액 {cash:,.0f}원")
                    self.logger.info(f"POOL 종목 매도를 통한 현금 확보 시도")
                    
                    # POOL 종목 보유 현황 확인
                    pool_holdings = []
                    for holding_code, holding_info in holdings.items():
                        # POOL 종목인지 확인
                        if holding_code in self.pool_stocks['종목코드'].values:
                            pool_holdings.append({
                                'code': holding_code,
                                'name': holding_info['name'],
                                'quantity': holding_info['quantity'],
                                'price': holding_info['current_price'],
                                'value': holding_info['quantity'] * holding_info['current_price']
                            })
                    
                    # 구글 스프레드시트 순서의 역순으로 정렬 (마지막에 추가된 종목부터 매도)
                    pool_codes = self.pool_stocks['종목코드'].tolist()
                    pool_holdings.sort(key=lambda x: pool_codes.index(x['code']) if x['code'] in pool_codes else float('inf'), reverse=True)
                    
                    cash_to_secure = required_cash - cash
                    secured_cash = 0
                    sold_stocks = []
                    
                    # 필요한 현금을 확보할 때까지 POOL 종목 매도
                    for pool_stock in pool_holdings:
                        if secured_cash >= cash_to_secure:
                            break
                            
                        sell_quantity = pool_stock['quantity']
                        expected_cash = sell_quantity * pool_stock['price']
                        
                        # 매도 주문 실행
                        result = self._retry_api_call(
                            self.kr_api.order_stock,
                            pool_stock['code'],
                            "SELL",
                            sell_quantity
                        )
                        
                        if result and result['rt_cd'] == '0':
                            secured_cash += expected_cash
                            sold_stocks.append(f"{pool_stock['name']}({pool_stock['code']}) {sell_quantity}주 ({expected_cash:,.0f}원)")
                            self.logger.info(f"현금 확보를 위한 POOL 종목 매도: {pool_stock['name']}({pool_stock['code']}) {sell_quantity}주 ({expected_cash:,.0f}원)")
                    
                    if secured_cash >= cash_to_secure:
                        self.logger.info(f"현금 확보 성공: {secured_cash:,.0f}원 (필요 금액: {cash_to_secure:,.0f}원)")
                        self.logger.info(f"매도한 POOL 종목: {', '.join(sold_stocks)}")
                        
                        # 매도 후 충분한 시간 대기 (주문 체결 시간 고려)
                        self.logger.info("매도 주문 체결 대기 중... (5초)")
                        time.sleep(5)  # 매도 주문 체결을 위해 5초 대기
                        
                        # 현금 업데이트
                        cash += secured_cash
                    else:
                        self.logger.info(f"현금 확보 실패: {secured_cash:,.0f}원 (필요 금액: {cash_to_secure:,.0f}원)")
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
                    
                    # 거래 내역 저장
                    if candidate['type'] == 'individual':
                        period_div_code = "D"
                        period_unit = "일"
                    else:
                        period_div_code = "W"
                        period_unit = "주"
                    
                    ma_condition = candidate.get('ma_condition', '종가')
                    
                    reason = ""
                    if ma_condition == "종가":
                        reason = f"{ma_period}{period_unit}선 매수 조건 충족 (전일 종가 {prev_close:,.0f}원 > MA {ma_value:,.0f}원)"
                    elif ma_condition == "정상매도후재매수":
                        reason = f"정상 매도 후 재매수 조건 충족 (전일 종가 {prev_close:,.0f}원 > 이평선, 현재가 > 직전 정상 매도가)"
                    elif ma_condition == "트레일링스탑매도후재매수":
                        reason = f"트레일링 스탑 매도 후 재매수 조건 충족 (매수 조건 충족 & 매도가 이상으로 상승)"
                    else:
                        reason = f"{ma_condition}{period_unit}선과 {ma_period}{period_unit}선의 골든크로스 발생"
                    
                    trade_data = {
                        "trade_type": "BUY",
                        "trade_action": "BUY",
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "quantity": quantity,
                        "price": price,
                        "total_amount": quantity * price,
                        "ma_period": ma_period,
                        "ma_value": ma_value,
                        "reason": reason,
                        "order_type": "BUY"
                    }
                    self.trade_history.add_trade(trade_data)
                    
                    # 매수 상세 정보 로깅
                    msg = f"매수 주문 실행: {stock_name}({stock_code}) {quantity}주"
                    msg += f"\n- 매수 사유: {reason}"
                    msg += f"\n- 매수 금액: {quantity * price:,.0f}원"
                    msg += f"\n- 배분 비율: {candidate['allocation_ratio']*100:.1f}%"
                    self.logger.info(msg)
                    
                    # 현금 차감
                    cash -= quantity * price
                    
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
    
    def _check_stock_buy_condition(self, row, holdings, cash, stock_type):
        """종목의 매수 조건을 확인합니다.
        
        Args:
            row: 종목 정보 행
            holdings: 보유 종목 정보
            cash: 사용 가능한 현금
            stock_type: 종목 타입 ('individual' 또는 'pool')
            
        Returns:
            list: 매수 후보 리스트
        """
        candidates = []
        stock_code = row['종목코드']
        stock_name = row['종목명']
        ma_period = int(row['매수기준'])
        allocation_ratio = float(row['배분비율']) / 100
        
        # 매수 기간 체크 (설정된 경우)
        if 'buy_start_date' in row and 'buy_end_date' in row:
            if not self._is_within_buy_period(row):
                self.logger.info(f"{stock_name}({stock_code}) - 매수 기간이 아님")
                return candidates
        
        # 이미 보유 중인 종목은 스킵
        if stock_code in holdings:
            self.logger.info(f"{stock_name}({stock_code}) - 이미 보유 중")
            return candidates
            
        # 당일 매도한 종목은 스킵
        sold_stocks = self.get_today_sold_stocks()
        if stock_code in sold_stocks:
            self.logger.info(f"{stock_name}({stock_code}) - 당일 매도 종목 재매수 제한")
            return candidates
        
        # 매수 조건 관련 설정값 가져오기
        ma_period = int(row['매수기준'])
        ma_condition = row.get('매수조건', '종가')
        period_div_code_raw = row.get('매수기준2', '일')
        period_div_code = "D" if period_div_code_raw == "일" else "W"
        period_unit = "일" if period_div_code == "D" else "주"
        
        # 현재가 조회
        price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
        if not price_data:
            self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
            return candidates
        
        current_price = float(price_data['output']['stck_prpr'])
        prev_close = float(price_data['output']['stck_sdpr'])
        
        # 기준 이평선 계산 (매수기준에 지정된 이평선)
        ma_values = self.calculate_ma(stock_code, ma_period, period_div_code)
        if ma_values is None:
            self.logger.error(f"{stock_name}({stock_code}) - {ma_period}{period_unit}선 계산 실패")
            return candidates
        
        ma_prev2, ma_prev = ma_values  # 전전일/전전주, 전일/전주 기준 이평선 값
        
        # 트레일링 스탑으로 매도된 종목 체크
        trailing_stop_price = self.get_trailing_stop_sell_price(stock_code)
        if trailing_stop_price is not None:
            self.logger.info(f"{stock_name}({stock_code}) - 트레일링 스탑 매도 이력 있음 (매도가: {trailing_stop_price:,}원)")
            
            # TS 매도 이후 이평선 이탈 여부 확인
            ts_sell_date = self.trade_history.get_last_ts_sell_date(stock_code)
            if ts_sell_date is None:
                self.logger.error(f"{stock_name}({stock_code}) - TS 매도 날짜 조회 실패")
                return candidates
            
            self.logger.info(f"{stock_name}({stock_code}) - 마지막 TS 매도 날짜: {ts_sell_date}")
            
            # 현재 종가가 이평선 위에 있는지 확인
            is_above_ma = prev_close > ma_prev
            
            if is_above_ma:
                # 이평선 위에 있는 경우 추가 조건 체크
                
                # 조건 1: 전일 종가가 TS 매도가보다 큰지 확인
                is_above_ts_price = prev_close > trailing_stop_price
                if is_above_ts_price:
                    self.logger.info(f"{stock_name}({stock_code}) - 전일 종가({prev_close:,.0f}원)가 TS 매도가({trailing_stop_price:,.0f}원)보다 크므로 매수조건 충족 여부와 관계없이 즉시 재매수")
                    
                    # 즉시 재매수 후보 추가 (매수 조건을 보지 않고 즉시 추가)
                    buy_amount = cash * allocation_ratio
                    buy_quantity = int(buy_amount / current_price)
                    
                    if buy_quantity > 0:
                        candidates.append({
                            'code': stock_code,
                            'name': stock_name,
                            'quantity': buy_quantity,
                            'price': current_price,
                            'amount': buy_quantity * current_price,
                            'allocation_ratio': allocation_ratio,
                            'ma_period': ma_period,
                            'ma_value': ma_prev,
                            'prev_close': prev_close,
                            'ma_condition': "트레일링스탑매도후재매수",
                            'type': stock_type
                        })
                        # 바로 반환하여 다른 조건 확인하지 않음
                        return candidates
                else:
                    # 조건 2: TS 매도 이후 이평선을 한 번이라도 이탈했는지 확인
                    has_crossed_below = self._check_ma_cross_below_since_ts_sell(stock_code, ts_sell_date, ma_period, period_div_code)
                    
                    if not has_crossed_below:
                        # 두 조건 모두 충족하지 않으면 재매수 제한
                        self.logger.info(f"{stock_name}({stock_code}) - TS 매도 이후 {ma_period}{period_unit}선 이탈 이력 없고, 전일 종가가 TS 매도가보다 작아 재매수 제한")
                        return candidates
                    else:
                        # 이탈 이력이 있으면 매수 조건 확인 후 재매수 가능 (다음 단계로 진행)
                        self.logger.info(f"{stock_name}({stock_code}) - TS 매도 이후 {ma_period}{period_unit}선 이탈 이력 있어 매수 조건 확인 후 재매수 가능")
            else:
                # 현재 이평선 아래에 있으면 매수 조건에 따라 결정 (다음 단계로 넘김)
                self.logger.info(f"{stock_name}({stock_code}) - 현재 {ma_period}{period_unit}선 아래에 있음, 매수 조건에 따라 결정")
        
        # 정상 매도된 종목 체크 (정상매도된 종목 재매수 조건)
        last_normal_sell_price = self.get_last_normal_sell_price(stock_code)
        if last_normal_sell_price is not None:
            # 현재가 조회
            price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
            if not price_data:
                self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
                return candidates
            
            current_price = float(price_data['output']['stck_prpr'])
            prev_close = float(price_data['output']['stck_sdpr'])
            
            # 매수 조건 체크
            ma_period = int(row['매수기준'])
            ma_condition = row.get('매수조건', '종가')
            
            # 매수기준2에 따른 일/주 구분 - 명확하게 처리
            period_div_code_raw = row.get('매수기준2', '일')
            period_div_code = "D" if period_div_code_raw == "일" else "W"
            period_unit = "일" if period_div_code == "D" else "주"
            
            # 매수기준 이평선 계산
            ma_values = self.calculate_ma(stock_code, ma_period, period_div_code)
            if ma_values is None:
                self.logger.error(f"{stock_name}({stock_code}) - {ma_period}{period_unit}선 계산 실패")
                return candidates
            
            ma_value = ma_values[1]  # 전일 이동평균값
            
            # 수정된 조건: 전일 종가가 이평선 위에 있고 직전 정상 매도가보다 높으면 즉시 재매수
            above_ma = prev_close > ma_value
            higher_than_last_sell = prev_close > last_normal_sell_price
            
            if above_ma and higher_than_last_sell:
                msg = f"{stock_name}({stock_code}) - 정상 매도 후 재매수 조건 충족"
                msg += f"\n- 전일 종가({prev_close:,}원)가 {ma_period}{period_unit}선({ma_value:,}원) 위에 있고,"
                msg += f"\n- 전일 종가({prev_close:,}원)가 직전 정상 매도가({last_normal_sell_price:,}원)보다 높음"
                self.logger.info(msg)
                
                # 매수 금액 계산 (현금 * 배분비율)
                buy_amount = cash * allocation_ratio
                
                # 매수 수량 계산 (매수금액 / 현재가)
                buy_quantity = int(buy_amount / current_price)
                
                if buy_quantity > 0:
                    candidates.append({
                        'code': stock_code,
                        'name': stock_name,
                        'quantity': buy_quantity,
                        'price': current_price,
                        'amount': buy_quantity * current_price,
                        'allocation_ratio': allocation_ratio,
                        'ma_period': ma_period,
                        'ma_value': ma_value,
                        'prev_close': prev_close,
                        'ma_condition': "정상매도후재매수",
                        'type': stock_type
                    })
                    self.logger.info(f"{stock_name}({stock_code}) - 정상 매도 후 재매수 후보에 추가됨 (매수조건 충족 여부와 관계없이)")
                else:
                    self.logger.info(f"{stock_name}({stock_code}) - 추가 매수 수량이 0 또는 음수")
                
                # 다른 매수 조건 확인 스킵
                return candidates
        
        # 현재가 조회
        price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
        if not price_data:
            self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
            return candidates
        
        current_price = float(price_data['output']['stck_prpr'])
        prev_close = float(price_data['output']['stck_sdpr'])
        
        # 매수 조건 체크
        ma_period = int(row['매수기준'])
        ma_condition = row.get('매수조건', '종가')  # 기본값은 '종가'
        
        # 매수기준2에 따른 일/주 구분 - 명확하게 처리
        period_div_code_raw = row.get('매수기준2', '일')
        period_div_code = "D" if period_div_code_raw == "일" else "W"
        period_unit = "일" if period_div_code == "D" else "주"
        
        is_buy, ma_value = self.check_buy_condition(stock_code, ma_period, prev_close, ma_condition, period_div_code)
        
        if is_buy:
            buy_msg = f"{stock_name}({stock_code}) - 매수 조건 충족"
            if ma_condition == "종가":
                buy_msg += f": 전일 종가({prev_close:,.0f}원)가 {ma_period}{period_unit}선({ma_value:,.0f}원)을 상향돌파"
            else:
                buy_msg += f": {ma_condition}{period_unit}선이 {ma_period}{period_unit}선을 골든크로스"
            self.logger.info(buy_msg)
            
            # 매수 금액 계산 (현금 * 배분비율)
            buy_amount = cash * allocation_ratio
            
            # 매수 수량 계산 (매수금액 / 현재가)
            buy_quantity = int(buy_amount / current_price)
            
            if buy_quantity > 0:
                candidates.append({
                    'code': stock_code,
                    'name': stock_name,
                    'quantity': buy_quantity,
                    'price': current_price,
                    'amount': buy_quantity * current_price,
                    'allocation_ratio': allocation_ratio,
                    'ma_period': ma_period,
                    'ma_value': ma_value,
                    'prev_close': prev_close,
                    'ma_condition': ma_condition,
                    'type': stock_type
                })
            else:
                self.logger.info(f"{stock_name}({stock_code}) - 추가 매수 수량이 0 또는 음수")
        else:
            if ma_value:
                miss_msg = f"{stock_name}({stock_code}) - 매수 조건 미충족"
                if ma_condition == "종가":
                    miss_msg += f": 전일 종가({prev_close:,.0f}원)가 {ma_period}{period_unit}선({ma_value:,.0f}원)을 상향돌파하지 않음"
                else:
                    miss_msg += f": {ma_condition}{period_unit}선이 {ma_period}{period_unit}선을 골든크로스하지 않음"
                self.logger.info(miss_msg)
            else:
                self.logger.info(f"{stock_name}({stock_code}) - 이동평균 계산 실패")
                
        return candidates
    
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
            
            self.logger.info(f"[국내 시장] 보유 종목 수: {len(balance['output1'])}개")
            
            # 보유 종목 매도 조건 체크
            sell_candidates = []
            
            # 구글 스프레드시트에 있는 종목 코드 리스트 생성
            sheet_stock_codes = set()
            # 개별 종목에서 종목 코드 추가
            for _, row in self.individual_stocks.iterrows():
                sheet_stock_codes.add(row['종목코드'])
            # POOL 종목에서 종목 코드 추가
            for _, row in self.pool_stocks.iterrows():
                sheet_stock_codes.add(row['종목코드'])
            
            # 각 종목별로 매도 조건 체크
            for holding in balance['output1']:
                # 보유량이 0이면 스킵
                if int(holding.get('hldg_qty', 0)) <= 0:
                    continue
                
                stock_code = holding['pdno']
                stock_name = holding['prdt_name']
                quantity = int(holding['hldg_qty'])
                
                # 구글 스프레드시트에서 삭제된 종목 체크
                if stock_code not in sheet_stock_codes:
                    # 현재가 조회
                    price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                    if not price_data:
                        self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
                        continue
                    
                    current_price = float(price_data['output']['stck_prpr'])
                    
                    self.logger.info(f"{stock_name}({stock_code}) - 구글 스프레드시트에서 삭제된 종목이므로 매도 후보에 추가합니다.")
                    
                    sell_candidates.append({
                        'code': stock_code,
                        'name': stock_name,
                        'quantity': quantity,
                        'price': current_price,
                        'ma_period': 0,
                        'ma_value': 0,
                        'prev_close': 0,
                        'ma_condition': "삭제됨",
                        'period_div_code': ""
                    })
                    continue
                
                # 종목이 개별 종목인지 POOL 종목인지 확인
                is_individual = False
                ma_period = 20  # 기본값
                ma_condition = "종가"  # 기본값
                period_div_code = "D"  # 기본값
                
                # 개별 종목에서 찾기
                individual_match = self.individual_stocks[self.individual_stocks['종목코드'] == stock_code]
                if not individual_match.empty:
                    is_individual = True
                    ma_period = int(individual_match.iloc[0]['매도기준'])
                    ma_condition = individual_match.iloc[0].get('매도조건', '종가')
                    period_div_code = individual_match.iloc[0].get('매도기준2', '일')  # 일/주 결정
                    period_div_code = "D" if period_div_code == "일" else "W"  # 일/주 변환
                else:
                    # POOL 종목에서 찾기
                    pool_match = self.pool_stocks[self.pool_stocks['종목코드'] == stock_code]
                    if not pool_match.empty:
                        ma_period = int(pool_match.iloc[0]['매도기준'])
                        ma_condition = pool_match.iloc[0].get('매도조건', '종가')
                        period_div_code = pool_match.iloc[0].get('매도기준2', '주')  # 일/주 결정
                        period_div_code = "D" if period_div_code == "일" else "W"  # 일/주 변환
                    else:
                        # 기준을 찾을 수 없는 경우 (기본값 사용)
                        self.logger.warning(f"{stock_name}({stock_code}) - 매도 기준을 찾을 수 없어 기본값 사용: {ma_period}일선, 전일 종가")
                
                # 현재가 조회
                price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                if not price_data:
                    self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
                    continue
                
                current_price = float(price_data['output']['stck_prpr'])
                prev_close = float(price_data['output']['stck_sdpr'])
                
                # 매도 조건 체크
                is_sell, ma_value = self.check_sell_condition(stock_code, ma_period, prev_close, ma_condition, period_div_code)
                
                if is_sell:
                    sell_msg = f"{stock_name}({stock_code}) - 매도 조건 충족"
                    if period_div_code == "D":
                        period_unit = "일선"
                    else:
                        period_unit = "주선"
                        
                    if ma_condition == "종가":
                        sell_msg += f": 전일 종가({prev_close:,.0f}원)가 {ma_period}{period_unit}({ma_value:,.0f}원) 아래로 하향돌파"
                    else:
                        sell_msg += f": {ma_condition}{period_unit}이 {ma_period}{period_unit}을 데드크로스"
                    
                    self.logger.info(sell_msg)
                    
                    sell_candidates.append({
                        'code': stock_code,
                        'name': stock_name,
                        'quantity': quantity,
                        'price': current_price,
                        'ma_period': ma_period,
                        'ma_value': ma_value,
                        'prev_close': prev_close,
                        'ma_condition': ma_condition,
                        'period_div_code': period_div_code
                    })
                else:
                    if ma_value:
                        if period_div_code == "D":
                            period_unit = "일선"
                        else:
                            period_unit = "주선"
                            
                        miss_msg = f"{stock_name}({stock_code}) - 매도 조건 미충족"
                        if ma_condition == "종가":
                            miss_msg += f": 전일 종가({prev_close:,.0f}원)가 {ma_period}{period_unit}({ma_value:,.0f}원) 아래로 하향돌파하지 않음"
                        else:
                            miss_msg += f": {ma_condition}{period_unit}이 {ma_period}{period_unit}을 데드크로스하지 않음"
                        
                        self.logger.info(miss_msg)
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 이동평균 계산 실패")
            
            # 매도 후보가 없으면 종료
            if not sell_candidates:
                self.logger.info("매도 조건을 충족하는 종목이 없습니다.")
                return
            
            self.logger.info(f"[국내 시장] 매도 후보 종목 수: {len(sell_candidates)}개")
            
            # 매도 실행
            for candidate in sell_candidates:
                stock_code = candidate['code']
                stock_name = candidate['name']
                quantity = candidate['quantity']
                price = candidate['price']
                ma_period = candidate['ma_period']
                ma_value = candidate['ma_value'] 
                prev_close = candidate['prev_close']
                ma_condition = candidate['ma_condition']
                period_div_code = candidate['period_div_code']
                
                # 매도 주문 실행
                self.logger.info(f"{stock_name}({stock_code}) - 매도 주문: {quantity}주 @ {price:,.0f}원")
                
                order_result = self._retry_api_call(
                    self.kr_api.order_stock,
                    stock_code,
                    "SELL",
                    quantity
                )
                
                if order_result and order_result['rt_cd'] == '0':
                    self.logger.info(f"{stock_name}({stock_code}) - 매도 주문 성공: 주문번호 {order_result['output']['ODNO']}")
                    
                    # 거래 내역 저장
                    reason = ""
                    if ma_condition == "삭제됨":
                        reason = "구글 스프레드시트에서 종목이 삭제됨"
                    else:
                        if period_div_code == "D":
                            period_unit = "일선"
                        else:
                            period_unit = "주선"
                            
                        if ma_condition == "종가":
                            reason = f"{ma_period}{period_unit} 매도 조건 충족 (전일 종가 {prev_close:,.0f}원 < MA {ma_value:,.0f}원)"
                        else:
                            reason = f"{ma_condition}{period_unit}과 {ma_period}{period_unit}의 데드크로스 발생"
                    
                    trade_data = {
                        "trade_type": "USER" if ma_condition == "삭제됨" else "SELL",
                        "trade_action": "SELL",
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "quantity": quantity,
                        "price": price,
                        "total_amount": quantity * price,
                        "ma_period": ma_period,
                        "ma_value": ma_value,
                        "reason": reason,
                        "order_type": "SELL"
                    }
                    self.trade_history.add_trade(trade_data)
                    
                    # 매도 상세 정보 로깅
                    msg = f"매도 주문 실행: {stock_name}({stock_code}) {quantity}주"
                    if ma_condition == "삭제됨":
                        msg += f"\n- 매도 사유: 구글 스프레드시트에서 종목이 삭제됨"
                    else:
                        msg += f"\n- 매도 사유: 이동평균 하향돌파 (전일종가: {prev_close:,.0f}원 < {ma_period}일선: {ma_value:,.0f}원)"
                    msg += f"\n- 매도 금액: {quantity * price:,.0f}원"
                    self.logger.info(msg)
                    
                    # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                    self.sold_stocks_cache_time = 0
                else:
                    self.logger.error(f"{stock_name}({stock_code}) - 매도 주문 실패")
            
        except Exception as e:
            self.logger.error(f"매도 주문 실행 중 오류 발생: {str(e)}")
            raise
    
    def _check_stop_conditions(self) -> None:
        """스탑로스 및 트레일링 스탑 조건을 확인합니다."""
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
                    
                    # 잔고 재조회
                    new_balance = self.kr_api.get_account_balance()
                    total_balance = float(new_balance['output2'][0]['tot_evlu_amt'])
                    d2_deposit = float(new_balance['output2'][0]['dnca_tot_amt'])
                    
                    msg = f"스탑로스 매도 실행: {name} {quantity}주"
                    msg += f"\n- 매도 사유: 손실률 {loss_pct:.2f}% (스탑로스 {self.settings['stop_loss']}% 도달)"
                    msg += f"\n- 매도 금액: {current_price * quantity:,.0f}원 (현재가 {current_price:,.0f}원)"
                    msg += f"\n- 매수 정보: 매수단가 {entry_price:,.0f}원 / 평가손익 {(current_price - entry_price) * quantity:,.0f}원"
                    msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원"
                    self.logger.info(msg)
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
                        msg += f"\n- 현재 수익률: +{profit_pct:.3f}% (목표가 {self.settings['trailing_start']}% 초과)"
                        msg += f"\n- 고점 대비 상승: +{price_change_pct:.3f}% (이전 고점 {highest_price:,}원 → 현재가 {current_price:,}원)"
                        msg += f"\n- 트레일링 스탑: 현재가 기준 {abs(self.settings['trailing_stop']):.3f}% 하락 시 매도"
                        self.logger.info(msg)
                
                # 현재가를 새로운 최고가로 사용
                highest_price = current_price
                
                # 최고가 캐시 업데이트 (당일 최고가 유지를 위해)
                current_date = datetime.now().strftime("%Y-%m-%d")
                if self.highest_price_cache_date != current_date:
                    self.highest_price_cache = {}
                    self.highest_price_cache_date = current_date
                
                self.highest_price_cache[stock_code] = current_price
                self.logger.debug(f"최고가 캐시 업데이트: {stock_code}, {current_price:,.0f}원")
            else:
                # 목표가(trailing_start) 초과 여부 확인
                profit_pct = (highest_price - entry_price) / entry_price * 100
                if profit_pct >= self.settings['trailing_start']:  # 목표가 초과 시에만 트레일링 스탑 체크
                    drop_pct = (current_price - highest_price) / highest_price * 100
                    
                    # 1% 이상 하락 시 메시지 출력
                    if drop_pct <= -1.0:
                        msg = f"고점 대비 하락 - {name}({stock_code})"
                        msg += f"\n- 현재 수익률: +{((current_price - entry_price) / entry_price * 100):.3f}%"
                        msg += f"\n- 고점 대비 하락: {drop_pct:.3f}% (고점 {highest_price:,}원 → 현재가 {current_price:,}원)"
                        msg += f"\n- 트레일링 스탑: {(self.settings['trailing_stop'] - drop_pct):.3f}% 더 하락하면 매도"
                        self.logger.info(msg)
                    
                    if drop_pct <= self.settings['trailing_stop']:
                        trade_msg = f"트레일링 스탑 조건 성립 - {name}({stock_code}): 고점대비 하락률 {drop_pct:.3f}% <= {self.settings['trailing_stop']}%"
                        self.logger.info(trade_msg)
                        
                        # 트레일링 스탑 매도
                        result = self._retry_api_call(self.kr_api.order_stock, stock_code, "SELL", quantity)
                        if result:
                            # 거래 내역 저장
                            trade_data = {
                                "trade_type": "TRAILING_STOP",
                                "trade_action": "SELL",
                                "stock_code": stock_code,
                                "stock_name": name,
                                "quantity": quantity,
                                "price": current_price,
                                "total_amount": quantity * current_price,
                                "reason": f"트레일링 스탑 조건 충족 (고점 {highest_price:,.0f}원 대비 하락률 {drop_pct:.3f}% <= {self.settings['trailing_stop']}%)",
                                "profit_loss": (current_price - entry_price) * quantity,
                                "profit_loss_pct": (current_price - entry_price) / entry_price * 100
                            }
                            self.trade_history.add_trade(trade_data)
                            
                            # 잔고 재조회
                            new_balance = self.kr_api.get_account_balance()
                            total_balance = float(new_balance['output2'][0]['tot_evlu_amt'])
                            d2_deposit = float(new_balance['output2'][0]['dnca_tot_amt'])
                            
                            msg = f"트레일링 스탑 매도 실행: {name} {quantity}주"
                            msg += f"\n- 매도 사유: 고점 대비 하락률 {drop_pct:.3f}% (트레일링 스탑 {self.settings['trailing_stop']}% 도달)"
                            msg += f"\n- 매도 금액: {current_price * quantity:,.0f}원 (현재가 {current_price:,.0f}원)"
                            msg += f"\n- 매수 정보: 매수단가 {entry_price:,.0f}원 / 평가손익 {(current_price - entry_price) * quantity:,.0f}원"
                            msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원"
                            self.logger.info(msg)
                            # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                            self.sold_stocks_cache_time = 0
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"스탑 조건 체크 중 오류 발생 ({stock_code}): {str(e)}")
            return False 

    def get_today_sold_stocks(self) -> List[str]:
        """API를 통해 당일 매도한 종목 코드 목록을 조회합니다.
        
        Returns:
            List[str]: 당일 매도한 종목 코드 목록
        """
        sold_stocks = []
        try:
            # 당일 체결 내역 조회
            executed_orders = self._retry_api_call(self.kr_api.get_today_executed_orders)
            
            if executed_orders and 'output1' in executed_orders:
                for order in executed_orders['output1']:
                    # 매도 주문만 필터링 (01: 매도)
                    if order['sll_buy_dvsn_cd'] == '01':
                        stock_code = order['pdno']
                        # 체결 수량이 있는 경우만 추가
                        if int(order['tot_ccld_qty']) > 0:
                            if stock_code not in sold_stocks:
                                sold_stocks.append(stock_code)
                                self.logger.debug(f"당일 매도 종목 확인: {order['prdt_name']}({stock_code})")
            
            return sold_stocks
        except Exception as e:
            self.logger.error(f"당일 매도 종목 조회 중 오류 발생: {str(e)}")
            return []  # 오류 발생 시 빈 리스트 반환
        
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
                    # 현재가
                    current_price = round(float(current_price_data['output']['stck_prpr']), 2)
                    
                    holdings_data.append([
                        stock_code,                                           # 종목코드
                        holding['prdt_name'],                                # 종목명
                        current_price,                                       # 현재가
                        '',                                                  # 구분
                        round(float(current_price_data['output']['prdy_ctrt']), 2),   # 등락률
                        round(float(holding['pchs_avg_pric']), 2),                    # 평단가
                        round(float(holding['evlu_pfls_rt']), 2),                     # 수익률
                        int(holding['hldg_qty']),                           # 보유량
                        round(float(holding['evlu_pfls_amt']), 2),                    # 평가손익
                        round(float(holding['pchs_amt']), 2),                         # 매입금액
                        round(float(holding['evlu_amt']), 2)                          # 평가금액
                    ])
            
            # 주식현황 시트 업데이트
            holdings_sheet = self._get_holdings_sheet()
            self._update_holdings_sheet(holdings_data, holdings_sheet)
            
            # 요약 정보 계산
            output2 = balance.get('output2', {})
            
            # 매입금액합계금액 - 보유 종목의 매입금액 합계
            total_purchase_amount = float(output2[0].get('pchs_amt_smtl_amt', 0))
            
            # 평가금액합계금액 - 보유 종목의 평가금액 합계
            total_eval_amount = float(output2[0].get('evlu_amt_smtl_amt', 0))
            
            # 총평가손익금액 - 보유 종목의 평가손익 합계
            total_eval_profit_loss = float(output2[0].get('evlu_pfls_smtl_amt', 0))
            
            # 총자산금액 - 예수금 + 평가금액
            total_asset_amount = float(output2[0].get('tot_evlu_amt', 0))
            
            # 총수익률 계산
            total_profit_rate = 0
            if total_purchase_amount > 0:
                total_profit_rate = round((total_eval_profit_loss / total_purchase_amount) * 100, 2)
            
            # 요약 정보 반올림
            total_purchase_amount = round(total_purchase_amount, 2)
            total_eval_amount = round(total_eval_amount, 2)
            total_eval_profit_loss = round(total_eval_profit_loss, 2)
            total_asset_amount = round(total_asset_amount, 2)
            
            # 평가손익금액은 F6, 수익률은 G6에 출력
            self.google_sheet.update_range(f"{holdings_sheet}!F6", [[total_eval_profit_loss]])
            self.google_sheet.update_range(f"{holdings_sheet}!G6", [[total_profit_rate]])
            
            # 나머지 정보는 K5:K7에 출력
            summary_data = [
                [total_purchase_amount],  # 매입금액합계금액
                [total_eval_amount],      # 평가금액합계금액
                [total_asset_amount]      # 총자산금액
            ]
            
            summary_range = f"{holdings_sheet}!K5:K7"
            self.google_sheet.update_range(summary_range, summary_data)
            
        except Exception as e:
            self.logger.error(f"국내 주식 현황 업데이트 실패: {str(e)}")
            raise
        
    def _get_holdings_sheet(self) -> str:
        """주식현황 시트 이름을 반환합니다."""
        return self.google_sheet.sheets['holdings_kr']  # 주식현황[KOR]
        
    def _update_holdings_sheet(self, holdings_data: list, holdings_sheet: str) -> None:
        """주식현황 시트를 업데이트합니다."""
        try:
            # 마지막 업데이트 시간 갱신
            now = datetime.now()
            update_time = now.strftime("%Y-%m-%d %H:%M:%S")
            self.google_sheet.update_last_update_time(update_time, holdings_sheet)
            
            # 에러 메시지 초기화
            self.google_sheet.update_error_message("", holdings_sheet)
            
            # 보유 종목 리스트 업데이트 (기존 데이터 초기화 후 새로운 데이터 추가)
            self.logger.info(f"국내 주식 현황 데이터 초기화 및 업데이트 시작 (총 {len(holdings_data)}개 종목)", send_discord=False)
            self.google_sheet.update_holdings(holdings_data, holdings_sheet)
            
            self.logger.info(f"국내 주식 현황 업데이트 완료 ({update_time})", send_discord=False)
            
        except Exception as e:
            error_msg = f"주식현황 시트 업데이트 실패: {str(e)}"
            self.logger.error(error_msg)
            self.google_sheet.update_error_message(error_msg, holdings_sheet)
            raise 

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
            if latest_trade.get("trade_type") != "TRAILING_STOP" or latest_trade.get("trade_action") != "SELL":
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
            if latest_trade.get("trade_type") == "SELL" and latest_trade.get("trade_action") == "SELL":
                return float(latest_trade.get("price", 0))
            
            return None
            
        except Exception as e:
            self.logger.error(f"정상 매도 가격 조회 중 오류 발생 ({stock_code}): {str(e)}")
            return None

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
            end_date = datetime.now().strftime("%Y%m%d")
            sell_date_obj = datetime.strptime(ts_sell_date, "%Y-%m-%d")
            start_date = sell_date_obj.strftime("%Y%m%d")
            
            df = self.kr_api.get_daily_price(stock_code, start_date, end_date, period_div_code)
            if df is None or len(df) < 2:  # 매도일 포함해서 최소 2일 이상 필요
                self.logger.error(f"{stock_code} - TS 매도 이후 시세 데이터 조회 실패")
                return False
            
            # 날짜 기준으로 정렬 (오래된 순)
            if 'stck_bsop_date' in df.columns:
                df = df.sort_values('stck_bsop_date', ascending=True)
            
            # TS 매도일 이후 데이터만 필터링 (매도일 제외)
            sell_date_formatted = sell_date_obj.strftime("%Y%m%d")
            df_after_sell = df[df['stck_bsop_date'] > sell_date_formatted]
            
            if len(df_after_sell) == 0:
                self.logger.info(f"{stock_code} - TS 매도 이후 시세 데이터가 없습니다")
                return False
            
            # 각 날짜에 대해 이동평균 계산 및 종가와 비교
            for i, row_data in df_after_sell.iterrows():
                date = row_data['stck_bsop_date']
                close = float(row_data['stck_clpr'])
                
                # 해당 날짜의 이동평균 계산
                end_date_for_ma = date
                days_for_ma = ma_period * 2  # 충분한 데이터 확보
                start_date_for_ma = (datetime.strptime(end_date_for_ma, "%Y%m%d") - timedelta(days=days_for_ma)).strftime("%Y%m%d")
                
                df_for_ma = self.kr_api.get_daily_price(stock_code, start_date_for_ma, end_date_for_ma, period_div_code)
                if df_for_ma is None or len(df_for_ma) < ma_period:
                    continue
                
                # 날짜 정렬
                if 'stck_bsop_date' in df_for_ma.columns:
                    df_for_ma = df_for_ma.sort_values('stck_bsop_date', ascending=True)
                
                # 이동평균 계산
                ma_series = df_for_ma['stck_clpr'].astype(float).rolling(window=ma_period).mean()
                if len(ma_series) < 1 or pd.isna(ma_series.iloc[-1]):
                    continue
                
                ma_value = ma_series.iloc[-1]
                
                # 종가가 이동평균보다 낮으면 이탈 확인
                if close < ma_value:
                    self.logger.info(f"{stock_code} - {date} 종가({close:,.0f}원)가 {ma_period}일선({ma_value:,.0f}원) 아래로 이탈 확인")
                    return True
            
            # 이탈 이력 없음
            self.logger.info(f"{stock_code} - TS 매도 이후 이평선 이탈 이력 없음")
            return False
            
        except Exception as e:
            self.logger.error(f"{stock_code} - 이평선 이탈 확인 중 오류 발생: {str(e)}")
            return False
