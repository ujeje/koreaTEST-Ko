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
            
            # 매수/매도 시간 설정 (G8, G9 셀에서 가져옴)
            buy_time = self.google_sheet.get_cell_value("G8", market_type="KOR")
            sell_time = self.google_sheet.get_cell_value("G9", market_type="KOR")
            
            # 시간 형식 확인 및 변환 (HH:MM -> HHMM)
            if buy_time and ":" in buy_time:
                buy_time = buy_time.replace(":", "")
            if sell_time and ":" in sell_time:
                sell_time = sell_time.replace(":", "")
                
            # 시간 형식 정규화 함수
            def normalize_time(time_str):
                if not time_str:
                    return None
                
                # 콜론이 있는 경우 (H:M 형식)
                if ":" in time_str:
                    hour, minute = time_str.split(":")
                    # 시간과 분이 한 자리인 경우 두 자리로 변환
                    hour = hour.zfill(2)
                    minute = minute.zfill(2)
                    return hour + minute
                
                # 숫자만 있는 경우
                if time_str.isdigit():
                    # 3자리 이하인 경우 (예: 130 -> 0130)
                    if len(time_str) <= 3:
                        return time_str.zfill(4)
                    return time_str
                
                return None
            
            # 시간 형식 정규화
            normalized_buy_time = normalize_time(buy_time)
            normalized_sell_time = normalize_time(sell_time)
            
            # 유효한 시간 형식인지 확인하고 설정
            if normalized_buy_time and len(normalized_buy_time) == 4:
                self.settings['buy_time'] = normalized_buy_time
            else:
                self.settings['buy_time'] = '1320'  # 기본값 13:20
                self.logger.warning(f"유효하지 않은 매수 시간 형식: {buy_time}, 기본값 13:20으로 설정")
                
            if normalized_sell_time and len(normalized_sell_time) == 4:
                self.settings['sell_time'] = normalized_sell_time
            else:
                self.settings['sell_time'] = '0930'  # 기본값 09:30
                self.logger.warning(f"유효하지 않은 매도 시간 형식: {sell_time}, 기본값 09:30으로 설정")
            
            self.logger.info(f"{self.market_type} 설정을 성공적으로 로드했습니다.")
            self.logger.info(f"매수 시간: {self.settings['buy_time'][:2]}:{self.settings['buy_time'][2:]}, 매도 시간: {self.settings['sell_time'][:2]}:{self.settings['sell_time'][2:]}")
        except Exception as e:
            self.logger.error(f"{self.market_type} 설정 로드 실패: {str(e)}")
            raise
    
    def check_market_condition(self) -> bool:
        """한국 시장 상태를 체크합니다."""
        now = datetime.now()
        current_time = now.strftime("%H%M")
        
        # 장 운영 시간 체크 (09:00 ~ 15:30)
        return self.config['trading']['kor_market_start'] <= current_time <= self.config['trading']['kor_market_end']
    
    def calculate_ma(self, stock_code: str, period: int = 20) -> Optional[float]:
        """이동평균을 계산합니다."""
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            # 이동평균 계산을 위해 필요한 데이터 기간 (기본 2배로 설정)
            required_days = period * 2
            start_date = (datetime.now() - timedelta(days=required_days)).strftime("%Y%m%d")
            
            # API 제한(100일)을 고려하여 데이터 조회
            all_data = []
            current_end_date = datetime.now()
            start_datetime = datetime.now() - timedelta(days=required_days)
            
            # 필요한 기간이 100일 이하인 경우 한 번에 조회
            if required_days <= 100:
                df = self.kr_api.get_daily_price(stock_code, start_date, end_date)
                if df is None or len(df) < period:
                    return None
                    
                ma = df['stck_clpr'].rolling(window=period).mean().iloc[-2]  # 전일 종가의 이동평균값
                return ma
            
            # 필요한 기간이 100일 초과인 경우 분할 조회
            while current_end_date >= start_datetime:
                # 현재 조회 기간의 시작일 계산 (최대 100일)
                current_start_date = current_end_date - timedelta(days=99)
                
                # 시작일보다 이전으로 가지 않도록 조정
                if current_start_date < start_datetime:
                    current_start_date = start_datetime
                
                # 날짜 형식 변환
                current_start_date_str = current_start_date.strftime("%Y%m%d")
                current_end_date_str = current_end_date.strftime("%Y%m%d")
                
                self.logger.debug(f"이동평균 계산을 위한 일별 시세 조회: {stock_code}, {current_start_date_str} ~ {current_end_date_str}")
                
                # API 호출하여 데이터 조회
                df = self.kr_api.get_daily_price(stock_code, current_start_date_str, current_end_date_str)
                if df is not None and len(df) > 0:
                    all_data.append(df)
                
                # 다음 조회 기간 설정 (하루 겹치지 않게)
                current_end_date = current_start_date - timedelta(days=1)
                
                # 시작일에 도달하면 종료
                if current_end_date < start_datetime:
                    break
            
            # 조회된 데이터가 없는 경우
            if not all_data:
                return None
            
            # 모든 데이터 합치기
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 중복 제거 (날짜 기준)
            if 'stck_bsop_date' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['stck_bsop_date'])
                combined_df = combined_df.sort_values('stck_bsop_date', ascending=True).reset_index(drop=True)
            
            # 데이터가 충분한지 확인
            if len(combined_df) < period:
                self.logger.warning(f"{stock_code}: 이동평균 계산을 위한 데이터가 부족합니다. (필요: {period}일, 실제: {len(combined_df)}일)")
                return None
            
            # 이동평균 계산
            ma = combined_df['stck_clpr'].astype(float).rolling(window=period).mean().iloc[-2]  # 전일 종가의 이동평균값
            return ma
        except Exception as e:
            self.logger.error(f"{period}일 이동평균 계산 실패 ({stock_code}): {str(e)}")
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
                df = self.kr_api.get_daily_price(stock_code, start_date_str, end_date_str)
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
    
    def check_buy_condition(self, stock_code: str, ma_period: int, prev_close: float) -> tuple[bool, Optional[float]]:
        """매수 조건을 확인합니다."""
        try:
            # 5일과 지정된 기간의 이동평균선 계산
            ma5 = self.calculate_ma(stock_code, 5)
            ma_target = self.calculate_ma(stock_code, ma_period)
            
            if ma5 is None or ma_target is None:
                return False, None
                
            # 전일 데이터 조회
            end_date = datetime.now(self.kr_timezone).strftime("%Y%m%d")
            start_date = (datetime.now(self.kr_timezone) - timedelta(days=2)).strftime("%Y%m%d")
            
            df = self.kr_api.get_daily_price(stock_code, start_date, end_date)
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
                self.logger.info(f"- 전전일: 5일선(₩{ma5_prev2:.2f}) < {ma_period}일선(₩{ma_target_prev2:.2f})")
                self.logger.info(f"- 전일: 5일선(₩{ma5_prev:.2f}) > {ma_period}일선(₩{ma_target_prev:.2f})")
            
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
            
            # 구글 시트에서 리밸런싱 비율 가져오기
            rebalancing_ratio = float(self.settings.get('rebalancing_ratio', 0))
            if not rebalancing_ratio:
                self.logger.error("리밸런싱 비율이 설정되지 않았습니다.")
                return
            
            # self.logger.info(f"리밸런싱 비율: {rebalancing_ratio * 100:.0f}%")
            
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
                        # 리밸런싱 비율 적용 (rebalancing_ratio가 1일 때 100%를 의미)
                        # 배분비율과 리밸런싱 비율을 곱하여 최종 목표 비율 계산
                        adjusted_target_ratio = target_ratio * rebalancing_ratio
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
            
            # 매수/매도 시간 확인 (구글 스프레드시트에서 가져온 설정 사용)
            buy_time = self.settings.get('buy_time', '1320')
            sell_time = self.settings.get('sell_time', '0930')
            
            # 1. 스탑로스/트레일링 스탑 체크 (매 루프마다 실행)
            self._check_stop_conditions()
            
            # 2. 매도 시간에 매도 실행 (아직 실행되지 않은 경우, 지정 시간부터 10분간만 허용)
            sell_end_time = f"{int(sell_time) + 10:04d}" if int(sell_time) % 100 < 50 else f"{int(sell_time) + 50:04d}"
            if current_time >= sell_time and current_time <= sell_end_time and not self.market_close_executed:
                self.logger.info(f"매도 시간 도달: {sell_time[:2]}:{sell_time[2:]}")
                self._execute_sell_orders()
                self.market_close_executed = True
                
                # 매도 후 리밸런싱 체크 및 실행 (미국 시장과 동일하게 매도 후 리밸런싱 실행)
                if self._is_rebalancing_day():
                    self.logger.info("리밸런싱 실행 날짜 조건 충족")
                    
                    # 계좌 잔고 조회
                    balance = self._retry_api_call(self.kr_api.get_account_balance)
                    if balance is not None:
                        self._rebalance_portfolio(balance)
                    else:
                        self.logger.error("계좌 잔고 조회 실패로 리밸런싱을 실행할 수 없습니다.")
            
            # 3. 매수 시간에 매수 실행 (아직 실행되지 않은 경우, 지정 시간부터 10분간만 허용)
            buy_end_time = f"{int(buy_time) + 10:04d}" if int(buy_time) % 100 < 50 else f"{int(buy_time) + 50:04d}"
            if current_time >= buy_time and current_time <= buy_end_time and not self.market_open_executed:
                self.logger.info(f"매수 시간 도달: {buy_time[:2]}:{buy_time[2:]}")
                self._execute_buy_orders()
                self.market_open_executed = True
                
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
                    self.logger.info(f"{stock_name}({stock_code}) - 당일 매도 종목 재매수 제한")
                    continue
                
                # 트레일링 스탑으로 매도된 종목 체크
                trailing_stop_price = self.get_trailing_stop_sell_price(stock_code)
                if trailing_stop_price is not None:
                    # 현재가 조회
                    price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                    if not price_data:
                        self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
                        continue
                    
                    current_price = float(price_data['output']['stck_prpr'])
                    
                    if current_price < trailing_stop_price:
                        msg = f"{stock_name}({stock_code}) - 트레일링 스탑 매도 종목 재매수 제한"
                        msg += f"\n- 현재가({current_price:,}원)가 트레일링 스탑 매도가({trailing_stop_price:,}원) 미만"
                        self.logger.info(msg)
                        continue
                    else:
                        msg = f"{stock_name}({stock_code}) - 트레일링 스탑 매도 종목 재매수 가능"
                        msg += f"\n- 현재가({current_price:,}원)가 트레일링 스탑 매도가({trailing_stop_price:,}원) 이상"
                        self.logger.info(msg)
                
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
                    self.logger.info(f"{stock_name}({stock_code}) - 매수 조건 충족: 5일선이 {ma_period}일선을 상향돌파")
                    
                    # 매수 금액 계산 (현금 * 배분비율)
                    buy_amount = cash * allocation_ratio
                    
                    # 매수 수량 계산 (매수금액 / 현재가)
                    buy_quantity = int(buy_amount / current_price)
                    
                    if buy_quantity > 0:
                        individual_candidates.append({
                            'code': stock_code,
                            'name': stock_name,
                            'quantity': buy_quantity,
                            'price': current_price,
                            'amount': buy_quantity * current_price,
                            'allocation_ratio': allocation_ratio,
                            'ma_period': ma_period,
                            'ma_value': ma_value,
                            'prev_close': prev_close,
                            'type': 'individual'
                        })
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 추가 매수 수량이 0 또는 음수")
                else:
                    if ma_value:
                        self.logger.info(f"{stock_name}({stock_code}) - 매수 조건 미충족: 골든크로스 미발생")
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
                    self.logger.info(f"{stock_name}({stock_code}) - 당일 매도 종목 재매수 제한")
                    continue
                
                # 트레일링 스탑으로 매도된 종목 체크
                trailing_stop_price = self.get_trailing_stop_sell_price(stock_code)
                if trailing_stop_price is not None:
                    # 현재가 조회
                    price_data = self._retry_api_call(self.kr_api.get_stock_price, stock_code)
                    if not price_data:
                        self.logger.error(f"{stock_name}({stock_code}) - 현재가 조회 실패")
                        continue
                    
                    current_price = float(price_data['output']['stck_prpr'])
                    
                    if current_price < trailing_stop_price:
                        msg = f"{stock_name}({stock_code}) - 트레일링 스탑 매도 종목 재매수 제한"
                        msg += f"\n- 현재가({current_price:,}원)가 트레일링 스탑 매도가({trailing_stop_price:,}원) 미만"
                        self.logger.info(msg)
                        continue
                    else:
                        msg = f"{stock_name}({stock_code}) - 트레일링 스탑 매도 종목 재매수 가능"
                        msg += f"\n- 현재가({current_price:,}원)가 트레일링 스탑 매도가({trailing_stop_price:,}원) 이상"
                        self.logger.info(msg)
                
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
                    self.logger.info(f"{stock_name}({stock_code}) - 매수 조건 충족: 5일선이 {ma_period}일선을 상향돌파")
                    
                    # 매수 금액 계산 (현금 * 배분비율)
                    buy_amount = cash * allocation_ratio
                    
                    # 매수 수량 계산 (매수금액 / 현재가)
                    buy_quantity = int(buy_amount / current_price)
                    
                    if buy_quantity > 0:
                        pool_candidates.append({
                            'code': stock_code,
                            'name': stock_name,
                            'quantity': buy_quantity,
                            'price': current_price,
                            'amount': buy_quantity * current_price,
                            'allocation_ratio': allocation_ratio,
                            'ma_period': ma_period,
                            'ma_value': ma_value,
                            'prev_close': prev_close,
                            'type': 'pool'
                        })
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 추가 매수 수량이 0 또는 음수")
                else:
                    if ma_value:
                        self.logger.info(f"{stock_name}({stock_code}) - 매수 조건 미충족: 골든크로스 미발생")
                    else:
                        self.logger.info(f"{stock_name}({stock_code}) - 이동평균 계산 실패")
            
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
                        "reason": f"{ma_period}일선 매수 조건 충족 (현재가 {price:,.0f}원 > MA {ma_value:,.0f}원)"
                    }
                    self.trade_history.add_trade(trade_data)
                    
                    # 매수 상세 정보 로깅
                    msg = f"매수 주문 실행: {stock_name}({stock_code}) {quantity}주"
                    msg += f"\n- 매수 사유: 이동평균 상향돌파 (전일종가: {prev_close:,.0f}원 > {ma_period}일선: {ma_value:,.0f}원)"
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
            
            for holding in balance['output1']:
                stock_code = holding['pdno']
                stock_name = holding['prdt_name']
                quantity = int(holding['hldg_qty'])
                current_price = float(holding['prpr'])
                
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
                    
                    sell_candidates.append({
                        'code': stock_code,
                        'name': stock_name,
                        'quantity': quantity,
                        'price': current_price,
                        'ma_period': ma_period,
                        'ma_value': ma_value,
                        'prev_close': prev_close
                    })
                else:
                    if ma_value:
                        self.logger.info(f"{stock_name}({stock_code}) - 매도 조건 미충족 (전일종가: {prev_close:,.0f}, {ma_period}일선: {ma_value:,.0f})")
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
                    trade_data = {
                        "trade_type": "SELL",
                        "trade_action": "SELL",
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "quantity": quantity,
                        "price": price,
                        "total_amount": quantity * price,
                        "ma_period": ma_period,
                        "ma_value": ma_value,
                        "reason": f"{ma_period}일선 매도 조건 충족 (현재가 {price:,.0f}원 < MA {ma_value:,.0f}원)",
                        "profit_loss": (price - prev_close) * quantity,
                        "profit_loss_pct": (price - prev_close) / prev_close * 100
                    }
                    self.trade_history.add_trade(trade_data)
                    
                    # 매도 상세 정보 로깅
                    msg = f"매도 주문 실행: {stock_name}({stock_code}) {quantity}주"
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
                    msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원 / D+2예수금 {d2_deposit:,.0f}원"
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
                        msg += f"\n- 현재 수익률: +{profit_pct:.1f}% (목표가 {self.settings['trailing_start']}% 초과)"
                        msg += f"\n- 고점 대비 상승: +{price_change_pct:.1f}% (이전 고점 {highest_price:,}원 → 현재가 {current_price:,}원)"
                        msg += f"\n- 트레일링 스탑: 현재가 기준 {abs(self.settings['trailing_stop']):.1f}% 하락 시 매도"
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
                        msg += f"\n- 현재 수익률: +{((current_price - entry_price) / entry_price * 100):.1f}%"
                        msg += f"\n- 고점 대비 하락: {drop_pct:.1f}% (고점 {highest_price:,}원 → 현재가 {current_price:,}원)"
                        msg += f"\n- 트레일링 스탑: {abs(self.settings['trailing_stop'] - drop_pct):.1f}% 더 하락하면 매도"
                        self.logger.info(msg)
                    
                    if drop_pct <= self.settings['trailing_stop']:
                        trade_msg = f"트레일링 스탑 조건 성립 - {name}({stock_code}): 고점대비 하락률 {drop_pct:.2f}% <= {self.settings['trailing_stop']}%"
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
                                "reason": f"트레일링 스탑 조건 충족 (고점 {highest_price:,.0f}원 대비 하락률 {drop_pct:.2f}% <= {self.settings['trailing_stop']}%)",
                                "profit_loss": (current_price - entry_price) * quantity,
                                "profit_loss_pct": (current_price - entry_price) / entry_price * 100
                            }
                            self.trade_history.add_trade(trade_data)
                            
                            # 잔고 재조회
                            new_balance = self.kr_api.get_account_balance()
                            total_balance = float(new_balance['output2'][0]['tot_evlu_amt'])
                            d2_deposit = float(new_balance['output2'][0]['dnca_tot_amt'])
                            
                            msg = f"트레일링 스탑 매도 실행: {name} {quantity}주"
                            msg += f"\n- 매도 사유: 고점 대비 하락률 {drop_pct:.2f}% (트레일링 스탑 {self.settings['trailing_stop']}% 도달)"
                            msg += f"\n- 매도 금액: {current_price * quantity:,.0f}원 (현재가 {current_price:,.0f}원)"
                            msg += f"\n- 매수 정보: 매수단가 {entry_price:,.0f}원 / 평가손익 {(current_price - entry_price) * quantity:,.0f}원"
                            msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원 / D+2예수금 {d2_deposit:,.0f}원"
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
            # 오류 발생 시 파일에 저장된 정보 반환
            return super().get_today_sold_stocks()
        
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
                        round(float(current_price_data['output']['stck_prpr']), 2),   # 현재가
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
            self.logger.info("국내 주식 요약 정보 업데이트 완료")
            
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
            self.logger.info(f"국내 주식 현황 데이터 초기화 및 업데이트 시작 (총 {len(holdings_data)}개 종목)")
            self.google_sheet.update_holdings(holdings_data, holdings_sheet)
            
            self.logger.info(f"국내 주식 현황 업데이트 완료 ({update_time})")
            
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