import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import pytz
from src.common.base_trader import BaseTrader
from src.overseas.kis_us_api import KISUSAPIManager
import time

class USTrader(BaseTrader):
    """미국 주식 트레이더"""
    
    def __init__(self, config_path: str):
        """
        Args:
            config_path (str): 설정 파일 경로
        """
        super().__init__(config_path, "USA")
        self.us_api = KISUSAPIManager(config_path)
        self.load_settings()
        self.us_timezone = pytz.timezone(self.config['trading']['usa_timezone'])
        self.last_api_call = 0
        self.api_call_interval = 1.0  # API 호출 간격 (초)
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
        current_time = datetime.now(self.us_timezone).strftime('%H%M')
        start_time = self.config['trading']['usa_market_start']
        
        # 장 시작 후 5분 이내
        return start_time <= current_time <= str(int(start_time) + 15).zfill(4)
        
    def _is_market_close_time(self) -> bool:
        """종가 매수 시점인지 확인합니다."""
        current_time = datetime.now(self.us_timezone).strftime('%H%M')
        end_time = self.config['trading']['usa_market_end']
        
        return True
        
        # 장 마감 15분 전부터
        close_start = str(int(end_time) - 15).zfill(4)
        return close_start <= current_time
        
    def calculate_ma(self, stock_code: str, period: int = 20) -> Optional[float]:
        """이동평균선을 계산합니다."""
        try:
            end_date = datetime.now(self.us_timezone).strftime('%Y%m%d')
            start_date = (datetime.now(self.us_timezone) - timedelta(days=period * 2)).strftime('%Y%m%d')
            
            df = self.us_api.get_daily_price(stock_code, start_date, end_date)
            if df is not None and not df.empty:
                df['clos'] = pd.to_numeric(df['clos'], errors='coerce')
                ma = df['clos'].rolling(window=period).mean().iloc[-2]  # 전일자 ?? 이동평균
                return float(ma) if not np.isnan(ma) else None
            return None
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
        
    def get_today_sold_stocks(self) -> List[str]:
        """API를 통해 당일 매도한 종목 코드 목록을 조회합니다.
        
        Returns:
            List[str]: 당일 매도한 종목 코드 목록
        """
        try:
            # 당일 체결 내역 조회
            executed_orders = self._retry_api_call(self.us_api.get_today_executed_orders)
            
            if executed_orders and 'output' in executed_orders:
                sold_stocks = []
                for order in executed_orders['output']:
                    # 매도 주문만 필터링 (01: 매도)
                    if order['sll_buy_dvsn_cd'] == '01':
                        stock_code = order['pdno'].split('.')[0]  # 거래소 코드 제거
                        # 체결 수량이 있는 경우만 추가
                        if int(order['ft_ccld_qty']) > 0:
                            if stock_code not in sold_stocks:
                                sold_stocks.append(stock_code)
                                self.logger.debug(f"당일 매도 종목 확인: {order['prdt_name']}({stock_code})")
            
            return sold_stocks
        except Exception as e:
            self.logger.error(f"당일 매도 종목 조회 중 오류 발생: {str(e)}")
            # 오류 발생 시 빈 리스트 반환
            return []
        
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
            #current_time = now.strftime("%H%M")
            
            # 리밸런싱 시간이 아니면 False 반환 (09:00 ~ 09:10)
            # if not ("0900" <= current_time <= "0910"):
            #     return False
            
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
            
            self.logger.info("포트폴리오 리밸런싱이 완료되었습니다.")
            
        except Exception as e:
            self.logger.error(f"포트폴리오 리밸런싱 중 오류 발생: {str(e)}")

    def execute_trade(self):
        """매매를 실행합니다."""
        try:
            now = datetime.now(self.us_timezone)
            current_date = now.date()
            
            # 날짜가 바뀌면 플래그 초기화
            if self.execution_date != current_date:
                self.execution_date = current_date
                self.market_open_executed = False
                self.market_close_executed = False
                self.sold_stocks_cache = []  # 당일 매도 종목 캐시 초기화
                self.sold_stocks_cache_time = 0  # 캐시 시간 초기화
                self.logger.info("새로운 거래일이 시작되었습니다.")
            
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
        """매도 조건을 체크하고 실행합니다."""
        try:
            # 보유 종목 확인
            for holding in balance['output1']:
                if int(holding.get('ord_psbl_qty', 0)) <= 0:
                    continue
                    
                # 거래소와 종목코드 결합
                exchange = holding.get('ovrs_excg_cd', '')  # NASD, NYSE, AMEX                
                stock_code = f"{holding['ovrs_pdno']}.{exchange}"
                name = holding.get('prdt_name', stock_code)
                quantity = int(holding['ord_psbl_qty'])
                
                # 현재가 조회
                current_price_data = self._retry_api_call(self.us_api.get_stock_price, stock_code)
                if current_price_data is None:
                    continue
                
                current_price = float(current_price_data['output']['last'])
                prev_close = float(current_price_data['output']['base'])
                
                # 종목 정보 찾기
                stock_info = None
                ma_period = 20  # 기본값
                
                # 개별 종목에서 찾기
                individual_match = self.individual_stocks[self.individual_stocks['종목코드'] == holding['ovrs_pdno']]
                if not individual_match.empty:
                    stock_info = individual_match.iloc[0]
                else:
                    # POOL 종목에서 찾기
                    pool_match = self.pool_stocks[self.pool_stocks['종목코드'] == holding['ovrs_pdno']]
                    if not pool_match.empty:
                        stock_info = pool_match.iloc[0]
                
                if stock_info is not None:
                    ma_period = int(stock_info['매매기준']) if stock_info['매매기준'] and str(stock_info['매매기준']).strip() != '' else 20
                
                # 매도 조건 체크
                should_sell, ma = self.check_sell_condition(stock_code, ma_period, prev_close)
                if should_sell and ma is not None:
                    trade_msg = f"매도 조건 성립 - {name}({stock_code}): 전일 종가 ${prev_close:.2f} < {ma_period}일 이동평균 [${ma:.2f}]"
                    self.logger.info(trade_msg)
                    
                    # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                    sell_price = current_price * 0.99
                    
                    result = self._retry_api_call(self.us_api.order_stock, stock_code, "SELL", quantity, sell_price)
                    if result:
                        msg = f"매도 주문 실행: {name}({stock_code}) {quantity}주 (지정가: ${current_price:,.2f})"
                        msg += f"\n- 매도 사유: 이동평균 하향돌파"
                        msg += f"\n- 매도 정보: 주문가 ${current_price:,.2f} / 총금액 ${current_price * quantity:,.2f}"
                        msg += f"\n- 이동평균: {ma_period}일선 ${ma:.2f} > 전일종가 ${prev_close:.2f}"
                        msg += f"\n- 매도 수익률: {((current_price - prev_close) / prev_close * 100):.2f}% (매수가 ${prev_close:,.2f})"
                        self.logger.info(msg)
                        self.sold_stocks_cache_time = 0  # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
        
        except Exception as e:
            self.logger.error(f"매도 조건 체크 중 오류 발생: {str(e)}")
    
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
                
                # 최대 보유 종목 수 체크 (개별 종목과 POOL 종목 각각 체크)
                total_individual_holdings = len([h for h in balance['output1'] if int(h.get('ord_psbl_qty', 0)) > 0 and any(s['종목코드'] == h['ovrs_pdno'].split('.')[0] for _, s in self.individual_stocks.iterrows())])
                total_pool_holdings = len([h for h in balance['output1'] if int(h.get('ord_psbl_qty', 0)) > 0 and any(s['종목코드'] == h['ovrs_pdno'].split('.')[0] for _, s in self.pool_stocks.iterrows())])
                
                # 현재 종목이 개별 종목인지 POOL 종목인지 확인
                is_individual = any(s['종목코드'] == stock_code.split('.')[0] for _, s in self.individual_stocks.iterrows())
                max_stocks = self.settings['max_individual_stocks'] if is_individual else self.settings['max_pool_stocks']
                current_holdings = total_individual_holdings if is_individual else total_pool_holdings
                
                should_buy, ma = self.check_buy_condition(stock_code, ma_period, prev_close)
                if should_buy and ma is not None:
                    # 당일 매도 종목 체크
                    if self.is_sold_today(stock_code):
                        msg = f"당일 매도 종목 재매수 제한 - {row['종목명']}({stock_code})"
                        self.logger.info(msg)
                        return

                    trade_msg = f"매수 조건 성립 - {row['종목명']}({stock_code}): 전일 종가 ${prev_close:.2f} > {ma_period}일 이동평균 [${ma:.2f}]"
                    self.logger.info(trade_msg)
                    
                    # 최대 보유 종목 수 초과 체크
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
                        
                        if secured_cash >= cash_to_secure:
                            self.logger.info(f"현금 확보 성공: ${secured_cash:.2f} (필요 금액: ${cash_to_secure:.2f})")
                            self.logger.info(f"매도한 POOL 종목: {', '.join(sold_stocks)}")
                            
                            # 매도 후 잠시 대기 (주문 체결 시간 고려)
                            self._wait_for_api_call()
                            
                            # 주문가능금액 다시 확인
                            buyable_data = self._retry_api_call(self.us_api.get_psbl_amt, stock_code)
                            if buyable_data is None:
                                return
                            available_cash = float(buyable_data['output']['frcr_ord_psbl_amt1'])
                        else:
                            self.logger.info(f"현금 확보 실패: ${secured_cash:.2f} (필요 금액: ${cash_to_secure:.2f})")
                            return
                    
                    if total_quantity > 0:
                        # 시장가 매수 (설정된 비율만큼)
                        market_quantity = int(total_quantity * self.settings['market_open_ratio'])
                        if market_quantity > 0:
                            # 매수 시 지정가의 1% 높게 설정하여 시장가처럼 거래
                            buy_price = current_price * 1.01
                            
                            result = self._retry_api_call(self.us_api.order_stock, stock_code, "BUY", market_quantity, buy_price)
                            if result:
                                msg = f"매수 주문 실행: {row['종목명']}({stock_code}) {market_quantity}주 (지정가: ${current_price:,.2f})"
                                msg += f"\n- 매수 사유: 이동평균 상향돌파"
                                msg += f"\n- 매수 정보: 주문가 ${current_price:,.2f} / 총금액 ${current_price * market_quantity:,.2f}"
                                msg += f"\n- 배분비율: {allocation_ratio*100}% (총자산 ${total_assets:,.2f} 중 ${buy_amount:,.2f})"
                                msg += f"\n- 이동평균: {ma_period}일선 ${ma:.2f} < 전일종가 ${prev_close:.2f}"
                                msg += f"\n- 계좌 상태: 총평가금액 ${total_assets:,.2f}"
                                msg += f"\n- 종가 매수 예정: {total_quantity - market_quantity}주 (전체 목표 수량의 {self.settings['market_close_ratio']*100:.0f}%)"
                                self.logger.info(msg)
            
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
                    self.logger.info(f"종가 매수 불필요: {row['종목명']}({stock_code}) - 이평선 조건 미충족")
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
                
                # 종가 매수 실행
                # 매수 시 지정가의 1% 높게 설정하여 시장가처럼 거래
                buy_price = current_price * 1.01
                
                result = self._retry_api_call(self.us_api.order_stock, stock_code, "BUY", additional_quantity, buy_price)
                if result:
                    msg = f"종가 매수 주문 실행: {row['종목명']}({stock_code}) {additional_quantity}주 (지정가: ${current_price:.2f})"
                    msg += f"\n- 매수 사유: 이동평균 상향돌파 종가 매수 (전체 목표의 {self.settings['market_close_ratio']*100:.0f}%)"
                    msg += f"\n- 기존 매수: {total_bought}주 / 추가 매수: {additional_quantity}주 / 총 목표: {total_target_quantity}주"
                    msg += f"\n- 매수 정보: 매수단가 ${current_price:,.2f} / 총금액 ${current_price * additional_quantity:,.2f}"
                    self.logger.info(msg)
        
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
            # 거래소와 종목코드 결합
            exchange = holding.get('ovrs_excg_cd', '')  # NASD, NYSE, AMEX
            stock_code = f"{holding['ovrs_pdno']}.{exchange}"
            entry_price = float(holding.get('pchs_avg_pric', 0)) if holding.get('pchs_avg_pric') and str(holding.get('pchs_avg_pric')).strip() != '' else 0
            quantity = int(holding.get('ord_psbl_qty', 0)) if holding.get('ord_psbl_qty') and str(holding.get('ord_psbl_qty')).strip() != '' else 0
            name = holding.get('prdt_name', stock_code)
            
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
                # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                sell_price = current_price * 0.99
                
                result = self._retry_api_call(self.us_api.order_stock, stock_code, "SELL", quantity, sell_price)
                if result:
                    msg = f"스탑로스 매도 실행: {name} {quantity}주 (지정가)"
                    msg += f"\n- 매도 사유: 손실률 {loss_pct:.2f}% (스탑로스 {self.settings['stop_loss']}% 도달)"
                    msg += f"\n- 매도 금액: ${current_price * quantity:,.2f} (현재가 ${current_price:,.2f})"
                    msg += f"\n- 매수 정보: 매수단가 ${entry_price:,.2f} / 평가손익 ${(current_price - entry_price) * quantity:,.2f}"
                    msg += f"\n- 계좌 상태: 총평가금액 ${total_assets:,.2f} / D+2예수금 ${d2_deposit:,.2f}"
                    self.logger.info(msg)
                    self.sold_stocks_cache_time = 0  # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
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
                        msg += f"\n- 고점 대비 상승: +{price_change_pct:.1f}% (이전 고점 ${highest_price:,.2f} → 현재가 ${current_price:,.2f})"
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
                        msg += f"\n- 고점 대비 하락: {drop_pct:.1f}% (고점 ${highest_price:,.2f} → 현재가 ${current_price:,.2f})"
                        msg += f"\n- 트레일링 스탑까지: {abs(self.settings['trailing_stop'] - drop_pct):.1f}% 더 하락하면 매도"
                        self.logger.info(msg)
                    
                    if drop_pct <= self.settings['trailing_stop']:
                        # 매도 시 지정가의 1% 낮게 설정하여 시장가처럼 거래
                        sell_price = current_price * 0.99
                        
                        result = self._retry_api_call(self.us_api.order_stock, stock_code, "SELL", quantity, sell_price)
                        if result:
                            msg = f"트레일링 스탑 매도 실행: {name} {quantity}주 (지정가)"
                            msg += f"\n- 매도 사유: 고점 대비 하락률 {drop_pct:.2f}% (트레일링 스탑 {self.settings['trailing_stop']}% 도달)"
                            msg += f"\n- 매도 금액: ${current_price * quantity:,.2f} (현재가 ${current_price:,.2f})"
                            msg += f"\n- 매수 정보: 매수단가 ${entry_price:,.2f} / 평가손익 ${(current_price - entry_price) * quantity:,.2f}"
                            msg += f"\n- 계좌 상태: 총평가금액 ${total_assets:,.2f} / D+2예수금 ${d2_deposit:,.2f}"
                            self.logger.info(msg)
                            self.sold_stocks_cache_time = 0  # 캐시 초기화하여 다음 API 호출 시 최신 정보 조회하도록 함
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"개별 종목 스탑 조건 체크 중 오류 발생: {str(e)}")
            return False 

    def update_stock_report(self) -> None:
        """미국 주식 현황을 구글 스프레드시트에 업데이트합니다."""
        try:
            # 계좌 잔고 조회
            balance = self.us_api.get_account_balance()
            if balance is None:
                raise Exception("계좌 잔고 조회 실패")
            
            # 보유 종목 데이터 생성
            holdings_data = []
            for holding in balance['output1']:
                if int(holding.get('ovrs_cblc_qty', 0)) <= 0:
                    continue
                
                full_stock_code = f"{holding['ovrs_pdno']}.{holding['ovrs_excg_cd']}"
                current_price_data = self._retry_api_call(self.us_api.get_stock_price, full_stock_code)
                
                if current_price_data:
                    holdings_data.append([
                        holding['ovrs_pdno'],                                           # 종목코드
                        holding['ovrs_item_name'],                           # 종목명
                        float(current_price_data['output']['last']),         # 현재가
                        '',                                                  # 구분
                        float(current_price_data['output']['rate']),         # 등락률
                        float(holding['pchs_avg_pric']),                     # 평단가
                        float(holding['evlu_pfls_rt']),                      # 수익률
                        int(holding['ovrs_cblc_qty']),                       # 보유량
                        float(holding['frcr_evlu_pfls_amt']),               # 평가손익
                        float(holding['frcr_pchs_amt1']),                   # 매입금액
                        float(holding['ovrs_stck_evlu_amt'])                # 평가금액
                    ])
            
            # 주식현황 시트 업데이트
            holdings_sheet = self._get_holdings_sheet()
            self._update_holdings_sheet(holdings_data, holdings_sheet)
            
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