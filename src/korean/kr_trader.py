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
        super().__init__(config_path, "KOR")
        self.kis_api = KISKRAPIManager(config_path)
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
            self.settings = self.google_sheet.get_settings()
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
    
    def _is_market_open_time(self) -> bool:
        """시가 매수 시점인지 확인합니다."""
        now = datetime.now()
        current_time = now.strftime("%H%M")
        start_time = self.config['trading']['kor_market_start']
        
        # 장 시작 후 5분 이내
        return start_time <= current_time <= str(int(start_time) + 5).zfill(4)
    
    def _is_market_close_time(self) -> bool:
        """종가 매수 시점인지 확인합니다."""
        now = datetime.now()
        current_time = now.strftime("%H%M")
        end_time = self.config['trading']['kor_market_end']
        
        # 장 마감 15분 전 ~ 5분 전
        close_start = str(int(end_time) - 15).zfill(4)  # 15:15
        close_end = str(int(end_time) - 5).zfill(4)     # 15:25
        return close_start <= current_time <= close_end
    
    def calculate_ma(self, stock_code: str, period: int = 20) -> Optional[float]:
        """이동평균을 계산합니다."""
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=period*2)).strftime("%Y%m%d")
            
            df = self.kis_api.get_daily_price(stock_code, start_date, end_date)
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
    
    def execute_trade(self):
        """매매를 실행합니다."""
        try:
            now = datetime.now()
            current_date = now.date()
            
            # 날짜가 바뀌면 플래그 초기화
            if self.execution_date != current_date:
                self.execution_date = current_date
                self.market_open_executed = False
                self.market_close_executed = False
                self.is_first_execution = True
                self._clear_daily_orders()
                self.logger.info("새로운 거래일이 시작되었습니다.")
            
            # 장 운영 시간이 아니면 스킵
            if not self.check_market_condition():
                return
            
            # 장 시작 매매 조건
            is_market_open = (self._is_market_open_time() and not self.market_open_executed) or self.is_first_execution
            # 종가 매수 조건
            is_market_close = self._is_market_close_time() and not self.market_close_executed
            
            # 매매 시점이 아니면서 첫 실행도 아닌 경우, 스탑로스/트레일링 스탑만 체크
            if not (is_market_open or is_market_close) and not self.is_first_execution:
                self._check_stop_conditions()
                return
            
            # 계좌 잔고 조회
            balance = self.kis_api.get_account_balance()
            if balance is None:
                self.logger.error("계좌 잔고 조회 실패")
                return
            
            # 개별 종목 매매 처리
            self._process_trades(balance, is_market_open, is_market_close)
            
            # 실행 플래그 설정
            if is_market_open:
                self.market_open_executed = True
                self.is_first_execution = False
                self.logger.info("장 시작 매매 실행 완료")
            elif is_market_close:
                self.market_close_executed = True
                self.logger.info("종가 매매 실행 완료")
                self._clear_daily_orders()
            
            # 주문 목록 저장
            self.save_daily_orders()
            
        except Exception as e:
            error_msg = f"매매 실행 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            self.send_discord_message(error_msg, error=True)
    
    def _process_trades(self, balance: Dict, is_market_open: bool, is_market_close: bool):
        """개별 종목 매매를 처리합니다."""
        # 개별 종목 매매
        for _, row in self.individual_stocks.iterrows():
            if len(row['종목코드']) <= 6:  # 한국 주식만 처리
                self._process_single_stock(row, balance, is_market_open, is_market_close)
        
        # POOL 종목 매매
        for _, row in self.pool_stocks.iterrows():
            if len(row['종목코드']) <= 6:  # 한국 주식만 처리
                self._process_single_stock(row, balance, is_market_open, is_market_close)
    
    def _process_single_stock(self, row: pd.Series, balance: Dict, is_market_open: bool, is_market_close: bool):
        """단일 종목의 매매를 처리합니다."""
        try:
            stock_code = row['종목코드']
            ma_period = int(row['매매기준']) if row['매매기준'] and str(row['매매기준']).strip() != '' else 20
            allocation_ratio = float(row['배분비율']) / 100 if row['배분비율'] and str(row['배분비율']).strip() != '' else 0.1
            
            # 현재가 조회 (재시도 로직 적용)
            current_price_data = self._retry_api_call(self.kis_api.get_stock_price, stock_code)
            if current_price_data is None:
                return
            
            current_price = float(current_price_data['output']['stck_prpr'])
            prev_close = float(current_price_data['output']['stck_sdpr'])
            
            # 보유 종목 확인
            holdings = [h for h in balance['output1'] if h['pdno'] == stock_code]
            is_holding = len(holdings) > 0
            
            # 최대 보유 종목 수 체크 (개별 종목과 POOL 종목 각각 체크)
            total_individual_holdings = len([h for h in balance['output1'] if int(h.get('hldg_qty', 0)) > 0 and any(s['종목코드'] == h['pdno'] for _, s in self.individual_stocks.iterrows())])
            total_pool_holdings = len([h for h in balance['output1'] if int(h.get('hldg_qty', 0)) > 0 and any(s['종목코드'] == h['pdno'] for _, s in self.pool_stocks.iterrows())])
            
            # 현재 종목이 개별 종목인지 POOL 종목인지 확인
            is_individual = any(s['종목코드'] == stock_code for _, s in self.individual_stocks.iterrows())
            max_stocks = self.settings['max_individual_stocks'] if is_individual else self.settings['max_pool_stocks']
            current_holdings = total_individual_holdings if is_individual else total_pool_holdings
            
            if is_holding:
                holding = holdings[0]
                quantity = int(holding['hldg_qty'])
                
                # 스탑로스/트레일링 스탑 체크
                if self._check_stop_conditions_for_stock(holding, current_price):
                    return
                
                # 장 시작 시점에만 매도 조건 체크
                if is_market_open:
                    should_sell, ma = self.check_sell_condition(stock_code, ma_period, prev_close)
                    if should_sell and ma is not None:
                        trade_msg = f"매도 조건 성립 - {row['종목명']}({stock_code}): 전일 종가 {prev_close:,}원 < {ma_period}일 이동평균 [{ma:,.0f}원]"
                        self.logger.info(trade_msg)
                        
                        result = self._retry_api_call(self.kis_api.order_stock, stock_code, "SELL", quantity)
                        if result:
                            # 잔고 재조회
                            new_balance = self.kis_api.get_account_balance()
                            total_balance = float(new_balance['output2'][0]['tot_evlu_amt'])
                            d2_deposit = float(new_balance['output2'][0]['dnca_tot_amt'])
                            
                            msg = f"매도 주문 실행: {row['종목명']} {quantity}주 (시장가)"
                            msg += f"\n- 매도 사유: 이동평균 하향돌파"
                            msg += f"\n- 매도 금액: {current_price * quantity:,.0f}원 (현재가 {current_price:,}원)"
                            msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원 / D+2예수금 {d2_deposit:,.0f}원"
                            self.logger.info(msg)
                            self.add_daily_sold_stock(stock_code)  # 당일 매도 종목에 추가
            
            # 매수 조건 체크
            elif is_market_open:
                should_buy, ma = self.check_buy_condition(stock_code, ma_period, prev_close)
                if should_buy and ma is not None:
                    # 당일 매도 종목 체크
                    if self.is_sold_today(stock_code):
                        msg = f"매수 조건 불충족 - {row['종목명']}({stock_code}): 당일 매도 종목 재매수 제한"
                        self.logger.info(msg)
                        return

                    trade_msg = f"매수 조건 성립 - {row['종목명']}({stock_code}): 전일 종가 {prev_close:,}원 > {ma_period}일 이동평균 [{ma:,.0f}원]"
                    self.logger.info(trade_msg)
                    
                    # 최대 보유 종목 수 초과 체크
                    if current_holdings >= max_stocks:
                        msg = f"매수 조건 불충족 - {row['종목명']}({stock_code}): 최대 보유 종목 수({max_stocks}개) 초과"
                        self.logger.info(msg)
                        return
                        
                    if not any(order['stock_code'] == stock_code for order in self.pending_close_orders):
                        buyable_data = self._retry_api_call(self.kis_api.get_buyable_amount, stock_code)
                        if buyable_data is None:
                            return
                        
                        # 현금 보유 비율 체크
                        available_cash = float(buyable_data['output']['nrcvb_buy_amt'])
                        total_assets = float(balance['output2'][0]['tot_evlu_amt'])  # 총 평가금액
                        min_cash = total_assets * self.settings['min_cash_ratio']
                        
                        if available_cash <= min_cash:
                            msg = f"매수 조건 불충족 - {row['종목명']}({stock_code}): 최소 현금 보유 비율({self.settings['min_cash_ratio']*100}%) 유지 필요"
                            msg += f"\n- 필요 현금: {min_cash:,.0f}원"
                            msg += f"\n- 가용 현금: {available_cash:,.0f}원"
                            self.logger.info(msg)
                            return
                        
                        # 매수 가능 금액 계산
                        buy_amount = total_assets * allocation_ratio
                        total_quantity = int(buy_amount / current_price)
                        
                        if total_quantity <= 0:
                            msg = f"매수 조건 불충족 - {row['종목명']}({stock_code}): 매수 자금 부족"
                            msg += f"\n- 필요자금: {current_price:,.0f}원/주"
                            msg += f"\n[시가 매수] 가용자금: {buy_amount*self.settings['market_open_ratio']:,.0f}원"
                            msg += f"\n[종가 매수] 가용자금: {buy_amount*self.settings['market_close_ratio']:,.0f}원"
                            self.logger.info(msg)
                            return
                        
                        if total_quantity > 0:
                            # 시장가 매수 (설정된 비율만큼)
                            market_quantity = int(total_quantity * self.settings['market_open_ratio'])
                            if market_quantity > 0:
                                result = self._retry_api_call(self.kis_api.order_stock, stock_code, "BUY", market_quantity)
                                if result:
                                    # 잔고 재조회
                                    new_balance = self.kis_api.get_account_balance()
                                    total_balance = float(new_balance['output2'][0]['tot_evlu_amt'])
                                    d2_deposit = float(new_balance['output2'][0]['dnca_tot_amt'])
                                    
                                    msg = f"매수 주문 실행: {row['종목명']} {market_quantity}주 (시장가)"
                                    msg += f"\n- 매수 사유: 이동평균 상향돌파 (배분비율: {allocation_ratio*100}%)"
                                    msg += f"\n- 매수 금액: {current_price * market_quantity:,.0f}원 (현재가 {current_price:,}원)"
                                    msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원 / D+2예수금 {d2_deposit:,.0f}원"
                                    self.logger.info(msg)
                                    # 종가 매수를 위한 정보 저장
                                    self.pending_close_orders.append({
                                        'stock_code': stock_code,
                                        'stock_name': row['종목명'],
                                        'quantity': total_quantity - market_quantity,
                                        'allocation_ratio': allocation_ratio
                                    })
                                    self.save_daily_orders()
                else:
                    msg = f"매수 조건 불충족 - {row['종목명']}({stock_code}): 전일 종가 {prev_close:,}원 <= {ma_period}일 이동평균 [{ma:,.0f}원]"
                    self.logger.info(msg)
            
            # 종가 매수 처리
            elif is_market_close:
                for order in self.pending_close_orders[:]:
                    if order['stock_code'] == stock_code:
                        # 현재 보유 수량 확인
                        holdings = [h for h in balance['output1'] if h['pdno'] == stock_code]
                        if not holdings or int(holdings[0].get('hldg_qty', 0)) <= 0:
                            msg = f"종가 매수 취소 - {order['stock_name']}({stock_code}): 보유 수량 없음"
                            self.logger.info(msg)
                            self.pending_close_orders.remove(order)
                            self.save_daily_orders()
                            return
                        
                        result = self._retry_api_call(self.kis_api.order_stock, stock_code, "BUY", order['quantity'], current_price)
                        if result:
                            # 잔고 재조회
                            new_balance = self.kis_api.get_account_balance()
                            total_balance = float(new_balance['output2'][0]['tot_evlu_amt'])
                            d2_deposit = float(new_balance['output2'][0]['dnca_tot_amt'])
                            
                            msg = f"종가 매수 주문 실행: {order['stock_name']} {order['quantity']}주 (지정가: {current_price:,}원)"
                            msg += f"\n- 매수 사유: 이동평균 상향돌파 잔여수량"
                            msg += f"\n- 매수 금액: {current_price * order['quantity']:,.0f}원"
                            msg += f"\n- 계좌 상태: 총평가금액 {total_balance:,.0f}원 / D+2예수금 {d2_deposit:,.0f}원"
                            self.logger.info(msg)
                        self.pending_close_orders.remove(order)
                        self.save_daily_orders()
            
        except Exception as e:
            self.logger.error(f"개별 종목 매매 처리 중 오류 발생: {str(e)}")
    
    def _check_stop_conditions(self):
        """스탑로스와 트레일링 스탑 조건을 체크합니다."""
        try:
            balance = self.kis_api.get_account_balance()
            if balance is None:
                return
            
            for holding in balance['output1']:
                stock_code = holding['pdno']
                current_price_data = self.kis_api.get_stock_price(stock_code)
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
                result = self._retry_api_call(self.kis_api.order_stock, stock_code, "SELL", quantity)
                if result:
                    # 잔고 재조회
                    new_balance = self.kis_api.get_account_balance()
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
                        
                        result = self._retry_api_call(self.kis_api.order_stock, stock_code, "SELL", quantity)
                        if result:
                            # 잔고 재조회
                            new_balance = self.kis_api.get_account_balance()
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