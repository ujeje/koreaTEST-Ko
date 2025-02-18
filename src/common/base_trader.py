import logging
import yaml
import os
import json
import time
from datetime import datetime
from typing import Dict, Optional
from src.utils.google_sheet_manager import GoogleSheetManager
from discord_webhook import DiscordWebhook
from src.utils.logger import setup_logger

class BaseTrader:
    """모든 트레이더의 기본이 되는 클래스입니다."""
    
    def __init__(self, config_path: str, market_type: str):
        """
        Args:
            config_path (str): 설정 파일 경로
            market_type (str): 시장 유형 (KOR/USA)
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
            
        self.market_type = market_type
        self.google_sheet = GoogleSheetManager(config_path)
        self.settings = None
        self.individual_stocks = None
        self.pool_stocks = None
        self.discord_webhook_url = self.config['discord']['webhook_url']
        self.is_first_execution = True
        self.pending_close_orders = []
        self.market_open_executed = False
        self.market_close_executed = False
        self.execution_date = None
        self.last_api_call = 0
        self.api_call_interval = 0.2
        self.max_retries = 3
        self.daily_orders_file = f'data/daily_orders_{market_type.lower()}.json'
        self.daily_sold_stocks_file = f'data/daily_sold_stocks_{market_type.lower()}.json'
        self.daily_sold_stocks = []
        
        # 디렉토리 생성
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        # 로거 설정
        self.logger = setup_logger(market_type, self.config)
        
        # 초기화
        self.load_settings()
        self.load_daily_orders()
        self.load_daily_sold_stocks()
    
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
                    logging.warning(f"API 호출 제한 도달. {wait_time}초 대기 후 재시도 ({attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)
                    continue
                raise
        return None
    
    def load_settings(self) -> None:
        """구글 스프레드시트에서 설정을 로드합니다."""
        try:
            self.settings = self.google_sheet.get_settings()
            self.individual_stocks = self.google_sheet.get_individual_stocks()
            self.pool_stocks = self.google_sheet.get_pool_stocks()
            logging.info(f"{self.market_type} 설정을 성공적으로 로드했습니다.")
        except Exception as e:
            logging.error(f"{self.market_type} 설정 로드 실패: {str(e)}")
            raise
    
    def load_daily_orders(self):
        """당일 매수 종목 정보를 로드합니다."""
        try:
            if os.path.exists(self.daily_orders_file):
                with open(self.daily_orders_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data.get('date') == datetime.now().strftime('%Y-%m-%d'):
                        self.pending_close_orders = data.get('orders', [])
                    else:
                        self._clear_daily_orders()
        except Exception as e:
            logging.error(f"당일 매수 종목 로드 실패: {str(e)}")
            self._clear_daily_orders()

    def save_daily_orders(self):
        """당일 매수 종목 정보를 저장합니다."""
        try:
            data = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'orders': self.pending_close_orders
            }
            with open(self.daily_orders_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"당일 매수 종목 저장 실패: {str(e)}")

    def _clear_daily_orders(self):
        """당일 매수 종목 정보를 초기화합니다."""
        self.pending_close_orders = []
        if os.path.exists(self.daily_orders_file):
            try:
                os.remove(self.daily_orders_file)
            except Exception as e:
                logging.error(f"당일 매수 종목 파일 삭제 실패: {str(e)}")

    def load_daily_sold_stocks(self):
        """당일 매도 종목 정보를 로드합니다."""
        try:
            if os.path.exists(self.daily_sold_stocks_file):
                with open(self.daily_sold_stocks_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data.get('date') == datetime.now().strftime('%Y-%m-%d'):
                        self.daily_sold_stocks = data.get('stocks', [])
                    else:
                        self._clear_daily_sold_stocks()
        except Exception as e:
            logging.error(f"당일 매도 종목 로드 실패: {str(e)}")
            self._clear_daily_sold_stocks()

    def save_daily_sold_stocks(self):
        """당일 매도 종목 정보를 저장합니다."""
        try:
            data = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'stocks': self.daily_sold_stocks
            }
            with open(self.daily_sold_stocks_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"당일 매도 종목 저장 실패: {str(e)}")

    def _clear_daily_sold_stocks(self):
        """당일 매도 종목 정보를 초기화합니다."""
        self.daily_sold_stocks = []
        if os.path.exists(self.daily_sold_stocks_file):
            try:
                os.remove(self.daily_sold_stocks_file)
            except Exception as e:
                logging.error(f"당일 매도 종목 파일 삭제 실패: {str(e)}")

    def add_daily_sold_stock(self, stock_code: str):
        """당일 매도 종목을 추가합니다."""
        if stock_code not in self.daily_sold_stocks:
            self.daily_sold_stocks.append(stock_code)
            self.save_daily_sold_stocks()
            logging.info(f"당일 매도 종목에 추가됨: {stock_code}")

    def is_sold_today(self, stock_code: str) -> bool:
        """해당 종목이 당일 매도되었는지 확인합니다."""
        return stock_code in self.daily_sold_stocks

    def check_buy_condition(self, stock_code: str, ma_period: int, prev_close: float) -> tuple[bool, Optional[float]]:
        """매수 조건을 체크합니다. (전일 종가가 이동평균선 위에 있는지)"""
        ma = self.calculate_ma(stock_code, ma_period)
        if ma is None:
            return False, None
        return bool(prev_close > ma), ma
    
    def check_sell_condition(self, stock_code: str, ma_period: int, prev_close: float) -> tuple[bool, Optional[float]]:
        """매도 조건을 체크합니다. (전일 종가가 이동평균선 아래에 있는지)"""
        ma = self.calculate_ma(stock_code, ma_period)
        if ma is None:
            return False, None
        return bool(prev_close < ma), ma
    
    def execute_trade(self) -> None:
        """매매를 실행합니다. 하위 클래스에서 구현해야 합니다."""
        raise NotImplementedError("This method should be implemented by subclass")
    
    def update_stock_report(self) -> None:
        """주식 현황을 구글 스프레드시트에 업데이트합니다."""
        try:
            # 계좌 잔고 조회
            if hasattr(self, 'kis_api'):
                api = self.kis_api
            elif hasattr(self, 'api'):
                api = self.api
            else:
                raise Exception("API 객체를 찾을 수 없습니다.")
            
            balance = api.get_account_balance()
            if balance is None:
                raise Exception("계좌 잔고 조회 실패")
            
            # 현재 시간 업데이트
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.google_sheet.update_last_update_time(now)
            self.google_sheet.update_error_message("")
            
            # 보유 주식 리스트 초기화
            self.google_sheet.update_holdings([[]])
            
            # 보유 주식 리스트 업데이트
            holdings_data = []
            for holding in balance['output1']:
                if int(holding.get('hldg_qty', 0)) <= 0:
                    continue
                    
                stock_code = holding['pdno']
                current_price_data = self._retry_api_call(api.get_stock_price, stock_code)
                
                if current_price_data:
                    holdings_data.append([
                        stock_code,                                                # 종목코드
                        holding['prdt_name'],                                     # 종목명
                        float(current_price_data['output']['stck_prpr']),        # 현재가
                        '',                                                       # 구분
                        float(current_price_data['output']['prdy_ctrt']),        # 등락률
                        float(holding['pchs_avg_pric']),                         # 평단가
                        float(holding['evlu_pfls_rt']),                          # 수익률
                        int(holding['hldg_qty']),                                # 보유량
                        float(holding['evlu_pfls_amt']),                         # 평가손익
                        float(holding['pchs_amt']),                              # 매입금액
                        float(holding['evlu_amt'])                               # 평가금액
                    ])
            
            if holdings_data:
                self.google_sheet.update_holdings(holdings_data)
            
            logging.info("주식 현황 업데이트 완료")
            
        except Exception as e:
            error_msg = f"주식 현황 업데이트 실패: {str(e)}"
            logging.error(error_msg)
            self.google_sheet.update_error_message(error_msg)
            raise
    
    def check_market_condition(self) -> bool:
        """시장 상태를 체크합니다. 하위 클래스에서 구현해야 합니다."""
        raise NotImplementedError("This method should be implemented by subclass")
    
    def _check_stop_conditions_for_stock(self, holding: Dict, current_price: float) -> bool:
        """개별 종목의 스탑로스와 트레일링 스탑 조건을 체크합니다."""
        try:
            stock_code = holding.get('pdno', holding.get('ovrs_pdno', ''))
            
            # 매수 평균가 처리
            pchs_avg_pric = holding.get('pchs_avg_pric', '')
            if not pchs_avg_pric or str(pchs_avg_pric).strip() == '':
                logging.warning(f"매수 평균가가 비어있습니다: {stock_code}")
                return False
            entry_price = float(pchs_avg_pric)
            
            # 보유 수량 처리
            hldg_qty = holding.get('hldg_qty', holding.get('ord_psbl_qty', ''))
            if not hldg_qty or str(hldg_qty).strip() == '':
                logging.warning(f"보유 수량이 비어있습니다: {stock_code}")
                return False
            quantity = int(hldg_qty)
            
            name = holding.get('prdt_name', stock_code)
            
            # 보유 수량이 없는 경우는 정상적인 상황이므로 조용히 리턴
            if quantity <= 0:
                return False
            
            # 매수 평균가가 유효하지 않은 경우에만 경고
            if entry_price <= 0:
                logging.warning(f"매수 평균가({entry_price})가 유효하지 않습니다: {name}")
                return False
            
            # 스탑로스 체크
            loss_pct = (current_price - entry_price) / entry_price * 100
            if loss_pct <= -self.settings['stop_loss']:
                result = self.kis_api.order_stock(stock_code, "SELL", quantity)
                if result:
                    self.send_discord_message(f"스탑로스 매도 실행: {name} {quantity}주 (손실률: {self.settings['stop_loss']}%)")
                return True
            
            # 트레일링 스탑 체크
            highest_price_str = holding.get('highest_price', '')
            highest_price = float(highest_price_str) if highest_price_str and str(highest_price_str).strip() != '' else entry_price
            
            if highest_price <= 0:
                highest_price = entry_price
            
            if current_price > highest_price:
                holding['highest_price'] = current_price
            else:
                drop_pct = (current_price - highest_price) / highest_price * 100
                if drop_pct <= -self.settings['trailing_stop_loss']:
                    result = self.kis_api.order_stock(stock_code, "SELL", quantity)
                    if result:
                        self.send_discord_message(f"트레일링 스탑 매도 실행: {name} {quantity}주 (하락률: {self.settings['trailing_stop_loss']}%)")
                    return True
            
            return False
            
        except Exception as e:
            logging.error(f"스탑 조건 체크 중 오류 발생: {str(e)}")
            return False 