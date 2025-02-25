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
        
        # 실전/모의투자에 따른 API 호출 간격 설정
        self.is_paper_trading = self.config['api']['is_paper_trading']
        self.api_call_interval = 0.5 if self.is_paper_trading else 0.3  # 모의투자: 0.5초, 실전투자: 0.3초
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
        """API 호출을 재시도합니다.
        
        Args:
            func: 호출할 API 함수
            *args: API 함수에 전달할 위치 인자
            **kwargs: API 함수에 전달할 키워드 인자
            
        Returns:
            API 호출 결과
        """
        for attempt in range(self.max_retries):
            try:
                self._wait_for_api_call()  # API 호출 전 대기
                result = func(*args, **kwargs)
                
                # API 응답 확인
                if isinstance(result, dict) and result.get('rt_cd') == '1':
                    error_msg = result.get('msg1', '알 수 없는 오류가 발생했습니다.')
                    if 'EGW00201' in str(result):  # 초당 거래건수 초과 오류
                        time.sleep(self.api_call_interval * 2)  # 추가 대기 시간
                        continue
                    raise Exception(error_msg)
                    
                return result
                
            except Exception as e:
                if attempt == self.max_retries - 1:  # 마지막 시도
                    raise Exception(f"API 호출 실패 (최대 재시도 횟수 초과): {str(e)}")
                time.sleep(self.api_call_interval * (attempt + 1))  # 점진적 대기 시간 증가
    
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
        """주식 현황을 구글 스프레드시트에 업데이트합니다. 하위 클래스에서 구현해야 합니다."""
        raise NotImplementedError("This method should be implemented by subclass")
    
    def _update_holdings_sheet(self, holdings_data: list, holdings_sheet: str) -> None:
        """주식 현황 시트를 업데이트합니다.
        
        Args:
            holdings_data (list): 보유 종목 데이터 리스트
            holdings_sheet (str): 주식현황 시트명
        """
        try:
            # 현재 시간 업데이트
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 시트 업데이트
            self.google_sheet.update_last_update_time(now, holdings_sheet)
            self.google_sheet.update_error_message("", holdings_sheet)
            
            # 보유 주식 리스트 초기화
            self.google_sheet.update_holdings([[]], holdings_sheet)
            
            # 보유 주식 리스트 업데이트
            if holdings_data:
                self.google_sheet.update_holdings(holdings_data, holdings_sheet)
            
            self.logger.info("주식 현황 업데이트 완료")
            
        except Exception as e:
            error_msg = f"주식 현황 업데이트 실패: {str(e)}"
            self.logger.error(error_msg)
            self.google_sheet.update_error_message(error_msg, holdings_sheet)
            raise
    
    def _get_holdings_sheet(self) -> str:
        """거래소별 주식현황 시트를 반환합니다."""
        if self.market_type == "KOR":
            return self.config['google_sheet']['sheets']['holdings_kr']  # 주식현황[KOR]
        elif self.market_type == "USA":
            return self.config['google_sheet']['sheets']['holdings_us']  # 주식현황[USA]
        else:
            raise ValueError(f"지원하지 않는 시장 유형입니다: {self.market_type}")
    
    def check_market_condition(self) -> bool:
        """시장 상태를 체크합니다. 하위 클래스에서 구현해야 합니다."""
        raise NotImplementedError("This method should be implemented by subclass")