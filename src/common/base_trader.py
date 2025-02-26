import logging
import yaml
import os
import time
from datetime import datetime
from typing import Dict, Optional, List
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
        self.market_open_executed = False
        self.market_close_executed = False
        self.execution_date = None
        self.last_api_call = 0
        
        # 실전/모의투자에 따른 API 호출 간격 설정
        self.is_paper_trading = self.config['api']['is_paper_trading']
        self.api_call_interval = 0.5 if self.is_paper_trading else 0.3  # 모의투자: 0.5초, 실전투자: 0.3초
        self.max_retries = 3
        
        # 당일 매도 종목 캐시 관련 설정
        self.sold_stocks_cache = []
        self.sold_stocks_cache_time = 0  # 매도 종목 캐시 시간 (초 단위)
        self.sold_stocks_cache_duration = 300  # 캐시 유효 시간 (5분)
        
        # 디렉토리 생성
        os.makedirs('logs', exist_ok=True)
        
        # 로거 설정
        self.logger = setup_logger(market_type, self.config)
    
    def send_discord_message(self, message: str, error: bool = False) -> None:
        """디스코드로 메시지를 전송합니다."""
        try:
            if not self.discord_webhook_url:
                return
                
            webhook = DiscordWebhook(url=self.discord_webhook_url, content=message)
            webhook.execute()
        except Exception as e:
            self.logger.error(f"디스코드 메시지 전송 실패: {str(e)}")
    
    def _wait_for_api_call(self) -> None:
        """API 호출 간격을 제어합니다."""
        current_time = time.time()
        elapsed = current_time - self.last_api_call
        if elapsed < self.api_call_interval:
            time.sleep(self.api_call_interval - elapsed)
        self.last_api_call = time.time()
    
    def _retry_api_call(self, func, *args, **kwargs) -> Optional[Dict]:
        """API 호출을 재시도합니다."""
        for attempt in range(self.max_retries):
            try:
                self._wait_for_api_call()
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                if attempt == self.max_retries - 1:  # 마지막 시도
                    raise Exception(f"API 호출 실패 (최대 재시도 횟수 초과): {str(e)}")
                time.sleep(self.api_call_interval * (attempt + 1))  # 점진적 대기 시간 증가
    
    def get_today_sold_stocks(self) -> List[str]:
        """API를 통해 당일 매도한 종목 코드 목록을 조회합니다.
        
        Returns:
            List[str]: 당일 매도한 종목 코드 목록
        """
        # 이 메서드는 하위 클래스에서 구현해야 합니다.
        return []
    
    def is_sold_today(self, stock_code: str) -> bool:
        """당일 매도 종목인지 확인합니다. API를 통해 실시간으로 확인합니다."""
        # 캐시 유효 시간이 지났으면 API로 최신 정보 조회
        current_time = time.time()
        if current_time - self.sold_stocks_cache_time > self.sold_stocks_cache_duration:
            self.sold_stocks_cache = self.get_today_sold_stocks()
            self.sold_stocks_cache_time = current_time
            
        return stock_code in self.sold_stocks_cache
    
    def load_settings(self) -> None:
        """구글 스프레드시트에서 설정을 로드합니다."""
        raise NotImplementedError("이 메서드는 하위 클래스에서 구현해야 합니다.")
    
    def execute_trade(self) -> None:
        """매매를 실행합니다."""
        raise NotImplementedError("이 메서드는 하위 클래스에서 구현해야 합니다.")
    
    def update_stock_report(self) -> None:
        """주식 현황을 업데이트합니다."""
        raise NotImplementedError("이 메서드는 하위 클래스에서 구현해야 합니다.")