import os
import logging
import time
from datetime import datetime
from discord_webhook import DiscordWebhook

class CustomLogger:
    """통합 로깅 시스템"""
    
    def __init__(self, market_type: str, config: dict):
        """
        Args:
            market_type (str): 시장 유형 (KOR/USA)
            config (dict): 설정 정보
        """
        self.market_type = market_type
        self.config = config
        self.discord_webhook_url = config['discord']['webhook_url']
        
        # 로그 디렉토리 생성
        os.makedirs('logs', exist_ok=True)
        
        # 로거 초기화
        self.logger = self._setup_logger()
        
        # 시장 상태 메시지 관리를 위한 변수 추가
        self.last_market_status_message = ""
        self.last_market_status_time = 0
        self.market_status_interval = 3600  # 1시간(3600초) 간격
    
    def _setup_logger(self) -> logging.Logger:
        """기본 로거를 설정합니다."""
        # 로거 생성
        logger = logging.getLogger(f'stock_trader.{self.market_type.lower()}')
        
        # 로그 레벨 설정
        log_level = getattr(logging, self.config['logging']['level'].upper(), logging.INFO)
        logger.setLevel(log_level)
        
        # 이미 핸들러가 있다면 제거
        if logger.handlers:
            logger.handlers.clear()
        
        # 시장별 로그 파일 설정
        log_file = os.path.join('logs', f'{self.market_type.lower()}_trading.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
        
        # 미국장의 경우 미국 시간으로 표시, 한국장은 한국 시간으로 표시
        if self.market_type.upper() == 'USA':
            formatter = logging.Formatter('[%(asctime)s ET] %(levelname)s: %(message)s',
                                        datefmt='%Y-%m-%d %H:%M:%S')
        else:
            formatter = logging.Formatter('[%(asctime)s KST] %(levelname)s: %(message)s',
                                        datefmt='%Y-%m-%d %H:%M:%S')
        
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # 콘솔 출력 설정
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # propagate 설정
        logger.propagate = False
        
        return logger
    
    def _format_discord_message(self, message: str, level: str) -> str:
        """디스코드 메시지 포맷을 지정합니다."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 메시지 종류에 따른 이모지와 포맷 선택
        if "매수" in message:
            emoji = "🟢"
            format_type = "ini"
        elif "매도" in message:
            emoji = "🔴"
            format_type = "ini"
        elif "스탑로스" in message:
            emoji = "⛔"
            format_type = "diff"
        elif "트레일링 스탑" in message:
            emoji = "🔻"
            format_type = "diff"
        elif level == "ERROR" or "오류" in message:
            emoji = "⚠️"
            format_type = "diff"
        elif level == "WARNING":
            emoji = "⚠️"
            format_type = "fix"
        elif "시작" in message:
            emoji = "🎯"
            format_type = "yaml"
        elif "종료" in message:
            emoji = "🏁"
            format_type = "yaml"
        elif "장 시작" in message:
            emoji = "⏰"
            format_type = "fix"
        else:
            emoji = "🔄"
            format_type = "ini"
        
        return f"```{format_type}\n[{now}] {emoji} {message}\n```"
    
    def _should_send_to_discord(self, message: str, level: str) -> bool:
        """디스코드로 전송해야 하는 메시지인지 확인합니다."""
        # 마켓 상태 메시지 체크
        market_status_keywords = [
            "현재 장 운영 시간이 아닙니다",
            "현재 운영 중인 시장이 없습니다",
            "주말은 거래일이 아닙니다",
            "오늘은 개장일이 아닙니다",
            "휴장"
        ]
        
        # 마켓 상태 메시지인 경우 특별 처리
        if any(keyword in message for keyword in market_status_keywords):
            current_time = time.time()
            
            # 동일한 메시지이고 시간 간격이 충분하지 않은 경우 전송하지 않음
            if (message == self.last_market_status_message and 
                current_time - self.last_market_status_time < self.market_status_interval):
                return False
            
            # 상태가 변경되었거나 시간 간격이 충분한 경우 전송
            self.last_market_status_message = message
            self.last_market_status_time = current_time
            return True
        
        # 매매 관련 메시지
        if any(keyword in message for keyword in ["매수", "매도", "스탑로스", "트레일링 스탑"]):
            return True
        
        # 오류 메시지
        if level == "ERROR" or "오류" in message:
            return True
        
        # 프로그램 상태 메시지
        if any(keyword in message for keyword in ["시작", "종료", "장 시작", "장 마감"]):
            return True
        
        # 자금 부족 등 중요 알림
        if "자금 부족" in message:
            return True
            
        return False
    
    def _send_to_discord(self, message: str, level: str):
        """디스코드로 메시지를 전송합니다."""
        try:
            formatted_message = self._format_discord_message(message, level)
            webhook = DiscordWebhook(
                url=self.discord_webhook_url,
                content=formatted_message,
                rate_limit_retry=True
            )
            response = webhook.execute()
            
            # 실제 오류 상태 코드일 때만 로그 출력 (4xx, 5xx)
            if response.status_code >= 400:
                self.logger.error(f"디스코드 메시지 전송 실패: 상태 코드 {response.status_code}")
        except Exception as e:
            self.logger.error(f"디스코드 메시지 전송 실패: {str(e)}")
    
    def info(self, message: str, send_discord: bool = True) -> None:
        """INFO 레벨 로그를 기록합니다."""
        self.logger.info(message)
        if send_discord and self.discord_webhook_url and self._should_send_to_discord(message, "INFO"):
            self._send_to_discord(message, "INFO")
    
    def warning(self, message: str, send_discord: bool = True):
        """WARNING 레벨 메시지를 기록합니다."""
        self.logger.warning(message)
        if send_discord and self.discord_webhook_url and self._should_send_to_discord(message, "WARNING"):
            self._send_to_discord(message, "WARNING")
    
    def error(self, message: str, send_discord: bool = True) -> None:
        """ERROR 레벨 로그를 기록합니다."""
        self.logger.error(message)
        if send_discord and self.discord_webhook_url and self._should_send_to_discord(message, "ERROR"):
            self._send_to_discord(message, "ERROR")
    
    def debug(self, message: str):
        """DEBUG 레벨 메시지를 기록합니다."""
        self.logger.debug(message)

def setup_logger(market_type: str, config: dict) -> CustomLogger:
    """로거를 설정합니다."""
    return CustomLogger(market_type, config) 