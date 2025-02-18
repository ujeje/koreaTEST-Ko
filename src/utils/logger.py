import os
import logging
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
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s',
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
        if send_discord and self.discord_webhook_url:
            self._send_to_discord(message, "INFO")
    
    def warning(self, message: str):
        """WARNING 레벨 메시지를 기록합니다."""
        self.logger.warning(message)
        if self._should_send_to_discord(message, "WARNING"):
            self._send_to_discord(message, "WARNING")
    
    def error(self, message: str, send_discord: bool = True) -> None:
        """ERROR 레벨 로그를 기록합니다."""
        self.logger.error(message)
        if send_discord and self.discord_webhook_url:
            self._send_to_discord(message, "ERROR")
    
    def debug(self, message: str):
        """DEBUG 레벨 메시지를 기록합니다."""
        self.logger.debug(message)

def setup_logger(market_type: str, config: dict) -> CustomLogger:
    """로거를 설정합니다."""
    return CustomLogger(market_type, config) 