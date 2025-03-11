import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import pytz

class TradeHistoryManager:
    """거래 내역을 JSON 파일로 저장하고 관리하는 클래스"""
    
    def __init__(self, market_type: str):
        """
        Args:
            market_type (str): 시장 유형 (KOR/USA)
        """
        self.market_type = market_type
        self.history_dir = os.path.join("data", "history")
        self.trade_history_file = os.path.join(self.history_dir, f"trade_history_{market_type.lower()}.json")
        self.stock_history_dir = os.path.join(self.history_dir, "stocks")
        
        # 시간대 설정
        if self.market_type.upper() == "USA":
            self.timezone = pytz.timezone("America/New_York")  # 미국 뉴욕 시간대
        else:
            self.timezone = pytz.timezone("Asia/Seoul")  # 한국 시간대
        
        # 디렉토리 생성
        os.makedirs(self.history_dir, exist_ok=True)
        os.makedirs(self.stock_history_dir, exist_ok=True)
        
        # 거래 내역 파일이 없으면 생성
        if not os.path.exists(self.trade_history_file):
            self._init_trade_history_file()
    
    def _init_trade_history_file(self) -> None:
        """거래 내역 파일을 초기화합니다."""
        initial_data = {
            "market_type": self.market_type,
            "trades": []
        }
        with open(self.trade_history_file, 'w', encoding='utf-8') as f:
            json.dump(initial_data, f, ensure_ascii=False, indent=2)
    
    def add_trade(self, trade_data: Dict[str, Any]) -> None:
        """거래 내역을 추가합니다.
        
        Args:
            trade_data (Dict[str, Any]): 거래 데이터
                - trade_type: 거래 유형 (BUY/SELL/REBALANCE/STOP_LOSS/TRAILING_STOP)
                - stock_code: 종목 코드
                - stock_name: 종목명
                - quantity: 거래 수량
                - price: 거래 가격
                - total_amount: 거래 금액
                - ma_period: 이동평균선 기간 (매매 기준)
                - ma_value: 이동평균선 값
                - reason: 거래 사유
                - profit_loss: 손익 (매도 시)
                - profit_loss_pct: 손익률 (매도 시)
        """
        try:
            # 현재 시간을 해당 시장의 시간대로 변환하여 추가
            now = datetime.now(pytz.UTC).astimezone(self.timezone)
            trade_data["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
            trade_data["timezone"] = self.timezone.zone  # 시간대 정보도 함께 저장
            
            # 거래 내역 파일 읽기
            with open(self.trade_history_file, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
            
            # 거래 내역 추가
            history_data["trades"].append(trade_data)
            
            # 거래 내역 파일 저장
            with open(self.trade_history_file, 'w', encoding='utf-8') as f:
                json.dump(history_data, f, ensure_ascii=False, indent=2)
            
            # 종목별 거래 내역 업데이트
            self._update_stock_history(trade_data)
            
        except Exception as e:
            print(f"거래 내역 추가 중 오류 발생: {str(e)}")
    
    def _update_stock_history(self, trade_data: Dict[str, Any]) -> None:
        """종목별 거래 내역을 업데이트합니다."""
        stock_code = trade_data["stock_code"]
        
        # 미국장의 경우 거래소 코드 제외 (코드.거래소 형식에서 코드만 추출)
        if self.market_type.upper() == "USA" and "." in stock_code:
            stock_code = stock_code.split(".")[0]
        
        stock_history_file = os.path.join(self.stock_history_dir, f"{stock_code}.json")
        
        # 종목 거래 내역 파일이 없으면 생성
        if not os.path.exists(stock_history_file):
            initial_data = {
                "stock_code": stock_code,
                "stock_name": trade_data["stock_name"],
                "first_buy_date": None,
                "last_sell_date": None,
                "trades": []
            }
            stock_data = initial_data
        else:
            # 기존 파일 읽기
            with open(stock_history_file, 'r', encoding='utf-8') as f:
                stock_data = json.load(f)
        
        # 거래 내역 추가
        stock_data["trades"].append(trade_data)
        
        # 매수/매도 날짜 업데이트
        trade_date = datetime.strptime(trade_data["timestamp"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        
        if trade_data["trade_type"] == "BUY":
            # 첫 매수일 업데이트 (없거나 전량 매도 후 다시 매수하는 경우)
            if stock_data["first_buy_date"] is None or stock_data.get("all_sold", True):
                stock_data["first_buy_date"] = trade_date
                stock_data["all_sold"] = False
        
        elif trade_data["trade_type"] == "SELL":
            # 마지막 매도일 업데이트
            stock_data["last_sell_date"] = trade_date
            
            # 전량 매도 여부 확인 (보유 수량 계산)
            total_buy = sum(t["quantity"] for t in stock_data["trades"] if t["trade_type"] == "BUY")
            total_sell = sum(t["quantity"] for t in stock_data["trades"] if t["trade_type"] == "SELL")
            
            if total_buy <= total_sell:
                stock_data["all_sold"] = True
                # 전량 매도 시 첫 매수일 초기화
                stock_data["first_buy_date"] = None
        
        # 파일 저장
        with open(stock_history_file, 'w', encoding='utf-8') as f:
            json.dump(stock_data, f, ensure_ascii=False, indent=2)
    
    def get_stock_history(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """종목별 거래 내역을 조회합니다."""
        # 미국장의 경우 거래소 코드 제외 (코드.거래소 형식에서 코드만 추출)
        if self.market_type.upper() == "USA" and "." in stock_code:
            stock_code = stock_code.split(".")[0]
            
        stock_history_file = os.path.join(self.stock_history_dir, f"{stock_code}.json")
        
        if not os.path.exists(stock_history_file):
            return None
        
        with open(stock_history_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_first_buy_date(self, stock_code: str) -> Optional[str]:
        """종목의 첫 매수일을 조회합니다."""
        stock_data = self.get_stock_history(stock_code)
        if stock_data:
            return stock_data.get("first_buy_date")
        return None 
    
    def get_trades_by_type_and_code(self, trade_type: str, stock_code: str) -> List[Dict[str, Any]]:
        """특정 거래 유형과 종목 코드에 해당하는 거래 내역을 조회합니다.
        
        Args:
            trade_type (str): 거래 유형 (BUY/SELL/REBALANCE/STOP_LOSS/TRAILING_STOP)
            stock_code (str): 종목 코드
            
        Returns:
            List[Dict[str, Any]]: 거래 내역 목록
        """
        try:
            stock_data = self.get_stock_history(stock_code)
            if not stock_data:
                return []
            
            # 해당 거래 유형의 거래 내역만 필터링
            filtered_trades = [
                trade for trade in stock_data["trades"] 
                if trade.get("trade_type") == trade_type
            ]
            
            # 시간순으로 정렬 (최신 거래가 마지막에 오도록)
            filtered_trades.sort(key=lambda x: x.get("timestamp", ""))
            
            return filtered_trades
        except Exception as e:
            print(f"거래 내역 조회 중 오류 발생: {str(e)}")
            return [] 