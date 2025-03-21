import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any
import pytz
import json

class TradeHistoryManager:
    """거래 내역을 SQLite 데이터베이스로 저장하고 관리하는 클래스
    
    이 클래스는 주식 거래 내역을 SQLite 데이터베이스에 저장하고 관리합니다.
    국내(KOR)와 해외(USA) 시장의 거래 내역을 각각 다른 데이터베이스에서 관리합니다.
    """
    
    def __init__(self, market_type: str):
        """TradeHistoryManager를 초기화합니다.
        
        Args:
            market_type (str): 시장 유형 (KOR/USA)
                - KOR: 국내 주식 시장
                - USA: 미국 주식 시장
        """
        self.market_type = market_type  # 시장 유형 (KOR/USA)
        self.db_dir = os.path.join("data", "history")  # 데이터베이스 저장 디렉토리
        self.db_path = os.path.join(self.db_dir, f"trade_history_{market_type.lower()}.db")  # 데이터베이스 파일 경로
        
        # 시간대 설정
        if self.market_type.upper() == "USA":
            self.timezone = pytz.timezone("America/New_York")  # 미국 뉴욕 시간대
        else:
            self.timezone = pytz.timezone("Asia/Seoul")  # 한국 시간대
        
        # 디렉토리 생성
        os.makedirs(self.db_dir, exist_ok=True)
        
        # 데이터베이스 초기화
        self._init_database()
    
    def _init_database(self) -> None:
        """데이터베이스와 필요한 테이블들을 초기화합니다.
        
        생성되는 테이블:
        1. trades: 모든 거래 내역을 저장하는 테이블
           - id: 자동 증가하는 기본키
           - trade_type: 거래 유형 (BUY/SELL/REBALANCE/STOP_LOSS/TRAILING_STOP)
           - trade_action: 거래 행동 (BUY/SELL)
           - stock_code: 종목 코드
           - stock_name: 종목명
           - quantity: 거래 수량
           - price: 거래 가격
           - total_amount: 거래 금액
           - ma_period: 이동평균선 기간
           - ma_value: 이동평균선 값
           - reason: 거래 사유
           - profit_loss: 손익
           - profit_loss_pct: 손익률
           - timestamp: 거래 시간
           - timezone: 시간대

        2. stock_history: 종목별 요약 정보를 저장하는 테이블
           - stock_code: 종목 코드 (기본키)
           - stock_name: 종목명
           - first_buy_date: 첫 매수일
           - last_sell_date: 마지막 매도일
           - all_sold: 전량 매도 여부
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 거래 내역 테이블 생성
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,  -- 자동 증가하는 기본키
            trade_type TEXT NOT NULL,              -- 거래 유형 (BUY/SELL/REBALANCE/STOP_LOSS/TRAILING_STOP)
            trade_action TEXT NOT NULL,            -- 거래 행동 (BUY/SELL)
            stock_code TEXT NOT NULL,              -- 종목 코드
            stock_name TEXT NOT NULL,              -- 종목명
            quantity INTEGER NOT NULL,             -- 거래 수량
            price REAL NOT NULL,                   -- 거래 가격
            total_amount REAL NOT NULL,            -- 거래 금액
            ma_period INTEGER,                     -- 이동평균선 기간
            ma_value REAL,                         -- 이동평균선 값
            reason TEXT,                           -- 거래 사유
            profit_loss REAL,                      -- 손익
            profit_loss_pct REAL,                  -- 손익률
            timestamp TEXT NOT NULL,               -- 거래 시간
            timezone TEXT NOT NULL                 -- 시간대
        )
        ''')
        
        # 종목별 거래 내역 테이블 생성
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_history (
            stock_code TEXT PRIMARY KEY,           -- 종목 코드 (기본키)
            stock_name TEXT NOT NULL,              -- 종목명
            first_buy_date TEXT,                   -- 첫 매수일
            last_sell_date TEXT,                   -- 마지막 매도일
            all_sold BOOLEAN DEFAULT 1             -- 전량 매도 여부
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_trade(self, trade_data: Dict[str, Any]) -> None:
        """거래 내역을 데이터베이스에 추가합니다.
        
        Args:
            trade_data (Dict[str, Any]): 거래 데이터
                - trade_type: 거래 유형 (BUY/SELL/REBALANCE/STOP_LOSS/TRAILING_STOP)
                - trade_action: 거래 행동 (BUY/SELL)
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
            trade_data["timezone"] = self.timezone.zone
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 거래 내역 추가
            cursor.execute('''
            INSERT INTO trades (
                trade_type, trade_action, stock_code, stock_name, quantity, price, total_amount,
                ma_period, ma_value, reason, profit_loss, profit_loss_pct,
                timestamp, timezone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade_data["trade_type"],
                trade_data["trade_action"],
                trade_data["stock_code"],
                trade_data["stock_name"],
                trade_data["quantity"],
                trade_data["price"],
                trade_data["total_amount"],
                trade_data.get("ma_period"),
                trade_data.get("ma_value"),
                trade_data.get("reason"),
                trade_data.get("profit_loss"),
                trade_data.get("profit_loss_pct"),
                trade_data["timestamp"],
                trade_data["timezone"]
            ))
            
            # 종목별 거래 내역 업데이트
            self._update_stock_history(cursor, trade_data)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"거래 내역 추가 중 오류 발생: {str(e)}")
    
    def _update_stock_history(self, cursor: sqlite3.Cursor, trade_data: Dict[str, Any]) -> None:
        """종목별 거래 내역 요약 정보를 업데이트합니다.
        
        Args:
            cursor (sqlite3.Cursor): 데이터베이스 커서
            trade_data (Dict[str, Any]): 거래 데이터
        """
        stock_code = trade_data["stock_code"]
        
        # 미국장의 경우 거래소 코드 제외 (코드.거래소 형식에서 코드만 추출)
        if self.market_type.upper() == "USA" and "." in stock_code:
            stock_code = stock_code.split(".")[0]
        
        trade_date = datetime.strptime(trade_data["timestamp"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        
        # 종목 정보 조회 또는 생성
        cursor.execute('''
        INSERT OR IGNORE INTO stock_history (stock_code, stock_name, first_buy_date, last_sell_date, all_sold)
        VALUES (?, ?, NULL, NULL, 1)
        ''', (stock_code, trade_data["stock_name"]))
        
        if trade_data["trade_action"] == "BUY":
            # 첫 매수일 업데이트 (없거나 전량 매도 후 다시 매수하는 경우)
            cursor.execute('''
            UPDATE stock_history
            SET first_buy_date = CASE 
                WHEN first_buy_date IS NULL OR all_sold = 1 THEN ?
                ELSE first_buy_date
            END,
            all_sold = 0
            WHERE stock_code = ?
            ''', (trade_date, stock_code))
        
        elif trade_data["trade_action"] == "SELL":
            # 마지막 매도일 업데이트
            cursor.execute('''
            UPDATE stock_history
            SET last_sell_date = ?
            WHERE stock_code = ?
            ''', (trade_date, stock_code))
            
            # 전량 매도 여부 확인 (보유 수량 계산)
            cursor.execute('''
            SELECT SUM(CASE 
                WHEN trade_action = 'BUY' THEN quantity 
                WHEN trade_action = 'SELL' THEN -quantity 
                ELSE 0 
            END) as total_quantity
            FROM trades
            WHERE stock_code = ?
            ''', (stock_code,))
            
            total_quantity = cursor.fetchone()[0] or 0
            
            if total_quantity <= 0:
                cursor.execute('''
                UPDATE stock_history
                SET all_sold = 1,
                    first_buy_date = NULL
                WHERE stock_code = ?
                ''', (stock_code,))
    
    def get_stock_history(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """종목별 거래 내역을 조회합니다.
        
        Args:
            stock_code (str): 종목 코드
            
        Returns:
            Optional[Dict[str, Any]]: 종목별 거래 내역 정보
                - stock_code: 종목 코드
                - stock_name: 종목명
                - first_buy_date: 첫 매수일
                - last_sell_date: 마지막 매도일
                - trades: 거래 내역 목록
        """
        # 미국장의 경우 거래소 코드 제외 (코드.거래소 형식에서 코드만 추출)
        if self.market_type.upper() == "USA" and "." in stock_code:
            stock_code = stock_code.split(".")[0]
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 종목 정보 조회
        cursor.execute('''
        SELECT stock_code, stock_name, first_buy_date, last_sell_date, all_sold
        FROM stock_history
        WHERE stock_code = ?
        ''', (stock_code,))
        
        stock_info = cursor.fetchone()
        if not stock_info:
            conn.close()
            return None
        
        # 거래 내역 조회
        cursor.execute('''
        SELECT *
        FROM trades
        WHERE stock_code = ?
        ORDER BY timestamp
        ''', (stock_code,))
        
        trades = cursor.fetchall()
        conn.close()
        
        # 결과 구성
        result = {
            "stock_code": stock_info[0],
            "stock_name": stock_info[1],
            "first_buy_date": stock_info[2],
            "last_sell_date": stock_info[3],
            "trades": []
        }
        
        # 컬럼명 매핑
        columns = [description[0] for description in cursor.description]
        
        # 거래 내역 추가
        for trade in trades:
            trade_dict = dict(zip(columns, trade))
            result["trades"].append(trade_dict)
        
        return result
    
    def get_first_buy_date(self, stock_code: str) -> Optional[str]:
        """종목의 첫 매수일을 조회합니다.
        
        Args:
            stock_code (str): 종목 코드
            
        Returns:
            Optional[str]: 첫 매수일 (YYYY-MM-DD 형식)
        """
        # 미국장의 경우 거래소 코드 제외 (코드.거래소 형식에서 코드만 추출)
        if self.market_type.upper() == "USA" and "." in stock_code:
            stock_code = stock_code.split(".")[0]
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT first_buy_date
        FROM stock_history
        WHERE stock_code = ?
        ''', (stock_code,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None
    
    def get_trades_by_type_and_code(self, trade_type: str, stock_code: str) -> List[Dict[str, Any]]:
        """특정 거래 유형과 종목 코드에 해당하는 거래 내역을 조회합니다.
        
        Args:
            trade_type (str): 거래 유형 (BUY/SELL/REBALANCE/STOP_LOSS/TRAILING_STOP)
            stock_code (str): 종목 코드
            
        Returns:
            List[Dict[str, Any]]: 거래 내역 목록
                - 각 거래 내역은 trades 테이블의 모든 필드를 포함
        """
        try:
            # 미국장의 경우 거래소 코드 제외 (코드.거래소 형식에서 코드만 추출)
            if self.market_type.upper() == "USA" and "." in stock_code:
                stock_code = stock_code.split(".")[0]
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT *
            FROM trades
            WHERE stock_code = ? AND trade_type = ?
            ORDER BY timestamp
            ''', (stock_code, trade_type))
            
            trades = cursor.fetchall()
            conn.close()
            
            # 컬럼명 매핑
            columns = [description[0] for description in cursor.description]
            
            # 결과 구성
            result = []
            for trade in trades:
                trade_dict = dict(zip(columns, trade))
                result.append(trade_dict)
            
            return result
            
        except Exception as e:
            print(f"거래 내역 조회 중 오류 발생: {str(e)}")
            return [] 