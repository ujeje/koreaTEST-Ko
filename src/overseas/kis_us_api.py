import os
import yaml
import requests
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
import pandas as pd
from src.utils.token_manager import TokenManager

class KISUSAPIManager:
    """한국투자증권 해외주식 API 매니저
    
    해외 주식 거래와 관련된 모든 API 호출을 담당하는 클래스입니다.
    실전투자와 모의투자를 모두 지원합니다.
    """
    
    def __init__(self, config_path: str):
        """API 매니저를 초기화합니다.
        
        Args:
            config_path (str): 설정 파일 경로
        """
        # 설정 파일 로드
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # 모의투자 여부에 따라 base_url 설정
        self.is_paper_trading = self.config['api']['is_paper_trading']
        self.base_url = self.config['api']['url_paper'] if self.is_paper_trading else self.config['api']['url_real']
        
        # API 인증 정보 설정
        self.api_key = self.config['api']['key']
        self.api_secret = self.config['api']['secret']
        self.account_no = self.config['api']['account']
        self.token_manager = TokenManager(config_path)
    
    def _check_token(self) -> str:
        """토큰의 유효성을 확인하고 필요시 갱신합니다."""
        return self.token_manager.get_token()
    
    def get_stock_price(self, stock_code: str) -> Optional[Dict]:
        """주식 현재가 정보를 조회합니다."""
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": "HHDFS00000300"
        }
        
        params = {
            "AUTH": "",
            "EXCD": self._get_exchange_code(stock_code),  # NYSE: NYS, NASDAQ: NAS, AMEX: AMS
            "SYMB": self._get_symbol(stock_code)
        }
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"주가 조회 실패: {response.text}")
            return None
    
    def get_account_balance(self) -> Optional[Dict]:
        """계좌 잔고를 조회합니다."""
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": "VTTS3012R" if self.is_paper_trading else "TTTS3012R"  # 모의/실전 구분
        }
        
        params = {
            "CANO": self.account_no[:8],
            "ACNT_PRDT_CD": self.account_no[8:],
            "OVRS_EXCG_CD": "NASD",  # NASD(미국전체), NAS(나스닥), NYSE(뉴욕), AMEX(아멕스)
            "TR_CRCY_CD": "USD",
            "CTX_AREA_FK200": "",   # 잔고는 한번에 최대 100개까지 조회 가능, 100개 초과시 수정필요
            "CTX_AREA_NK200": ""
        }
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"잔고 조회 실패: {response.text}")
            return None
    
    def get_psbl_amt(self, stock_code: str) -> Optional[Dict]:
        """해외주식 매수가능금액 조회
        
        Args:
            stock_code (str): 종목코드 (종목코드.거래소 형식)
            
        Returns:
            Dict: 매수가능금액 정보를 담은 딕셔너리
        """
        try:
            access_token = self._check_token()
            
            # API 경로 설정
            url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-psamount"
            
            # 헤더 설정
            headers = {
                "Content-Type": "application/json; charset=utf-8",
                "authorization": f"Bearer {access_token}",
                "appkey": self.api_key,
                "appsecret": self.api_secret,
                "tr_id": "VTTS3007R" if self.is_paper_trading else "TTTS3007R"  # 모의/실전 구분
            }
            
            # 요청 파라미터
            params = {
                "CANO": self.account_no[:8],
                "ACNT_PRDT_CD": self.account_no[8:],
                "OVRS_EXCG_CD": self._get_ovrs_exchange_code(stock_code),
                "ITEM_CD": self._get_symbol(stock_code),
                "OVRS_ORD_UNPR": "0"  # 주문단가 0으로 설정
            }
            
            # API 요청
            response = requests.get(url, headers=headers, params=params)
            
            # 응답 확인
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0':
                    return data
                else:
                    logging.error(f"매수가능금액 조회 실패: {data['msg1']}")
                    return None
            else:
                logging.error(f"매수가능금액 조회 실패: {response.status_code}")
                return None
                
        except Exception as e:
            logging.error(f"매수가능금액 조회 중 오류 발생: {str(e)}")
            return None
    
    def order_stock(self, stock_code: str, order_type: str, quantity: int, price: float = 0) -> Optional[Dict]:
        """주식 주문을 실행합니다.
        
        Args:
            stock_code (str): 종목코드
            order_type (str): 주문 유형 ("BUY" 또는 "SELL")
            quantity (int): 주문 수량
            price (float, optional): 주문 가격. 시장가 주문시 0. Defaults to 0.
            
        Returns:
            Dict: 주문 결과를 담은 딕셔너리
        """
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/order"
        
        # 모의/실전 구분
        if self.is_paper_trading:
            tr_id = "VTTT1002U" if order_type == "BUY" else "VTTT1006U"
        else:
            tr_id = "TTTT1002U" if order_type == "BUY" else "TTTT1006U"
            
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": tr_id
        }
        
        data = {
            "CANO": self.account_no[:8],
            "ACNT_PRDT_CD": self.account_no[8:],
            "PDNO": self._get_symbol(stock_code),
            "ORD_DVSN": "00" if price > 0 else "01",  # 00: 지정가, 01: 시장가
            "ORD_QTY": str(quantity),
            "OVRS_EXCG_CD": self._get_exchange_code(stock_code),
            "ORD_UNPR": str(price) if price > 0 else "0",
            "ORD_SVR_DVSN_CD": "0"
        }
        
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"주문 실패: {response.text}")
            return None
    
    def get_daily_price(self, stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """일별 주가 정보를 조회합니다."""
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": "HHDFS76240000"
        }
        
        params = {
            "AUTH": "",
            "EXCD": self._get_exchange_code(stock_code),
            "SYMB": self._get_symbol(stock_code),
            "GUBN": "0",  # 0: 일, 1: 주, 2: 월
            "BYMD": start_date,
            "MODP": "1"
        }
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'output2' in data:
                return pd.DataFrame(data['output2'])
            return None
        else:
            logging.error(f"일별 주가 조회 실패: {response.text}")
            return None
    
    def _get_exchange_code(self, stock_code: str) -> str:
        """종목코드에서 거래소 코드를 추출합니다."""
        if '.NYSE' in stock_code:
            return 'NYS'
        elif '.NASD' in stock_code:
            return 'NAS'
        elif '.AMEX' in stock_code:
            return 'AMS'
        return 'NAS'  # 기본값
    
    def _get_ovrs_exchange_code(self, stock_code: str) -> str:
        """종목코드에서 거래소 코드를 추출합니다."""
        if '.NYSE' in stock_code:
            return 'NYSE'
        elif '.NASD' in stock_code:
            return 'NASD'
        elif '.AMEX' in stock_code:
            return 'AMEX'
        return 'NASD'  # 기본값
    
    def _get_symbol(self, stock_code: str) -> str:
        """종목코드에서 심볼을 추출합니다."""
        return stock_code.split('.')[0]
    
    def get_total_balance(self) -> Optional[Dict]:
        """해외주식 체결기준현재잔고를 조회합니다.
        
        Returns:
            Dict: 체결기준현재잔고 정보를 담은 딕셔너리
            - output3['tot_asst_amt']: 총자산금액
            - output3['frcr_evlu_tota']: 외화평가총액
            - output3['tot_evlu_pfls_amt']: 총평가손익금액
        """
        try:
            access_token = self._check_token()
            
            # API 경로 설정
            url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-present-balance"
            
            # 헤더 설정
            headers = {
                "Content-Type": "application/json; charset=utf-8",
                "authorization": f"Bearer {access_token}",
                "appkey": self.api_key,
                "appsecret": self.api_secret,
                "tr_id": "VTRP6504R" if self.is_paper_trading else "CTRP6504R"  # 모의/실전 구분
            }
            
            # 요청 파라미터
            params = {
                "CANO": self.account_no[:8],
                "ACNT_PRDT_CD": self.account_no[8:],
                "WCRC_FRCR_DVSN_CD": "02",  # 01:원화, 02:외화
                "NATN_CD": "840",  # 미국
                "TR_MKET_CD": "00",  # 전체
                "INQR_DVSN_CD": "00"  # 전체조회
            }
            
            # API 요청
            response = requests.get(url, headers=headers, params=params)
            
            # 응답 확인
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0':
                    return data
                else:
                    logging.error(f"체결기준현재잔고 조회 실패: {data['msg1']}")
                    return None
            else:
                logging.error(f"체결기준현재잔고 조회 실패: {response.status_code}")
                return None
                
        except Exception as e:
            logging.error(f"체결기준현재잔고 조회 중 오류 발생: {str(e)}")
            return None 