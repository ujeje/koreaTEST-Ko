import os
import yaml
import json
import logging
import time
from datetime import datetime, timedelta
import requests
from typing import Dict, Optional
from src.utils.network_utils import get_public_ip, generate_global_uid

class TokenManager:
    """한국투자증권 API 토큰 관리자"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: str = None):
        if self._initialized and not config_path:
            return
            
        if config_path:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            # 모의투자 여부에 따라 설정
            self.is_paper_trading = self.config['api']['is_paper_trading']
            self.account_type = self.config['api']['account_type']  # 개인/법인 구분
            
            if self.is_paper_trading:
                self.base_url = self.config['api']['paper']['url']
                self.api_key = self.config['api']['paper']['key']
                self.api_secret = self.config['api']['paper']['secret']
                self.account_no = self.config['api']['paper']['account']
            else:
                self.base_url = self.config['api']['real']['url']
                self.api_key = self.config['api']['real']['key']
                self.api_secret = self.config['api']['real']['secret']
                self.account_no = self.config['api']['real']['account']
            
            self.access_token = None
            self.token_expired_time = None
            self.last_token_request = 0
            self._initialized = True
    
    def get_token(self) -> str:
        """토큰을 가져옵니다. 필요한 경우 새로 생성합니다."""
        current_time = datetime.now()
        
        # 토큰이 없거나 만료되었으면 새로 생성
        if (not self.access_token or 
            not self.token_expired_time or 
            current_time >= self.token_expired_time):
            
            # API 호출 제한 (1분당 1회) 체크
            if time.time() - self.last_token_request < 60:
                time.sleep(60 - (time.time() - self.last_token_request))
            
            self._create_token()
        
        return self.access_token
    
    def _create_token(self) -> None:
        """토큰을 생성하고 저장합니다."""
        url = f"{self.base_url}/oauth2/tokenP"
        
        data = {
            "grant_type": "client_credentials",
            "appkey": self.api_key,
            "appsecret": self.api_secret
        }
        
        headers = {
            "content-type": "application/json"
        }
        
        # 법인계좌인 경우 추가 헤더 설정
        if self.account_type == "C":  # C: 법인
            headers.update({
                "custtype": "B"  # B: 법인
            })
        
        self.last_token_request = time.time()
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response_data = response.json()
        
        if response.status_code == 200:
            self.access_token = response_data.get('access_token')
            self.token_expired_time = datetime.now() + timedelta(seconds=response_data.get('expires_in', 86400))
            logging.info("토큰이 성공적으로 생성되었습니다.")
        else:
            logging.error(f"토큰 생성 실패: {response_data}")
            raise Exception("토큰 생성에 실패했습니다.") 