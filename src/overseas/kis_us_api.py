import os
import yaml
import requests
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
import pandas as pd
from src.utils.token_manager import TokenManager
import time

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
        self.logger = logging.getLogger('us_api')
        self.token_manager = TokenManager(config_path)
        self.config = self.token_manager.config
        
        # 실전/모의투자 설정
        self.is_paper_trading = self.config['api']['is_paper_trading']
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
        
        self.logger.info(f"해외주식 API 매니저 초기화 완료 (모의투자: {self.is_paper_trading})")
        
        # API 호출 간격 제어
        self.last_api_call = 0
        self.api_call_interval = 0.5 if self.is_paper_trading else 0.3  # 모의투자: 0.5초, 실전투자: 0.3초
    
    def _check_token(self) -> str:
        """토큰의 유효성을 확인하고 필요시 갱신합니다."""
        return self.token_manager.get_token()
    
    def get_stock_price(self, stock_code: str) -> Optional[Dict]:
        """해외주식 현재가 정보를 조회합니다.
        
        Args:
            stock_code (str): 종목코드 (종목코드.거래소 형식)
            
        Returns:
            Dict: 현재가 정보를 담은 딕셔너리
                - output: 현재가 정보
                    - rsym: 실시간조회종목코드 (D+시장구분+종목코드)
                    - zdiv: 소수점자리수
                    - base: 전일종가
                    - pvol: 전일거래량
                    - last: 현재가
                    - sign: 대비기호 (1:상한, 2:상승, 3:보합, 4:하한, 5:하락)
                    - diff: 대비 (현재가-전일종가)
                    - rate: 등락율
                    - tvol: 당일거래량
                    - tamt: 당일거래대금
                    - ordy: 매수가능여부
        """
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": "HHDFS00000300"
        }
        
        params = {
            "AUTH": "",
            "EXCD": self._get_exchange_code(stock_code),  # NYS, NAS, AMS 등
            "SYMB": self._get_symbol(stock_code)
        }
        
        response = requests.get(url, headers=headers, params=params)
        time.sleep(self.api_call_interval)
        
        if response.status_code == 200:
            result = response.json()
            if result['rt_cd'] == '0':  # 정상 응답
                return result
            else:
                logging.error(f"해외주식 현재가 조회 실패: {result['msg1']}")
                return None
        else:
            logging.error(f"해외주식 현재가 조회 실패: {response.text}")
            return None
    
    def get_account_balance(self) -> Optional[Dict]:
        """해외주식 잔고를 조회합니다.
        
        Returns:
            Dict: 계좌 잔고 정보를 담은 딕셔너리
                - output1: 보유 종목 리스트
                    - ovrs_pdno: 해외상품번호
                    - ovrs_item_name: 해외종목명
                    - frcr_evlu_pfls_amt: 외화평가손익금액
                    - evlu_pfls_rt: 평가손익율
                    - pchs_avg_pric: 매입평균가격
                    - ovrs_cblc_qty: 해외잔고수량
                    - ord_psbl_qty: 주문가능수량
                    - frcr_pchs_amt1: 외화매입금액1
                    - ovrs_stck_evlu_amt: 해외주식평가금액
                    - now_pric2: 현재가격2
                    - tr_crcy_cd: 거래통화코드
                - output2: 계좌 요약 정보
                    - frcr_buy_amt_smtl1: 외화매수금액합계1
                    - ovrs_rlzt_pfls_amt: 해외실현손익금액
                    - tot_evlu_pfls_amt: 총평가손익금액
                    - tot_pftrt: 총수익률
        """
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": "VTTS3012R" if self.is_paper_trading else "TTTS3012R"  # 모의/실전 구분
        }
        
        params = {
            "CANO": self.account_no[:8],
            "ACNT_PRDT_CD": self.account_no[8:],
            "OVRS_EXCG_CD": "NASD",  # 미국 전체 (실전: NASD, 모의: NASD/NYSE/AMEX)
            "TR_CRCY_CD": "USD",     # 거래통화코드: 미국달러
            "CTX_AREA_FK200": "",    # 연속조회검색조건
            "CTX_AREA_NK200": ""     # 연속조회키
        }
        
        response = requests.get(url, headers=headers, params=params)
        time.sleep(self.api_call_interval)
        
        if response.status_code == 200:
            data = response.json()
            if data['rt_cd'] == '0':  # 정상 응답
                # 연속조회 필요 여부 확인
                tr_cont = data.get('tr_cont', 'D')
                
                # 연속 조회가 필요한 경우 (tr_cont가 M인 경우)
                if tr_cont == 'M':
                    next_data = self._get_remaining_balance(
                        url, headers, params,
                        data['ctx_area_fk200'],
                        data['ctx_area_nk200']
                    )
                    if next_data:
                        # output1(보유종목) 리스트 합치기
                        data['output1'].extend(next_data.get('output1', []))
                        # output2(계좌잔고) 정보는 마지막 것으로 업데이트
                        data['output2'] = next_data.get('output2', data['output2'])
                
                return data
            else:
                logging.error(f"잔고 조회 실패: {data['msg1']}")
                return None
        else:
            logging.error(f"잔고 조회 실패: {response.text}")
            return None
    
    def _get_remaining_balance(self, url: str, headers: dict, params: dict, ctx_area_fk200: str, ctx_area_nk200: str) -> Optional[Dict]:
        """연속 조회가 필요한 경우 나머지 잔고를 조회합니다."""
        try:
            # 연속조회 파라미터 설정
            params['CTX_AREA_FK200'] = ctx_area_fk200
            params['CTX_AREA_NK200'] = ctx_area_nk200
            
            response = requests.get(url, headers=headers, params=params)
            time.sleep(self.api_call_interval)
            
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0':  # 정상 응답
                    # 더 연속 조회가 필요한 경우 재귀 호출
                    if data.get('tr_cont') == 'M':
                        next_data = self._get_remaining_balance(
                            url, headers, params,
                            data['ctx_area_fk200'],
                            data['ctx_area_nk200']
                        )
                        if next_data:
                            # output1(보유종목) 리스트 합치기
                            data['output1'].extend(next_data.get('output1', []))
                            # output2(계좌잔고) 정보는 마지막 것으로 업데이트
                            data['output2'] = next_data.get('output2', data['output2'])
                    return data
                else:
                    logging.error(f"연속 잔고 조회 실패: {data['msg1']}")
                    return None
            else:
                logging.error(f"연속 잔고 조회 실패: {response.text}")
                return None
                
        except Exception as e:
            logging.error(f"연속 잔고 조회 중 오류 발생: {str(e)}")
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
            time.sleep(self.api_call_interval)
            
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
        """해외주식 주문을 실행합니다.
        
        Args:
            stock_code (str): 종목코드 (종목코드.거래소 형식)
            order_type (str): 주문 유형 ("BUY" 또는 "SELL")
            quantity (int): 주문 수량
            price (float, optional): 주문 가격. 시장가 주문시 0. Defaults to 0.
            
        Returns:
            Dict: 주문 결과를 담은 딕셔너리
                - output: 주문 결과 정보
                    - KRX_FWDG_ORD_ORGNO: 한국거래소전송주문조직번호
                    - ODNO: 주문번호
                    - ORD_TMD: 주문시각 (HHMMSS)
        """
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/order"
        
        # 모의/실전 구분 및 매수/매도 구분
        if self.is_paper_trading:
            tr_id = "VTTT1002U" if order_type == "BUY" else "VTTT1001U"  # 모의투자: 매수/매도
        else:
            tr_id = "TTTT1002U" if order_type == "BUY" else "TTTT1006U"  # 실전투자: 매수/매도
            
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": tr_id
        }
        
        data = {
            "CANO": self.account_no[:8],                                # 종합계좌번호
            "ACNT_PRDT_CD": self.account_no[8:],                      # 계좌상품코드
            "OVRS_EXCG_CD": self._get_ovrs_exchange_code(stock_code), # 해외거래소코드
            "PDNO": self._get_symbol(stock_code),                      # 상품번호(종목코드)
            "ORD_QTY": str(quantity),                                  # 주문수량
            "OVRS_ORD_UNPR": str(price) if price > 0 else "0",        # 해외주문단가
            "ORD_SVR_DVSN_CD": "0",                                   # 주문서버구분코드 (기본값: "0")
            "ORD_DVSN": "00"                                          # 주문구분: 지정가 주문
        }
        
        response = requests.post(url, headers=headers, data=json.dumps(data))
        time.sleep(self.api_call_interval)
        
        if response.status_code == 200:
            result = response.json()
            if result['rt_cd'] == '0':  # 정상 응답
                return result
            else:
                logging.error(f"주문 실패: {result['msg1']}")
                return None
        else:
            logging.error(f"주문 실패: {response.text}")
            return None
    
    def get_daily_price(self, stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """해외주식의 기간별시세를 조회합니다.
        
        Args:
            stock_code (str): 종목코드 (종목코드.거래소 형식)
            start_date (str): 시작일자 (YYYYMMDD)
            end_date (str): 종료일자 (YYYYMMDD)
            
        Returns:
            Optional[pd.DataFrame]: 일별 주가 데이터프레임
                - xymd: 일자 (YYYYMMDD)
                - clos: 종가
                - sign: 대비기호 (1:상한, 2:상승, 3:보합, 4:하한, 5:하락)
                - diff: 대비 (해당일 종가 - 해당 전일 종가)
                - rate: 등락율
                - open: 시가
                - high: 고가
                - low: 저가
                - tvol: 거래량
                - tamt: 거래대금
                - pbid: 매수호가 (마지막 체결 시점)
                - vbid: 매수호가잔량
                - pask: 매도호가 (마지막 체결 시점)
                - vask: 매도호가잔량
                
        Note:
            - 한 번의 호출에 최대 100건까지 조회 가능
            - 무료시세(지연체결가)만 제공됨
            - 지연시간: 미국(0분), 홍콩/베트남/중국/일본(15분)
            - 미국의 경우 장중 당일 시가는 상이할 수 있으며, 익일 정정 표시
        """
        try:
            access_token = self._check_token()
            
            url = f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice"
            headers = {
                "Content-Type": "application/json; charset=utf-8",
                "authorization": f"Bearer {access_token}",
                "appkey": self.api_key,
                "appsecret": self.api_secret,
                "tr_id": "HHDFS76240000"
            }
            
            params = {
                "AUTH": "",                                # 사용자권한정보
                "EXCD": self._get_exchange_code(stock_code),  # 거래소코드
                "SYMB": self._get_symbol(stock_code),     # 종목코드
                "GUBN": "0",                              # 0:일, 1:주, 2:월
                "BYMD": end_date,                         # 조회 기준일자
                "MODP": "1"                               # 수정주가 반영 여부 (1:반영)
            }
            
            # API 호출 후 대기
            response = requests.get(url, headers=headers, params=params)
            time.sleep(self.api_call_interval)
            
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0':  # 정상 응답
                    # output1 정보 저장 (실시간조회종목코드, 소수점자리수, 전일종가)
                    output1 = data.get('output1', {})
                    rsym = output1.get('rsym', '')  # 실시간조회종목코드
                    zdiv = output1.get('zdiv', '')  # 소수점자리수
                    nrec = output1.get('nrec', '')  # 전일종가
                    
                    # output2 데이터를 DataFrame으로 변환
                    if 'output2' in data:
                        df = pd.DataFrame(data['output2'])
                        # 날짜(xymd) 기준으로 오름차순 정렬 (과거 -> 최근)
                        df['xymd'] = pd.to_datetime(df['xymd'], format='%Y%m%d')
                        df = df.sort_values('xymd', ascending=True).reset_index(drop=True)
                        
                        # 데이터 타입 변환
                        numeric_columns = ['clos', 'diff', 'rate', 'open', 'high', 'low', 'tvol', 'tamt', 'pbid', 'vbid', 'pask', 'vask']
                        for col in numeric_columns:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                        
                        return df
                    else:
                        logging.error("일별 주가 데이터가 없습니다.")
                else:
                    error_msg = data.get('msg1', '알 수 없는 오류가 발생했습니다.')
                    logging.error(f"일별 주가 조회 실패: {error_msg}")
            else:
                logging.error(f"일별 주가 조회 실패: {response.text}")
            return None
            
        except Exception as e:
            logging.error(f"일별 주가 조회 중 오류 발생: {str(e)}")
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
        
        Note:
            모의계좌의 경우 output3(외화평가총액 등)만 정상 출력됩니다.
            잔고 확인을 원하실 경우에는 get_account_balance() API를 사용해주세요.
            
            온라인국가는 수수료가 반영된 최종 정산금액으로 잔고가 변동되며, 결제작업 지연으로 조회시간 차이가 발생할 수 있습니다.
            - 미국 온라인국가: 당일 장 종료후 08:40 ~ 08:45분 경
            - 애프터연장 참여 신청계좌: 10:30 ~ 10:35분 경 (Summer Time: 09:30 ~ 09:35분 경)
            
            미국은 메인 시스템이 아닌 별도 시스템을 통해 거래되므로, 18시 10~15분 이후 발생하는 미국 매매내역은 실시간 반영되지 않습니다.
            - 일반/통합증거금 계좌: 미국장 종료 + 30분 후부터 조회 가능
            - 애프터연장 신청계좌: 실시간 반영 (단, 시스템정산작업시간(23:40~00:10) 제외)
            
        Returns:
            Dict: 체결기준현재잔고 정보를 담은 딕셔너리
                - output1: 체결기준 잔고 (실전계좌만 제공)
                    - prdt_name: 종목명
                    - cblc_qty13: 결제보유수량
                    - thdt_buy_ccld_qty1: 당일매수체결수량
                    - thdt_sll_ccld_qty1: 당일매도체결수량
                    - ccld_qty_smtl1: 체결기준 현재 보유수량
                    - ord_psbl_qty1: 주문가능수량
                    - frcr_pchs_amt: 외화매입금액
                    - frcr_evlu_amt2: 외화평가금액
                    - evlu_pfls_amt2: 평가손익금액
                    - evlu_pfls_rt1: 평가손익율
                    - pdno: 종목코드
                    - bass_exrt: 기준환율
                    - buy_crcy_cd: 매수통화코드
                    - ovrs_now_pric1: 해외현재가격
                    - avg_unpr3: 평균단가
                    - tr_mket_name: 거래시장명
                    - natn_kor_name: 국가한글명
                    - loan_rmnd: 대출잔액
                    - loan_dt: 대출일자
                    - loan_expd_dt: 대출만기일자
                    - ovrs_excg_cd: 해외거래소코드
                - output2: 통화별 잔고 (실전계좌만 제공)
                    - crcy_cd: 통화코드
                    - crcy_cd_name: 통화코드명
                    - frcr_buy_amt_smtl: 외화매수금액합계
                    - frcr_sll_amt_smtl: 외화매도금액합계
                    - frcr_dncl_amt_2: 외화예수금액
                    - frst_bltn_exrt: 최초고시환율
                    - frcr_buy_mgn_amt: 외화매수증거금액
                    - frcr_drwg_psbl_amt_1: 외화출금가능금액
                    - nxdy_frcr_drwg_psbl_amt: 익일외화출금가능금액
                - output3: 계좌 요약 정보 (모의/실전계좌 모두 제공)
                    - pchs_amt_smtl: 매입금액합계 (원화환산)
                    - evlu_amt_smtl: 평가금액합계 (원화환산)
                    - evlu_pfls_amt_smtl: 평가손익금액합계 (원화환산)
                    - tot_asst_amt: 총자산금액
                    - frcr_evlu_tota: 외화평가총액
                    - evlu_erng_rt1: 평가수익율
                    - tot_evlu_pfls_amt: 총평가손익금액
                    - frcr_use_psbl_amt: 외화사용가능금액
                    - tot_loan_amt: 총대출금액
                    - tot_frcr_cblc_smtl: 총외화잔고합계
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
                "INQR_DVSN_CD": "00"  # 전체조회 (01:일반해외주식, 02:미니스탁)
            }
            
            # API 요청
            response = requests.get(url, headers=headers, params=params)
            time.sleep(self.api_call_interval)
            
            # 응답 확인
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0':
                    # 모의계좌인 경우 output3만 사용 가능함을 로그로 기록
                    if self.is_paper_trading:
                        logging.info("모의계좌는 output3(외화평가총액 등)만 정상 출력됩니다.")
                    
                    # 연속 조회 여부 확인
                    tr_cont = data.get('tr_cont', 'F')
                    if tr_cont in ['F', 'D', 'E']:  # 마지막 데이터
                        return data
                    else:  # 연속 데이터 있음
                        logging.warning("체결기준현재잔고 연속 데이터가 있습니다. tr_cont 처리가 필요할 수 있습니다.")
                        return data
                else:
                    logging.error(f"체결기준현재잔고 조회 실패: {data['msg1']}")
                    return None
            else:
                logging.error(f"체결기준현재잔고 조회 실패: {response.text}")
                return None
                
        except Exception as e:
            logging.error(f"체결기준현재잔고 조회 중 오류 발생: {str(e)}")
            return None
    
    def get_today_executed_orders(self, stock_code: str = None) -> Optional[Dict]:
        """해외주식 당일 체결내역을 조회합니다.
        
        Args:
            stock_code (str, optional): 종목코드 (종목코드.거래소 형식). None인 경우 전체 종목 조회.
            
        Returns:
            Dict: 당일 체결내역 정보를 담은 딕셔너리
                - output: 당일 체결내역 리스트
                    - ord_dt: 주문일자
                    - ord_gno_brno: 주문채번지점번호
                    - odno: 주문번호
                    - orgn_odno: 원주문번호
                    - sll_buy_dvsn_cd: 매도매수구분코드 (01:매도, 02:매수)
                    - sll_buy_dvsn_cd_name: 매도매수구분코드명
                    - rvse_cncl_dvsn: 정정취소구분
                    - rvse_cncl_dvsn_name: 정정취소구분명
                    - pdno: 상품번호
                    - prdt_name: 상품명
                    - ft_ord_qty: FT주문수량
                    - ft_ord_unpr3: FT주문단가3
                    - ft_ccld_qty: FT체결수량
                    - ft_ccld_unpr3: FT체결단가3
                    - ft_ccld_amt3: FT체결금액3
                    - nccs_qty: 미체결수량
                    - prcs_stat_name: 처리상태명
                    - rjct_rson: 거부사유
                    - ord_tmd: 주문시각
                    - tr_mket_name: 거래시장명
                    - tr_natn: 거래국가
                    - tr_natn_name: 거래국가명
                    - ovrs_excg_cd: 해외거래소코드
        """
        try:
            access_token = self._check_token()
            
            # API 경로 설정
            url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-ccnl"
            
            # 헤더 설정
            headers = {
                "Content-Type": "application/json; charset=utf-8",
                "authorization": f"Bearer {access_token}",
                "appkey": self.api_key,
                "appsecret": self.api_secret,
                "tr_id": "VTTS3035R" if self.is_paper_trading else "TTTS3035R"  # 모의/실전 구분
            }
            
            # 요청 파라미터
            params = {
                "CANO": self.account_no[:8],
                "ACNT_PRDT_CD": self.account_no[8:],
                "PDNO": "%" if stock_code else "%",  # 전종목일 경우 "%" 입력 (모의투자는 ""만 가능)
                "ORD_STRT_DT": (datetime.now() - timedelta(hours=13)).strftime("%Y%m%d"),  # 미국 현지 시각 기준 당일 날짜 (한국시간 - 13시간)
                "ORD_END_DT": (datetime.now() - timedelta(hours=13)).strftime("%Y%m%d"),   # 미국 현지 시각 기준 당일 날짜 (한국시간 - 13시간)
                "SLL_BUY_DVSN": "00",  # 전체 (00:전체, 01:매도, 02:매수)
                "CCLD_NCCS_DVSN": "00",  # 전체 (00:전체, 01:체결, 02:미체결)
                "OVRS_EXCG_CD": "%" if stock_code else "%",  # 전종목일 경우 "%" 입력
                "SORT_SQN": "DS",  # 정렬순서 (DS:정순, AS:역순)
                "ORD_DT": "",  # Null 값 설정
                "ORD_GNO_BRNO": "",  # Null 값 설정
                "ODNO": "",  # Null 값 설정
                "CTX_AREA_NK200": "",  # 연속조회키
                "CTX_AREA_FK200": ""   # 연속조회검색조건
            }
            
            # 모의투자계좌의 경우 일부 파라미터 조정
            if self.is_paper_trading:
                params["PDNO"] = ""  # 모의투자는 전체 조회만 가능
                params["OVRS_EXCG_CD"] = ""  # 모의투자는 전체 조회만 가능
                params["SLL_BUY_DVSN"] = "00"  # 모의투자는 전체 조회만 가능
                params["CCLD_NCCS_DVSN"] = "00"  # 모의투자는 전체 조회만 가능
            
            # API 요청
            response = requests.get(url, headers=headers, params=params)
            time.sleep(self.api_call_interval)
            
            # 응답 확인
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0':
                    # 연속 조회 필요 여부 확인
                    tr_cont = data.get('tr_cont', 'D')
                    
                    # 연속 조회가 필요한 경우 (tr_cont가 M인 경우)
                    if tr_cont == 'M':
                        next_data = self._get_remaining_executed_orders(
                            url, headers, params,
                            data['ctx_area_fk200'],
                            data['ctx_area_nk200']
                        )
                        if next_data:
                            # output 리스트 합치기
                            data['output'].extend(next_data.get('output', []))
                    
                    return data
                else:
                    self.logger.error(f"당일 체결내역 조회 실패: {data['msg1']}")
                    return None
            else:
                self.logger.error(f"당일 체결내역 조회 실패: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"당일 체결내역 조회 중 오류 발생: {str(e)}")
            return None
    
    def _get_remaining_executed_orders(self, url: str, headers: dict, params: dict, ctx_area_fk200: str, ctx_area_nk200: str) -> Optional[Dict]:
        """연속 조회가 필요한 경우 나머지 체결 내역을 조회합니다."""
        params['CTX_AREA_FK200'] = ctx_area_fk200
        params['CTX_AREA_NK200'] = ctx_area_nk200
        
        response = requests.get(url, headers=headers, params=params)
        time.sleep(self.api_call_interval)
        
        if response.status_code == 200:
            data = response.json()
            if data['rt_cd'] == '0':  # 정상 응답
                # 연속 조회가 필요한 경우 재귀 호출
                if data.get('tr_cont', 'D') == 'M':
                    next_data = self._get_remaining_executed_orders(
                        url, headers, params,
                        data['ctx_area_fk200'],
                        data['ctx_area_nk200']
                    )
                    if next_data:
                        # output 리스트 합치기
                        data['output'].extend(next_data.get('output', []))
                return data
            else:
                self.logger.error(f"연속 체결 내역 조회 실패: {data['msg1']}")
                return None
        else:
            self.logger.error(f"연속 체결 내역 조회 실패: {response.text}")
            return None 
