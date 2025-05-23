import os
import yaml
import requests
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import pandas as pd
from src.utils.token_manager import TokenManager
import time

class KISKRAPIManager:
    """한국투자증권 국내주식 API 매니저
    
    국내 주식 거래와 관련된 모든 API 호출을 담당하는 클래스입니다.
    실전투자와 모의투자를 모두 지원합니다.
    """
    
    def __init__(self, config_path: str):
        """API 매니저를 초기화합니다.
        
        Args:
            config_path (str): 설정 파일 경로
        """
        self.logger = logging.getLogger('kr_api')
        self.token_manager = TokenManager(config_path)
        self.config = self.token_manager.config
        
        # 실전/모의투자 설정
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
        
        self.logger.info(f"국내주식 API 매니저 초기화 완료 (모의투자: {self.is_paper_trading}, 계좌유형: {self.account_type})")
    
    def _get_headers(self, tr_id: str) -> Dict:
        """API 요청에 사용할 헤더를 생성합니다.
        
        Args:
            tr_id (str): TR ID
            
        Returns:
            Dict: API 요청 헤더
        """
        access_token = self._check_token()
        
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": tr_id
        }
        
        # 법인계좌인 경우 추가 헤더 설정
        if self.account_type == "C":  # C: 법인
            headers.update({
                "custtype": "B"  # B: 법인
            })
        
        return headers
    
    def _check_token(self) -> str:
        """토큰의 유효성을 확인하고 필요시 갱신합니다."""
        return self.token_manager.get_token()
    
    def get_stock_price(self, stock_code: str) -> Optional[Dict]:
        """주식 현재가 정보를 조회합니다."""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._get_headers("FHKST01010100")
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code
        }
        
        response = requests.get(url, headers=headers, params=params)
        time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
        
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"주가 조회 실패: {response.text}")
            return None
    
    def get_today_executed_orders(self, stock_code: str = None) -> Optional[Dict]:
        """당일 체결된 주문 내역을 조회합니다.
        
        Args:
            stock_code (str, optional): 종목코드. 지정하지 않으면 모든 종목의 체결 내역을 조회합니다.
            
        Returns:
            Dict: 당일 체결 내역 정보를 담은 딕셔너리
                - output1: 체결 내역 리스트
                    - ord_dt: 주문일자
                    - ord_gno_brno: 주문채번지점번호
                    - odno: 주문번호
                    - orgn_odno: 원주문번호
                    - ord_dvsn_name: 주문구분명
                    - sll_buy_dvsn_cd: 매도매수구분코드 (01:매도, 02:매수)
                    - sll_buy_dvsn_cd_name: 매도매수구분코드명
                    - pdno: 상품번호 (종목코드)
                    - prdt_name: 상품명 (종목명)
                    - ord_qty: 주문수량
                    - ord_unpr: 주문단가
                    - ord_tmd: 주문시각
                    - tot_ccld_qty: 총체결수량
                    - avg_prvs: 평균가 (총체결금액 / 총체결수량)
                    - cncl_yn: 취소여부
                    - tot_ccld_amt: 총체결금액
                    - loan_dt: 대출일자
                    - ord_dvsn_cd: 주문구분코드
                - output2: 응답상세2
                    - tot_ord_qty: 총주문수량
                    - tot_ccld_qty: 총체결수량
                    - pchs_avg_pric: 매입평균가격
                    - tot_ccld_amt: 총체결금액
                    - prsm_tlex_smtl: 추정제비용합계
        """
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        headers = self._get_headers("VTTC8001R" if self.is_paper_trading else "TTTC8001R")
        
        params = {
            "CANO": self.account_no[:8],                          # 종합계좌번호
            "ACNT_PRDT_CD": self.account_no[8:],                 # 계좌상품코드
            "INQR_STRT_DT": datetime.now().strftime("%Y%m%d"),   # 조회시작일자
            "INQR_END_DT": datetime.now().strftime("%Y%m%d"),    # 조회종료일자
            "SLL_BUY_DVSN_CD": "00",  # 매도매수구분코드 (00:전체, 01:매도, 02:매수)
            "INQR_DVSN": "00",        # 조회구분 (00:역순, 01:정순)
            "PDNO": stock_code if stock_code else "",  # 상품번호 (종목코드, 공란:전체)
            "CCLD_DVSN": "00",        # 체결구분 (00:전체, 01:체결, 02:미체결)
            "ORD_GNO_BRNO": "",       # 주문채번지점번호 (Null 값 설정)
            "ODNO": "",               # 주문번호 (Null 값 설정)
            "INQR_DVSN_3": "00",      # 조회구분3 (00:전체, 01:현금, 02:융자, 03:대출, 04:대주)
            "INQR_DVSN_1": "",        # 조회구분1 (공란:전체, 1:ELW, 2:프리보드)
            "CTX_AREA_FK100": "",     # 연속조회검색조건
            "CTX_AREA_NK100": ""      # 연속조회키
        }
        
        response = requests.get(url, headers=headers, params=params)
        time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
        
        if response.status_code == 200:
            data = response.json()
            if data['rt_cd'] == '0':  # 정상 응답
                # 연속조회 필요 여부 확인
                tr_cont = data.get('tr_cont', 'D')
                
                # 연속 조회가 필요한 경우 (tr_cont가 M인 경우)
                if tr_cont == 'M':
                    next_data = self._get_remaining_executed_orders(
                        url, headers, params,
                        data['ctx_area_fk100'],
                        data['ctx_area_nk100']
                    )
                    if next_data:
                        # output1(체결내역) 리스트 합치기
                        data['output1'].extend(next_data.get('output1', []))
                
                return data
            else:
                self.logger.error(f"체결 내역 조회 실패: {data['msg1']}")
                return None
        else:
            self.logger.error(f"체결 내역 조회 실패: {response.text}")
            return None
    
    def _get_remaining_executed_orders(self, url: str, headers: dict, params: dict, ctx_area_fk100: str, ctx_area_nk100: str) -> Optional[Dict]:
        """연속 조회가 필요한 경우 나머지 체결 내역을 조회합니다."""
        params['CTX_AREA_FK100'] = ctx_area_fk100
        params['CTX_AREA_NK100'] = ctx_area_nk100
        
        response = requests.get(url, headers=headers, params=params)
        time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
        
        if response.status_code == 200:
            data = response.json()
            if data['rt_cd'] == '0':  # 정상 응답
                # 연속 조회가 필요한 경우 재귀 호출
                if data.get('tr_cont', 'D') == 'M':
                    next_data = self._get_remaining_executed_orders(
                        url, headers, params,
                        data['ctx_area_fk100'],
                        data['ctx_area_nk100']
                    )
                    if next_data:
                        # output1(체결내역) 리스트 합치기
                        data['output1'].extend(next_data.get('output1', []))
                return data
            else:
                self.logger.error(f"연속 체결 내역 조회 실패: {data['msg1']}")
                return None
        else:
            self.logger.error(f"연속 체결 내역 조회 실패: {response.text}")
            return None
    
    def get_account_balance(self) -> Optional[Dict]:
        """계좌 잔고를 조회합니다.
        
        Returns:
            Dict: 계좌 잔고 정보를 담은 딕셔너리
                - output1: 보유 종목 리스트
                    - pdno: 종목코드
                    - prdt_name: 종목명
                    - hldg_qty: 보유수량
                    - ord_psbl_qty: 주문가능수량
                    - pchs_avg_pric: 매입평균가격
                    - pchs_amt: 매입금액
                    - prpr: 현재가
                    - evlu_amt: 평가금액
                    - evlu_pfls_amt: 평가손익금액
                    - evlu_pfls_rt: 평가손익율
                - output2: 계좌 요약 정보
                    - dnca_tot_amt: 예수금총금액
                    - nxdy_excc_amt: D+1 예수금
                    - prvs_rcdl_excc_amt: D+2 예수금
                    - tot_evlu_amt: 총평가금액
        """
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
        headers = self._get_headers("VTTC8434R" if self.is_paper_trading else "TTTC8434R")
        
        params = {
            "CANO": self.account_no[:8],
            "ACNT_PRDT_CD": self.account_no[8:],
            "AFHR_FLPR_YN": "N",           # 시간외단일가여부
            "OFL_YN": "",                  # 오프라인여부
            "INQR_DVSN": "02",             # 조회구분 (02: 종목별)
            "UNPR_DVSN": "01",             # 단가구분
            "FUND_STTL_ICLD_YN": "N",      # 펀드결제분포함여부
            "FNCG_AMT_AUTO_RDPT_YN": "N",  # 융자금액자동상환여부
            "PRCS_DVSN": "01",             # 처리구분 (01: 전일매매미포함)
            "CTX_AREA_FK100": "",          # 연속조회검색조건
            "CTX_AREA_NK100": ""           # 연속조회키
        }
        
        response = requests.get(url, headers=headers, params=params)
        time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
        
        if response.status_code == 200:
            data = response.json()
            if data['rt_cd'] == '0':  # 정상 응답
                # 연속조회 필요 여부 확인
                tr_cont = data.get('tr_cont', 'D')
                
                # 연속 조회가 필요한 경우 (tr_cont가 M인 경우)
                if tr_cont == 'M':
                    next_data = self._get_remaining_balance(
                        url, headers, params,
                        data['ctx_area_fk100'],
                        data['ctx_area_nk100']
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
    
    def _get_remaining_balance(self, url: str, headers: dict, params: dict, ctx_area_fk100: str, ctx_area_nk100: str) -> Optional[Dict]:
        """연속 조회가 필요한 경우 나머지 잔고를 조회합니다."""
        try:
            # 연속조회 파라미터 설정
            params['CTX_AREA_FK100'] = ctx_area_fk100
            params['CTX_AREA_NK100'] = ctx_area_nk100
            
            response = requests.get(url, headers=headers, params=params)
            time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
            
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0':  # 정상 응답
                    # 더 연속 조회가 필요한 경우 재귀 호출
                    if data.get('tr_cont') == 'M':
                        next_data = self._get_remaining_balance(
                            url, headers, params,
                            data['ctx_area_fk100'],
                            data['ctx_area_nk100']
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
    
    def get_buyable_amount(self, stock_code: str) -> Optional[Dict]:
        """매수가능금액을 조회합니다.
        
        Args:
            stock_code (str): 종목코드
            
        Returns:
            Dict: 매수가능금액 정보를 담은 딕셔너리
                - output: 매수가능 정보
                    - nrcvb_buy_amt: 미수없는매수금액
                    - nrcvb_buy_qty: 미수없는매수수량
                    - max_buy_amt: 최대매수금액
                    - max_buy_qty: 최대매수수량
                    - ord_psbl_cash: 주문가능현금
        """
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
        headers = self._get_headers("VTTC8908R" if self.is_paper_trading else "TTTC8908R")
        
        params = {
            "CANO": self.account_no[:8],
            "ACNT_PRDT_CD": self.account_no[8:],
            "PDNO": stock_code,
            "ORD_UNPR": "0",
            "ORD_DVSN": "01",  # 01: 시장가 (종목증거금율 반영)
            "CMA_EVLU_AMT_ICLD_YN": "N",  # CMA 평가금액 포함안함
            "OVRS_ICLD_YN": "N"  # 해외포함안함
        }
        
        response = requests.get(url, headers=headers, params=params)
        time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
        
        if response.status_code == 200:
            data = response.json()
            if data['rt_cd'] == '0':  # 정상 응답
                return data
            else:
                logging.error(f"매수가능금액 조회 실패: {data['msg1']}")
                return None
        else:
            logging.error(f"매수가능금액 조회 실패: {response.text}")
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
                - output: 주문 결과 정보
                    - KRX_FWDG_ORD_ORGNO: 한국거래소전송주문조직번호
                    - ODNO: 주문번호
                    - ORD_TMD: 주문시각 (HHMMSS)
        """
        access_token = self._check_token()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash"
        
        # 모의/실전 구분
        if self.is_paper_trading:
            tr_id = "VTTC0802U" if order_type == "BUY" else "VTTC0801U"
        else:
            tr_id = "TTTC0802U" if order_type == "BUY" else "TTTC0801U"
            
        headers = self._get_headers(tr_id)
        
        data = {
            "CANO": self.account_no[:8],
            "ACNT_PRDT_CD": self.account_no[8:],
            "PDNO": stock_code,
            "ORD_DVSN": "00" if price > 0 else "01",  # 00: 지정가, 01: 시장가
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price) if price > 0 else "0",  # 시장가 주문 시 0으로 설정
            "ALGO_NO": ""  # 알고리즘 번호 (미사용)
        }
        
        response = requests.post(url, headers=headers, data=json.dumps(data))
        time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
        
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
    
    def get_daily_price(self, stock_code: str, start_date: str, end_date: str, period_div_code: str = "D") -> Optional[pd.DataFrame]:
        """일별/주별 주가 정보를 조회합니다.
        
        Args:
            stock_code (str): 종목코드
            start_date (str): 조회 시작일자 (YYYYMMDD)
            end_date (str): 조회 종료일자 (YYYYMMDD)
            period_div_code (str): 기간 구분 코드 (D: 일봉, W: 주봉)
            
        Returns:
            Optional[pd.DataFrame]: 일별/주별 주가 데이터프레임
        """
        try:
            access_token = self._check_token()
            
            url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
            headers = self._get_headers("FHKST03010100")
            
            params = {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": stock_code,
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
                "FID_PERIOD_DIV_CODE": period_div_code,  # D:일봉, W:주봉
                "FID_ORG_ADJ_PRC": "0"       # 수정주가 여부 (0: 수정주가, 1: 원주가)
            }
            
            # API 호출 후 대기
            response = requests.get(url, headers=headers, params=params)
            time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
            
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0' and 'output2' in data:  # 정상 응답 확인
                    df = pd.DataFrame(data['output2'])
                    # 날짜(stck_bsop_date) 기준으로 오름차순 정렬 (과거 -> 최근)
                    df['stck_bsop_date'] = pd.to_datetime(df['stck_bsop_date'], format='%Y%m%d')
                    df = df.sort_values('stck_bsop_date', ascending=True).reset_index(drop=True)
                    
                    # 컬럼명 매핑 (기존 코드와의 호환성을 위해)
                    df['종가'] = df['stck_clpr'].astype(float)
                    df['시가'] = df['stck_oprc'].astype(float)
                    df['고가'] = df['stck_hgpr'].astype(float)
                    df['저가'] = df['stck_lwpr'].astype(float)
                    df['거래량'] = df['acml_vol'].astype(float)
                    df['거래대금'] = df['acml_tr_pbmn'].astype(float)
                    
                    return df
                else:
                    error_msg = data.get('msg1', '알 수 없는 오류가 발생했습니다.')
                    logging.error(f"주가 조회 실패: {error_msg}")
            else:
                logging.error(f"주가 조회 실패: {response.text}")
            return None
            
        except Exception as e:
            logging.error(f"주가 조회 중 오류 발생: {str(e)}")
            return None

    def check_holiday(self, base_date: str = None) -> Optional[Dict]:
        """국내 휴장일 여부를 조회합니다.
        
        Args:
            base_date (str, optional): 기준일자 (YYYYMMDD). 기본값은 오늘 날짜.
            
        Returns:
            Optional[Dict]: 휴장일 정보를 담은 딕셔너리
                - bass_dt: 기준일자
                - wday_dvsn_cd: 요일구분코드 (01:일요일, 02:월요일, 03:화요일, 04:수요일, 05:목요일, 06:금요일, 07:토요일)
                - bzdy_yn: 영업일여부 (Y/N)
                - tr_day_yn: 거래일여부 (Y/N)
                - opnd_yn: 개장일여부 (Y/N)
                - sttl_day_yn: 결제일여부 (Y/N)
        """
        try:
            # 기준일자가 없으면 오늘 날짜 사용
            if base_date is None:
                base_date = datetime.now().strftime("%Y%m%d")
                
            access_token = self._check_token()
            
            url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/chk-holiday"
            headers = self._get_headers("CTCA0903R")
            
            params = {
                "BASS_DT": base_date,   # 기준일자(YYYYMMDD)
                "CTX_AREA_NK": "",      # 연속조회키
                "CTX_AREA_FK": ""       # 연속조회검색조건
            }
            
            self.logger.info(f"국내 휴장일 조회: {base_date}")
            
            # API 호출 후 대기
            response = requests.get(url, headers=headers, params=params)
            time.sleep(0.5 if self.is_paper_trading else 0.3)  # 모의투자: 0.5초, 실전투자: 0.3초
            
            if response.status_code == 200:
                data = response.json()
                if data['rt_cd'] == '0' and 'output' in data:  # 정상 응답 확인
                    return data['output']  # 휴장일 정보 반환
                else:
                    error_msg = data.get('msg1', '알 수 없는 오류가 발생했습니다.')
                    self.logger.error(f"휴장일 조회 실패: {error_msg}")
            else:
                self.logger.error(f"휴장일 조회 실패: {response.text}")
            return None
            
        except Exception as e:
            self.logger.error(f"휴장일 조회 중 오류 발생: {str(e)}")
            return None 