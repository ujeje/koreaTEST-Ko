import os
import yaml
import pandas as pd
import logging
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

class GoogleSheetManager:
    """구글 스프레드시트 관리자 클래스"""
    
    def __init__(self, config_path: str):
        """
        Args:
            config_path (str): 설정 파일 경로
        """
        self.logger = logging.getLogger('google_sheet_manager')
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
            self.logger.info("설정 파일을 로드했습니다: %s", config_path)
        
        self.spreadsheet_id = self.config['google_sheet']['spreadsheet_id']
        self.sheets = self.config['google_sheet']['sheets']
        self.coordinates = self.config['google_sheet']['coordinates']
        
        # 서비스 계정 인증
        creds_path = self.config['google_sheet']['credentials_path']
        creds = ServiceAccountCredentials.from_service_account_file(
            creds_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        self.logger.info("구글 서비스 계정 인증이 완료되었습니다.")
        
        # 스프레드시트 서비스 생성
        self.service = build('sheets', 'v4', credentials=creds)
        self.sheet = self.service.spreadsheets()
        self.logger.info("스프레드시트 서비스가 초기화되었습니다. (Spreadsheet ID: %s)", self.spreadsheet_id)
        
        # 디스코드 웹훅 URL
        self.discord_webhook_url = self.config['discord']['webhook_url']
    
    def get_settings(self, market_type: str = "KOR") -> dict:
        """구글 스프레드시트에서 설정값을 가져옵니다.
        
        Args:
            market_type (str): 시장 유형 (KOR/USA)
            
        Returns:
            dict: 설정값 딕셔너리
            
        Raises:
            Exception: 설정 로드 중 오류 발생 시
        """
        try:
            self.logger.info(f"{market_type} 시장 설정을 로드합니다.")
            
            # 시장 유형에 따른 설정 시트 선택
            if market_type == "KOR":
                settings_sheet = self.config['google_sheet']['sheets']['settings_kr']
            elif market_type == "USA":
                settings_sheet = self.config['google_sheet']['sheets']['settings_us']
            else:
                raise ValueError(f"지원하지 않는 시장 유형입니다: {market_type}")
            
            # 설정값 범위 지정
            ranges = [
                f"{settings_sheet}!{self.coordinates['settings']['max_individual_stocks']}",
                f"{settings_sheet}!{self.coordinates['settings']['max_pool_stocks']}",
                f"{settings_sheet}!{self.coordinates['settings']['stop_loss']}",
                f"{settings_sheet}!{self.coordinates['settings']['trailing_start']}",
                f"{settings_sheet}!{self.coordinates['settings']['trailing_stop']}",
                f"{settings_sheet}!{self.coordinates['settings']['rebalancing_date']}",
            ]

            # 설정값 조회
            result = self.service.spreadsheets().values().batchGet(
                spreadsheetId=self.spreadsheet_id,
                ranges=ranges
            ).execute()
            
            value_ranges = result.get('valueRanges', [])
            
            # 설정값 파싱
            settings = {
                # 'max_individual_stocks': int(value_ranges[0]['values'][0][0]) if value_ranges[0].get('values') else 5,
                # 'max_pool_stocks': int(value_ranges[1]['values'][0][0]) if value_ranges[1].get('values') else 5,
                'max_individual_stocks': 99,
                'max_pool_stocks': 99,
                'stop_loss': float(value_ranges[2]['values'][0][0]) if value_ranges[2].get('values') else 5.0,
                'trailing_start': float(value_ranges[3]['values'][0][0]) if value_ranges[3].get('values') else 10.0,
                'trailing_stop': float(value_ranges[4]['values'][0][0]) if value_ranges[4].get('values') else 5.0,
                'rebalancing_date': value_ranges[5]['values'][0][0] if value_ranges[5].get('values') else "",
            }

            return settings
            
        except Exception as e:
            error_msg = f"설정 로드 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            from discord_webhook import DiscordWebhook
            webhook = DiscordWebhook(url=self.discord_webhook_url, content=f"```diff\n- {error_msg}\n```")
            webhook.execute()
            raise Exception(f"구글 시트에서 설정을 로드하지 못했습니다: {str(e)}")

    def _parse_date(self, date_str) -> str:
        """다양한 형식의 날짜를 MMDD 형식으로 변환합니다."""
        try:
            if pd.isna(date_str) or str(date_str).strip() == '':
                return ''
            
            # datetime 객체인 경우
            if isinstance(date_str, datetime):
                return date_str.strftime("%m%d")
            
            date_str = str(date_str).strip()
            
            # 구분자로 분리된 경우 (예: 3/15, 3-15, 3.15)
            for sep in ['/', '-', '.', ',']:
                if sep in date_str:
                    parts = date_str.split(sep)
                    if len(parts) >= 2:
                        month = str(int(parts[0])).zfill(2)  # 앞의 0 제거 후 다시 추가
                        day = str(int(parts[1])).zfill(2)    # 앞의 0 제거 후 다시 추가
                        return f"{month}{day}"
            
            # MMDD 형식인 경우
            if len(date_str) == 4 and date_str.isdigit():
                return date_str
            
            return ''
        except:
            return ''

    def _check_trading_period(self, start_date: str, end_date: str) -> bool:
        """매수 기간을 체크합니다."""
        try:
            # 날짜가 없으면 True 반환 (제한 없음)
            if not start_date or not end_date:
                return True
            
            # 현재 날짜의 월일 추출
            current_mmdd = datetime.now().strftime("%m%d")
            
            # 시작일과 종료일을 MMDD 형식으로 변환
            start_mmdd = self._parse_date(start_date)
            end_mmdd = self._parse_date(end_date)
            
            if not start_mmdd or not end_mmdd:
                return True
            
            # 시작일이 종료일보다 작거나 같은 경우 (같은 해 내에서의 기간)
            if start_mmdd <= end_mmdd:
                return start_mmdd <= current_mmdd <= end_mmdd
            
            # 시작일이 종료일보다 큰 경우 (연말에서 다음해 초까지의 기간)
            else:
                # 현재 날짜가 시작일 이후이거나 종료일 이전인 경우
                return current_mmdd >= start_mmdd or current_mmdd <= end_mmdd
                
        except Exception as e:
            self.logger.error(f"매수 기간 체크 중 오류 발생: {str(e)}")
            return True

    def get_individual_stocks(self, market_type: str = "KOR") -> pd.DataFrame:
        """개별 종목 정보를 가져옵니다.
        
        Args:
            market_type (str): 시장 유형 (KOR/USA)
            
        Returns:
            pd.DataFrame: 개별 종목 정보 데이터프레임
            
        Raises:
            Exception: 데이터 로드 중 오류 발생 시
        """
        try:
            self.logger.info(f"{market_type} 시장의 개별 종목 정보를 로드합니다...")
            
            # 시장별 설정 시트 선택
            if market_type == "KOR":
                settings_sheet = self.sheets['settings_kr']  # 투자설정[KOR]
            elif market_type == "USA":
                settings_sheet = self.sheets['settings_us']  # 투자설정[USA]
            else:
                raise ValueError(f"지원하지 않는 시장 유형입니다: {market_type}")
            
            result = self.sheet.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{settings_sheet}!{self.coordinates['settings']['individual_stocks']}"
            ).execute()
            
            values = result.get('values', [])
            columns = ['거래소', '종목코드', '종목명', '매수시작', '매수종료', '배분비율', '매수조건', '매수기준', '매수타이밍', '매수기준2', '매수설명', '매도조건', '매도기준', '매도타이밍', '매도기준2', '매도설명']
            if not values:
                error_msg = "개별 종목 시트에 데이터가 없습니다. 설정을 확인해주세요."
                self.logger.error(error_msg)
                from discord_webhook import DiscordWebhook
                webhook = DiscordWebhook(url=self.discord_webhook_url, content=f"```diff\n- {error_msg}\n```")
                webhook.execute()
                raise Exception("구글 시트에서 개별 종목 정보를 가져오지 못했습니다.")
            
            # 첫 행이 헤더인지 확인하고 처리
            if '종목코드' in values[0]:
                df = pd.DataFrame(values[1:], columns=values[0])
            else:
                df = pd.DataFrame(values, columns=columns)
            
            # 빈 데이터 처리
            df = df.dropna(subset=['종목코드'])
            df = df[df['종목코드'].str.strip() != '']
            
            # 시장 타입에 맞는 종목만 필터링
            if market_type == "KOR":
                df = df[df['거래소'] == "KOR"]
            else:  # US
                df = df[df['거래소'].isin(['NYSE', 'NASD', 'AMEX'])]
            
            # 데이터 타입 변환 및 유효성 검사
            def safe_numeric_conversion(value, default):
                try:
                    if pd.isna(value) or str(value).strip() == '':
                        return default
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            # 매수/매도 기준과 배분비율의 유효성 검사 및 변환
            df['매수기준'] = df['매수기준'].apply(lambda x: safe_numeric_conversion(x, 20))
            df['매도기준'] = df['매도기준'].apply(lambda x: safe_numeric_conversion(x, 20))
            df['배분비율'] = df['배분비율'].apply(lambda x: safe_numeric_conversion(x, 10))
            
            # 매수/매도 조건과 기준2가 없는 경우 기본값 설정
            if '매수조건' not in df.columns:
                df['매수조건'] = '종가'
            if '매도조건' not in df.columns:
                df['매도조건'] = '종가'
            if '매수기준2' not in df.columns:
                df['매수기준2'] = '일'
            if '매도기준2' not in df.columns:
                df['매도기준2'] = '일'
            if '매수타이밍' not in df.columns:
                df['매수타이밍'] = '골든구간'
            if '매도타이밍' not in df.columns:
                df['매도타이밍'] = '데드구간'
            
            # 매수 기간이 유효한 종목만 필터링
            valid_period = df.apply(lambda x: self._check_trading_period(x['매수시작'], x['매수종료']), axis=1)
            df = df[valid_period]
            
            # 컬럼 순서 재정렬
            df = df.reindex(columns=columns)
            
            return df
            
        except Exception as e:
            error_msg = f"개별 종목 로드 실패: {str(e)}\n구글 스프레드시트 '개별 종목' 설정을 확인해주세요."
            self.logger.error(error_msg)
            from discord_webhook import DiscordWebhook
            webhook = DiscordWebhook(url=self.discord_webhook_url, content=f"```diff\n- {error_msg}\n```")
            webhook.execute()
            raise Exception(f"구글 시트에서 개별 종목 정보를 로드하지 못했습니다: {str(e)}")
    
    def get_pool_stocks(self, market_type: str = "KOR") -> pd.DataFrame:
        """POOL 종목 정보를 가져옵니다.
        
        Args:
            market_type (str): 시장 유형 (KOR/USA)
            
        Returns:
            pd.DataFrame: POOL 종목 정보 데이터프레임
            
        Raises:
            Exception: 데이터 로드 중 오류 발생 시
        """
        try:
            self.logger.info(f"{market_type} 시장의 POOL 종목 정보를 로드합니다...")
            
            # 시장별 설정 시트 선택
            if market_type == "KOR":
                settings_sheet = self.sheets['settings_kr']  # 투자설정[KOR]
            elif market_type == "USA":
                settings_sheet = self.sheets['settings_us']  # 투자설정[USA]
            else:
                raise ValueError(f"지원하지 않는 시장 유형입니다: {market_type}")
            
            result = self.sheet.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{settings_sheet}!{self.coordinates['settings']['pool_stocks']}"
            ).execute()
            
            values = result.get('values', [])
            columns = ['거래소', '종목코드', '종목명', '매수시작', '매수종료', '배분비율', '매수조건', '매수기준', '매수타이밍', '매수기준2', '매수설명', '매도조건', '매도기준', '매도타이밍', '매도기준2', '매도설명']
            if not values:
                error_msg = "POOL 종목 시트에 데이터가 없습니다. 설정을 확인해주세요."
                self.logger.error(error_msg)
                from discord_webhook import DiscordWebhook
                webhook = DiscordWebhook(url=self.discord_webhook_url, content=f"```diff\n- {error_msg}\n```")
                webhook.execute()
                raise Exception("구글 시트에서 POOL 종목 정보를 가져오지 못했습니다.")
            
            # 첫 행이 헤더인지 확인하고 처리
            if '종목코드' in values[0]:
                df = pd.DataFrame(values[1:], columns=values[0])
            else:
                df = pd.DataFrame(values, columns=columns)
            
            # 빈 데이터 처리
            df = df.dropna(subset=['종목코드'])
            df = df[df['종목코드'].str.strip() != '']
            
            # 시장 타입에 맞는 종목만 필터링
            if market_type == "KOR":
                df = df[df['거래소'] == "KOR"]
            else:  # US
                df = df[df['거래소'].isin(['NYSE', 'NAS', 'AMEX'])]
            
            # 데이터 타입 변환 및 유효성 검사
            def safe_numeric_conversion(value, default):
                try:
                    if pd.isna(value) or str(value).strip() == '':
                        return default
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            # 매수/매도 기준과 배분비율의 유효성 검사 및 변환
            df['매수기준'] = df['매수기준'].apply(lambda x: safe_numeric_conversion(x, 20))
            df['매도기준'] = df['매도기준'].apply(lambda x: safe_numeric_conversion(x, 20))
            df['배분비율'] = df['배분비율'].apply(lambda x: safe_numeric_conversion(x, 10))
            
            # 매수/매도 조건과 기준2가 없는 경우 기본값 설정
            if '매수조건' not in df.columns:
                df['매수조건'] = '종가'
            if '매도조건' not in df.columns:
                df['매도조건'] = '종가'
            if '매수기준2' not in df.columns:
                df['매수기준2'] = '일'
            if '매도기준2' not in df.columns:
                df['매도기준2'] = '일'
            if '매수타이밍' not in df.columns:
                df['매수타이밍'] = '골든구간'
            if '매도타이밍' not in df.columns:
                df['매도타이밍'] = '데드구간'
            
            # 매수 기간이 유효한 종목만 필터링
            valid_period = df.apply(lambda x: self._check_trading_period(x['매수시작'], x['매수종료']), axis=1)
            df = df[valid_period]
            
            # 컬럼 순서 재정렬
            df = df.reindex(columns=columns)
            
            return df
            
        except Exception as e:
            error_msg = f"POOL 종목 로드 실패: {str(e)}\n구글 스프레드시트 'POOL 종목' 설정을 확인해주세요."
            self.logger.error(error_msg)
            from discord_webhook import DiscordWebhook
            webhook = DiscordWebhook(url=self.discord_webhook_url, content=f"```diff\n- {error_msg}\n```")
            webhook.execute()
            raise Exception(f"구글 시트에서 POOL 종목 정보를 로드하지 못했습니다: {str(e)}")
    
    def update_last_update_time(self, value: str, holdings_sheet: str) -> None:
        """마지막 업데이트 시간을 갱신합니다."""
        try:
            self.update_cell(
                f"{holdings_sheet}!{self.coordinates['holdings']['last_update']}", 
                value
            )
        except Exception as e:
            self.logger.error(f"마지막 업데이트 시간 갱신 실패: {str(e)}")
    
    def update_error_message(self, value: str, holdings_sheet: str) -> None:
        """에러 메시지를 갱신합니다."""
        try:
            self.update_cell(
                f"{holdings_sheet}!{self.coordinates['holdings']['error_message']}", 
                value
            )
        except Exception as e:
            self.logger.error(f"에러 메시지 갱신 실패: {str(e)}")
    
    def update_holdings(self, values: list, holdings_sheet: str) -> None:
        """보유 종목 리스트를 갱신합니다."""
        try:
            # 보유 종목 리스트 영역 초기화
            range_name = f"{holdings_sheet}!{self.coordinates['holdings']['stock_list']}"
            self.sheet.values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            self.logger.debug(f"보유 종목 리스트 영역 초기화 완료: {range_name}")
            
            # 새로운 보유 종목 리스트 업데이트
            self.update_range(range_name, values)
        except Exception as e:
            self.logger.error(f"보유 종목 리스트 갱신 실패: {str(e)}")
        
    def update_cell(self, range_name: str, value: str) -> None:
        """특정 셀의 값을 업데이트합니다."""
        try:
            self.logger.info("셀 업데이트를 시작합니다. (범위: %s, 값: %s)", range_name, value)
            body = {
                'values': [[value]]
            }
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            self.logger.info("셀 업데이트가 완료되었습니다.")
            
        except HttpError as e:
            self.logger.error("셀 업데이트 실패: %s", str(e))
            raise Exception(f"셀 업데이트 실패: {str(e)}")
    
    def update_range(self, range_name: str, values: list) -> None:
        """특정 범위의 값을 업데이트합니다."""
        try:
            self.logger.info("범위 업데이트를 시작합니다. (범위: %s)", range_name)
            body = {
                'values': values
            }
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            self.logger.info("범위 업데이트가 완료되었습니다.")
            
        except HttpError as e:
            self.logger.error("범위 업데이트 실패: %s", str(e))
            raise Exception(f"범위 업데이트 실패: {str(e)}")
    
    def clear_range(self, range_name: str) -> None:
        """특정 범위의 값을 지웁니다."""
        try:
            self.logger.info("범위 지우기를 시작합니다. (범위: %s)", range_name)
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            self.logger.info("범위 지우기가 완료되었습니다.")
            
        except HttpError as e:
            self.logger.error("범위 지우기 실패: %s", str(e))
            raise Exception(f"범위 지우기 실패: {str(e)}")
            
    def get_cell_value(self, cell: str, market_type: str = "KOR") -> str:
        """특정 셀의 값을 가져옵니다.
        
        Args:
            cell (str): 셀 주소 (예: "G8")
            market_type (str): 시장 유형 (KOR/USA)
            
        Returns:
            str: 셀 값 (없으면 빈 문자열)
        """
        try:
            # 시장 유형에 따른 설정 시트 선택
            if market_type == "KOR":
                settings_sheet = self.config['google_sheet']['sheets']['settings_kr']
            elif market_type == "USA":
                settings_sheet = self.config['google_sheet']['sheets']['settings_us']
            else:
                raise ValueError(f"지원하지 않는 시장 유형입니다: {market_type}")
                
            # 셀 범위 지정
            range_name = f"{settings_sheet}!{cell}"
            
            # 셀 값 조회
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                self.logger.debug(f"셀 {cell}에 값이 없습니다.")
                return ""
                
            return str(values[0][0]) if values[0] else ""
            
        except HttpError as error:
            self.logger.error(f"셀 값 조회 실패: {error}")
            return ""
        except Exception as e:
            self.logger.error(f"셀 값 조회 중 오류 발생: {e}")
            return "" 