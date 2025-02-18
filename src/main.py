import os
import yaml
import time
import logging
import pytz
from datetime import datetime
from src.korean.kr_trader import KRTrader
from src.overseas.us_trader import USTrader
from src.utils.logger import setup_logger
from src.utils.google_sheet_manager import GoogleSheetManager

def is_korean_market_time() -> bool:
    """한국 시장 운영 시간인지 확인합니다."""
    now = datetime.now()
    current_time = now.strftime("%H%M")
    
    # 주말 체크
    if now.weekday() >= 5:  # 5: 토요일, 6: 일요일
        return False
        
    # 장 운영 시간 체크 (09:00 ~ 15:30)
    return "0900" <= current_time <= "1530"

def is_us_market_time() -> bool:
    """미국 시장 운영 시간인지 확인합니다."""
    us_tz = pytz.timezone('America/New_York')
    now = datetime.now(us_tz)
    current_time = now.strftime("%H%M")
    
    # 주말 체크
    if now.weekday() >= 5:  # 5: 토요일, 6: 일요일
        return False
        
    # 장 운영 시간 체크 (09:30 ~ 16:00)
    return "0930" <= current_time <= "1600"

def main():
    """메인 실행 함수"""
    config_path = 'config/config.yaml'
    
    # 설정 파일 로드
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 메인 로거 설정
    logger = setup_logger('MAIN', config)
    
    # 거래 시장 설정 확인
    market_type = config['trading']['market']['type']

    traders = []

    # 트레이더 초기화
    if ('KOR' in market_type):
        kr_trader = KRTrader(config_path)
        traders.append(('KOR', kr_trader))
        logger.info("한국 주식 트레이더가 초기화되었습니다.")

    if ('USA' in market_type):
        us_trader = USTrader(config_path)
        traders.append(('USA', us_trader))
        logger.info("미국 주식 트레이더가 초기화되었습니다.")

    if not traders:
        logger.error("설정된 거래 시장이 없습니다.")
        return

    # 구글 시트에서 설정 로드
    google_sheet = GoogleSheetManager('config/config.yaml')
    settings = google_sheet.get_settings()

    # 설정된 시장별 매매 조건 출력 및 시장 시간 체크
    for market, _ in traders:
        if market == 'USA':
            logger.info("\n=== 미국 시장 매매 설정 ===")
            # 미국 종목 정보 출력
            individual_stocks = google_sheet.get_individual_stocks(market_type="USA")
            pool_stocks = google_sheet.get_pool_stocks(market_type="USA")
            
            logger.info("=== 미국 개별 종목 매매 조건 ===")
            for _, row in individual_stocks.iterrows():
                logger.info(f"{row['종목명']}({row['종목코드']}): {row['매매기준']}일선 / 배분비율 {row['배분비율']}% / 매수기간 {row.get('매수시작', '제한없음')}~{row.get('매수종료', '제한없음')}")
            
            logger.info("\n=== 미국 POOL 종목 매매 조건 ===")
            for _, row in pool_stocks.iterrows():
                logger.info(f"{row['종목명']}({row['종목코드']}): {row['매매기준']}일선 / 배분비율 {row['배분비율']}%")

            # 미국 시장 시간 체크 및 대기
            if not is_us_market_time():
                logger.info("\n미국 시장은 현재 개장 전입니다. 개장까지 대기합니다...")
                while not is_us_market_time():
                    time.sleep(60)  # 1분 대기
                logger.info("미국 시장이 개장되었습니다!")

        elif market == 'KOR':
            logger.info("\n=== 한국 시장 매매 설정 ===")
            # 한국 종목 정보 출력
            individual_stocks = google_sheet.get_individual_stocks(market_type="KOR")
            pool_stocks = google_sheet.get_pool_stocks(market_type="KOR")
            
            logger.info("=== 한국 개별 종목 매매 조건 ===")
            for _, row in individual_stocks.iterrows():
                logger.info(f"{row['종목명']}({row['종목코드']}): {row['매매기준']}일선 / 배분비율 {row['배분비율']}% / 매수기간 {row.get('매수시작', '제한없음')}~{row.get('매수종료', '제한없음')}")
            
            logger.info("\n=== 한국 POOL 종목 매매 조건 ===")
            for _, row in pool_stocks.iterrows():
                logger.info(f"{row['종목명']}({row['종목코드']}): {row['매매기준']}일선 / 배분비율 {row['배분비율']}%")

            # 한국 시장 시간 체크 및 대기
            if not is_korean_market_time():
                logger.info("\n한국 시장은 현재 개장 전입니다. 개장까지 대기합니다...")
                while not is_korean_market_time():
                    time.sleep(60)  # 1분 대기
                logger.info("한국 시장이 개장되었습니다!")

    # 공통 설정 출력
    logger.info("\n=== 공통 매매 조건 설정값 ===")
    logger.info(f"최대 개별 종목 수: {settings['max_individual_stocks']}개")
    logger.info(f"최대 POOL 종목 수: {settings['max_pool_stocks']}개")
    logger.info(f"최소 현금 보유 비율: {settings['min_cash_ratio']*100}%")
    logger.info("\n=== 매수 타이밍 ===")
    logger.info(f"시가 매수 비율: {settings['market_open_ratio']*100}%")
    logger.info(f"종가 매수 비율: {settings['market_close_ratio']*100}%")
    logger.info("\n=== 스탑로스/트레일링 스탑 설정 ===")
    logger.info(f"스탑로스: {settings['stop_loss']}%")
    logger.info(f"트레일링 시작: {settings['trailing_start']}%")
    logger.info(f"트레일링 스탑: {settings['trailing_stop']}%")

    prev_session = None

    try:
        while True:
            try:
                # 현재 운영 중인 시장 확인
                is_kor_time = is_korean_market_time()
                is_us_time = is_us_market_time()

                # 두 시장 모두 설정된 경우, 세션 변경 감지
                if len(market_type) > 1:
                    active_markets = set()
                    if is_kor_time:
                        active_markets.add('KOR')
                    if is_us_time:
                        active_markets.add('USA')
                    if active_markets != prev_session:
                        logger.info("\n=== 현재 운영 중인 시장이 변경되었습니다 ===")
                        for market in active_markets:
                            logger.info(f"{market} 시장이 운영 중입니다.")
                        prev_session = active_markets

                for market, trader in traders:
                    try:
                        # 해당 시장 운영 시간에만 매매 실행
                        if (market == 'KOR' and is_kor_time) or (market == 'USA' and is_us_time):
                            # 매매 실행
                            trader.execute_trade()
                            # 주식 현황 업데이트 (매 루프마다)
                            trader.update_stock_report()
                        else:
                            if market == 'KOR':
                                logger.debug("현재 한국 시장 운영 시간이 아닙니다.")
                            else:
                                logger.debug("현재 미국 시장 운영 시간이 아닙니다.")
                                
                    except Exception as e:
                        error_msg = f"트레이딩 중 오류 발생: {str(e)}"
                        logger.error(error_msg)
                        trader.send_discord_message(error_msg, error=True)
                
                # 1분 대기
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"메인 루프 실행 중 오류 발생: {str(e)}")
                time.sleep(60)  # 오류 발생시에도 1분 대기
            
    except KeyboardInterrupt:
        # 프로그램 종료 메시지
        for _, trader in traders:
            trader.send_discord_message("자동매매 프로그램이 종료되었습니다.")
        logger.info("프로그램이 종료되었습니다.")

if __name__ == "__main__":
    main() 