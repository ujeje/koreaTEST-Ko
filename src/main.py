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
        
    # 장 운영 시간 체크 (미국 현지 시간 기준 09:30 ~ 16:00)
    return "0930" <= current_time <= "1600"

def print_trading_settings(logger, market: str, trader) -> None:
    """트레이딩 설정을 출력합니다."""
    logger.info(f"\n=== {market} 시장 매매 설정 ===")
    logger.info("=== 개별 종목 매매 조건 ===")
    for _, row in trader.individual_stocks.iterrows():
        logger.info(f"{row['종목명']}({row['종목코드']}): {row['매매기준']}일선 / 배분비율 {row['배분비율']}% / 매수기간 {row.get('매수시작', '제한없음')}~{row.get('매수종료', '제한없음')}")
    
    logger.info("\n=== POOL 종목 매매 조건 ===")
    for _, row in trader.pool_stocks.iterrows():
        logger.info(f"{row['종목명']}({row['종목코드']}): {row['매매기준']}일선 / 배분비율 {row['배분비율']}%")
    
    logger.info("\n=== 공통 매매 조건 설정값 ===")
    logger.info(f"최대 개별 종목 수: {trader.settings['max_individual_stocks']}개")
    logger.info(f"최대 POOL 종목 수: {trader.settings['max_pool_stocks']}개")
    
    logger.info("\n=== 매수 타이밍 ===")
    if market == "KOR":
        # 한국 시장은 매수/매도 시간 출력
        logger.info(f"매수 시간: {trader.settings['buy_time']}")
        logger.info(f"매도 시간: {trader.settings['sell_time']}")
    else:
        # 미국 시장은 시가/종가 매수 비율 출력
        logger.info(f"시가 매수 비율: {trader.settings['market_open_ratio']*100}%")
        logger.info(f"종가 매수 비율: {trader.settings['market_close_ratio']*100}%")
    
    logger.info("\n=== 스탑로스/트레일링 스탑 설정 ===")
    logger.info(f"스탑로스: {trader.settings['stop_loss']}%")
    logger.info(f"트레일링 시작: {trader.settings['trailing_start']}%")
    logger.info(f"트레일링 스탑: {trader.settings['trailing_stop']}%")

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

    traders = {}

    # 트레이더 초기화 (아직 설정은 로드하지 않음)
    if ('KOR' in market_type):
        kr_trader = KRTrader(config_path)
        traders['KOR'] = kr_trader
        logger.info("한국 주식 트레이더가 초기화되었습니다.")

    if ('USA' in market_type):
        us_trader = USTrader(config_path)
        traders['USA'] = us_trader
        logger.info("미국 주식 트레이더가 초기화되었습니다.")

    if not traders:
        logger.error("설정된 거래 시장이 없습니다.")
        return

    # 현재 활성화된 시장 추적
    active_markets = set()
    prev_active_markets = set()

    try:
        while True:
            try:
                # 현재 운영 중인 시장 확인
                is_kor_time = is_korean_market_time()
                is_us_time = is_us_market_time()
                
                # 현재 활성화된 시장 설정
                current_active_markets = set()
                if is_kor_time and 'KOR' in traders:
                    current_active_markets.add('KOR')
                if is_us_time and 'USA' in traders:
                    current_active_markets.add('USA')
                
                # 활성화된 시장이 변경되었는지 확인
                if current_active_markets != active_markets:
                    # 새로 열린 시장 확인
                    new_markets = current_active_markets - active_markets
                    # 닫힌 시장 확인
                    closed_markets = active_markets - current_active_markets
                    
                    # 닫힌 시장 처리
                    for market in closed_markets:
                        logger.info(f"\n=== {market} 시장이 종료되었습니다 ===")
                    
                    # 새로 열린 시장 처리
                    for market in new_markets:
                        logger.info(f"\n=== {market} 시장이 개장되었습니다 ===")
                        # 설정 로드
                        traders[market].load_settings()
                        logger.info(f"{market} 시장의 설정을 로드했습니다.")
                        # 설정 출력
                        #print_trading_settings(logger, market, traders[market])
                    
                    # 활성화된 시장 업데이트
                    active_markets = current_active_markets
                
                # 활성화된 시장이 있는 경우에만 매매 실행
                if active_markets:
                    for market in active_markets:
                        try:
                            # 매매 실행
                            traders[market].execute_trade()
                            # 주식 현황 업데이트
                            traders[market].update_stock_report()
                        except Exception as e:
                            logger.error(f"{market} 시장 매매 실행 중 오류 발생: {str(e)}")
                else:
                    logger.info("현재 운영 중인 시장이 없습니다. 대기 중...")
                
                # 대기
                # time.sleep(5)
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"메인 루프 실행 중 오류 발생: {str(e)}")
                time.sleep(30)
                
    except KeyboardInterrupt:
        logger.info("프로그램이 종료되었습니다.")

if __name__ == "__main__":
    main() 