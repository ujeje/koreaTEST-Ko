# 주식 자동매매 시스템

이 프로젝트는 한국과 미국 주식 시장에서 자동으로 매매를 수행하는 시스템입니다.

## 주요 기능

- 한국/미국 주식 시장 자동 매매
- 구글 스프레드시트를 통한 설정 관리
- 디스코드를 통한 실시간 알림
- 스탑로스/트레일링 스탑 자동 실행
- 시가/종가 분할 매수 전략

## 설치 방법

1. 저장소 클론
```bash
git clone [repository_url]
cd [repository_name]
```

2. 가상환경 생성 및 활성화
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. 필요한 패키지 설치
```bash
pip install -r requirements.txt
```

4. 설정 파일 구성
- `config/config.yaml` 파일에서 필요한 설정을 구성
- 구글 API 인증 정보 설정 (`config/google_credentials.json`)
- 한국투자증권 API 키 설정
- 디스코드 웹훅 URL 설정

## 실행 방법

```bash
python -m src.main
```

## 설정 파일 구성

`config/config.yaml` 파일에서 다음 설정들을 구성할 수 있습니다:

- API 설정 (한국투자증권)
- 디스코드 웹훅 설정
- 구글 스프레드시트 설정
- 매매 설정
  - 최대 보유 종목 수
  - 종목당 최대 투자금액
  - 손절/익절 기준
  - 시장별 거래 시간
- 로깅 설정

## 프로젝트 구조

```
src/
├── common/         # 공통 모듈
├── korean/        # 한국 주식 관련 모듈
├── overseas/      # 해외 주식 관련 모듈
├── utils/         # 유틸리티 모듈
└── main.py        # 메인 실행 파일
```

## 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다. 