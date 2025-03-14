# 주식 트레이딩 프로그램 배포 가이드

이 문서는 주식 트레이딩 프로그램을 실행 파일로 배포하는 방법을 설명합니다.

## 실행 파일 배포 방법

### 1. 필요한 패키지 설치

먼저 PyInstaller를 설치합니다:

```bash
pip install pyinstaller
```

### 2. 실행 파일 빌드

`build_exe.py` 스크립트를 실행하여 실행 파일을 생성합니다:

```bash
python build_exe.py
```

이 스크립트는 다음 작업을 수행합니다:
- PyInstaller를 사용하여 `src/main.py`를 실행 파일로 변환
- 필요한 모든 종속성 패키지를 포함
- 설정 파일과 로그 디렉토리 생성

### 3. 배포 파일 구조

빌드가 완료되면 `dist` 폴더에 다음과 같은 구조로 파일이 생성됩니다:

```
dist/
├── StockTrader.exe     # 실행 파일
├── config/             # 설정 파일 디렉토리
│   └── config.yaml     # 설정 파일
└── logs/               # 로그 파일 디렉토리
```

### 4. 구글 API 인증 파일 설정

구글 스프레드시트 API를 사용하기 위해 인증 파일을 설정해야 합니다:

1. 구글 클라우드 콘솔에서 서비스 계정 키를 다운로드
2. `dist/config/` 디렉토리에 인증 파일(credentials.json)을 복사
3. `config.yaml` 파일에서 인증 파일 경로를 올바르게 설정

### 5. 실행 파일 실행

배포된 실행 파일을 실행하려면:

1. `dist` 폴더로 이동
2. `StockTrader.exe`를 실행

### 6. 자동 실행 설정 (선택 사항)

Windows에서 시스템 시작 시 자동으로 실행되도록 설정하려면:

1. 바탕화면에 `StockTrader.exe`의 바로가기 생성
2. `Win + R` 키를 누르고 `shell:startup`을 입력하여 시작 프로그램 폴더 열기
3. 생성한 바로가기를 시작 프로그램 폴더에 복사

## 주의사항

- 실행 파일은 빌드된 환경과 유사한 환경에서 실행해야 합니다.
- 구글 API 인증 파일이 올바르게 설정되어 있는지 확인하세요.
- 방화벽 설정에서 네트워크 접근 권한을 허용해야 할 수 있습니다.
- 실행 파일 크기가 큰 경우 배포 시간이 오래 걸릴 수 있습니다. 