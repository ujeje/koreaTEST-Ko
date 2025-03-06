import os
import subprocess
import shutil

def build_executable():
    """PyInstaller를 사용하여 실행 파일을 생성합니다."""
    print("주식 트레이딩 프로그램 실행 파일 빌드를 시작합니다...")
    
    # 빌드 디렉토리 생성
    if not os.path.exists('dist'):
        os.makedirs('dist')
    
    # 설정 파일 디렉토리 생성
    if not os.path.exists('dist/config'):
        os.makedirs('dist/config')
    
    # 로그 디렉토리 생성
    if not os.path.exists('dist/logs'):
        os.makedirs('dist/logs')
    
    # PyInstaller 명령어 실행
    pyinstaller_cmd = (
        'pyinstaller '
        '--name=StockTrader '
        '--onefile '
        '--distpath=./dist '
        '--add-data="config/config.yaml;config" '
        '--hidden-import=pandas '
        '--hidden-import=numpy '
        '--hidden-import=pytz '
        '--hidden-import=yaml '
        '--hidden-import=google.auth '
        '--hidden-import=google.oauth2 '
        '--hidden-import=googleapiclient '
        'src/main.py'
    )
    
    try:
        print("PyInstaller 실행 중...")
        subprocess.run(pyinstaller_cmd, shell=True, check=True)
        print("PyInstaller 실행 완료")
        
        # 설정 파일 복사
        if os.path.exists('config/config.yaml'):
            print("설정 파일을 dist/config 폴더로 복사합니다.")
            shutil.copy('config/config.yaml', 'dist/config/')
            print("설정 파일 복사 완료")
        else:
            print("경고: config/config.yaml 파일을 찾을 수 없습니다.")
        
        # 실행 파일 확인
        if os.path.exists('dist/StockTrader.exe'):
            print("실행 파일 생성 확인: dist/StockTrader.exe")
        else:
            print("경고: dist/StockTrader.exe 파일을 찾을 수 없습니다.")
        
        print("빌드가 완료되었습니다. dist 폴더에서 StockTrader.exe를 실행하세요.")
    except subprocess.CalledProcessError as e:
        print(f"오류: PyInstaller 실행 중 문제가 발생했습니다. {e}")
    except Exception as e:
        print(f"오류: 빌드 중 예상치 못한 문제가 발생했습니다. {e}")

if __name__ == "__main__":
    build_executable() 