import socket
import uuid
import requests
from datetime import datetime

def get_public_ip() -> str:
    """
    현재 시스템의 공인 IP 주소를 가져옵니다.
    
    Returns:
        str: 공인 IP 주소
    """
    try:
        response = requests.get('https://api.ipify.org')
        return response.text
    except Exception as e:
        # 외부 API 호출 실패 시 로컬 IP 반환
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname)

def generate_global_uid(customer_identification_key: str = None) -> str:
    """
    거래 건별로 유니크한 globalUID를 생성합니다.
    형식: YYYYMMDD_HHMMSS_CUSTOMER_KEY_UUID
    
    Args:
        customer_identification_key (str, optional): 고객식별키. 기본값은 None.
    
    Returns:
        str: 생성된 globalUID
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = str(uuid.uuid4())[:8]  # UUID의 앞 8자리만 사용
    
    if customer_identification_key:
        return f"{timestamp}_{customer_identification_key}_{unique_id}"
    else:
        return f"{timestamp}_{unique_id}" 