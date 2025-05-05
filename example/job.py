import requests
import time
import os
import json
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 서버 URL 설정 
SERVER_URL = os.getenv('PLAYWRIGHT_URL', 'http://localhost:5000') 
POLL_INTERVAL_SECONDS = 10 # 상태 확인 간격 
MAX_POLL_ATTEMPTS = 60 # 최대 상태 확인 횟수 

def submit_job(script_path: str, job_name: str, additional_files_info: Optional[List[Dict[str, str]]] = None) -> Optional[str]:
    """
    서버에 작업을 제출하고 job_id를 반환합니다.

    Args:
        script_path: 실행할 크롤링 스크립트 파일 경로.
        job_name: 작업의 고유 이름.
        additional_files_info: 추가 파일 정보 리스트. 각 요소는 {'path': '파일경로', 'name': '서버에저장될이름'} 형태의 딕셔너리.

    Returns:
        제출 성공 시 job_id 문자열, 실패 시 None.
    """
    if not os.path.exists(script_path):
        logging.error(f"Error: Script file not found at {script_path}")
        return None

    submit_url = f"{SERVER_URL}/api/jobs/submit"
    files_to_upload = []
    opened_files = [] 

    try:
        # 스크립트 파일 추가
        script_file_obj = open(script_path, "rb")
        opened_files.append(script_file_obj)
        files_to_upload.append(
            ("script_file", (os.path.basename(script_path), script_file_obj, "text/x-python"))
        )
        logging.info(f"Preparing script file: {script_path}")

        # 추가 파일 처리
        if additional_files_info:
            for file_info in additional_files_info:
                path = file_info.get('path')
                name = file_info.get('name')
                if not path or not name:
                    logging.warning(f"Invalid additional file info skipped: {file_info}")
                    continue

                if os.path.exists(path):
                    try:
                        add_file_obj = open(path, "rb")
                        opened_files.append(add_file_obj)
                        files_to_upload.append(
                            ("additional_files", (name, add_file_obj, "application/octet-stream"))
                        )
                        logging.info(f"Preparing additional file: {name} from {path}")
                    except Exception as e:
                        logging.error(f"Error opening additional file {path}: {e}")
                        return None
                else:
                    logging.warning(f"Additional file not found at {path}, skipping.")

        # 요청 데이터
        data = {"jobname": job_name}

        logging.info(f"Submitting job '{job_name}' to {submit_url}...")
        response = requests.post(submit_url, files=files_to_upload, data=data, timeout=30)
        response.raise_for_status()

        result = response.json()
        if response.status_code == 202 and 'job_id' in result:
            job_id = result['job_id']
            logging.info(f"Job submitted successfully. Job ID: {job_id}, Status: {result.get('status', 'N/A')}")
            return job_id
        else:
            logging.error(f"Unexpected response from server: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Error submitting job: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Server response: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during job submission: {e}")
        return None
    finally:
        # 열린 파일 핸들 모두 닫기
        for f in opened_files:
            try:
                f.close()
            except Exception as e:
                logging.warning(f"Error closing file handle: {e}")


def poll_job_status(job_id: str) -> Optional[str]:
    """
    작업 상태를 폴링하고 최종 상태 ('COMPLETED', 'FAILED', 'TIMEOUT', None)를 반환합니다.

    Args:
        job_id: 확인할 작업 ID.

    Returns:
        최종 상태 문자열 또는 None (Job ID 못 찾음).
    """
    status_url = f"{SERVER_URL}/api/jobs/status/{job_id}"
    logging.info(f"Polling job status for {job_id} at {status_url}...")

    for attempt in range(MAX_POLL_ATTEMPTS):
        try:
            response = requests.get(status_url, timeout=10)
            if response.status_code == 404:
                 logging.error(f"Error: Job ID {job_id} not found on server.")
                 return None 
            response.raise_for_status()

            status_data = response.json()
            current_status = status_data.get('status')
            logging.info(f"Attempt {attempt + 1}/{MAX_POLL_ATTEMPTS}: Job status = {current_status}")

            if current_status in ['COMPLETED', 'FAILED']:
                logging.info(f"Job {job_id} reached final status: {current_status}")
                return current_status # 최종 상태 도달
            elif current_status in ['PENDING', 'RUNNING']:
                # 아직 진행 중이면 잠시 대기 후 다시 시도
                time.sleep(POLL_INTERVAL_SECONDS)
            else:
                 logging.warning(f"Unknown job status received: {current_status}. Continuing poll.")
                 time.sleep(POLL_INTERVAL_SECONDS)

        except requests.exceptions.Timeout:
            logging.warning(f"Polling request timed out for job {job_id}. Retrying...")
            time.sleep(POLL_INTERVAL_SECONDS)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error polling status for job {job_id}: {e}. Retrying after delay...")
            # 네트워크 오류 등 발생 시 잠시 후 재시도
            time.sleep(POLL_INTERVAL_SECONDS * 2)
        except Exception as e:
            logging.error(f"An unexpected error occurred during status polling: {e}")
            time.sleep(POLL_INTERVAL_SECONDS * 2)


    logging.warning(f"Polling timed out for job {job_id} after {MAX_POLL_ATTEMPTS} attempts.")
    return "TIMEOUT" # 폴링 시간 초과


def get_job_results(job_id: str) -> Optional[Dict[str, Any]]:
    """
    작업 결과를 서버에서 가져옵니다.

    Args:
        job_id: 결과를 가져올 작업 ID.

    Returns:
        서버에서 받은 결과 딕셔너리 또는 None (실패 시).
    """
    results_url = f"{SERVER_URL}/api/jobs/results/{job_id}"
    logging.info(f"Fetching results for job {job_id} from {results_url}...")
    try:
        response = requests.get(results_url, timeout=30)
        if response.status_code == 404:
            logging.error(f"Error: Job ID {job_id} not found when fetching results.")
            return None
        if response.status_code == 202:
             logging.warning(f"Job {job_id} is still processing according to results endpoint.")
             return response.json() 

        response.raise_for_status() 

        results_data = response.json()
        logging.info(f"Successfully fetched results for job {job_id}.")

        # 결과 출력 
        print("\n" + "=" * 20 + f" Results for Job {job_id} " + "=" * 20)
        print(f"Status: {results_data.get('status')}")
        print(f"Job Name: {results_data.get('jobname')}")
        print(f"Submitted At: {results_data.get('submitted_at')}")
        print(f"Duration: {results_data.get('duration_seconds'):.2f} seconds" if results_data.get('duration_seconds') is not None else "N/A")
        print("-" * 20 + " Crawl Result/Error " + "-" * 19)
        print(json.dumps(results_data.get('result'), indent=2, ensure_ascii=False))
        print("-" * 60)

        # 파일 정보 출력 및 다운로드 트리거
        if 'files' in results_data and results_data['files'] and 'error' not in results_data['files']:
            logging.info("Result files available:")
            for filename, download_url_path in results_data['files'].items():
                logging.info(f"- {filename}: {SERVER_URL}{download_url_path}")
            # 파일 다운로드 실행
            download_files(job_id, results_data['files'])
        elif 'files' in results_data and 'error' in results_data['files']:
             logging.error(f"Server reported an error listing files: {results_data['files']['error']}")
        else:
             logging.info("No result files found or reported.")

        print("=" * 60 + "\n")
        return results_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching results for job {job_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Server response: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while fetching results: {e}")
        return None

def download_files(job_id: str, files_dict: Dict[str, str]):
    """
    결과 파일을 지정된 URL에서 다운로드합니다.

    Args:
        job_id: 파일을 저장할 하위 폴더 이름으로 사용될 작업 ID.
        files_dict: 파일 이름과 다운로드 URL 경로 맵.
    """
    download_dir = Path('downloads') / job_id
    try:
        download_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Downloading files to: {download_dir.resolve()}")
    except OSError as e:
         logging.error(f"Failed to create download directory {download_dir}: {e}")
         return

    for filename, file_url_path in files_dict.items():
        # 서버 응답의 URL 경로가 예상대로 오는지 확인
        if isinstance(file_url_path, str) and file_url_path.startswith('/api/jobs/download/'):
            download_url = f'{SERVER_URL}{file_url_path}'
            file_path = download_dir / filename
            try:
                logging.info(f"Downloading {filename} from {download_url}...")
                # stream=True 로 대용량 파일 처리 개선
                file_response = requests.get(download_url, stream=True, timeout=60) # 다운로드 타임아웃
                file_response.raise_for_status() # HTTP 에러 확인

                with open(file_path, 'wb') as f:
                    # chunk 단위로 파일 쓰기
                    for chunk in file_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logging.info(f"Successfully downloaded and saved: {filename}")

            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to download {filename}: {e}")
            except IOError as e:
                 logging.error(f"Failed to save file {filename} to {file_path}: {e}")
            except Exception as e:
                 logging.error(f"An unexpected error occurred during download of {filename}: {e}")
        else:
             logging.warning(f"Invalid or unexpected file URL path for {filename}: {file_url_path}")


if __name__ == "__main__":
    # --- 사용 예시 ---
    crawl_script_to_submit = "crawl.py" 
    unique_job_name = "naver_news_crawl"

    # 추가 파일 정보 (필요한 경우)
    additional_files = [
        # {'path': 'local_config.json', 'name': 'config.json'},
        # {'path': 'logo.png', 'name': 'logo.png'}
    ]
    # 추가 파일이 없다면 None 또는 빈 리스트 전달
    # additional_files = None

    # 1. 작업 제출
    submitted_job_id = submit_job(crawl_script_to_submit, unique_job_name, additional_files)

    if submitted_job_id:
        # 2. 상태 폴링
        final_status = poll_job_status(submitted_job_id)

        # 3. 결과 가져오기 
        if final_status == 'COMPLETED' or final_status == 'FAILED':
            get_job_results(submitted_job_id)
        elif final_status == 'TIMEOUT':
             logging.warning(f"Job {submitted_job_id} processing timed out from client perspective.")
        else:
             logging.error(f"Could not determine the final status for job {submitted_job_id}.")
    else:
        logging.error("Job submission failed. Cannot proceed.")