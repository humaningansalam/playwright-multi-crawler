import requests
import os
from pathlib import Path

def submit_job(url):
    # 스크립트 파일 경로 확인
    script_path = "crawl.py"
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Required script file not found: {script_path}")

    # 업로드할 파일 리스트 생성
    files = [
        ("script_file", ("script.py", open(script_path, "rb"), "application/octet-stream"))  # 필수 파일
    ]

    # 추가 파일 경로 및 이름
    additional_files_paths = [
        ("image", "image"),  # (파일 경로, 파일 이름)
        ("cookies.json", "cookies.json")
    ]

    # 존재하는 추가 파일들만 처리
    for file_path, file_name in additional_files_paths:
        if os.path.exists(file_path):
            try:
                files.append(
                    ("additional_files", (file_name, open(file_path, "rb"), "application/octet-stream"))
                )
                print(f"Adding additional file: {file_name}")
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

    data = {
        "jobname": "crawl_naver"  # 작업 이름
    }

    try:
        # POST 요청으로 서버에 작업 제출
        print(f"Submitting job to {url}/submit")
        print(f"Files being sent: {[f[1][0] for f in files]}")
        
        response = requests.post(f"{url}/submit", files=files, data=data)
        response.raise_for_status()

        # 응답 처리
        result = response.json()
        if 'job_id' in result:
            job_id = result['job_id']
            print(f"Job submitted successfully with ID: {job_id}")
            print("Result:", result)
            
            # 파일 다운로드 호출
            if 'files' in result:
                download_files(url, job_id, result['files'])
            else:
                print("No files to download")
        else:
            print("Warning: No job_id in response")

    except requests.exceptions.RequestException as e:
        print(f"Error submitting job: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Server response: {e.response.text}")
    finally:
        # 열린 파일들 정리
        for _, file_tuple in files:
            try:
                file_tuple[1].close()
            except:
                pass

def download_files(url, job_id, files):
    """파일 다운로드 함수"""
    # 작업 ID 별로 다운로드 디렉토리 생성
    download_dir = Path('downloads') / job_id
    download_dir.mkdir(parents=True, exist_ok=True)
    
    for filename, file_url in files.items():
        try:
            print(f"Downloading {filename}...")
            file_response = requests.get(f'{url}{file_url}', stream=True)
            file_response.raise_for_status()
            
            file_path = download_dir / filename
            with open(file_path, 'wb') as f:
                for chunk in file_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Successfully downloaded: {filename} to {file_path}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {filename}: {e}")

if __name__ == "__main__":
    # 서버 URL 설정 (환경변수에서 가져오거나 기본값 사용)
    url = os.getenv('PLAYWRIGHT_URL', 'http://localhost:5000')
    submit_job(url)