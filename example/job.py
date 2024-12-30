import requests
import os

def submit_job(url):
    # 업로드할 파일 리스트 생성
    files_to_send = [
        ("script_file", open("crawl.py", "rb"))  # 필수 파일
    ]

    # 추가 파일 경로 및 이름
    additional_files_paths = [
        ("image", "image"),  # (파일 경로, 파일 이름)
        ("cookies.json", "cookies.json")
    ]

    for file_path, file_name in additional_files_paths:
        if os.path.exists(file_path):
            try:
                files_to_send.append(
                    ("additional_files", (file_name, open(file_path, "rb")))
                )
            except FileNotFoundError:
                print(f"Error: File not found: {file_path}")

    data = {
        "jobname": "crawl_naver"  # 작업 이름
    }

    # POST 요청으로 서버에 작업 제출
    print("Submitting job...")
    response = requests.post(f"{url}/submit", files=files_to_send, data=data)

    # 응답 상태 및 내용 출력
    if response.status_code == 200:
        result = response.json()
        job_id = result.get("job_id")
        print(f"Job submitted with ID: {job_id}")
        print("Result:", result)

        # 파일 다운로드 호출
        download_files(url, job_id, result.get("files", {}))
    else:
        print(f"Failed to submit job: {response.text}") 

def download_files(url, job_id, files):
    """파일 다운로드 함수"""
    # 작업 ID 별로 다운로드 디렉토리 생성
    download_dir = f'downloads/{job_id}'
    os.makedirs(download_dir, exist_ok=True)
    
    for filename, file_url in files.items():
        # 다운로드 요청
        file_response = requests.get(f'{url}{file_url}', stream=True)
        if file_response.status_code == 200:
            file_path = os.path.join(download_dir, filename)
            with open(file_path, 'wb') as f:
                for chunk in file_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded {filename} to {file_path}")
        else:
            print(f"Failed to download {filename}")

if __name__ == "__main__":
    # 서버 URL 설정
    url = 'http://localhost:5000'  # 로컬 서버 주소
    submit_job(url)