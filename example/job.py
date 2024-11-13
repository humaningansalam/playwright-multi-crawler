import requests
import os

def submit_job(url):
    files = {
        "script_file": open("crawl.py", "rb"),
        "additional_files": open("image1.png", "rb"),  # 추가 파일 예시 1
    }

    data = {
        "jobname": "crawl_naver"
    }

    response = requests.post(url+'/submit', files={**files}, data=data)

    # 응답 상태 및 내용 출력
    if response.status_code == 200:
        result = response.json()
        job_id = result.get("job_id")
        print("Job submitted with ID:", job_id)
        print("Result:", result)

        # 파일 다운로드 호출
        download_files(url, job_id, result.get("files", {}))
    else:
        print("Failed to submit job:", response.text)

def download_files(url, job_id, files):
    """파일 다운로드 함수"""
    download_dir = f'downloads/{job_id}'
    os.makedirs(download_dir, exist_ok=True)
    
    for filename, file_url in files.items():
        file_response = requests.get(f'{url}{file_url}', stream=True)
        if file_response.status_code == 200:
            file_path = os.path.join(download_dir, filename)
            with open(file_path, 'wb') as f:
                for chunk in file_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded {filename}")
        else:
            print(f"Failed to download {filename}")

if __name__ == "__main__":
    url = 'http://localhost:5000' 
    submit_job(url)
