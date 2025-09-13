import requests
import concurrent.futures
import os
import time
import mimetypes

# Shopify 설정
SHOP_URL = "Put Your Shop URL"
ACCESS_TOKEN = "Get Your Access Token and Put"
API_VERSION = "2025-07"
GRAPHQL_URL = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"

UPLOAD_DIR = "upload"
VALID_EXTS = {".jpg", ".jpeg", ".png", ".gif"}
MAX_FILE_SIZE_MB = 15
UPLOAD_DELAY_SECONDS = 2

files_to_upload = []
for root, dirs, files in os.walk(UPLOAD_DIR):
    for file in files:
        if os.path.splitext(file)[1].lower() in VALID_EXTS:
            files_to_upload.append(os.path.join(root, file))

total_files = len(files_to_upload)
completed = 0


def get_mime_type(file_path):
    mime_type, _ = mimetypes.guess_type(file_path)
    if mime_type is None:
        ext = os.path.splitext(file_path)[1].lower()
        if ext in {'.jpg', '.jpeg'}:
            return 'image/jpeg'
        elif ext == '.png':
            return 'image/png'
        elif ext == '.gif':
            return 'image/gif'
    return mime_type


def upload_file(local_path):
    global completed
    filename = os.path.basename(local_path)
    file_size_bytes = os.path.getsize(local_path)
    file_size_mb = file_size_bytes / (1024 * 1024)

    if file_size_mb > MAX_FILE_SIZE_MB:
        return f"[{filename}] 파일 크기가 {MAX_FILE_SIZE_MB}MB를 초과하여 건너뜁니다."

    start_time = time.time()

    # 1. Shopify로부터 임시 업로드 URL 요청
    query = """
    mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets {
          url
          resourceUrl
          parameters {
            name
            value
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    mime_type = get_mime_type(local_path)
    if not mime_type:
        return f"[{filename}] 알 수 없는 MIME 타입입니다. 업로드를 건너뜁니다."

    variables = {
        "input": [
            {
                "filename": filename,
                "mimeType": mime_type,
                "fileSize": str(file_size_bytes),
                "resource": "FILE"
            }
        ]
    }

    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(GRAPHQL_URL, headers=headers, json={"query": query, "variables": variables})
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return f"[{filename}] URL 요청 실패: {e}\n응답 내용: {resp.text}"

    if "errors" in data or data.get("data", {}).get("stagedUploadsCreate", {}).get("userErrors"):
        return f"[{filename}] GraphQL 에러: {data.get('errors', data)}"

    staged_targets = data.get("data", {}).get("stagedUploadsCreate", {}).get("stagedTargets")
    if not staged_targets:
        return f"[{filename}] 임시 업로드 타겟을 얻지 못했습니다."

    target = staged_targets[0]
    upload_url = target['url']
    params = {p['name']: p['value'] for p in target['parameters']}

    # 2. 임시 URL에 파일 직접 업로드 (PUT 요청)
    with open(local_path, "rb") as f:
        try:
            upload_resp = requests.put(upload_url, data=f, headers=params)
            upload_resp.raise_for_status()
        except Exception as e:
            return f"[{filename}] 파일 직접 업로드 실패: {e}\n응답 내용: {upload_resp.text}"

    # 3. Shopify에 파일 생성 완료 알림
    final_file_create_query = """
    mutation fileCreate($files: [FileCreateInput!]!) {
        fileCreate(files: $files) {
            files {
                alt
                preview {
                    ... on MediaPreviewImage {
                        image {
                            url
                        }
                    }
                }
            }
            userErrors {
                field
                message
            }
        }
    }
    """
    final_variables = {
        "files": [
            {
                "alt": filename,
                "contentType": "IMAGE",
                "originalSource": target['resourceUrl']
            }
        ]
    }

    try:
        final_resp = requests.post(GRAPHQL_URL, headers=headers,
                                   json={"query": final_file_create_query, "variables": final_variables})
        final_resp.raise_for_status()
        final_data = final_resp.json()
    except Exception as e:
        return f"[{filename}] 파일 생성 실패: {e}"

    elapsed = time.time() - start_time
    completed += 1
    progress = (completed / total_files) * 100

    if "errors" in final_data:
        return f"[{filename}] 최종 GraphQL Error: {final_data['errors']} - 진행률: {progress:.1f}%"

    file_info = final_data.get("data", {}).get("fileCreate", {}).get("files")
    if not file_info:
        return f"[{filename}] 최종 업로드 실패: {final_data.get('data', final_data)} - 진행률: {progress:.1f}%"

    # URL을 안전하게 가져오는 로직은 유지
    file_url = None
    preview_data = file_info[0].get('preview')
    if preview_data:
        image_data = preview_data.get('image')
        if image_data:
            file_url = image_data.get('url')

    if not file_url:
        return f"[{filename}] 업로드 완료됨. - 진행률: {progress:.1f}%"

    return f"[{filename}] 업로드 완료 → {file_url} ({elapsed:.2f}s) - 진행률: {progress:.1f}%"


# 멀티스레드 업로드 실행
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(upload_file, f) for f in files_to_upload]
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        print(result)
        time.sleep(UPLOAD_DELAY_SECONDS)
