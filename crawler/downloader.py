import requests


def download_simple(url):
    status_code = -1
    content = None
    try:
        response = requests.get(url, allow_redirects=True)
        status_code = response.status_code
        content = response.content
    except requests.RequestException:
        pass
    return status_code, content