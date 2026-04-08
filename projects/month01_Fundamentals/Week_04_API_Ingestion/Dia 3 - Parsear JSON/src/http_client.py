#==BIBLIOTECAS==#
import requests
import pandas as pd

#==REQUISIÇÕES==#
def get(url, params=None, headers=None):
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    print(f"[get] {response.status_code} — {url}")
    return response.json()

#==TRATAMENTO==#
def safe_get(url, params=None, headers=None):
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        print(f"[safe_get] {response.status_code} — {url}")
        return response.json()
    except requests.exceptions.Timeout:
        print(f"[safe_get] Timeout — {url}")
    except requests.exceptions.HTTPError as e:
        print(f"[safe_get] HTTP Error {e.response.status_code} — {url}")
    except requests.exceptions.ConnectionError:
        print(f"[safe_get] Connection Error — {url}")
    return None

#==CONVERSÃO==#
def response_to_dataframe(data, record_path=None):
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict) and record_path:
        df = pd.DataFrame(data[record_path])
    else:
        df = pd.json_normalize(data)

    print(f"[convert] {len(df)} linhas extraídas")
    return df