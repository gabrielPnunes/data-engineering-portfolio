#==BLIBIOTECAS E FUNÇÕES==#
import json
from pathlib import Path
from http_client import get, safe_get, response_to_dataframe


#==CAMINHOS==#
base_path = Path("projects/month01_Fundamentals/Data/Raw")
base_path.mkdir(parents=True, exist_ok=True)

json_path = base_path / "weather.json"
csv_path  = base_path / "weather.csv"

#=CONFIGURAÇÕES E URL==#
URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
    "latitude": -15.45,
    "longitude": -47.62,
    "hourly": "temperature_2m,precipitation,windspeed_10m",
    "forecast_days": 3,
    "timezone": "America/Sao_Paulo"
}


print("\n=== GET: Open Meteo ===")
data = get(URL, params=PARAMS)

print(f"[info] Chaves disponíveis: {list(data.keys())}")
print(f"[info] Chaves em 'hourly': {list(data['hourly'].keys())}")

#==SALVAR JSON==#
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print(f"[save] JSON → {json_path}")

#==TRANSFORMAR EM DF==#
df = response_to_dataframe(data, record_path="hourly")
print(df.head(10))

#==SAVLAR EM CSV==#
df.to_csv(csv_path, index=False)
print(f"[save] CSV → {csv_path}")


#==TESTES SAFEGET==#
print("\n=== SFE GET: URL inválida ===")
result = safe_get("https://api.invalida.xyz/dados")
print(f"Resultado: {result}")

print("\n=== SAFE GET: URL válida ===")
result = safe_get(URL, params=PARAMS)

if result:
    df2 = response_to_dataframe(result, record_path="hourly")
    print(f"[info] Shape: {df2.shape}")

print("\n[done] Ingestão concluída")