import requests
import pandas as pd

class RickAndMortyApiClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def fetch_all_characters(self):
        """
        Consome todas as páginas da API e retorna
        uma lista com todos os personagens (JSON bruto)
        """
        url = self.base_url
        all_characters = []

        while url:
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            all_characters.extend(data["results"])
            url = data["info"]["next"]  # próxima página

        return all_characters

client = RickAndMortyApiClient(
    base_url="https://rickandmortyapi.com/api/character"
)

raw_characters = client.fetch_all_characters()

df = pd.DataFrame(raw_characters)
print(df.head())

def clean_characters(df):
    df = df.copy()

    df["origin_name"] = df["origin"].apply(lambda x: x["name"])
    df["location_name"] = df["location"].apply(lambda x: x["name"])

    cols = [
        "id",
        "name",
        "status",
        "species",
        "gender",
        "origin_name",
        "location_name"
    ]

    return df[cols]

df_clean = clean_characters(df)
print(df_clean.head())

status_count = df_clean["status"].value_counts()
print(status_count)

species_count = df_clean["species"].value_counts().head(10)
print(species_count)

gender_count = df_clean["gender"].value_counts()
print(gender_count)


