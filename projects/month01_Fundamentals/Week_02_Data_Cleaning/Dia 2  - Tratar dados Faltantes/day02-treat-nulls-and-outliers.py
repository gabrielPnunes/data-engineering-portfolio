#==BIBLIOTECAS==#
import pandas as pd
from src.extractor import extract

#==FUNÇÕES PARA NULOS==#
def drop_null_rows(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    qtd_antes = len(df)
    print(f"[clean] drop_null_rows: {qtd_antes - len(df)} linhas removidas")
    return df

def fill_null_with_value(df: pd.DataFrame, columns: str, value) -> pd.DataFrame:
    qtd_nulls = df[columns].isnull().sum()                      
    df[columns] = df[columns].fillna(value)                     
    print(f"[clean] fill_null_with_value: {qtd_nulls} nulos em '{columns}' preenchidos com '{value}'")
    return df

def fill_null_with_median(df: pd.DataFrame, columns: str) -> pd.DataFrame:
    qtd_nulls = df[columns].isnull().sum()                     
    mediana = df[columns].median()
    df[columns] = df[columns].fillna(mediana)                  
    print(f"[clean] fill_null_with_median: {qtd_nulls} nulos em '{columns}' preenchidos com mediana={mediana:.2f}")
    return df

def fill_null_with_mean(df: pd.DataFrame, columns: str) -> pd.DataFrame:
    qtd_nulls = df[columns].isnull().sum()
    media = df[columns].mean()
    df[columns] = df[columns].fillna(media)
    print(f"[clean] fill_null_with_mode: {qtd_nulls} nulos em '{columns}' preenchidos com mode='{media}'")
    return df

def fill_null_with_mode(df: pd.DataFrame, columns: str) -> pd.DataFrame:
    qtd_nulls = df[columns].isnull().sum()                     
    moda = df[columns].mode()[0]                                
    df[columns] = df[columns].fillna(moda)                      
    print(f"[clean] fill_null_with_mode: {qtd_nulls} nulos em '{columns}' preenchidos com mode='{moda}'")
    return df


#==FUNÇÕES PARA OUTLIERS==#
def detect_outliers_iqr(df: pd.DataFrame, columns: str) -> pd.DataFrame:
    q1 = df[columns].quantile(0.25)
    q3 = df[columns].quantile(0.75)
    iqr = q3 - q1                                               
    lower = q1 - 1.5 * iqr                                     
    upper = q3 + 1.5 * iqr                                     

    outliers = df[(df[columns] < lower) | (df[columns] > upper)]
    print(f"[clean] detect_outliers_iqr: {len(outliers)} outliers em '{columns}' (lower={lower:.2f}, upper={upper:.2f})")
    return outliers

def remove_outlier(df: pd.DataFrame, columns: str) -> pd.DataFrame:
    q1 = df[columns].quantile(0.25)
    q3 = df[columns].quantile(0.75)
    iqr = q3 - q1                                              
    lower = q1 - 1.5 * iqr                                    
    upper = q3 + 1.5 * iqr                                     

    qtd_antes = len(df)
    df = df[(df[columns] >= lower) & (df[columns] <= upper)]
    print(f"[clean] remove_outliers_iqr: {qtd_antes - len(df)} outliers removidos de '{columns}'")
    return df


#==EXECUÇÃO==#
if __name__ == "__main__":
    df = extract("C:/Users/Ceifas/OneDrive/Desktop/data-engineering-portfolio/projects/month01_Fundamentals/Data/Raw/sales.parquet")

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    df = drop_null_rows(df, columns=["total_price", "order_id"])
    df = fill_null_with_value(df, columns="city", value="N/A")

    detect_outliers_iqr(df, columns="total_price")                   
    df = remove_outlier(df, columns="total_price")                   

    print(f"\n[result] {len(df)} linhas após limpeza")
    print(df.head())