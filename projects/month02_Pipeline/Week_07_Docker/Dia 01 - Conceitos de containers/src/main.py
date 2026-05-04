import pandas as pd
import platform
import sys


def main():
    print("=" * 40)
    print("[container] iniciando")
    print(f"[container] Python:   {sys.version}")
    print(f"[container] Sistema:  {platform.system()} {platform.release()}")
    print(f"[container] Pandas:   {pd.__version__}")
    print("=" * 40)

    data = {
        "delivery_id": [1, 2, 3],
        "status":      ["enviado", "pendente", "cancelado"],
        "cost":        [670.00, 690.00, 45.00],
    }

    df = pd.DataFrame(data)
    print(f"\n[data] Shape: {df.shape}")
    print(df)
    print("\n[container] finalizado")


if __name__ == "__main__":
    main()