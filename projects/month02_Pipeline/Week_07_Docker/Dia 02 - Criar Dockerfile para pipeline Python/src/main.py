import os
import sys
import platform
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ENV    = os.getenv("ENV",         "development")
SAMPLE = int(os.getenv("SAMPLE_SIZE", 3))


def main():
    print("=" * 40)
    print(f"[container] Python:  {sys.version.split()[0]}")
    print(f"[container] Sistema: {platform.system()} {platform.release()}")
    print(f"[container] Pandas:  {pd.__version__}")
    print(f"[container] ENV:     {ENV}")
    print(f"[container] SAMPLE:  {SAMPLE}")
    print("=" * 40)

    data = {
        "delivery_id": list(range(1, SAMPLE + 1)),
        "status":      (["delivered", "pending", "canceled"] * SAMPLE)[:SAMPLE],
        "cost":        [round(100 + i * 10.5, 2) for i in range(SAMPLE)],
    }

    df = pd.DataFrame(data)
    print(f"\n[data] Shape: {df.shape}")
    print(df)
    print("\n[container] finalizado")


if __name__ == "__main__":
    main()