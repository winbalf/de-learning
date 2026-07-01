import pandas as pd

data = {
    "city": ["Sydney", "Melbourne", "Brisbane"],
    "population": [5_300_000, 5_100_000, 2_600_000],
    "country": ["AU", "AU", "AU"]
}

df = pd.DataFrame(data)
print(df)
print(f"\nTotal population: {df['population'].sum():,}")