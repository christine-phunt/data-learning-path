import pandas as pd

df = pd.read_csv("titles.csv").pipe(split_production_countries)

# obtain country ranks
vcs = df["prod_country1"].value_counts()
top3 = vcs.index[:3]
top10 = vcs.index[:10]
top20 = vcs.index[:20]

# Looping - DON'T DO THIS
vals = []
for ind, row in df.iterrows():
    country = row["prod_country1"]
    if country in top3:
        vals.append("top3")
    elif country in top10:
        vals.append("top10")
    elif country in top20:
        vals.append("top20")
    else:
        vals.append("other")
df["prod_country_rank"] = vals

# df[col].apply() - DO THIS


def get_prod_country_rank(country):
    if country in top3:
        return "top3"
    elif country in top10:
        return "top10"
    elif country in top20:
        return "top20"
    else:
        return "other"


df["prod_country_rank"] = df["prod_country1"].apply(get_prod_country_rank)
print(df.sample(5, random_state=14).iloc[:, -4:])
