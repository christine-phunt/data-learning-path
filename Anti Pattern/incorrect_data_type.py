import pandas as pd


df = pd.read_csv("titles.csv")

df = df.assign(
    age_certification=lambda df_: df_["age_certification"].astype("category")
)
