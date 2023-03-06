import pandas as pd


df = pd.read_csv("titles.csv")

# Mutation - DON'T DO THIS
df_bad = df.query("runtime > 30 & type == 'SHOW'")
df_bad["score"] = df_bad[["imdb_score", "tmdb_score"]].sum(axis=1)
df_bad = df_bad[["seasons", "score"]]
df_bad = df_bad.groupby("seasons").agg(["count", "mean"])
df_bad = df_bad.droplevel(axis=1, level=0)
df_bad = df_bad.query("count > 10")

# Chaining - DO THIS
df_good = (df
           .query("runtime > 30 & type == 'SHOW'")
           .assign(score=lambda df_: df_[["imdb_score", "tmdb_score"]].sum(axis=1))
           [["seasons", "score"]]
           .groupby("seasons")
           .agg(["count", "mean"])
           .droplevel(axis=1, level=0)
           .query("count > 10")
           )
