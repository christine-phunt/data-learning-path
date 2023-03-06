import pandas as pd


df = pd.read_csv("titles.csv").pipe(split_production_countries)


def get_prod_country_rank(df_):
    vcs = df_["prod_country1"].value_counts()
    return np.select(
        condlist=(
            df_["prod_country1"].isin(vcs.index[:3]),
            df_["prod_country1"].isin(vcs.index[:10]),
            df_["prod_country1"].isin(vcs.index[:20]),
        ),
        choicelist=("top3", "top10", "top20"),
        default="other"
    )


df = df.assign(prod_country_rank=lambda df_: get_prod_country_rank(df_))
