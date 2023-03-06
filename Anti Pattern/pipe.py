import pandas as pd
df = pd.read_csv("titles.csv")


def split_prod_countries(df_):
    # split `production_countries` column (containing lists of country
    # strings) into three individual columns of single country strings
    dfc = pd.DataFrame(df_["production_countries"].apply(eval).to_list())
    dfc = dfc.iloc[:, :3]
    dfc.columns = ["prod_country1", "prod_country2", "prod_country3"]
    return df_.drop("production_countries", axis=1).join(dfc)


df_pipe = df.pipe(split_prod_countries)

print(df["production_countries"].sample(5, random_state=14))
# returns:
# 3052    ['CA', 'JP', 'US']
# 1962                ['US']
# 2229                ['GB']
# 2151          ['KH', 'US']
# 3623                ['ES']

print(df_pipe.sample(5, random_state=14).iloc[:, -3:])
