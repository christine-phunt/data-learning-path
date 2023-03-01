print("Python first program");

import pandas as pd
from pandasql import sqldf
from pandasql import load_births

births = load_births()

print(sqldf("SELECT * FROM births where births > 250000 limit 5;", locals()))