import pandas as pd

df = pd.read_csv(r'./jt.data.1.AllItems.txt', sep="\t")

unique_years = df['year'].unique()

for year in unique_years:
    df_year = df[df['year'] == year]
    df_year.to_csv(
        f'C:/Users/guill/Documents/GitHub/lmos_opioids/data/source/job_openings/job_openings_{int(year)}.csv',
        sep=',',
        index=False
    )