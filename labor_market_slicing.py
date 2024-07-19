import pandas as pd

df = pd.read_csv(r'./ladata64County.txt', sep="\t")
df_states = pd.read_csv(r'./la.data.3.AllStatesS.txt', sep="\t")

unique_years = df['year'].unique()
unique_years_states = df_states['year'].unique()

for year in unique_years:
    df_year = df[df['year'] == year]
    df_year.to_csv(
        f'C:/Users/guill/Documents/GitHub/lmos_opioids/data/source/labor_market_outcomes/lmos_data_{int(year)}.csv',
        sep=',',
        index=False
    )

for year in unique_years_states:
    df_states_year = df_states[df_states['year'] == year]
    df_states_year.to_csv(
        f'C:/Users/guill/Documents/GitHub/lmos_opioids/data/source/labor_market_outcomes_states/lmos_data_{int(year)}.csv',
        sep=',',
        index=False
    )