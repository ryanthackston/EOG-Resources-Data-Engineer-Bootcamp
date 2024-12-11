
import pandas_datareader.data as web
import pandas as pd

# Get WTI oil prices from FRED
# wti_prices = web.DataReader("DCOILWTICO", "fred", start="2000-01-01")

# print(wti_prices.head())

###################

# Ticker symbol for WTI oil on Yahoo Finance is "CL=F"
ticker = "CL=F"

# Get historical WTI oil prices from 1999 to today
wti_data = yf.download(ticker, start="1990-02-01", end="2024-10-21", interval="1d")

close_columns = wti_data['Close']

full_date_range = pd.date_range(start=close_columns.index.min(), end=close_columns.index.max(), freq='D')

df_reindexed = close_columns.reindex(full_date_range)

df_reindexed.ffill(inplace=True)


# Display the data
print(df_reindexed)

# Save to a CSV file if needed
 #wti_data.to_csv('wti_oil_prices_1999_to_2024.csv')


# # Get WTI oil prices from FRED
wti_prices2 = web.DataReader("DCOILWTICO", "fred", start="1990-01-02")
oil_prices = wti_prices2['DCOILWTICO']
full_date_range = pd.date_range(start=oil_prices.index.min(), end=oil_prices.index.max(), freq='D')
df_reindexed_oil = oil_prices.reindex(full_date_range)
df_reindexed_oil.ffill(inplace=True)


henry_hub = web.DataReader("DHHNGSP", "fred", start="1990-01-02")
gas_prices = henry_hub['DHHNGSP'] * 5.8
full_date_range_gas = pd.date_range(start=gas_prices.index.min(), end=gas_prices.index.max(), freq='D')
df_reindexed_gas = gas_prices.reindex(full_date_range_gas)
df_reindexed_gas.ffill(inplace=True)


print(wti_prices2.head())




# Join the two DataFrames along the Date index
df_combined = pd.merge(df_reindexed_oil, df_reindexed_gas, left_index=True, right_index=True, how='inner')

# Rename the columns for clarity
df_combined.columns = ['Oil_Price', 'Gas_Price']

# Print the first few rows of the combined DataFrame
print(df_combined.head())

file_path = '/Users/rthackston/Downloads'

# Export the DataFrame to CSV
df_combined.to_csv(file_path)

print(f"CSV file saved to: {file_path}")


# ###################

# # Ticker symbol for WTI oil on Yahoo Finance is "CL=F"
# ticker = "CL=F"

# # Get historical WTI oil prices from 1999 to today
# wti_data = yf.download(ticker, start="1999-01-01", end="2024-10-21", interval="1d")

# # Display the data
# print(wti_data.head())

# # Save to a CSV file if needed
#  wti_data.to_csv('wti_oil_prices_1999_to_2024.csv')


 