turso_db_url = "libsql://cairns-fuel-h-unter.aws-ap-northeast-1.turso.io"
import libsql

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker  
import seaborn as sns
from datetime import date, timedelta


def plot_fuel_price_trends():
    turso_db_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")

    conn = libsql.connect(database=turso_db_url, auth_token=turso_token)
    result = conn.execute("""
    SELECT * FROM Price_Records AS P
    JOIN Sites AS S ON P.Site_ID = S.Site_ID
    JOIN Fuel_Types AS F ON P.Fuel_ID = F.Fuel_ID
    JOIN Brands AS B ON S.Brand_ID = B.Brand_ID
    """)
    saved_result = result.fetchall().copy()  # Save a copy of the results
    conn.close()


    df = pd.DataFrame(saved_result)
    column_name_to_saved_result_index = {
        "site_id": 0,
        "site_name": 6,
        "fuel_id": 1,
        "fuel_name": 12,
        "transaction_datetime": 2,
        "price": 3,
        "brand_id": 5,
        "brand_name": 14,
        "address": 7,
        "postcode": 8,
        "latitude": 9,
        "longitude": 10,
    }
    desired_columns = ["site_id", "site_name", "fuel_name", "transaction_datetime", "price", "brand_name", "address", "postcode", "latitude", "longitude"]
    df = df[[column_name_to_saved_result_index[col] for col in desired_columns]]
    df.columns = desired_columns
    df['transaction_datetime'] = pd.to_datetime(df['transaction_datetime'])
    df['transaction_date'] = df['transaction_datetime'].dt.date

    min_prices = df.groupby(['transaction_date', 'fuel_name'])['price'].transform('min')
    cheapest_df = df[df['price'] == min_prices].copy()
    cheapest_df = cheapest_df[cheapest_df['price'] < 4.0]
    


    # get a timeseries lineplot of price trends with colour stratified by fuel_name
    fig, ax = plt.subplots(figsize=(8, 3))
    excluded_fuel_names = ["Premium Diesel", "Premium Unleaded 95", "Premium Unleaded 98"]
    filtered_df = cheapest_df[
        (~cheapest_df['fuel_name'].isin(excluded_fuel_names)) & 
        (cheapest_df['transaction_date'] >= date.today() - timedelta(days=30))
    ]
    ax.set_prop_cycle(color=plt.cm.Set1.colors)
    for fuel_name, group in filtered_df.groupby('fuel_name'):
        plt.plot(group['transaction_date'], group['price'], marker='o', label=fuel_name)
    text_size = 10
    plt.xlabel('Date', fontsize=text_size)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.xticks(rotation=45, fontsize=text_size, ha='right')
    plt.ylabel('Price', fontsize=text_size)
    plt.title('Cheapest Fuel Prices Over Time by Fuel Type (Past 30 Days)', fontsize=text_size)
    plt.legend(bbox_to_anchor=(1, 0.5), loc='center left', title='Fuel Type', 
            title_fontsize=text_size,fontsize=text_size, frameon=False)
    ax.grid(True, axis='y', alpha=0.5)
    plt.tight_layout()
    plt.savefig('cheapest_fuel_30_days.svg', format='svg', bbox_inches='tight')



if __name__ == "__main__":
    plot_fuel_price_trends()