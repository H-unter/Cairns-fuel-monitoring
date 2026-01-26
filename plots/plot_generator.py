import libsql

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker  
from matplotlib.lines import Line2D
import seaborn as sns
from datetime import date, timedelta

def retrieve_prices_df():
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
    return df

def get_plot_data(plot_days, max_price, df, excluded_fuels=["Premium Diesel", "Premium Unleaded 95", "Premium Unleaded 98", "LPG"]):
    # Sort by time and filter outliers immediately
    df = df.sort_values('transaction_datetime')
    df = df[df['price'] <= max_price]

    # Define full simulation range and the specific plotting window
    start_date = df['transaction_date'].min()
    end_date = df['transaction_date'].max()
    plot_start_date = end_date - timedelta(days=plot_days)
    all_dates = pd.date_range(start=start_date, end=end_date)

    # Initialize state trackers
    current_prices = {fuel: {} for fuel in df['fuel_name'].unique()}
    plot_data = {} # Structure: { fuel: {'dates': [], 'prices': []} }
    transactions_by_date = df.groupby('transaction_date')

    # Single pass: Replay history and collect plot data simultaneously
    for current_datetime in all_dates:
        d = current_datetime.date()
        
        # 1. Update State: Apply today's price changes
        if d in transactions_by_date.groups:
            for _, row in transactions_by_date.get_group(d).iterrows():
                current_prices[row['fuel_name']][row['site_id']] = row['price']
                
        # 2. Collect Output: If date is within window, save the price lists
        if d >= plot_start_date:
            for fuel, sites in current_prices.items():
                if fuel in excluded_fuels:
                    continue
                    
                active_prices = list(sites.values())
                if active_prices:
                    if fuel not in plot_data:
                        plot_data[fuel] = {'dates': [], 'prices': []}
                    plot_data[fuel]['dates'].append(d)
                    plot_data[fuel]['prices'].append(active_prices)
    return plot_data

def draw_violin_plot(ax, prices, dates, colour):
    """Draws a violin plot on the given axes."""
    parts = ax.violinplot(
        prices,
        positions=dates,
        showmeans=True,
        showmedians=False,
        showextrema=True,
        widths=1.6
    )
    
    # Styling
    # Note: 'side' parameter is not standard in matplotlib.violinplot, so it is omitted.
    parts['cmeans'].set_color(colour)
    parts['cmins'].set_color(colour)
    parts['cmaxes'].set_color(colour)
    parts['cbars'].set_color(colour)
    
    for pc in parts['bodies']:
        pc.set_facecolor(colour)
        pc.set_alpha(0.6)

def draw_box_plot(ax, prices, dates, colour, text_size):
    """Draws a box plot on the given axes."""
    bp = ax.boxplot(
        prices,
        positions=dates,
        widths=0.4,
        patch_artist=True,
        showmeans=True,
        showfliers=True
    )
    
    # Styling Box Elements
    for box in bp['boxes']:
        box.set_facecolor(colour)
        box.set_edgecolor(colour)
        box.set_alpha(0.6)
        box.set_linewidth(2)
        
    for element in ['whiskers', 'caps']:
        plt.setp(bp[element], color=colour, linewidth=1.5)
        
    # Styling Lines and Markers
    plt.setp(bp['medians'], color=colour, linewidth=1.5)
    plt.setp(bp['means'], marker='D', markeredgecolor=colour, markerfacecolor='white', markersize=4)
    plt.setp(bp['fliers'], marker='x', markeredgecolor=colour, markersize=4)
    
    # Legend (Specific to Box Plot)
    legend_elements = [
        Line2D([0], [0], marker='D', color='w', label='Mean',
               markerfacecolor='white', markeredgecolor=colour, markersize=4),
        Line2D([0], [0], marker='x', color='w', label='Outlier',
               markeredgecolor=colour, markersize=4)
    ]
    
    ax.legend(
        handles=legend_elements,
        loc='lower right', 
        bbox_to_anchor=(1, 0.88), 
        frameon=False, 
        ncol=2,
        fontsize=text_size
    )

def plot_main(plot_data, is_boxplot=True, is_violinplot=False):
    fuels_to_plot = sorted(plot_data.keys())
    n_plots = len(fuels_to_plot)

    fig, ax = plt.subplots(
        n_plots, 
        1, 
        figsize=(7, 2 * n_plots), 
        sharex=True
    )
    if n_plots == 1: ax = [ax]

    colours = plt.rcParams['axes.prop_cycle'].by_key()['color']
    text_size = 10
    text_size_large = 14

    for i, fuel_name in enumerate(fuels_to_plot):
        data = plot_data[fuel_name]
        colour = colours[i % len(colours)]
        numeric_dates = mdates.date2num(data['dates'])
        
        # --- CONDITIONAL PLOTTING ---
        if is_violinplot:
            draw_violin_plot(ax[i], data['prices'], numeric_dates, colour)
            
        if is_boxplot:
            draw_box_plot(ax[i], data['prices'], numeric_dates, colour, text_size)
        # ----------------------------

        # Standard Axis Formatting
        ax[i].set_ylabel('Price ($/L)', fontsize=text_size)
        ax[i].set_title(fuel_name, fontsize=text_size_large, loc='left')

        ax[i].yaxis.set_major_locator(ticker.MultipleLocator(0.1))
        ax[i].xaxis.set_major_locator(mdates.DayLocator(interval=5))
        ax[i].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        
        # Grid and Spines
        ax[i].grid(True, axis='y', alpha=0.5, zorder=-1)

    ax[-1].set_xlabel('Date', fontsize=text_size)

    for a in ax:
        a.tick_params(axis='x', which='both', labelbottom=True)
        plt.setp(a.get_xticklabels(), rotation=35, ha='right', fontsize=text_size)

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig('plots/fuel_price_distribution.svg', format='svg')

def main():
    df = retrieve_prices_df()
    plot_data = get_plot_data(plot_days=60, max_price=5.0, df=df)
    plot_main(plot_data, is_boxplot=True, is_violinplot=False)

if __name__ == "__main__":
    main()

