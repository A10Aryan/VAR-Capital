import blpapi
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, date

# Load the Excel file to get the list of stocks and their corresponding purchase and sale dates
excel_file = 'data.xlsx'
df = pd.read_excel(excel_file, sheet_name = "Sheet1")

# Extract the list of tickers, purchase dates, and sell dates from the DataFrame
tickers = df['Ticker'].tolist()
purchase_dates = pd.to_datetime(df['Purchase Date'], errors = 'coerce').tolist()
sell_dates = pd.to_datetime(df['Sale Date'], errors = 'coerce').tolist()

def fetch_bloomberg_data(ticker, start_date, end_date):
    try:
        # Check if dates are valid
        if pd.isna(start_date) or pd.isna(end_date):
            raise ValueError("Invalid date encountered.")

        session_options = blpapi.SessionOptions()
        session_options.setServerHost("localhost")
        session_options.setServerPort(8194)

        session = blpapi.Session(session_options)
        if not session.start():
            raise Exception("Failed to start Bloomberg session.")
        if not session.openService("//blp/refdata"):
            raise Exception("Failed to open Bloomberg refdata service.")

        ref_data_service = session.getService("//blp/refdata")
        request = ref_data_service.createRequest("HistoricalDataRequest")

        request.getElement("securities").appendValue(ticker)
        request.getElement("fields").appendValue("PX_LAST")
        request.set("startDate", start_date.strftime('%Y%m%d'))
        request.set("endDate", end_date.strftime('%Y%m%d'))
        request.set("periodicitySelection", "DAILY")

        session.sendRequest(request)

        data = []
        while True:
            event = session.nextEvent()
            for msg in event:
                if msg.hasElement("securityData"):
                    security_data = msg.getElement("securityData")
                    field_data = security_data.getElement("fieldData")
                    for i in range(field_data.numValues()):
                        data_point = field_data.getValueAsElement(i)
                        date = data_point.getElementAsDatetime("date").strftime('%Y-%m-%d')
                        price = data_point.getElementAsFloat("PX_LAST")
                        data.append([date, price])
            if event.eventType() == blpapi.Event.RESPONSE:
                break

        df = pd.DataFrame(data, columns = ["Date", "Adj Close"])
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace = True)
        return df

    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return None
    finally:
        session.stop()

def calculate_metrics(stock_data, market_data):
    try:
        # Calculate daily returns
        stock_returns = stock_data['Adj Close'].pct_change().dropna()
        market_returns = market_data['Adj Close'].pct_change().dropna()

        # Align the data by date
        returns_data = pd.concat([stock_returns, market_returns], axis = 1).dropna()
        if returns_data.empty:
            print("No overlapping data between stock and market within the specified date range.")
            return None, None, None

        stock_returns, market_returns = returns_data.iloc[:, 0], returns_data.iloc[:, 1]

        # Calculate alpha and beta using linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(market_returns, stock_returns)
        alpha = intercept
        beta = slope

        # Calculate Sharpe ratio
        excess_returns = stock_returns - 0.03 / 252  # Assuming risk-free rate of 3% annually
        sharpe_ratio = np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)

        return alpha, beta, sharpe_ratio
    except Exception as e:
        print(f"Error calculating metrics for the stock: {e}")
        return None, None, None

# Initialize variables for portfolio-level metrics and results storage
portfolio_alpha = 0.0
portfolio_beta = 0.0
portfolio_sharpe_ratio = 0.0
num_stocks_processed = 0

results = []

# File to log tickers that could not be processed
error_log_file = "error_tickers.txt"
with open(error_log_file, "w") as error_log:

    # Fetch and calculate metrics for each stock in the portfolio
    for ticker, purchase_date, sell_date in zip(tickers, purchase_dates, sell_dates):
        if pd.isna(purchase_date):
            error_log.write(f"Could not process {ticker}: Invalid purchase date format.\n")
            continue

        # Set sell date to today if it's blank
        if pd.isna(sell_date):
            sell_date = pd.Timestamp(date.today())

        stock_data = fetch_bloomberg_data(ticker, purchase_date, sell_date)
        if stock_data is None or stock_data.empty:
            error_log.write(f"Could not process {ticker}: Error fetching stock data.\n")
            continue

        # Fetch market data (for simplicity, let's assume FTSE 100 Index as a proxy for the market index)
        market_ticker = 'UKX Index'  # FTSE 100 Index
        market_data = fetch_bloomberg_data(market_ticker, purchase_date, sell_date)
        if market_data is None or market_data.empty:
            error_log.write(f"Could not process {ticker}: Error fetching market data.\n")
            continue

        # Calculate metrics
        alpha, beta, sharpe_ratio = calculate_metrics(stock_data, market_data)
        if alpha is None or beta is None or sharpe_ratio is None:
            error_log.write(f"Could not process {ticker}: Error calculating metrics.\n")
            continue

        # Accumulate metrics for the portfolio
        portfolio_alpha += alpha
        portfolio_beta += beta
        portfolio_sharpe_ratio += sharpe_ratio
        num_stocks_processed += 1

        # Store results for each stock
        results.append([ticker, alpha, beta, sharpe_ratio])

        # Print results for each stock
        print(f"Metrics for {ticker}:")
        print(f"Alpha: {alpha:.4f}")
        print(f"Beta: {beta:.4f}")
        print(f"Sharpe Ratio: {sharpe_ratio:.4f}")
        print("=" * 50)

# Calculate average portfolio metrics
if num_stocks_processed > 0:
    portfolio_alpha /= num_stocks_processed
    portfolio_beta /= num_stocks_processed
    portfolio_sharpe_ratio /= num_stocks_processed

# Print portfolio-level metrics
print("Portfolio Metrics:")
print(f"Average Alpha: {portfolio_alpha:.4f}")
print(f"Average Beta: {portfolio_beta:.4f}")
print(f"Average Sharpe Ratio: {portfolio_sharpe_ratio:.4f}")

# Save results to an Excel spreadsheet
results_df = pd.DataFrame(results, columns = ["Ticker", "Alpha", "Beta", "Sharpe Ratio"])
results_df.to_excel("metrics_results.xlsx", index = False)
