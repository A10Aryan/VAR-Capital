import blpapi
import pandas as pd
import numpy as np
import datetime
from blpapi import SessionOptions, Session
from scipy import stats

def get_bloomberg_data(tickers, fields, start_date, end_date):
    session_options = blpapi.SessionOptions()
    session_options.setServerHost("localhost")
    session_options.setServerPort(8194)

    session = blpapi.Session(session_options)
    if not session.start():
        raise Exception("Failed to start Bloomberg session.")

    if not session.openService("//blp/refdata"):
        raise Exception("Failed to open Bloomberg service.")

    service = session.getService("//blp/refdata")
    request = service.createRequest("HistoricalDataRequest")

    for ticker in tickers:
        request.getElement("securities").appendValue(ticker)
    
    for field in fields:
        request.getElement("fields").appendValue(field)

    request.set("startDate", start_date.strftime('%Y%m%d'))
    request.set("endDate", end_date.strftime('%Y%m%d'))
    request.set("periodicitySelection", "DAILY")

    session.sendRequest(request)
    result = {}
    while True:
        ev = session.nextEvent(500)
        for msg in ev:
            if msg.hasElement("securityData"):
                security_data = msg.getElement("securityData")
                for security in security_data.values():
                    ticker = security.getElementAsString("security")
                    field_data = security.getElement("fieldData")
                    prices = {}
                    for field in field_data.values():
                        date = field.getElementAsDatetime("date")
                        value = field.getElementAsFloat("PX_LAST")
                        prices[date] = value
                    result[ticker] = prices

        if ev.eventType() == blpapi.Event.RESPONSE:
            break

    session.stop()
    return result


def calculate_alpha_beta_sharpe(stock_prices, market_prices, risk_free_rate = 0.03): # Change risk-free rate as per will.

    stock_returns = np.diff(np.log(stock_prices))
    market_returns = np.diff(np.log(market_prices))

    beta, alpha, _, _, _ = stats.linregress(market_returns, stock_returns)

    excess_return = stock_returns - risk_free_rate / 252
    sharpe_ratio = np.mean(excess_return) / np.std(stock_returns)

    return alpha, beta, sharpe_ratio


def merger_arbitrage_analysis_bloomberg(target_ticker, deal_price, expected_close_date, market_ticker):

    start_date = datetime.date.today() - datetime.timedelta(days = 365)
    end_date = datetime.date.today()

    stock_data = get_bloomberg_data([target_ticker], ['PX_LAST'], start_date, end_date)
    market_data = get_bloomberg_data([market_ticker], ['PX_LAST'], start_date, end_date)

    stock_prices = list(stock_data[target_ticker].values())
    market_prices = list(market_data[market_ticker].values())

    # Calculate Alpha, Beta, and Sharpe Ratio
    alpha, beta, sharpe_ratio = calculate_alpha_beta_sharpe(stock_prices, market_prices)

    print(f"Alpha: {alpha:.4f}, Beta: {beta:.4f}, Sharpe Ratio: {sharpe_ratio:.4f}")

    # Perform regular merger arbitrage analysis (as before)
    current_target_price = stock_prices[-1]
    arbitrage_spread = (deal_price - current_target_price) / deal_price * 100
    print(f"Arbitrage spread: {arbitrage_spread:.2f}%")

    today = datetime.date.today()
    days_to_close = (expected_close_date - today).days
    annualized_return = (arbitrage_spread / days_to_close) * 365 if arbitrage_spread > 0 else None

    return {
        'Target': target_ticker,
        'Current Target Price': current_target_price,
        'Deal Price': deal_price,
        'Arbitrage Spread (%)': arbitrage_spread,
        'Annualized Return (%)': annualized_return,
        'Alpha': alpha,
        'Beta': beta,
        'Sharpe Ratio': sharpe_ratio
    }


def read_excel_stock_data(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Ensure that required columns exist
    required_columns = ['Company Name', 'Ticker', 'Buy Date', 'Sell Date', 'Deal Price']
    if not all(column in df.columns for column in required_columns):
        raise ValueError(f"Excel file must contain the following columns: {required_columns}")
    
    return df


def merger_arbitrage_from_excel(file_path, market_ticker):

    stock_data = read_excel_stock_data(file_path)

    results = []

    for index, row in stock_data.iterrows():
        target_ticker = row['Ticker']
        deal_price = row['Deal Price']
        expected_close_date = pd.to_datetime(row['Sell Date']).date()

        result = merger_arbitrage_analysis_bloomberg(target_ticker, deal_price, expected_close_date, market_ticker)
        results.append(result)

    df_results = pd.DataFrame(results)

    output_file = 'arbitrage_analysis_results.xlsx'
    df_results.to_excel(output_file, index=False)
    print(f"Results saved to {output_file}")

    return df_results

excel_file_path = "event_data.xlsx"
market_ticker = "SPX Index"
results_df = merger_arbitrage_from_excel(excel_file_path, market_ticker)
