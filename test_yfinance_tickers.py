import yfinance as yf

def check_tickers():
    tickers = {
        "Nikkei 225": "^N225",
        "TOPIX (Yahoo Japan Code)": "998405.T",
        "TOPIX ETF (Nomura)": "1306.T",
        "USD/JPY": "USDJPY=X",
        "EUR/JPY": "EURJPY=X"
    }

    for name, ticker in tickers.items():
        print(f"Checking {name} ({ticker})...")
        try:
            data = yf.Ticker(ticker)
            history = data.history(period="5d")
            if not history.empty:
                print(f"  Success! Last close: {history['Close'].iloc[-1]}")
            else:
                print(f"  Failed (Empty history)")
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    check_tickers()
