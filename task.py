import json
import websocket
import numpy as np
import typing as t
import cryptocompare
import datetime as dt
import collections as col
import dateutil.relativedelta
from dataclasses import dataclass
import sklearn.linear_model as sklinmod


# ======================================================================================================================
# Calculate a and b of regression

# Collect historical prices
def get_historical(start: dt.datetime, end: dt.datetime, ticker: str) -> t.List[float]:
    CURRENCY: str = "USD"
    result = []
    for n in range(int((end - start).days)):
        new_day = start + dt.timedelta(days=n)
        hourly_data: t.List[t.Dict] = cryptocompare.get_historical_price_hour(coin=ticker,
                                                                              currency=CURRENCY,
                                                                              toTs=new_day,
                                                                              limit=23)
        print(f'For day: {new_day} got data:')
        for hour_data in hourly_data:
            timestamp: dt.datetime = dt.datetime.fromtimestamp(hour_data['time'])
            close: float = hour_data["close"]
            print(f'   ticker: {ticker}: {timestamp}: close: {close}')
            result.append(close)
    return result

# Calculate returns
def calc_ret(first: float, second: float) -> float:
    return (second - first) / first

def get_historical_and_calc_returns(start: dt.datetime, end: dt.datetime, ticker: str):
    data: t.List[float] = get_historical(start, end, ticker)
    result: t.List[float] = [calc_ret(first, second) for first, second in zip(data, data[1:])]
    return result

# Regression analysis
def calculate_a_and_b(ind_returns: t.List[float], dep_returns: t.List[float]) -> t.List[float]:
    dep = np.array(dep_returns).reshape(-1, 1)
    ind = np.array(ind_returns).reshape(-1, 1)
    model = sklinmod.LinearRegression()
    model.fit(ind, dep)
    a, b = model.coef_[0][0], model.intercept_[0]
    print(f"Model: a: {a}, b: {b}")
    return [a, b]



# ======================================================================================================================
# Monitor for price change

@dataclass
class Price:
    val: float = 0.
    time: dt.datetime = dt.datetime.now()

    @staticmethod
    def from_json(parsed_json) -> 'Price':
        return Price(val=float(parsed_json['data']['p']), time=dt.datetime.fromtimestamp(parsed_json['data']['E'] / 1000))


def monitor_price(ab: t.List[float], threshold_return: float, period: dt.timedelta):
    # Deque where we will store returns for the last 60 minutes
    ind_prices: t.Deque[Price] = col.deque()
    dep_prices: t.Deque[Price] = col.deque()

    def remove_prefix_older_than(prices: t.Deque[Price], now: dt.datetime, period: dt.timedelta):
        while prices and (now - prices[0].time) > period + dt.timedelta(seconds=1):
            prices.popleft()
        return prices

    def on_message(wsapp, message):
        nonlocal ind_prices
        nonlocal dep_prices
        json_message = json.loads(message)

        if json_message['stream'] == 'btcusdt@trade':
            ind_prices.append(Price.from_json(json_message))
            now = ind_prices[-1].time
        elif json_message['stream'] == 'ethusdt@trade':
            dep_prices.append(Price.from_json(json_message))
            now = dep_prices[-1].time
        else:
            print(f"Unknown stream: {json_message['stream']}")
            return

        ind_prices = remove_prefix_older_than(ind_prices, now, period)
        dep_prices = remove_prefix_older_than(dep_prices, now, period)
        if not ind_prices or ind_prices[-1].time - ind_prices[0].time < period:
            return
        if not dep_prices or dep_prices[-1].time - dep_prices[0].time < period:
            return

        ind_ret: float = calc_ret(ind_prices[0].val, ind_prices[-1].val)
        dep_ret: float = calc_ret(dep_prices[0].val, dep_prices[-1].val)

        # Idiosyncratic movement of ETH (собственное движение цены ETH)
        idiosyncratic_return: float = dep_ret - (ab[0] * ind_ret + ab[1])
        if abs(idiosyncratic_return) > threshold_return:
            print(f"{ind_prices[-1].time}: idiosyncratic_return: {idiosyncratic_return}.Price has changed more than 1%.")

    def on_error(wsapp, error): # Error handling
        print(error)

    socket = f'wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade'
    wsapp = websocket.WebSocketApp(socket, on_message=on_message, on_error=on_error)
    wsapp.run_forever()


# ======================================================================================================================
# main

if __name__ == "__main__":
    DEP_TICKER: str = "ETH"
    IND_TICKER: str = "BTC"
    now: dt.datetime = dt.datetime.now()
    end = dt.datetime(now.year, now.month, now.day)

    # choose 2 days because I am going to track the price for the last hour
    # so, I am interested in most recent trends
    start = end - dateutil.relativedelta.relativedelta(days=2)

    dep_returns: t.List[float] = get_historical_and_calc_returns(start, end, DEP_TICKER)
    ind_returns: t.List[float] = get_historical_and_calc_returns(start, end, IND_TICKER)
    coefficients: t.List[float] = calculate_a_and_b(ind_returns, dep_returns)
    monitor_price(coefficients, threshold_return=1/100, period=dt.timedelta(minutes=60))
