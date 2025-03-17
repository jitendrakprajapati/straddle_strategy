from PyKite import pykite
from datetime import datetime as dt, timedelta, date
import pandas as pd
import time
import numpy as np
import os
import sys
import configparser
import pyotp
from logzero import logger
import kiteapp as kt
import csv, json

time_now = dt.now()
day_min = time_now.hour * 100 + time_now.minute

assert (time_now.hour * 100 + time_now.minute > 914) and (
    time_now.hour * 100 + time_now.minute < 1530
)


def login_kite():
    kite = pykite()  # kite instance creation
    config = configparser.ConfigParser()
    config.read("config.ini")
    token = config["ENCTOKEN"]["enctoken"]
    return token


def strike_price_stock():
    enc_token = login_kite()
    kite = kt.KiteApp("self", "UZ2906", enc_token)
    kws = kite.kws()
    stock = {260105: "BANKNIFTY"}

    ltp_data = {}

    def on_ticks(ws, ticks):
        for symbol in ticks:
            ltp_data[stock[symbol["instrument_token"]]] = {
                "ltp": symbol["last_price"],
                "High": symbol["ohlc"]["high"],
                "Low": symbol["ohlc"]["low"],
            }

    def on_connect(ws, response):
        ws.subscribe(list(stock.keys()))
        ws.set_mode(
            ws.MODE_QUOTE, list(stock.keys())
        )  # MODE_FULL , MODE_QUOTE, MODE_LTP

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect(threaded=True)
    while len(ltp_data.keys()) != len(list(stock.keys())):
        time.sleep(5)
        continue
    for i in list(stock.values()):
        ltp = ltp_data[i]["ltp"]
        time_ltp = dt.now()
        # high = ltp_data[i]['High']
        print(i, ltp, time_ltp)

    strike_price_stock = round(ltp / 100) * 100

    print(strike_price_stock)

    return strike_price_stock


def create_straddle():
    # Strike Selection

    strike_price = strike_price_stock()

    kite = pykite()

    # Token Selection

    instruments = pd.read_csv("instruments_nfo.csv")

    option_info = instruments.copy()

    option_info = instruments[
        (instruments.name == "BANKNIFTY")
        & (instruments.instrument_type.isin(["CE", "PE"]))
        & (instruments.strike == strike_price)
    ]
    option_info["expiry"] = pd.to_datetime(
        option_info["expiry"], format="%Y-%m-%d"
    ).dt.date

    option_info["today"] = date.today()
    option_info["day_diff"] = (
        option_info["expiry"] - option_info["today"]
    ) / np.timedelta64(1, "D")

    option_info = option_info[option_info["day_diff"] >= 0]
    option_info = option_info[option_info["day_diff"] == option_info["day_diff"].min()]

    ce_trading_symbol = option_info[(instruments.instrument_type.isin(["CE"]))]
    ce_trading_symbol = ce_trading_symbol["tradingsymbol"]

    print(ce_trading_symbol)

    pe_trading_symbol = option_info[(instruments.instrument_type.isin(["PE"]))]
    pe_trading_symbol = pe_trading_symbol["tradingsymbol"]

    print(pe_trading_symbol)

    json_data_ce = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=kite.EXCHANGE_NFO,
        tradingsymbol=ce_trading_symbol,
        transaction_type=kite.TRANSACTION_TYPE_SELL,
        quantity=30,
        product=kite.PRODUCT_NRML,
        order_type=kite.ORDER_TYPE_MARKET,
        price=0,
    )
    # json_data_ce = json_data_ce["data"]

    print(json_data_ce)
    # orderID_ce = json_data_ce["order_id", "N/A"]
    if json_data_ce["status"] == "success":
        orderID_ce = json_data_ce["data"]["order_id"]
    else:
        orderID_ce = "N/A"

    print("1", orderID_ce)

    json_data_pe = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=kite.EXCHANGE_NFO,
        tradingsymbol=pe_trading_symbol,
        transaction_type=kite.TRANSACTION_TYPE_SELL,
        quantity=30,
        product=kite.PRODUCT_NRML,
        order_type=kite.ORDER_TYPE_MARKET,
        price=0,
    )
    # json_data_pe = json_data_pe["data"]
    print(json_data_pe)
    # orderID_pe = json_data_pe["order_id", "N/A"]
    if json_data_pe["status"] == "success":
        orderID_pe = json_data_pe["data"]["order_id"]
    else:
        orderID_pe = "N/A"

    print("2", orderID_pe)

    time.sleep(2)

    status = 5  # Default to "N/A" if parsing fails
    if orderID_pe and orderID_ce != "N/A":
        status = 1
    elif orderID_ce == "N/A" and orderID_pe != "N/A":
        print("call order not placed")
        status = 2  # ce order failed
    elif orderID_pe == "N/A" and orderID_ce != "N/A":
        print("PUT order not placed")
        status = 3  # pe order failed
    else:
        print("koi bada error aaya h")
        status = 5

    current_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    ce_trading_symbol = option_info[option_info.instrument_type == "CE"][
        "tradingsymbol"
    ].values[0]
    pe_trading_symbol = option_info[option_info.instrument_type == "PE"][
        "tradingsymbol"
    ].values[0]

    order_book = kite.orders()

    df_orders = pd.DataFrame(order_book["data"])
    df_orders["order_id"] = df_orders["order_id"].astype(str)
    orderID_ce = str(orderID_ce)
    orderID_pe = str(orderID_pe)
    print(orderID_ce)
    print("TRUE")
    print(orderID_pe)
    # orderID_pe = "240829200450227"
    # orderID_ce = "240829200449638"

    # Convert order book data to a DataFrame
    df_orders = pd.DataFrame(order_book["data"])

    # Filter rows by order IDs
    ce_order = df_orders[df_orders["order_id"] == orderID_ce]
    pe_order = df_orders[df_orders["order_id"] == orderID_pe]

    # Prepare DataFrame for output with separate column names
    output_df = pd.DataFrame(
        {
            "status": status,
            "entry_time": current_time,
            "strike_price": strike_price,
            "instrument_token_ce": ce_order["instrument_token"].values,
            "tradingsymbol_ce": ce_order["tradingsymbol"].values,
            "orderId_ce": orderID_ce,
            "entry_price_ce": ce_order["average_price"].values,
            "instrument_token_pe": pe_order["instrument_token"].values,
            "tradingsymbol_pe": pe_order["tradingsymbol"].values,
            "orderId_pe": orderID_pe,
            "entry_price_pe": pe_order["average_price"].values,
            "combined_entry": ce_order["average_price"].values
            + pe_order["average_price"].values,
            "combined_min_low": ce_order["average_price"].values
            + pe_order["average_price"].values,
            "combined_SL": (
                ce_order["average_price"].values + pe_order["average_price"].values
            )
            * 1.05,
            "exit_time": "",
            "pe_exit_price": "",
            "ce_exit_price": "",
            "profit_cme": "",
            "profit_SL": "",
            "profit_mkt": "",
        }
    )

    # Save the DataFrame to CSV
    csv_file_path = "straddle_sell_NFO_data_new.csv"  # Specify your desired file path
    df_existing = pd.read_csv(csv_file_path)
    if not df_existing.empty:
        last_row = df_existing.iloc[-1]
        if str(last_row["orderId_ce"]) == str(orderID_ce) and str(
            last_row["orderId_pe"]
        ) == str(orderID_pe):
            print("Duplicate order IDs detected. Returning existing DataFrame.")
            return df_existing

    df = output_df.to_csv(csv_file_path, mode="a", header=False, index=False)
    return df


def get_ltp():

    csv_file_path = "straddle_sell_NFO_data_new.csv"

    number_of_rows = sum(1 for _ in open(csv_file_path))
    if number_of_rows <= 1:
        print("CSV file is empty or has insufficient data.")
        return

    # Read only the last row while preserving the header
    df_csv = pd.read_csv(csv_file_path, skiprows=range(1, number_of_rows - 1))
    if df_csv.empty:
        print("CSV file is empty.")
        return

    last_row = df_csv.iloc[-1]
    print(last_row)

    enc_token = login_kite()

    kite = kt.KiteApp("self", "UZ2906", enc_token)
    kws = kite.kws()
    last_row = df_csv.iloc[-1]

    df_csv["instrument_token_ce"] = (
        df_csv["instrument_token_ce"].astype(str).str.strip()
    )
    df_csv["instrument_token_pe"] = (
        df_csv["instrument_token_pe"].astype(str).str.strip()
    )
    df_csv["tradingsymbol_ce"] = df_csv["tradingsymbol_ce"].str.strip()
    df_csv["tradingsymbol_pe"] = df_csv["tradingsymbol_pe"].str.strip()

    ce_instrument_token = int(last_row["instrument_token_ce"])
    pe_instrument_token = int(last_row["instrument_token_pe"])
    ce_tradingsymbol = f"{last_row['tradingsymbol_ce']}"
    pe_tradingsymbol = f"{last_row['tradingsymbol_pe']}"

    stock = {
        ce_instrument_token: ce_tradingsymbol,
        pe_instrument_token: pe_tradingsymbol,
    }
    ltp_data = {}

    def on_ticks(ws, ticks):
        for symbol in ticks:
            ltp_data[stock[symbol["instrument_token"]]] = {
                "ltp": symbol["last_price"],
                "High": symbol["ohlc"]["high"],
                "Low": symbol["ohlc"]["low"],
            }

    def on_connect(ws, response):
        ws.subscribe(list(stock.keys()))
        ws.set_mode(
            ws.MODE_QUOTE, list(stock.keys())
        )  # MODE_FULL , MODE_QUOTE, MODE_LTP

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect(threaded=True)
    initial_SL_set = False
    while len(ltp_data.keys()) != len(list(stock.keys())):
        time.sleep(2)
        continue

    while last_row["status"] == 1:

        try:
            ltp_ce = (
                ltp_data[list(stock.values())[0]]["ltp"]
                if list(stock.values())[0] in ltp_data
                else None
            )
            ltp_pe = (
                ltp_data[list(stock.values())[1]]["ltp"]
                if list(stock.values())[1] in ltp_data
                else None
            )
            combine_ltp = ltp_ce + ltp_pe
            time_ltp = dt.now()

            print(
                f"CE LTP: {ltp_ce}, PE LTP: {ltp_pe}, Combined LTP: {combine_ltp}, Time: {time_ltp}"
            )
            time.sleep(2)

            last_row = df_csv.iloc[-1]

            if combine_ltp < last_row["combined_min_low"]:
                print("1")

                df_csv.loc[df_csv.index[-1], "combined_min_low"] = combine_ltp
                profit_cme = (
                    df_csv.loc[df_csv.index[-1], "combined_min_low"]
                    - df_csv.loc[df_csv.index[-1], "combined_entry"]
                ) * (-30)

                df_csv.loc[df_csv.index[-1], "profit_cme"] = profit_cme

                if combine_ltp <= last_row["combined_entry"] * 0.95:
                    print("2")
                    #  Set combined_SL to combined_entry for the first time
                    if not initial_SL_set:
                        print("3")
                        df_csv.loc[df_csv.index[-1], "combined_SL"] = last_row[
                            "combined_entry"
                        ]
                        initial_SL_set = True
                    else:
                        print("4")
                        decreased_amount = (
                            last_row["combined_entry"] * 0.95
                        ) - df_csv.loc[df_csv.index[-1], "combined_min_low"]

                        # Update combined_SL to decrease only after it is set to combined_entry
                        df_csv.loc[df_csv.index[-1], "combined_SL"] = (
                            df_csv.loc[df_csv.index[-1], "combined_entry"]
                            - decreased_amount
                        )

                        profit_SL = (
                            df_csv.loc[df_csv.index[-1], "combined_SL"]
                            - df_csv.loc[df_csv.index[-1], "combined_entry"]
                        ) * (-30)

                        df_csv.loc[df_csv.index[-1], "profit_SL"] = profit_SL

            last_row = df_csv.iloc[-1]
            if (
                combine_ltp >= last_row["combined_SL"]
            ):  # df_csv.loc[df_csv.index[-1], "combined_SL"]
                kite = pykite()
                print("Order Placed")
                print("5")
                # ce_trading_symbol = last_row["tradingsymbol_ce"]
                print(ce_tradingsymbol)

                place_order_ce = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NFO,
                    tradingsymbol=ce_tradingsymbol,
                    transaction_type=kite.TRANSACTION_TYPE_BUY,
                    quantity=30,
                    product=kite.PRODUCT_NRML,
                    order_type=kite.ORDER_TYPE_MARKET,
                    price=0,
                )
                print(place_order_ce)
                # pe_trading_symbol = last_row["tradingsymbol_pe"]
                print(pe_tradingsymbol)

                place_order_pe = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NFO,
                    tradingsymbol=pe_tradingsymbol,
                    transaction_type=kite.TRANSACTION_TYPE_BUY,
                    quantity=30,
                    product=kite.PRODUCT_NRML,
                    order_type=kite.ORDER_TYPE_MARKET,
                    price=0,
                )
                print(place_order_pe)
                print("6")
                df_csv.loc[df_csv.index[-1], "status"] = 0

                if place_order_pe["status"] == "success":
                    orderID_pe_buy = place_order_pe["data"]["order_id"]
                if place_order_ce["status"] == "success":
                    orderID_ce_buy = place_order_ce["data"]["order_id"]

                orderbook = kite.orders()

                df_orders_buy = pd.DataFrame(orderbook["data"])
                df_orders_buy["order_id"] = df_orders_buy["order_id"].astype(str)
                orderID_ce_buy = str(orderID_ce_buy)
                orderID_pe_buy = str(orderID_pe_buy)
                # filter buy order IDs wrt their orderbook

                ce_order_buy = df_orders_buy[
                    df_orders_buy["order_id"] == orderID_ce_buy
                ]
                pe_order_buy = df_orders_buy[
                    df_orders_buy["order_id"] == orderID_pe_buy
                ]

                pe_exit_price = (
                    pe_order_buy["average_price"].values[0]
                    if not pe_order_buy.empty
                    else None
                )
                ce_exit_price = (
                    ce_order_buy["average_price"].values[0]
                    if not ce_order_buy.empty
                    else None
                )
                exit_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")

                df_csv.loc[df_csv.index[-1], "pe_exit_price"] = pe_exit_price
                df_csv.loc[df_csv.index[-1], "ce_exit_price"] = ce_exit_price
                df_csv.loc[df_csv.index[-1], "exit_time"] = exit_time
                profit_mkt = (
                    pe_exit_price
                    + ce_exit_price
                    - df_csv.loc[df_csv.index[-1], "combined_entry"]
                ) * 30
                df_csv.loc[df_csv.index[-1], "profit_mkt"] = profit_mkt
                df_csv.to_csv(csv_file_path, index=False)
                PL_datasheet = df_csv.copy()
                PL_datasheet.to_csv("PL_datasheet.csv", mode="a", index=False)
                break

        except KeyError as e:
            print(f"Error accessing LTP data: {e}")
            time.sleep(2)

        print("Updating existing data.")
        df_ltp = df_csv.to_csv(csv_file_path, index=False)

    return df_ltp


csv_file_path = "straddle_sell_NFO_data_new.csv"
n = 0

if os.path.exists(csv_file_path):
    print("file exist")

else:
    headers = [
        "status",
        "entry_time",
        "strike_price",
        "instrument_token_ce",
        "tradingsymbol_ce",
        "orderId_ce",
        "entry_price_ce",
        "instrument_token_pe",
        "tradingsymbol_pe",
        "orderId_pe",
        "entry_price_pe",
        "combined_entry",
        "combined_min_low",
        "combined_SL",
        "exit_time",
        "pe_exit_price",
        "ce_exit_price",
        "profit_cme",
        "profit_SL",
        "profit_mkt",
    ]

    df_straddle = pd.DataFrame(columns=headers)
    df_straddle["status"] = 0
    df_straddle.to_csv(csv_file_path, mode="a", index=False)


def update_status(status):
    """Update the status in the CSV file to the given value."""
    try:
        df = pd.read_csv(csv_file_path)
        df.loc[df.index[-1], "status"] = status
        df.to_csv(csv_file_path, index=False)
    except Exception as e:
        print(f"Error updating status: {e}")


def place_order():
    global n  # Declare n as a global variable
    csv_file_path = "straddle_sell_NFO_data_new.csv"

    try:
        df_straddle = pd.read_csv(csv_file_path)

        if df_straddle is not None and "status" in df_straddle.columns:
            last_status = df_straddle["status"].iloc[-1]
            print(last_status)
            n = n + 1

            if last_status == 1:
                get_ltp()
            if last_status == 0:
                create_straddle()
                time.sleep(5)
                get_ltp()
        else:
            print("DataFrame is None or 'status' column not found")
    except FileNotFoundError:
        print("CSV file not found. Please check the file path.")
        update_status(5)
    except pd.errors.EmptyDataError:
        print("CSV file is empty. Please check the file content.")
        update_status(5)

    except KeyboardInterrupt:
        print("Process interrupted by user.")
        update_status(0)
    except Exception as e:
        print(f"An error occurred: {e}")
        update_status(5)
    return n


while n < 3:
    print("entry_turn: ", n)
    new_strike_price = strike_price_stock()
    df_straddle = pd.read_csv(csv_file_path)
    if df_straddle["status"].iloc[-1] == 0:
        if df_straddle["strike_price"].iloc[-1] != new_strike_price:
            place_order()

        else:
            continue
    elif df_straddle["status"].iloc[-1] == 1:
        get_ltp()

    else:
        break
