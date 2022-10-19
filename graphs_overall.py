#!/usr/bin/env python3

import telegram
import tweepy
import mariadb
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from scipy import signal
from credentials import *
from statistics import mean, stdev
from datetime import datetime
from config import *

KRAKEN_AVERAGE_RECORDS = 2500
STAKE_RECORDS = 700

def main():
  conn = connectToMariaDB()
  cur = conn.cursor()

  bot = connectToTelegram()
  api = connectToTwitter()

# PRICE DATA

  print("Querying database for price data...")
  query = f"SELECT Timestamp,TokenPriceBSC,CoinPriceBSC,TokenPriceETH,CoinPriceETH,TokenPricePOLY,CoinPricePOLY,TokenPriceFTM,CoinPriceFTM,TokenPriceAVAX,CoinPriceAVAX FROM price_data ORDER BY TimeStamp desc limit " + str(KRAKEN_AVERAGE_RECORDS)
  cur.execute(query)
  price_data_result = cur.fetchall()
  price_timestamp, token_price_bsc, coin_price_bsc, token_price_eth, coin_price_eth, token_price_poly, coin_price_poly, token_price_ftm, coin_price_ftm, token_price_avax, coin_price_avax  = zip(*price_data_result)

  price_datetime = []
  for timestamp in price_timestamp:
    price_datetime.append(datetime.fromtimestamp(timestamp))

  price_data = {"DateTime":list(price_datetime),"BSC":list(token_price_bsc),"ETH":list(token_price_eth),"POLY":list(token_price_poly),"FTM":list(token_price_ftm),"AVAX":list(token_price_avax)}
  price_dataframe = pd.DataFrame(price_data)
  print(price_dataframe)


  for chain in CHAINS:
    if(chain == "bsc"):
      native = "BNB"
      LINE_COLOR = BSC_COLOR
    if(chain == "eth"):
      native = "ETH"
      LINE_COLOR = ETH_COLOR
    if(chain == "poly"):
      native = "MATIC"
      LINE_COLOR = POLY_COLOR
    if(chain == "ftm"):
      native = "FTM"
      LINE_COLOR = FTM_COLOR
    if(chain == "avax"):
      native = "AVAX"
      LINE_COLOR = AVAX_COLOR

# PRICE DATA BY CHAIN

    globals()[f"{chain}_fig1"] = px.line(price_dataframe, x="DateTime",y=f"{chain.upper()}")
    globals()[f"{chain}_fig1"].update_traces(line_color=LINE_COLOR)

# STAKE DATA BY CHAIN

    query = f"SELECT TimeStamp,{native}Rewards,{native}Performance FROM {chain}_kraken_rewards ORDER BY TimeStamp desc limit " + str(STAKE_RECORDS)
    cur.execute(query)
    globals()[f"{chain}_kraken_rewards"] = cur.fetchall()
    globals()[f"{chain}_timestamp"], globals()[f"{chain}_rewards"], globals()[f"{chain}_performance"] = zip(*globals()[f"{chain}_kraken_rewards"])

    globals()[f"{chain}_datetime"] = []
    for timestamp in globals()[f"{chain}_timestamp"]:
      globals()[f"{chain}_datetime"].append(datetime.fromtimestamp(timestamp))

    globals()[f"{chain}_fig2"] = px.line(x=globals()[f"{chain}_datetime"],y=signal.savgol_filter(globals()[f"{chain}_performance"],12,3))
    globals()[f"{chain}_fig2"].update_traces(line_color=LINE_COLOR)

# MULTICHAIN PRICE GRAPH

  price = make_subplots()
  price.add_traces(globals()[f"{CHAINS[0]}_fig1"].data + globals()[f"{CHAINS[1]}_fig1"].data + globals()[f"{CHAINS[2]}_fig1"].data + globals()[f"{CHAINS[3]}_fig1"].data + globals()[f"{CHAINS[4]}_fig1"].data)
  price.layout.xaxis.title="Date and Time"
  price.layout.yaxis.title="Price"
  for i, chain in enumerate(CHAINS):
    price['data'][i]['showlegend'] = True
    price['data'][i]['name'] = f"{chain.upper()}"
  price.update_layout(font = {'color':FONT_COLOR})
  price.update_layout(paper_bgcolor=PAPER_COLOR)
  price.update_layout(plot_bgcolor=PLOT_COLOR)
  price.update_xaxes(gridcolor=GRID_COLOR)
  price.update_yaxes(gridcolor=GRID_COLOR)
  price.update_layout(title_text="EverRise Token Price")
  price.write_image(PATH + "token_price_overall.png")

# MULTICHAIN PERFORMANCE

  stake_performance = make_subplots()
  stake_performance.add_traces(globals()[f"{CHAINS[0]}_fig2"].data + globals()[f"{CHAINS[1]}_fig2"].data + globals()[f"{CHAINS[2]}_fig2"].data + globals()[f"{CHAINS[3]}_fig2"].data + globals()[f"{CHAINS[4]}_fig2"].data)
  stake_performance.layout.xaxis.title="Date and Time"
  stake_performance.layout.yaxis.title="Percentage Performance"
  for i, chain in enumerate(CHAINS):
    stake_performance['data'][i]['showlegend'] = True
    stake_performance['data'][i]['name'] = f"{chain.upper()}"
  stake_performance.update_layout(title_text="EverRise 36X Stake Percentage Comparison")
  stake_performance.update_layout(font = {'color':FONT_COLOR})
  stake_performance.update_layout(paper_bgcolor=PAPER_COLOR)
  stake_performance.update_layout(plot_bgcolor=PLOT_COLOR)
  stake_performance.update_xaxes(gridcolor=GRID_COLOR)
  stake_performance.update_yaxes(gridcolor=GRID_COLOR)
  stake_performance.write_image(PATH + "multichain_performance_overall.png")

# SEND TELEGRAM MESSAGES
  if(TELEGRAM):
    bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + "token_price_overall.png", "rb"))
    bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + "multichain_performance_overall.png", "rb"))

  if(TWITTER):
    message = '#EverRise $RISE'
    filename = PATH + "token_price_overall.png"
    media = api.media_upload(filename)
    api.update_status(status=message, media_ids=[media.media_id])

    filename = PATH + "multichain_performance_overall.png"
    media = api.media_upload(filename)
    api.update_status(status=message, media_ids=[media.media_id])

def connectToTwitter():
    print("Connecting to Twitter...")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    print("Success!")
    return api

def connectToTelegram():
    print("Connecting to Telegram Bot...")
    try:
      bot = telegram.Bot(token=TELEGRAM_API_KEY)
      print("Success!")
    except telegram.Error as e:
      print(f"Error connecting to Telegram: {e}")
    return bot

def connectToMariaDB():
    print("Connecting to remote MariaDB Instance...")
    try:
      conn = mariadb.connect(
      user=MARIADB_USER,
      password=MARIADB_PW,
      host=MARIADB_HOST,
      port=3306,
      database=MARIADB_DB
    )
      print("Success!")
    except mariadb.Error as e:
      print(f"Error connecting to MariaDB Platform: {e}")
    return conn


if __name__ == "__main__":
  main()
