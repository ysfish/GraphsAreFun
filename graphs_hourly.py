#!/usr/bin/env python3

import telegram
import tweepy
import mariadb
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from credentials import *
from statistics import mean, stdev
from datetime import datetime
from config import *
from scipy import signal

RECORDS_TO_RETURN = 168

def main():
  conn = connectToMariaDB()
  cur = conn.cursor()

  bot = connectToTelegram()
  api = connectToTwitter()

  print("Querying database for TimeStamp data...")
  query = f"SELECT TimeStamp,BlockNumber,TimeStampAverage,BlockNumberAverage FROM kraken_average ORDER BY TimeStamp desc limit " + str(RECORDS_TO_RETURN)
  cur.execute(query)
  bsc_averages = cur.fetchall()
  bsc_timestamp, bsc_blocknumber, bsc_timestamp_average, bsc_blocknumber_average = zip(*bsc_averages)
  bsc_timestamp_average_hours = []
  for number in bsc_timestamp_average:
    bsc_timestamp_average_hours.append(number / 3600)

  bsc_datetime = []
  for timestamp in bsc_timestamp:
    bsc_datetime.append(datetime.fromtimestamp(timestamp))

  print("Querying database for Liquidity data...")
  query = f"SELECT Timestamp,LiquidityTokenBSC,LiquidityTokenETH,LiquidityTokenPOLY,LiquidityTokenFTM,LiquidityTokenAVAX FROM liquidity ORDER BY TimeStamp desc limit " + str(RECORDS_TO_RETURN)
  cur.execute(query)
  liquidity_token_result = cur.fetchall()
  liquidity_token_timestamp, liquidity_token_bsc, liquidity_token_eth, liquidity_token_poly, liquidity_token_ftm, liquidity_token_avax = zip(*liquidity_token_result)
  liquidity_datetime = []
  for timestamp in liquidity_token_timestamp:
    liquidity_datetime.append(datetime.fromtimestamp(timestamp))
  
  liquidity_data = {"DateTime":liquidity_datetime, "LiquidityTokenBSC": liquidity_token_bsc, "LiquidityTokenETH": liquidity_token_eth, "LiquidityTokenPOLY": liquidity_token_poly, "LiquidityTokenFTM": liquidity_token_ftm,  "LiquidityTokenAVAX": liquidity_token_avax}
  liquidity_dataframe = pd.DataFrame(liquidity_data)
  print(liquidity_dataframe)

  print("Querying database for price data...")
  query = f"SELECT Timestamp,TokenPriceBSC,CoinPriceBSC,TokenPriceETH,CoinPriceETH,TokenPricePOLY,CoinPricePOLY,TokenPriceFTM,CoinPriceFTM,TokenPriceAVAX,CoinPriceAVAX FROM price_data ORDER BY TimeStamp desc limit " + str(RECORDS_TO_RETURN)
  cur.execute(query)
  price_data_result = cur.fetchall()
  price_timestamp, token_price_bsc, coin_price_bsc, token_price_eth, coin_price_eth, token_price_poly, coin_price_poly, token_price_ftm, coin_price_ftm, token_price_avax, coin_price_avax  = zip(*price_data_result)

  price_datetime = []
  for timestamp in price_timestamp:
    price_datetime.append(datetime.fromtimestamp(timestamp))

  price_data = {"DateTime": price_datetime, "TokenPriceBSC": signal.savgol_filter(token_price_bsc,12,3), "TokenPriceETH": signal.savgol_filter(token_price_eth,12,3), "TokenPricePOLY": signal.savgol_filter(token_price_poly,12,3), "TokenPriceFTM": signal.savgol_filter(token_price_ftm,12,3), "TokenPriceAVAX": signal.savgol_filter(token_price_avax,12,3)}
  price_dataframe = pd.DataFrame(price_data)
  print(price_dataframe)

# KABOOM DATA - TIME AND BLOCKS BETWEEN KABOOMS

  print("Creating Time Between Kabooms graph...")
  time_between_kabooms = px.line(x=bsc_datetime, y=bsc_timestamp_average_hours, title="Time Between Kraken Buybacks", labels={"x": "Date and Time", "y": "Hours Between Kabooms"})
  time_between_kabooms.update_layout(font = {'color':FONT_COLOR})
  time_between_kabooms.update_layout(paper_bgcolor=PAPER_COLOR)
  time_between_kabooms.update_layout(plot_bgcolor=PLOT_COLOR)
  time_between_kabooms.update_xaxes(gridcolor=GRID_COLOR)
  time_between_kabooms.update_yaxes(gridcolor=GRID_COLOR)
  time_between_kabooms.write_image(PATH + "datetime_delta.png")

  print("Creating Blocks Between Kabooms graph...")
  blocks_between_kabooms = px.line(x=bsc_datetime, y=bsc_blocknumber_average, title="Blocks Between Kraken Buybacks", labels={"x": "Date and Time", "y": "BSC Blocks Between Kabooms"})
  blocks_between_kabooms.update_layout(font = {'color':FONT_COLOR})
  blocks_between_kabooms.update_layout(paper_bgcolor=PAPER_COLOR)
  blocks_between_kabooms.update_layout(plot_bgcolor=PLOT_COLOR)
  blocks_between_kabooms.update_xaxes(gridcolor=GRID_COLOR)
  blocks_between_kabooms.update_yaxes(gridcolor=GRID_COLOR)
  blocks_between_kabooms.write_image(PATH + "blocknumber_delta.png")

# MULTICHAIN PERFORMANCE

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

# STAKE PERFORMANCE BY CHAIN

    print(f"Querying database for {chain.upper()} Stake Performance Data...")
    query = f"SELECT TimeStamp,{native}Rewards,{native}Performance FROM {chain}_kraken_rewards ORDER BY TimeStamp desc limit " + str(RECORDS_TO_RETURN)
    cur.execute(query)
    kraken_rewards = cur.fetchall()
    globals()[f"{chain}_timestamp"], globals()[f"{chain}_rewards"], globals()[f"{chain}_performance"] = zip(*kraken_rewards)
    globals()[f"{chain}_datetime"] = []
    for stamp in globals()[f"{chain}_timestamp"]:
      globals()[f"{chain}_datetime"].append(datetime.fromtimestamp(stamp))
    globals()[f"{chain}_fig1"] = px.line(x=globals()[f"{chain}_datetime"],y=globals()[f"{chain}_performance"])
    globals()[f"{chain}_fig1"].update_traces(line_color=LINE_COLOR)

# LIQUIDITY BY CHAIN

    globals()[f"{chain}_fig2"] = px.line(liquidity_dataframe, x="DateTime",y=f"LiquidityToken{chain.upper()}")
    globals()[f"{chain}_fig2"].update_traces(line_color=LINE_COLOR)

# PRICE BY CHAIN

    globals()[f"{chain}_fig3"] = px.line(price_dataframe, x="DateTime",y=f"TokenPrice{chain.upper()}")
    globals()[f"{chain}_fig3"].update_traces(line_color=LINE_COLOR)
      
# MULTICHAIN PERFORMANCE

  print("Creating New MultiChain Performance graph...")
  multichain_performance = make_subplots()
  multichain_performance.add_traces(globals()[f"{CHAINS[0]}_fig1"].data + globals()[f"{CHAINS[1]}_fig1"].data + globals()[f"{CHAINS[2]}_fig1"].data + globals()[f"{CHAINS[3]}_fig1"].data + globals()[f"{CHAINS[4]}_fig1"].data)
  multichain_performance.layout.xaxis.title="Date and Time"
  multichain_performance.layout.yaxis.title="Percentage Performance"
  for i, chain in enumerate(CHAINS):
    multichain_performance['data'][i]['showlegend'] = True
    multichain_performance['data'][i]['name'] = f"{chain.upper()}"
  multichain_performance.update_layout(font = {'color':FONT_COLOR})
  multichain_performance.update_layout(paper_bgcolor=PAPER_COLOR)
  multichain_performance.update_layout(plot_bgcolor=PLOT_COLOR)
  multichain_performance.update_xaxes(gridcolor=GRID_COLOR)
  multichain_performance.update_yaxes(gridcolor=GRID_COLOR)
  multichain_performance.update_layout(title_text="EverRise 36X Stake Percentage Comparison")
  multichain_performance.write_image(PATH + "multichain_performance.png")

# MULTICHAIN LIQUIDITY
  
  liquidity = make_subplots()
  liquidity.add_traces(globals()[f"{CHAINS[0]}_fig2"].data + globals()[f"{CHAINS[1]}_fig2"].data + globals()[f"{CHAINS[2]}_fig2"].data + globals()[f"{CHAINS[3]}_fig2"].data + globals()[f"{CHAINS[4]}_fig2"].data)
  liquidity.layout.xaxis.title="Date and Time"
  liquidity.layout.yaxis.title="Tokens"
  for i, chain in enumerate(CHAINS):
    liquidity['data'][i]['showlegend'] = True
    liquidity['data'][i]['name'] = f"{chain.upper()}"
  liquidity.update_layout(font = {'color':FONT_COLOR})
  liquidity.update_layout(paper_bgcolor=PAPER_COLOR)
  liquidity.update_layout(plot_bgcolor=PLOT_COLOR)
  liquidity.update_xaxes(gridcolor=GRID_COLOR)
  liquidity.update_yaxes(gridcolor=GRID_COLOR, type="log")
  liquidity.update_layout(title_text="EverRise Liquidity in Tokens")
  liquidity.write_image(PATH + "liquidity_token.png")

# PRICE DATA

  price = make_subplots()
  price.add_traces(globals()[f"{CHAINS[0]}_fig3"].data + globals()[f"{CHAINS[1]}_fig3"].data + globals()[f"{CHAINS[2]}_fig3"].data + globals()[f"{CHAINS[3]}_fig3"].data + globals()[f"{CHAINS[4]}_fig3"].data)
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
  price.write_image(PATH + "token_price_history.png")

# SEND TELEGRAM MESSAGES

  if(TELEGRAM):
    bot.send_message(chat_id=CHAT_ID, text="*Hourly Update:*", parse_mode=telegram.ParseMode.MARKDOWN)
    bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + "datetime_delta.png", "rb"))
    bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + "liquidity_token.png", "rb"))
    bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + "token_price_history.png", "rb"))
    bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + "multichain_performance.png", "rb"))

  if(TWITTER):
    message = '#EverRise $RISE'
    filename = PATH + "datetime_delta.png"
    media = api.media_upload(filename)
    api.update_status(status=message, media_ids=[media.media_id])

    filename = PATH + "liquidity_token.png"
    media = api.media_upload(filename)
    api.update_status(status=message, media_ids=[media.media_id])

    filename = PATH + "token_price_history.png"
    media = api.media_upload(filename)
    api.update_status(status=message, media_ids=[media.media_id])

    filename = PATH + "multichain_performance.png"
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
