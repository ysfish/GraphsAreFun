#!/usr/bin/env python3

import asyncio
import json
import telegram
import time
import tweepy
import mariadb
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from credentials import *
from statistics import mean, stdev
from datetime import datetime
from config import *


def main():
  conn = connectToMariaDB()
  cur = conn.cursor()

  bot = connectToTelegram()
  api = connectToTwitter()

# QUERY DATABASE AND BUILD DATAFRAMES

# STAKED TOKEN DATA

  print("Querying database for staked token data...")
  query = f"SELECT DateTime,Timestamp,TotalStaked,BSCStaked,ETHStaked,POLYStaked,FTMStaked,AVAXStaked FROM staked_tokens ORDER BY Timestamp desc limit 336"
  cur.execute(query)
  staked_tokens_result = cur.fetchall()
  staked_tokens_datetime, staked_tokens_timestamp, total_staked, bsc_staked, eth_staked, poly_staked, ftm_staked, avax_staked  = zip(*staked_tokens_result)

  staked_tokens_datetime = []
  for timestamp in staked_tokens_timestamp:
    staked_tokens_datetime.append(datetime.fromtimestamp(timestamp))

  staked_tokens_data = {"DateTime":list(staked_tokens_datetime),"Total":list(total_staked),"BSC":list(bsc_staked),"ETH":list(eth_staked),"POLY":list(poly_staked),"FTM":list(ftm_staked),"AVAX":list(avax_staked)}
  staked_tokens_dataframe = pd.DataFrame(staked_tokens_data)
  print(staked_tokens_dataframe)

# UNIFIED KRAKEN DATA

  print("Querying database for Unified Kraken Data...")
  query = f"SELECT Timestamp,KrakenReservesUSD,TokenPrice,DailyVolumeUSD,DailyVolume,DailyKrakenTax,DailyRewards,KrakenFactor FROM unified_data ORDER BY Timestamp desc limit 60"
  cur.execute(query)
  unified_result = cur.fetchall()
  unified_timestamp, kraken_reserves_usd, token_price, daily_volume_usd, daily_volume, daily_kraken_tax, daily_rewards, kraken_factor = zip(*unified_result)

  unified_datetime = []
  for timestamp in unified_timestamp:
    unified_datetime.append(datetime.fromtimestamp(timestamp))

  unified_data = {"DateTime":list(unified_datetime),"KrakenUSD":list(kraken_reserves_usd),"TokenPrice":list(token_price),"DailyVolumeUSD":list(daily_volume_usd),"DailyVolume":list(daily_volume),"DailyKrakenTax":list(daily_kraken_tax),"DailyRewards":list(daily_rewards),"KrakenFactor":list(kraken_factor)}
  unified_dataframe = pd.DataFrame(unified_data)

# EVERSWAP DATA

  print("Querying Database for EverSwap data...")
  query = f"SELECT TimeStamp, EverSwapTotalUSD, EverSwapBSCinUSD, EverSwapETHinUSD, EverSwapPOLYinUSD, EverSwapFTMinUSD, EverSwapAVAXinUSD from everswap_data ORDER BY TimeStamp desc limit 60"
  cur.execute(query)
  everswap_result = cur.fetchall()
  everswap_timestamp, everswap_total_usd, everswap_bsc_usd, everswap_eth_usd, everswap_poly_usd, everswap_ftm_usd, everswap_avax_usd = zip(*everswap_result)

  everswap_datetime = []
  for timestamp in everswap_timestamp:
    everswap_datetime.append(datetime.fromtimestamp(timestamp))

  everswap_data = {"DateTime":list(everswap_datetime),"EverSwapTotalUSD":list(everswap_total_usd),"EverSwapBSC":list(everswap_bsc_usd),"EverSwapETH":list(everswap_eth_usd),"EverSwapPOLY":list(everswap_poly_usd),"EverSwapFTM":list(everswap_ftm_usd),"EverSwapAVAX":list(everswap_avax_usd)}
  everswap_dataframe = pd.DataFrame(everswap_data)
  print(everswap_dataframe)

# UNIFIED DATA GRAPH CREATION

  fig = px.line(staked_tokens_dataframe,title='EverRise Staked Tokens - Total',x='DateTime',y='Total')
  fig.update_layout(font = {'color':FONT_COLOR})
  fig.update_layout(paper_bgcolor=PAPER_COLOR)
  fig.update_layout(plot_bgcolor=PLOT_COLOR)
  fig.update_xaxes(gridcolor=GRID_COLOR)
  fig.update_yaxes(gridcolor=GRID_COLOR)
  fig.layout.xaxis.title="Date and Time"
  fig.layout.yaxis.title="Staked Tokens"
  fig.update_yaxes(type="log")
  fig.write_image(PATH + "unified_staked_tokens.png")

  fig2 = px.line(unified_dataframe,x='DateTime',y='DailyVolumeUSD')
  fig2.update_layout(font = {'color':FONT_COLOR})
  fig2.update_layout(paper_bgcolor=PAPER_COLOR)
  fig2.update_layout(plot_bgcolor=PLOT_COLOR)
  fig2.update_xaxes(gridcolor=GRID_COLOR)
  fig2.update_yaxes(gridcolor=GRID_COLOR)
  fig2.write_image(PATH + "unified_daily_volume.png")

  fig3 = px.line(unified_dataframe,x='DateTime',y='KrakenFactor')
  fig3.update_layout(font = {'color':FONT_COLOR})
  fig3.update_layout(paper_bgcolor=PAPER_COLOR)
  fig3.update_layout(plot_bgcolor=PLOT_COLOR)
  fig3.update_xaxes(gridcolor=GRID_COLOR)
  fig3.update_yaxes(gridcolor=GRID_COLOR)
  fig3.write_image(PATH + "unified_kraken_factor.png")

  for chain in CHAINS:
    if chain == "bsc":
      chain_name = "BNB Chain"
      LINE_COLOR = BSC_COLOR
    if chain == "eth":
      chain_name = "Ethereum"
      LINE_COLOR = ETH_COLOR
    if chain == "poly":
      chain_name = "Polygon"
      LINE_COLOR = POLY_COLOR
    if chain == "ftm":
      chain_name = "Fantom"
      LINE_COLOR = FTM_COLOR
    if chain == "avax":
      chain_name = "Avalanche"
      LINE_COLOR = AVAX_COLOR

# STAKED TOKENS GRAPHS

    fig = px.line(staked_tokens_dataframe,title=f'EverRise Staked Tokens - {chain.upper()}',x='DateTime',y=chain.upper())
    fig.update_traces(line_color=LINE_COLOR)
    fig.update_layout(font = {'color':FONT_COLOR})
    fig.update_layout(paper_bgcolor=PAPER_COLOR)
    fig.update_layout(plot_bgcolor=PLOT_COLOR)
    fig.update_xaxes(gridcolor=GRID_COLOR)
    fig.update_yaxes(gridcolor=GRID_COLOR)
    fig.write_image(PATH + f"{chain}_staked_tokens.png")

# BLOCKCHAIN DATA BY BLOCKCHAIN

    print(f"Querying database for {chain.upper()} Kraken Data...")
    query = f"SELECT DateTime,Timestamp,KrakenReserves{chain.upper()},TokenPrice{chain.upper()},CoinPrice{chain.upper()},DailyVolume{chain.upper()},DailyVolume{chain.upper()}inUSD,DailyKrakenTax{chain.upper()},DailyRewards{chain.upper()},KrakenFactor{chain.upper()} FROM {chain}_data ORDER BY Timestamp desc limit 60"
    cur.execute(query)
    result = cur.fetchall()
    globals()[f"{chain}_datetime"], globals()[f"{chain}_timestamp"], globals()[f"{chain}_kraken_reserves"], globals()[f"{chain}_token_price"], globals()[f"{chain}_coin_price"], globals()[f"{chain}_daily_volume"], globals()[f"{chain}_daily_volume_usd"], globals()[f"{chain}_daily_kraken_tax"], globals()[f"{chain}_daily_rewards"], globals()[f"{chain}_kraken_factor"] = zip(*result)

    globals()[f"{chain}_datetime"] = []
    for timestamp in globals()[f"{chain}_timestamp"]:
      globals()[f"{chain}_datetime"].append(datetime.fromtimestamp(timestamp))

    globals()[f"{chain}_data"] = {"DateTime":list(globals()[f"{chain}_datetime"]),f"KrakenReserves{chain.upper()}":list(globals()[f"{chain}_kraken_reserves"]),f"TokenPrice{chain.upper()}":list(globals()[f"{chain}_token_price"]),f"CoinPrice{chain.upper()}":list(globals()[f"{chain}_coin_price"]),f"DailyVolume{chain.upper()}":list(globals()[f"{chain}_daily_volume"]),f"DailyVolume{chain.upper()}inUSD":list(globals()[f"{chain}_daily_volume_usd"]),f"DailyKrakenTax{chain.upper()}":list(globals()[f"{chain}_daily_kraken_tax"]),f"DailyRewards{chain.upper()}":list(globals()[f"{chain}_daily_rewards"]),f"KrakenFactor{chain.upper()}":list(globals()[f"{chain}_kraken_factor"])}
    globals()[f"{chain}_dataframe"] = pd.DataFrame(globals()[f"{chain}_data"])
    print(globals()[f"{chain}_dataframe"])

    globals()[f"{chain}_fig1"] = px.line(globals()[f"{chain}_dataframe"], x="DateTime",y=f"KrakenReserves{chain.upper()}", title=f"Kraken Reserves, {chain_name}")
    globals()[f"{chain}_fig2"] = px.line(globals()[f"{chain}_dataframe"], x="DateTime",y=f"DailyVolume{chain.upper()}",title=f"EverRise Daily Volume in USD, {chain_name}")
    globals()[f"{chain}_fig3"] = px.line(globals()[f"{chain}_dataframe"], x="DateTime",y=f"KrakenFactor{chain.upper()}",title=f'EverRise Kraken Factor, {chain_name}')
    globals()[f"{chain}_fig4"] = px.line(everswap_dataframe,x="DateTime",y=f"EverSwap{chain.upper()}")

    index = ["1", "2", "3", "4"]
    for item in index:
      globals()[f"{chain}_fig{item}"].update_traces(line_color=LINE_COLOR)
      globals()[f"{chain}_fig{item}"].update_layout(font = {'color':FONT_COLOR})
      globals()[f"{chain}_fig{item}"].update_layout(paper_bgcolor=PAPER_COLOR)
      globals()[f"{chain}_fig{item}"].update_layout(plot_bgcolor=PLOT_COLOR)
      globals()[f"{chain}_fig{item}"].update_xaxes(gridcolor=GRID_COLOR)
      globals()[f"{chain}_fig{item}"].update_yaxes(gridcolor=GRID_COLOR)
      globals()[f"{chain}_fig{item}"].layout.xaxis.title="Date and Time"
    globals()[f"{chain}_fig1"].write_image(PATH + f"{chain}_kraken_from_stats.png")
    globals()[f"{chain}_fig2"].write_image(PATH + f"{chain}_daily_volume.png")
    globals()[f"{chain}_fig3"].write_image(PATH + f"{chain}_kraken_factor.png")
    globals()[f"{chain}_fig4"].write_image(PATH + f"{chain}_everswap.png")

# KRAKEN FACTOR MULTICHAIN

  fig16 = make_subplots()
  fig16.add_traces(globals()[f"{CHAINS[0]}_fig3"].data + globals()[f"{CHAINS[1]}_fig3"].data + globals()[f"{CHAINS[2]}_fig3"].data + globals()[f"{CHAINS[3]}_fig3"].data + globals()[f"{CHAINS[4]}_fig3"].data)
  index = [0, 1, 2, 3, 4]
  for i, chain in enumerate(CHAINS):
    fig16['data'][i]['showlegend'] = True
    fig16['data'][i]['name'] = f"{chain.upper()}"
  fig16.layout.xaxis.title="Date and Time"
  fig16.update_layout(title_text="EverRise Kraken Factor")
  fig16.update_layout(font = {'color':FONT_COLOR})
  fig16.update_layout(paper_bgcolor=PAPER_COLOR)
  fig16.update_layout(plot_bgcolor=PLOT_COLOR)
  fig16.update_xaxes(gridcolor=GRID_COLOR)
  fig16.update_yaxes(gridcolor=GRID_COLOR)
  fig16.write_image(PATH + "multichain_kraken_factor.png")

# SEND TELEGRAM MESSAGES

  if(TELEGRAM):
    bot.send_message(chat_id=CHAT_ID, text="*Daily Update:*", parse_mode=telegram.ParseMode.MARKDOWN_V2)
    bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + 'unified_staked_tokens.png', 'rb'))
    for chain in CHAINS:
      bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + f"{chain}_staked_tokens.png", "rb"))
      time.sleep(1)
      bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + f"{chain}_daily_volume.png", "rb"))
      time.sleep(1)
    bot.send_message(chat_id=CHAT_ID, text="*Kraken Factor:*", parse_mode=telegram.ParseMode.MARKDOWN_V2)
    bot.unpin_chat_message(chat_id=CHAT_ID)
    message = bot.send_message(chat_id=CHAT_ID, text=f"KF = (DailyVolume * {TAX_RATE} - DailyRewards) / KrakenReserves * 100")
    bot.pin_chat_message(chat_id=CHAT_ID, message_id=message.message_id, disable_notification=True)
    for chain in CHAINS:
      bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + f"{chain}_kraken_factor.png", "rb"))
#    bot.send_photo(chat_id=CHAT_ID, photo=open(PATH + 'multichain_kraken_factor.png', 'rb'))

  if(TWITTER):
    message = "#EverRise $RISE"
    filename = PATH + "unified_staked_tokens.png"
    media = api.media_upload(filename)
    api.update_status(status=message, media_ids=[media.media_id])
    for chain in CHAINS:
      filename = PATH + f"{chain}_staked_tokens.png"
      media = api.media_upload(filename)
      api.update_status(status=message, media_ids=[media.media_id])

      filename = PATH + f"{chain}_daily_volume.png"
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
