import numpy as np
import pandas as pd
import requests
import itertools
import datetime
import time
import random
import schedule
#import sys
import os

pd.options.mode.chained_assignment = None
MAX_ATTEMPTS = 10

coin = ['BTC', 'ETH', 'DAI', 'USDT', 'USDC']
fiat = ['ARS', 'USD']
volume = [1]

values_combination = list(itertools.product(coin, fiat, volume))

def make_request(url):
    attempts = 0
    while attempts < MAX_ATTEMPTS:
        response = requests.get(url)
        if response.status_code == 200 and response.text != '' and response.text.lower().find('invalid') == -1:
            return response.json()

        ## Exponential back off to conquer API rate limit
        ## (https://medium.com/clover-platform-blog/conquering-api-rate-limiting-dcac5552714d)
        time.sleep((2 ** attempts) + random.random())
        attempts = attempts + 1

def get_data():
    """Loop over url's in Criptoya API, and parse data into a DataFrame."""
    ## Loop over url's and parse data into a Data Frame
    rows = []
    for coin, fiat, volume in values_combination:
        url = "https://criptoya.com/api/"+ coin + "/" + fiat + "/" + str(volume)
        coin_fiat_data = make_request(url)
        if coin_fiat_data is not None:
            for exchange, values in coin_fiat_data.items():
                row = [exchange, coin, fiat, values['ask'], values['bid'], values['totalAsk'], values['totalBid'], values['time']]
                rows.append(row)

    return pd.DataFrame(rows, columns=['exchange', 'coin', 'fiat', 'ask', 'bid', 'total_ask', 'total_bid', 'time'])

def calc_percent(df):
    ## Join the result DataFrame to itself by a common key in all rows to 
    ## have a new table with the Cartesian Product of the rows. With the result,
    ## perform calculations on all the possible combinations to see if an opportunity
    ## arises.
    merged = df.merge(df, left_on=['coin', 'fiat'], right_on=['coin', 'fiat'], suffixes=['_buy', '_sell'])
    arbitrages = merged[merged['total_ask_buy'] < merged['total_bid_sell']]
    arbitrages['difference'] = arbitrages['total_bid_sell'] - arbitrages['total_ask_buy']
    arbitrages['percent'] = (arbitrages['total_bid_sell'] / arbitrages['total_ask_buy']) - 1

    return arbitrages

def filter_values(df, min_gain_percent):
    ## Filter opportunities with a min gain percent threshold and drop those involving "sesocio"
    filters = (df['percent'] > min_gain_percent) & (df['exchange_buy'] != "sesocio")
    Arbitrajes = df[filters]
    return Arbitrajes.sort_values('percent', ascending = False).reset_index()


def telegram_bot_sendtext(bot_message, bot_token, bot_chat_id):
    ## https://medium.com/@ManHay_Hong/how-to-create-a-telegram-bot-and-send-messages-with-python-4cf314d9fa3e
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chat_id + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    if response.status_code != 200:
        print('Error trying to use Telegram API ' + response.text)
    return response.json()

def get_parameters():
    api_id = os.getenv('TELEGRAM_API_ID')
    chat_id = os.getenv('TELEGRAM_API_CHATID')
    min_gain_percent = 0.04 if os.getenv('MIN_GAIN_PERCENT') is None else float(os.getenv('MIN_GAIN_PERCENT'))
    return api_id, chat_id, min_gain_percent

def flow():
    opportunities = calc_percent(get_data())
    bot_id, chat_id, min_gain_percent = get_parameters()

    Arbitrajes = filter_values(opportunities, min_gain_percent)
    
    if len(Arbitrajes)!=0: 
        for index, row in Arbitrajes.iterrows():
            message="Oportunidad de arbitraje: COMPRAR "+ str(row['coin']) + " en " + str(row['exchange_buy']).upper() + " por " + str("${:,.2f}".format(row['total_ask_buy'])) + " y vender en " + str(row['exchange_sell']).upper() + " por " + str("${:,.2f}".format(row['total_bid_sell'])) + ". Ganancia estimada: " + "*" + str("{:.2%}".format(row['percent'])) + "*"
            if bot_id is not None and chat_id is not None:
                telegram_bot_sendtext(message, bot_id, chat_id)
            else:
                print(message)


##Considering that Github Actions cancels jobs with more than 6 hrs running
script_end_time = datetime.datetime.now()+datetime.timedelta(hours=5, minutes=55)    

## Schedule job
schedule.every(1).minutes.do(flow)

while datetime.datetime.now() < script_end_time:
    schedule.run_pending()
    time.sleep(30)
