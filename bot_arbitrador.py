import numpy as np
import pandas as pd
import requests
import itertools
import datetime
import time
import random
import schedule
import sys

coin = ['BTC', 'ETH', 'DAI', 'USDT', 'USDC']
fiat = ['ARS', 'USD']
volume = [1]

values_combination = list(itertools.product(coin, fiat, volume))


def get_data():
    """Loop over url's in Criptoya API, and parse data into a DataFrame."""
    
    ## Loop over url's and parse data into a Data Frame
    Cotizaciones = pd.DataFrame()
    
    for coin, fiat, volume in values_combination:
       
        url = "https://criptoya.com/api/"+ coin + "/" + fiat + "/" + str(volume)
        
        ## Exponential back off to conquer API rate limit
        ## (https://medium.com/clover-platform-blog/conquering-api-rate-limiting-dcac5552714d)
        
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts:
            
            answer_api = requests.get(url)
            
            if answer_api.status_code != 429:
                break
            
            # If rate limited, wait and try again
            time.sleep((2 ** attempts) + random.random())
            attempts = attempts + 1
            
    
        ## Check if request was successful and retrieves data
        
        if (answer_api.status_code != 200 or (answer_api.text == '' or answer_api.text.lower().find('invalid') != -1)):
            
            empty_record = {'ask': np.nan,
                            'totalAsk': np.nan,
                            'bid': np.nan,
                            'totalBid': np.nan,
                            'time': time.time(),
                            'response_code': answer_api.status_code,
                            'coin': coin,
                            'fiat': fiat,
                            'volume': volume,
                            'exchange': None}
            
            Cotizaciones_intermedio = pd.Series(empty_record)
            
            
        else:
            
            Cotizaciones_intermedio = pd.DataFrame(answer_api.json()).T
            Cotizaciones_intermedio['response_code'] = answer_api.status_code
            Cotizaciones_intermedio['coin'] = coin
            Cotizaciones_intermedio['fiat'] = fiat
            Cotizaciones_intermedio['volume'] = volume
            Cotizaciones_intermedio['exchange'] = Cotizaciones_intermedio.index
        
           
        Cotizaciones = Cotizaciones.append(Cotizaciones_intermedio, ignore_index=True)
        
        return Cotizaciones
    



def calc_percent(df):
    
    Cotizaciones_full = df.dropna()
    
    ## Join the result DataFrame to itself by a common key in all rows to 
    ## have a new table with the Cartesian Product of the rows. With the result,
    ## perform calculations on all the possible combinations to see if an opportunity
    ## arises.
    
    
    Cotizaciones_full_percent = pd.merge(Cotizaciones_full, Cotizaciones_full,
                                         how='inner',
                                         left_on=['response_code', 'coin', 'fiat'], 
                                         right_on=['response_code', 'coin', 'fiat'],
                                         suffixes=('_buy', '_sell'))
    
    
    Cotizaciones_full_percent['Percent'] = (Cotizaciones_full_percent['totalBid_sell'] / Cotizaciones_full_percent['totalAsk_buy'])-1
    
    return Cotizaciones_full_percent


def filter_values(df, min_gain_percent):
    
    ## Filter opportunities with a min gain percent treshold and drop those involving "sesocio"
    filters= (df['Percent']>min_gain_percent) & (df['exchange_buy'] != "sesocio")
    Arbitrajes = df[filters]
    Arbitrajes = Arbitrajes.sort_values('Percent', ascending = False).reset_index()
    
    return Arbitrajes



def telegram_bot_sendtext(bot_message):
    
    ## https://medium.com/@ManHay_Hong/how-to-create-a-telegram-bot-and-send-messages-with-python-4cf314d9fa3e
    
    bot_token = sys.argv[1]
    bot_chatID = sys.argv[2]
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()




def flow():
    
    Arbitrajes = filter_values(df=calc_percent(get_data()), min_gain_percent=0.02)
    
    if len(Arbitrajes)==0:
        
        message="Sin oportunidades detectadas"
        telegram_bot_sendtext(message)
        
    else:
            
        for index, row in Arbitrajes.iterrows():
            message="Oportunidad de arbitraje: COMPRAR "+ str(row['coin']) + " en " + str(row['exchange_buy']).upper() + " por " + str("${:,.2f}".format(row['totalAsk_buy'])) + " y vender en " + str(row['exchange_sell']).upper() + " por " + str("${:,.2f}".format(row['totalBid_sell'])) + ". Ganancia estimada: " + "*" + str("{:.2%}".format(row['Percent'])) + "*"
            telegram_bot_sendtext(message)                                                                                                          




## Schedule job
schedule.every(1).minutes.do(flow)

while True:
    if datetime.datetime.now().time().hour>=9 & datetime.datetime.now().time().hour<=23:
        schedule.run_pending()
        time.sleep(30)
    else:
        time.sleep(60)
