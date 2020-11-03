import yfinance as yf
import pymysql
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Numeric
import logging

from db_portfoliotracker import *

logging.basicConfig(filename='error.log', filemode='a', format='FX_RATES: %(name)s - %(levelname)s - %(message)s')


currencies = {'RON','USD','GBP','NOK','SEK','CAD'}



def make_currency_pairs_string(currencies):
	currency_pairs =[]
	for cur in currencies:
		currency_pairs.append(''.join(['EUR', cur, "=X"]))
	return ' '.join(currency_pairs)

currency_pairs = make_currency_pairs_string(currencies)

start ='2010-1-1'
today = date.today()
data = yf.download(currency_pairs, start=start, end= today)
data = data['Close']
data = data.fillna(method='ffill')
new_cols =[c.strip('=X') for c in data.columns]
data.columns = new_cols
data.index.rename('date_col', inplace=True)
print(data)
engine =create_engine(f'mysql+pymysql://{user}:{password}@{host}/{db}')
con = engine.connect()

try:
	data.to_sql(con=con, name='fx_rates', if_exists='append',dtype={'date_col':Date()})
	print('Forex rates updated successfully')
except Exception as e:
	logging.error(e)
finally:
	con.close()
