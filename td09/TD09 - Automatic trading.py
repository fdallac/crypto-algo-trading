# import packages
import json, time, requests, base64, hmac, hashlib, sqlite3, asyncio
import dateutil.parser as dp
from requests.auth import AuthBase
from res.config import config

# set parameters
pub_url = config['API_PUB_URL']
pro_url = config['API_PRO_URL']
exchange_name = config['EXCHANGE_NAME']
pair = config['PAIR']
candle_duration = config['CANDLE_DURATION']

# create custom authentication
class Auth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or b'').decode()
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode()

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request

auth = Auth(config['API_KEY'], config['API_SECRET'], config['API_PASS'])

# connect to SQL database
connection = sqlite3.connect('test.db')
c = connection.cursor()

# create update table
c.execute('''CREATE TABLE IF NOT EXISTS last_checks
    (Id INTEGER PRIMARY KEY AUTOINCREMENT, exchange TEXT, trading_pair TEXT, duration TEXT, table_name TEXT, last_check INT,
    startdate INT, last_id INT)''')

# create candles table
candles_table_name = str(exchange_name).replace('-', '_') + '_' + str(pair).replace('-', '_') + '_Candles_'+ str(candle_duration)
table_creation_statement = '''CREATE TABLE IF NOT EXISTS ''' + candles_table_name + \
    '''(Id INTEGER PRIMARY KEY AUTOINCREMENT, date INT, high REAL, low REAL, open REAL, close REAL, volume REAL,
    quotevolume REAL, weightedaverage REAL, sma_7 REAL, ema_7 REAL, sma_30 REAL, ema_30 REAL, sma_200 REAL, ema_200 REAL)'''
c.execute(table_creation_statement)

# create trades table
trades_table_name = str(exchange_name) + '_' + str(pair).replace('-', '_') + '_Trades'
table_creation_statement = '''CREATE TABLE IF NOT EXISTS ''' + trades_table_name + \
    '''(Id INTEGER PRIMARY KEY AUTOINCREMENT, uuid TEXT, traded_btc REAL, price REAL, created_at_int INT, side TEXT)'''
c.execute(table_creation_statement)


### ::: functions :::

def listCurrencies(api_url=pro_url):
    # call API
    res = requests.get(api_url + 'currencies').json()
    # return tickers for currencies
    return [r['id'] for r in res]

def listCryptoCurrencies(api_url=pro_url):
    # call API
    res = requests.get(pro_url + 'currencies').json()
    # return tickers only for crypto-type currencies
    return [r['id'] for r in res if (r['details']['type']=='crypto')]

def getDepth(direction, pair=pair, api_url=pub_url):
    # :direction: must be 'mid', 'bid', 'ask'
    # :pair: is like 'BTC-USD'

    Dict = {'mid': 'spot', 'bid': 'sell', 'ask': 'buy'}
    # call Coinbase API
    res = requests.get(api_url + 'prices/{0}/{1}'.format(pair, Dict[direction])).json()['data']
    timestamp = requests.get(api_url + 'time').json()['data']
    # add timestamp
    res.update(timestamp)
    # return dict like {'base', 'currency', 'amount', 'iso', 'epoch'}
    return res

def listTradablePairs(api_url=pro_url):
    res = requests.get(api_url + 'products').json()
    return [r['id'] for r in res]

def getOrderBook(pair, level=2, api_url=pro_url):
    # :pair: is in the form 'BTC-USD'
    # [:level: see: https://docs.pro.coinbase.com/#get-product-order-book]

    res = requests.get(api_url + 'products/{0}/book?level={1}'.format(pair, level)).json()
    return res

def refreshDataCandles(pair=pair, duration=candle_duration, cursor=c, table=candles_table_name, api_url=pro_url):
    # :pair: is like 'BTC-USD'
    # :duration: in seconds

    # call API
    res = requests.get(api_url + 'products/{0}/candles?granularity{1}'.format(pair, duration)).json()
    # check if new candles before to update the db
    last_date = cursor.execute('''SELECT date FROM ''' + table + ''' ORDER BY date DESC LIMIT 1''').fetchone()
    if last_date is None:
        last_date = [-1,]
    if (res[0][0] != last_date[0]):
        # insert data in db
        for r in res:
            cursor.execute('''INSERT INTO ''' + table \
                + '''(date,high,low,open,close,volume,quotevolume,weightedaverage,sma_7,ema_7,sma_30,ema_30,sma_200,ema_200)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                [r[0], r[2], r[1], r[3], r[4], r[5], 0, 0, 0, 0, 0, 0, 0, 0])
        # get last id
        last_id = cursor.lastrowid
        # insert update in db
        cursor.execute('''INSERT INTO last_checks(exchange,trading_pair,duration,table_name,last_check,startdate,last_id)
            VALUES(?,?,?,?,?,?,?)''',
            [exchange_name, pair, duration, table, int(time.time()), res[-1][0], last_id])

def refreshData(pair=pair, cursor=c, table=trades_table_name, api_url=pro_url):
    # :pair: is like 'BTC-USD'
    
    # call API
    res = requests.get(api_url + 'products/{0}/trades'.format(pair)).json()
    # insert data in db
    for r in res:
        cursor.execute('''INSERT INTO ''' + table \
            + '''(uuid,traded_btc,price,created_at_int,side) VALUES (?,?,?,?,?)''',
            [r['trade_id'], r['size'], r['price'], int(dp.parse(r['time']).timestamp()), r['side']])
    # get last id
    last_id = cursor.lastrowid
    # insert update in db
    cursor.execute('''INSERT INTO last_checks(exchange,trading_pair,duration,table_name,last_check,startdate,last_id)
        VALUES(?,?,?,?,?,?,?)''',
        [exchange_name, pair, 0, table, int(time.time()), int(dp.parse(res[-1]['time']).timestamp()), last_id])

def createOrder(direction, price, amount, order_type, pair=pair, auth=auth, api_url=pro_url):
    # :direction: must be 'buy' or 'sell'
    # :order_type: must be 'limit' or 'market', see: https://docs.pro.coinbase.com/#place-a-new-order

    # set order
    order = {
    'size': amount,
    'price': price,
    'side': direction,
    'product_id': pair,
    'type': order_type
    }
    # call API
    res = requests.post(api_url + 'orders', json=order, auth=auth).json()
    # return order_id
    return res

def cancelOrder(order_id, auth=auth, api_url=pro_url):
    # :order_id: is returned by createOrder()

    res = requests.post(api_url + 'orders/{}'.format(order_id), auth=auth).json()
    return res



# ::: debug test :::

# print cryptocurrency
print('\n\nExchange Market: {}\n'.format(exchange_name))
print('List of cryptocurrencies:\n{}\n'.format(listCryptoCurrencies()))
print('List of tradable pairs:\n{}\n'.format(listTradablePairs()))

# print price of selected pair
print('\nSelected pair: {}\n'.format(pair))
print('Ask price: {0}\nBid price: {1}\n'.format(getDepth(direction='ask')['amount'], getDepth(direction='bid')['amount']))

# print order 
book_res = getOrderBook(pair=pair)
print('\nOrder book (level=2)\n\nAsks: {0}\n\nBids: {1}\n'.format(book_res['asks'], book_res['bids']))

# get data from Coinbae
refreshDataCandles()
refreshData()

# print database tables
print('\nTable (head of): {}\n'.format(candles_table_name))
for r in c.execute('''SELECT * FROM ''' + candles_table_name + ''' ORDER BY Id LIMIT 10'''):
    print(r)
print('\nTable (head of): {}\n'.format(trades_table_name))
for r in c.execute('''SELECT * FROM ''' + trades_table_name + ''' ORDER BY Id LIMIT 10'''):
    print(r)
print('\nTable: last_checks\n')
for r in c.execute('''SELECT * FROM last_checks ORDER BY Id'''):
    print(r)

# post limit order and cancel it
order_res = createOrder(direction='sell', price='1000', amount='0.1', order_type='limit')
canc_res = cancelOrder(order_res)
print('\n\nOrder response: {0}\nCancellation response: {1}'.format(order_res, canc_res))

# save database
connection.commit()
