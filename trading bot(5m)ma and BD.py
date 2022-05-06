from binance.client import Client
from binance import ThreadedWebsocketManager
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import time
import warnings
import schedule

class futurestrader():#triple sma crossover
    
    def __init__(self,symbol,bar_length,ma_s,ma_m,ma_l,sma,rstd,units,position = 0,leverage = 6):
        
        self.symbol = symbol
        self.bar_length = bar_length
        self.available_intervals = ['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M']
        self.units = units
        self.position = position
        self.leverage = leverage
        self.cum_profits = 0
        
        #self.trades = 0
        #self.trade_values=[]
        
        #************add strategy - specific attributes here***********
        self.ma_s =ma_s
        self.ma_m =ma_m
        self.ma_l =ma_l
        self.sma =sma
        self.rstd =rstd
        #**************************************************************
        
    def start_trading(self,historical_days):
        
        client.futures_change_leverage(symbol =self.symbol, leverage = self.leverage)
        
        self.twm = ThreadedWebsocketManager()
        self.twm.start()
        
        if self.bar_length in self.available_intervals:
            self.get_most_recent(symbol = self.symbol,interval = self.bar_length,days = historical_days)
            self.twm.start_kline_futures_socket(callback = self.stream_candles,symbol = self.symbol,interval = self.bar_length)
            
        #else to be added later
    
    def get_most_recent(self,symbol,interval,days):# plus ma column
        
    
        now = datetime.utcnow()
        past = str(now - timedelta(days = days))
    
        bars = client.futures_historical_klines(symbol = symbol, interval = interval, start_str = past, end_str = None, limit = 1000)
    
        df = pd.DataFrame(bars)
        df['Date'] = pd.to_datetime(df.iloc[:,0],unit = 'ms')
        df.columns = ['Open Time','Open','High','Low','Close','Volume','Close Time','Quote Asset Volume','Number of trades','Taker Buy Base Asset Volume','Taker Buy Qoute Asset Volume','Ignore','Date']
        df = df[['Date','Open','High','Low','Close','Volume']].copy()
        df.set_index('Date',inplace =True)
        for column in df.columns:
            df[column] = pd.to_numeric(df[column],errors = 'coerce')
        df['Complete'] = [True for row in range(len(df)-1)]+[False]
       
        self.data = df
    
    
    def stream_candles(self,msg):
        
        '''define how to process incoming websocket messages'''
        #extract the required items from msg
        event_time = pd.to_datetime(msg['E'],unit ='ms')
        start_time = pd.to_datetime(msg['k']['t'],unit ='ms')
        first = float(msg['k']['o'])
        high = float(msg['k']['h'])
        low = float(msg['k']['l'])
        close = float(msg['k']['c'])
        volume = float(msg['k']['v'])
        complete = msg['k']['x']
        
        #stop trading session
        
        ##if self.trades >= 5:#stop session after 5 trades
            ##self.twm.stop()
            ##if self.position == 1:
                ##order = client.create_order(symbol = self.symbol,side = 'SELL', type = 'MARKET',quantity = self.units)
              ##self.report_trade(order,'going nneutral and stop')
              ##self.position =0
          ##elif self.position == -1:
              ##order = client.create_order(symbol = self.symbol, side = 'BUY', type = 'MARKET', quantity = self.units )
              ##self.report_trade(order,'going neutral aand stop ')
              ##self.position = 0
            ##else:
                ##print('stop')
                
    
        #print out
        print('.',end ='',flush = True)#just print something to get a feedback (everything ok)
    
        #feed of (add new memeber/update latest bar)
        self.data.loc[start_time] = [first,high,low,close,volume,complete]
        
        
        #prepare features and define strategy/trading position wheneve the latest bar is complete
        #if complete == True:
        self.define_strategy()
               
    def define_strategy(self): #"strategy - specific"
        
        data = self.data.copy()
        
        #****************define your strategy***********
        data = data[['Open','Close']].copy()
        
        data['ma_s']=data.Close.rolling(window = ma_s).mean()
        data['ma_m']=data.Open.rolling(window = ma_m).mean()
        data['ma_l']=data.Open.rolling(window = ma_l).mean()
        data['rstd']=data.Close.rolling(window = rstd).std()
        data['sma']=data.Close.rolling(window = sma).mean()
        data['upperband']=data['sma'] + 2 * data['rstd']
        data['lowerband']=data['sma'] - 2 * data['rstd']


        data.dropna(inplace = True)
        
        cond1 = (data.ma_s>data.sma)#long
        cond2=(data.ma_s<data.sma)# go short
        
        data['position'] = 0
        
        data.loc[cond1,'position']=1
        data.loc[cond2,'position']=-1
        data["change"] = data["position"].diff()
        self.prepared_data = data.copy()
            
        if((data.upperband.iloc[-1] - data.sma.iloc[-1]) > 80):
            self.execute_trades()
        
        #*****************************************************************
        
    def execute_trades(self):
        if self.prepared_data['change'].iloc[-1]!=0 :
            if self.prepared_data['position'].iloc[-1] == 1: #if position is long -> go/stay long
                if self.position == 0:
                    order = client.futures_create_order(symbol = self.symbol,side = 'BUY', type = 'MARKET',quantity = self.units)
                    self.report_trade(order,'going long')
                    time.sleep(50)
                    
                elif self.position == -1:
                    order = client.futures_create_order(symbol = self.symbol, side = 'BUY', type = 'MARKET', quantity = 2*self.units)
                    self.report_trade(order,'going long')
                    time.sleep(50)
                    
                self.position = 1
                
            elif self.prepared_data['position'].iloc[-1] == 0: #if position is neutral -> go/stay neutral
                if self.position == 1:
                    order = client.futures_create_order(symbol= self.symbol,side='SELL',type='MARKET',quantity=self.units)
                    self.report_trade(order, 'going neutral')
                    
                elif self.position == -1:
                    order = client.futures_create_order(symbol= self.symbol,side='BUY',type='MARKET',quantity=self.units)
                    self.report_trade(order, 'going neutral')

                self.position = 0
                    
            if self.prepared_data['position'].iloc[-1] == -1: #if position is short -> go/stay short
                if self.position == 0:
                    order = client.futures_create_order(symbol= self.symbol,side='SELL',type='MARKET',quantity=self.units)
                    self.report_trade(order, 'going short')
                    time.sleep(50)
                    
                elif self.position == 1:
                    order = client.futures_create_order(symbol= self.symbol,side='SELL',type='MARKET',quantity=2*self.units)
                    self.report_trade(order, 'going short')
                    time.sleep(50)
                    
                self.position = -1
                
            
        else:    
            if self.prepared_data['position'].iloc[-1] == 1: #if position is long -> go/stay long
                if self.position == 0:
                    order = client.futures_create_order(symbol = self.symbol,side = 'BUY', type = 'MARKET',quantity = self.units)
                    self.report_trade(order,'going long')
                
                elif self.position == -1:
                    order = client.futures_create_order(symbol = self.symbol, side = 'BUY', type = 'MARKET', quantity = 2*self.units )
                    self.report_trade(order,'going long')
                
                self.position = 1
            
            elif self.prepared_data['position'].iloc[-1] == 0: #if position is neutral -> go/stay neutral
                if self.position == 1:
                    order = client.futures_create_order(symbol= self.symbol,side='SELL',type='MARKET',quantity=self.units)
                    self.report_trade(order, 'going neutral')
                
                elif self.position == -1:
                    order = client.futures_create_order(symbol= self.symbol,side='BUY',type='MARKET',quantity=self.units)
                    self.report_trade(order, 'going neutral')
                self.position = 0
            
            if self.prepared_data['position'].iloc[-1] == -1: #if position is short -> go/stay short
                if self.position == 0:
                    order = client.futures_create_order(symbol= self.symbol,side='SELL',type='MARKET',quantity=self.units)
                    self.report_trade(order, 'going short')
                
                elif self.position == 1:
                    order = client.futures_create_order(symbol= self.symbol,side='SELL',type='MARKET',quantity=2*self.units)
                    self.report_trade(order, 'going short')
                self.position = -1
                
            
    def report_trade(self,order,going): #new
        
        time.sleep(0.1)
        order_time = order['updateTime']
        trades = client.futures_account_trades(symbol = self.symbol,startTime = order_time)
        order_time = pd.to_datetime(order_time,unit ='ms')
        
        #extract data from order object
        
        df =pd.DataFrame(trades)
        columns = ['qty','quoteQty','commission','realizedPnl']
        for column in columns:
            df[column] = pd.to_numeric(df[column],errors = 'coerce')
        
        base_units = round(df.qty.sum(),5)
        quote_units = round(df.quoteQty.sum(),5)
        commission = -round(df.commission.sum(),5)
        real_profit = round(df.realizedPnl.sum(),5)
        price = round(quote_units / base_units, 5)
        
        #calculate cummulative trading profits
        self.cum_profits += round((commission + real_profit),5) 
        
        #print trade report
        
        print(2*"\n"+100*'-')
        print("{} | {}".format(order_time,going))
        print("{} | base units = {} | quote units = {} | price = {} ".format(order_time,base_units,quote_units,price))
        print("{} | profit = {} | cumprofits = {} ".format(order_time,real_profit,self.cum_profits))
        print(100*"-"+'\n')


API_Key="WJGfnr9EAjwmakTC88jOGHpkTGQnOk4UDUez5rE96SnmgExPSTcWxA7ZudW8lhIq"
Secret_Key="OHa7G9kAsLmalkAuOzEN9B8NOBXitBuTemWmHPf7BIsrz58qoaSIVokJUMEKn1lo"

client= Client(api_key=API_Key,api_secret=Secret_Key,tld ='com', testnet =False)

symbol = 'BTCUSDT'
bar_length ='5m'
ma_s =5
ma_m=10
ma_l=25
sma = 20
rstd = 20
units = 0.001
position = 0
leverage =6

trader = futurestrader(symbol = symbol, bar_length = bar_length,ma_s=ma_s,ma_m=ma_m,ma_l=ma_l,sma=sma,rstd=rstd
                      ,units = units, position = position,leverage = leverage)

def starting():
     trader.start_trading(historical_days = 5/24)

 schedule.every().day.at("20:02").do(starting)


 while True:
     schedule.run_pending()
     time.sleep(.)