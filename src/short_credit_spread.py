"""
Reference: https://github.com/quantgalore/selling-volatility/blob/main/spread-production-tastytrade.py
"""
import sys
import datetime
import pandas as pd
import numpy as np
from termcolor import cprint
from ib_async import *
from dotenv import dotenv_values
from zoneinfo import ZoneInfo
from utils.ibkr import specific_option_contract, convert_tickers_to_full_chain, noChainFoundException
from zoneinfo import ZoneInfo
from utils.alerts import Alerts
from utils.polygon import get_ticker_data, schedule_trading_dates
from datetime import timedelta
from utils.options import find_closest_strike

def get_date_today(tz : str = "US/Eastern") -> str:
    """Return today date in yyyymmdd format"""
    dt = datetime.datetime.now(ZoneInfo(tz))
    return dt.date() # return dt.date().strftime("%Y-%m-%d")

def is_market_open_today(ib: IB, underlying: Stock) -> bool:
    """Req contract details (liquidHours) return the following string
        20090507:0700-1830,1830-2330;20090508:CLOSED"""
    today = get_date_today().strftime("%Y%m%d")
    trading_days = ib.reqContractDetails(underlying)[0].liquidHours
    trading_days_dict = {d.split(':')[0]:d.split(':')[1] for d in trading_days.split(';')}
    for k,v in trading_days_dict.items():
        if (today in k) and (v == "CLOSED"):
            return False
    return True 

class ShortCreditSpread:
    def __init__(self, auth_config, params, services = None):
        # other params: MAX attempts
        self.replace_cancelled_orders_attempt = 3
        self.get_option_chain_attempt = 3
        self.connect_attempt = 10

        # strategy details
        self.today = get_date_today().strftime("%Y-%m-%d")
        self.exp_date = self.today # 0dte
        self.params = params
        self.size = 1
        self.dist_factor = 0.5 
        self.spread_width = 5

        # configs
        self.host = auth_config['TWS_HOST']
        self.port = int(auth_config['TWS_PORT'])
        self.polygon_api_key = auth_config['POLYGON_API_KEY']
        # IB Client
        self.ib = IB()
        self.subscribe_events()
        self.clientId = 0
        self.connect()
        # services
        self.alerts = Alerts(services) 

        

        # tickers for polygon
        self.ticker = "I:SPX"
        self.vix_ticker = "I:VIX1D" # for vol regime
        self.options_ticker = "SPX" # for finding SPX options 
        self.underlying_ticker = "SPY" # we use SPY to calculate trend regime TODO: why not SPX

        # TODO: symbol (change to SPX) -> refer to 0DTE code
        self.symbol = 'SPX'
        self.exchange = 'SMART'
        self.primary_exchange = 'CBOE'
        self.ccy = 'USD'
        self.trading_class = 'SPXW'
        self.underlying_contract = Index(symbol = self.symbol, exchange= self.primary_exchange,currency = self.ccy)
        self.ib.qualifyContracts(self.underlying_contract)
        self.ib.reqMarketDataType(3)
        self.ib.sleep(3)
        # check market open
        if not is_market_open_today(self.ib, self.underlying_contract):
            self.stop()
            sys.exit()
            
        # States - Orders, order statuses, trades, contracts etc.
        self.filtered_contracts = dict()
        self.positions = self.ib.positions()
        self.trade_dict = dict()

    def connect(self):
        curr_reconnect = 0 
        delay = 30
        while True:
            try:
                self.ib.connect(self.host, self.port, self.clientId)
                if self.ib.isConnected():
                    print("connected")
                    break
            except Exception as e:
                if curr_reconnect < self.connect_attempt:
                    curr_reconnect += 1
                    self.ib.sleep(delay)
                else:
                    self.alerts.error(f"Reconnect failure after {self.connect_attempt} tries")
                    sys.exit()
        
    def run(self):
        self.ib.run()
    
    def stop(self):
        self.ib.disconnect()
        
    def exit_program(self):
        self.alerts.info(f"Program exited at market close")
        self.stop()
        sys.exit()
    
    def subscribe_events(self):
        """subscribe to callbacks to listen to events
        https://ib-insync.readthedocs.io/api.html
        """
        self.ib.orderStatusEvent += self.on_order_status_event
        self.ib.disconnectedEvent += self.on_disconnection
        self.ib.positionEvent += self.on_position

    ##################
    # STRATEGY LOGIC #
    ##################

    def compute_regimes(self):
        # calculate regimes with Polygon endpoints
        trading_dates = schedule_trading_dates("NYSE", "2024-05-01", (datetime.datetime.today()-timedelta(days = 1)))
        last_trading_date = trading_dates[-1]

        # calculate previous day's market variables
        # calculate vix1D
        vix_data = get_ticker_data(self.vix_ticker, "2024-01-01", last_trading_date, polygon_api_key=self.polygon_api_key)
        vix_data.index = pd.to_datetime(vix_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        vix_data["1_mo_avg"] = vix_data["c"].rolling(window=20).mean()
        vix_data["3_mo_avg"] = vix_data["c"].rolling(window=60).mean()
        vix_data['vol_regime'] = vix_data.apply(lambda row: 1 if (row['1_mo_avg'] > row['3_mo_avg']) else 0, axis=1)
        vix_data["str_date"] = vix_data.index.strftime("%Y-%m-%d")
        self.vol_regime = vix_data["vol_regime"].iloc[-1]
        cprint(f"Vol regime: {self.vol_regime}, vix last close: {vix_data['c'].iloc[-1]}", "green")
        # underlying trend regime
        # TODO: why SPY instead of SPX here.
        hist_underlying_data = get_ticker_data(self.underlying_ticker, "2024-01-01", last_trading_date, polygon_api_key=self.polygon_api_key)
        hist_underlying_data.index = pd.to_datetime(hist_underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        hist_underlying_data["1_mo_avg"] = hist_underlying_data["c"].rolling(window=20).mean()
        hist_underlying_data["3_mo_avg"] = hist_underlying_data["c"].rolling(window=60).mean()
        hist_underlying_data['regime'] = hist_underlying_data.apply(lambda row: 1 if (row['c'] > row['1_mo_avg']) else 0, axis=1)
        self.trend_regime = hist_underlying_data['regime'].iloc[-1]
        cprint(f"trend regime: {self.trend_regime}", "green")

        

    def compute_expected_move(self):
        # calculate expected move
        underlying_data = get_ticker_data(self.ticker, start_date = self.today, end_date= self.today, timespan = "minute", polygon_api_key=self.polygon_api_key)
        underlying_data.index = pd.to_datetime(underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")

        live_vix_data = get_ticker_data(self.vix_ticker, start_date = self.today, end_date= self.today, timespan = "minute", polygon_api_key=self.polygon_api_key)

        live_vix_data.index = pd.to_datetime(live_vix_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        index_price = live_vix_data[live_vix_data.index.time >= pd.Timestamp("09:35").time()]["c"].iloc[0]
        price = underlying_data[underlying_data.index.time >= pd.Timestamp("09:35").time()]["c"].iloc[0]

        expected_move = (round((index_price / np.sqrt(252)), 2)/100) * self.dist_factor
        cprint(f"Expected_move: {expected_move}", "red")

        if self.trend_regime == 1: # put
            self.right = "P"
            self.short_strike = price - (price * expected_move)
            self.long_strike = self.short_strike - self.spread_width
            
        elif self.trend_regime == 0: # call
            self.right = "C"
            self.short_strike = price + (price * expected_move)
            self.long_strike = self.short_strike + self.spread_width

        cprint(f"short strike: {self.short_strike}, long strike: {self.long_strike}", "red")
    def get_all_expirations(self):
            chains = self.ib.reqSecDefOptParams(self.underlying_contract.symbol, 
                                                '', 
                                                self.underlying_contract.secType, 
                                                self.underlying_contract.conId)
            chain = next(c for c in chains if c.tradingClass == self.trading_class and c.exchange == self.exchange)
            all_expirations = sorted(exp for exp in chain.expirations)
            return all_expirations
    
    def get_all_contracts(self):
        # exp_list = self.get_all_expirations()
        expiration = self.today.replace("-", "") # exp_list[0] expiration only accepts YYYYMMDD
        ## SHORT LEG 
        short_cds = self.ib.reqContractDetails(
            Option(
                symbol = self.underlying_contract.symbol, 
                lastTradeDateOrContractMonth=expiration, 
                right = self.right,
                exchange = self.exchange,  # for SPX, use SMART (primary exchange). By default, exchange is CBOE for SPX.
                tradingClass = self.trading_class)
            )
        self.contracts = self.ib.qualifyContracts(*[cd.contract for cd in short_cds])
        self.ib.sleep(15)
    
    
    def get_option_chain(self, contracts):
        attempts = 1
        # aggregate full chain (reattempt in 30s if failed for 3 attempts)
        while attempts <= self.get_option_chain_attempt:
            self.alerts.info(f"Attempt {attempts}: Requesting option chain...")
            tickers = self.ib.reqTickers(*contracts)
            self.ib.sleep(30)
            try:
                df = convert_tickers_to_full_chain(tickers)
            except noChainFoundException as e:
                if attempts == self.get_option_chain_attempt:
                    self.alerts.warning(f"Missing data for tickers. Program exited after 3 attempts. Please troubleshoot market data subscription manually.")
                    sys.exit()
                self.ib.sleep(30)
                attempts += 1
            else: 
                break 
            
        return df.sort_values('strike').reset_index(drop = True)
    
    def schedule_all_tasks(self):
        """INDICATE WHAT TASKS YOU WANT TO RUN HERE"""
        # avoid PYTZ (use ZoneInfo instead)
        # calculate regime before market open (using Polygon data) with previous day close data
        self.ib.schedule(datetime.datetime(int(self.today[:4]),int(self.today[5:7]), int(self.today[8:10]), 9, 0, 0, tzinfo =ZoneInfo("US/Eastern")),
                         self.compute_regimes)  
        # run strategy
        self.ib.schedule(datetime.datetime(int(self.today[:4]),int(self.today[5:7]), int(self.today[8:10]), 9, 37, 0, tzinfo =ZoneInfo("US/Eastern")),
                         self.run_strategy)        
        self.ib.schedule(datetime.datetime(int(self.today[:4]),int(self.today[5:7]), int(self.today[8:10]), 17, 0, 0, tzinfo =ZoneInfo("US/Eastern")), 
                         self.exit_program)
        
    
    def run_strategy(self):
        util.startLoop()
        # Calculate expected move after 5 mins after market opens 9:35
        self.compute_expected_move()
        # Get option chain
        self.get_all_contracts()
        self.chain_df = self.get_option_chain(self.contracts)
        
        # find SHORT and LONG contract
        self.short_leg = find_closest_strike(self.chain_df, self.short_strike, self.right) 
        self.long_leg = find_closest_strike(self.chain_df, self.long_strike, self.right)

        # check if the width found is correct. If not, then do not enter
        if self.right == "P":
            if self.short_leg['strike'] - self.long_leg['strike'] != self.spread_width:
                self.alerts.error("Width between short and long strike is not correct. Please check manually.")
                return 
        elif self.right == "C":
            if self.long_leg['strike'] - self.short_leg['strike'] != self.spread_width:
                self.alerts.error("Width between short and long strike is not correct. Please check manually.")
                return

        self.filtered_contracts = {
            "short_put": specific_option_contract(self.short_leg['index']),
            "long_put": specific_option_contract(self.long_leg['index']),
        }
        # Qualify contracts
        tradable_contracts = self.ib.qualifyContracts(*list(self.filtered_contracts.values()))

        if len(tradable_contracts) == 2:
            
            short_mid = (self.short_leg['bid'] + self.short_leg['ask'])/2
            long_mid = (self.long_leg['bid'] + self.long_leg['ask'])/2
            short_spread = (self.short_leg['ask'] - self.short_leg['bid'])
            long_spread = (self.long_leg['ask'] - self.long_leg['bid'])
            self.buy_at_mkt_price = round(self.short_leg['bid'] - self.long_leg['ask'],2)
            self.optimal_price =  round(short_mid - long_mid,2)
            self.alerts.info(f"Short leg found:  {self.filtered_contracts['short_put'].localSymbol} @ {self.short_leg['bid']}, spread: {round(short_spread/short_mid * 100,2)}%")
            self.alerts.info(f"Long leg found:  {self.filtered_contracts['long_put'].localSymbol} @ {self.long_leg['ask']}, spread: {round(long_spread/long_mid * 100,2)}%")
            self.alerts.info(f"Credit Spread: Market price @ {self.buy_at_mkt_price}, Mid price @ {self.optimal_price}")
            
        else:
            self.alerts.warning("Incomplete contracts found. No order placed.")
            return
        
        # Place order (if naked puts not allowed in trading account level, long has to be placed first)
        long_put_order = MarketOrder('BUY', self.size)
        long_put_trade = self.ib.placeOrder(self.filtered_contracts['long_put'], long_put_order)
        
        self.trade_dict['long_put'] = long_put_trade
        while not long_put_trade.isActive():
            self.ib.waitOnUpdate()
            
        short_put_order = MarketOrder('SELL', self.size) # use limit order if possible
        short_put_trade = self.ib.placeOrder(self.filtered_contracts['short_put'], short_put_order)
        while not short_put_trade.isActive():
            self.ib.waitOnUpdate()
        
    ##################
    # EVENT HANDLERS #
    ##################

    def on_order_status_event(self, trade: Trade):
        """OrderStatus Event"""
        # if cancelled, replaces order
        if (trade.orderStatus.status == 'Cancelled') or (trade.orderStatus.status == "ApiCancelled"):
            cancel_msg = f"*Order cancelled*: {trade.order.orderId} {trade.order.action} {trade.order.totalQuantity} of {trade.contract.localSymbol}"
            self.alerts.info(cancel_msg)

        if trade.orderStatus.status == "Filled":
            # post fill messages
            fill_msg = f"*Fills*: {trade.contract.localSymbol} {trade.orderStatus.filled} unit filled at {trade.orderStatus.avgFillPrice}"
            self.alerts.info(fill_msg)

    
    def on_disconnection(self):
        self.connect()

    def on_position(self, position: Position):
        self.positions = self.ib.positions()
        
if __name__ == "__main__":
    
    auth_config = dotenv_values(".env")
    services = None
    params = {}

    trading_app = ShortCreditSpread(auth_config, params, services)
    
    try:
        trading_app.schedule_all_tasks() # schedule all tasks
        trading_app.run()
    except (KeyboardInterrupt, SystemExit) as e:
        trading_app.stop()
