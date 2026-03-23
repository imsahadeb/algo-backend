import os
import logging
import random
import time
import json
import urllib.request
from datetime import datetime, timezone, timedelta
import pandas as pd
from dotenv import dotenv_values
import pyotp
import threading

# SQLAlchemy Imports
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

# Angel One SmartAPI
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# APScheduler
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 1. ENVIRONMENT & DB SETUP ---
def load_env_vars():
    env_vars = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.split("=", 1)
                    env_vars[k.strip()] = v.strip()
    return env_vars

ENV = load_env_vars()
DB_URL = ENV.get("DB_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DB_URL)
Base = declarative_base()
SessionLocal = sessionmaker(bind=engine)

class PaperTrade(Base):
    __tablename__ = 'forward_test_trades'
    id = Column(Integer, primary_key=True, index=True)
    trade_date = Column(DateTime, default=datetime.utcnow)
    coin_toss = Column(String(10)) # HEAD or TAIL
    instrument_type = Column(String(10)) # CE or PE
    symbol_token = Column(String(50))
    trading_symbol = Column(String(100))
    entry_time = Column(DateTime)
    entry_price = Column(Float)
    sl_price = Column(Float)
    tp_price = Column(Float)
    exit_time = Column(DateTime, nullable=True)
    exit_price = Column(Float, nullable=True)
    exit_reason = Column(String(50), nullable=True) # SL, TP, EOD
    net_pnl = Column(Float, nullable=True) # considering brokerage
    status = Column(String(20), default="OPEN") # OPEN, CLOSED

class AlgoStrategy(Base):
    __tablename__ = 'algo_strategies'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    description = Column(String(500))
    status = Column(String(20), default="INACTIVE")
    run_days = Column(String(50), default="0,1,2,3,4")
    target_premium = Column(Float, default=100.0)
    sl_points = Column(Float, default=30.0)
    tp_points = Column(Float, default=60.0)
    opt_type = Column(String(10), default="BOTH")
    deploy_until = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# --- 2. GLOBAL STATE ---
smartApi = None
auth_token = None
feed_token = None
sws = None
active_trade_id = None
active_token = None

# --- 3. HELPER FUNCTIONS ---
def initialize_angel_one():
    global smartApi, auth_token, feed_token
    api_key = ENV.get("API KEY")
    client_id = ENV.get("client_id")
    pin = ENV.get("PIN")
    totp_secret = ENV.get("TOTP_TOKEN")
    
    smartApi = SmartConnect(api_key=api_key)
    totp = pyotp.TOTP(totp_secret).now()
    session = smartApi.generateSession(client_id, pin, totp)
    
    if session.get('status'):
        auth_token = session['data']['jwtToken']
        feed_token = session['data']['feedToken']
        logger.info("Angel One Login Successful for Algo Engine.")
        return True
    else:
        logger.error(f"Angel One Login Failed: {session}")
        return False

def get_nifty_options_df():
    logger.info("Downloading latest Scrip Master...")
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode())
    df = pd.DataFrame(data)
    nifty_opt = df[(df['name'] == 'NIFTY') & (df['instrumenttype'] == 'OPTIDX')]
    # Parse expiry dates to find the active chains
    nifty_opt['expiry_date'] = pd.to_datetime(nifty_opt['expiry'], format='%d%b%Y')
    return nifty_opt

def fetch_multi_ltp(tokens):
    exchangeTokens = {"NSE": ["26000"]} # Nifty Spot
    exchangeTokens["NFO"] = tokens
    try:
        res = smartApi.getMarketData("LTP", exchangeTokens)
        if res.get('status') and res.get('data'):
            return res['data']['fetched']
    except Exception as e:
        logger.error(f"Error fetching rapid market data: {e}")
    return []

# --- 4. WEBSOCKET HANDLERS ---
def on_data(wsapp, message):
    global active_trade_id, active_token
    # Process incoming tick
    if not active_trade_id or not active_token:
        return
        
    try:
        token = message.get("token")
        if token != active_token:
            return
            
        ltp = float(message.get("last_traded_price", 0))
        if ltp <= 0: return
        
        # Check SL and TP in db
        db = SessionLocal()
        trade = db.query(PaperTrade).filter(PaperTrade.id == active_trade_id).first()
        if not trade or trade.status == "CLOSED":
            db.close()
            return
            
        close_trade = False
        exit_reason = ""
        
        if ltp <= trade.sl_price:
            close_trade = True
            exit_reason = "SL Hit"
        elif ltp >= trade.tp_price:
            close_trade = True
            exit_reason = "TP Hit"
            
        # Check EOD (3:20 PM)
        now = datetime.now()
        if (now.hour == 15 and now.minute >= 20) or now.hour > 15:
            close_trade = True
            exit_reason = "EOD Squareoff"
            
        if close_trade:
            # Execute Exit
            # Calculate Slippage and Brokerage
            slippage = ltp * 0.005 # 0.5% slippage on exit
            final_exit_price = ltp - slippage # For a long position exit, we get slightly less
            
            # Lot size for Nifty is 65
            qty = 65
            gross_pnl = (final_exit_price - trade.entry_price) * qty
            # Approximate slippage & charges (brokerage + STT + exchanged)
            charges = 60.0
            net_pnl = gross_pnl - charges
            
            trade.exit_time = datetime.utcnow()
            trade.exit_price = final_exit_price
            trade.exit_reason = exit_reason
            trade.net_pnl = net_pnl
            trade.status = "CLOSED"
            db.commit()
            logger.info(f"TRADE CLOSED [{exit_reason}]: Token {token} @ {final_exit_price}. Net PnL: ₹{net_pnl:.2f}")
            
            # Unsubscribe from WebSocket
            active_trade_id = None
            active_token = None
            sws.unsubscribe("nse_fo", [token])
            
        db.close()
    except Exception as e:
        logger.error(f"WS Data Error: {e}")

def on_open(wsapp):
    logger.info("WebSocket Connected.")
    if active_token:
        # Proper token initialization format
        sws.subscribe("EXCHANGE_NFO", 2, [{"exchangeType": 2, "tokens": [active_token]}])

def on_error(wsapp, error):
    logger.error(f"WebSocket Error: {error}")

def start_websocket():
    global sws
    if not feed_token or not auth_token:
        initialize_angel_one()
        
    client_id = ENV.get("client_id")
    api_key = ENV.get("API KEY")
    sws = SmartWebSocketV2(auth_token, api_key, client_id, feed_token)
    sws.on_data = on_data
    sws.on_open = on_open
    sws.on_error = on_error
    sws.connect()

# --- 5. ALGO STRATEGY EXECUTION ---
def execute_10am_strategy():
    global active_trade_id, active_token
    logger.info("Executing 10 AM Strategy...")
    
    # 0. Check if already traded today
    db = SessionLocal()
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    existing_trade = db.query(PaperTrade).filter(PaperTrade.trade_date >= today_start).first()
    if existing_trade:
        logger.info("Trade already taken today. Skipping.")
        db.close()
        return
        
    algo = db.query(AlgoStrategy).filter(AlgoStrategy.name == "10 AM Nifty Premium Sniper").first()
    if not algo or algo.status != "ACTIVE":
        logger.info("Algo is INACTIVE. Skipping.")
        db.close()
        return
        
    if algo.deploy_until and datetime.utcnow() > algo.deploy_until:
        algo.status = "INACTIVE"
        db.commit()
        logger.info("Algo deployment expired. Skipping.")
        db.close()
        return
        
    current_dow = str(datetime.now().weekday())
    if current_dow not in algo.run_days.split(","):
        logger.info(f"Skipping today (DOW {current_dow} not in {algo.run_days})")
        db.close()
        return
        
    targ_premium = getattr(algo, 'target_premium', 100.0)
    opt_type_config = getattr(algo, 'opt_type', "BOTH")
    sl_pts = getattr(algo, 'sl_points', 30.0)
    tp_pts = getattr(algo, 'tp_points', 60.0)
        
    if not smartApi or not getattr(smartApi, 'access_token', None):
        if not initialize_angel_one(): 
            db.close()
            return
    # 1. Toss Coin
    if opt_type_config == "BOTH":
        toss = random.choice(["HEAD", "TAIL"])
        opt_type = "CE" if toss == "HEAD" else "PE"
    else:
        toss = opt_type_config
        opt_type = "CE" if opt_type_config == "CE" else "PE"
        
    logger.info(f"Bias: {opt_type_config} -- Toss: {toss} -> Selecting {opt_type}")
    
    # 2. Get Nifty Spot
    spot_data = fetch_multi_ltp(["26000"]) # Wait NFO needs tokens. For Spot we use NSE:26000
    try:
        spot_res = smartApi.getMarketData("LTP", {"NSE": ["26000"]})
        spot_price = float(spot_res['data']['fetched'][0]['ltp'])
    except Exception as e:
        logger.error(f"Failed to fetch Nifty Spot: {e}")
        db.close()
        return
        
    logger.info(f"Nifty Spot LTP: {spot_price}")
    
    # 3. Filter Options Data
    df = get_nifty_options_df()
    # Nearest Expiry
    future_expiries = df[df['expiry_date'] >= datetime.now()]['expiry_date'].unique()
    if len(future_expiries) == 0:
        logger.error("No valid expiries found.")
        db.close()
        return
    
    current_expiry = sorted(future_expiries)[0]
    df_exp = df[(df['expiry_date'] == current_expiry) & (df['symbol'].str.endswith(opt_type))]
    
    # Strike mapping logic: We look for Strikes near spot
    # Get strikes within +/- 500 points
    df_near = df_exp[(df_exp['strike'].astype(float)/100 >= spot_price - 500) & (df_exp['strike'].astype(float)/100 <= spot_price + 500)]
    tokens_to_check = df_near['token'].tolist()
    
    if not tokens_to_check:
        logger.error("No valid tokens found near spot.")
        db.close()
        return
        
    # 4. Fetch LTPs for these tokens to find closest to 100
    # Batch request max 50 tokens at a time natively
    best_token = None
    best_symbol = None
    closest_diff = 99999
    entry_ltp = 0
    
    chunks = [tokens_to_check[x:x+40] for x in range(0, len(tokens_to_check), 40)]
    for chunk in chunks:
        res = fetch_multi_ltp(chunk)
        for item in res:
            ltp = float(item['ltp'])
            diff = abs(ltp - targ_premium)
            if diff < closest_diff:
                closest_diff = diff
                best_token = item.get('symbolToken') or item.get('exchangeToken')
                best_symbol = item['tradingSymbol']
                entry_ltp = ltp
                
    if not best_token:
        logger.error("Failed to find a suitable option premium.")
        db.close()
        return
        
    logger.info(f"Target selected: {best_symbol} (Token: {best_token}) @ {entry_ltp} (Closest to 100)")
    
    # 5. Paper Execute Trade
    slippage = entry_ltp * 0.005 # 0.5% slippage on entry
    entry_price = entry_ltp + slippage
    sl_price = entry_price - sl_pts
    tp_price = entry_price + tp_pts
    
    trade = PaperTrade(
        coin_toss=toss,
        instrument_type=opt_type,
        symbol_token=best_token,
        trading_symbol=best_symbol,
        entry_time=datetime.utcnow(),
        entry_price=entry_price,
        sl_price=sl_price,
        tp_price=tp_price,
        status="OPEN"
    )
    db.add(trade)
    db.commit()
    db.refresh(trade)
    
    logger.info(f"TRADE EXECUTED [PAPER]: {best_symbol} Entry @ {entry_price:.2f}. SL: {sl_price:.2f}, TP: {tp_price:.2f}")
    
    active_trade_id = trade.id
    active_token = best_token
    db.close()
    
    # 6. Subscribe to WebSocket
    if sws:
        # Subscribe mode 2 = SNAPQUOTE
        token_list = [{"exchangeType": 2, "tokens": [active_token]}]
        sws.subscribe("EXCHANGE_NFO", 2, token_list)
        logger.info(f"Subscribed to WS for continuous tracking of NFO token {active_token}")

# --- 6. SCHEDULER & BOOTSTRAP ---
def start_algo_job():
    scheduler = BackgroundScheduler()
    # "at exact 10 am in trading days" -> Mon to Fri
    scheduler.add_job(execute_10am_strategy, 'cron', day_of_week='mon-fri', hour=10, minute=0, timezone='Asia/Kolkata')
    scheduler.start()
    logger.info("Algo scheduler started. Waiting for 10 AM Mon-Fri.")
    
    # Start the continuous websocket background thread
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

if __name__ == "__main__":
    logger.info("Initializing Postgres DB and Algo Engine...")
    start_algo_job()
    
    # Catch-up: Run immediately in background to cover any missed 10AM executions
    def delayed_catchup():
        import time
        time.sleep(3) # Wait for main thread login to complete
        execute_10am_strategy()
    threading.Thread(target=delayed_catchup, daemon=True).start()
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Algo Engine shutting down.")
