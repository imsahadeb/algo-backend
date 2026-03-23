import os
import pyotp
import yfinance as yf
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from logzero import logger
import threading
import json
import asyncio

def load_env():
    env_vars = {}
    try:
        with open('.env') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    env_vars[key.strip()] = val.strip()
    except FileNotFoundError:
        pass
    return env_vars
    return env_vars

ENV_VARS = load_env()

# Database Setup for Paper Trades
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

DB_URL = ENV_VARS.get("DB_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

SessionLocal = None
if DB_URL:
    try:
        engine = create_engine(DB_URL)
        SessionLocal = sessionmaker(bind=engine)
        Base = declarative_base()

        class PaperTrade(Base):
            __tablename__ = 'forward_test_trades'
            id = Column(Integer, primary_key=True, index=True)
            trade_date = Column(DateTime, default=datetime.utcnow)
            coin_toss = Column(String(10))
            instrument_type = Column(String(10))
            symbol_token = Column(String(50))
            trading_symbol = Column(String(100))
            entry_time = Column(DateTime)
            entry_price = Column(Float)
            sl_price = Column(Float)
            tp_price = Column(Float)
            exit_time = Column(DateTime, nullable=True)
            exit_price = Column(Float, nullable=True)
            exit_reason = Column(String(50), nullable=True)
            net_pnl = Column(Float, nullable=True)
            status = Column(String(20), default="OPEN")

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
        
        # Seed default
        db = SessionLocal()
        if db.query(AlgoStrategy).count() == 0:
            db.add(AlgoStrategy(
                name="10 AM Nifty Premium Sniper",
                description="Executes a paper option trade daily at 10 AM by targeting a specific premium. Features auto SL/TP and conditional direction based on coin toss or forced bias.",
                status="INACTIVE"
            ))
            db.commit()
        db.close()
    except Exception as e:
        logger.error(f"Failed to init DB in API: {e}")

app = FastAPI()

# Configure CORS to allow our Next.js frontend to communicate securely
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class LoginRequest(BaseModel):
    username: str
    pin: str

class LoginResponse(BaseModel):
    status: bool
    message: str
    auth_token: str | None = None
    refresh_token: str | None = None
    feed_token: str | None = None

@app.post("/api/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    api_key = ENV_VARS.get("API KEY")
    token = ENV_VARS.get("TOTP_TOKEN")
    
    if not api_key or not token:
        logger.error("Missing API KEY or TOTP_TOKEN in backend .env file.")
        raise HTTPException(status_code=500, detail="Missing API KEY or TOTP_TOKEN in backend .env.")

    smartApi = SmartConnect(api_key)
    
    try:
        totp = pyotp.TOTP(token).now()
    except Exception as e:
        logger.error(f"Invalid TOTP Token: {e}")
        raise HTTPException(status_code=500, detail="Invalid internal TOTP Token")

    try:
        data = smartApi.generateSession(request.username, request.pin, totp)
        if data.get('status') == False:
            logger.error(f"Login failed: {data}")
            raise HTTPException(status_code=401, detail=f"Login failed: {data.get('message')}")
            
        auth_token = data['data']['jwtToken']
        refresh_token = data['data']['refreshToken']
        feed_token = data['data']['feedToken']
        
        return LoginResponse(
            status=True,
            message="Login successful",
            auth_token=auth_token,
            refresh_token=refresh_token,
            feed_token=feed_token
        )
    except Exception as e:
        logger.exception(f"Login API external error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

USER_PROFILE_CACHE = {}

@app.get("/api/user-status")
async def get_user_status(request: Request):
    global USER_PROFILE_CACHE
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = auth_header.split("Bearer ")[-1].strip()
    api_key = ENV_VARS.get("API KEY")
    smartApi = SmartConnect(api_key, access_token=token)
    
    funds = 0.0
    m2m = 0.0
    name = USER_PROFILE_CACHE.get("name", "Angel One User")

    try:
        # Load profile dynamically initially using local .env backend credentials
        if name == "Angel One User":
            totp_secret = ENV_VARS.get("TOTP_TOKEN")
            client_id = ENV_VARS.get("client_id")
            pin = ENV_VARS.get("PIN")
            if totp_secret and client_id and pin:
                import pyotp
                smart_init = SmartConnect(api_key=api_key)
                totp = pyotp.TOTP(totp_secret).now()
                session = smart_init.generateSession(client_id, pin, totp)
                if session and session.get("status"):
                    name = session.get("data", {}).get("name", name)
                    USER_PROFILE_CACHE["name"] = name

        rms = smartApi.rmsLimit()
        if rms and rms.get('status') and rms.get('data'):
            funds = float(rms['data'].get('net', 0))
            
        pos = smartApi.position()
        if pos and pos.get('status') and pos.get('data'):
            positions = pos['data'] if isinstance(pos['data'], list) else []
            for p in positions:
                pnlStr = p.get('pnl', '0')
                m2m += float(pnlStr) if pnlStr else 0.0
                
        return {"status": True, "funds": funds, "m2m": m2m, "name": name}
    except Exception as e:
        logger.exception(e)
        return {"status": False, "funds": 0, "m2m": 0, "name": name}

@app.get("/api/positions")
async def get_positions(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    token = auth_header.split("Bearer ")[-1].strip()
    api_key = ENV_VARS.get("API KEY")
    smartApi = SmartConnect(api_key, access_token=token)
    
    response_data = []
    try:
        try:
            res = smartApi.position()
            if res and res.get('status') and res.get('data'):
                response_data = res['data'] if isinstance(res['data'], list) else []
        except Exception:
            pass # Angel One HTTP limit failure, move on to display valid DB Paper Trades

        if SessionLocal:
            db = SessionLocal()
            open_trades = db.query(PaperTrade).filter(PaperTrade.status == "OPEN").all()
            
            nfo_tokens = [t.symbol_token for t in open_trades if t.symbol_token]
            ltp_map = {}
            if nfo_tokens:
                # Use Global WebSocket Ticks to bypass ALL API tracking limits & provide 0-delay updates
                target_nfo = []
                for tk in nfo_tokens:
                    tk_str = str(tk)
                    if tk_str not in TOKEN_MAP:
                        TOKEN_MAP[tk_str] = f"NFO_{tk_str}"
                        if ANGEL_SWS:
                            try:
                                ANGEL_SWS.subscribe("md", 2, [{"exchangeType": 2, "tokens": [tk_str]}])
                            except Exception: pass
                    
                    tick_data = LATEST_TICKS.get(tk_str, {})
                    ltp_val = float(tick_data.get('ltp', 0.0))
                    if ltp_val == 0.0:
                        target_nfo.append(tk_str)
                    ltp_map[tk] = ltp_val
                    
                if target_nfo:
                    try:
                        ltp_res = smartApi.getMarketData("LTP", {"NFO": target_nfo})
                        if ltp_res and ltp_res.get('status') and ltp_res.get('data') and ltp_res['data']['fetched']:
                            for f in ltp_res['data']['fetched']:
                                tk_id = str(f.get('symbolToken') or f.get('exchangeToken'))
                                ltp_map[tk_id] = float(f.get('ltp', 0))
                    except Exception:
                        pass

            for t in open_trades:
                ltp = ltp_map.get(t.symbol_token, 0.0)
                qty = 65 # Updated Nifty Lot size
                m2m = (ltp - t.entry_price) * qty if ltp > 0 else 0
                response_data.append({
                    "tradingsymbol": f"[ALGO] {t.trading_symbol}",
                    "symbolName": t.trading_symbol,
                    "netqty": str(qty),
                    "buyavgprice": str(round(t.entry_price, 2)),
                    "ltp": str(round(ltp, 2)),
                    "pnl": str(round(m2m, 2)),
                    "instrumenttype": "OPTIDX",
                    "exchange": "NFO"
                })
            db.close()
            
        return {"status": True, "data": response_data, "message": "SUCCESS", "errorcode": ""}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/market-data")
async def get_market_data_api(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    token = auth_header.split("Bearer ")[-1].strip()
    api_key = ENV_VARS.get("API KEY")
    smartApi = SmartConnect(api_key, access_token=token)
    
    body = await request.json()
    mode = body.get("mode", "FULL")
    # Default to tracking some major stock tokens if none provided
    # Nifty=26000, BankNifty=26009, Reliance=2885, Infy=1594
    exchangeTokens = body.get("exchangeTokens", {"NSE": ["26000", "26009", "2885", "1594"]}) 
    
    try:
        res = smartApi.getMarketData(mode, exchangeTokens)
        return res
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))

active_ws_connections: list[WebSocket] = []

MASTER_SMART_API = None
MASTER_TOKEN_TIME = 0
ANGEL_SWS = None
LATEST_TICKS = {}
SWS_THREAD = None

TOKEN_MAP = {
    "26000": "NIFTY",
    "26009": "BANKNIFTY",
    "2885": "RELIANCE",
    "1594": "INFY"
}

def on_ws_data(wsapp, message):
    try:
        t = str(message.get("token"))
        if t in TOKEN_MAP:
            # Angel One snap quotes provide price in paise, so unconditionally divide by 100
            ltp = float(message.get("last_traded_price", 0)) / 100.0
            
            # Preserve existing change metrics if already populated
            existing = LATEST_TICKS.get(t, {})
            LATEST_TICKS[t] = {
                "tradingSymbol": TOKEN_MAP[t],
                "ltp": ltp,
                "netChange": existing.get("netChange", 0),
                "percentChange": existing.get("percentChange", 0)
            }
    except Exception:
        pass

def start_angel_sws(api_key, client_id, auth_token, feed_token):
    global ANGEL_SWS
    try:
        ANGEL_SWS = SmartWebSocketV2(auth_token, api_key, client_id, feed_token)
        def on_open(wsapp):
            token_list = [{"exchangeType": 1, "tokens": list(TOKEN_MAP.keys())}]
            ANGEL_SWS.subscribe("md", 2, token_list)
        ANGEL_SWS.on_open = on_open
        ANGEL_SWS.on_data = on_ws_data
        ANGEL_SWS.connect()
    except Exception as e:
        logger.error(f"SWS Thread Error: {e}")

def get_master_smart_api():
    global MASTER_SMART_API, MASTER_TOKEN_TIME, SWS_THREAD
    import time
    if MASTER_SMART_API and (time.time() - MASTER_TOKEN_TIME < 3600*8):
        return MASTER_SMART_API
        
    api_key = ENV_VARS.get("API KEY")
    master_api = SmartConnect(api_key=api_key)
    totp_secret = ENV_VARS.get("TOTP_TOKEN")
    client_id = ENV_VARS.get("client_id")
    pin = ENV_VARS.get("PIN")
    try:
        import pyotp
        totp = pyotp.TOTP(totp_secret).now()
        session = master_api.generateSession(client_id, pin, totp)
        if session and session.get("status"):
            auth_token = session.get("data", {}).get("jwtToken")
            feed_token = session.get("data", {}).get("feedToken")
            master_api.access_token = auth_token
            MASTER_TOKEN_TIME = time.time()
            MASTER_SMART_API = master_api
            
            if not SWS_THREAD or not SWS_THREAD.is_alive():
                # Fetch baseline changes over HTTP to preserve the percentages
                try:
                    base_res = master_api.getMarketData("LTP", {"NSE": list(TOKEN_MAP.keys())})
                    if base_res and base_res.get("status"):
                        for f in base_res.get("data", {}).get("fetched", []):
                            tk = str(f.get("symbolToken") or f.get("exchangeToken"))
                            if tk in TOKEN_MAP:
                                ltp_val = float(f.get("ltp", 0))
                                net_c = float(f.get("netChange", 0))
                                p_c = float(f.get("percentChange", 0))
                                LATEST_TICKS[tk] = {
                                    "tradingSymbol": TOKEN_MAP[tk],
                                    "ltp": ltp_val,
                                    "netChange": net_c,
                                    "percentChange": p_c
                                }
                except Exception:
                    pass
                    
                SWS_THREAD = threading.Thread(target=start_angel_sws, args=(api_key, client_id, auth_token, feed_token), daemon=True)
                SWS_THREAD.start()
                
            return MASTER_SMART_API
    except Exception as e:
        logger.error(f"WS Auth Error: {e}")
    return master_api

@app.websocket("/ws/market-data")
async def websocket_market_data(websocket: WebSocket, token: str):
    await websocket.accept()
    active_ws_connections.append(websocket)
    get_master_smart_api()
    
    async def fetch_data():
        last_sent_ticks = {}
        try:
            while websocket in active_ws_connections:
                if LATEST_TICKS != last_sent_ticks:
                    fetched = list(LATEST_TICKS.values())
                    if fetched:
                        await websocket.send_json({"type": "MARKET_DATA", "data": {"fetched": fetched}})
                    last_sent_ticks = dict(LATEST_TICKS)
                await asyncio.sleep(0.5)
        except Exception:
            pass

    task = asyncio.create_task(fetch_data())
    
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("action") == "subscribe":
                active_tokens = data.get("exchangeTokens", active_tokens)
    except WebSocketDisconnect:
        if websocket in active_ws_connections: active_ws_connections.remove(websocket)
        task.cancel()
    except Exception:
        if websocket in active_ws_connections: active_ws_connections.remove(websocket)
        task.cancel()

@app.get("/api/historical/{symbol}")
async def get_historical(symbol: str, interval: str = "1d"):
    yf_symbol = symbol
    if symbol.startswith("NSE:"):
        base = symbol.split(":")[1]
        yf_symbol = "^NSEI" if base == "NIFTY" else ("^NSEBANK" if base == "BANKNIFTY" else base + ".NS")
            
    # Resolve timeframe periods to accommodate YFinance free-tier constraints natively
    period = "20y"
    yf_interval = "1d"
    
    if interval == "1m": period, yf_interval = "7d", "1m"
    elif interval in ["5m", "15m", "30m"]: period, yf_interval = "60d", interval
    elif interval in ["1h", "1H"]: period, yf_interval = "730d", "1h"
    elif interval == "1D": period, yf_interval = "20y", "1d"
    elif interval == "1W": period, yf_interval = "max", "1wk"
    elif interval in ["1M", "4W"]: period, yf_interval = "max", "1mo"
    elif interval == "1d": period, yf_interval = "20y", "1d"
            
    try:
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(period=period, interval=yf_interval)
        
        if df.empty:
            return {"status": False, "data": []}
            
        data = []
        is_intraday = yf_interval in ["1m", "5m", "15m", "30m", "1h", "90m"]
        
        for index, row in df.iterrows():
            # Trick lightweight-charts into rendering IST by forcibly shifting UTC epoch integer forward 5.5 hours
            time_val = int(index.timestamp()) + 19800 if is_intraday else index.strftime("%Y-%m-%d")
            data.append({
                "time": time_val,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"])
            })
            
        return {"status": True, "data": data}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/paper-trades")
async def get_paper_trades():
    if not SessionLocal:
        return {"status": False, "error": "Database not configured"}
    try:
        db = SessionLocal()
        trades = db.query(PaperTrade).order_by(PaperTrade.id.desc()).limit(100).all()
        db.close()
        
        result = []
        from datetime import timedelta
        for t in trades:
            # Shift DB UTC time to IST manually
            ist_offset = timedelta(hours=5, minutes=30)
            entry_ist = (t.entry_time + ist_offset) if t.entry_time else None
            exit_ist = (t.exit_time + ist_offset) if t.exit_time else None
            
            pnl_val = round((t.exit_price - t.entry_price) * 65 - 60.0, 2) if t.exit_price else 0.0
            
            result.append({
                "id": t.id,
                "symbol": t.trading_symbol,
                "entry_time": entry_ist.strftime("%d %b %H:%M") if entry_ist else "-",
                "exit_time": exit_ist.strftime("%H:%M:%S") if exit_ist else "-",
                "entry_price": round(t.entry_price, 2),
                "exit_price": round(t.exit_price, 2) if t.exit_price else 0.0,
                "exit_reason": t.exit_reason if t.exit_reason else "Pending",
                "status": t.status,
                "toss_strategy": getattr(t, 'toss_res', 'FAIL') + " : " + getattr(t, 'toss_opt', 'UNK'),
                "sl": round(t.sl_price, 2) if t.sl_price else 0.0,
                "tp": round(t.tp_price, 2) if t.tp_price else 0.0,
                "pnl": pnl_val
            })
        return {"status": True, "data": result}
    except Exception as e:
        logger.exception(e)
        return {"status": False, "error": str(e)}

class AlgoUpdate(BaseModel):
    status: str
    run_days: str
    target_premium: float
    sl_points: float
    tp_points: float
    opt_type: str
    deploy_days: int

@app.get("/api/algos")
async def get_algos():
    if not SessionLocal: return {"status": False, "error": "DB"}
    try:
        db = SessionLocal()
        algos = db.query(AlgoStrategy).all()
        db.close()
        res = []
        for a in algos:
            res.append({
                "id": a.id, "name": a.name, "description": a.description,
                "status": a.status, "run_days": a.run_days,
                "target_premium": a.target_premium, "sl_points": a.sl_points,
                "tp_points": a.tp_points, "opt_type": a.opt_type,
                "deploy_until": a.deploy_until.strftime("%Y-%m-%d %H:%M:%S") if a.deploy_until else None
            })
        return {"status": True, "data": res}
    except Exception as e:
        return {"status": False, "error": str(e)}

@app.post("/api/algos/{algo_id}")
async def update_algo(algo_id: int, req: AlgoUpdate):
    if not SessionLocal: return {"status": False, "error": "DB"}
    try:
        from datetime import timedelta
        db = SessionLocal()
        algo = db.query(AlgoStrategy).filter(AlgoStrategy.id == algo_id).first()
        if not algo:
            db.close()
            return {"status": False, "error": "Not Found"}
        
        algo.status = req.status
        algo.run_days = req.run_days
        algo.target_premium = req.target_premium
        algo.sl_points = req.sl_points
        algo.tp_points = req.tp_points
        algo.opt_type = req.opt_type
        if req.deploy_days > 0 and req.status == "ACTIVE":
            algo.deploy_until = datetime.utcnow() + timedelta(days=req.deploy_days)
        elif req.status == "INACTIVE":
            algo.deploy_until = None
            
        db.commit()
        db.close()
        return {"status": True, "message": "Updated successfully"}
    except Exception as e:
        return {"status": False, "error": str(e)}
