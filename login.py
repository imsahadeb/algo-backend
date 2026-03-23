import os
import pyotp
from SmartApi import SmartConnect
from logzero import logger

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

def main():
    env_vars = load_env()
    
    api_key = env_vars.get("API KEY")
    username = env_vars.get("client_id")
    pwd = env_vars.get("PIN")
    token = env_vars.get("TOTP_TOKEN")
    
    if not all([api_key, username, pwd, token]):
        logger.error("Missing credentials in .env file. Please ensure 'API KEY', 'client_id', 'PIN', and 'TOTP_TOKEN' are set.")
        return

    smartApi = SmartConnect(api_key)
    
    try:
        totp = pyotp.TOTP(token).now()
    except Exception as e:
        logger.error("Invalid Token: The provided TOTP_TOKEN is not valid.")
        return

    logger.info(f"Attempting login for client: {username}...")
    data = smartApi.generateSession(username, pwd, totp)
    
    if data['status'] == False:
        logger.error(f"Login failed: {data}")
    else:
        logger.info("Login Successful!")
        logger.info(f"Auth Token: {data['data']['jwtToken']}")
        logger.info(f"Refresh Token: {data['data']['refreshToken']}")
        
        # logout
        try:
            logout = smartApi.terminateSession(username)
            logger.info("Logout Successful")
        except Exception as e:
            logger.exception(f"Logout failed: {e}")

if __name__ == "__main__":
    main()
