import asyncio
import os
import json
import time
import re
import hashlib
import requests
import traceback
import sqlite3
import aiohttp
from datetime import datetime
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from telegram import Update, BotCommand
from telegram.ext import (
    Application, 
    CommandHandler, 
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.constants import ParseMode
import io
from concurrent.futures import ThreadPoolExecutor
import shutil
import threading

# ==========================================
#              CONFIGURATION
# ==========================================

BOT_TOKEN = "8337631014:AAHNgGgjcOLQD6QvsUHJisPhoes_FoG06vU"

# Bot Owners (can use /approve command)
OWNERS = [7312616147, 7613349080]

# Plan Definitions
PLANS = {
    "plan1": {
        "name": "1 Day Trial",
        "duration_days": 1,
        "max_accounts": 1,
        "max_forwarders": 1,
        "description": "1 IVAS ACC, 1 FORWARDING CHNL"
    },
    "plan2": {
        "name": "30 Day Standard",
        "duration_days": 30,
        "max_accounts": 2,
        "max_forwarders": 4,
        "description": "2 IVAS ACCS, 4 FORWARDERS PER ACC"
    },
    "plan3": {
        "name": "Reseller",
        "duration_days": 30,
        "max_accounts": 2,
        "max_forwarders": 2,
        "description": "2 IVAS ACCS, 2 FORWARDERS EACH"
    },
    "plan4": {
        "name": "Free Tier",
        "duration_days": 3650, # 10 years
        "max_accounts": 1,
        "max_forwarders": 0,
        "description": "1 IVAS ACC, NO FORWARDER"
    }
}

# Will be loaded from JSON
# Structure: {user_id: {"plan": "plan1", "expiry": timestamp, "added_at": timestamp}}
APPROVED_USERS = {}

# API Endpoints - v6 domain
BASE_URL = "https://iva.blacktide.qzz.io"
LOGIN_URL = f"{BASE_URL}/login"
DASHBOARD_URL = f"{BASE_URL}/portal"
SMS_PAGE_URL = f"{BASE_URL}/portal/sms/received"
GET_SMS_URL = f"{BASE_URL}/portal/sms/received/getsms"
GET_SMS_NUMBER_URL = f"{BASE_URL}/portal/sms/received/getsms/number"
GET_SMS_CONTENT_URL = f"{BASE_URL}/portal/sms/received/getsms/number/sms"
ADD_URL = f"{BASE_URL}/portal/numbers/termination/number/add"
GET_NUMBERS_URL = f"{BASE_URL}/portal/live/getNumbers"
NUMBERS_PAGE_URL = f"{BASE_URL}/portal/numbers"
BULK_REMOVE_URL = f"{BASE_URL}/portal/numbers/return/number/bluck"
SMS_TEST_URL = f"{BASE_URL}/portal/sms/test/sms"

# Files
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, "otp_data.db")
ACCOUNTS_FILE = os.path.join(SCRIPT_DIR, "ivas_accounts.json")
APPROVED_USERS_FILE = os.path.join(SCRIPT_DIR, "approved_users.json")
SESSIONS_DIR = os.path.join(SCRIPT_DIR, "sessions")
LOG_FILE = os.path.join(SCRIPT_DIR, "otp_monitor.log")
MONITOR_STATE_FILE = os.path.join(SCRIPT_DIR, "monitor_state.json")  # New file to persist monitoring state

# Default User-Agent
DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def log_to_file(message):
    """Write detailed log to file"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {message}\n")
    except:
        pass

# Proxy Configuration
# Format in proxy.txt: host:port:username:password (one per line)
PROXY_FILE = os.path.join(SCRIPT_DIR, "proxy.txt")
PROXY_LIST = []  # List of proxy URLs
PROXY_INDEX = 0  # Current proxy index for rotation

def load_proxies():
    """Load proxies from file. Format: host:port:username:password"""
    global PROXY_LIST
    PROXY_LIST = []
    
    if not os.path.exists(PROXY_FILE):
        print("[PROXY] No proxy.txt found, running without proxy")
        return []
    
    try:
        with open(PROXY_FILE, 'r') as f:
            lines = f.readlines()
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            parts = line.split(':')
            if len(parts) == 4:
                # Format: host:port:username:password
                host, port, username, password = parts
                proxy_url = f"http://{username}:{password}@{host}:{port}"
                PROXY_LIST.append(proxy_url)
            elif len(parts) == 2:
                # Format: host:port (no auth)
                host, port = parts
                proxy_url = f"http://{host}:{port}"
                PROXY_LIST.append(proxy_url)
            elif line.startswith('http'):
                # Already formatted URL
                PROXY_LIST.append(line)
        
        if PROXY_LIST:
            print(f"[PROXY] Loaded {len(PROXY_LIST)} proxies")
        else:
            print("[PROXY] No valid proxies found in proxy.txt")
        
        return PROXY_LIST
    except Exception as e:
        print(f"[PROXY] Error loading proxies: {e}")
        return []

def get_next_proxy():
    """Get next proxy from rotation list, returns None if no proxies"""
    global PROXY_INDEX, PROXY_LIST
    
    if not PROXY_LIST:
        return None
    
    proxy = PROXY_LIST[PROXY_INDEX % len(PROXY_LIST)]
    PROXY_INDEX += 1
    return proxy

def load_proxy():
    """Legacy function - returns a proxy or None"""
    if PROXY_LIST:
        return get_next_proxy()
    return None

# Global state
cancel_flags = {}
otp_monitoring_active = False
otp_monitoring_task = None
telegram_app = None
failed_users = set()
db_conn = None
thread_pool = ThreadPoolExecutor(max_workers=8)  # More workers for concurrent users
user_conversations = {}
user_locks = {}  # Per-user locks for concurrent safety

# New: Track which accounts were actively monitoring when bot stopped
monitoring_state = {
    "active_accounts": set(),  # Set of account usernames that were being monitored
    "last_stop_time": None
}

# Conversation states
STATE_NONE = 0
STATE_ADD_EMAIL = 1
STATE_ADD_PASS = 2
STATE_ADD_USERNAME = 3
STATE_ADD_CHANNEL = 4  # Optional channel/group for OTPs
STATE_EDIT_CHOICE = 5
STATE_EDIT_VALUE = 6

# Country flags mapping
COUNTRY_FLAGS = {
    "BENIN": "🇧🇯", "COTE": "🇨🇮", "COTE D'IVOIRE": "🇨🇮", "CÔTE D'IVOIRE": "🇨🇮",
    "IVORY": "🇨🇮", "IVORY COAST": "🇨🇮", "NIGERIA": "🇳🇬", "GHANA": "🇬🇭",
    "KENYA": "🇰🇪", "SOUTH AFRICA": "🇿🇦", "SOUTHAFRICA": "🇿🇦", "CAMEROON": "🇨🇲",
    "SENEGAL": "🇸🇳", "TOGO": "🇹🇬", "MALI": "🇲🇱", "BURKINA": "🇧🇫",
    "BURKINA FASO": "🇧🇫", "NIGER": "🇳🇪", "GUINEA": "🇬🇳", "GUINEA-BISSAU": "🇬🇼",
    "BISSAU": "🇬🇼", "LIBERIA": "🇱🇷", "SIERRA": "🇸🇱", "SIERRA LEONE": "🇸🇱",
    "GAMBIA": "🇬🇲", "INDIA": "🇮🇳", "PAKISTAN": "🇵🇰", "BANGLADESH": "🇧🇩",
    "USA": "🇺🇸", "UK": "🇬🇧", "FRANCE": "🇫🇷", "GERMANY": "🇩🇪",
    "INDONESIA": "🇮🇩", "MALAYSIA": "🇲🇾", "PHILIPPINES": "🇵🇭", "VIETNAM": "🇻🇳",
    "THAILAND": "🇹🇭", "BRAZIL": "🇧🇷", "MEXICO": "🇲🇽", "COLOMBIA": "🇨🇴",
    "ARGENTINA": "🇦🇷", "CHILE": "🇨🇱", "PERU": "🇵🇪", "ECUADOR": "🇪🇨",
    "EGYPT": "🇪🇬", "MOROCCO": "🇲🇦", "TUNISIA": "🇹🇳", "ALGERIA": "🇩🇿",
    "TANZANIA": "🇹🇿", "UGANDA": "🇺🇬", "RWANDA": "🇷🇼", "ETHIOPIA": "🇪🇹",
    "MOZAMBIQUE": "🇲🇿", "ZAMBIA": "🇿🇲", "ZIMBABWE": "🇿🇼", "BOTSWANA": "🇧🇼",
}

# ==========================================
#           USER & ACCOUNT MANAGEMENT
# ==========================================

def load_approved_users():
    """Load approved users from JSON"""
    global APPROVED_USERS
    APPROVED_USERS = {}
    
    if os.path.exists(APPROVED_USERS_FILE):
        try:
            with open(APPROVED_USERS_FILE, 'r') as f:
                data = json.load(f)
                users_data = data.get('users', [])
                
                # Handle old format (list of IDs)
                if isinstance(users_data, list):
                    for uid in users_data:
                        # Default to Plan 4 (Free) for migrated users
                        APPROVED_USERS[int(uid)] = {
                            "plan": "plan4",
                            "expiry": time.time() + (3650 * 86400),
                            "added_at": time.time()
                        }
                # Handle new format (dict)
                elif isinstance(users_data, dict):
                    # Convert keys to int
                    for k, v in users_data.items():
                        APPROVED_USERS[int(k)] = v
                        
        except Exception as e:
            print(f"[!] Error loading users: {e}")
            APPROVED_USERS = {}
            
    # Ensure owners are always approved with max privileges (Plan 2 equivalent but unlimited)
    for owner_id in OWNERS:
        if owner_id not in APPROVED_USERS:
            APPROVED_USERS[owner_id] = {
                "plan": "plan2", # Give owners best plan features
                "expiry": time.time() + (3650 * 86400),
                "added_at": time.time()
            }
            
    return APPROVED_USERS

def save_approved_users():
    """Save approved users to JSON"""
    try:
        with open(APPROVED_USERS_FILE, 'w') as f:
            json.dump({'users': APPROVED_USERS}, f, indent=2)
        return True
    except:
        return False

def is_user_allowed(user_id: int) -> bool:
    if user_id in OWNERS:
        return True
        
    if user_id in APPROVED_USERS:
        user_data = APPROVED_USERS[user_id]
        # Check expiry
        if user_data.get('expiry', 0) > time.time():
            # Update last used
            user_data['last_used'] = time.time()
            # Save occasionally? For now just in memory until next save
            return True
            
    return False

async def send_access_denied(update: Update):
    """Send access denied message with plan info"""
    plans_msg = ""
    for plan_id, plan in PLANS.items():
        duration = f"{plan['duration_days']} days" if plan['duration_days'] < 3650 else "Lifetime"
        plans_msg += (
            f"🔹 <b>{plan['name']}</b>\n"
            f"   ├ ⏳ {duration}\n"
            f"   ├ 🔐 {plan['max_accounts']} Accounts\n"
            f"   └ 📢 {plan['max_forwarders']} Forwarders\n\n"
        )

    msg = (
        "╔═══════════════════════════════╗\n"
        "║   🚫 𝗔𝗖𝗖𝗘𝗦𝗦 𝗗𝗘𝗡𝗜𝗘𝗗           ║\n"
        "╚═══════════════════════════════╝\n\n"
        "❌ You are not approved to use this bot.\n"
        "Please contact the owner to purchase a plan.\n\n"
        "<b>👤 Owner Contact:</b> @nothomeopbot\n\n"
        "<b>📋 Available Plans:</b>\n\n"
        f"{plans_msg}"
    )
    try:
        if update and hasattr(update, 'message') and update.message:
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except:
        pass

def is_owner(user_id: int) -> bool:
    return user_id in OWNERS

def load_accounts():
    """Load all IVAS accounts from JSON"""
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_accounts(accounts):
    """Save all IVAS accounts to JSON"""
    try:
        with open(ACCOUNTS_FILE, 'w') as f:
            json.dump(accounts, f, indent=2)
        return True
    except:
        return False

def get_user_accounts(user_id):
    """Get accounts for a specific user"""
    accounts = load_accounts()
    return {k: v for k, v in accounts.items() if v.get('user_id') == user_id}

def get_all_accounts_by_user():
    """Get all accounts grouped by user_id - for OTP routing"""
    accounts = load_accounts()
    by_user = {}
    for username, acc in accounts.items():
        uid = acc.get('user_id')
        if uid:
            if uid not in by_user:
                by_user[uid] = []
            by_user[uid].append(username)
    return by_user

def get_default_account(user_id):
    """Get default account for user"""
    accounts = get_user_accounts(user_id)
    for username, acc in accounts.items():
        if acc.get('is_default'):
            return username, acc
    if accounts:
        first = list(accounts.items())[0]
        return first[0], first[1]
    return None, None

def get_email_hash(email):
    """Generate a safe hash for email to use as session filename"""
    return hashlib.md5(email.lower().strip().encode()).hexdigest()[:16]

def get_session_file_by_email(email):
    """Get session file path based on email - SHARED across all users with same email"""
    if not os.path.exists(SESSIONS_DIR):
        os.makedirs(SESSIONS_DIR)
    email_hash = get_email_hash(email)
    return os.path.join(SESSIONS_DIR, f"session_{email_hash}.json")

def get_session_file(username):
    """Get session file path for a specific account - shared by email"""
    if not os.path.exists(SESSIONS_DIR):
        os.makedirs(SESSIONS_DIR)
    # Get email from accounts to share session
    accounts = load_accounts()
    if username in accounts:
        email = accounts[username].get('email')
        if email:
            session_file = get_session_file_by_email(email)
            log_to_file(f"[Session] {username} -> {email} -> {os.path.basename(session_file)}")
            return session_file
        else:
            log_to_file(f"[Session] {username}: No email found in account data")
    else:
        log_to_file(f"[Session] {username}: Account not found in ivas_accounts.json")
    # Fallback to username-based if account not found
    return os.path.join(SESSIONS_DIR, f"session_{username}.json")

def load_account_session(username):
    """Load session for specific account - shared by email"""
    session_file = get_session_file(username)
    if os.path.exists(session_file):
        for _ in range(3):
            try:
                with open(session_file, 'r') as f:
                    return json.load(f)
            except:
                time.sleep(0.1)
    return None

def save_account_session(username, session_data):
    """Save session for specific account"""
    session_file = get_session_file(username)
    try:
        with open(session_file, 'w') as f:
            json.dump(session_data, f)
        return True
    except:
        return False

def get_country_flag(rangename):
    """Get country flag from range name"""
    rangename_upper = rangename.upper()
    for key, flag in COUNTRY_FLAGS.items():
        if key in rangename_upper:
            return flag
    return "🌐"

def get_country_name(rangename):
    """Get country name from range"""
    rangename_upper = rangename.upper()
    if "BENIN" in rangename_upper:
        return "BENIN"
    elif "COTE" in rangename_upper or "CÔTE" in rangename_upper or "IVORY" in rangename_upper:
        return "COTE D'IVOIRE"
    elif "NIGERIA" in rangename_upper:
        return "NIGERIA"
    elif "GHANA" in rangename_upper:
        return "GHANA"
    elif "KENYA" in rangename_upper:
        return "KENYA"
    elif "SOUTH AFRICA" in rangename_upper or "SOUTHAFRICA" in rangename_upper:
        return "SOUTH AFRICA"
    elif "CAMEROON" in rangename_upper:
        return "CAMEROON"
    elif "SENEGAL" in rangename_upper:
        return "SENEGAL"
    elif "TOGO" in rangename_upper:
        return "TOGO"
    elif "MALI" in rangename_upper:
        return "MALI"
    elif "BURKINA" in rangename_upper:
        return "BURKINA FASO"
    elif "NIGER" in rangename_upper:
        return "NIGER"
    elif "GUINEA" in rangename_upper:
        if "BISSAU" in rangename_upper:
            return "GUINEA-BISSAU"
        return "GUINEA"
    elif "LIBERIA" in rangename_upper:
        return "LIBERIA"
    elif "SIERRA" in rangename_upper:
        return "SIERRA LEONE"
    elif "GAMBIA" in rangename_upper:
        return "GAMBIA"
    elif "INDIA" in rangename_upper:
        return "INDIA"
    elif "PAKISTAN" in rangename_upper:
        return "PAKISTAN"
    elif "BANGLADESH" in rangename_upper:
        return "BANGLADESH"
    elif "USA" in rangename_upper:
        return "USA"
    elif "UK" in rangename_upper:
        return "UK"
    elif "FRANCE" in rangename_upper:
        return "FRANCE"
    elif "GERMANY" in rangename_upper:
        return "GERMANY"
    elif "INDONESIA" in rangename_upper:
        return "INDONESIA"
    elif "MALAYSIA" in rangename_upper:
        return "MALAYSIA"
    elif "PHILIPPINES" in rangename_upper:
        return "PHILIPPINES"
    elif "VIETNAM" in rangename_upper:
        return "VIETNAM"
    elif "THAILAND" in rangename_upper:
        return "THAILAND"
    elif "BRAZIL" in rangename_upper:
        return "BRAZIL"
    elif "MEXICO" in rangename_upper:
        return "MEXICO"
    elif "COLOMBIA" in rangename_upper:
        return "COLOMBIA"
    elif "ARGENTINA" in rangename_upper:
        return "ARGENTINA"
    elif "CHILE" in rangename_upper:
        return "CHILE"
    elif "PERU" in rangename_upper:
        return "PERU"
    elif "ECUADOR" in rangename_upper:
        return "ECUADOR"
    elif "EGYPT" in rangename_upper:
        return "EGYPT"
    elif "MOROCCO" in rangename_upper:
        return "MOROCCO"
    elif "TUNISIA" in rangename_upper:
        return "TUNISIA"
    elif "ALGERIA" in rangename_upper:
        return "ALGERIA"
    elif "TANZANIA" in rangename_upper:
        return "TANZANIA"
    elif "UGANDA" in rangename_upper:
        return "UGANDA"
    elif "RWANDA" in rangename_upper:
        return "RWANDA"
    elif "ETHIOPIA" in rangename_upper:
        return "ETHIOPIA"
    elif "MOZAMBIQUE" in rangename_upper:
        return "MOZAMBIQUE"
    elif "ZAMBIA" in rangename_upper:
        return "ZAMBIA"
    elif "ZIMBABWE" in rangename_upper:
        return "ZIMBABWE"
    elif "BOTSWANA" in rangename_upper:
        return "BOTSWANA"
    else:
        return "UNKNOWN"

def load_monitoring_state():
    """Load monitoring state from file"""
    global monitoring_state
    if os.path.exists(MONITOR_STATE_FILE):
        try:
            with open(MONITOR_STATE_FILE, 'r') as f:
                data = json.load(f)
                monitoring_state["active_accounts"] = set(data.get("active_accounts", []))
                monitoring_state["last_stop_time"] = data.get("last_stop_time")
                log_to_file(f"[State] Loaded monitoring state: {len(monitoring_state['active_accounts'])} active accounts")
        except Exception as e:
            log_to_file(f"[State] Error loading monitoring state: {e}")
            monitoring_state["active_accounts"] = set()
    else:
        log_to_file("[State] No monitoring state file found, starting fresh")

def save_monitoring_state():
    """Save monitoring state to file"""
    try:
        state_data = {
            "active_accounts": list(monitoring_state["active_accounts"]),
            "last_stop_time": monitoring_state["last_stop_time"]
        }
        with open(MONITOR_STATE_FILE, 'w') as f:
            json.dump(state_data, f, indent=2)
        log_to_file(f"[State] Saved monitoring state: {len(monitoring_state['active_accounts'])} active accounts")
    except Exception as e:
        log_to_file(f"[State] Error saving monitoring state: {e}")

def delete_account_sessions_by_email(email):
    """Delete all session files for a given email hash (strict refresh)"""
    try:
        email_hash = get_email_hash(email)
        session_file = os.path.join(SESSIONS_DIR, f"session_{email_hash}.json")
        if os.path.exists(session_file):
            os.remove(session_file)
            log_to_file(f"[Session] Deleted session file for {email}: {session_file}")
            return True
        else:
            log_to_file(f"[Session] No session file found for {email}: {session_file}")
            return False
    except Exception as e:
        log_to_file(f"[Session] Error deleting session for {email}: {e}")
        return False

def login_and_get_session(email, password, username, force_new=False):
    """Login to IVAS and return session data (cookies, headers, etc.)"""
    try:
        # If force_new, delete existing session first
        if force_new:
            delete_account_sessions_by_email(email)
            log_to_file(f"[Login] Force new session for {username}/{email}, deleted old session")
        else:
            # Check if session already exists and is valid
            session_data = load_account_session(username)
            if session_data:
                # Validate session by trying to access dashboard
                session = requests.Session()
                session.headers.update(session_data.get('headers', {}))
                if 'cookies' in session_data:
                    for name, value in session_data['cookies'].items():
                        session.cookies.set(name, value)

                try:
                    proxy = load_proxy()
                    resp = session.get(DASHBOARD_URL, proxies={'http': proxy, 'https': proxy} if proxy else None, timeout=10)
                    if resp.status_code == 200:
                        # Check if we're still logged in by looking for dashboard elements
                        if 'logout' in resp.text.lower() or 'dashboard' in resp.text.lower():
                            log_to_file(f"[Login] Using existing valid session for {username}/{email}")
                            return True, session_data
                except:
                    pass  # Session validation failed, continue with login

        log_to_file(f"[Login] Logging in for {username}/{email}")
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': DEFAULT_UA,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Get login page to extract CSRF token
        proxy = load_proxy()
        login_page = session.get(LOGIN_URL, proxies={'http': proxy, 'https': proxy} if proxy else None, timeout=10)
        login_page.raise_for_status()
        
        soup = BeautifulSoup(login_page.text, 'html.parser')
        csrf_token = None
        
        # Look for CSRF token in meta tags or hidden inputs
        csrf_meta = soup.find('meta', attrs={'name': 'csrf-token'})
        if csrf_meta:
            csrf_token = csrf_meta.get('content')
        else:
            csrf_input = soup.find('input', attrs={'name': '_token'})
            if csrf_input:
                csrf_token = csrf_input.get('value')
        
        if not csrf_token:
            # Try to find CSRF token in script tags or other locations
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'csrf' in script.string.lower():
                    import re
                    matches = re.findall(r'["\']([a-f0-9]{40})["\']', script.string)
                    if matches:
                        csrf_token = matches[0]
                        break
        
        if not csrf_token:
            return False, "No CSRF token found"
        
        # Prepare login data
        login_data = {
            '_token': csrf_token,
            'email': email,
            'password': password,
            'remember': 'on'
        }
        
        # Perform login
        login_resp = session.post(LOGIN_URL, data=login_data, 
                                  proxies={'http': proxy, 'https': proxy} if proxy else None, timeout=15)
        
        if login_resp.status_code != 200:
            return False, f"Login failed with status {login_resp.status_code}"
        
        # Check if login was successful
        if 'dashboard' not in login_resp.text.lower() and 'logout' not in login_resp.text.lower():
            if 'invalid' in login_resp.text.lower() or 'incorrect' in login_resp.text.lower():
                return False, "Invalid email or password"
            elif 'captcha' in login_resp.text.lower() or 'verify' in login_resp.text.lower():
                return False, "Captcha or verification required"
            else:
                return False, f"Login failed - {login_resp.text[:200]}..."
        
        # Extract cookies and headers for future requests
        cookies = {}
        for cookie in session.cookies:
            cookies[cookie.name] = cookie.value
        
        session_data = {
            'cookies': cookies,
            'headers': dict(session.headers),
            'email': email,
            'username': username,
            'created_at': time.time()
        }
        
        # Save session data
        if save_account_session(username, session_data):
            log_to_file(f"[Login] Successfully created session for {username}/{email}")
            return True, session_data
        else:
            return False, "Failed to save session"
            
    except requests.exceptions.Timeout:
        return False, "Login timeout"
    except requests.exceptions.RequestException as e:
        return False, f"Network error: {str(e)}"
    except Exception as e:
        log_to_file(f"[Login] Error logging in {username}: {e}")
        traceback.print_exc()
        return False, str(e)
        
        # Get login page to extract CSRF token
        proxy = load_proxy()
        login_page = session.get(LOGIN_URL, proxies={'http': proxy, 'https': proxy} if proxy else None, timeout=10)
        login_page.raise_for_status()
        
        soup = BeautifulSoup(login_page.text, 'html.parser')
        csrf_token = None
        
        # Look for CSRF token in meta tags or hidden inputs
        csrf_meta = soup.find('meta', attrs={'name': 'csrf-token'})
        if csrf_meta:
            csrf_token = csrf_meta.get('content')
        else:
            csrf_input = soup.find('input', attrs={'name': '_token'})
            if csrf_input:
                csrf_token = csrf_input.get('value')
        
        if not csrf_token:
            # Try to find CSRF token in script tags or other locations
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'csrf' in script.string.lower():
                    import re
                    matches = re.findall(r'["\']([a-f0-9]{40})["\']', script.string)
                    if matches:
                        csrf_token = matches[0]
                        break
        
        if not csrf_token:
            return False, "No CSRF token found"
        
        # Prepare login data
        login_data = {
            '_token': csrf_token,
            'email': email,
            'password': password,
            'remember': 'on'
        }
        
        # Perform login
        login_resp = session.post(LOGIN_URL, data=login_data, 
                                  proxies={'http': proxy, 'https': proxy} if proxy else None, timeout=15)
        
        if login_resp.status_code != 200:
            return False, f"Login failed with status {login_resp.status_code}"
        
        # Check if login was successful
        if 'dashboard' not in login_resp.text.lower() and 'logout' not in login_resp.text.lower():
            if 'invalid' in login_resp.text.lower() or 'incorrect' in login_resp.text.lower():
                return False, "Invalid email or password"
            elif 'captcha' in login_resp.text.lower() or 'verify' in login_resp.text.lower():
                return False, "Captcha or verification required"
            else:
                return False, f"Login failed - {login_resp.text[:200]}..."
        
        # Extract cookies and headers for future requests
        cookies = {}
        for cookie in session.cookies:
            cookies[cookie.name] = cookie.value
        
        session_data = {
            'cookies': cookies,
            'headers': dict(session.headers),
            'email': email,
            'username': username,
            'created_at': time.time()
        }
        
        # Save session data
        if save_account_session(username, session_data):
            log_to_file(f"[Login] Successfully created session for {username}/{email}")
            return True, session_data
        else:
            return False, "Failed to save session"
            
    except requests.exceptions.Timeout:
        return False, "Login timeout"
    except requests.exceptions.RequestException as e:
        return False, f"Network error: {str(e)}"
    except Exception as e:
        log_to_file(f"[Login] Error logging in {username}: {e}")
        traceback.print_exc()
        return False, str(e)

async def refresh_account_session(username, force_new=False):
    """Refresh session for a specific account"""
    try:
        accounts = load_accounts()
        if username not in accounts:
            log_to_file(f"[Refresh] Account {username} not found in accounts")
            return False
            
        acc = accounts[username]
        email = acc.get('email')
        password = acc.get('password')
        
        if not email or not password:
            log_to_file(f"[Refresh] Missing email or password for {username}")
            return False
        
        log_to_file(f"[Refresh] Refreshing session for {username} (force_new={force_new})")
        
        # Use thread pool to run the synchronous login in background
        loop = asyncio.get_event_loop()
        success, result = await loop.run_in_executor(
            thread_pool, 
            lambda: login_and_get_session(email, password, username, force_new=force_new)
        )
        
        if success:
            log_to_file(f"[Refresh] Session refreshed successfully for {username}")
            return True
        else:
            log_to_file(f"[Refresh] Failed to refresh session for {username}: {result}")
            return False
            
    except Exception as e:
        log_to_file(f"[Refresh] Error refreshing session for {username}: {e}")
        return False

def get_session_headers(username):
    """Get headers and cookies for a specific account's session"""
    session_data = load_account_session(username)
    if not session_data:
        return None, None
    
    headers = session_data.get('headers', {}).copy()
    cookies = session_data.get('cookies', {})
    
    # Add default headers if not present
    headers.setdefault('User-Agent', DEFAULT_UA)
    headers.setdefault('Accept', 'application/json, text/html, */*; q=0.01')
    headers.setdefault('X-Requested-With', 'XMLHttpRequest')
    
    return headers, cookies

def make_authenticated_request(username, url, method='GET', data=None, json_data=None):
    """Make an authenticated request using the account's session"""
    try:
        headers, cookies = get_session_headers(username)
        if not headers or not cookies:
            return None, "No valid session found"
        
        session = requests.Session()
        session.headers.update(headers)
        for name, value in cookies.items():
            session.cookies.set(name, value)
        
        proxy = load_proxy()
        kwargs = {
            'proxies': {'http': proxy, 'https': proxy} if proxy else None,
            'timeout': 30
        }
        
        if method.upper() == 'POST':
            if json_data:
                kwargs['json'] = json_data
            elif data:
                kwargs['data'] = data
            response = session.post(url, **kwargs)
        else:
            if data:
                kwargs['params'] = data
            response = session.get(url, **kwargs)
        
        return response, None
        
    except requests.exceptions.Timeout:
        return None, "Request timeout"
    except requests.exceptions.RequestException as e:
        return None, f"Network error: {str(e)}"
    except Exception as e:
        log_to_file(f"[Request] Error making request for {username}: {e}")
        return None, str(e)

async def otp_monitoring_loop():
    """OTP monitoring - SIMPLE: ranges → numbers → SMS, every 2 seconds"""
    global otp_monitoring_active, telegram_app, failed_users, sent_otps_cache
    log_to_file("[OTP] Starting OTP monitoring loop")
    
    # Load accounts to monitor
    accounts = load_accounts()
    otp_monitor_accounts = list(accounts.keys())
    
    # Show monitoring info with email grouping
    email_groups = {}
    for username, acc in accounts.items():
        email = acc.get('email', 'unknown')
        if email not in email_groups:
            email_groups[email] = []
        email_groups[email].append(username)
    
    log_to_file(f"[OTP] Monitoring {len(otp_monitor_accounts)} accounts in {len(email_groups)} email groups:")
    for email, usernames in email_groups.items():
        log_to_file(f"[OTP]   {email} -> {usernames}")
    
    session_refresh_needed = set()
    sent_otps_cache = {}  # Track sent OTPs to avoid duplicates
    last_sms_check = {}  # Track last SMS check time per account
    
    while otp_monitoring_active:
        try:
            current_time = time.time()
            
            # Process any session refreshes needed
            if session_refresh_needed:
                for username in list(session_refresh_needed):
                    # Refresh the session
                    log_to_file(f"[OTP] Attempting to refresh session for {username}")
                    if await refresh_account_session(username):
                        session_refresh_needed.discard(username)
                        print(f"[OTP] ✅ Session refreshed for {username}")
                        log_to_file(f"[OTP] Session refreshed for {username}")
                    else:
                        log_to_file(f"[OTP] Session refresh failed for {username}")
                        session_refresh_needed.discard(username)  # Don't keep retrying
                        print(f"[OTP] ❌ Session refresh failed for {username}")
            
            # Get current accounts (in case they were modified)
            accounts = load_accounts()
            otp_monitor_accounts = list(accounts.keys())
            
            if not otp_monitor_accounts:
                log_to_file("[OTP] No accounts to monitor, sleeping...")
                await asyncio.sleep(5)
                continue
            
            log_to_file(f"[OTP] Querying {len(otp_monitor_accounts)} accounts: {', '.join(otp_monitor_accounts)}")
            
            # Process each account
            for username in otp_monitor_accounts:
                acc = accounts.get(username)
                if not acc:
                    continue
                    
                email = acc.get('email')
                user_id = acc.get('user_id')
                
                # Check if session exists and is valid
                session_data = load_account_session(username)
                if not session_data:
                    log_to_file(f"[OTP] {username}: No session data, queuing refresh")
                    session_refresh_needed.add(username)
                    continue
                
                try:
                    # Get available ranges for this account
                    response, error = make_authenticated_request(username, GET_NUMBERS_URL)
                    if error:
                        log_to_file(f"[OTP] {username}: Request failed: {error}")
                        if "419" in error or "401" in error or "expired" in error.lower():
                            session_refresh_needed.add(username)
                        continue
                    
                    if response is None:
                        log_to_file(f"[OTP] {username}: No response received")
                        session_refresh_needed.add(username)
                        continue
                    
                    if response.status_code == 419 or response.status_code == 401:
                        log_to_file(f"[OTP] {username}: Session expired (419/401), queuing refresh")
                        session_refresh_needed.add(username)
                        continue
                    
                    if response.status_code != 200:
                        log_to_file(f"[OTP] {username}: Got status {response.status_code}")
                        if response.status_code == 500:
                            session_refresh_needed.add(username)  # Server error, might need refresh
                        continue
                    
                    try:
                        ranges_data = response.json()
                    except:
                        log_to_file(f"[OTP] {username}: Invalid JSON response: {response.text[:200]}...")
                        continue
                    
                    if not ranges_data or not isinstance(ranges_data, list):
                        log_to_file(f"[OTP] {username}: No ranges found (empty result)")
                        continue
                    
                    log_to_file(f"[OTP] {username}: Found {len(ranges_data)} ranges")
                    
                    # Process each range
                    for range_item in ranges_data:
                        try:
                            range_name = range_item.get('range', 'Unknown')
                            range_number = range_item.get('number', '')
                            country_name = get_country_name(range_name)
                            flag = get_country_flag(range_name)
                            
                            # Get SMS for this range/number
                            sms_url = f"{GET_SMS_NUMBER_URL}/{quote_plus(range_number)}"
                            sms_response, sms_error = make_authenticated_request(username, sms_url)
                            
                            if sms_error:
                                log_to_file(f"[OTP] {username}: SMS request failed for {range_number}: {sms_error}")
                                continue
                            
                            if sms_response is None:
                                continue
                            
                            if sms_response.status_code == 419 or sms_response.status_code == 401:
                                log_to_file(f"[OTP] {username}: Session expired during SMS fetch for {range_number}, queuing refresh")
                                session_refresh_needed.add(username)
                                break  # Break to refresh session
                            
                            if sms_response.status_code != 200:
                                continue
                            
                            try:
                                sms_data = sms_response.json()
                            except:
                                continue
                            
                            if not sms_data or not isinstance(sms_data, list):
                                continue
                            
                            # Process each SMS
                            for sms_item in sms_data:
                                try:
                                    sms_number = sms_item.get('number', '')
                                    sms_content = sms_item.get('content', '')
                                    sms_time = sms_item.get('created_at', '')
                                    
                                    if not sms_content:
                                        continue
                                    
                                    # Create unique ID for this SMS to avoid duplicates
                                    sms_id = f"{range_number}_{sms_number}_{sms_content[:50]}"
                                    if sms_id in sent_otps_cache:
                                        continue
                                    
                                    # Mark as sent to avoid duplicates
                                    sent_otps_cache[sms_id] = time.time()
                                    
                                    # Clean up old cache entries (older than 1 hour)
                                    current_time = time.time()
                                    sent_otps_cache = {k: v for k, v in sent_otps_cache.items() 
                                                     if current_time - v < 3600}
                                    
                                    # Format the OTP message
                                    otp_msg = (
                                        f"📱 <b>NEW OTP RECEIVED</b>\n\n"
                                        f"🔢 <b>Number:</b> <code>{sms_number}</code>\n"
                                        f"🏷️ <b>Range:</b> {flag} {country_name} ({range_number})\n"
                                        f"💬 <b>Message:</b> {sms_content}\n"
                                        f"⏰ <b>Time:</b> {sms_time}\n"
                                        f"👤 <b>Account:</b> {username}"
                                    )
                                    
                                    # Send to user's channels
                                    otp_channels = acc.get('otp_channels', [])
                                    for channel_id in otp_channels:
                                        try:
                                            if telegram_app:
                                                await telegram_app.bot.send_message(
                                                    chat_id=channel_id,
                                                    text=otp_msg,
                                                    parse_mode=ParseMode.HTML
                                                )
                                                log_to_file(f"[OTP] Sent OTP to channel {channel_id} for {username}")
                                        except Exception as e:
                                            log_to_file(f"[OTP] Error sending to channel {channel_id}: {e}")
                                
                                except Exception as e:
                                    log_to_file(f"[OTP] Error processing SMS for {username}: {e}")
                                    continue
                                    
                        except Exception as e:
                            log_to_file(f"[OTP] Error processing range for {username}: {e}")
                            continue
                
                except Exception as e:
                    log_to_file(f"[OTP] Error monitoring {username}: {e}")
                    session_refresh_needed.add(username)
                    continue
            
            # Sleep before next iteration
            await asyncio.sleep(2)  # Check every 2 seconds
            
        except Exception as e:
            log_to_file(f"[OTP] Error in monitoring loop: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)  # Wait longer on error

    log_to_file("[OTP] OTP monitoring loop stopped")

# Background session renewal task
async def background_session_renewal_loop():
    """Background task: every hour, renew all sessions in background and atomically replace old session files."""
    log_to_file("[Renewal] Starting background session renewal loop")
    
    while True:
        try:
            accounts = load_accounts()
            emails_done = set()
            
            log_to_file(f"[Renewal] Starting hourly renewal for {len(accounts)} accounts")
            
            for username, acc in accounts.items():
                email = acc.get('email')
                if not email or email in emails_done:
                    continue
                emails_done.add(email)
                
                # Generate new session in background (force new)
                log_to_file(f"[Renewal] Generating new session for {username}/{email} in background...")
                success, msg = await refresh_account_session(username, force_new=True)
                
                if success:
                    log_to_file(f"[Renewal] Successfully renewed session for {email} (account: {username})")
                else:
                    log_to_file(f"[Renewal] Failed to renew session for {email} (account: {username}): {msg}")
        
        except Exception as e:
            log_to_file(f"[Renewal] Error in renewal loop: {e}")
            traceback.print_exc()
        
        # Sleep for 1 hour
        await asyncio.sleep(3600)

def start_background_renewal():
    """Start the background renewal task"""
    loop = asyncio.get_event_loop()
    
    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(background_session_renewal_loop())
    
    t = threading.Thread(target=run, daemon=True)
    t.start()
    log_to_file("[Renewal] Background renewal thread started")

# ==========================================
#              COMMAND HANDLERS
# ==========================================

async def cmd_startotp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /startotp [username] command"""
    global otp_monitoring_active, otp_monitoring_task, monitoring_state
    
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    args = context.args
    accounts = load_accounts()
    
    if args and len(args) > 0:
        target_username = args[0]
        if target_username not in accounts:
            await update.message.reply_text(f"❌ Account '{target_username}' not found in your accounts.")
            return
        
        # Check if this account belongs to the user
        if accounts[target_username].get('user_id') != user_id and not is_owner(user_id):
            await update.message.reply_text(f"❌ You don't own account '{target_username}'.")
            return
        
        # Add to monitoring state
        monitoring_state["active_accounts"].add(target_username)
        save_monitoring_state()
        
        # Start monitoring if not already active
        if not otp_monitoring_active:
            otp_monitoring_active = True
            otp_monitoring_task = asyncio.create_task(otp_monitoring_loop())
            await update.message.reply_text(f"✅ Started OTP monitoring for account: <b>{target_username}</b>\n🟢 Bot is now monitoring all accounts", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(f"✅ Added account <b>{target_username}</b> to monitoring", parse_mode=ParseMode.HTML)
    else:
        # Start monitoring all user's accounts
        user_accounts = get_user_accounts(user_id)
        if not user_accounts:
            await update.message.reply_text("❌ You don't have any accounts added. Use /add first.")
            return
        
        # Add all user accounts to monitoring state
        for username in user_accounts.keys():
            monitoring_state["active_accounts"].add(username)
        
        save_monitoring_state()
        
        # Start monitoring if not already active
        if not otp_monitoring_active:
            otp_monitoring_active = True
            otp_monitoring_task = asyncio.create_task(otp_monitoring_loop())
            await update.message.reply_text(f"✅ Started OTP monitoring for all your accounts\n🟢 Bot is now monitoring: {', '.join(user_accounts.keys())}")
        else:
            await update.message.reply_text(f"✅ Added all your accounts to monitoring: {', '.join(user_accounts.keys())}")

async def cmd_stopotp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stopotp command"""
    global otp_monitoring_active, otp_monitoring_task, monitoring_state
    
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    # Get user's accounts to stop monitoring
    user_accounts = get_user_accounts(user_id)
    user_account_names = set(user_accounts.keys())
    
    # Remove user's accounts from monitoring state
    monitoring_state["active_accounts"] = monitoring_state["active_accounts"] - user_account_names
    monitoring_state["last_stop_time"] = time.time()
    save_monitoring_state()
    
    # If no accounts left in monitoring state, stop the monitoring
    if not monitoring_state["active_accounts"]:
        if otp_monitoring_active and otp_monitoring_task:
            otp_monitoring_active = False
            otp_monitoring_task.cancel()
            otp_monitoring_task = None
            await update.message.reply_text("🔴 Stopped OTP monitoring for all your accounts")
        else:
            await update.message.reply_text("ℹ️ OTP monitoring was not active for your accounts")
    else:
        await update.message.reply_text(f"🔴 Stopped OTP monitoring for your accounts: {', '.join(user_account_names)}\n🟢 Other accounts still being monitored")

async def cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /refresh command (strict: always delete old, force new)"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    args = context.args
    if not args or len(args) == 0:
        await update.message.reply_text("❌ Please specify an account username. Usage: /refresh username")
        return
    
    username = args[0]
    accounts = load_accounts()
    
    if username not in accounts:
        await update.message.reply_text(f"❌ Account '{username}' not found.")
        return
    
    # Check ownership
    if accounts[username].get('user_id') != user_id and not is_owner(user_id):
        await update.message.reply_text(f"❌ You don't own account '{username}'.")
        return
    
    processing_msg = await update.message.reply_text(f"⏳ Strictly refreshing {username} (deleting all old sessions)...")
    
    # Force delete and recreate session
    success = await refresh_account_session(username, force_new=True)
    
    if success:
        await processing_msg.edit_text(f"✅ Session strictly refreshed for <b>{username}</b>", parse_mode=ParseMode.HTML)
    else:
        await processing_msg.edit_text(f"❌ Failed to refresh session for <b>{username}</b>", parse_mode=ParseMode.HTML)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user_id = update.effective_user.id
    
    welcome_msg = (
        "╔═══════════════════════════════╗\n"
        "║        🤖 IVAS OTP BOT        ║\n"
        "╚═══════════════════════════════╝\n\n"
        "Welcome to the IVAS OTP Monitoring Bot!\n\n"
        
        "📋 <b>Available Commands:</b>\n"
        "  ▸ /add — Add new IVAS account\n"
        "  ▸ /myaccs — View your accounts\n"
        "  ▸ /startotp [user] — Start scanning\n"
        "  ▸ /stopotp — Stop all scanning\n"
        "  ▸ /refresh [user] — Refresh session\n"
        "  ▸ /help — Show this help\n\n"
        
        "⚙️ <i>Features:</i>\n"
        "  ▸ Real-time OTP monitoring\n"
        "  ▸ Automatic session renewal\n"
        "  ▸ Shared sessions for same emails\n"
        "  ▸ Channel forwarding\n"
        "  ▸ Persistent monitoring state\n"
        "  ▸ /startotp — Start scanning\n"
        "  ▸ /stopotp — Stop all scanning\n"
        "  ▸ /refresh — Refresh session\n"
        "  ▸ /status — Check bot status\n\n"
        "🔄 <i>Sessions auto-refresh hourly</i>\n\n"
        "📞 Need help? Contact: @nothomeopbot"
    )
    
    await update.message.reply_text(welcome_msg, parse_mode=ParseMode.HTML)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    help_msg = (
        "╔═══════════════════════════════╗\n"
        "║         📚 HELP GUIDE         ║\n"
        "╚═══════════════════════════════╝\n\n"
        
        "🔐 <b>Account Management:</b>\n"
        "  • /add - Add new IVAS account\n"
        "    Enter email, password, username\n\n"
        
        "  • /myaccs - View your accounts\n"
        "    Shows all your added accounts\n\n"
        
        "📡 <b>OTP Monitoring:</b>\n"
        "  • /startotp [username] - Start monitoring\n"
        "    Without username: monitors all your accounts\n\n"
        "  • /stopotp - Stop monitoring your accounts\n\n"
        "  • /refresh username - Force refresh session\n"
        "    Deletes old session, logs in fresh\n\n"
        
        "💡 <b>Tips:</b>\n"
        "  • Accounts with same email share sessions\n"
        "  • OTPs sent to your channel/group\n"
        "  • Sessions auto-refresh every hour\n"
        "  • Bot remembers monitoring state on restart\n\n"
        
        "📞 Support: @nothomeopbot"
    )
    
    await update.message.reply_text(help_msg, parse_mode=ParseMode.HTML)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command"""
    global otp_monitoring_active
    user_id = update.effective_user.id
    
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    # Get user's accounts
    user_accounts = get_user_accounts(user_id)
    user_account_names = list(user_accounts.keys())
    
    # Check monitoring state for user's accounts
    user_monitoring = [acc for acc in user_account_names if acc in monitoring_state["active_accounts"]]
    
    otp_emoji = "🟢" if otp_monitoring_active else "⚫"
    otp_text = "Active" if otp_monitoring_active else "Stopped"
    
    status_msg = (
        f"╔═══════════════════════════════╗\n"
        f"║         📊 BOT STATUS        ║\n"
        f"╚═══════════════════════════════╝\n\n"
        
        f"🤖 <b>Bot Status:</b> {otp_emoji} {otp_text}\n"
        f"👤 <b>Your Accounts:</b> {len(user_account_names)}\n"
        f"📡 <b>Monitoring:</b> {len(user_monitoring)}/{len(user_account_names)}\n\n"
    )
    
    if user_account_names:
        status_msg += "<b>📋 Your Accounts:</b>\n"
        for acc_name in user_account_names:
            is_monitoring = "🟢" if acc_name in user_monitoring else "🔴"
            status_msg += f"  • {is_monitoring} {acc_name}\n"
    
    status_msg += f"\n🔄 <i>Last state save: {monitoring_state.get('last_stop_time', 'Never')}</i>"
    
    await update.message.reply_text(status_msg, parse_mode=ParseMode.HTML)

# ==========================================
#              MAIN APPLICATION
# ==========================================

def main():
    """Main function to start the bot"""
    global telegram_app
    
    print("[INFO] Loading proxies...")
    load_proxies()
    
    print("[INFO] Loading approved users...")
    load_approved_users()
    
    print("[INFO] Loading monitoring state...")
    load_monitoring_state()
    
    print("[INFO] Setting up database...")
    # Initialize database if needed
    global db_conn
    db_conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = db_conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS otp_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_number TEXT,
            otp_code TEXT,
            message_content TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            account_username TEXT
        )
    ''')
    db_conn.commit()
    
    print("[INFO] Creating Telegram application...")
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add command handlers
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("add", cmd_add))
    application.add_handler(CommandHandler("myaccs", cmd_myaccs))
    application.add_handler(CommandHandler("startotp", cmd_startotp))
    application.add_handler(CommandHandler("stopotp", cmd_stopotp))
    application.add_handler(CommandHandler("refresh", cmd_refresh))
    application.add_handler(CommandHandler("status", cmd_status))
    
    # Store reference to application for sending messages
    telegram_app = application
    
    # Set bot commands
    async def set_bot_commands():
        commands = [
            BotCommand("start", "Start the bot"),
            BotCommand("help", "Show help"),
            BotCommand("add", "Add IVAS account"),
            BotCommand("myaccs", "View your accounts"),
            BotCommand("startotp", "Start OTP monitoring"),
            BotCommand("stopotp", "Stop OTP monitoring"),
            BotCommand("refresh", "Refresh session"),
            BotCommand("status", "Check bot status"),
        ]
        await application.bot.set_my_commands(commands)
    
    # Run the command setup
    asyncio.run(set_bot_commands())
    
    print("[INFO] Starting background session renewal...")
    start_background_renewal()
    
    print("[INFO] Checking if we should resume monitoring...")
    # Check if there were active accounts when bot was stopped
    if monitoring_state["active_accounts"]:
        print(f"[INFO] Found {len(monitoring_state['active_accounts'])} accounts to resume monitoring for: {list(monitoring_state['active_accounts'])}")
        global otp_monitoring_active, otp_monitoring_task
        otp_monitoring_active = True
        otp_monitoring_task = asyncio.create_task(otp_monitoring_loop())
        log_to_file(f"[INFO] Resumed monitoring for {len(monitoring_state['active_accounts'])} accounts")
    else:
        print("[INFO] No accounts to resume monitoring for")
    
    print("[INFO] Starting bot polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# Complete command handlers
async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /add command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    # Get user plan to check account limits
    user_plan = APPROVED_USERS.get(user_id, {}).get('plan', 'plan4')
    max_accounts = PLANS[user_plan]['max_accounts']
    
    user_accounts = get_user_accounts(user_id)
    if len(user_accounts) >= max_accounts:
        await update.message.reply_text(
            f"❌ You've reached the account limit for your plan ({max_accounts}).\n"
            f"Upgrade your plan to add more accounts."
        )
        return
    
    # Start conversation to collect account details
    user_conversations[user_id] = {
        'state': STATE_ADD_EMAIL,
        'data': {}
    }
    
    await update.message.reply_text(
        "📧 Please send the email address for the IVAS account:"
    )

async def cmd_myaccs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /myaccs command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    user_accounts = get_user_accounts(user_id)
    
    if not user_accounts:
        await update.message.reply_text("❌ You don't have any accounts added yet.\nUse /add to add an IVAS account.")
        return
    
    acc_list = []
    for username, acc in user_accounts.items():
        is_monitoring = "🟢" if username in monitoring_state["active_accounts"] else "🔴"
        channel_count = len(acc.get('otp_channels', []))
        acc_list.append(
            f"• {is_monitoring} <code>{username}</code>\n"
            f"  └ Email: {acc.get('email', 'no email')}, Channels: {channel_count}"
        )
    
    msg = f"📋 <b>Your Accounts ({len(user_accounts)}):</b>\n\n" + "\n".join(acc_list)
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

if __name__ == '__main__':
    main()