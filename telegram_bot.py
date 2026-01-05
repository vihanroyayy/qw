import shutil
import threading
async def background_session_renewal_loop():
    """Background task: every hour, renew all sessions in background and atomically replace old session files."""
    while True:
        try:
            accounts = load_accounts()
            emails_done = set()
            for username, acc in accounts.items():
                email = acc.get('email')
                if not email or email in emails_done:
                    continue
                emails_done.add(email)
                # Generate new session in background
                log_to_file(f"[Renewal] Generating new session for {username}/{email} in background...")
                success, msg = login_and_get_session(email, acc.get('password'), username, force_new=True)
                if success:
                    # Move new session file to .new, then atomically replace
                    session_file = get_session_file_by_email(email)
                    new_file = session_file + ".new"
                    try:
                        shutil.copy2(session_file, new_file)
                        os.replace(new_file, session_file)
                        log_to_file(f"[Renewal] Session for {email} replaced atomically.")
                    except Exception as e:
                        log_to_file(f"[Renewal] Failed to atomically replace session for {email}: {e}")
                else:
                    log_to_file(f"[Renewal] Failed to renew session for {email}: {msg}")
        except Exception as e:
            log_to_file(f"[Renewal] Error in renewal loop: {e}")
        await asyncio.sleep(3600)  # 1 hour
def start_background_renewal():
    loop = asyncio.get_event_loop()
    def run():
        asyncio.set_event_loop(loop)
        loop.create_task(background_session_renewal_loop())
        loop.run_forever()
    t = threading.Thread(target=run, daemon=True)
    t.start()
# -*- coding: utf-8 -*-
"""
IvaSMS Telegram Bot v16 - Shared Sessions by Email
Features:
  - Shared sessions for accounts with same email (one session file per unique email)
  - OTPs sent to ALL users with duplicate emails
  - Multi-user concurrent support
  - Robust network retry for connection errors
  - Fixed 419 CSRF errors with auto-refresh
"""

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

def get_country_flag(range_name):
    """Get country flag emoji from range name"""
    if not range_name:
        return "🌍"
    range_upper = range_name.upper()
    if range_upper in COUNTRY_FLAGS:
        return COUNTRY_FLAGS[range_upper]
    first_word = range_name.split()[0].upper()
    if first_word in COUNTRY_FLAGS:
        return COUNTRY_FLAGS[first_word]
    if "COTE" in range_upper or "IVOIRE" in range_upper:
        return "🇨🇮"
    if "BURKINA" in range_upper:
        return "🇧🇫"
    return "🌍"

def get_country_name(range_name):
    """Get proper country name from range"""
    if not range_name:
        return "Unknown"
    range_upper = range_name.upper()
    if "COTE" in range_upper or "IVOIRE" in range_upper:
        return "Côte d'Ivoire"
    if "BURKINA" in range_upper:
        return "Burkina Faso"
    return range_name.split()[0].title()

# ==========================================
#           SESSION MANAGEMENT
# ==========================================

def save_session(cookies, ua, token, username=None):
    """Save session to disk"""
    try:
        session_data = {"cookies": cookies, "ua": ua, "token": token, "time": time.time()}
        if username:
            save_account_session(username, session_data)
            print(f"[💾] Session saved for {username}")
        return True
    except Exception as e:
        print(f"[!] Failed to save session: {e}")
        return False

def load_session(username=None):
    """Load session from disk"""
    if username:
        return load_account_session(username)
    return None

def init_database():
    """Initialize SQLite database for OTP tracking"""
    global db_conn
    db_conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    db_conn.execute('PRAGMA journal_mode=WAL')
    db_conn.execute('PRAGMA synchronous=NORMAL')
    c = db_conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS otp_hashes (
            hash TEXT PRIMARY KEY,
            phone TEXT,
            service TEXT,
            range_name TEXT,
            message TEXT,
            otp TEXT,
            timestamp TEXT,
            created_at REAL,
            username TEXT
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_hash ON otp_hashes(hash)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_username ON otp_hashes(username)')
    db_conn.commit()
    print(f"[DB] Database initialized: {DB_FILE}")

def is_otp_seen(msg_hash):
    global db_conn
    if db_conn is None:
        return False
    c = db_conn.cursor()
    c.execute('SELECT 1 FROM otp_hashes WHERE hash = ? LIMIT 1', (msg_hash,))
    return c.fetchone() is not None

def save_otp_to_db(msg_hash, phone, service, range_name, message, otp, timestamp, username=None):
    global db_conn
    if db_conn is None:
        return False
    try:
        c = db_conn.cursor()
        c.execute('''
            INSERT OR IGNORE INTO otp_hashes 
            (hash, phone, service, range_name, message, otp, timestamp, created_at, username)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (msg_hash, phone, service, range_name, message, otp, timestamp, time.time(), username))
        db_conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Error saving: {e}")
        return False

def get_otp_count():
    global db_conn
    if db_conn is None:
        return 0
    try:
        c = db_conn.cursor()
        c.execute('SELECT COUNT(*) FROM otp_hashes')
        return c.fetchone()[0]
    except:
        return 0

def create_robust_session():
    """Create a requests session with retry logic for network errors"""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    
    # Aggressive retry strategy for connection errors
    retry_strategy = Retry(
        total=8,  # More retries
        backoff_factor=2,  # Wait 2, 4, 8, 16, 32... seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
        raise_on_status=False,
        connect=5,  # Retry on connection errors
        read=5,     # Retry on read errors
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=5,   # Fewer connections to avoid issues
        pool_maxsize=10,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def delete_all_sessions_for_email(email):
    """Delete all session files for a given email hash (strict refresh)"""
    email_hash = get_email_hash(email)
    for fname in os.listdir(SESSIONS_DIR):
        if email_hash in fname:
            try:
                os.remove(os.path.join(SESSIONS_DIR, fname))
                log_to_file(f"[Session] Deleted old session file: {fname}")
            except Exception as e:
                log_to_file(f"[Session] Failed to delete {fname}: {e}")

def login_and_get_session(email=None, password=None, username=None, force_new=False):
    """Login and create new session - always fresh if force_new is True"""
    print(f"\n[🔄] STARTING LOGIN for {username or email}...")
    log_to_file(f"STARTING LOGIN for {username or email}")
    if not email or not password:
        log_to_file("No credentials provided")
        return False, "No credentials provided"

    session_file = get_session_file_by_email(email)
    if force_new:
        delete_all_sessions_for_email(email)
    elif os.path.exists(session_file):
        try:
            with open(session_file, 'r') as f:
                session_data = json.load(f)
            # Verify if session is still valid
            cookies = session_data.get('cookies', {})
            ua = session_data.get('ua', DEFAULT_UA)
            s = create_robust_session()
            for k, v in cookies.items():
                s.cookies.set(k, v)
            headers = {"User-Agent": ua}
            resp = s.get(DASHBOARD_URL, headers=headers, timeout=15, allow_redirects=False)
            if resp.status_code == 200:
                print(f"[♻️] Reusing existing session for {email}")
                log_to_file(f"Reusing existing session for {email}")
                return True, "Session reused"
            else:
                log_to_file(f"Existing session invalid (Status {resp.status_code}), logging in...")
        except Exception as e:
            log_to_file(f"Error checking existing session: {e}")

    max_login_retries = 5  # Increased to 5 attempts
    for attempt in range(max_login_retries):
        s = None
        try:
            s = create_robust_session()
            ua = DEFAULT_UA
            headers = {
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive"
            }
            
            log_to_file(f"Fetching Login Page... (attempt {attempt + 1}/{max_login_retries})")
            resp = s.get(LOGIN_URL, headers=headers, timeout=30)
            
            if resp.status_code != 200:
                raise Exception(f"Failed to load login page. Status: {resp.status_code}")
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            login_token = soup.find('input', {'name': '_token'})
            if not login_token: 
                raise Exception("No _token found on login page")
            login_token = login_token['value']

            print("[2] Sending Credentials...")
            
            post_headers = {
                "User-Agent": ua,
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": BASE_URL,
                "Referer": LOGIN_URL,
                "Upgrade-Insecure-Requests": "1",
                "Accept-Encoding": "gzip, deflate"
            }
            
            data = {
                "_token": login_token,
                "email": email,
                "password": password,
                "remember": "on",
                "g-recaptcha-response": "" 
            }
            
            login_resp = s.post(LOGIN_URL, data=data, headers=post_headers, allow_redirects=False)
            
            if login_resp.status_code in [302, 301]:
                s.get(DASHBOARD_URL, headers=headers)
            
            has_session = any('session' in name.lower() for name in s.cookies.keys())
            if not has_session:
                raise Exception("Login Failed: No session cookie")
            
            log_to_file("Login Success!")
            
            log_to_file("Fetching Dashboard...")
            dash_resp = s.get(DASHBOARD_URL, headers=headers, timeout=30)
            
            # Also fetch SMS page to get the correct CSRF token for OTP monitoring
            log_to_file("Fetching SMS Page for CSRF token...")
            sms_resp = s.get(SMS_PAGE_URL, headers=headers, timeout=30)
            
            soup = BeautifulSoup(sms_resp.text, 'html.parser')
            meta = soup.find('meta', {'name': 'csrf-token'})
            final_token = meta['content'] if meta else login_token
            
            if not meta:
                # Fallback to dashboard token
                soup = BeautifulSoup(dash_resp.text, 'html.parser')
                meta = soup.find('meta', {'name': 'csrf-token'})
                final_token = meta['content'] if meta else login_token
            
            final_cookies = s.cookies.get_dict()
            
            # Save session using email-based filename (shared across all usernames with same email)
            session_file = get_session_file_by_email(email)
            session_data = {"cookies": final_cookies, "ua": ua, "token": final_token, "time": time.time(), "email": email}
            try:
                with open(session_file, 'w') as f:
                    json.dump(session_data, f)
                log_to_file(f"Session saved for {email} (shared)")
            except Exception as se:
                log_to_file(f"Failed to save session: {se}")
            
            log_to_file("Login Complete!")
            return True, "Login successful"

        except (ConnectionResetError, ConnectionAbortedError, 
                requests.exceptions.ConnectionError, 
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                OSError) as e:
            # Clean up any stale connection
            if s:
                try:
                    s.close()
                except:
                    pass
            
            err_msg = str(e)
            # Make error message cleaner
            if '10054' in err_msg or 'forcibly closed' in err_msg.lower():
                err_msg = "Connection reset by server"
            elif 'connection aborted' in err_msg.lower():
                err_msg = "Connection aborted"
            elif 'timeout' in err_msg.lower():
                err_msg = "Connection timeout"
            
            print(f"[⚠️] Network error (attempt {attempt + 1}/{max_login_retries}): {err_msg}")
            
            if attempt < max_login_retries - 1:
                wait_time = (attempt + 1) * 3  # 3, 6, 9, 12, 16 seconds
                print(f"[🔄] Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            return False, f"Network unstable. Try again in a few seconds."
        
        except Exception as e:
            if s:
                try:
                    s.close()
                except:
                    pass
            
            print(f"[❌] Login Error: {e}")
            traceback.print_exc()
            if attempt < max_login_retries - 1:
                time.sleep(3)
                continue
            return False, str(e)
    
    return False, "Login failed after all retries"

def check_session_health(username=None):
    """Check if session is still valid - with network retry"""
    session = load_session(username)
    if not session:
        return False, "No session found"
    
    max_retries = 5
    for attempt in range(max_retries):
        s = None
        try:
            s = create_robust_session()
            s.cookies.update(session['cookies'])
            
            headers = {
                "User-Agent": session['ua'],
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive"
            }
            
            resp = s.get(SMS_PAGE_URL, headers=headers, timeout=45)
            html = resp.text
            
            if s:
                s.close()
            
            if resp.status_code == 419:
                return False, "CSRF Token Expired"
            if resp.status_code == 403:
                return False, "Access Forbidden"
            if "Login" in html and "Sign in" in html:
                return False, "Session Expired"
            if "Received" in html or "Dashboard" in html or "portal" in html.lower():
                return True, "Session Active"
            return False, "Unknown state"
            
        except (ConnectionResetError, ConnectionAbortedError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout,
                OSError) as e:
            if s:
                try:
                    s.close()
                except:
                    pass
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 2)  # 2, 4, 6, 8 seconds
                continue
            return False, "Network unstable"
        except Exception as e:
            if s:
                try:
                    s.close()
                except:
                    pass
            return False, str(e)
    
    return False, "Health check failed"

def ensure_valid_session(username=None, email=None, password=None):
    """Ensure we have a valid session, login if needed"""
    is_valid, msg = check_session_health(username)
    if not is_valid:
        print(f"[!] Session invalid ({msg}), logging in...")
        return login_and_get_session(email, password, username)
    return True, msg

# ==========================================
#           HELPER FUNCTIONS
# ==========================================

def generate_otp_hash(phone_number, message, service, username=None):
    """Generate unique hash for an OTP"""
    if username:
        unique_str = f"{phone_number}|{message}|{service}|{username}"
    else:
        unique_str = f"{phone_number}|{message}|{service}"
    return hashlib.md5(unique_str.encode()).hexdigest()

def extract_otp_from_message(message):
    if not message or not isinstance(message, str):
        return None
    patterns = [
        # WhatsApp style: "code: 751-399" or "code: 751 399"
        r'(?:code|otp|pin|password|passcode)[:\s]+(\d{3}[-\s]?\d{3,4})',
        # Code with hyphen/space: 751-399, 1234-5678
        r'\b(\d{3}[-\s]\d{3})\b',
        r'\b(\d{4}[-\s]\d{4})\b',
        # "code is XXXXXX" or "code: XXXXXX"
        r'(?:code|otp|pin)[:\s]+is[:\s]*(\d{4,8})',
        r'(?:code|otp|pin|password|passcode)[:\s]+([A-Za-z0-9]{4,8})',
        # Alphanumeric codes like AB1234
        r'\b([A-Z]{2,3}[0-9]{4,6})\b',
        # Plain digits (most specific lengths first)
        r'\b(\d{8})\b',
        r'\b(\d{7})\b', 
        r'\b(\d{6})\b',
        r'\b(\d{5})\b',
        r'\b(\d{4})\b',
    ]
    for pattern in patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            code = match.group(1)
            # Skip if it's a common word
            if code.lower() in ['with', 'this', 'your', 'code', 'from', 'that', 'have', 'been', 'will']:
                continue
            # Skip year-like 4-digit numbers
            if code.isdigit() and len(code) == 4 and (code.startswith('19') or code.startswith('20')):
                continue
            # Skip very long pure digit strings (likely phone numbers)
            if code.isdigit() and len(code) >= 10:
                continue
            return code
    return None

def format_otp_message(phone_number, range_name, service, message, otp, timestamp, username=None):
    """Format OTP message for Telegram"""
    flag = get_country_flag(range_name)
    country = get_country_name(range_name)
    
    if len(phone_number) > 8:
        masked = f"+{phone_number[:3]}••••{phone_number[-4:]}"
    else:
        masked = phone_number
    
    otp_display = f"『 {otp} 』" if otp else "━━━"
    acc_line = f"│ 👤  Account: {username}" if username else ""
    
    formatted = f"""┏━━━━━━━━━━━━━━━━━━━━━━┓
┃  {flag} 𝗡𝗘𝗪 𝗢𝗧𝗣 𝗥𝗘𝗖𝗘𝗜𝗩𝗘𝗗  {flag}
┗━━━━━━━━━━━━━━━━━━━━━━┛

🔐 𝗢𝗧𝗣 𝗖𝗼𝗱𝗲: {otp_display}

╭─────────────────────
│ 🌐  {country}
│ 📱  {masked}
│ 🏷️  {service}
│ ⏰  {timestamp}
{acc_line}
╰─────────────────────

💬 𝗠𝗲𝘀𝘀𝗮𝗴𝗲:
┌────────────────────┐
  {message}
└────────────────────┘"""
    
    return formatted

# ==========================================
#           ADD NUMBER FUNCTIONS
# ==========================================

def get_account_name(s, ua, username=None):
    """Extract account name from dashboard"""
    try:
        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Encoding": "gzip, deflate"
        }
        resp = s.get(DASHBOARD_URL, headers=headers, timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            for selector in ['.dropdown-toggle', '.user-name', '.navbar-text', 'span.d-none']:
                elem = soup.select_one(selector)
                if elem and elem.get_text(strip=True):
                    name = elem.get_text(strip=True)
                    if name and len(name) > 2 and '@' not in name:
                        return name
    except:
        pass
    # Return username as fallback instead of EMAIL
    return username or "Unknown"

def add_number_and_get(termination_id, username=None):
    """Add termination and fetch numbers - returns (numbers, status, range_name, account_name)"""
    session_data = load_session(username)
    if not session_data:
        return None, "No session found", None, username
    
    cookies = session_data['cookies']
    ua = session_data['ua']
    token = session_data['token']
    
    s = create_robust_session()
    s.cookies.update(cookies)
    
    account_name = get_account_name(s, ua, username)
    
    headers = {
        "User-Agent": ua,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRF-TOKEN": token,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": BASE_URL,
        "Referer": DASHBOARD_URL,
        "Accept-Encoding": "gzip, deflate"
    }
    
    payload = f"_token={token}&id={termination_id}"
    range_name = None
    
    try:
        resp = s.post(ADD_URL, data=payload, headers=headers, timeout=15)
        print(f"[ADDNUM] Add: {resp.status_code}")
        print(f"[ADDNUM] Add response: {resp.text[:200]}")
        
        if resp.status_code == 200:
            # Extract range name from response like: {"message":"done add number from termination [UKRAINE 15764]"}
            try:
                add_data = resp.json()
                msg = add_data.get("message", "")
                # Extract text between [ and ]
                import re
                match = re.search(r'\[([^\]]+)\]', msg)
                if match:
                    range_name = match.group(1)
                    print(f"[ADDNUM] Extracted range: {range_name}")
            except:
                pass
        
        if resp.status_code == 400 and "maximum" in resp.text.lower():
            print(f"[ADDNUM] Already in panel, fetching numbers...")
        elif resp.status_code in [419, 401, 302]:
            return None, "Session expired", None, account_name
        elif resp.status_code != 200:
            return None, f"Add failed: {resp.status_code}", None, account_name
    except Exception as e:
        print(f"[ADDNUM] Add error: {e}")
        return None, f"Add error: {e}", None, account_name
    
    headers["Referer"] = f"{BASE_URL}/portal/live/my_sms"
    payload = f"termination_id={termination_id}&_token={token}"
    
    try:
        resp = s.post(GET_NUMBERS_URL, data=payload, headers=headers, timeout=15)
        print(f"[ADDNUM] Fetch: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            numbers = [str(item.get("Number")) for item in data if "Number" in item]
            # range_name already extracted from add response, keep it
            print(f"[ADDNUM] ✅ Got {len(numbers)} numbers, Range: {range_name}")
            return numbers, "Success", range_name, account_name
    except Exception as e:
        print(f"[ADDNUM] Fetch error: {e}")
        return [], f"Fetch error: {e}", None, account_name
    
    return [], f"Fetch failed: {resp.status_code}", None, account_name

def get_numbers_only(termination_id, username=None):
    """Get numbers for a termination WITHOUT adding - returns (numbers, status, account_name)"""
    session_data = load_session(username)
    if not session_data:
        return None, "No session found", None
    
    cookies = session_data['cookies']
    ua = session_data['ua']
    token = session_data['token']
    
    s = create_robust_session()
    s.cookies.update(cookies)
    
    # Get account name
    account_name = get_account_name(s, ua, username)
    
    headers = {
        "User-Agent": ua,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRF-TOKEN": token,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/portal/live/my_sms",
        "Accept-Encoding": "gzip, deflate"
    }
    
    payload = f"termination_id={termination_id}&_token={token}"
    
    try:
        resp = s.post(GET_NUMBERS_URL, data=payload, headers=headers, timeout=15)
        print(f"[GETNUM] Fetch: {resp.status_code}")
        
        if resp.status_code in [419, 401, 302]:
            return None, "Session expired", account_name
        
        if resp.status_code == 200:
            data = resp.json()
            # Check if we got a list of numbers
            if isinstance(data, list) and len(data) > 0 and "Number" in data[0]:
                numbers = [str(item.get("Number")) for item in data if "Number" in item]
                print(f"[GETNUM] ✅ Got {len(numbers)} numbers")
                return numbers, "Success", account_name
            else:
                # No numbers - range not added
                return [], "not_added", account_name
    except Exception as e:
        print(f"[GETNUM] Fetch error: {e}")
        return None, f"Fetch error: {e}", account_name
    
    return [], f"Fetch failed: {resp.status_code}", account_name

# ==========================================
#           DELETE ALL FUNCTIONS
# ==========================================

def sync_fetch_all_number_ids(s, ua, token):
    """Fetch all number IDs using sync requests"""
    request_counter = int(time.time() * 1000)
    
    params = (
        f"draw=2&columns%5B0%5D%5Bdata%5D=number_id&columns%5B0%5D%5Bname%5D=id"
        f"&columns%5B0%5D%5Borderable%5D=false&columns%5B1%5D%5Bdata%5D=Number"
        f"&columns%5B2%5D%5Bdata%5D=range&columns%5B3%5D%5Bdata%5D=A2P"
        f"&columns%5B4%5D%5Bdata%5D=P2P&columns%5B5%5D%5Bdata%5D=LimitA2P"
        f"&order%5B0%5D%5Bcolumn%5D=1&order%5B0%5D%5Bdir%5D=desc"
        f"&start=0&length=1000&search%5Bvalue%5D=&_={request_counter}"
    )
    
    headers = {
        "User-Agent": ua,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRF-TOKEN": token,
        "Referer": NUMBERS_PAGE_URL,
        "Accept-Encoding": "gzip, deflate"
    }
    
    try:
        url = f"{NUMBERS_PAGE_URL}?{params}"
        resp = s.get(url, headers=headers, timeout=30)
        
        if resp.status_code != 200:
            return [], 0
        
        data = resp.json()
        total = data.get("recordsTotal", 0)
        records = data.get("data", [])
        
        number_ids = []
        for record in records:
            html = record.get("number_id", "")
            match = re.search(r'value="(\d+)"', html)
            if match:
                number_ids.append(match.group(1))
        
        return number_ids, total
    except Exception as e:
        print(f"[DEL] Fetch error: {e}")
        return [], 0

def sync_bulk_remove(s, ua, token, number_ids):
    """Remove numbers in bulk"""
    if not number_ids:
        return 0
    
    headers = {
        "User-Agent": ua,
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRF-TOKEN": token,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": BASE_URL,
        "Referer": NUMBERS_PAGE_URL,
        "Accept-Encoding": "gzip, deflate"
    }
    
    payload = "&".join([f"NumberID%5B%5D={nid}" for nid in number_ids])
    
    try:
        resp = s.post(BULK_REMOVE_URL, data=payload, headers=headers, timeout=30)
        if resp.status_code == 200:
            try:
                result = resp.json()
            except Exception as e:
                print(f"[DEL] JSON decode error: {e}")
                return 0
            if isinstance(result, dict):
                removed_str = result.get("NumberDoneRemove", "")
                removed_list = [n.strip() for n in removed_str.split(",") if n.strip()]
                return len(removed_list)
            else:
                print(f"[DEL] Unexpected result type: {type(result)}")
                return 0
    except Exception as e:
        print(f"[DEL] Remove error: {e}")
    return 0

async def delete_all_numbers_async(user_id, progress_callback=None, username=None):
    """Delete all numbers from panel"""
    cancel_flags[user_id] = False
    
    session_data = load_session(username)
    if not session_data:
        return 0, 0, "No session", username
    
    cookies = session_data['cookies']
    ua = session_data['ua']
    token = session_data['token']
    
    s = create_robust_session()
    s.cookies.update(cookies)
    
    total_removed = 0
    delete_attempts = 0  # Only count actual delete operations
    max_iterations = 100
    
    loop = asyncio.get_event_loop()
    
    for _ in range(max_iterations):
        if cancel_flags.get(user_id, False):
            return total_removed, delete_attempts, "Cancelled", username
        
        # Fetch all numbers
        number_ids, total = await loop.run_in_executor(
            thread_pool, 
            lambda: sync_fetch_all_number_ids(s, ua, token)
        )
        
        # No numbers left - we're done
        if total == 0:
            if progress_callback:
                await progress_callback(-1, 0, total_removed)  # -1 = verification complete
            break
        
        if not number_ids:
            await asyncio.sleep(0.3)
            continue
        
        # Count this as a delete attempt (actual deletion happening)
        delete_attempts += 1
        
        if progress_callback:
            await progress_callback(delete_attempts, len(number_ids), total_removed)
        
        # Delete all fetched numbers
        removed = await loop.run_in_executor(
            thread_pool,
            lambda ids=number_ids: sync_bulk_remove(s, ua, token, ids)
        )
        total_removed += removed
        
        # Small delay before verification check
        await asyncio.sleep(0.5)
    
    return total_removed, delete_attempts, "Complete", username

# ==========================================
#           GET STATS FUNCTIONS
# ==========================================

def get_stats(date_str=None, username=None):
    """Get SMS statistics from portal"""
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    session_data = load_session(username)
    if not session_data:
        return None, "No session"
    
    cookies = session_data['cookies']
    ua = session_data['ua']
    token = session_data['token']
    
    s = create_robust_session()
    s.cookies.update(cookies)
    
    # Construct multipart body manually to match browser exactly
    boundary = f"----WebKitFormBoundary{int(time.time()*1000)}"
    
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="from"\r\n\r\n'
        f"{date_str}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="to"\r\n\r\n'
        f"{date_str}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="_token"\r\n\r\n'
        f"{token}\r\n"
        f"--{boundary}--\r\n"
    )
    
    headers = {
        "User-Agent": ua,
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
        "Origin": BASE_URL,
        "Referer": SMS_PAGE_URL,
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate"
    }
    
    try:
        resp = s.post(GET_SMS_URL, data=body.encode('utf-8'), headers=headers, timeout=30)
        
        if resp.status_code != 200:
            return None, f"Request failed: {resp.status_code}"
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        ranges = []
        
        # Parse cards
        cards = soup.find_all('div', class_='card')
        for card in cards:
            onclick = card.get('onclick', '')
            if isinstance(onclick, str) and 'getDetials' in onclick:
                match = re.search(r"getDetials\('([^']+)'\)", onclick)
                if match:
                    range_name = match.group(1)
                    
                    # Find stats in p tags
                    # Structure: <p class="mb-0 pb-0">VALUE</p>
                    p_tags = card.find_all('p')
                    stat_ps = []
                    for p in p_tags:
                        p_class = p.get('class', [])
                        if not isinstance(p_class, list):
                            continue
                        if 'mb-0' in p_class:
                            stat_ps.append(p)
                    
                    if len(stat_ps) >= 4:
                        count = stat_ps[0].get_text(strip=True)
                        paid = stat_ps[1].get_text(strip=True)
                        unpaid = stat_ps[2].get_text(strip=True)
                        revenue = stat_ps[3].get_text(strip=True)
                        
                        ranges.append({
                            "range": range_name,
                            "count": count,
                            "paid": paid,
                            "unpaid": unpaid,
                            "revenue": revenue
                        })
        
        return ranges, "Success"
        
    except Exception as e:
        print(f"Error in get_stats: {e}")
        return None, f"Error: {str(e)}"

def format_stats_message(ranges, date_str):
    """Format stats for Telegram"""
    total_count = 0
    total_paid = 0
    total_revenue = 0.0
    
    for r in ranges:
        try:
            total_count += int(r['count']) if r['count'] else 0
            total_paid += int(r['paid']) if r['paid'] else 0
            total_revenue += float(r['revenue']) if r['revenue'] else 0.0
        except:
            pass
    
    lines = []
    lines.append(f"📊 <b>SMS Statistics</b>")
    lines.append(f"📅 Date: <code>{date_str}</code>")
    lines.append(f"💰 Revenue: <b>${total_revenue:.3f}</b>")
    lines.append("")
    
    if ranges:
        lines.append("<pre>")
        lines.append(f"{'Range':<18} {'Cnt':>5} {'Paid':>5} {'Rev':>8}")
        lines.append("─" * 40)
        
        for r in ranges:
            rn = r['range'][:17]
            lines.append(f"{rn:<18} {r['count']:>5} {r['paid']:>5} {r['revenue']:>8}")
        
        lines.append("─" * 40)
        lines.append(f"{'TOTAL':<18} {total_count:>5} {total_paid:>5} {total_revenue:>8.3f}")
        lines.append("</pre>")
    else:
        lines.append("No data for this date.")
    
    return "\n".join(lines)

# ==========================================
#           OTP MONITORING (Multi-Account)
# ==========================================

aio_session = None
last_session_reset = 0

def _create_new_session():
    """Create a new aiohttp session - FAST timeouts"""
    timeout = aiohttp.ClientTimeout(
        total=10,       # Increased slightly for stability
        connect=5,     
        sock_read=5,   
        sock_connect=5
    )
    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=50,
        ttl_dns_cache=300,
        force_close=True, # Force close to avoid WinError 64 on stale connections
        ssl=False,
        enable_cleanup_closed=True
    )
    return aiohttp.ClientSession(timeout=timeout, connector=connector)

async def get_aio_session():
    """Get or create aiohttp session - simple and fast"""
    global aio_session, last_session_reset
    
    # Create session if none exists
    if aio_session is None or aio_session.closed:
        aio_session = _create_new_session()
        last_session_reset = time.time()
        print(f"[OTP] 🔗 HTTP session created")
        
    return aio_session

async def reset_aio_session():
    """Reset session on errors"""
    global aio_session, last_session_reset
    old = aio_session
    aio_session = _create_new_session()
    last_session_reset = time.time()
    if old and not old.closed:
        try:
            await old.close()
        except:
            pass
    print(f"[OTP] 🔄 HTTP session reset")

# Track last refresh time to prevent refresh loops
last_session_refresh_time = {}  # username -> timestamp
SESSION_REFRESH_COOLDOWN = 120  # Don't refresh same account within 120 seconds

async def async_fetch_ranges(cookies, ua, token, date_str, username):
    """Fetch SMS ranges - SIMPLE and FAST
    Returns: list of ranges (empty [] is valid), or None on 419 only
    """
    for attempt in range(3):
        try:
            session = await get_aio_session()
            
            boundary = f"----WebKitFormBoundary{int(time.time()*1000)}"
            
            body = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="from"\r\n\r\n'
                f"{date_str}\r\n"
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="to"\r\n\r\n'
                f"{date_str}\r\n"
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="_token"\r\n\r\n'
                f"{token}\r\n"
                f"--{boundary}--\r\n"
            )
            
            cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
            
            headers = {
                "User-Agent": ua,
                "Accept": "text/html, */*; q=0.01",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": BASE_URL,
                "Referer": SMS_PAGE_URL,
                "Cookie": cookie_str
            }
            
            async with session.post(GET_SMS_URL, data=body.encode('utf-8'), headers=headers) as resp:
                if resp.status == 419:
                    return None  # CSRF expired - needs refresh
                if resp.status != 200:
                    return []
                
                text = await resp.text()
                
                # Check for login redirect
                if 'login' in text.lower() and 'password' in text.lower() and len(text) > 5000:
                    return None
                
                # Parse ranges using BeautifulSoup for precision
                soup = BeautifulSoup(text, 'html.parser')
                ranges = []
                cards = soup.find_all('div', class_='card')
                for card in cards:
                    onclick = card.get('onclick', '')
                    if isinstance(onclick, str) and 'getDetials' in onclick:
                        match = re.search(r"getDetials\('([^']+)'\)", onclick)
                        if match:
                            ranges.append(match.group(1))
                
                return list(set(ranges))
                
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            if attempt < 2:
                await asyncio.sleep(0.2)
                continue
            print(f"[OTP] ⚠️ Fetch ranges error for {username}: {e}")
            return []  # Network error - just return empty, try next cycle
        except Exception as e:
            print(f"[OTP] ⚠️ Fetch ranges error for {username}: {e}")
            return []

async def async_fetch_numbers(cookies, ua, token, date_str, range_name, username):
    """Fetch phone numbers for a range - SIMPLE and FAST"""
    for attempt in range(3):
        try:
            session = await get_aio_session()
            cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
            
            headers = {
                "User-Agent": ua,
                "Accept": "text/html, */*; q=0.01",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": BASE_URL,
                "Referer": SMS_PAGE_URL,
                "Cookie": cookie_str
            }
            
            payload = f"_token={token}&start={date_str}&end={date_str}&range={quote_plus(range_name)}"
            
            async with session.post(GET_SMS_NUMBER_URL, data=payload, headers=headers) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
                
                # Use BeautifulSoup for robust parsing
                soup = BeautifulSoup(text, 'html.parser')
                phones = set()
                
                # Method 1: Look for onclick="getDetialsNumber('PHONE')"
                elements_with_onclick = soup.find_all(attrs={"onclick": re.compile(r"getDetialsNumber")})
                for el in elements_with_onclick:
                    onclick = el.get('onclick', '')
                    match = re.search(r"getDetialsNumber[^']*\('(\d{10,15})'", onclick)
                    if match:
                        phones.add(match.group(1))
                
                # Method 2: Look for divs containing numbers (fallback)
                if not phones:
                     matches = re.findall(r"getDetialsNumber[^']*\('(\d{10,15})'", text)
                     phones.update(matches)
                
                return [(phone, range_name, username) for phone in phones]
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            if attempt < 2:
                await asyncio.sleep(0.2)
                continue
            print(f"[OTP] ⚠️ Fetch numbers error for {username}: {e}")
            return []
        except Exception as e:
            print(f"[OTP] ⚠️ Fetch numbers error for {username}: {e}")
            return []

async def async_fetch_sms(cookies, ua, token, date_str, phone, range_name, username):
    """Fetch SMS for a phone number - SIMPLE and FAST"""
    for attempt in range(3):
        try:
            session = await get_aio_session()
            cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
            
            headers = {
                "User-Agent": ua,
                "Accept": "text/html, */*; q=0.01",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": BASE_URL,
                "Referer": SMS_PAGE_URL,
                "Cookie": cookie_str
            }
            
            payload = f"_token={token}&start={date_str}&end={date_str}&Number={phone}&Range={quote_plus(range_name)}"
            
            async with session.post(GET_SMS_CONTENT_URL, data=payload, headers=headers) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
                soup = BeautifulSoup(text, 'html.parser')
                messages = []
                
                for card in soup.find_all('div', class_='card'):
                    # Ensure it's a message card
                    cols = card.find_all('div', class_=re.compile(r'^col'))
                    if not cols:
                        continue
                        
                    service = "Unknown"
                    message = ""
                    
                    if len(cols) >= 1:
                        service_text = cols[0].get_text(strip=True)
                        service = re.sub(r'^CLI\s*', '', service_text).strip()
                    
                    if len(cols) >= 2:
                        p_tag = cols[1].find('p', class_='mb-0')
                        if p_tag:
                            message = p_tag.get_text(strip=True)
                        else:
                            message = cols[1].get_text(strip=True)
                            message = re.sub(r'Message Content\s*', '', message).strip()
                    
                    if message and len(message) > 2:
                        messages.append({
                            "phone": phone, 
                            "range": range_name, 
                            "service": service or "Unknown", 
                            "message": message,
                            "username": username
                        })
                
                return messages
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            if attempt < 2:
                await asyncio.sleep(0.2)
                continue
            return []
        except:
            return []

async def refresh_account_session(username, force_new=False):
    """Refresh session for an account (strict if force_new)"""
    accounts = load_accounts()
    if username not in accounts:
        log_to_file(f"[Refresh] {username}: NOT FOUND in ivas_accounts.json")
        log_to_file(f"[Refresh] Available accounts: {list(accounts.keys())}")
        return False
    acc = accounts[username]
    log_to_file(f"[Refresh] {username}: Starting login with email {acc['email']}")
    success, msg = login_and_get_session(acc['email'], acc['password'], username, force_new=force_new)
    log_to_file(f"[Refresh] {username}: Result={success}, Message={msg}")
    return success


# --- Monitored Accounts Persistence ---
OTP_MONITOR_ACCOUNTS_FILE = os.path.join(SESSIONS_DIR, "otp_monitor_accounts.json")

def save_monitored_accounts():
    try:
        with open(OTP_MONITOR_ACCOUNTS_FILE, 'w') as f:
            json.dump(sorted(list(otp_monitor_accounts)), f)
    except Exception as e:
        log_to_file(f"[OTP] Failed to save monitored accounts: {e}")

def load_monitored_accounts():
    if os.path.exists(OTP_MONITOR_ACCOUNTS_FILE):
        try:
            with open(OTP_MONITOR_ACCOUNTS_FILE, 'r') as f:
                data = json.load(f)
            if isinstance(data, list):
                return set(data)
        except Exception as e:
            log_to_file(f"[OTP] Failed to load monitored accounts: {e}")
    return set()

# Global to track which accounts to monitor
otp_monitor_accounts = load_monitored_accounts()  # Empty = monitor all, otherwise only these usernames
sent_otps_cache = {} # (owner_id, content_hash) -> timestamp

async def otp_monitoring_loop():
    """OTP monitoring - SIMPLE: ranges → numbers → SMS, every 2 seconds"""
    global otp_monitoring_active, telegram_app, failed_users, otp_monitor_accounts, sent_otps_cache
    
    # Group accounts by email to show shared email info
    all_accs = load_accounts()
    email_groups = {}
    for uname, acc_data in all_accs.items():
        em = acc_data.get('email', '')
        if em not in email_groups:
            email_groups[em] = []
        email_groups[em].append(uname)
    
    # Show monitoring info with email grouping
    acc_info = ", ".join(otp_monitor_accounts) if otp_monitor_accounts else "ALL"
    print(f"[OTP] 🚀 Monitoring started for: {acc_info}")
    log_to_file(f"[OTP] Monitoring started for: {acc_info}")
    save_monitored_accounts()
    
    # Show email groupings
    for em, usernames in email_groups.items():
        if len(usernames) > 1:
            msg = f"[OTP] 📧 Shared email {em}: {'/'.join(usernames)}"
            print(msg)
            log_to_file(msg)
    
    session_refresh_needed = set()
    
    while otp_monitoring_active:
        start = time.time()
        new_count = 0
        total_ranges = 0
        total_phones = 0
        account_stats = {}
        
        # Clean up old cache entries (older than 5 minutes)
        current_time = time.time()
        sent_otps_cache = {k: v for k, v in sent_otps_cache.items() if current_time - v < 300}
        
        try:
            all_accounts = load_accounts()
            date_str = datetime.now().strftime("%Y-%m-%d")
            
            # Filter accounts if specific ones are set - process ALL monitored accounts
            if otp_monitor_accounts:
                # Get ALL monitored accounts that exist
                accounts = {k: v for k, v in all_accounts.items() if k in otp_monitor_accounts}
                
                # Check for non-existent accounts
                non_existent = otp_monitor_accounts - set(all_accounts.keys())
                if non_existent:
                    print(f"[OTP] ⚠️ Accounts not found in file: {non_existent}")
                    print(f"[OTP] ⚠️ Available accounts: {list(all_accounts.keys())}")
                    # Don't remove them immediately - might be added later
            else:
                # Monitor ALL accounts
                accounts = all_accounts
            
            if not accounts:
                msg = "[OTP] No accounts to monitor (check ivas_accounts.json)"
                print(msg)
                log_to_file(msg)
                await asyncio.sleep(2)
                continue
            
            # Track which accounts we're supposed to monitor vs actually processing
            expected_accounts = set(accounts.keys())
            processed_accounts = set()
            skipped_accounts = {}
            
            # Refresh sessions that need it FIRST (with cooldown to prevent loops)
            if session_refresh_needed:
                current_time = time.time()
                for username in list(session_refresh_needed):
                    # Check cooldown - don't refresh if we just refreshed
                    last_refresh = last_session_refresh_time.get(username, 0)
                    if current_time - last_refresh < SESSION_REFRESH_COOLDOWN:
                        log_to_file(f"[OTP] {username}: skipping refresh (cooldown {int(SESSION_REFRESH_COOLDOWN - (current_time - last_refresh))}s)")
                        session_refresh_needed.discard(username)
                        continue
                    
                    log_to_file(f"[OTP] Refreshing session for {username}...")
                    if await refresh_account_session(username):
                        session_refresh_needed.discard(username)
                        last_session_refresh_time[username] = current_time
                        print(f"[OTP] ✅ Session refreshed for {username}")
                        log_to_file(f"[OTP] Session refreshed for {username}")
                        
                        # Verify session was actually saved
                        session_data = load_session(username)
                        if session_data:
                            log_to_file(f"[OTP] {username}: Verified session exists with cookies: {len(session_data.get('cookies', {}))} items")
                        else:
                            log_to_file(f"[OTP] ⚠️ {username}: Session refresh succeeded but session file not found!")
                            print(f"[OTP] ⚠️ {username}: Session not loading after refresh!")
                    else:
                        log_to_file(f"[OTP] Session refresh failed for {username}")
                        session_refresh_needed.discard(username)  # Don't keep retrying
                        last_session_refresh_time[username] = current_time
                await asyncio.sleep(1)
                continue
            
            # ========== STEP 1: Fetch ALL ranges from ALL accounts in PARALLEL ==========
            range_tasks = []
            range_task_usernames = []
            
            log_to_file(f"[OTP] Querying {len(accounts)} accounts: {', '.join(accounts.keys())}")
            
            for username, acc in accounts.items():
                session_data = load_session(username)
                if not session_data:
                    # Session missing or unreadable - queue refresh
                    print(f"[OTP] ❌ {username}: NO SESSION DATA FOUND")
                    log_to_file(f"[OTP] {username}: No session data, queuing refresh")
                    log_to_file(f"[OTP] {username}: Account exists? {username in accounts}, Email: {acc.get('email', 'NONE')}")
                    session_refresh_needed.add(username)
                    account_stats[username] = (0, 0)
                    skipped_accounts[username] = "No session"
                    continue
                
                log_to_file(f"[OTP] {username}: Sending range request...")
                range_tasks.append(
                    async_fetch_ranges(
                        session_data['cookies'],
                        session_data['ua'],
                        session_data['token'],
                        date_str,
                        username
                    )
                )
                range_task_usernames.append(username)
            
            # Execute ALL range fetches in parallel
            if range_tasks:
                log_to_file(f"[OTP] Sending {len(range_tasks)} range requests...")
            range_results = await asyncio.gather(*range_tasks, return_exceptions=True)
            
            # Process range results and prepare number fetch tasks
            account_ranges_map = {}  # username -> list of ranges
            current_time = time.time()
            
            for idx, result in enumerate(range_results):
                username = range_task_usernames[idx]
                
                if result is None:  # 419 error - may need refresh
                    # Always queue for refresh - the refresh handler checks cooldown
                    last_refresh = last_session_refresh_time.get(username, 0)
                    session_refresh_needed.add(username)  # Always queue it
                    if current_time - last_refresh >= SESSION_REFRESH_COOLDOWN:
                        log_to_file(f"[OTP] {username}: Session expired (419), queued for refresh")
                    else:
                        remaining_cooldown = int(SESSION_REFRESH_COOLDOWN - (current_time - last_refresh))
                        log_to_file(f"[OTP] {username}: Session expired, queued (cooldown: {remaining_cooldown}s remaining)")
                    account_stats[username] = (0, 0)
                    skipped_accounts[username] = "Session expired (419)"
                    continue
                
                if isinstance(result, Exception):
                    log_to_file(f"[OTP] {username}: Range fetch error - {type(result).__name__}")
                    account_stats[username] = (0, 0)
                    skipped_accounts[username] = f"Error: {type(result).__name__}"
                    continue
                    
                if not result:
                    log_to_file(f"[OTP] {username}: No ranges found (empty result)")
                    print(f"[OTP] ⚠️ {username}: 0 ranges returned")
                    account_stats[username] = (0, 0)
                    processed_accounts.add(username)
                    continue
                
                log_to_file(f"[OTP] {username}: Found {len(result)} ranges - {', '.join(result[:3])}{'...' if len(result) > 3 else ''}")
                account_ranges_map[username] = result
                total_ranges += len(result)
                processed_accounts.add(username)
            
            # ========== STEP 2: Fetch ALL numbers from ALL ranges in PARALLEL ==========
            number_tasks = []
            number_task_info = []  # (username, range_name)
            
            for username, ranges in account_ranges_map.items():
                session_data = load_session(username)
                if not session_data:
                    log_to_file(f"[OTP] {username}: Session lost before number fetch")
                    continue
                
                log_to_file(f"[OTP] {username}: Fetching numbers from {len(ranges)} ranges...")
                for range_name in ranges:
                    number_tasks.append(
                        async_fetch_numbers(
                            session_data['cookies'],
                            session_data['ua'],
                            session_data['token'],
                            date_str,
                            range_name,
                            username
                        )
                    )
                    number_task_info.append((username, range_name))
            
            # Execute ALL number fetchs in parallel
            number_results = await asyncio.gather(*number_tasks, return_exceptions=True)
            
            # Collect all phones and track per-account stats
            all_phones = []  # List of (phone, range_name, username)
            account_phone_counts = {}  # username -> phone count
            
            for idx, result in enumerate(number_results):
                username, range_name = number_task_info[idx]
                
                if isinstance(result, list):
                    for phone_tuple in result:
                        all_phones.append(phone_tuple)
                    
                    if username not in account_phone_counts:
                        account_phone_counts[username] = 0
                    account_phone_counts[username] += len(result)
            
            # Update account stats
            for username in account_ranges_map:
                num_ranges = len(account_ranges_map.get(username, []))
                num_phones = account_phone_counts.get(username, 0)
                account_stats[username] = (num_ranges, num_phones)
            
            total_phones = len(all_phones)
            
            if not all_phones:
                elapsed = time.time() - start
                stats_str = " | ".join([f"{u}:{r}R/{n}N" for u, (r, n) in account_stats.items()])
                if not stats_str:
                    stats_str = "no accounts"
                print(f"[OTP] ⚡ {elapsed:.2f}s | {stats_str} | 0 new")
                await asyncio.sleep(2)
                continue
            
            # ========== STEP 3: Fetch ALL SMS from ALL phones in PARALLEL ==========
            sms_tasks = []
            sms_task_info = []  # (phone, range_name, username)
            
            # Count phones per account for logging
            phones_per_account = {}
            for phone, range_name, username in all_phones:
                phones_per_account[username] = phones_per_account.get(username, 0) + 1
            
            for username, count in phones_per_account.items():
                log_to_file(f"[OTP] {username}: Fetching SMS from {count} numbers...")
            
            for phone, range_name, username in all_phones:
                session_data = load_session(username)
                if session_data:
                    sms_tasks.append(
                        async_fetch_sms(
                            session_data['cookies'],
                            session_data['ua'],
                            session_data['token'],
                            date_str,
                            phone,
                            range_name,
                            username
                        )
                    )
                    sms_task_info.append((phone, range_name, username))
            
            # Execute ALL SMS fetches in parallel
            sms_results = await asyncio.gather(*sms_tasks, return_exceptions=True)
            
            # Build username -> (user_id, channels list) mapping
            account_owners = {}
            for username, acc in accounts.items():
                channels = acc.get('otp_channels', [])
                if not channels and acc.get('otp_channel_id'):
                    channels = [acc.get('otp_channel_id')]
                account_owners[username] = {
                    'user_id': acc.get('user_id'),
                    'channels': channels
                }
            
            # ========== STEP 4: Process and send OTPs ==========
            for idx, result in enumerate(sms_results):
                if not isinstance(result, list):
                    continue
                
                for msg in result:
                    phone = msg['phone']
                    range_name = msg['range']
                    service = msg['service']
                    message = msg['message']
                    msg_username = msg['username']
                    
                    if not message:
                        continue
                    
                    msg_hash = generate_otp_hash(phone, message, service, msg_username)
                    if is_otp_seen(msg_hash):
                        continue
                    
                    # NEW OTP!
                    otp = extract_otp_from_message(message)
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
                    save_otp_to_db(msg_hash, phone, service, range_name, message, otp, timestamp, msg_username)
                    
                    formatted = format_otp_message(phone, range_name, service, message, otp, timestamp, msg_username)
                    
                    # Send to channels or user DM
                    owner_info = account_owners.get(msg_username, {})
                    channels = owner_info.get('channels', [])
                    owner_id = owner_info.get('user_id')
                    
                    # Deduplication check for same owner/channels
                    content_hash = hashlib.md5(f"{phone}|{message}|{service}".encode()).hexdigest()
                    
                    if channels:
                        for channel_id in channels:
                            # Check if we sent this content to this channel recently
                            cache_key = (channel_id, content_hash)
                            if cache_key in sent_otps_cache:
                                continue
                            
                            sent_otps_cache[cache_key] = time.time()
                            asyncio.create_task(fast_send(channel_id, formatted, range_name, service, msg_username))
                            
                    elif owner_id and owner_id not in failed_users:
                        # Check if we sent this content to this owner recently
                        cache_key = (owner_id, content_hash)
                        if cache_key in sent_otps_cache:
                            continue
                            
                        sent_otps_cache[cache_key] = time.time()
                        asyncio.create_task(fast_send(owner_id, formatted, range_name, service, msg_username))
                    
                    new_count += 1
            
            elapsed = time.time() - start
            stats_str = " | ".join([f"{u}:{r}R/{n}N" for u, (r, n) in account_stats.items()])
            if not stats_str:
                stats_str = "no data"
            
            # Log detailed accounting to file
            unprocessed = expected_accounts - processed_accounts - set(skipped_accounts.keys())
            if skipped_accounts:
                skip_str = ", ".join([f"{u}({reason})" for u, reason in skipped_accounts.items()])
                log_to_file(f"[OTP] Skipped: {skip_str}")
            if unprocessed:
                log_to_file(f"[OTP] Unprocessed: {', '.join(unprocessed)}")
            
            log_to_file(f"[OTP] {elapsed:.2f}s | {stats_str} | {new_count} new | Processed: {len(processed_accounts)}/{len(expected_accounts)}")
            
            # Simple terminal output
            print(f"[OTP] ⚡ {elapsed:.2f}s | {stats_str} | {new_count} new")
            
        except asyncio.CancelledError:
            print("[OTP] Task cancelled")
            break
        except Exception as e:
            elapsed = time.time() - start
            print(f"[OTP] ❌ Error after {elapsed:.2f}s: {e}")
        
        # Fast polling - 2 seconds
        elapsed = time.time() - start
        if elapsed < 0.1:
            await asyncio.sleep(1) # Prevent tight loop
        await asyncio.sleep(2)
    
    print("[OTP] 🛑 Stopped")

async def fast_send(user_id, message, range_name, service, username):
    """Send OTP to user - with retry"""
    global telegram_app, failed_users
    if user_id in failed_users:
        return
    if telegram_app is None or not hasattr(telegram_app, 'bot') or telegram_app.bot is None:
        print(f"[⚠️] telegram_app or bot is not initialized. Cannot send message to {user_id}.")
        return
    for attempt in range(3):
        try:
            await telegram_app.bot.send_message(
                chat_id=user_id, 
                text=message, 
                read_timeout=10, 
                write_timeout=10
            )
            print(f"[✅] {username}: {range_name} {service} → {user_id}")
            return
        except Exception as e:
            err = str(e).lower()
            if 'chat not found' in err or 'blocked' in err or 'forbidden' in err:
                failed_users.add(user_id)
                print(f"[⚠️] Disabled {user_id}")
                return
            if 'retry_after' in err or 'flood' in err:
                # Rate limited, wait and retry
                await asyncio.sleep(5)
                continue
            if attempt < 2:
                await asyncio.sleep(1)
                continue
            print(f"[⚠️] Failed to send to {user_id}: {err[:50]}")

# ==========================================
#           TELEGRAM HANDLERS
# ==========================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user_id = update.effective_user.id
    
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    owner_cmds = ""
    if is_owner(user_id):
        owner_cmds = """
<b>━━━━━ 👑 𝗢𝘄𝗻𝗲𝗿 ━━━━━</b>

  ▸ /approve <code>&lt;plan&gt; &lt;userid&gt;</code> — Approve user
  ▸ /revoke <code>&lt;userid&gt;</code> — Revoke access
  ▸ /listusers — List approved users
  ▸ /listplans — Show available plans
  ▸ /setlimit <code>&lt;id&gt; &lt;type&gt; &lt;val&gt;</code> — Set limits
  ▸ /proxy — View all proxies
  ▸ /addproxy — Replace proxy list
"""
    
    # Get user plan info
    user_plan_info = ""
    if user_id in APPROVED_USERS and user_id not in OWNERS:
        data = APPROVED_USERS[user_id]
        plan_name = data.get('plan', 'plan4')
        plan = PLANS.get(plan_name, PLANS['plan4'])
        expiry = data.get('expiry', 0)
        expiry_str = datetime.fromtimestamp(expiry).strftime('%Y-%m-%d') if expiry > time.time() else "Expired"
        
        user_plan_info = f"\n<b>📋 Plan: {plan['name']}</b> (Expires: {expiry_str})\n"

    msg = f"""╔═════════════════════════════╗
║    🔷 𝗦𝗧𝗔𝗥𝗞 𝗢𝗧𝗣𝘀 v14 🔷   ║
╚═════════════════════════════╝
{user_plan_info}
<b>━━━━━ 🔐 𝗔𝗰𝗰𝗼𝘂𝗻𝘁𝘀 ━━━━━</b>

  ▸ /addivas — Add new IVAS account
  ▸ /listivas — List your accounts
  ▸ /delivas [user] — Delete account
  ▸ /editivas [user] — Edit account
  ▸ /defaultacc [user] — Set default
  ▸ /setchannel — Manage OTP channels

<b>━━━━━ 📱 𝗡𝘂𝗺𝗯𝗲𝗿𝘀 ━━━━━</b>

  ▸ /addnum <code>&lt;id&gt;</code> [user] — Add &amp; fetch
  ▸ /getnum <code>&lt;id&gt;</code> [user] — Get only
  ▸ /delallnum [user] — Remove all

<b>━━━━━ 🔍 𝗥𝗮𝗻𝗴𝗲𝘀 ━━━━━</b>

  ▸ /getrange <code>&lt;keyword&gt;</code> — Search active ranges

<b>━━━━━ 📊 𝗦𝘁𝗮𝘁𝘀 ━━━━━</b>

  ▸ /getstats [user] [date] — Get stats

<b>━━━━━ 🔔 𝗢𝗧𝗣 ━━━━━</b>

  ▸ /startotp [user] — Start scanning
  ▸ /stopotp — Stop all scanning

<b>━━━━━ ⚙️ 𝗦𝘆𝘀𝘁𝗲𝗺 ━━━━━</b>

  ▸ /refresh [user] — Refresh session
  ▸ /status — Bot status
  ▸ /cancel — Cancel operation
{owner_cmds}
╭───────────────────────────────────╮
│  💡 <i>[user] = optional, uses default</i>
│  🔔 <i>OTPs go only to account owner</i>
│  🔄 <i>Sessions auto-refresh hourly</i>
│  🛡️ <i>Network-resilient with retries</i>
╰───────────────────────────────────╯"""
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /approve command - Owner only"""
    user_id = update.effective_user.id
    if not is_owner(user_id):
        return
    
    if not hasattr(context, 'args') or context.args is None or len(context.args) < 2:
        plans_list = "\n".join([f"• <b>{k}</b>: {v['name']}" for k, v in PLANS.items()])
        await update.message.reply_text(
            f"❌ <b>Usage:</b> /approve &lt;plan&gt; &lt;userid&gt;\n\n"
            f"<b>Available Plans:</b>\n{plans_list}",
            parse_mode=ParseMode.HTML
        )
        return
    plan_name = context.args[0].lower() if isinstance(context.args[0], str) else str(context.args[0]).lower()
    if plan_name not in PLANS:
        await update.message.reply_text(f"❌ Invalid plan. Use: {', '.join(PLANS.keys())}")
        return
        
    try:
        target_id = int(context.args[1])
    except:
        await update.message.reply_text("❌ Invalid user ID")
        return
    
    plan = PLANS[plan_name]
    expiry = time.time() + (plan['duration_days'] * 86400)
    
    APPROVED_USERS[target_id] = {
        "plan": plan_name,
        "expiry": expiry,
        "added_at": time.time(),
        "last_used": 0
    }
    save_approved_users()
    
    expiry_date = datetime.fromtimestamp(expiry).strftime('%Y-%m-%d')
    
    await update.message.reply_text(
        f"✅ <b>User Approved!</b>\n\n"
        f"👤 User: <code>{target_id}</code>\n"
        f"📋 Plan: <b>{plan['name']}</b>\n"
        f"⏳ Expires: {expiry_date}\n"
        f"📝 {plan['description']}",
        parse_mode=ParseMode.HTML
    )

async def cmd_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /revoke command - Owner only"""
    user_id = update.effective_user.id
    if not is_owner(user_id):
        return
    
    if not hasattr(context, 'args') or context.args is None or not context.args:
        await update.message.reply_text("❌ Usage: /revoke [userid]")
        return
    
    try:
        target_id = int(context.args[0])
    except:
        await update.message.reply_text("❌ Invalid user ID")
        return
    
    if target_id in OWNERS:
        await update.message.reply_text("❌ Cannot revoke owner access")
        return
    
    if target_id in APPROVED_USERS:
        del APPROVED_USERS[target_id]
        save_approved_users()
        await update.message.reply_text(f"🚫 User <code>{target_id}</code> revoked", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"ℹ️ User {target_id} not found")

async def cmd_proxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /proxy command - Owner only - Show all proxies"""
    user_id = update.effective_user.id
    if not is_owner(user_id):
        return
    
    if not PROXY_LIST:
        await update.message.reply_text("📭 No proxies configured.\n\nUse /addproxy to add proxies.")
        return
    
    # Format proxy list (hide passwords partially)
    proxy_lines = []
    for i, proxy in enumerate(PROXY_LIST, 1):
        # Parse and mask password
        try:
            if '@' in proxy:
                # Format: http://user:pass@host:port
                auth_part = proxy.split('@')[0].replace('http://', '').replace('https://', '')
                host_part = proxy.split('@')[1]
                user = auth_part.split(':')[0]
                proxy_lines.append(f"{i}. {user}:****@{host_part}")
            else:
                proxy_lines.append(f"{i}. {proxy}")
        except:
            proxy_lines.append(f"{i}. {proxy[:30]}...")
    
    msg = f"🌐 <b>Proxy List ({len(PROXY_LIST)} total)</b>\n\n"
    msg += "<code>" + "\n".join(proxy_lines) + "</code>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_addproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /addproxy command - Owner only - Replace all proxies"""
    global PROXY_LIST, PROXY_INDEX, aio_session, last_session_reset
    
    user_id = update.effective_user.id
    if not is_owner(user_id):
        return
    
    # Get proxy list from message (after command)
    message_text = update.message.text
    
    # Remove the /addproxy command
    if ' ' in message_text:
        proxy_text = message_text.split(' ', 1)[1]
    elif '\n' in message_text:
        proxy_text = message_text.split('\n', 1)[1]
    else:
        await update.message.reply_text(
            "❌ <b>Usage:</b>\n\n"
            "<code>/addproxy\n"
            "host:port:user:pass\n"
            "host:port:user:pass\n"
            "...</code>\n\n"
            "Or: <code>/addproxy host:port:user:pass</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Parse new proxies
    new_proxies = []
    lines = proxy_text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        parts = line.split(':')
        if len(parts) == 4:
            # Format: host:port:username:password
            host, port, username, password = parts
            proxy_url = f"http://{username}:{password}@{host}:{port}"
            new_proxies.append(proxy_url)
        elif len(parts) == 2:
            # Format: host:port (no auth)
            host, port = parts
            proxy_url = f"http://{host}:{port}"
            new_proxies.append(proxy_url)
        elif line.startswith('http'):
            # Already formatted URL
            new_proxies.append(line)
    
    if not new_proxies:
        await update.message.reply_text("❌ No valid proxies found in your message.")
        return
    
    # Save to file
    try:
        with open(PROXY_FILE, 'w') as f:
            for line in lines:
                line = line.strip()
                if line:
                    f.write(line + '\n')
    except Exception as e:
        await update.message.reply_text(f"❌ Error saving proxies: {e}")
        return
    
    # Update global list
    old_count = len(PROXY_LIST)
    PROXY_LIST = new_proxies
    PROXY_INDEX = 0
    
    # Force session reset to use new proxies
    old_session = aio_session
    aio_session = None
    last_session_reset = 0
    
    if old_session and not old_session.closed:
        try:
            await old_session.close()
        except:
            pass
    
    await update.message.reply_text(
        f"✅ <b>Proxies Updated!</b>\n\n"
        f"Old: {old_count} proxies\n"
        f"New: {len(new_proxies)} proxies\n\n"
        f"🔄 Session reset - new proxies active!",
        parse_mode=ParseMode.HTML
    )
    print(f"[PROXY] Updated: {old_count} -> {len(new_proxies)} proxies")

async def cmd_setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setlimit command - Owner only"""
    user_id = update.effective_user.id
    if not is_owner(user_id):
        return

    if len(context.args) < 3:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /setlimit &lt;userid&gt; &lt;type&gt; &lt;value&gt;\n\n"
            "<b>Types:</b>\n"
            "• <code>accounts</code> (Max IVAS accounts)\n"
            "• <code>forwarders</code> (Max forwarders per account)\n"
            "• <code>getrange</code> (1 = Enable, 0 = Disable)\n\n"
            "<b>Example:</b> /setlimit 12345678 accounts 5",
            parse_mode=ParseMode.HTML
        )
        return
    try:
        target_id = int(context.args[0])
        limit_type = context.args[1].lower() if isinstance(context.args[1], str) else str(context.args[1]).lower()
        value = int(context.args[2])
    except:
        await update.message.reply_text("❌ Invalid arguments. UserID and Value must be numbers.")
        return
    
    if target_id not in APPROVED_USERS:
        await update.message.reply_text("❌ User not found in approved list.")
        return
    
    user_data = APPROVED_USERS[target_id]
    
    if limit_type in ['accounts', 'acc', 'account']:
        user_data['custom_max_accounts'] = value
        msg_type = "Max Accounts"
    elif limit_type in ['forwarders', 'fwd', 'forwarder']:
        user_data['custom_max_forwarders'] = value
        msg_type = "Max Forwarders"
    elif limit_type in ['getrange', 'range']:
        user_data['custom_getrange'] = value
        msg_type = "GetRange Access (1=ON, 0=OFF)"
    else:
        await update.message.reply_text("❌ Invalid limit type. Use 'accounts', 'forwarders', or 'getrange'.")
        return
    
    save_approved_users()
    
    await update.message.reply_text(
        f"✅ <b>Limit Updated!</b>\n\n"
        f"👤 User: <code>{target_id}</code>\n"
        f"⚙️ {msg_type}: <b>{value}</b>\n"
        f"ℹ️ This overrides their plan limit.",
        parse_mode=ParseMode.HTML
    )

async def cmd_listusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /listusers command - Owner only"""
    user_id = update.effective_user.id
    if not is_owner(user_id):
        return
    
    users_list = []
    current_time = time.time()
    
    for uid, data in APPROVED_USERS.items():
        role = "👑 Owner" if uid in OWNERS else "👤 User"
        
        # Handle old format (just in case, though migration should have fixed it)
        if isinstance(data, dict):
            plan_name = data.get('plan', 'plan4') # Default to free
            expiry = data.get('expiry', 0)
            last_used = data.get('last_used', 0)
            
            plan_info = PLANS.get(plan_name, PLANS['plan4'])
            plan_display = plan_info['name']
            
            # Check for overrides
            overrides = []
            if 'custom_max_accounts' in data:
                overrides.append(f"Accs: {data['custom_max_accounts']}")
            if 'custom_max_forwarders' in data:
                overrides.append(f"Fwds: {data['custom_max_forwarders']}")
            if 'custom_getrange' in data:
                val = "ON" if data['custom_getrange'] == 1 else "OFF"
                overrides.append(f"Range: {val}")
            
            override_str = f" (Override: {', '.join(overrides)})" if overrides else ""
            
            if uid in OWNERS:
                plan_display = "Unlimited Tier"
                expiry_str = "Never"
                status = "🟢 Active"
            else:
                if expiry < current_time:
                    status = "🔴 Expired"
                    expiry_str = "Expired"
                else:
                    status = "🟢 Active"
                    expiry_str = datetime.fromtimestamp(expiry).strftime('%Y-%m-%d')
            
            last_active = datetime.fromtimestamp(last_used).strftime('%Y-%m-%d %H:%M') if last_used > 0 else "Never"
            
            # Determine GetRange Status
            getrange_status = "🔴 Disabled"
            if uid in OWNERS:
                getrange_status = "🟢 Active"
            else:
                custom_getrange = data.get('custom_getrange')
                if custom_getrange is not None:
                    getrange_status = "🟢 Active" if custom_getrange == 1 else "🔴 Disabled"
                else:
                    # Plan based (plan4 is free/disabled, others active)
                    if plan_name != 'plan4':
                        getrange_status = "🟢 Active"

            users_list.append(
                f"<b>{role}</b> <code>{uid}</code>\n"
                f"├ Plan: {plan_display}{override_str}\n"
                f"├ Status: {status}\n"
                f"├ GetRange: {getrange_status}\n"
                f"├ Expires: {expiry_str}\n"
                f"└ Last Active: {last_active}"
            )
        else:
            # Fallback for any unmigrated data
            users_list.append(f"  • <code>{uid}</code> — {role} (Legacy Data)")
    
    msg = f"👥 <b>Approved Users ({len(APPROVED_USERS)})</b>\n\n" + "\n\n".join(users_list) if users_list else "None"
    
    # Split message if too long
    if len(msg) > 4000:
        chunks = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_listplans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /listplans command - Show available plans"""
    msg = "📋 <b>Available Plans</b>\n\n"
    
    for plan_id, plan in PLANS.items():
        duration = f"{plan['duration_days']} days" if plan['duration_days'] < 3650 else "Lifetime"
        msg += (
            f"🔹 <b>{plan['name']}</b> (<code>{plan_id}</code>)\n"
            f"   ├ ⏳ Duration: {duration}\n"
            f"   ├ 🔐 Max Accounts: {plan['max_accounts']}\n"
            f"   ├ 📢 Max Forwarders: {plan['max_forwarders']}\n"
            f"   └ 📝 {plan['description']}\n\n"
        )
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_addivas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /addivas command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    # Check Plan Limits
    user_data = APPROVED_USERS.get(user_id)
    if user_data and user_id not in OWNERS:
        plan_name = user_data.get('plan', 'plan4')
        plan = PLANS.get(plan_name, PLANS['plan4'])
        
        # Check for override, otherwise use plan limit
        max_accs = user_data.get('custom_max_accounts', plan['max_accounts'])
        
        current_accs = len(get_user_accounts(user_id))
        if current_accs >= max_accs:
            await update.message.reply_text(
                f"❌ <b>Limit Reached</b>\n\n"
                f"Your limit is max {max_accs} accounts.\n"
                f"You currently have {current_accs}.\n\n"
                f"Please upgrade your plan or delete an existing account.",
                parse_mode=ParseMode.HTML
            )
            return
    
    user_conversations[user_id] = {'state': STATE_ADD_EMAIL, 'data': {}}
    
    await update.message.reply_text(
        "➕ <b>Add IVAS Account</b>\n\n"
        "📧 <b>Step 1/4:</b> Enter your IVAS email:\n\n"
        "<i>Send /cancel to abort</i>",
        parse_mode=ParseMode.HTML
    )

async def cmd_listivas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /listivas command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    # Check if user wants to show passwords
    show_pass = hasattr(context, 'args') and context.args and isinstance(context.args[0], str) and context.args[0].lower() == 'showpass'
    accounts = get_user_accounts(user_id)
    
    if not accounts:
        await update.message.reply_text("ℹ️ No accounts found. Use /addivas to add one.")
        return
    
    lines = []
    for username, acc in accounts.items():
        default_mark = " ⭐" if acc.get('is_default') else ""
        email = acc.get('email', 'N/A')
        added = acc.get('added_date', 'N/A')[:10] if acc.get('added_date') else 'N/A'
        # Support both old single channel and new multiple channels format
        channels = acc.get('otp_channels', [])
        if not channels and acc.get('otp_channel_id'):  # Migrate old format
            channels = [acc.get('otp_channel_id')]
        if channels:
            if len(channels) == 1:
                otp_dest = f"📢 {channels[0]}"
            else:
                otp_dest = f"📢 {len(channels)} channels"
        else:
            otp_dest = "💬 DM"
        
        if show_pass:
            pwd = acc.get('password', '***')
            lines.append(f"┌─ <b>{username}</b>{default_mark}\n│ 📧 {email}\n│ 🔑 {pwd}\n│ 📨 {otp_dest}\n│ 📅 {added}\n└────────────────")
        else:
            lines.append(f"┌─ <b>{username}</b>{default_mark}\n│ 📧 {email}\n│ 📨 {otp_dest}\n│ 📅 {added}\n└────────────────")
    
    msg = f"📋 <b>Your Accounts ({len(accounts)})</b>\n\n{chr(10).join(lines)}\n\n💡 Use /listivas showpass to see passwords"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_delivas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delivas command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    accounts = get_user_accounts(user_id)
    if not accounts:
        await update.message.reply_text("❌ No accounts to delete")
        return
    
    if context.args:
        username = context.args[0]
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
    else:
        # Use default account
        username, acc = get_default_account(user_id)
        if not acc:
            acc_list = "\n".join([f"  • <code>{u}</code>" for u in accounts.keys()])
            await update.message.reply_text(
                f"<b>Usage:</b>\n"
                f"• /delivas — Delete default account\n"
                f"• /delivas <code>&lt;user&gt;</code> — Delete specific account\n\n"
                f"<b>Your accounts:</b>\n{acc_list}",
                parse_mode=ParseMode.HTML
            )
            return
    
    all_accounts = load_accounts()
    email_to_delete = all_accounts[username].get('email')
    del all_accounts[username]
    save_accounts(all_accounts)
    
    # Only delete session file if no other account uses the same email
    email_still_in_use = any(acc.get('email') == email_to_delete for acc in all_accounts.values())
    if not email_still_in_use:
        session_file = get_session_file_by_email(email_to_delete) if email_to_delete else None
        if session_file and os.path.exists(session_file):
            os.remove(session_file)
            print(f"[DEL] Session file removed for {email_to_delete}")
    else:
        print(f"[DEL] Session kept - email {email_to_delete} still in use by other account")
    
    await update.message.reply_text(f"🗑️ Deleted: <b>{username}</b>", parse_mode=ParseMode.HTML)

async def cmd_editivas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /editivas command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    accounts = get_user_accounts(user_id)
    if not accounts:
        await update.message.reply_text("❌ No accounts to edit")
        return
    
    if context.args:
        username = context.args[0]
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
    else:
        # Use default account
        username, acc = get_default_account(user_id)
        if not acc:
            acc_list = "\n".join([f"  • <code>{u}</code>" for u in accounts.keys()])
            await update.message.reply_text(
                f"<b>Usage:</b>\n"
                f"• /editivas — Edit default account\n"
                f"• /editivas <code>&lt;user&gt;</code> — Edit specific account\n\n"
                f"<b>Your accounts:</b>\n{acc_list}",
                parse_mode=ParseMode.HTML
            )
            return
    
    user_conversations[user_id] = {'state': STATE_EDIT_CHOICE, 'data': {'username': username}}
    await update.message.reply_text(
        f"✏️ Editing: <b>{username}</b>\n\nWhat to edit? Reply with:\n• <code>email</code>\n• <code>password</code>",
        parse_mode=ParseMode.HTML
    )

async def cmd_setchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setchannel [username] <add|remove|clear> [channel_id] command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    accounts = get_user_accounts(user_id)
    if not accounts:
        await update.message.reply_text("❌ No accounts found")
        return
    
    # Show help if no args
    if not hasattr(context, 'args') or context.args is None or len(context.args) < 1:
        acc_list = []
        for u, a in accounts.items():
            channels = a.get('otp_channels', [])
            mark = "⭐" if a.get('is_default') else "○"
            if channels:
                dest = "📢 " + ", ".join([str(c) for c in channels])
            else:
                dest = "💬 DM only"
            acc_list.append(f"  {mark} <code>{u}</code>\n    └─ {dest}")
        await update.message.reply_text(
            f"<b>📢 Channel Management</b>\n\n"
            f"<b>Commands (uses default account):</b>\n"
            f"• <code>/setchannel add ID</code> - Add channel\n"
            f"• <code>/setchannel remove ID</code> - Remove channel\n"
            f"• <code>/setchannel clear</code> - Clear all (DM only)\n\n"
            f"<b>Or specify account:</b>\n"
            f"• <code>/setchannel user add ID</code>\n"
            f"• <code>/setchannel user remove ID</code>\n"
            f"• <code>/setchannel user clear</code>\n\n"
            f"<b>Your accounts:</b>\n{chr(10).join(acc_list)}\n\n"
            f"<i>⭐ = default account</i>",
            parse_mode=ParseMode.HTML
        )
        return
    first_arg = context.args[0].lower() if isinstance(context.args[0], str) else str(context.args[0]).lower()
    
    if first_arg in ['add', 'remove', 'clear']:
        # Using default account
        username, acc = get_default_account(user_id)
        if not acc:
            await update.message.reply_text("❌ No default account set. Use /setchannel <username> <action>")
            return
        action = first_arg
        channel_arg_index = 1
    else:
        # First arg is username
        username = context.args[0]
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        if len(context.args) < 2:
            await update.message.reply_text(f"❌ Usage: /setchannel {username} <add|remove|clear> [channel_id]")
            return
        action = context.args[1].lower()
        channel_arg_index = 2
    
    all_accounts = load_accounts()
    
    # Ensure otp_channels exists as a list
    if 'otp_channels' not in all_accounts[username]:
        # Migrate from old single channel format
        old_ch = all_accounts[username].get('otp_channel_id')
        all_accounts[username]['otp_channels'] = [old_ch] if old_ch else []
    
    channels = all_accounts[username]['otp_channels']
    
    if action == 'clear':
        all_accounts[username]['otp_channels'] = []
        save_accounts(all_accounts)
        await update.message.reply_text(
            f"✅ <b>{username}</b> cleared all channels\n"
            f"📨 OTPs will now be sent to your DM only",
            parse_mode=ParseMode.HTML
        )
    elif action == 'add':
        # Check Plan Limits for Forwarders
        user_data = APPROVED_USERS.get(user_id)
        if user_data and user_id not in OWNERS:
            plan_name = user_data.get('plan', 'plan4')
            plan = PLANS.get(plan_name, PLANS['plan4'])
            
            # Check for override, otherwise use plan limit
            max_fwds = user_data.get('custom_max_forwarders', plan['max_forwarders'])
            
            current_fwds = len(channels)
            if current_fwds >= max_fwds:
                 await update.message.reply_text(
                    f"❌ <b>Limit Reached</b>\n\n"
                    f"Your limit is max {max_fwds} forwarders per account.\n"
                    f"You currently have {current_fwds}.\n\n"
                    f"Please upgrade your plan or remove an existing channel.",
                    parse_mode=ParseMode.HTML
                )
                 return

        if not hasattr(context, 'args') or context.args is None or len(context.args) <= channel_arg_index:
            await update.message.reply_text("❌ Usage: /setchannel [user] add <channel_id>")
            return
        try:
            channel_id_val = context.args[channel_arg_index]
            channel_id = int(channel_id_val) if isinstance(channel_id_val, str) or isinstance(channel_id_val, int) else int(str(channel_id_val))
            if channel_id in channels:
                await update.message.reply_text(f"ℹ️ Channel <code>{channel_id}</code> already added", parse_mode=ParseMode.HTML)
                return
            channels.append(channel_id)
            all_accounts[username]['otp_channels'] = channels
            save_accounts(all_accounts)
            await update.message.reply_text(
                f"✅ <b>{username}</b> added channel:\n"
                f"📢 <code>{channel_id}</code>\n\n"
                f"Total channels: {len(channels)}\n"
                f"⚠️ Make sure bot is admin in that channel/group!",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            await update.message.reply_text("❌ Invalid channel ID. Must be a number like -1001234567890")
    elif action == 'remove':
        if not hasattr(context, 'args') or context.args is None or len(context.args) <= channel_arg_index:
            await update.message.reply_text("❌ Usage: /setchannel [user] remove <channel_id>")
            return
        try:
            channel_id_val = context.args[channel_arg_index]
            channel_id = int(channel_id_val) if isinstance(channel_id_val, str) or isinstance(channel_id_val, int) else int(str(channel_id_val))
            if channel_id not in channels:
                await update.message.reply_text(f"ℹ️ Channel <code>{channel_id}</code> not in list", parse_mode=ParseMode.HTML)
                return
            channels.remove(channel_id)
            all_accounts[username]['otp_channels'] = channels
            save_accounts(all_accounts)
            dest = f"{len(channels)} channels" if channels else "DM only"
            await update.message.reply_text(
                f"✅ <b>{username}</b> removed channel:\n"
                f"🗑️ <code>{channel_id}</code>\n\n"
                f"Remaining: {dest}",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            await update.message.reply_text("❌ Invalid channel ID. Must be a number like -1001234567890")
    else:
        await update.message.reply_text(
            f"❌ Unknown action: {action}\n\n"
            f"Use: <code>add</code>, <code>remove</code>, or <code>clear</code>",
            parse_mode=ParseMode.HTML
        )

async def cmd_defaultacc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /defaultacc command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    accounts = get_user_accounts(user_id)
    if not accounts:
        await update.message.reply_text("❌ No accounts found")
        return
    
    if context.args:
        username = context.args[0]
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        
        all_accounts = load_accounts()
        for uname, acc in all_accounts.items():
            if acc.get('user_id') == user_id:
                acc['is_default'] = False
        all_accounts[username]['is_default'] = True
        save_accounts(all_accounts)
        
        await update.message.reply_text(f"⭐ Default set: <b>{username}</b>", parse_mode=ParseMode.HTML)
    else:
        acc_list = []
        for u, a in accounts.items():
            mark = "⭐" if a.get('is_default') else "○"
            acc_list.append(f"  {mark} <code>{u}</code>")
        await update.message.reply_text(f"Usage: /defaultacc &lt;username&gt;\n\n{chr(10).join(acc_list)}", parse_mode=ParseMode.HTML)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages for conversation flows"""
    # Guard against None values (channel posts, etc.)
    if not update or not hasattr(update, 'effective_user') or not update.effective_user:
        return
    if not hasattr(update, 'message') or not update.message or not hasattr(update.message, 'text') or not update.message.text:
        return
    
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    if user_id not in user_conversations:
        return
    
    conv = user_conversations[user_id]
    state = conv.get('state', STATE_NONE)
    
    if state == STATE_ADD_EMAIL:
        if '@' not in text or '.' not in text:
            await update.message.reply_text("❌ Invalid email. Try again:")
            return
        
        email = text.strip().lower()
        
        # Duplicate email check removed - anyone can add duplicates now
        
        conv['data']['email'] = email
        conv['state'] = STATE_ADD_PASS
        await update.message.reply_text("🔑 <b>Step 2/4:</b> Enter your IVAS password:", parse_mode=ParseMode.HTML)
    
    elif state == STATE_ADD_PASS:
        conv['data']['password'] = text
        conv['state'] = STATE_ADD_USERNAME
        await update.message.reply_text("👤 <b>Step 3/4:</b> Enter a username for this account:", parse_mode=ParseMode.HTML)
    
    elif state == STATE_ADD_USERNAME:
        username = text.replace(' ', '_').lower()
        
        accounts = load_accounts()
        if username in accounts:
            await update.message.reply_text("❌ Username exists. Choose another:")
            return
        
        email = conv['data']['email']
        password = conv['data']['password']
        
        # Check if another user already has this email - share the session
        existing_with_email = [u for u, a in accounts.items() if a.get('email', '').lower() == email.lower()]
        sharing_email = len(existing_with_email) > 0
        
        processing = await update.message.reply_text("⏳ Verifying credentials...")
        
        # If session already exists for this email, reuse it
        if sharing_email:
            await processing.edit_text(f"⏳ Checking shared session for {email}...")
        
        success, msg = login_and_get_session(email, password, username)
        
        if not success:
            await processing.edit_text(f"❌ Login failed: {msg}")
            del user_conversations[user_id]
            return
        
        # Store username and move to channel step
        conv['data']['username'] = username
        conv['data']['sharing_email'] = sharing_email
        
        # Check Plan Limits for Forwarders
        user_data = APPROVED_USERS.get(user_id)
        max_fwds = 0
        if user_data and user_id not in OWNERS:
            plan_name = user_data.get('plan', 'plan4')
            plan = PLANS.get(plan_name, PLANS['plan4'])
            
            # Check for override, otherwise use plan limit
            max_fwds = user_data.get('custom_max_forwarders', plan['max_forwarders'])
        else:
            max_fwds = 999 # Owner
            
        if max_fwds > 0:
            conv['state'] = STATE_ADD_CHANNEL
            await processing.edit_text(
                f"✅ Login successful!\n\n"
                f"📢 <b>Step 4/4 (Optional):</b>\n\n"
                f"Send OTPs to a Channel/Group?\n\n"
                f"• Send the <b>Channel/Group ID</b> (e.g. <code>-1001234567890</code>)\n"
                f"• Or type <b>skip</b> to receive OTPs in DM\n\n"
                f"⚠️ <i>If using channel/group, add this bot as admin with all permissions first!</i>",
                parse_mode=ParseMode.HTML
            )
        else:
            # Skip channel step for plans with 0 forwarders
            accounts = load_accounts()
            user_accounts = get_user_accounts(user_id)
            is_first = len(user_accounts) == 0
            
            accounts[username] = {
                'email': email,
                'password': password,
                'user_id': user_id,
                'username': username,
                'added_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'is_default': is_first,
                'edit_count': 0,
                'otp_channels': []  # DM only
            }
            save_accounts(accounts)
            del user_conversations[user_id]
            
            sharing_note = f"\n🔗 Shared session with: {', '.join(existing_with_email)}" if sharing_email else ""
            
            await processing.edit_text(
                f"✅ <b>Account Added</b>\n\n"
                f"👤 Username: {username}\n"
                f"📧 Email: {email}\n"
                f"⭐ Default: {'Yes' if is_first else 'No'}\n"
                f"📨 OTPs: 💬 DM (Plan Limit){sharing_note}\n\n"
                f"💡 Use <code>/addnum [id]</code> to add numbers",
                parse_mode=ParseMode.HTML
            )
    
    elif state == STATE_ADD_CHANNEL:
        username = conv['data']['username']
        email = conv['data']['email']
        password = conv['data']['password']
        
        channel_id = None
        if text.lower() != 'skip':
            # Validate channel ID
            try:
                channel_id = int(text)
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid ID. Send a numeric ID like <code>-1001234567890</code>\n"
                    "Or type <b>skip</b> to receive OTPs in DM",
                    parse_mode=ParseMode.HTML
                )
                return
        
        accounts = load_accounts()
        user_accounts = get_user_accounts(user_id)
        is_first = len(user_accounts) == 0
        sharing_email = conv['data'].get('sharing_email', False)
        
        # Get existing accounts with same email
        existing_with_email = [u for u, a in accounts.items() if a.get('email', '').lower() == email.lower()]
        
        accounts[username] = {
            'email': email,
            'password': password,
            'user_id': user_id,
            'username': username,
            'added_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'is_default': is_first,
            'edit_count': 0,
            'otp_channels': [channel_id] if channel_id else []  # List of channels, empty = DM only
        }
        save_accounts(accounts)
        
        del user_conversations[user_id]
        
        otp_dest = f"📢 Channel: <code>{channel_id}</code>" if channel_id else "💬 DM (your private chat)"
        add_more = "\n\n💡 Add more channels: /setchannel" if channel_id else ""
        sharing_note = f"\n🔗 Shared session with: {', '.join(existing_with_email)}" if len(existing_with_email) > 0 else ""
        
        await update.message.reply_text(
            f"✅ <b>Account Added</b>\n\n"
            f"👤 Username: {username}\n"
            f"📧 Email: {email}\n"
            f"⭐ Default: {'Yes' if is_first else 'No'}\n"
            f"📨 OTPs: {otp_dest}{sharing_note}{add_more}\n\n"
            f"💡 Use <code>/addnum [id]</code> to add numbers",
            parse_mode=ParseMode.HTML
        )
    
    elif state == STATE_EDIT_CHOICE:
        choice = text.lower()
        if choice not in ['email', 'password']:
            await update.message.reply_text("❌ Reply with 'email' or 'password':")
            return
        
        conv['data']['edit_field'] = choice
        conv['state'] = STATE_EDIT_VALUE
        await update.message.reply_text(f"📝 Enter new <b>{choice}</b>:", parse_mode=ParseMode.HTML)
    
    elif state == STATE_EDIT_VALUE:
        username = conv['data']['username']
        field = conv['data']['edit_field']
        
        accounts = load_accounts()
        if username not in accounts:
            await update.message.reply_text("❌ Account not found")
            del user_conversations[user_id]
            return
        
        accounts[username][field] = text
        save_accounts(accounts)
        
        email = accounts[username]['email']
        password = accounts[username]['password']
        
        processing = await update.message.reply_text("⏳ Updating session...")
        success, _ = login_and_get_session(email, password, username)
        
        del user_conversations[user_id]
        
        if success:
            await processing.edit_text(f"✅ Updated {field} for <b>{username}</b>", parse_mode=ParseMode.HTML)
        else:
            await processing.edit_text(f"⚠️ {field} updated but login failed. Check credentials.")

async def cmd_addnum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /addnum command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("❌ Usage: /addnum [id] [username]")
        return
    
    termination_id = context.args[0]
    
    if hasattr(context, 'args') and context.args and len(context.args) > 1:
        username = context.args[1]
        accounts = get_user_accounts(user_id)
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        acc = accounts[username]
    else:
        username, acc = get_default_account(user_id)
        if not acc:
            await update.message.reply_text("❌ No IVAS account. Use /addivas to add one.")
            return
    
    print(f"[CMD] /addnum {termination_id} for {username} from {user_id}")
    
    processing_msg = await update.message.reply_text(
        f"⏳ Adding termination ID: {termination_id}\n👤 Account: {username}"
    )
    
    is_valid, msg = check_session_health(username)
    if not is_valid:
        await processing_msg.edit_text("🔄 Refreshing session...")
        success, _ = login_and_get_session(acc['email'], acc['password'], username)
        if not success:
            await processing_msg.edit_text(f"❌ Session refresh failed for {username}")
            return
    
    numbers, status, range_name, account_name = add_number_and_get(termination_id, username)
    
    if numbers is None:
        await processing_msg.edit_text("🔄 Refreshing session...")
        success, _ = login_and_get_session(acc['email'], acc['password'], username)
        if success:
            numbers, status, range_name, account_name = add_number_and_get(termination_id, username)
    
    if numbers is None:
        await processing_msg.edit_text(f"❌ {status}")
        return
    
    flag = get_country_flag(range_name) if range_name else "🌍"
    range_display = range_name if range_name else "Unknown"
    
    if not numbers:
        await processing_msg.edit_text(
            f"✅ Added but 0 numbers\n\n"
            f"👤 Account: {username}\n"
            f"🆔 ID: {termination_id}\n"
            f"{flag} Range: {range_display}"
        )
        return
    
    await processing_msg.edit_text(
        f"✅ <b>Added Successfully</b>\n\n"
        f"👤 Account: {username}\n"
        f"🆔 ID: {termination_id}\n"
        f"{flag} Range: {range_display}\n"
        f"📱 Numbers: {len(numbers)}",
        parse_mode=ParseMode.HTML
    )
    
    file_content = "\n".join(str(n) for n in numbers)
    file_bytes = io.BytesIO(file_content.encode('utf-8'))
    file_bytes.name = f"numbers_{termination_id}.txt"
    
    await update.message.reply_document(
        document=file_bytes,
        filename=f"numbers_{termination_id}.txt",
        caption=f"📦 {len(numbers)} numbers | {flag} {range_display} | /startotp to scan"
    )

async def cmd_getnum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /getnum <id> [username] command - Get numbers without adding"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("❌ Usage: /getnum [id] [username]")
        return
    
    termination_id = context.args[0]
    
    # Get account - either specified or default
    if len(context.args) > 1:
        username = context.args[1]
        accounts = get_user_accounts(user_id)
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        acc = accounts[username]
    else:
        username, acc = get_default_account(user_id)
        if not acc:
            await update.message.reply_text("❌ No IVAS account. Use /addivas to add one.")
            return
    
    print(f"[CMD] /getnum {termination_id} for {username} from {user_id}")
    
    processing_msg = await update.message.reply_text(
        f"⏳ Getting numbers for ID: {termination_id}\n👤 Account: {username}"
    )
    
    # Ensure session is valid
    is_valid, msg = check_session_health(username)
    if not is_valid:
        await processing_msg.edit_text("🔄 Refreshing session...")
        success, _ = login_and_get_session(acc['email'], acc['password'], username)
        if not success:
            await processing_msg.edit_text(f"❌ Session refresh failed for {username}")
            return
    
    # Fetch numbers only (no add)
    numbers, status, account_name = get_numbers_only(termination_id, username)
    print(f"[GETNUM] Result: {len(numbers) if numbers else 0} numbers, status: {status}")
    
    if numbers is None:
        # Session expired, try refresh
        await processing_msg.edit_text("🔄 Refreshing session...")
        success, _ = login_and_get_session(acc['email'], acc['password'], username)
        if success:
            numbers, status, account_name = get_numbers_only(termination_id, username)
    
    if numbers is None:
        await processing_msg.edit_text(f"❌ {status}")
        return
    
    if status == "not_added" or len(numbers) == 0:
        await processing_msg.edit_text(
            f"⚠️ <b>No numbers found</b>\n\n"
            f"🆔 ID: {termination_id}\n"
            f"👤 Account: {username}\n\n"
            f"💡 Range not added. Use:\n"
            f"<code>/addnum {termination_id}</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    print(f"[GETNUM] ✅ Got {len(numbers)} numbers for {termination_id}")
    
    # Update message with success
    await processing_msg.edit_text(
        f"✅ <b>Numbers Found</b>\n\n"
        f"👤 Account: {username}\n"
        f"🆔 ID: {termination_id}\n"
        f"📱 Numbers: {len(numbers)}",
        parse_mode=ParseMode.HTML
    )
    
    # Send as file
    file_content = "\n".join(str(n) for n in numbers)
    file_bytes = io.BytesIO(file_content.encode('utf-8'))
    file_bytes.name = f"numbers_{termination_id}.txt"
    
    await update.message.reply_document(
        document=file_bytes,
        filename=f"numbers_{termination_id}.txt",
        caption=f"📦 {len(numbers)} numbers | /startotp to scan"
    )

async def cmd_delallnum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delallnum command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    if context.args:
        username = context.args[0]
        accounts = get_user_accounts(user_id)
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        acc = accounts[username]
    else:
        username, acc = get_default_account(user_id)
        if not acc:
            await update.message.reply_text("❌ No IVAS account. Use /addivas to add one.")
            return
    
    cancel_flags[user_id] = False
    
    processing_msg = await update.message.reply_text(f"⏳ Deleting numbers for {username}...")
    
    is_valid, msg = check_session_health(username)
    if not is_valid:
        await processing_msg.edit_text("🔄 Refreshing session...")
        success, _ = login_and_get_session(acc['email'], acc['password'], username)
        if not success:
            await processing_msg.edit_text(f"❌ Session error: {msg}")
            return
    
    async def progress_update(attempt, batch_count, total_so_far):
        try:
            if attempt == -1:
                # Verification complete
                await processing_msg.edit_text(
                    f"✅ Verified - All numbers removed!\n"
                    f"Total deleted: {total_so_far}"
                )
            else:
                await processing_msg.edit_text(
                    f"🗑️ Deleting... Attempt {attempt}\n"
                    f"This batch: {batch_count}\n"
                    f"Total deleted: {total_so_far}"
                )
        except:
            pass
    
    total_removed, attempts, status, _ = await delete_all_numbers_async(user_id, progress_update, username)
    
    await processing_msg.edit_text(
        f"✅ <b>Completed</b>\n\n"
        f"👤 Account: {username}\n"
        f"🗑️ Deleted: {total_removed}\n"
        f"🔄 Attempts: {attempts}",
        parse_mode=ParseMode.HTML
    )

async def cmd_getstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /getstats command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    username = None
    
    for arg in context.args:
        if isinstance(arg, str) and '-' in arg and len(arg) == 10:
            date_str = arg
        else:
            username = arg
    
    if username:
        accounts = get_user_accounts(user_id)
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        acc = accounts[username]
    else:
        username, acc = get_default_account(user_id)
        if not acc:
            await update.message.reply_text("❌ No IVAS account. Use /addivas to add one.")
            return
    
    processing_msg = await update.message.reply_text(f"⏳ Fetching stats for {username}...")
    
    is_valid, msg = check_session_health(username)
    if not is_valid:
        await processing_msg.edit_text("🔄 Refreshing session...")
        success, _ = login_and_get_session(acc['email'], acc['password'], username)
        if not success:
            await processing_msg.edit_text(f"❌ Session error: {msg}")
            return
    
    ranges, status = get_stats(date_str, username)
    
    if ranges is None:
        await processing_msg.edit_text("🔄 Refreshing session...")
        success, _ = login_and_get_session(acc['email'], acc['password'], username)
        if success:
            ranges, status = get_stats(date_str, username)
    
    if ranges is None:
        await processing_msg.edit_text(f"❌ Failed: {status}")
        return
    
    formatted = format_stats_message(ranges, date_str)
    formatted = f"👤 <b>{username}</b>\n\n" + formatted
    await processing_msg.edit_text(formatted, parse_mode=ParseMode.HTML)

async def cmd_startotp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /startotp [username] command"""
    global otp_monitoring_active, otp_monitoring_task, otp_monitor_accounts
    
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    # Determine which account(s) to monitor
    target_username = None
    if context.args:
        # Specific account provided
        username = context.args[0]
        accounts = get_user_accounts(user_id)
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        target_username = username
    else:
        # Use default account
        username, acc = get_default_account(user_id)
        if not acc:
            await update.message.reply_text("❌ No IVAS account. Use /addivas to add one.")
            return
        target_username = username
    
    # Add to monitored set
    if target_username in otp_monitor_accounts and otp_monitoring_active:
        await update.message.reply_text(f"ℹ️ Already scanning for <b>{target_username}</b>!", parse_mode=ParseMode.HTML)
        return

    otp_monitor_accounts.add(target_username)
    save_monitored_accounts()

    if not otp_monitoring_active:
        otp_monitoring_active = True
        otp_monitoring_task = asyncio.create_task(otp_monitoring_loop())
        await update.message.reply_text(
            f"✅ <b>Scanning Started</b>\n\n"
            f"👤 <b>{target_username}</b>\n"
            f"🔄 Checking every 2 seconds\n"
            f"📨 OTPs sent to owner/channels\n\n"
            f"/stopotp to stop",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            f"✅ <b>Added to Scan</b>\n\n"
            f"👤 <b>{target_username}</b>\n"
            f"🔄 Now monitoring this account too",
            parse_mode=ParseMode.HTML
        )

async def cmd_stopotp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stopotp command"""
    global otp_monitoring_active, otp_monitoring_task, otp_monitor_accounts
    
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    if not otp_monitoring_active:
        await update.message.reply_text("ℹ️ Not running")
        return
    
    # Determine which account to stop
    target_username = None
    if context.args:
        username = context.args[0]
        accounts = get_user_accounts(user_id)
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        target_username = username
    else:
        # Use default account
        username, acc = get_default_account(user_id)
        if not acc:
            await update.message.reply_text("❌ No IVAS account.")
            return
        target_username = username
        
    if target_username in otp_monitor_accounts:
        otp_monitor_accounts.remove(target_username)
        save_monitored_accounts()
        if not otp_monitor_accounts:
            # No more accounts to monitor, stop the loop
            otp_monitoring_active = False
            if otp_monitoring_task:
                otp_monitoring_task.cancel()
                otp_monitoring_task = None
            await update.message.reply_text(f"🛑 <b>Scan Stopped</b> for {target_username}\n(No other accounts scanning)", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(f"🛑 <b>Scan Stopped</b> for {target_username}\n(Other accounts still scanning)", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"ℹ️ {target_username} is not currently being scanned.")

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /cancel command"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    cancel_flags[user_id] = True
    if user_id in user_conversations:
        del user_conversations[user_id]
    
    await update.message.reply_text("🚫 Cancelled")

async def cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /refresh command (strict: always delete old, force new)"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return

    if context.args:
        username = context.args[0]
        accounts = get_user_accounts(user_id)
        if username not in accounts:
            await update.message.reply_text(f"❌ Account '{username}' not found")
            return
        acc = accounts[username]
    else:
        username, acc = get_default_account(user_id)
        if not acc:
            await update.message.reply_text("❌ No IVAS account. Use /addivas to add one.")
            return

    processing_msg = await update.message.reply_text(f"⏳ Strictly refreshing {username} (deleting all old sessions)...")
    success = await refresh_account_session(username, force_new=True)
    if success:
        await processing_msg.edit_text(f"✅ Session strictly refreshed for <b>{username}</b>", parse_mode=ParseMode.HTML)
    else:
        await processing_msg.edit_text(f"❌ Failed to refresh session for <b>{username}</b>", parse_mode=ParseMode.HTML)

async def cmd_getrange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /getrange command - Search for active ranges by keyword"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    # Check Permissions (Plan & Overrides)
    if user_id not in OWNERS:
        user_data = APPROVED_USERS.get(user_id)
        if user_data:
            # Check override first
            custom_getrange = user_data.get('custom_getrange')
            if custom_getrange is not None:
                if custom_getrange == 0:
                    await update.message.reply_text("❌ <b>Access Denied</b>\n\nYour access to /getrange has been disabled by admin.", parse_mode=ParseMode.HTML)
                    return
                # If 1, allow (continue)
            else:
                # Check plan
                plan_name = user_data.get('plan', 'plan4')
                # Allow plan1, plan2, plan3. Block plan4 (Free).
                if plan_name == 'plan4':
                     await update.message.reply_text(
                         "❌ <b>Access Denied</b>\n\n"
                         "/getrange is available in <b>Trial</b> and <b>Paid Plans</b> only.\n"
                         "Please upgrade to use this feature.", 
                         parse_mode=ParseMode.HTML
                     )
                     return

    if not hasattr(context, 'args') or context.args is None or len(context.args) < 1:
        await update.message.reply_text(
            "❌ <b>Usage:</b> /getrange [keyword]\n\n"
            "<b>Example:</b> /getrange whatsapp\n\n"
            "This searches for active ranges that have received SMS matching the keyword.",
            parse_mode=ParseMode.HTML
        )
        return
    keyword = " ".join(context.args)
    
    # Get user's default account
    username, acc = get_default_account(user_id)
    if not acc:
        await update.message.reply_text("❌ No IVAS account. Use /addivas to add one.")
        return
    
    session_data = load_session(username)
    if not session_data:
        await update.message.reply_text(f"❌ No session for {username}. Use /refresh")
        return
    
    processing_msg = await update.message.reply_text(f"🔍 Searching for ranges with '<b>{keyword}</b>'...", parse_mode=ParseMode.HTML)
    
    try:
        cookies = session_data.get('cookies', {})
        ua = session_data.get('ua', DEFAULT_UA)
        
        # Use synchronous requests with thread pool (more reliable for auth)
        def fetch_ranges_sync(cookies_dict, search_keyword):
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            s = requests.Session()
            
            # Configure Proxy
            if PROXY_LIST:
                import random
                proxy = random.choice(PROXY_LIST)
                s.proxies = {
                    "http": proxy,
                    "https": proxy
                }
            
            for k, v in cookies_dict.items():
                s.cookies.set(k, v)
            
            # Exact same request as browser - only search value changes
            timestamp = str(int(time.time() * 1000))
            url = (
                f"{SMS_TEST_URL}?draw=1"
                f"&columns%5B0%5D%5Bdata%5D=range"
                f"&columns%5B0%5D%5Borderable%5D=false"
                f"&columns%5B1%5D%5Bdata%5D=termination.test_number"
                f"&columns%5B1%5D%5Bsearchable%5D=false"
                f"&columns%5B1%5D%5Bborderable%5D=false"
                f"&columns%5B2%5D%5Bdata%5D=originator"
                f"&columns%5B2%5D%5Borderable%5D=false"
                f"&columns%5B3%5D%5Bdata%5D=messagedata"
                f"&columns%5B3%5D%5Borderable%5D=false"
                f"&columns%5B4%5D%5Bdata%5D=senttime"
                f"&columns%5B4%5D%5Bsearchable%5D=false"
                f"&columns%5B4%5D%5Bborderable%5D=false"
                f"&order%5B0%5D%5Bcolumn%5D=0"
                f"&order%5B0%5D%5Bdir%5D=asc"
                f"&start=0"
                f"&length=25"
                f"&search%5Bvalue%5D={quote_plus(search_keyword)}"
                f"&_={timestamp}"
            )
            
            headers = {
                "User-Agent": ua,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{BASE_URL}/portal/sms/test/sms"
            }
            
            try:
                # Try with proxy first
                resp = s.get(url, headers=headers, timeout=30, verify=False)
            except Exception as e:
                # Fallback to direct connection if proxy fails
                print(f"[!] Proxy failed: {e}, retrying direct...")
                s.proxies = {}
                resp = s.get(url, headers=headers, timeout=30, verify=False)
            
            try:
                return resp.status_code, resp.json()
            except Exception as e:
                print(f"[!] JSON Decode Error: {e}")
                print(f"[!] Response Text: {resp.text[:500]}") # Log first 500 chars
                return resp.status_code, None
        
        loop = asyncio.get_event_loop()
        status_code, data = await loop.run_in_executor(thread_pool, fetch_ranges_sync, cookies, keyword)
        
        # Auto-refresh on 401/419 and retry
        if status_code in [401, 419] or (status_code == 200 and data is None):
            await processing_msg.edit_text("🔄 Session expired or invalid, refreshing...")
            success, msg = login_and_get_session(acc['email'], acc['password'], username)
            if success:
                # Reload session and retry
                session_data = load_session(username)
                if session_data:
                    cookies = session_data.get('cookies', {})
                    status_code, data = await loop.run_in_executor(thread_pool, fetch_ranges_sync, cookies, keyword)
            
            if status_code != 200 or data is None:
                await processing_msg.edit_text(f"❌ Request failed with status {status_code}. Try /refresh manually.")
                return
        
        if status_code != 200 or data is None:
            await processing_msg.edit_text(f"❌ Request failed with status {status_code}")
            return
        
        records = data.get('data', [])
        if not records:
            await processing_msg.edit_text(
                f"📭 No results found for '<b>{keyword}</b>'",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Extract unique ranges with their termination IDs
        ranges_dict = {}  # range_name -> termination_id
        for record in records:
            range_name = record.get('range', '')
            termination_id = record.get('termination_id', '')
            if range_name and termination_id:
                if range_name not in ranges_dict:
                    ranges_dict[range_name] = termination_id
        
        if not ranges_dict:
            await processing_msg.edit_text(
                f"📭 No ranges found for '<b>{keyword}</b>'",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Build response message
        total_filtered = data.get('recordsFiltered', len(records))
        
        import html
        lines = []
        for range_name, term_id in sorted(ranges_dict.items()):
            flag = get_country_flag(range_name)
            # Escape HTML entities in range name to prevent formatting issues
            safe_range = html.escape(str(range_name))
            safe_term_id = html.escape(str(term_id))
            lines.append(f"{flag} <code>{safe_range}</code> - <code>{safe_term_id}</code>")
        
        # Split into chunks if too long
        safe_keyword = html.escape(keyword)
        header = f"🔍 <b>Active Ranges for '{safe_keyword}'</b>\n"
        header += f"📊 Total matches: {total_filtered} | Unique ranges: {len(ranges_dict)}\n\n"
        
        message = header + "\n".join(lines)
        
        # Telegram message limit is 4096 characters
        if len(message) > 4000:
            # Send in chunks
            await processing_msg.edit_text(header + f"Found {len(ranges_dict)} ranges, sending list...")
            
            chunk_lines = []
            chunk_size = 0
            for line in lines:
                if chunk_size + len(line) + 1 > 3800:
                    await update.message.reply_text("\n".join(chunk_lines), parse_mode=ParseMode.HTML)
                    chunk_lines = []
                    chunk_size = 0
                chunk_lines.append(line)
                chunk_size += len(line) + 1
            
            if chunk_lines:
                await update.message.reply_text("\n".join(chunk_lines), parse_mode=ParseMode.HTML)
        else:
            await processing_msg.edit_text(message, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        print(f"[!] getrange error: {e}")
        traceback.print_exc()
        await processing_msg.edit_text(f"❌ Error: {str(e)[:100]}")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command"""
    global otp_monitoring_active
    
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        await send_access_denied(update)
        return
    
    accounts = get_user_accounts(user_id)
    acc_lines = []
    
    for username, acc in accounts.items():
        is_valid, msg = check_session_health(username)
        status_icon = "🟢" if is_valid else "🔴"
        default_mark = "⭐" if acc.get('is_default') else "○"
        acc_lines.append(f"  {status_icon} {username}{default_mark}")
    
    otp_emoji = "🟢" if otp_monitoring_active else "⚫"
    otp_text = "Active" if otp_monitoring_active else "Stopped"
    otp_count = get_otp_count()
    
    accounts_section = "\n".join(acc_lines) if acc_lines else "  No accounts"
    
    await update.message.reply_text(
        f"📊 <b>Status</b>\n\n"
        f"<b>🔐 Accounts:</b>\n{accounts_section}\n\n"
        f"<b>📡 Scanner:</b> {otp_emoji} {otp_text}\n"
        f"<b>📬 OTPs Received:</b> {otp_count}",
        parse_mode=ParseMode.HTML
    )

# ==========================================
#           SESSION AUTO-REFRESH
# ==========================================

async def session_maintenance(app):
    """Background task - refresh sessions periodically"""
    print("[∞] Session maintenance started")
    
    await asyncio.sleep(600)  # Wait 10 mins before first refresh
    
    while True:
        try:
            accounts = load_accounts()
            loop = asyncio.get_event_loop()
            
            # Group by email to avoid redundant refreshes
            email_map = {}
            for username, acc in accounts.items():
                email = acc.get('email')
                if email and email not in email_map:
                    email_map[email] = (username, acc['password'])
            
            for email, (username, password) in email_map.items():
                try:
                    # Check health first
                    is_valid, msg = check_session_health(username)
                    
                    if not is_valid:
                        print(f"[🔄] Auto-refreshing session for {email}...")
                        await loop.run_in_executor(
                            thread_pool,
                            lambda e=email, p=password, u=username: login_and_get_session(e, p, u)
                        )
                except Exception as e:
                    print(f"[⚠️] Error refreshing {email}: {e}")
            
        except Exception as e:
            print(f"[!] Maintenance error: {e}")
        
        await asyncio.sleep(3600)  # Every 1 hour (Silent refresh)

# ==========================================
#           MAIN
# ==========================================

def main():
    """Start the bot"""
    global telegram_app
    
    print("="  * 50)
    print("🤖 IvaSMS Telegram Bot v10 Starting...")
    print("="  * 50)
    
    init_database()
    load_approved_users()
    load_proxies()  # Load proxy list at startup
    
    if not os.path.exists(SESSIONS_DIR):
        os.makedirs(SESSIONS_DIR)
    # Restore monitored accounts on startup
    global otp_monitor_accounts
    otp_monitor_accounts = load_monitored_accounts()
    
    print(f"[i] Owners: {OWNERS}")
    print(f"[i] Approved users: {len(APPROVED_USERS)}")
    
    from telegram.request import HTTPXRequest
    
    request = HTTPXRequest(
        connect_timeout=30,
        read_timeout=30,
        write_timeout=30,
        pool_timeout=30,
        connection_pool_size=20
    )
    application = Application.builder().token(BOT_TOKEN).request(request).concurrent_updates(True).build()
    telegram_app = application
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("addnum", cmd_addnum))
    application.add_handler(CommandHandler("getnum", cmd_getnum))
    application.add_handler(CommandHandler("delallnum", cmd_delallnum))
    application.add_handler(CommandHandler("getstats", cmd_getstats))
    application.add_handler(CommandHandler("startotp", cmd_startotp))
    application.add_handler(CommandHandler("stopotp", cmd_stopotp))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_handler(CommandHandler("refresh", cmd_refresh))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("approve", cmd_approve))
    application.add_handler(CommandHandler("revoke", cmd_revoke))
    application.add_handler(CommandHandler("listusers", cmd_listusers))
    application.add_handler(CommandHandler("listplans", cmd_listplans))
    application.add_handler(CommandHandler("setlimit", cmd_setlimit))
    application.add_handler(CommandHandler("proxy", cmd_proxy))
    application.add_handler(CommandHandler("addproxy", cmd_addproxy))
    application.add_handler(CommandHandler("addivas", cmd_addivas))
    application.add_handler(CommandHandler("listivas", cmd_listivas))
    application.add_handler(CommandHandler("delivas", cmd_delivas))
    application.add_handler(CommandHandler("editivas", cmd_editivas))
    application.add_handler(CommandHandler("setchannel", cmd_setchannel))
    application.add_handler(CommandHandler("defaultacc", cmd_defaultacc))
    application.add_handler(CommandHandler("getrange", cmd_getrange))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    async def set_commands(app):
        commands = [
            BotCommand("start", "Show help menu"),
            BotCommand("addivas", "Add new IVAS account"),
            BotCommand("listivas", "List your accounts"),
            BotCommand("delivas", "Delete an account"),
            BotCommand("editivas", "Edit an account"),
            BotCommand("defaultacc", "Set default account"),
            BotCommand("setchannel", "Set OTP channel/group"),
            BotCommand("addnum", "Add termination & get numbers"),
            BotCommand("getnum", "Get numbers (no add)"),
            BotCommand("delallnum", "Delete all numbers"),
            BotCommand("getrange", "Search active ranges"),
            BotCommand("getstats", "Get statistics"),
            BotCommand("startotp", "Start OTP monitoring"),
            BotCommand("stopotp", "Stop OTP monitoring"),
            BotCommand("refresh", "Refresh session"),
            BotCommand("status", "Check bot status"),
            BotCommand("cancel", "Cancel operation"),
            BotCommand("approve", "[Owner] Approve user"),
            BotCommand("revoke", "[Owner] Revoke user"),
            BotCommand("listusers", "[Owner] List users"),
            BotCommand("listplans", "Show available plans"),
            BotCommand("setlimit", "[Owner] Set custom limits"),
            BotCommand("proxy", "[Owner] View proxies"),
            BotCommand("addproxy", "[Owner] Replace proxies"),
        ]
        await app.bot.set_my_commands(commands)
    
    async def startup(app):
        await set_commands(app)
        asyncio.create_task(session_maintenance(app))
    
    application.post_init = startup
    
    print("\n[✅] Bot is running!")
    print("[i] OTPs go ONLY to account owner")
    print("[i] concurrent_updates=True for multi-user support")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    # Start background session renewal
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(background_session_renewal_loop())
    except Exception:
        # Fallback for non-async main
        threading.Thread(target=lambda: asyncio.run(background_session_renewal_loop()), daemon=True).start()
    main()
