"""
tests/test_security.py — Security regression tests
Run with: pytest tests/test_security.py -v
"""
import re
import os
import sys
import hmac
import time

# ── Helpers ─────────────────────────────────────────────────────────────────

# Base dir = project root (parent of tests/)
_BASE = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

def read(path):
    full = os.path.join(_BASE, path)
    with open(full) as f:
        return f.read()


# ══════════════════════════════════════════════════════════════════════════════
# WEB PANEL SECURITY
# ══════════════════════════════════════════════════════════════════════════════

class TestWebPanelSecurity:
    """Verify all web panel security fixes are present."""

    def test_no_autologin_bypass(self):
        """Auto-login on API failure must be removed."""
        html = read('web_panel_frontend.html')
        # Must NOT have the old auto-login pattern
        assert 'Dev mode — auto-login' not in html
        assert "document.getElementById('loginScreen').style.display = 'none'" not in html.split('.catch')[1][:200] if '.catch' in html else True

    def test_xss_esc_function_present(self):
        """esc() function for HTML escaping must exist."""
        html = read('web_panel_frontend.html')
        assert 'function esc(str)' in html
        assert '.replace(/&/g,' in html
        assert ".replace(/</g," in html

    def test_user_data_escaped_in_innerHTML(self):
        """User names must be wrapped in esc() before innerHTML."""
        html = read('web_panel_frontend.html')
        assert 'esc(u.name' in html
        assert 'esc(u.username' in html
        assert 'esc(l.msg' in html

    def test_no_localstorage_password(self):
        """Password must not be stored in localStorage."""
        html = read('web_panel_frontend.html')
        assert "localStorage.setItem('panel_pwd'" not in html
        assert "localStorage.getItem('panel_pwd'" not in html

    def test_session_storage_used(self):
        """sessionStorage must be used for auth token."""
        html = read('web_panel_frontend.html')
        assert 'sessionStorage' in html

    def test_no_hardcoded_owner_id_in_frontend(self):
        """Real owner ID must not be hardcoded in frontend."""
        html = read('web_panel_frontend.html')
        assert '5000948476' not in html

    def test_panel_secret_none_blocks_auth(self):
        """Auth check must return False when PANEL_SECRET is None."""
        src = read('web_panel.py')
        assert 'if PANEL_SECRET is None' in src
        assert 'hmac.compare_digest' in src

    def test_rate_limit_cleanup(self):
        """Rate limiter dict must have cleanup to prevent memory leak."""
        src = read('web_panel.py')
        assert '_rate_limits_last_cleanup' in src

    def test_no_default_password(self):
        """Default password 'admin123' must not exist."""
        src = read('web_panel.py')
        assert '"admin123"' not in src
        assert "'admin123'" not in src


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG / CREDENTIALS
# ══════════════════════════════════════════════════════════════════════════════

class TestCredentialSecurity:

    def test_owner_id_required(self):
        """OWNER_ID must be required with no hardcoded fallback."""
        src = read('config.py')
        assert 'required=True' in src
        assert '5000948476' not in src

    def test_no_real_phone_numbers(self):
        """Real phone numbers must not appear in code."""
        for fname in ['main.py', 'lang.py', 'time_helper.py']:
            src = read(fname)
            assert '+917582943989' not in src, f"Real phone number in {fname}"
            assert '+919876543210' not in src, f"Real phone number in {fname}"

    def test_dotenv_loaded(self):
        """load_dotenv must be called in config.py."""
        src = read('config.py')
        assert 'load_dotenv' in src

    def test_no_default_admin_credentials(self):
        """No default admin password anywhere."""
        for fname in ['config.py', 'web_panel.py', 'admin.py']:
            src = read(fname)
            for bad in ['admin123', 'password123', 'test123']:
                assert bad not in src, f"Default credential '{bad}' found in {fname}"


# ══════════════════════════════════════════════════════════════════════════════
# DATA INTEGRITY
# ══════════════════════════════════════════════════════════════════════════════

class TestDataIntegrity:

    def test_backup_is_redirect(self):
        """backup.py must be thin redirect, not duplicate."""
        src = read('backup.py')
        assert 'from database import' in src
        assert 'def get_user_data' not in src
        assert 'def block_user' not in src
        assert len(src.splitlines()) < 40, "backup.py should be < 40 lines"

    def test_no_duplicate_block_user(self):
        """database.py must have only one block_user definition."""
        src = read('database.py')
        count = src.count('def block_user(')
        assert count == 1, f"Expected 1 block_user, found {count}"

    def test_no_duplicate_give_premium(self):
        """premium.py must have only one give_premium definition."""
        src = read('premium.py')
        count = src.count('async def give_premium(')
        assert count == 1, f"Expected 1 give_premium, found {count}"
        assert '_original_give_premium' not in src

    def test_spend_coins_has_lock(self):
        """spend_coins must use per-user lock."""
        src = read('task_board.py')
        func_start = src.find('def spend_coins(')
        func_end = src.find('\ndef ', func_start + 1)
        func_body = src[func_start:func_end]
        assert '_get_coin_lock' in func_body

    def test_reseller_commission_validated_on_update(self):
        """Commission must be validated on both create AND update."""
        src = read('reseller.py')
        update_section = src[src.find('if user_id in resellers:'):src.find('else:')]
        assert 'commission_val' in update_section or 'max(0.0' in update_section


# ══════════════════════════════════════════════════════════════════════════════
# INJECTION / INJECTION PREVENTION
# ══════════════════════════════════════════════════════════════════════════════

class TestInjectionPrevention:

    def test_ffmpeg_text_escaping(self):
        """FFmpeg drawtext must have proper escaping."""
        src = read('watermark.py')
        # Must escape backslash, quote, colon, and percent
        assert '.replace("\\\\", ' in src or ".replace('\\\\'" in src
        assert '.replace("%"' in src
        assert '.replace("\'"' in src or ".replace(\"'\"" in src

    def test_ffmpeg_scale_clamped(self):
        """FFmpeg logo scale must be clamped to prevent memory abuse."""
        src = read('watermark.py')
        assert 'max(1, min(50' in src

    def test_ad_click_no_user_id_from_data(self):
        """Ad click must never trust user_id from callback data."""
        src = read('ui/ads_menu.py')
        # Find adclick_cb function
        start = src.find('async def adclick_cb(')
        end = src.find('\n\n\n', start)
        func = src[start:end]
        assert 'event.sender_id' in func
        assert 'SECURITY FIX' in func
        # Must NOT have the vulnerable pattern
        assert "int(parts[2])" not in func

    def test_no_eval_exec(self):
        """No eval() or exec() in Python files."""
        for root, dirs, files in os.walk(_BASE):
            dirs[:] = [d for d in dirs if d not in ['__pycache__', '.git', 'tests']]
            for fname in files:
                if not fname.endswith('.py'):
                    continue
                path = os.path.join(root, fname)
                with open(path) as f:
                    src = f.read()
                # Skip system/venv files
                if '.cache' in path or 'venv' in path or 'site-packages' in path:
                    continue
                assert 'eval(' not in src or '# noqa' in src, f"eval() in {path}"


# ══════════════════════════════════════════════════════════════════════════════
# BRUTE FORCE PROTECTION
# ══════════════════════════════════════════════════════════════════════════════

class TestBruteForceProtection:

    def test_otp_attempt_counter_exists(self):
        """OTP brute force protection must be implemented."""
        src = read('main.py')
        assert '_otp_attempts' in src
        assert '_OTP_MAX_ATTEMPTS' in src
        assert '_OTP_LOCKOUT_SECS' in src

    def test_otp_attempts_cleared_on_success(self):
        """OTP attempts must be cleared after successful login."""
        src = read('main.py')
        assert '_otp_attempts.pop(user_id' in src

    def test_otp_attempts_incremented_on_failure(self):
        """OTP attempts must increment on PhoneCodeInvalidError."""
        src = read('main.py')
        fail_idx = src.find('PhoneCodeInvalidError')
        assert fail_idx != -1
        nearby = src[fail_idx:fail_idx + 300]
        assert '_bf_count' in nearby or '_otp_attempts' in nearby


# ══════════════════════════════════════════════════════════════════════════════
# BACKUP / RESTORE SECURITY
# ══════════════════════════════════════════════════════════════════════════════

class TestBackupSecurity:

    def test_backup_enforces_premium_limits(self):
        """Backup restore must not bypass premium source/dest limits."""
        src = read('main.py')
        assert 'free_source_limit' in src
        assert 'free_dest_limit' in src
        assert '_src_limit' in src
        assert '_dest_limit' in src

    def test_backup_validates_force_sub_schema(self):
        """Admin force_sub restore must validate it's a dict."""
        src = read('main.py')
        assert 'isinstance(_fs_val, dict)' in src


# ══════════════════════════════════════════════════════════════════════════════
# MEMORY SAFETY
# ══════════════════════════════════════════════════════════════════════════════

class TestMemorySafety:

    def test_antispam_deques_have_maxlen(self):
        """Anti-spam sliding windows must have maxlen."""
        src = read('anti_spam.py')
        assert 'deque(maxlen=' in src

    def test_db_iteration_snapshots(self):
        """Critical db.items() iterations must use list() snapshot."""
        src = read('main.py')
        assert 'list(db.items())' in src
        # Verify no bare db.items() in for loops remain
        bare = re.findall(r'for .+ in db\.items\(\)(?! *[\)])', src)
        assert len(bare) == 0, f"Bare db.items() found: {bare}"

    def test_rebalance_uses_snapshot(self):
        """rebalance_workers must use list() snapshot."""
        src = read('worker_manager.py')
        assert 'list(db.items())' in src

    def test_sqlite_wal_mode(self):
        """SQLite dedup must use WAL journal mode."""
        src = read('forward_engine.py')
        assert 'journal_mode=WAL' in src
        assert '_dedup_sqlite_lock' in src

    def test_coin_locks_have_cleanup(self):
        """_coin_locks must have cleanup to prevent memory leak."""
        src = read('task_board.py')
        assert '_cleanup_coin_locks' in src


# ══════════════════════════════════════════════════════════════════════════════
# INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════════════════════

class TestInfrastructure:

    def test_dockerignore_exists(self):
        """.dockerignore must exist."""
        path = os.path.join(os.path.dirname(__file__), '..', '.dockerignore')
        assert os.path.exists(path), ".dockerignore missing"

    def test_dockerignore_excludes_sessions(self):
        """.dockerignore must exclude session files."""
        src = read('.dockerignore')
        assert '*.session' in src
        assert 'wal.jsonl' in src
        assert '.env' in src

    def test_qrcode_in_requirements(self):
        """qrcode must be in requirements.txt."""
        src = read('requirements.txt')
        assert 'qrcode' in src

    def test_anti_sleep_is_async(self):
        """anti_sleep must use await asyncio.sleep (not blocking time.sleep)."""
        src = read('anti_sleep.py')
        assert 'await asyncio.sleep' in src
        assert 'time.sleep' not in src

    def test_no_t_dot_time(self):
        """_t.time() NameError bug must be fixed."""
        src = read('main.py')
        assert '_t.time()' not in src

    def test_railway_aligned_with_nixpacks(self):
        """railway.json startCommand must match nixpacks venv path."""
        src = read('railway.json')
        assert '/opt/venv/bin/python3' in src

    def test_no_xor_dead_code(self):
        """_xor_obfuscate dead code must be removed."""
        src = read('session_vault.py')
        assert '_xor_obfuscate' not in src
