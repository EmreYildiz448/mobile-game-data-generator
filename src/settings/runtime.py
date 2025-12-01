import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv, dotenv_values

def _prevalidate_env(path: str | None = None) -> bool:
    """
    Read .env as simple key-value pairs, validate critical relationships.
    If invalid, return False so we can skip load_dotenv() and use defaults.
    """
    env_dict = dotenv_values(dotenv_path=path)  # defaults to ".env" if None

    if not env_dict:
        # No .env file or it's empty → nothing to validate
        return True

    errors: list[str] = []

    def _parse_iso(key: str):
        v = env_dict.get(key)
        if v is None:
            return None
        try:
            return datetime.fromisoformat(v)
        except Exception:
            errors.append(f"{key} must be ISO date (YYYY-MM-DD). Got {v!r}.")
            return None
        
    def _parse_int(key: str):
        v = env_dict.get(key)
        if v is None:
            return None
        try:
            return int(v)
        except Exception:
            errors.append(f"{key} must be an integer. Got {v!r}.")
            return None
        
    # Check START_DATE <= END_DATE if both provided in .env
    start = _parse_iso("START_DATE")
    end = _parse_iso("END_DATE")
    if start and end and start > end:
        errors.append(
            f"START_DATE ({start.date()}) cannot be later than END_DATE ({end.date()})."
        )

    # Check AB_START <= AB_END if both provided
    ab_start = _parse_iso("AB_START")
    ab_end = _parse_iso("AB_END")
    if ab_start and ab_end and ab_start > ab_end:
        errors.append(
            f"AB_START ({ab_start.date()}) cannot be later than AB_END ({ab_end.date()})."
        )

    for int_key in ["NUM_ACC", "NUM_ADS", "NUM_CAMPAIGNS", "NUM_WORKERS"]:
        val = _parse_int(int_key)
        if val is not None and val == 0:
            errors.append(f"{int_key} cannot be 0. Provided: {val}")

    # If any errors, print them and reject .env
    if errors:
        print("[runtime] Ignoring .env due to configuration errors:")
        for msg in errors:
            print("  -", msg)
    # Clear stale shell variables
        for key in env_dict.keys():
            os.environ.pop(key, None)
        return False

    return True

def _apply_env_with_validation(path: str | None = None) -> None:
    """
    Run .env validation & loading at most once per OS process.

    Child worker processes spawned by ProcessPoolExecutor will inherit
    os.environ from the parent, so they don't need to re-run validation.
    """
    # If this flag is set, we've already done validation in this process
    if os.getenv("TC_ENV_VALIDATED") == "1":
        return

    # Validate .env
    if _prevalidate_env(path):
        # Safe to load .env into os.environ
        load_dotenv(dotenv_path=path)
        # you could optionally print a debug line here if you want
    # else: .env is ignored; defaults remain in effect

    # Mark as validated so subsequent imports (and child-process imports)
    # in this process won't re-run validation.
    os.environ["TC_ENV_VALIDATED"] = "1"


# Apply env once when runtime.py is first imported in this process
_apply_env_with_validation()



PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DATA_EXT_DIR = DATA_DIR / "external"
DATA_INT_DIR = DATA_DIR / "interim"
SQL_DIR = PROJECT_ROOT / "sql"
SQL_SLV_DIR = SQL_DIR / "silver"
SQL_GLD_DIR = SQL_DIR / "gold"
SQL_ANLYT_DIR = SQL_DIR / "analytics"
OUT_DIR = PROJECT_ROOT / "output"
DUCKDB_DIR = OUT_DIR / "duckdb"
REPORT_DIR = OUT_DIR / "reports"

# --- Reporting layout ---
REPORT_AB_DIR         = REPORT_DIR / "ab_test"

REPORT_ML_DIR         = REPORT_DIR / "ml"
REPORT_ML_SUMMARY_DIR = REPORT_ML_DIR / "summary"
REPORT_ML_INDIV_DIR   = REPORT_ML_DIR / "individual"

FIG_DIR = REPORT_DIR / "figures"  # (can be removed later)

DUCKDB_FILENAME = "tilecrashers.duckdb"
DUCKDB_PATH = DUCKDB_DIR / DUCKDB_FILENAME

def _b(name, default=False):
    v = os.getenv(name)
    return str(v).strip().lower() in ("1", "true", "yes", "y") if v is not None else default

def _i(name, default):
    v = os.getenv(name)
    return int(v) if v is not None else default

def _f(name, default):
    v = os.getenv(name)
    return float(v) if v is not None else default

def _s(name, default):
    v = os.getenv(name)
    return v if v is not None else default

def _dt_from_env(name, default_iso):
    """Parse ISO-8601 date or datetime string from env; fallback to given default ISO string."""
    v = os.getenv(name)
    src = v if v is not None else default_iso
    # fromisoformat handles 'YYYY-MM-DD' and 'YYYY-MM-DDTHH:MM:SS'
    return datetime.fromisoformat(src)

SEED           = _i("SEED", 42)                         # SEED
NUM_ACCOUNTS   = _i("NUM_ACC", 1000)                    # total_accounts
NUM_ADS        = _i("NUM_ADS", 20)                      # num_ads
NUM_CAMPAIGNS  = _i("NUM_CAMPAIGNS", 10)                # num_campaigns

# How many worker processes to use for event generation.
# Clamp to available CPU cores in main() — this default just avoids oversubscription if unset.
NUM_WORKERS = _i("NUM_WORKERS", 8)

START_DATE     = _dt_from_env("START_DATE", "2024-12-31")  # start_date
END_DATE       = _dt_from_env("END_DATE",   "2025-03-31")  # end_date
ANALYTICS_MAX_DATERANGE = (END_DATE - START_DATE + timedelta(days=10)).days

CAMPAIGN_START = START_DATE  # dynamic
if END_DATE.month == 12:
    CAMPAIGN_END = datetime(END_DATE.year + 1, 1, 1)
else:
    CAMPAIGN_END = datetime(END_DATE.year, END_DATE.month + 1, 1)

BASE_ARCHETYPE_WEIGHT = _i("BASE_ARCHETYPE_WEIGHT", 1)

FREE_CASUAL_MULTIPLIER        = _i("FREE_CASUAL_MULTIPLIER", 6)
FREE_REGULAR_MULTIPLIER       = _i("FREE_REGULAR_MULTIPLIER", 6)
FREE_HARDCORE_MULTIPLIER      = _i("FREE_HARDCORE_MULTIPLIER", 4)
LOWSPENDER_CASUAL_MULTIPLIER  = _i("LOWSPENDER_CASUAL_MULTIPLIER", 2)
LOWSPENDER_REGULAR_MULTIPLIER = _i("LOWSPENDER_REGULAR_MULTIPLIER", 1)
LOWSPENDER_HARDCORE_MULTIPLIER= _i("LOWSPENDER_HARDCORE_MULTIPLIER", 1)
HIGHSPENDER_REGULAR_MULTIPLIER= _i("HIGHSPENDER_REGULAR_MULTIPLIER", 1)
HIGHSPENDER_HARDCORE_MULTIPLIER= _i("HIGHSPENDER_HARDCORE_MULTIPLIER", 1)

ARCHETYPE_WEIGHTS = [
    BASE_ARCHETYPE_WEIGHT * FREE_CASUAL_MULTIPLIER,         # free_casual
    BASE_ARCHETYPE_WEIGHT * FREE_REGULAR_MULTIPLIER,        # free_regular
    BASE_ARCHETYPE_WEIGHT * FREE_HARDCORE_MULTIPLIER,       # free_hardcore
    BASE_ARCHETYPE_WEIGHT * LOWSPENDER_CASUAL_MULTIPLIER,   # lowspend_casual
    BASE_ARCHETYPE_WEIGHT * LOWSPENDER_REGULAR_MULTIPLIER,  # lowspend_regular
    BASE_ARCHETYPE_WEIGHT * LOWSPENDER_HARDCORE_MULTIPLIER, # lowspend_hardcore
    BASE_ARCHETYPE_WEIGHT * HIGHSPENDER_REGULAR_MULTIPLIER, # highspend_regular
    BASE_ARCHETYPE_WEIGHT * HIGHSPENDER_HARDCORE_MULTIPLIER # highspend_hardcore
]

SEARCH_QUERY_KEYWORDS_MIN =_i("SEARCH_QUERY_KEYWORDS_MIN", 1)
SEARCH_QUERY_KEYWORDS_MAX = _i("SEARCH_QUERY_KEYWORDS_MAX", 2 )
PEAK_HOUR_UTC = _i("PEAK_HOUR_UTC", 12)
PEAK_HOUR_AMPLITUDE = _i("PEAK_HOUR_AMPLITUDE", 8)
MAX_ORGANIC_WEIGHT = _f("MAX_ORGANIC_WEIGHT", 0.6)
FRIEND_WEIGHT_MIN = _f("FRIEND_WEIGHT_MIN", 0.05)
FRIEND_WEIGHT_MAX = _f("FRIEND_WEIGHT_MAX", 0.10)
GROWTH_FACTOR = _f("GROWTH_FACTOR", 0.25)

AD_SHOW_PROBABILITY = _f("AD_SHOW_PROBABILITY", 0.5)
AD_SHOW_PROBABILITY_INCREASE = _f("AD_SHOW_PROBABILITY_INCREASE", 0.25)
DEFAULT_AD_LENGTH = _i("DEFAULT_AD_LENGTH", 30)
REWARD_DIAMOND_AMOUNT = _i("REWARD_DIAMOND_AMOUNT", 25)
MIN_AD_WATCH_LENGTH = _i("MIN_AD_WATCH_LENGTH", 10)
REWARD_AD_SKIP_PROBABILITY = _f("REWARD_AD_SKIP_PROBABILITY", 0.05)

DEFAULT_APP_VERSION = _s("DEFAULT_APP_VERSION", "1.0.0")
AB_CONTROL_SUFFIX = _s("AB_CONTROL_SUFFIX", "a")
AB_TEST_SUFFIX = _s("AB_TEST_SUFFIX", "b")
CONTROL_VERSION = f"{DEFAULT_APP_VERSION}.{AB_CONTROL_SUFFIX}" # CONTROL_VERSION
AB_TEST_VERSION = f"{DEFAULT_APP_VERSION}.{AB_TEST_SUFFIX}"    # AB_TEST_VERSION
AB_TEST_LAUNCH_DATE = _dt_from_env("AB_START", "2025-03-03")   # AB_TEST_LAUNCH_DATE
AB_TEST_END_DATE    = _dt_from_env("AB_END",   "2025-04-14")   # AB_TEST_END_DATE
AB_TEST_ROLLOUT_DAYS = _i("AB_ROLLOUT_DAYS", 5)                # AB_TEST_ROLLOUT_DAYS
AB_TEST_TARGET_PERCENTAGE = _f("AB_TARGET_PCT", 0.20)          # AB_TEST_TARGET_PERCENTAGE
AB_MONETIZATION_EFFECT_TEST = _f("AB_MONETIZATION_EFFECT_TEST", 1.5)
AB_MONETIZATION_EFFECT_CONTROL = _f("AB_MONETIZATION_EFFECT_CONTROL", 1)
AB_CONFLICT_OS = _s("AB_CONFLICT_OS", "iOS 16.3.0")
AB_ERROR_MULTIPLIER = _f("AB_ERROR_MULTIPLIER", 10.0)

increment = AB_TEST_TARGET_PERCENTAGE / AB_TEST_ROLLOUT_DAYS
AB_TEST_DAILY_THRESHOLDS = {
    AB_TEST_LAUNCH_DATE + timedelta(days=i): round(increment * (i + 1), 4)
    for i in range(AB_TEST_ROLLOUT_DAYS)
}  # AB_TEST_DAILY_THRESHOLDS

WRITE_TO_DB    = _b("WRITE_TO_DB", False)               # WRITE_TO_DB
WRITE_TO_FILE  = _b("WRITE_TO_FILE", True)              # WRITE_TO_FILE
WRITE_TO_DUCK  = _b("WRITE_TO_DUCK", True)            
OUTPUT_FORMAT  = _s("OUTPUT_FORMAT", "csv").lower()     # fmt
_sr            = os.getenv("SAMPLE_ROWS")               # sample_rows
SAMPLE_ROWS    = int(_sr) if _sr is not None else None  # sample_rows

FREE_SPENDING_FACTOR = _f("FREE_SPENDING_FACTOR", 0)
LOWSPENDER_SPENDING_FACTOR = _f("LOWSPENDER_SPENDING_FACTOR", 0.025)
HIGHSPENDER_SPENDING_FACTOR = _f("HIGHSPENDER_SPENDING_FACTOR", 0.2)
FALLBACK_SPENDING_FACTOR = _f("FALLBACK_SPENDING_FACTOR", 0)

TERMINATING_ERROR_SUBTYPES = _s("TERMINATING_ERROR_SUBTYPES","app_crash,resource_fail,network_error").split(",")

DIFFMULT_MIN_VALUE = _i("DIFFMULT_MIN_VALUE", 5)
DIFFMULT_MAX_VALUE = _i("DIFFMULT_MAX_VALUE", 50)
# DIFFMULT_MAX_LEVEL = _i("DIFFMULT_MAX_LEVEL", 100) Dependent on level generation logic, ignore if level data is static
DIFFMULT_GROWTH_EXPONENT = _i("GROWTH_EXPONENT", 2)

MIDGAME_MIN_LEVEL = _i("MIDGAME_MIN_LEVEL", 50)
MIDGAME_MAX_LEVEL = _i("MIDGAME_MAX_LEVEL", 79)
MIDGAME_BASE_DIFFMULT = _f("MIDGAME_BASE_DIFFMULT", 1.5)
LATEGAME_MIN_LEVEL = MIDGAME_MAX_LEVEL + 1         # Subject to change if level data is made dynamic (_i("LATEGAME_MIN_LEVEL", 80))
LATEGAME_MAX_LEVEL = _i("LATEGAME_MAX_LEVEL", 100) # Subject to change if level data is made dynamic
LATEGAME_BASE_DIFFMULT = _f("LATEGAME_BASE_DIFFMULT", 3)

OP_ITEM_SUCCESS_FACTOR = _f("OP_SUCCESS_FACTOR", 1.5)
OP_COMBO_SUCCESS_FACTOR = _f("OP_COMBO_SUCCESS_FACTOR", 1.25)
UP_ITEM_SUCCESS_FACTOR = _f("UP_ITEM_SUCCESS_FACTOR", 0.67)
UP_COMBO_SUCCESS_FACTOR = _f("UP_COMBO_SUCCESS_FACTOR", 0.8)

MIN_GAP_HOURS = _i("MIN_GAP_HOURS", 4)
MAX_GAP_HOURS = _i("MAX_GAP_HOURS", 72)
MAX_SESSIONS_PER_DAY = _i("MAX_SESSIONS_PER_DAY", 6)

# General retention baseline & decay
DEFAULT_RETENTION_BASE     = _f("DEFAULT_RETENTION_BASE", 0.30)   # base_retention default (calculate_retention_probability)
PROGRESS_BONUS_PER_LEVEL   = _f("PROGRESS_BONUS_PER_LEVEL", 0.02) # 0.02 * levels_completed (calculate_retention_probability)
PROGRESS_BONUS_CAP         = _f("PROGRESS_BONUS_CAP", 0.25)       # cap for progress_bonus (calculate_retention_probability)
SPEND_DIVISOR              = _f("SPEND_DIVISOR", 100.0)           # total_spent / 100 (calculate_retention_probability)
CURRENCY_DIVISOR           = _f("CURRENCY_DIVISOR", 10000.0)      # (gold+diamonds)/10000 (calculate_retention_probability)
CURRENCY_FACTOR_CAP        = _f("CURRENCY_FACTOR_CAP", 0.10)      # cap for currency_factor (calculate_retention_probability)
INVENTORY_DIVISOR          = _f("INVENTORY_DIVISOR", 1000.0)      # inventory_value/1000 (calculate_retention_probability)
INVENTORY_FACTOR_CAP       = _f("INVENTORY_FACTOR_CAP", 0.10)     # cap for inventory_factor (calculate_retention_probability)
INVESTMENT_BONUS_CAP       = _f("INVESTMENT_BONUS_CAP", 0.20)     # cap for investment_bonus (calculate_retention_probability)
DEFAULT_DECAY_CONSTANT     = _f("DEFAULT_DECAY_CONSTANT", 0.10)   # fallback decay_constant (calculate_retention_probability)
DECAY_MULTIPLIER           = _f("DECAY_MULTIPLIER", 1.50)         # multiplies decay_penalty (calculate_retention_probability)

# Session termination shaping
DEFAULT_TERMINATION_BASE   = _f("DEFAULT_TERMINATION_BASE", 0.05) # base_termination if missing (calculate_session_termination_probability)
FATIGUE_SLOPE_PER_MIN      = _f("FATIGUE_SLOPE_PER_MIN", 0.01)    # +1% per minute (calculate_session_termination_probability)
FATIGUE_CAP                = _f("FATIGUE_CAP", 0.20)              # cap for fatigue factor (calculate_session_termination_probability)
FAIL_STREAK_STEP           = _f("FAIL_STREAK_STEP", 0.05)         # +5% per fail (calculate_session_termination_probability)
FAIL_STREAK_CAP            = _f("FAIL_STREAK_CAP", 0.20)          # cap for fail part (calculate_session_termination_probability)
SUCCESS_STREAK_STEP        = _f("SUCCESS_STREAK_STEP", 0.03)      # -3% per success (calculate_session_termination_probability)
SUCCESS_STREAK_CAP         = _f("SUCCESS_STREAK_CAP", 0.10)       # cap for success part (calculate_session_termination_probability)
SCHEDULE_SINE_AMPLITUDE    = _f("SCHEDULE_SINE_AMPLITUDE", 0.10)  # 0.1 * sin(...) (calculate_session_termination_probability)
ENGAGEMENT_COMPLETED_DELTA = _f("ENGAGEMENT_COMPLETED_DELTA", -0.05) # -5% if reward ad completed (calculate_session_termination_probability)
ENGAGEMENT_SKIPPED_DELTA   = _f("ENGAGEMENT_SKIPPED_DELTA", 0.05)    # +5% if ad skipped (calculate_session_termination_probability)

# Ad engagement fallback (when account map lacks explicit prob)
DEFAULT_AD_ENGAGEMENT_PROB = _f("DEFAULT_AD_ENGAGEMENT_PROB", 0.30)  # ad_engagement_probability default (generate_primary_loop)

# Score shaping per level outcome
SUCCESS_RANDOM_DIVISOR     = _f("SUCCESS_RANDOM_DIVISOR", 1.50)   # random.random() / 1.5 (generate_primary_loop)
MAX_SCORE_MULTIPLIER       = _f("MAX_SCORE_MULTIPLIER", 1.50)     # three_stars_floor * 1.5 (generate_primary_loop)
FAIL_SCORE_MIN_FACTOR      = _f("FAIL_SCORE_MIN_FACTOR", 0.35)    # success_score_floor * 0.35 (generate_primary_loop)
FAIL_SCORE_MAX_FACTOR      = _f("FAIL_SCORE_MAX_FACTOR", 0.95)    # success_score_floor * 0.95 (generate_primary_loop)

# Subscription logic
SUBSCRIPTION_DURATION_DAYS = _i("SUBSCRIPTION_DURATION_DAYS", 30) # +timedelta(days=30) (generate_primary_loop)

# Churn shaping
CHURN_EXPONENT             = _f("CHURN_EXPONENT", 1.50)           # (churn_count ** 1.5) (calculate_full_churn)
CHURN_REDUCTION_MULT       = _f("CHURN_REDUCTION_MULT", 5.00)     # * 5 (calculate_full_churn)
BUF_SUCCESS_STREAK_STEP    = _f("BUF_SUCCESS_STREAK_STEP", 0.05)  # +0.05 * min(success_streak, cap) (calculate_full_churn)
BUF_SUCCESS_STREAK_CAP     = _i("BUF_SUCCESS_STREAK_CAP", 10)     # cap for success_streak buffer (calculate_full_churn)
BUF_LEVEL_STEP             = _f("BUF_LEVEL_STEP", 0.02)           # +0.02 * min(level, cap) (calculate_full_churn)
BUF_LEVEL_CAP              = _i("BUF_LEVEL_CAP", 50)              # cap for level buffer (calculate_full_churn)
BUF_SPENT_STEP             = _f("BUF_SPENT_STEP", 0.10)           # +0.10 * min(spent, cap) (calculate_full_churn)
BUF_SPENT_CAP              = _i("BUF_SPENT_CAP", 100)             # cap for spent buffer (calculate_full_churn)
BUF_INVENTORY_STEP         = _f("BUF_INVENTORY_STEP", 0.02)       # +0.02 * min(inventory_size, cap) (calculate_full_churn)
BUF_INVENTORY_CAP          = _i("BUF_INVENTORY_CAP", 50)          # cap for inventory buffer (calculate_full_churn)
PENALTY_FAIL_STREAK_STEP   = _f("PENALTY_FAIL_STREAK_STEP", 0.05) # +0.05 * min(fail_streak, cap) (calculate_full_churn)
PENALTY_FAIL_STREAK_CAP    = _i("PENALTY_FAIL_STREAK_CAP", 10)    # cap for penalty (calculate_full_churn)
MAX_CHURN_REDUCTION_FRAC   = _f("MAX_CHURN_REDUCTION_FRAC", 0.50) # original_full_churn/2 (calculate_full_churn)

REFERRAL_REWARD_CHEST_ID = _s("REFERRAL_REWARD_CHEST_ID", "ch_epic")

EXEC_STAT_TESTS = _b("EXEC_STAT_TESTS", True)
EXEC_ML_TESTS = _b("EXEC_ML_TESTS", True)