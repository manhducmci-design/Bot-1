# main.py — VERSION 8.7.R10 (iqair) — full features merge
# ==============================================================================
# v8.5 = v8.6 + toàn bộ v8.7 features đã fix trong quá trình review:
#   [PERF v8.6]
#   1. check_connect cache 5 phút
#   2. Fetch M1/M5/M15 song song + cache M5/M15 theo khung
#   3. Balance updater adaptive: idle 60s / active 20s
#   4. Poll pending result trong countdown: 5s/lần
#   5. Timing log: fetch_candles / check_connect / execute_trade / zones / pa_scan
#
#   [LOGIC v8.7]
#   6. STRONG_TRUE_REVERSAL_PATTERNS — loại continuation patterns khỏi override
#      logic (fix bug 3Soldiers vừa continuation vừa reversal)
#   7. ENABLE_REGIME_RANGE_SANITY — downgrade STRONG_TREND_* xuống RANGING_WIDE
#      khi range 20 nến M5 quá hẹp so với ATR (catch case "trend label sai")
#   8. ZONE-BIAS-OVERRIDE — cho phép counter-zone-bias khi có M1 retest/
#      false_breakout xác nhận cùng hướng signal (giải deadlock zone vs trend)
#   9. Countdown dashboard — hiển thị countdown_message/deadline trên dashboard
#  10. Log khởi động bổ sung: Regime range sanity
# ==============================================================================

import asyncio
import atexit
import time
import sys
import socket
import logging
import os
import threading
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json
import math
import webbrowser
from datetime import datetime, timedelta, timezone
from trade_tracker import TradeTracker
from learning_loop import load_learning_report


if getattr(sys, "frozen", False) or "__compiled__" in globals():
    os.chdir(os.path.dirname(os.path.abspath(sys.argv[0])))

from zone_manager import ZoneManager
from config import (
    ASSET, CANDLE_TIME, CANDLE_NUMBER, EXPIRY_TIME,
    EMAIL, PASSWORD, LICENSE_EMAIL, ACCOUNT_TYPE, STRATEGY_369, ENABLE_FAKEY,
    SKIP_M5_LAST_MINUTE, LEVEL_TOLERANCE, DERIV_API_TOKEN,
)

try:
    from config import ZONE_TOLERANCE_MINOR, ZONE_TOLERANCE_MAJOR
except ImportError:
    ZONE_TOLERANCE_MINOR = 0.0002
    ZONE_TOLERANCE_MAJOR = 0.0005

SKIP_M5_FIRST_MINUTE = True # tắt mở skip nến đầu tiên M0 của nến M5

try:
    from config import ADX_THRESHOLD
except ImportError:
    ADX_THRESHOLD = 18

try:
    from core.price_validator import PriceValidator
    from config import TWELVEDATA_API_KEY, VALIDATE_CANDLE, CANDLE_TOLERANCE_PIPS
    _HAS_PV = True
except ImportError:
    _HAS_PV = False
    VALIDATE_CANDLE = False
    TWELVEDATA_API_KEY = ""
    CANDLE_TOLERANCE_PIPS = 2.0

from core.connection import connect_iq, is_asset_available
from core.data_fetcher import get_candles, get_candles_raw
from core.network_utils import call_with_timeout, set_heartbeat_fn
from core.trade_debug import log_trade_debug
# === BROKER FRAMEWORK (opt-in, zero-impact khi tắt) ===
try:
    from brokers.factory import (
        USE_BROKER_FRAMEWORK,
        create_broker,
        connect_broker as _connect_broker,
        get_broker_name,
        get_broker_display_name,
    )
    _HAS_BROKER_FRAMEWORK = True
except ImportError as _bf_err:
    _HAS_BROKER_FRAMEWORK = False
    USE_BROKER_FRAMEWORK = False
    def get_broker_name(): return "iq"
    def get_broker_display_name(name=None): return "IQ Option"
    logging.warning(f"brokers framework unavailable: {_bf_err}")

def _emit_trade_debug_event(event, scan_context=None, **details):
    context = scan_context or {}
    payload = {
        "ts": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        "run_id": context.get("run_id"),
        "scan_id": context.get("scan_id"),
        "candle_ts": context.get("candle_ts"),
        "m5_pos": context.get("m5_pos"),
        "direction": context.get("direction"),
        "pattern": context.get("pattern"),
        "patterns": context.get("patterns", []),
        "stars": context.get("stars"),
        **details,
    }
    try:
        log_trade_debug(event, **payload)
    except Exception:
        logging.exception(
            "Unable to write trade debug event %s (run_id=%s, scan_id=%s)",
            event,
            payload["run_id"],
            payload["scan_id"],
        )


def _emit_gate_event(
    scan_context,
    gate,
    outcome,
    reason,
    *,
    fallback=False,
    evaluated=True,
    **details,
):
    _emit_trade_debug_event(
        "gate_decision",
        scan_context,
        gate=gate,
        outcome=outcome,
        reason=str(reason),
        fallback=bool(fallback),
        evaluated=bool(evaluated),
        **details,
    )


from strategies.strategy_369 import (
    Strategy369Config, evaluate_369_pinbar, evaluate_369_fakey,
    trend_369, volume_confirmed,
)
from strategies.price_action import (
    pattern_M, pattern_2_rau_nguoc, pattern_3R,
    pattern_pinbar, pattern_engulfing, pattern_star,
    pattern_inside_bar, pattern_doji, pattern_railway,
    pattern_spinning_top, is_valid_signal_candle,
    evaluate_break_and_retest,
    log_candle_metrics,          # ← THÊM DÒNG NÀY

)
from strategies.filters import (
    check_gap, check_atr, check_ema, check_m5_trend,
    check_contra_m5_breakout, check_zone_state_conflict,
    check_momentum_extreme_confirmation,
)
from strategies.strat_visual_matcher import (
    CONTINUATION_PATTERN_NAMES as STRAT_CONTINUATION_PATTERNS,
    REVERSAL_PATTERN_NAMES as STRAT_REVERSAL_PATTERNS,
    match_strat_template,
)
from trading.executor import execute_trade, get_trade_result, cleanup_old_results
from trading.risk_manager import RiskManager
from trading.stats_manager import StatsManager
from trading.atr_scaler import get_atr_scaled_params, compute_middle_rr_threshold
from trading.market_structure import analyze_market_structure, market_structure_snapshot
from trading.zone_policy import (
    get_zone_direction,
    find_aligned_touched_edge_zone,
    has_minor_zone_trend_alignment,
    is_counter_edge_breakout_confirmed,
    is_countertrend_confirmation_sufficient,
    is_minor_zone_soft_pass_eligible,
    is_middle_zone_countertrend_confirmed,
    is_middle_zone_label,
)
from strategies.shadow_mode import ShadowMode, ShadowSignal
from strategies.cold_start import evaluate as cold_start_eval

try:
    from trading.knowledge_brain import KnowledgeBrain
    _HAS_KNOWLEDGE_BRAIN = True
except ImportError as _kb_err:
    _HAS_KNOWLEDGE_BRAIN = False
    logging.warning(f"knowledge_brain import failed: {_kb_err}")

try:
    from trading.momentum_analyzer import (
        detect_overextended, format_metrics, get_adaptive_momentum_cfg,
        CFG as MOMENTUM_CFG,
    )
    _HAS_MOMENTUM = True
except ImportError as _mom_err:
    _HAS_MOMENTUM = False
    logging.warning(f"momentum_analyzer import failed: {_mom_err}")

try:
    from trading.adaptive_brain import AdaptiveMetaBrain
    _HAS_META_BRAIN = True
except ImportError as _mb_err:
    _HAS_META_BRAIN = False
    logging.warning(f"adaptive_brain import failed: {_mb_err}")

from dashboard.web import (
    DashboardState,
    prepare_chart_candles,
    prepare_live_chart_candle,
    start_dashboard,
)
from licensing.client import DEFAULT_SERVER_URL, LicenseClient, LicenseLimitExceeded


# ============ CẤU HÌNH CHÍNH ============
TRADING_ASSET = "EURUSD"
OPTION_TYPE = (
    "BINARY"
    if _HAS_BROKER_FRAMEWORK
    and USE_BROKER_FRAMEWORK
    and get_broker_name() == "deriv"
    else "TURBO"
)
TRADE_EXPIRY_MINUTES = 1
TRADE_EXPIRY_SECONDS = 60
PIP_SIZE = 0.0001
M15_CANDLE_NUMBER = 60
MAX_LIVE_SCAN_START_DELAY_SECONDS = 4
MAX_LIVE_ENTRY_DELAY_SECONDS = 10
RR_RELAX_FACTOR = 0.7

ENABLE_OVERRIDE = True
ENABLE_SESSION_FILTER = True
ENABLE_REGIME_DETECT = True
ENABLE_REGIME_RANGE_SANITY = True
ENABLE_LEARNING_FILTERS = True
LEARNING_MIN_PATTERN_SAMPLES = 5

# ============ v8.7.R2: ATR-NORMALIZED CONFLUENCE ============
ENABLE_ATR_NORMALIZED_CONFLUENCE = True
ATR_NORM_LOOKBACK = 3
ATR_NORM_WEAK_THRESHOLD = 0.7
ATR_NORM_STRONG_THRESHOLD = 1.5
ATR_NORM_VERY_STRONG_THRESHOLD = 2.0

# ============ v8.7.R2: EXHAUSTION-REVERSAL ============
ENABLE_EXHAUSTION_REVERSAL = True
M15_EXTENDED_THRESHOLD = 1.5
M5_COMPRESSION_RATIO = 0.5
M15_HIGH_RELAXED_STARS = 3
M5_COMPRESSION_LOOKBACK = 5
M15_CONTAINMENT_TOLERANCE = 0.0001

REGIME_RANGE_LOOKBACK = 20
REGIME_RANGE_ATR_PERIOD = 14
REGIME_RANGE_ATR_SCALE = 0.5
REGIME_RANGE_CONFIDENCE_SCALE = 1.5

ENABLE_WEEKEND_GUARD = True
ENABLE_STALE_GUARD = True
STALE_THRESHOLD = 5
STALE_SLEEP_SECONDS = 120

# ============ CONFIRMED SWING / MARKET STRUCTURE ============
ENABLE_MARKET_STRUCTURE = True
STRUCTURE_SWING_LEFT = 3
STRUCTURE_SWING_RIGHT = 3
STRUCTURE_MIN_PROMINENCE = 0.0002
STRUCTURE_EQUAL_TOLERANCE = 0.00003
STRUCTURE_BOS_BUFFER = 0.00002
ENABLE_SWING_RETEST_CONFIRMATION = True
STRUCTURE_RETEST_TOLERANCE = 0.00015
STRUCTURE_RETEST_MAX_BARS = 5
SWING_RETEST_STAR_BONUS = 1
_market_structure_last_logged = {}

ENABLE_WEIGHTED_CONFLICT = True
CONFLICT_DIFF_MIN = 2

ENABLE_M5_M15_CONFLUENCE = True

TRADE_ASIA = True
TRADE_LATE_NY = True
ASIA_MEAN_REVERSION_MIN_ZONE = 1
LATE_NY_MIN_SETUP = 2

NO_VOTE_PATTERNS = {'InsideBar', 'Doji', 'SpinningTop'}

MARUBOZU_MIN_BODY_RATIO = 0.70
MARUBOZU_MAX_WICK_RATIO = 0.15
ENABLE_MOMENTUM_PA_PATTERN = True
MOMENTUM_PA_MIN_BODY_PIPS = 2.5
MOMENTUM_PA_MIN_BODY_RATIO = 0.60

MIN_STARS_FOR_OPPOSITE_M5         = 3
MIN_STARS_FOR_VOLUME_BYPASS       = 2
ENABLE_F2_STRICT = True
F2_STRICT_MIN_SETUP_STARS = 3
MIN_STARS_FOR_WEAK_PA             = 2
MIN_STARS_FOR_M5_M15_CONFLICT     = 2
MIN_STARS_FOR_NO_ZONE_CONFLUENCE  = 3
MIN_STARS_FOR_SIDEWAYS            = 2

PA_SCALE = {0: 0, 1: 1, 2: 2, 3: 3}

NEAR_BOTH_TF_BONUS = True
NEAR_VERY_CLOSE_PIPS = 0.5
NEAR_CLOSE_PIPS = 2.0

MIN_ENTRY_TO_OPPOSITE_ZONE_PIPS = 1.5

WEAK_PA_PATTERNS = {'Piercing', '3R'}

STRONG_REVERSAL_PATTERNS = {
    'Marubozu', 'MornStar', '3Soldiers',
    'Engulfing', 'Railway', 'Mau-M', '3Inside', 'Star',
    'BeltHold', 'BreakRetest'
}

ENABLE_SETUP_4_QUALITY_GATE = True
# Chỉ 6 patterns TOP thực sự tốt
SETUP_4_HIGH_QUALITY_PATTERNS = {
    'Star', 'MornStar', 'Mau-M', '3Inside', 'Marubozu', 'BeltHold'
}
# BỎ: | STRONG_REVERSAL_PATTERNS

# ============ CẤU HÌNH v8.0 ============
ENABLE_KNOWLEDGE_BRAIN = True
KNOWLEDGE_BRAIN_VERBOSE = True
ENABLE_COLD_START_GATE = True
COLD_START_MAX_SAMPLES = 10
COLD_START_QUALITY_FLOOR = 0.50
COLD_START_ACTIVE_QUALITY_FLOOR = 0.55
COLD_START_HARD_DISABLE = True
ENABLE_SHADOW_MODE = True
SHADOW_MAX_PER_PATTERN_PER_DAY = 30
SHADOW_CANDLE_STALE_GRACE_SECONDS = 5
SHADOW_ENABLED_PATTERNS = {
    "369-Pinbar",
    "Strat_22_BullCont", "Strat_22_BearCont",
    "Mau-M", "Marubozu", "Momentum", "Piercing",
    # NEW:
    "Strat_212_BullCont", "Strat_212_BearCont",
    "Strat_212_BullRev", "Strat_212_BearRev",
    "Strat_312_BullRev", "Strat_312_BearRev",
    "Strat_322_BullRev", "Strat_322_BearRev",
    "Strat_22_BullRev", "Strat_22_BearRev",
    "Strat_Rev13_Bull",
    "Strat_Rev122_Bull", "Strat_Rev122_Bear",
    "3R", "Tasuki", "BreakRetest",
}

# ============ CẤU HÌNH v8.1 — NEW FILTERS + KB VETO ============
ENABLE_ZONE_SUBTYPE_AWARENESS = True
ENABLE_MTF_ZONE_ENTRY_FILTER = True
ENABLE_MINOR_ZONE_TREND_OVERRIDE = True
MINOR_ZONE_TREND_OVERRIDE_MIN_STARS = 3
ENABLE_M15_ZONE_SYSTEM = True
ENABLE_PA_CORRELATION_FILTER = True
ENABLE_PA_369_CONTEXT = True
PA_369_CONTEXT_NEAR_EDGE_PIPS = 2.0
PA_369_CONTEXT_MIN_SETUP_STARS = 3
ENABLE_M15_SEVERITY_BLOCK = True
ENABLE_KB_SOFT_VETO = True

M15_HIGH_SEVERITY_STARS_REQUIRED = 4

# ============ v8.2: PA CORRELATION GROUPS ============
PA_CORRELATION_GROUPS = [
    {'Star', 'MornStar'},
    {'Engulfing', '3Inside'},
    {'BeltHold', 'Marubozu', 'Momentum'},
    {'Tweezer', 'Pinbar'},
    {'2Rau', '3R'},
    {'Mau-M'},
    {'3Soldiers'},
    {'Railway'},
    {'Tasuki'},
]

# ============ v8.3: MOMENTUM + CONTINUATION CONFIG ============
ENABLE_MOMENTUM_FILTER = True
ENABLE_CONTINUATION_DOWNGRADE = True
ENABLE_COUNTERTREND_CONFIRMATION = True
COUNTERTREND_MIN_PA_CONFIRMATIONS = 2
ENABLE_Z60_COUNTERTREND_CONFIRMATION = True
DAILY_LOSS_LIMIT = 35.0
MAX_CONSECUTIVE_LOSSES = 5
CONSECUTIVE_LOSS_PAUSE_SECONDS = 600
NEW_REGIME_PENALTY = 0.40
NEW_PULLBACK_REGIME_PENALTY = 0.65
# ============ v8.8: SESSION-AWARE F2 VOLUME ============
# Vấn đề: IQ Option cung cấp tick volume (không phải real volume).
# Ở ASIA, tick volume thấp & ổn định → filter 1.2×SMA20 fail ~50% ngẫu nhiên,
# chặn setup chất lượng cao (xem log 11:47, 11:56 trong main.py run 2026-11-25).
#
# Giải pháp:
#   1. ASIA dùng threshold 1.0×SMA20 (không yêu cầu spike)
#   2. Bypass hoàn toàn nếu setup >= 3★ HOẶC >= 3 PA đồng thuận
#   3. Session khác giữ nguyên 1.2×SMA20
#
# Rollback: đặt ENABLE_SESSION_AWARE_VOLUME = False
ENABLE_SESSION_AWARE_VOLUME = True
ASIA_VOLUME_RELAX_RATIO = 1.0        # ASIA: cần >= 1.0× SMA20
ASIA_VOLUME_BYPASS_MIN_STARS = 3     # ASIA: setup >= 3★ → bypass
ASIA_VOLUME_BYPASS_MIN_PA = 3        # ASIA: >= 3 PA đồng thuận → bypass
DEFAULT_VOLUME_RATIO = 1.2           # Session khác giữ nguyên 1.2×
# ============ v8.9: TIERED RELAXATION (Tier 1) ============
# Tier 1: reset MetaBrain + nới KB nhẹ + nới M15 soft override.
# Rollback: đặt ENABLE_* = False.
ENABLE_METABRAIN_WR20_RESET = True
METABRAIN_WR20_RESET_THRESHOLD = 40.0    # reset nếu WR20 < 40%

# KB soft veto (Tier 1B) — nới nhẹ
# Trước: 0.55 + 0.05 → veto khi posterior < 0.60
# Sau:   0.50 + 0.03 → veto khi posterior < 0.53
KB_SOFT_VETO_THRESHOLD = 0.50
KB_VETO_MARGIN = 0.03

# M15 soft override (Tier 1C) — nới nhẹ
# Trước: 0.80 → bypass chỉ khi regime_conf < 0.80
# Sau:   0.85 → bypass khi regime_conf < 0.85
M15_HIGH_SOFT_OVERRIDE_MAX_CONFIDENCE = 0.85
# ============ END v8.9 Tier 1 ============
# Continuation patterns: cần breakout context, không phải reversal thật
CONTINUATION_PATTERNS = {
    '3Soldiers', 'Railway', 'Marubozu', 'Momentum', 'Tasuki'
} | STRAT_CONTINUATION_PATTERNS
# True reversal patterns: đảo chiều thật, có ý nghĩa tại zone
TRUE_REVERSAL_PATTERNS = {
    'Engulfing', 'MornStar', 'Star', 'Mau-M',
    '3Inside', 'Tweezer', 'BeltHold', 'BreakRetest' , 'Pinbar'
} | STRAT_REVERSAL_PATTERNS

# v8.7 FIX: "Strong reversal THẬT" = strong pattern VÀ không phải continuation.
# 3Soldiers/Railway/Marubozu là pattern mạnh (weight 2) nhưng là continuation
# (đã bị CONTINUATION_DOWNGRADE -1★ nếu thiếu breakout context). Không thể
# vừa coi là continuation (downgrade) vừa coi là strong reversal (override).
STRONG_TRUE_REVERSAL_PATTERNS = (
    STRONG_REVERSAL_PATTERNS
    - CONTINUATION_PATTERNS
    - STRAT_REVERSAL_PATTERNS    # v8.9: Strat reversal không override F1
)
# Config (thêm vào block v8.1 config)
ENABLE_M15_HIGH_SOFT_OVERRIDE = True
M15_HIGH_SOFT_OVERRIDE_MIN_STARS = 4  # was 4 in v8.5, lower to 3



# ============ LOGGING ============
logging.getLogger("iqair").setLevel(logging.WARNING)
logging.getLogger("websocket").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

LOG_FILE = os.path.join("logs", "bot_trading.log")
if not os.path.exists("logs"):
    os.makedirs("logs")
root_logger = logging.getLogger()
if not root_logger.handlers:
    logging.basicConfig(
        filename=LOG_FILE, level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8',
    )


def log_msg(msg: str, level: str = "info") -> None:
    if sys.stdout is not None and not isinstance(sys.stdout, _LoggerStream):
        print(msg)
    if level == "trade":
        logging.info(msg)
    else:
        getattr(logging, level, logging.info)(msg)


class _LoggerStream:
    def __init__(self, level: int) -> None:
        self.level = level
        self._buffer = ""
        self._lock = threading.Lock()

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError("Log stream accepts text only.")
        with self._lock:
            self._buffer += text
            while "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
                if line.strip():
                    logging.log(self.level, "%s", line.rstrip())
        return len(text)

    def flush(self) -> None:
        with self._lock:
            if self._buffer.strip():
                logging.log(self.level, "%s", self._buffer.rstrip())
            self._buffer = ""

    def isatty(self) -> bool:
        return False

    @property
    def encoding(self) -> str:
        return "utf-8"


def _configure_windowed_logging() -> None:
    if sys.stdout is None:
        sys.stdout = _LoggerStream(logging.INFO)
    if sys.stderr is None:
        sys.stderr = _LoggerStream(logging.ERROR)


# ============ HELPERS ============
def _count_independent_pa(pa_names):
    if not pa_names:
        return 0
    remaining = set(pa_names)
    groups_found = 0
    for group in PA_CORRELATION_GROUPS:
        if remaining & group:
            groups_found += 1
            remaining -= group
    groups_found += len(remaining)
    return groups_found


def _sum_independent_vote_weights(weighted_signals):
    standalone_total = 0.0
    grouped_weights = {}
    for signal, weight in weighted_signals:
        name, _direction, source = signal
        if source != 'PA' or name == 'BreakRetest':
            standalone_total += weight
            continue

        group_index = next(
            (index for index, group in enumerate(PA_CORRELATION_GROUPS) if name in group),
            None,
        )
        key = ('group', group_index) if group_index is not None else ('pattern', name)
        grouped_weights[key] = max(grouped_weights.get(key, 0.0), weight)

    return standalone_total + sum(grouped_weights.values())


def _is_edge_zone(touched_zones):
    for lbl, _ltype, _zval in touched_zones:
        lbl_lower = str(lbl).lower()
        if 'z30' in lbl_lower or 'z90' in lbl_lower:
            return True
    return False


def _get_m15_severity(trend, sideways, t15, s15, action):
    if t15 == 0 or s15:
        return 'none'
    if (t15 == 1 and action == 'put') or (t15 == -1 and action == 'call'):
        return 'high'
    return 'none'


def _has_setup_4_quality_pattern(pattern_names):
    return any(name in SETUP_4_HIGH_QUALITY_PATTERNS for name in pattern_names)


def _should_downgrade_setup_4(setup_stars, pattern_names):
    return (
        ENABLE_SETUP_4_QUALITY_GATE
        and setup_stars == 4
        and not _has_setup_4_quality_pattern(pattern_names)
    )


def _get_swing_retest_action(market_structure, m5_trend):
    if not ENABLE_SWING_RETEST_CONFIRMATION:
        return None
    m1_structure = market_structure.get("M1", {})
    structure_event = m1_structure.get("structure_event")
    if structure_event == "BULLISH_BOS_RETEST" and m5_trend in (0, 1):
        return "call"
    if structure_event == "BEARISH_BOS_RETEST" and m5_trend in (0, -1):
        return "put"
    return None
def _get_m1_flip_override(market_structure, consensus_action):
    """v8.9: M1 BOS_RETEST = trend flip confirmation.

    M5 EMA20 lag ~100 phút → không kịp flip khi M1 bounce.
    M1 BOS + RETEST là xác nhận đủ mạnh để bypass counter-M5 gate.

    Khác với _get_swing_retest_action (dùng cho alignment voting tại line 3865):
    hàm này CHỈ dùng để bypass COUNTERTREND gate.
    """
    if not ENABLE_SWING_RETEST_CONFIRMATION:
        return False
    m1_structure = market_structure.get("M1", {})
    structure_event = m1_structure.get("structure_event")
    if structure_event == "BULLISH_BOS_RETEST" and consensus_action == "call":
        return True
    if structure_event == "BEARISH_BOS_RETEST" and consensus_action == "put":
        return True
    return False

def _is_shadow_market_available(asset_status, weekend_observation, asset_check_enabled):
    return not weekend_observation and (
        not asset_check_enabled or asset_status == "OPEN"
    )


def _extract_fresh_shadow_price(candles, now=None):
    if not isinstance(candles, list) or not candles:
        return None
    candle = candles[-1]
    if not isinstance(candle, dict):
        return None
    try:
        price = float(candle["close"])
        candle_timestamp = float(candle["from"])
        observed_at = time.time() if now is None else float(now)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None

    age = observed_at - candle_timestamp
    if (
        not math.isfinite(price)
        or price <= 0
        or not math.isfinite(candle_timestamp)
        or not math.isfinite(observed_at)
        or age < -SHADOW_CANDLE_STALE_GRACE_SECONDS
        or age > CANDLE_TIME + SHADOW_CANDLE_STALE_GRACE_SECONDS
    ):
        return None
    return price


def _get_learning_blacklist(report, min_samples):
    blacklist = {}
    for item in report['blacklist']:
        if not isinstance(item, dict):
            raise ValueError("learning blacklist entries must be objects")
        pattern = item.get('pattern')
        sample_count = item.get('n')
        win_rate = item.get('wr')
        if (
            not isinstance(pattern, str)
            or not pattern.strip()
            or isinstance(sample_count, bool)
            or not isinstance(sample_count, int)
            or isinstance(win_rate, bool)
            or not isinstance(win_rate, (int, float))
            or not math.isfinite(win_rate)
            or not 0 <= win_rate <= 1
        ):
            raise ValueError("learning blacklist entry has invalid pattern, n, or wr")
        if sample_count >= min_samples:
            blacklist[pattern] = {'n': sample_count, 'wr': float(win_rate)}
    return blacklist


def _get_learning_report_path():
    bundle_dir = getattr(sys, "_MEIPASS", None)
    return Path(bundle_dir or Path.cwd()) / "learning_report.json"


def _score_setup(pa_effective_count, is_369_present, zone_stars, has_br):
    pa_score = PA_SCALE.get(min(pa_effective_count, 3), 0)
    pa369_score = int(
        is_369_present and (pa_effective_count >= 1 or zone_stars >= 1)
    )
    setup_stars = min(pa_score + pa369_score + zone_stars, 4)

    if has_br:
        setup_stars = max(setup_stars, 3)
        setup_reason = f"BreakRetest (minimum 3*; scored {setup_stars}*)"
    else:
        parts = []
        if pa_effective_count:
            parts.append(f"PAx{pa_effective_count}={pa_score}*")
        if is_369_present:
            parts.append("369")
        if zone_stars:
            parts.append(f"zone{zone_stars}*")
        setup_reason = " + ".join(parts) if parts else "no signal"
    return setup_stars, setup_reason


def _get_zone_stars_early(zone, current_from_ts, log_tracker):
    touched = zone.get('touched_zones', [])
    zone_is_edge = _is_edge_zone(touched)
    m1_at = zone.get('at_m1_zone', zone.get('at_zone_minor', False))
    m5_at = zone.get('at_m5_zone', zone.get('at_zone_major', False))

    if ENABLE_ZONE_SUBTYPE_AWARENESS:
        stars = 0
        if m1_at and zone_is_edge:
            stars = 1
        if m1_at and m5_at and zone_is_edge:
            stars = 2
        if m1_at and not zone_is_edge:
            if log_tracker.get('candle_ts') != current_from_ts:
                log_tracker['candle_ts'] = current_from_ts
                log_msg(
                    "      ⚠️ Middle zone touched (z60 not counted as zone*)",
                    "trade"
                )
    else:
        stars = 0
        if m1_at:
            stars = 1
        if m1_at and m5_at:
            stars = 2

    return stars, zone_is_edge, m1_at, m5_at


def _has_valid_breakout_context(zone, action):
    event = zone.get('zone_event') or {}
    expected_direction = 'up' if action == 'call' else 'down'
    return (
        str(event.get('type', '')).lower() == 'breakout'
        and str(event.get('direction', '')).lower() == expected_direction
    )


# ============ v8.7.R2: ATR-NORMALIZED HELPERS ============
def _get_trend_strength(df, lookback=None):
    if lookback is None:
        lookback = ATR_NORM_LOOKBACK
    if isinstance(lookback, bool) or not isinstance(lookback, int) or lookback < 1:
        return 0.0
    if df is None or len(df) < lookback + 1:
        return 0.0

    atr = calculate_atr_simple(df, 14)
    if atr <= 0:
        return 0.0

    try:
        closes = [float(value) for value in df['close'].iloc[-(lookback + 1):].values]
        move = abs(closes[-1] - closes[0])
        return move / atr
    except (KeyError, TypeError, ValueError, IndexError, AttributeError):
        return 0.0


def _is_m15_extended(df_m15, threshold=None):
    if threshold is None:
        threshold = M15_EXTENDED_THRESHOLD
    if (
        isinstance(threshold, bool)
        or not isinstance(threshold, (int, float))
        or not math.isfinite(threshold)
        or threshold < 0
    ):
        return False
    if df_m15 is None or len(df_m15) < 2:
        return False

    atr15 = calculate_atr_simple(df_m15, 14)
    if atr15 <= 0:
        return False

    try:
        last = df_m15.iloc[-1]
        move = abs(float(last['close']) - float(last['open']))
        return (move / atr15) > threshold
    except (KeyError, TypeError, ValueError, IndexError, AttributeError):
        return False


def _is_m5_compressed_in_m15(df_m5, df_m15, lookback=None):
    if lookback is None:
        lookback = M5_COMPRESSION_LOOKBACK
    if isinstance(lookback, bool) or not isinstance(lookback, int) or lookback < 1:
        return False, 0.0
    if df_m5 is None or len(df_m5) < lookback:
        return False, 0.0
    if df_m15 is None or len(df_m15) < 1:
        return False, 0.0

    try:
        m15_last = df_m15.iloc[-1]
        m15_high = float(m15_last['high'])
        m15_low = float(m15_last['low'])
        m15_range = m15_high - m15_low
        if m15_range <= 0:
            return False, 0.0

        m5_recent = df_m5.iloc[-lookback:]
        m5_high = float(m5_recent['high'].max())
        m5_low = float(m5_recent['low'].min())
        m5_range = m5_high - m5_low
        inside = (
            m5_low >= m15_low - M15_CONTAINMENT_TOLERANCE
            and m5_high <= m15_high + M15_CONTAINMENT_TOLERANCE
        )
        ratio = m5_range / m15_range
        return inside and ratio < M5_COMPRESSION_RATIO, ratio
    except (KeyError, TypeError, ValueError, IndexError, AttributeError):
        return False, 0.0


# ============ TRADE COUNTER ============
_trades_today = {'count': 0, 'date': datetime.now().date()}
_COUNTER_FILE = os.path.join("logs", "trade_counter.json")


def _load_counter():
    try:
        if not os.path.exists(_COUNTER_FILE):
            return
        with open(_COUNTER_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        date_str = data.get('date')
        if date_str:
            try:
                _trades_today['date'] = datetime.fromisoformat(date_str).date()
            except (ValueError, TypeError):
                _trades_today['date'] = datetime.now().date()
        _trades_today['count'] = int(data.get('count', 0))
    except Exception as e:
        logging.warning(f"[counter] Unable to load: {e}")


def _save_counter():
    try:
        with open(_COUNTER_FILE, 'w', encoding='utf-8') as f:
            json.dump({'date': _trades_today['date'].isoformat(),
                       'count': _trades_today['count']}, f)
    except Exception as e:
        logging.warning(f"[counter] Unable to save: {e}")


def _get_trades_today():
    today = datetime.now().date()
    if _trades_today['date'] != today:
        _trades_today['date'] = today
        _trades_today['count'] = 0
        _save_counter()
    return _trades_today['count']


def _increment_trades_today():
    _get_trades_today()
    _trades_today['count'] += 1
    _save_counter()


# ============ CONFIG TIMEOUTS ============
NEWS_RANGE_THRESHOLD = 0.0025
NEWS_LOOKBACK_CANDLES = 3
NEWS_COOLDOWN_ENABLED = True

BALANCE_IDLE_INTERVAL = 60
BALANCE_PENDING_INTERVAL = 20
BALANCE_ACTIVE_WINDOW = 120
BALANCE_LOG_THRESHOLD = 0.01
TRADE_LOCK_TIMEOUT = 10

TIMEOUT_GET_BALANCE = 12
TIMEOUT_CHECK_CONNECT = 5
TIMEOUT_GET_CANDLES = 10
TIMEOUT_EXECUTE_TRADE = 10
CHART_LIVE_UPDATE_INTERVAL = 15

CONNECT_CHECK_INTERVAL = 300
COUNTDOWN_POLL_INTERVAL = 5

ENABLE_ASSET_CHECK = True
ASSET_CHECK_TIMEOUT = 8
ASSET_COOLDOWN_ON_FAILURE = 180

ENABLE_M15_TREND = True
CONNECT_WAIT_LOG_INTERVAL = 30
SOCKET_DEFAULT_TIMEOUT = 10

_iq_holder = {'iq': None}
_stop_event = threading.Event()
_last_bal = {'value': None}
_balance_request_lock = threading.Lock()
_BALANCE_BUSY = object()

_knowledge_brain = None
_meta_brain = None
_balance_unavailable = False

# ============ PERFORMANCE STATE ============
_connect_cache = {'last_check': 0.0, 'last_ok': True}
_candle_cache = {
    'm5':  {'bucket': -1, 'df': None},
    'm15': {'bucket': -1, 'df': None},
}
_countdown_poll = {'last': 0.0}
_phase_times = {}
_stats_holder = {'stats': None}
_risk_mgr_holder = {'manager': None}
_tracker_holder = {'tracker': None}
_pending_results_lock = threading.RLock()
_last_trade_time = {'ts': 0.0}
_latency_history = {
    'fetch_candles': [],
    'scan_inputs_parallel': [],
}
_latency_p95_counter = {'n': 0}


# ============ TIMING HELPERS ============
class _phase:
    """Context manager đo thời gian từng phase. Cộng dồn vào _phase_times."""
    __slots__ = ('label', 't0')

    def __init__(self, label):
        self.label = label
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *args):
        dt = (time.perf_counter() - self.t0) * 1000
        _phase_times[self.label] = _phase_times.get(self.label, 0.0) + dt


def _flush_phase_times():
    if _phase_times:
        # --- rolling buffer ---
        for _key in _latency_history:
            if _key in _phase_times:
                _latency_history[_key].append(_phase_times[_key])
                if len(_latency_history[_key]) > 30:
                    _latency_history[_key].pop(0)

        parts = " | ".join(
            f"{k}={v:.0f}ms"
            for k, v in sorted(_phase_times.items(), key=lambda x: -x[1])[:8]
        )
        log_msg(f"      ⏱️ cycle: {parts}", "trade")
        _phase_times.clear()

        # --- p50/p95 mỗi 10 cycle (~10 phút) ---
        _latency_p95_counter['n'] += 1
        if _latency_p95_counter['n'] % 10 == 0:
            stats_parts = []
            for _key, _samples in _latency_history.items():
                if len(_samples) >= 5:
                    _s = sorted(_samples)
                    _p50 = _s[len(_s) // 2]
                    _p95 = _s[min(int(len(_s) * 0.95), len(_s) - 1)]
                    stats_parts.append(
                        f"{_key} p50/p95={_p50:.0f}/{_p95:.0f}ms (n={len(_s)})"
                    )
            if stats_parts:
                log_msg(f"      📊 latency: " + " | ".join(stats_parts), "trade")


# ============ CACHED CHECK_CONNECT ============
def _is_connected_cached(iq):
    now = time.time()
    if now - _connect_cache['last_check'] < CONNECT_CHECK_INTERVAL:
        return _connect_cache['last_ok']
    ok = call_with_timeout(
        iq.check_connect, timeout=TIMEOUT_CHECK_CONNECT,
        default=False, name="check_connect")
    _connect_cache['last_check'] = now
    _connect_cache['last_ok'] = bool(ok)
    return bool(ok)


def _invalidate_candle_cache():
    _candle_cache['m5'] = {'bucket': -1, 'df': None}
    _candle_cache['m15'] = {'bucket': -1, 'df': None}


def _invalidate_connection_cache():
    _connect_cache['last_check'] = 0.0


def _closed_candles(frame, timeframe_seconds, now=None):
    if frame is None:
        return None
    if timeframe_seconds <= 0:
        raise ValueError("Candle timeframe must be positive.")
    if 'from' not in frame.columns:
        return frame.iloc[0:0].copy()

    now = time.time() if now is None else float(now)
    return frame.loc[
        frame['from'] + timeframe_seconds <= now
    ].reset_index(drop=True)


# ============ PARALLEL CANDLE FETCH ============
def _fetch_candles_parallel(iq):
    now = time.time()
    m5_bucket = int(now // 300)
    m15_bucket = int(now // 900)

    fut_m5 = None
    fut_m15 = None

    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_m1 = ex.submit(
            call_with_timeout, get_candles, iq, TRADING_ASSET, CANDLE_TIME,
            CANDLE_NUMBER, timeout=TIMEOUT_GET_CANDLES, default=None, name="M1")

        if _candle_cache['m5']['bucket'] == m5_bucket and _candle_cache['m5']['df'] is not None:
            pass
        else:
            fut_m5 = ex.submit(
                call_with_timeout, get_candles, iq, TRADING_ASSET, 300, 60,
                timeout=TIMEOUT_GET_CANDLES, default=None, name="M5")

        need_m15 = (
            ENABLE_M15_TREND
            or ENABLE_M15_ZONE_SYSTEM
            or ENABLE_MARKET_STRUCTURE
        )
        if need_m15:
            if _candle_cache['m15']['bucket'] == m15_bucket and _candle_cache['m15']['df'] is not None:
                pass
            else:
                fut_m15 = ex.submit(
                    call_with_timeout, get_candles, iq, TRADING_ASSET, 900,
                    M15_CANDLE_NUMBER,
                    timeout=TIMEOUT_GET_CANDLES, default=None, name="M15")

        df = fut_m1.result()
        df_m5 = fut_m5.result() if fut_m5 else _candle_cache['m5']['df']
        if need_m15:
            df_m15 = fut_m15.result() if fut_m15 else _candle_cache['m15']['df']
        else:
            df_m15 = None

    fetched_at = time.time()
    if fut_m5 and df_m5 is not None:
        df_m5 = _closed_candles(df_m5, 300, fetched_at)
    if fut_m5 and df_m5 is not None:
        _candle_cache['m5'] = {'bucket': m5_bucket, 'df': df_m5}
    if fut_m15 and df_m15 is not None:
        df_m15 = _closed_candles(df_m15, 900, fetched_at)
    if fut_m15 and df_m15 is not None:
        _candle_cache['m15'] = {'bucket': m15_bucket, 'df': df_m15}

    return df, df_m5, df_m15


def _run_timed_phase(label, callback, *args, **kwargs):
    with _phase(label):
        return callback(*args, **kwargs)


def _fetch_scan_inputs(iq):
    """Fetch balance and market candles concurrently for the next scan."""
    with _phase("scan_inputs_parallel"):
        with ThreadPoolExecutor(max_workers=2) as executor:
            balance_future = executor.submit(
                _run_timed_phase,
                "scan_balance",
                _get_balance,
                iq,
                default=0.0,
                name="scan_balance",
            )
            candles_future = executor.submit(
                _run_timed_phase,
                "fetch_candles",
                _fetch_candles_parallel,
                iq,
            )
            try:
                balance = balance_future.result()
            except Exception:
                logging.exception("Unable to read scan balance")
                balance = 0.0
            candles = candles_future.result()
    return balance, candles


def _chart_live_updater_thread(dashboard):
    while not _stop_event.is_set():
        cycle_started = time.monotonic()
        iq = _iq_holder.get("iq")
        live_candle = None
        if iq is not None:
            candles = call_with_timeout(
                get_candles_raw,
                iq,
                TRADING_ASSET,
                CANDLE_TIME,
                2,
                time.time(),
                timeout=TIMEOUT_GET_CANDLES,
                default=None,
                name="chart_live",
            )
            live_candle = prepare_live_chart_candle(candles, CANDLE_TIME)

        dashboard_updates = {"forming_candle": live_candle}
        if live_candle is not None:
            dashboard_updates["last_price"] = live_candle["close"]
        dashboard.update(**dashboard_updates)

        remaining = max(
            0.0,
            CHART_LIVE_UPDATE_INTERVAL - (time.monotonic() - cycle_started),
        )
        if _stop_event.wait(remaining):
            return


def _get_balance(iq, default=None, name="get_balance", busy_default=None,
                 retries=1, retry_delay=1.0):
    for attempt in range(retries):
        if not _balance_request_lock.acquire(blocking=False):
            return default if busy_default is None else busy_default
        try:
            val = call_with_timeout(
                iq.get_balance,
                timeout=TIMEOUT_GET_BALANCE,
                default=None,
                name=f"{name}_try{attempt+1}",
            )
        finally:
            _balance_request_lock.release()

        if val is None:
            if attempt < retries - 1:
                logging.warning(
                    f"[{name}] attempt {attempt+1}/{retries} failed (timeout), "
                    f"retry in {retry_delay}s"
                )
                time.sleep(retry_delay)
            continue

        try:
            bal_f = float(val)
            if bal_f >= 0:
                return bal_f
        except (TypeError, ValueError):
            pass

    logging.error(f"[{name}] all {retries} attempts failed; returning default={default}")
    return default


_countdown_active = {'flag': False}
_countdown_result_callback = None
_last_processed_candle_from = {'ts': None}
_asset_cooldown = {}
_asset_status_cache = {'status': None, 'timestamp': 0}
ASSET_CACHE_TTL = 30
_stale_tracker = {'ohlc': None, 'count': 0}
_regime_tracker = {'last_stable': None}
_middle_zone_log_tracker = {'candle_ts': None}


# ============ COOLDOWN ============
def _set_asset_cooldown(asset, seconds):
    _asset_cooldown[asset] = time.time() + seconds
    end_str = datetime.fromtimestamp(_asset_cooldown[asset]).strftime('%H:%M:%S')
    log_msg(f"      Asset cooldown: {asset} paused for {seconds}s (until {end_str})", "warning")


def _is_asset_in_cooldown(asset):
    exp = _asset_cooldown.get(asset)
    if not exp:
        return False, 0
    if time.time() < exp:
        return True, int(exp - time.time())
    _asset_cooldown.pop(asset, None)
    return False, 0


# ============ HEARTBEAT ============
WATCHDOG_TIMEOUT = 150
_last_heartbeat = {'ts': time.time()}


def _heartbeat():
    _last_heartbeat['ts'] = time.time()


set_heartbeat_fn(_heartbeat)


def _watchdog_thread(dashboard):
    log_msg(f"Watchdog started (timeout {WATCHDOG_TIMEOUT}s)")
    while not _stop_event.is_set():
        try:
            elapsed = time.time() - _last_heartbeat['ts']
            if elapsed > WATCHDOG_TIMEOUT:
                log_msg(f"WATCHDOG: main thread stalled {elapsed:.0f}s - exit", "error")
                os._exit(1)
        except Exception:
            pass
        _stop_event.wait(15)
    log_msg("Watchdog stopped")


def _balance_updater_thread(dashboard):
    log_msg(
        f"Balance updater started (adaptive: idle={BALANCE_IDLE_INTERVAL}s, "
        f"active={BALANCE_PENDING_INTERVAL}s)"
    )
    balance_timeout_count = [0]
    global _balance_unavailable

    while not _stop_event.is_set():
        wait_interval = BALANCE_IDLE_INTERVAL
        try:
            has_pending = False
            stats_now = _stats_holder['stats']
            if stats_now is not None:
                try:
                    has_pending = bool(stats_now.get_ready_pending())
                except Exception:
                    has_pending = False

            recent_trade = (time.time() - _last_trade_time['ts']) < BALANCE_ACTIVE_WINDOW

            iq = _iq_holder.get('iq')
            if iq is None:
                _stop_event.wait(BALANCE_IDLE_INTERVAL)
                continue

            ping_started = time.perf_counter()
            bal = _get_balance(iq, default=None, name="balance_updater",
                               busy_default=_BALANCE_BUSY)

            if bal is _BALANCE_BUSY:
                pass
            elif bal is not None and bal > 0:
                ping_latency_ms = round((time.perf_counter() - ping_started) * 1000)
                ping_quality = ("GOOD" if ping_latency_ms < 100
                                else "MODERATE" if ping_latency_ms <= 250
                                else "SLOW")
                bal_f = float(bal)
                dashboard.update(
                    current_balance=bal_f, ping_latency_ms=ping_latency_ms,
                    ping_quality=ping_quality, ping_connected=True,
                )
                if (not _countdown_active['flag']
                        and _last_bal['value'] is not None
                        and abs(bal_f - _last_bal['value']) > BALANCE_LOG_THRESHOLD):
                    delta = bal_f - _last_bal['value']
                    log_msg(
                        f"[BALANCE] ${_last_bal['value']:.2f} → ${bal_f:.2f} "
                        f"(delta={delta:+.2f})",
                        "trade"
                    )
                _last_bal['value'] = bal_f
                balance_timeout_count[0] = 0

                if _balance_unavailable:
                    log_msg("✅ [BALANCE] Recovered — trading unblocked", "trade")
                    _balance_unavailable = False
            else:
                balance_timeout_count[0] += 1
                dashboard.update(
                    ping_latency_ms=None, ping_quality="NO RESPONSE",
                    ping_connected=False,
                )
                if balance_timeout_count[0] in (5, 20, 50):
                    log_msg(
                        f"      ⚠️ Balance timeout #{balance_timeout_count[0]}",
                        "warning"
                    )
                if balance_timeout_count[0] >= 3:
                    if not _balance_unavailable:
                        log_msg(
                            "⚠️ [BALANCE] 3 consecutive failures — blocking trades",
                            "warning"
                        )
                    _balance_unavailable = True

            wait_interval = (
                BALANCE_PENDING_INTERVAL
                if (has_pending or recent_trade)
                else BALANCE_IDLE_INTERVAL
            )
        except Exception:
            logging.exception("balance updater exception")
            dashboard.update(ping_latency_ms=None, ping_quality="NO RESPONSE",
                             ping_connected=False)
            wait_interval = BALANCE_IDLE_INTERVAL

        _stop_event.wait(wait_interval)

    log_msg("Balance updater stopped")


# ============ COUNTDOWN ============
def countdown(wait_seconds, message="Waiting", dashboard=None):
    _countdown_active['flag'] = True
    show_stdout_countdown = (
        sys.stdout is not None and not isinstance(sys.stdout, _LoggerStream)
    )
    try:
        if dashboard is not None:
            dashboard.update(
                countdown_message=message,
                countdown_deadline=time.time() + wait_seconds,
            )
        for i in range(wait_seconds, 0, -1):
            _heartbeat()
            if dashboard is not None and not dashboard.is_running():
                return False
            if _countdown_result_callback is not None:
                _countdown_result_callback()
            if show_stdout_countdown:
                sys.stdout.write(
                    f"\r[{datetime.now().strftime('%H:%M:%S')}] "
                    f"{message}... {i}s left   "
                )
                sys.stdout.flush()
            if _stop_event.wait(1):
                return False
        if show_stdout_countdown:
            sys.stdout.write("\r" + " " * 80 + "\r")
            sys.stdout.flush()
        return True
    finally:
        if dashboard is not None:
            dashboard.update(countdown_message=None, countdown_deadline=None)
        _countdown_active['flag'] = False


def wait_until_next_minute(
    dashboard=None,
    buffer=1,
    *,
    mark_signal_skip=True,
    signal_reason=None,
):
    if dashboard is not None and mark_signal_skip:
        dashboard.update(
            current_signal="SKIP",
            current_signal_reason=(
                signal_reason or "No trade placed in this scan; check the scan log."
            ),
        )
    now = datetime.now()
    next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    wait = int((next_min - now).total_seconds()) + buffer
    return countdown(max(1, wait), "Waiting for next candle", dashboard)


def _m5_position_for_scan_time(scan_started_at):
    return int(scan_started_at.timestamp() // CANDLE_TIME) % 5


def _is_live_entry_window_open(scan_started_at, current_time):
    scan_minute = scan_started_at.replace(second=0, microsecond=0)
    current_minute = current_time.replace(second=0, microsecond=0)
    return (
        scan_minute == current_minute
        and scan_started_at.second <= MAX_LIVE_SCAN_START_DELAY_SECONDS
        and current_time.second <= MAX_LIVE_ENTRY_DELAY_SECONDS
    )


def _should_skip_live_trade_for_m5_position(position):
    if OPTION_TYPE == "TURBO" and TRADE_EXPIRY_SECONDS <= 60:
        return False
    return (
        (SKIP_M5_FIRST_MINUTE and position == 0)
        or (SKIP_M5_LAST_MINUTE and position == 4)
    )


def _can_bypass_f2_volume(setup_stars, zone_stars, pattern_names, has_br,
                          session=None):
    """Kiểm tra setup có đủ mạnh để bypass F2 volume gate không.

    v8.8: thêm session-aware logic cho ASIA.
      - session=None → hành vi cũ (backward compat, chỗ gọi cũ vẫn chạy)
      - session='ASIA' + ENABLE_SESSION_AWARE_VOLUME → bypass nếu 3★+ hoặc 3+ PA
      - session khác → hành vi cũ
    """
    # v8.8: ASIA session-aware bypass
    if (
        ENABLE_SESSION_AWARE_VOLUME
        and session == 'ASIA'
    ):
        if setup_stars >= ASIA_VOLUME_BYPASS_MIN_STARS:
            return True
        if len(pattern_names) >= ASIA_VOLUME_BYPASS_MIN_PA:
            return True

    # Hành vi cũ (giữ nguyên cho mọi session khác + backward compat)
    if not ENABLE_F2_STRICT:
        return setup_stars >= MIN_STARS_FOR_VOLUME_BYPASS
    return (
        setup_stars >= F2_STRICT_MIN_SETUP_STARS
        and zone_stars >= 1
        and (
            has_br
            or any(name in STRONG_REVERSAL_PATTERNS for name in pattern_names)
        )
    )

def _evaluate_f2_volume(df, session, setup_stars, zone_stars,
                       pattern_names, has_br, min_ratio_default=1.2):
    """Đánh giá toàn bộ F2 volume gate (v8.8).

    min_ratio_default: ngưỡng volume cho session non-ASIA. Caller truyền
                       cfg369.MinVolumeRatio để giữ nguyên behavior gốc.

    Fail-open (passed=True) khi:
      - df rỗng/invalid
      - volume_confirmed raise exception
      - bypass rules khớp

    Fail-closed (passed=False) khi:
      - không bypass được VÀ volume < threshold
    """
    if df is None or len(df) < 1:
        return True, "F2 skipped: no candle data (fail-open)"

    # 1) Bypass check (đủ mạnh để bỏ qua volume check)
    if _can_bypass_f2_volume(setup_stars, zone_stars, pattern_names, has_br,
                             session=session):
        return True, (
            f"bypass (setup={setup_stars}★, "
            f"zone={zone_stars}★, "
            f"PA={len(pattern_names)}, "
            f"session={session})"
        )

    # 2) Threshold theo session
    if ENABLE_SESSION_AWARE_VOLUME and session == 'ASIA':
        ratio = ASIA_VOLUME_RELAX_RATIO
        ratio_tag = f"ASIA {ratio:.1f}×"
    else:
        ratio = min_ratio_default
        ratio_tag = f"default {ratio:.2f}×"

    # 3) Volume check
    try:
        confirmed = volume_confirmed(
            df, len(df) - 1, period=20, min_ratio=ratio
        )
    except Exception as exc:
        return True, f"F2 error, fail-open: {type(exc).__name__}: {exc}"

    if confirmed:
        return True, f"volume ≥ {ratio_tag} SMA20"
    return False, f"volume < {ratio_tag} SMA20"

def connect_iq_with_heartbeat(**kwargs):
    _orig = socket.getdefaulttimeout()
    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)
    _heartbeat()

    result = {'done': False, 'iq': None, 'exception': None}
    t_start = time.time()
    next_wait_log = t_start + CONNECT_WAIT_LOG_INTERVAL

    def _run():
        try:
            if _HAS_BROKER_FRAMEWORK and USE_BROKER_FRAMEWORK:
                broker_name = get_broker_name()
                logging.info("[broker] using adapter: %s (%s)",
                             broker_name, get_broker_display_name(broker_name))
                broker = create_broker(broker_name)
                _connect_broker(broker, **kwargs)
                result['iq'] = broker
            else:
                result['iq'] = connect_iq(**kwargs)
        except Exception as e:
            result['exception'] = e
        finally:
            result['done'] = True

    t = threading.Thread(target=_run, daemon=True, name="ConnectWorker")
    t.start()
    while not result['done']:
        _heartbeat()
        t.join(timeout=1)
        now = time.time()
        if now >= next_wait_log:
            logging.warning("IQ Option connection pending after %ss", int(now - t_start))
            next_wait_log = now + CONNECT_WAIT_LOG_INTERVAL

    socket.setdefaulttimeout(_orig)
    _heartbeat()
    if result['exception']:
        raise result['exception']
    return result['iq']


# ============ CHECK PENDING ============
def check_pending_results(iq, stats, risk_mgr):
    with _pending_results_lock:
        return _check_pending_results_impl(iq, stats, risk_mgr)


def _check_pending_results_impl(iq, stats, risk_mgr):
    has_new = False
    for t in stats.get_ready_pending():
        try:
            order_id = t["id"]
            state, pnl = get_trade_result(order_id)
            if state is None:
                continue

            if state == 'unknown':
                tracker = _tracker_holder.get('tracker')
                if tracker is not None and tracker.is_pending(order_id):
                    if pnl is None or abs(float(pnl)) <= TradeTracker.ZERO_TOL:
                        continue
                if pnl is not None and abs(pnl) > TradeTracker.ZERO_TOL:
                    inferred = 'win' if pnl > 0 else 'loss'
                    if _knowledge_brain is not None:
                        for kb_ctx in t.get('kb_contexts', []):
                            try:
                                _knowledge_brain.observe(
                                    kb_ctx.get('pattern', 'unknown'),
                                    kb_ctx.get('action', 'unknown'),
                                    kb_ctx, inferred, source="live")
                            except Exception as e:
                                logging.warning(f"KB observe: {e}")
                    stats.record_result(order_id, inferred, t["amount"], pnl=pnl)
                    _observe_meta_trade_result(inferred, pnl, t.get('kb_contexts', []))
                    if inferred == "win":
                        risk_mgr.add_win()
                    elif inferred == "loss":
                        risk_mgr.add_loss()
                    _mark_tracker_watcher_result(order_id, inferred, pnl)
                    log_msg(f"   {'WIN' if inferred=='win' else 'LOSS'} "
                            f"{t['id']} ({t.get('pattern','?')}): {pnl:+.2f} [inferred]",
                            "trade")
                    has_new = True
                else:
                    # Không thể xác định kết quả → KHÔNG mark draw, giữ pending
                    log_msg(
                        f"   ⚠️ [PENDING-UNKNOWN] {order_id} ({t.get('pattern','?')}) "
                        f"state=unknown pnl=None — giữ pending chờ tracker",
                        "warning",
                    )
                continue

            if state in ('win', 'loss') and _knowledge_brain is not None:
                kb_contexts = t.get('kb_contexts', [])
                for kb_ctx in kb_contexts:
                    try:
                        _knowledge_brain.observe(
                            kb_ctx.get('pattern', 'unknown'),
                            kb_ctx.get('action', 'unknown'),
                            kb_ctx, state, source="live")
                    except Exception as e:
                        logging.warning(f"KB observe: {e}")
                if kb_contexts and KNOWLEDGE_BRAIN_VERBOSE:
                    kb_stats = _knowledge_brain.get_stats()
                    log_msg(
                        f"   🧠 KB updated {len(kb_contexts)} patterns "
                        f"with {state.upper()} (posterior: {kb_stats['n_keys']} keys)",
                        "trade")

            stats.record_result(order_id, state, t["amount"], pnl=pnl)
            if state in ('win', 'loss'):
                _observe_meta_trade_result(state, pnl, t.get('kb_contexts', []))
            if state == "win":
                risk_mgr.add_win()
            elif state == "loss":
                risk_mgr.add_loss()
            _mark_tracker_watcher_result(order_id, state, pnl)

            result_tag = {"win": "WIN", "loss": "LOSS", "draw": "DRAW"}.get(state, "?")
            log_msg(f"   {result_tag} {t['id']} ({t.get('pattern','?')}): {pnl:+.2f}",
                    "trade")
            has_new = True
        except Exception as e:
            logging.error(f"Error checking trade {t.get('id','?')}: {e}")
    return has_new


def _mark_tracker_watcher_result(trade_id, outcome, pnl):
    tracker = _tracker_holder.get('tracker')
    if tracker is not None:
        tracker.mark_resolved(
            trade_id,
            outcome,
            pnl=pnl,
            already_learned=True,
            source='executor_watcher',
        )


def _handle_tracker_resolution(record):
    if record.get('already_learned'):
        return

    with _pending_results_lock:
        stats = _stats_holder.get('stats')
        risk_mgr = _risk_mgr_holder.get('manager')
        if stats is None or risk_mgr is None:
            log_msg("[tracker] Cannot apply resolved result: stats or risk manager unavailable",
                    "warning")
            return

        trade_id = record.get('trade_id')
        pending = next(
            (
                trade for trade in stats.data.get('pending', [])
                if str(trade.get('id')) == str(trade_id)
            ),
            None,
        )
        if pending is None:
            return

        outcome = str(record.get('outcome', '')).lower()
        if outcome == 'lose':
            outcome = 'loss'
        if outcome not in {'win', 'loss', 'draw'}:
            log_msg(
                f"[tracker] Unsupported result for trade {trade_id}: {outcome}",
                "warning",
            )
            return

        kb_contexts = record.get('kb_contexts', [])
        pnl = record.get('pnl')
        if outcome in {'win', 'loss'} and _knowledge_brain is not None:
            for kb_ctx in kb_contexts:
                try:
                    _knowledge_brain.observe(
                        kb_ctx.get('pattern', 'unknown'),
                        kb_ctx.get('action', 'unknown'),
                        kb_ctx,
                        outcome,
                        source="live",
                    )
                except Exception as error:
                    logging.warning("KB observe from tracker: %s", error)

        stats.record_result(pending['id'], outcome, pending['amount'], pnl=pnl)
        if outcome in {'win', 'loss'}:
            _observe_meta_trade_result(outcome, pnl, kb_contexts)
        if outcome == 'win':
            risk_mgr.add_win()
        elif outcome == 'loss':
            risk_mgr.add_loss()


def _get_trade_expiry_candles(record):
    iq = _iq_holder.get('iq')
    if iq is None:
        return None
    return get_candles(
        iq,
        record.get('asset', TRADING_ASSET),
        60,
        2,
    )


def _observe_meta_trade_result(state, pnl, kb_contexts):
    if _meta_brain is None:
        return

    try:
        regime = kb_contexts[0].get('regime') if kb_contexts else None
        _meta_brain.observe_trade_result(state, pnl, regime=regime)
        report = _meta_brain.get_self_awareness_report()
    except Exception as e:
        logging.warning(f"AdaptiveMetaBrain result update failed: {e}")
        return

    log_msg(
        f"   🧬 [Self-Aware] Health={report['health']} | WR20={report['wr_20']}% | "
        f"Strictness={report['global_strictness']}x | DailyPnL=${report['daily_pnl']:+.2f}",
        "trade"
    )


# ============ PATTERN HELPERS ============
def normalize_direction(direction):
    if direction is None:
        return None
    d = str(direction).lower()
    if d in ('call', 'buy', 'up', '1', 'long'):
        return 'call'
    if d in ('put', 'sell', 'down', '-1', 'short'):
        return 'put'
    return None


def is_after_news(df, threshold=NEWS_RANGE_THRESHOLD, lookback=NEWS_LOOKBACK_CANDLES):
    if not NEWS_COOLDOWN_ENABLED:
        return False, 0, -1
    if len(df) < lookback + 3:
        return False, 0, -1
    for i in range(2, 2 + lookback):
        if len(df) < i + 1:
            break
        c = df.iloc[-i]
        if (c['high'] - c['low']) > threshold:
            return True, (c['high'] - c['low']) * 10000, i
    return False, 0, -1


def _trend_str(trend, sideways):
    if sideways:
        return "SIDEWAYS"
    if trend == 1:
        return "TANG"
    if trend == -1:
        return "GIAM"
    return "FLAT"


def check_asset_status(iq, force_refresh=False):
    now = time.time()
    if (not force_refresh and _asset_status_cache['status'] is not None
            and (now - _asset_status_cache['timestamp']) < ASSET_CACHE_TTL):
        return _asset_status_cache['status'], True

    status = "UNKNOWN"
    try:
        if _HAS_BROKER_FRAMEWORK and USE_BROKER_FRAMEWORK:
            from brokers.factory import is_asset_available_generic
            result = call_with_timeout(
                is_asset_available_generic, iq, TRADING_ASSET,
                timeout=ASSET_CHECK_TIMEOUT, default=None, name="asset_check")
        else:
            result = call_with_timeout(is_asset_available, iq, TRADING_ASSET,
                                        timeout=ASSET_CHECK_TIMEOUT, default=None,
                                        name="asset_check")
        if result is True:
            status = "OPEN"
        elif result is False:
            status = "CLOSED"
    except Exception as e:
        logging.debug(f"Asset check error: {e}")

    _asset_status_cache['status'] = status
    _asset_status_cache['timestamp'] = now
    return status, False
def _is_weekend_observation(now, asset_status, asset_check_enabled=True):
    return (
        ENABLE_WEEKEND_GUARD
        and now.weekday() >= 5
        and not (asset_check_enabled and asset_status == "OPEN")
    )

def find_nearest_opposite_zone(zone_mgr, price, action, include_middle=True):
    try:
        grid = zone_mgr.get_grid(price)
    except Exception:
        return None

    best_edge = None
    best_any = None
    for v, lbl, zt, _tol in grid:
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        lbl_lower = str(lbl).lower()

        if action == 'call':
            if v <= price:
                continue
            dist = (v - price) * 10000
        else:
            if v >= price:
                continue
            dist = (price - v) * 10000

        is_edge = ('z30' in lbl_lower) or ('z90' in lbl_lower)
        candidate = (lbl, v, dist, is_edge)

        if is_edge:
            if best_edge is None or dist < best_edge[2]:
                best_edge = candidate
        if include_middle:
            if best_any is None or dist < best_any[2]:
                best_any = candidate

    if include_middle and best_any is not None:
        return best_any
    return best_edge
def _check_middle_edge_support(zone_mgr, price, action, tol_pips=2.0):
    """Kiểm tra xem target middle zone (z60) có edge zone (z30/z90) gần không.

    Trả True nếu:
      - Có middle zone (z60) theo hướng action, VÀ
      - Có edge zone (z30/z90) trong ±tol_pips quanh middle zone đó.

    Mục đích: chọn R:R threshold chính xác cho middle zone.
    """
    try:
        grid = zone_mgr.get_grid(price)
    except Exception:
        return False

    # Bước 1: tìm middle zone gần nhất theo hướng action
    nearest_middle_price = None
    nearest_middle_dist = None
    for v, lbl, _zt, _tol in grid:
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        if 'z60' not in str(lbl).lower():
            continue
        if action == 'call' and v > price:
            d = (v - price) * 10000
        elif action == 'put' and v < price:
            d = (price - v) * 10000
        else:
            continue
        if nearest_middle_dist is None or d < nearest_middle_dist:
            nearest_middle_dist = d
            nearest_middle_price = v

    if nearest_middle_price is None:
        return False

    # Bước 2: có edge zone (z30/z90) nào trong ±tol_pips của middle zone?
    for v, lbl, _zt, _tol in grid:
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        lbl_lo = str(lbl).lower()
        if 'z30' not in lbl_lo and 'z90' not in lbl_lo:
            continue
        if abs(v - nearest_middle_price) * 10000 <= tol_pips:
            return True
    return False
# ============================================================
# v8.7.R3.1: R:R TARGET WITH M15 FALLBACK
# ============================================================

def _find_rr_target_with_fallback(
    zone_mgr,
    zone_mgr_m15,
    price,
    action,
    rr_edge_min,
    rr_middle_min,
    block_check=True,
):
    """
    Tìm target R:R với M15 fallback khi M1 fail.

    - Bước 1: query M1/M5 grid (giống find_nearest_opposite_zone include_middle=True).
              Nếu pass R:R → trả.
    - Bước 2: query M15 grid. Nếu pass R:R **và** không bị M1 zone cản đường
              → trả (source='M15').
    - Bước 3: cả hai fail → trả M1 result (có thể None hoặc fail R:R) để
              caller log lỗi rõ.

    block_check=True: trước khi dùng M15 target, kiểm tra có M1 zone nào
    nằm giữa entry và target M15 không. Nếu có → bỏ M15, trả M1 (fail).
    Lý do: M15 target 3.6 pips nhưng M1 zone 0.6 pips chen giữa → giá có
    thể bị M1 zone chặn trước khi tới M15 target. Đây là rủi ro thực tế
    với TURBO 1 phút.

    Returns: (lbl, value, dist, is_edge, source) where source ∈ {'M1','M15'}
             hoặc None.
    """

    def _query(mgr, source_tag):
        try:
            grid = mgr.get_grid(price)
        except Exception:
            return None
        best_edge = None
        best_any = None
        for v, lbl, zt, _tol in grid:
            try:
                v = float(v)
            except (TypeError, ValueError):
                continue
            lbl_lower = str(lbl).lower()
            if action == 'call':
                if v <= price:
                    continue
                dist = (v - price) * 10000
            else:
                if v >= price:
                    continue
                dist = (price - v) * 10000
            is_edge = ('z30' in lbl_lower) or ('z90' in lbl_lower)
            candidate = (lbl, v, dist, is_edge, source_tag)
            if is_edge:
                if best_edge is None or dist < best_edge[2]:
                    best_edge = candidate
            if best_any is None or dist < best_any[2]:
                best_any = candidate
        # v8.7.R11: ưu tiên edge zone nếu có (edge = z30/z90, target an toàn hơn)
        return best_edge if best_edge is not None else best_any

    def _passes_rr(cand):
        if cand is None:
            return False
        _, _, dist, is_edge, _ = cand
        threshold = rr_edge_min if is_edge else rr_middle_min
        return dist >= threshold

    def _find_blocking_m1_zone(m15_target_val):
        if not block_check:
            return None
        try:
            grid = zone_mgr.get_grid(price)
        except Exception:
            return None
        down = m15_target_val < price
        blocking = None
        for v, lbl, zt, _tol in grid:
            try:
                v = float(v)
            except (TypeError, ValueError):
                continue
            if down and m15_target_val < v < price:
                d = (price - v) * 10000
                if blocking is None or d < blocking[1]:
                    blocking = (lbl, d)
            elif not down and price < v < m15_target_val:
                d = (v - price) * 10000
                if blocking is None or d < blocking[1]:
                    blocking = (lbl, d)
        return blocking

    # 1) M1
    m1 = _query(zone_mgr, 'M1')
    if _passes_rr(m1):
        return m1

    # 2) M15 fallback
    if zone_mgr_m15 is not None:
        m15 = _query(zone_mgr_m15, 'M15')
        if _passes_rr(m15):
            blocking = _find_blocking_m1_zone(m15[1])
            if blocking is None:
                try:
                    logging.info(
                        "   ↪️ R:R fallback M15 %s @ %.5f (%.1f pips ≥ %.1f)",
                        m15[0], m15[1], m15[2],
                        rr_edge_min if m15[3] else rr_middle_min,
                    )
                except Exception:
                    pass
                return m15
            try:
                logging.info(
                    "   ⚠️ M15 fallback %s @ %.5f skipped — "
                    "blocked by M1 %s (%.1f pips)",
                    m15[0], m15[1], blocking[0], blocking[1],
                )
            except Exception:
                pass

    # 3) Không cái nào pass → trả M1 (caller tự log lỗi)
    return m1

# ============ MARKET REGIME + SESSION ============
def calculate_atr_simple(df, period=14):
    try:
        if len(df) < period + 1:
            return 0.0
        tr = []
        for i in range(-period, 0):
            h = float(df.iloc[i]['high'])
            l = float(df.iloc[i]['low'])
            pc = float(df.iloc[i - 1]['close'])
            tr.append(max(h - l, abs(h - pc), abs(l - pc)))
        return sum(tr) / len(tr)
    except Exception:
        return 0.0


def _get_compressed_trend_regime(df_m5, trend_m5, trend_m15, vol_ratio):
    if (
        not ENABLE_REGIME_RANGE_SANITY
        or len(df_m5) < REGIME_RANGE_LOOKBACK
        or trend_m5 == 0
        or trend_m5 != trend_m15
    ):
        return None

    window = df_m5.iloc[-REGIME_RANGE_LOOKBACK:]
    recent_range = float(window['high'].max() - window['low'].min())
    avg_atr = calculate_atr_simple(df_m5, REGIME_RANGE_ATR_PERIOD)
    expected_range_for_trend = (
        avg_atr * math.sqrt(REGIME_RANGE_LOOKBACK) * REGIME_RANGE_ATR_SCALE
    )
    if (
        not math.isfinite(recent_range)
        or not math.isfinite(expected_range_for_trend)
        or expected_range_for_trend <= 0
        or recent_range >= expected_range_for_trend
    ):
        return None

    confidence_denominator = (
        expected_range_for_trend * REGIME_RANGE_CONFIDENCE_SCALE
    )
    if confidence_denominator <= 0:
        return None
    wide_prob = max(0.0, min(1.0, recent_range / confidence_denominator))
    return (
        'RANGING_WIDE',
        trend_m5,
        trend_m15,
        vol_ratio,
        round(wide_prob, 2),
    )


def detect_market_regime(df_m5, df_m15):
    trend_m5, sideways_m5 = trend_369(df_m5)
    if df_m15 is not None and len(df_m15) >= 50:
        trend_m15, sideways_m15 = trend_369(df_m15)
    else:
        trend_m15, sideways_m15 = 0, True

    atr_now = calculate_atr_simple(df_m5, 14)
    atr_long = calculate_atr_simple(df_m5, 50) if len(df_m5) >= 51 else atr_now
    vol_ratio = (atr_now / atr_long) if atr_long > 0 else 1.0

    vol_shock_score = max(0.0, min(1.0, (vol_ratio - 1.5) / 0.8))

    if trend_m5 == -1 and trend_m15 == 1:
        conf = max(0.4, 1.0 - vol_shock_score * 0.5)
        return 'PULLBACK_UP', trend_m5, trend_m15, vol_ratio, round(conf, 2)
    if trend_m5 == 1 and trend_m15 == -1:
        conf = max(0.4, 1.0 - vol_shock_score * 0.5)
        return 'PULLBACK_DOWN', trend_m5, trend_m15, vol_ratio, round(conf, 2)
    if vol_shock_score >= 0.65:
        return 'VOLATILE', trend_m5, trend_m15, vol_ratio, round(vol_shock_score, 2)
    if not sideways_m5 and trend_m5 == trend_m15 != 0:
        compressed_range_regime = _get_compressed_trend_regime(
            df_m5, trend_m5, trend_m15, vol_ratio
        )
        if compressed_range_regime is not None:
            return compressed_range_regime
        conf = max(0.5, 1.0 - abs(vol_ratio - 1.1) * 0.3)
        name = 'STRONG_TREND_UP' if trend_m5 == 1 else 'STRONG_TREND_DOWN'
        return name, trend_m5, trend_m15, vol_ratio, round(conf, 2)
    if sideways_m5 and sideways_m15:
        wide_prob = max(0.0, min(1.0, (vol_ratio - 1.15) / 0.45))
        if wide_prob >= 0.5:
            return 'RANGING_WIDE', trend_m5, trend_m15, vol_ratio, round(wide_prob, 2)
        return 'RANGING_TIGHT', trend_m5, trend_m15, vol_ratio, round(1.0 - wide_prob, 2)
    return 'MIXED', trend_m5, trend_m15, vol_ratio, 0.45


def detect_session(now=None):
    try:
        if now is None:
            now = datetime.now(timezone.utc)
        now = now.astimezone(timezone.utc)
        h = now.hour
    except Exception:
        return 'UNKNOWN'
    if 0 <= h < 7:   return 'ASIA'
    if 7 <= h < 12:  return 'LONDON'
    if 12 <= h < 16: return 'LONDON_NY_OVERLAP'
    if 16 <= h < 21: return 'NEWYORK'
    return 'LATE_NY'


def session_threshold_adjust(session):
    return {'LONDON_NY_OVERLAP': -1, 'LONDON': 0, 'NEWYORK': 0,
            'ASIA': +1, 'LATE_NY': +1}.get(session, 0)



def _all_zones_are_edges(touched_zones):
    """Trả True nếu tất cả zone chạm đều là edge (z30/z90), không có middle (z60)."""
    if not touched_zones:
        return False
    for lbl, _ltype, _zval in touched_zones:
        lbl_lower = str(lbl).lower()
        if 'z60' in lbl_lower:  # middle zone
            return False
    return True


def should_override_filter(agreeing_names, zone_stars, pa_count,
                            consensus_action, regime, session,
                            zone_is_edge=False, touched_zones=None):
    # v8.9.1: yêu cầu >=2 real reversal (không tính continuation/Strat)
    if not ENABLE_OVERRIDE:
        return False, ""
    has_strong = any(n in STRONG_TRUE_REVERSAL_PATTERNS for n in agreeing_names)
    effective_zone = zone_stars if (zone_is_edge or not ENABLE_ZONE_SUBTYPE_AWARENESS) else 0

    real_reversal_count = sum(
        1 for n in agreeing_names
        if n in TRUE_REVERSAL_PATTERNS
        and n not in CONTINUATION_PATTERNS
        and n not in STRAT_REVERSAL_PATTERNS
    )

    if pa_count >= 3 and effective_zone >= 1:
        if real_reversal_count >= 2:
            return True, f"3+_PA+zone+2reversals ({pa_count}PA+z{effective_zone})"

    if has_strong and effective_zone >= 1 and pa_count >= 2:
        if real_reversal_count >= 2:
            return True, "strong_reversal+zone+2PA"

    if regime == 'PULLBACK_UP' and consensus_action == 'call' and effective_zone >= 1 and (pa_count >= 2 or effective_zone >= 2):
        return True, f"pullback_up+zone+({pa_count}PA/z{effective_zone})"
    if regime == 'PULLBACK_DOWN' and consensus_action == 'put' and effective_zone >= 1 and (pa_count >= 2 or effective_zone >= 2):
        return True, f"pullback_down+zone+({pa_count}PA/z{effective_zone})"

    if regime in ('STRONG_TREND_UP', 'STRONG_TREND_DOWN') and effective_zone >= 2:
        if touched_zones is None:
            return True, "strong_trend+zone2"
        if _all_zones_are_edges(touched_zones):
            if real_reversal_count >= 1:
                return True, "strong_trend+zone2_edges+reversal"
        if pa_count >= 2 and has_strong:
            if real_reversal_count >= 2:
                return True, "strong_trend+zone2+strong_reversal"
        return False, ""

    return False, ""

def get_session_block_reason(session, regime, zone_stars, setup_stars,
                              agreeing_names, pa_count):
    if not ENABLE_SESSION_FILTER:
        return ""
    if session == 'ASIA' and not TRADE_ASIA:
        if regime == 'RANGING_TIGHT' and zone_stars >= ASIA_MEAN_REVERSION_MIN_ZONE:
            return ""
        return f"[SESSION] Asia RANGING_TIGHT + zone{ASIA_MEAN_REVERSION_MIN_ZONE}* only"
    if session == 'LATE_NY' and TRADE_LATE_NY:
        has_strong = any(n in STRONG_TRUE_REVERSAL_PATTERNS for n in agreeing_names)
        if has_strong and setup_stars >= LATE_NY_MIN_SETUP:
            return ""
        return f"[SESSION] Late NY strong_reversal + {LATE_NY_MIN_SETUP}* only"
    return ""


# ============ 17 PA PATTERNS + MORNING/EVENING STAR + 3 SOLDIERS ============
def pattern_morning_evening_star(df):
    if len(df) < 3: return 0
    try:
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        c1_o, c1_c = float(c1['open']), float(c1['close'])
        c2_o, c2_c = float(c2['open']), float(c2['close'])
        c3_o, c3_c = float(c3['open']), float(c3['close'])
    except (KeyError, TypeError, ValueError): return 0
    c1_body = abs(c1_c - c1_o); c2_body = abs(c2_c - c2_o); c3_body = abs(c3_c - c3_o)
    if c1_body <= 0 or c3_body <= 0: return 0
    c1_mid = (c1_o + c1_c) / 2
    if (c1_c < c1_o and c2_body < c1_body * 0.5 and c3_c > c3_o
            and c3_body > c1_body * 0.6 and c3_c > c1_mid): return 1
    if (c1_c > c1_o and c2_body < c1_body * 0.5 and c3_c < c3_o
            and c3_body > c1_body * 0.6 and c3_c < c1_mid): return -1
    return 0

def pattern_three_soldiers(df):
    if len(df) < 3: return 0
    try:
        candles = [df.iloc[-3], df.iloc[-2], df.iloc[-1]]
        bodies, dirs = [], []
        for c in candles:
            o, cl = float(c['open']), float(c['close'])
            h, l = float(c['high']), float(c['low'])
            rng = h - l
            if rng <= 0: return 0
            bodies.append(abs(cl - o) / rng)
            dirs.append(1 if cl > o else -1 if cl < o else 0)
    except (KeyError, TypeError, ValueError): return 0
    if not (dirs[0] == dirs[1] == dirs[2] != 0): return 0
    if not all(b >= 0.60 for b in bodies): return 0
    if dirs[0] == 1:
        if (float(candles[1]['close']) > float(candles[0]['close'])
                and float(candles[2]['close']) > float(candles[1]['close'])): return 1
    else:
        if (float(candles[1]['close']) < float(candles[0]['close'])
                and float(candles[2]['close']) < float(candles[1]['close'])): return -1
    return 0

def pattern_tweezer(df, tol=0.00005):
    if len(df) < 2: return 0
    try:
        c1, c2 = df.iloc[-2], df.iloc[-1]
        c1_h, c1_l = float(c1['high']), float(c1['low'])
        c2_h, c2_l = float(c2['high']), float(c2['low'])
        c1_o, c1_c = float(c1['open']), float(c1['close'])
        c2_o, c2_c = float(c2['open']), float(c2['close'])
    except (KeyError, TypeError, ValueError): return 0
    if (abs(c1_l - c2_l) <= tol and c1_c < c1_o and c2_c > c2_o): return 1
    if (abs(c1_h - c2_h) <= tol and c1_c > c1_o and c2_c < c2_o): return -1
    return 0

def pattern_marubozu(df):
    try:
        c = df.iloc[-1]
        o, cl = float(c['open']), float(c['close'])
        h, l = float(c['high']), float(c['low'])
    except (KeyError, TypeError, ValueError): return 0
    rng = h - l
    if rng <= 0: return 0
    body = abs(cl - o)
    if body / rng < MARUBOZU_MIN_BODY_RATIO: return 0
    upper = h - max(o, cl); lower = min(o, cl) - l
    if (upper / rng > MARUBOZU_MAX_WICK_RATIO
            or lower / rng > MARUBOZU_MAX_WICK_RATIO): return 0
    return 1 if cl > o else (-1 if cl < o else 0)

def pattern_momentum(df):
    try:
        c = df.iloc[-1]
        o, cl = float(c['open']), float(c['close'])
        h, l = float(c['high']), float(c['low'])
    except (KeyError, TypeError, ValueError):
        return 0
    rng = h - l
    if rng <= 0:
        return 0
    body = abs(cl - o)
    if body < MOMENTUM_PA_MIN_BODY_PIPS * PIP_SIZE:
        return 0
    if body / rng < MOMENTUM_PA_MIN_BODY_RATIO:
        return 0
    return 1 if cl > o else (-1 if cl < o else 0)

def pattern_piercing_darkcloud(df):
    if len(df) < 2: return 0
    try:
        c1, c2 = df.iloc[-2], df.iloc[-1]
        c1_o, c1_c = float(c1['open']), float(c1['close'])
        c2_o, c2_c = float(c2['open']), float(c2['close'])
    except (KeyError, TypeError, ValueError): return 0
    c1_mid = (c1_o + c1_c) / 2
    c1_body = abs(c1_c - c1_o); c2_body = abs(c2_c - c2_o)
    if c1_body <= 0 or c2_body <= 0: return 0
    if (c1_c < c1_o and c2_c > c2_o and c2_o < c1_c and c2_c > c1_mid
            and c2_c < c1_o and c2_body > c1_body * 0.5): return 1
    if (c1_c > c1_o and c2_c < c2_o and c2_o > c1_c and c2_c < c1_mid
            and c2_c > c1_o and c2_body > c1_body * 0.5): return -1
    return 0

def pattern_belt_hold(df):
    try:
        c = df.iloc[-1]
        o, cl = float(c['open']), float(c['close'])
        h, l = float(c['high']), float(c['low'])
    except (KeyError, TypeError, ValueError): return 0
    rng = h - l
    if rng <= 0: return 0
    body = abs(cl - o)
    if body / rng < 0.70: return 0
    lower_wick = min(o, cl) - l; upper_wick = h - max(o, cl)
    if (cl > o and lower_wick / rng < 0.05 and upper_wick / rng < 0.20): return 1
    if (cl < o and upper_wick / rng < 0.05 and lower_wick / rng < 0.20): return -1
    return 0

def pattern_three_inside(df):
    if len(df) < 3: return 0
    try:
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        c1_o, c1_c = float(c1['open']), float(c1['close'])
        c1_h, c1_l = float(c1['high']), float(c1['low'])
        c2_c = float(c2['close']); c2_o = float(c2['open'])
        c2_h = float(c2['high']); c2_l = float(c2['low'])
        c3_o, c3_c = float(c3['open']), float(c3['close'])
    except (KeyError, TypeError, ValueError): return 0
    if not (c2_h <= c1_h and c2_l >= c1_l): return 0
    if (c1_c < c1_o and c2_c > c2_o and c3_c > c3_o and c3_c > c1_o): return 1
    if (c1_c > c1_o and c2_c < c2_o and c3_c < c3_o and c3_c < c1_o): return -1
    return 0

def pattern_tasuki_gap(df):
    if len(df) < 3: return 0
    try:
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        c1_o, c1_c = float(c1['open']), float(c1['close'])
        c1_h = float(c1['high']); c1_l = float(c1['low'])
        c2_o, c2_c = float(c2['open']), float(c2['close'])
        c2_h = float(c2['high']); c2_l = float(c2['low'])
        c3_o, c3_c = float(c3['open']), float(c3['close'])
    except (KeyError, TypeError, ValueError): return 0
    if c1_c == c1_o: return 0
    if (c1_c > c1_o and c2_c > c2_o and c2_l > c1_h and c3_c < c3_o
            and c2_o < c3_o < c2_c and c3_c > c1_h): return 1
    if (c1_c < c1_o and c2_c < c2_o and c2_h < c1_l and c3_c > c3_o
            and c2_o > c3_o > c2_c and c3_c < c1_l): return -1
    return 0


def scan_all_pa_patterns(df, tweezer_tol=0.00005, strat_target_pips=1.2):
    results = {}
    log_candle_metrics(df)   # ← thêm dòng này

    try:
        mapping = [
            ('2Rau', pattern_2_rau_nguoc(df)),
            ('3R', pattern_3R(df)),
            ('Pinbar', pattern_pinbar(df)),
            ('Engulfing', pattern_engulfing(df)),
            ('Star', pattern_star(df)),
            ('InsideBar', pattern_inside_bar(df)),
            ('Doji', pattern_doji(df)),
            ('Railway', pattern_railway(df)),
            ('Mau-M', pattern_M(df)),
            ('MornStar', pattern_morning_evening_star(df)),
            ('3Soldiers', pattern_three_soldiers(df)),
            ('Tweezer', pattern_tweezer(df, tol=tweezer_tol)),
            ('Marubozu', pattern_marubozu(df)),
            ('Piercing', pattern_piercing_darkcloud(df)),
            ('BeltHold', pattern_belt_hold(df)),
            ('3Inside', pattern_three_inside(df)),
            ('Tasuki', pattern_tasuki_gap(df)),
        ]
        if ENABLE_MOMENTUM_PA_PATTERN:
            mapping.append(('Momentum', pattern_momentum(df)))
    except Exception as e:
        logging.error(f"scan_all_pa_patterns: {e}")
        return results
    for name, sig in mapping:
        if sig == 1: results[name] = 'call'
        elif sig == -1: results[name] = 'put'

    strat_match = match_strat_template(df, min_target_pips=strat_target_pips)
    if strat_match:
        results[strat_match['name']] = strat_match['direction']
        target = (
            f"target room={strat_match['target_pips']:.1f} pips"
            if strat_match['target_pips'] is not None
            else "no fixed target in chart"
        )
        logging.info(
            "The Strat matched %s %s | entry=%.5f | %s",
            strat_match['name'], strat_match['direction'].upper(),
            strat_match['entry_price'], target,
        )
    return results


def _format_pa_pattern_summary(pa_patterns):
    parts = []
    for name, direction in pa_patterns.items():
        tag = "(neutral)" if name in NO_VOTE_PATTERNS else ""
        parts.append(f"{name}->{direction.upper()}{tag}")
    return ", ".join(parts) if parts else "no pattern"


def _validate_price_candle(price_validator, candle, tolerance_pips):
    try:
        return price_validator.is_candle_valid(candle, tolerance_pips)
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        return False, f"validator error ({type(exc).__name__}): {exc}"


_PA_REVERSAL_NEEDS_CONTEXT = {
    'Pinbar', 'Engulfing', 'Star', 'MornStar',
    'BeltHold', '3Inside', 'Tweezer',
}


def _needs_369_context(agreeing_names):
    return any(name in _PA_REVERSAL_NEEDS_CONTEXT for name in agreeing_names)


def _has_369_or_edge_context(
    agreeing_signals,
    touched_zones,
    setup_stars,
    pa_effective_count=0,
    near_zones=None,
    last_close=None,
):
    has_369 = any(signal[2] == '369' for signal in agreeing_signals)
    if has_369:
        return True, "369_present"

    edge_label = next(
        (
            str(label)
            for label, _timeframe, _value in touched_zones
            if 'z30' in str(label).lower() or 'z90' in str(label).lower()
        ),
        None,
    )
    if edge_label:
        return True, f"touch_edge({edge_label})"

    if pa_effective_count >= 2:
        return True, f"multi_pa({pa_effective_count})"

    if last_close is not None:
        for label, _timeframe, level in near_zones or ():
            if 'z30' not in str(label).lower() and 'z90' not in str(label).lower():
                continue
            distance_pips = abs(float(last_close) - float(level)) / PIP_SIZE
            if distance_pips <= PA_369_CONTEXT_NEAR_EDGE_PIPS:
                return True, f"near_edge({label}@{distance_pips:.1f}p)"

    if setup_stars >= PA_369_CONTEXT_MIN_SETUP_STARS:
        return True, f"strong_setup({setup_stars}★)"

    return False, "no_context"


# ============ MAIN ============

# ============ v8.7.R8: M15 HIGH SOFT OVERRIDE ============
def _should_soft_override_m15_high(setup_stars, required, regime_conf):
    """Trả True nếu setup đủ mạnh + regime confidence thấp → cho phép counter-M15."""
    return (
        ENABLE_M15_HIGH_SOFT_OVERRIDE
        and setup_stars >= M15_HIGH_SOFT_OVERRIDE_MIN_STARS
        and setup_stars < required
        and regime_conf < M15_HIGH_SOFT_OVERRIDE_MAX_CONFIDENCE
    )


def main():
    lock_dir = (
        Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        / "EURUSDTradingBot"
        if os.name == "nt"
        else Path(os.environ.get("XDG_RUNTIME_DIR", Path.home() / ".local" / "run"))
        / "eurusd-trading-bot"
    )
    lock_dir.mkdir(parents=True, exist_ok=True)
    instance_lock = open(lock_dir / "single-instance.lock", "a+", encoding="utf-8")
    if os.name == "nt":
        import msvcrt
        try:
            msvcrt.locking(instance_lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            instance_lock.close()
            return 1
    else:
        import fcntl
        try:
            fcntl.flock(instance_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            instance_lock.close()
            return 1
    global _knowledge_brain, _meta_brain, _balance_unavailable
    run_started_at = datetime.now().astimezone()
    run_id = f"{run_started_at.strftime('%Y%m%d_%H%M%S_%f')}_{os.getpid()}"
    scan_sequence = 0
    _configure_windowed_logging()
    _load_counter()

    learning_blacklist = {}
    learning_filter_data_status = "disabled"
    if ENABLE_LEARNING_FILTERS:
        learning_report = load_learning_report(_get_learning_report_path())
        if learning_report is None:
            learning_filter_data_status = "unavailable"
            log_msg(
                "      ⚠️ [LEARNING-FILTER] learning_report.json missing or invalid; "
                "blacklist filter inactive",
                "warning",
            )
        else:
            try:
                learning_blacklist = _get_learning_blacklist(
                    learning_report, LEARNING_MIN_PATTERN_SAMPLES
                )
            except (KeyError, ValueError) as exc:
                learning_filter_data_status = "unavailable"
                log_msg(
                    f"      ⚠️ [LEARNING-FILTER] invalid blacklist: {exc}; "
                    "filter inactive",
                    "warning",
                )
            else:
                learning_filter_data_status = "active"
                log_msg(
                    f"      Learning blacklist: {len(learning_blacklist)} patterns "
                    f"with n>={LEARNING_MIN_PATTERN_SAMPLES}",
                    "info",
                )

    cfg369 = Strategy369Config(**STRATEGY_369)
    if isinstance(cfg369.MinVolumeRatio, (tuple, list)):
        cfg369.MinVolumeRatio = float(cfg369.MinVolumeRatio[0])

    zone_mgr = ZoneManager(
        U=cfg369.Unit,
        tolerance_minor=ZONE_TOLERANCE_MINOR,
        tolerance_major=ZONE_TOLERANCE_MAJOR,
        retest_timeout=5,
        grid_profile='small',
    )
    zone_mgr_m5 = ZoneManager(
        U=cfg369.Unit,
        tolerance_minor=ZONE_TOLERANCE_MINOR,
        tolerance_major=ZONE_TOLERANCE_MAJOR,
        retest_timeout=5,
        grid_profile='small',
    )
    zone_mgr_m15 = ZoneManager(
        U=cfg369.Unit,
        tolerance_minor=ZONE_TOLERANCE_MINOR,
        tolerance_major=ZONE_TOLERANCE_MAJOR,
        retest_timeout=5,
        grid_profile='big',
    )

    license_client = LicenseClient(
        server_url=os.environ.get("LICENSE_SERVER_URL", DEFAULT_SERVER_URL),
        license_email=LICENSE_EMAIL,
    )
    dashboard_broker_name = (
        get_broker_name()
        if _HAS_BROKER_FRAMEWORK and USE_BROKER_FRAMEWORK
        else "iq"
    )
    saved_creds = (
        {"api_token": DERIV_API_TOKEN}
        if dashboard_broker_name == "deriv" and DERIV_API_TOKEN
        else (EMAIL, PASSWORD) if dashboard_broker_name == "iq" and EMAIL and PASSWORD
        else None
    )
    dashboard = DashboardState(
        asset=TRADING_ASSET, timeframe=CANDLE_TIME,
        credentials=saved_creds, account_type=ACCOUNT_TYPE,
        option_type="BINARY", api_key=TWELVEDATA_API_KEY,
        license_client=license_client,
        trade_log_path=LOG_FILE,
        broker_name=dashboard_broker_name,
        broker_display_name=get_broker_display_name(dashboard_broker_name),
    )
    _srv, dashboard_url = start_dashboard(dashboard)
    log_msg(f"Dashboard: {dashboard_url}")

    _should_auto_open = (os.name == "nt" or sys.platform == "darwin")
    if _should_auto_open:
        try:
            webbrowser.open(dashboard_url, new=2)
        except Exception as error:
            log_msg(f"→ Open manually: {dashboard_url}")
    else:
        log_msg(f"→ Open manually: {dashboard_url}")

    log_msg(f"========== BOT KHOI DONG - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==========")
    log_msg(f"Gate audit run_id: {run_id}")
    log_msg("main.py VERSION 8.7.R10 - v8.7 features merged "
            "(STRONG_TRUE_REVERSAL + REGIME_RANGE_SANITY + M15_HIGH_SOFT + "
            "ZONE-BIAS-OVERRIDE + countdown dashboard)")
    log_msg(f"Weekend guard: {'ON' if ENABLE_WEEKEND_GUARD else 'OFF'} | "
            f"Stale guard: {'ON' if ENABLE_STALE_GUARD else 'OFF'}")
    log_msg(f"v8.4 filters: zone_subtype=ON | pa_correlation=ON | "
            f"m15_severity_block={'ON' if ENABLE_M15_SEVERITY_BLOCK else 'OFF'} | "
            f"kb_soft_veto={'ON' if ENABLE_KB_SOFT_VETO else 'OFF'} "
            f"(thr={KB_SOFT_VETO_THRESHOLD:.2f}+{KB_VETO_MARGIN:.2f}) | "
            f"z60_breakout_ext=ON")
    log_msg(
        f"Zone grids: M1/M5 small=ON | "
        f"M15 zbig={'ON' if ENABLE_M15_ZONE_SYSTEM else 'OFF'} | "
        f"MTF entry filter={'ON' if ENABLE_MTF_ZONE_ENTRY_FILTER else 'OFF'}"
    )
    log_msg(
        f"Minor-zone trend soft-pass="
        f"{'ON' if ENABLE_MINOR_ZONE_TREND_OVERRIDE else 'OFF'} | "
        f"min setup={MINOR_ZONE_TREND_OVERRIDE_MIN_STARS}★ | "
        "requires aligned STRONG_TREND + non-sideways M15"
    )
    log_msg(f"v8.4 momentum: {'ON' if (ENABLE_MOMENTUM_FILTER and _HAS_MOMENTUM) else 'OFF'} | "
            f"continuation_downgrade={'ON' if ENABLE_CONTINUATION_DOWNGRADE else 'OFF'}")
    log_msg(
        f"🧪 v8.7.R2 ATR-Norm: "
        f"{'ON' if ENABLE_ATR_NORMALIZED_CONFLUENCE else 'OFF'} | "
        f"Exhaustion: {'ON' if ENABLE_EXHAUSTION_REVERSAL else 'OFF'}"
    )
    log_msg(
        f"Countertrend confirmation: "
        f"{'ON' if ENABLE_COUNTERTREND_CONFIRMATION else 'OFF'} "
        f"(min independent PA={COUNTERTREND_MIN_PA_CONFIRMATIONS})"
    )
    log_msg(
        f"⚡ perf: connect_cache={CONNECT_CHECK_INTERVAL}s | "
        f"balance_adaptive=idle_{BALANCE_IDLE_INTERVAL}s/pending_{BALANCE_PENDING_INTERVAL}s | "
        f"fetch_parallel=ON | m5m15_cache=ON | countdown_poll={COUNTDOWN_POLL_INTERVAL}s"
    )

    if ENABLE_KNOWLEDGE_BRAIN and _HAS_KNOWLEDGE_BRAIN:
        _knowledge_brain = KnowledgeBrain()
        kb_stats = _knowledge_brain.get_stats()
        kb_live = kb_stats['by_source']['live']
        kb_shadow = kb_stats['by_source']['shadow']
        log_msg(
            f"🧠 knowledge_brain v8.2: ACTIVE | "
            f"posterior={kb_stats['n_keys']} keys "
            f"(all L0/L1/L2={kb_stats['by_level']['L0']}/"
            f"{kb_stats['by_level']['L1']}/{kb_stats['by_level']['L2']}) | "
            f"live={kb_live['L0']}/{kb_live['L1']}/{kb_live['L2']} | "
            f"shadow={kb_shadow['L0']}/{kb_shadow['L1']}/{kb_shadow['L2']} | "
            f"legacy mixed={kb_stats['legacy_mixed_keys']} "
            "(preserved; excluded from source-scoped scoring)")
    else:
        reason = "disabled" if not ENABLE_KNOWLEDGE_BRAIN else "module unavailable"
        log_msg(f"🧠 knowledge_brain: OFF ({reason})")

    shadow = None
    shadow_loop = None
    shadow_thread = None
    if ENABLE_SHADOW_MODE and _knowledge_brain is not None:
        async def _fetch_shadow_price():
            iq_client = _iq_holder.get("iq")
            if iq_client is None:
                return None
            candles = await asyncio.to_thread(
                call_with_timeout,
                get_candles_raw,
                iq_client,
                TRADING_ASSET,
                CANDLE_TIME,
                1,
                time.time(),
                timeout=TIMEOUT_GET_CANDLES,
                default=None,
                name="shadow_price",
            )
            if not isinstance(candles, list) or not candles:
                return None
            return _extract_fresh_shadow_price(candles)

        shadow = ShadowMode(
            brain=_knowledge_brain,
            fetch_price=_fetch_shadow_price,
            max_per_pattern_per_day=SHADOW_MAX_PER_PATTERN_PER_DAY,
            enabled_patterns=SHADOW_ENABLED_PATTERNS,
        )
        shadow_loop = asyncio.new_event_loop()

        def _run_shadow_loop():
            asyncio.set_event_loop(shadow_loop)
            shadow_loop.run_forever()

        shadow_thread = threading.Thread(
            target=_run_shadow_loop,
            daemon=True,
            name="ShadowModeLoop",
        )
        shadow_thread.start()
        try:
            asyncio.run_coroutine_threadsafe(
                shadow.start(), shadow_loop
            ).result(timeout=5)
        except (RuntimeError, TimeoutError) as error:
            log_msg(f"[SHADOW] Unable to start worker: {error}", "warning")
            shadow_loop.call_soon_threadsafe(shadow_loop.stop)
            shadow_thread.join(timeout=5)
            shadow = None
            shadow_loop = None
            shadow_thread = None
        else:
            log_msg(
                f"[SHADOW] ACTIVE | patterns={len(SHADOW_ENABLED_PATTERNS)} | "
                f"max={SHADOW_MAX_PER_PATTERN_PER_DAY}/pattern/day",
                "info",
            )
    def _stop_shadow_mode():
        if shadow is None or shadow_loop is None:
            return
        if shadow_loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(
                    shadow.stop(), shadow_loop
                ).result(timeout=10)
            except TimeoutError as error:
                log_msg(f"[SHADOW] Worker shutdown timed out: {error}", "warning")
            shadow_loop.call_soon_threadsafe(shadow_loop.stop)
        if shadow_thread is not None:
            shadow_thread.join(timeout=5)

    if shadow is not None:
        atexit.register(_stop_shadow_mode)
    elif ENABLE_SHADOW_MODE:
        log_msg(
            "[SHADOW] Disabled because KnowledgeBrain is unavailable or disabled",
            "warning",
        )

    if _HAS_META_BRAIN:
        _meta_brain = AdaptiveMetaBrain(
            max_daily_loss=DAILY_LOSS_LIMIT,
            max_consec_loss=MAX_CONSECUTIVE_LOSSES,
            pause_sec=CONSECUTIVE_LOSS_PAUSE_SECONDS,
            new_regime_penalty=NEW_REGIME_PENALTY,
            new_pullback_regime_penalty=NEW_PULLBACK_REGIME_PENALTY,
        )
        sa_rep = _meta_brain.get_self_awareness_report()

        # v8.9 Tier 1: auto-reset WR20 nếu data cũ lỗi thời
        if (
            ENABLE_METABRAIN_WR20_RESET
            and sa_rep['wr_20'] < METABRAIN_WR20_RESET_THRESHOLD
        ):
            log_msg(
                f"[MetaBrain] WR20={sa_rep['wr_20']}% < "
                f"{METABRAIN_WR20_RESET_THRESHOLD}% — attempting WR20 reset",
                "warning",
            )
            reset_ok = False
            # Thử method reset_wr20 nếu có
            try:
                if hasattr(_meta_brain, 'reset_wr20'):
                    _meta_brain.reset_wr20()
                    reset_ok = True
                    log_msg("[MetaBrain] reset_wr20() OK")
            except Exception as exc:
                log_msg(f"[MetaBrain] reset_wr20() failed: {exc}", "warning")
            if not reset_ok:
                log_msg(
                    "[MetaBrain] reset_wr20() not available — "
                    "xóa logs/meta_brain_state.json (hoặc tên tương ứng) thủ công",
                    "warning",
                )
            sa_rep = _meta_brain.get_self_awareness_report()

        log_msg(
            f"🧬 AdaptiveMetaBrain v9.0: ACTIVE | strictness={sa_rep['global_strictness']}x | "
            f"WR20={sa_rep['wr_20']}% | daily_pnl=${sa_rep['daily_pnl']:+.2f} | "
            f"CB={MAX_CONSECUTIVE_LOSSES} losses/{CONSECUTIVE_LOSS_PAUSE_SECONDS // 60}m | "
            f"daily_loss_limit=${DAILY_LOSS_LIMIT:.2f}"
        )

    if ENABLE_MOMENTUM_FILTER and _HAS_MOMENTUM:
        log_msg(f"📊 momentum_analyzer v8.3: ACTIVE (RSI, position, stoch, consec, maD, breakout)")
    elif ENABLE_MOMENTUM_FILTER and not _HAS_MOMENTUM:
        log_msg("📊 momentum_analyzer: MISSING — momentum filter disabled", "warning")

    def update_conn(status):
        labels = {"connecting": "Connecting", "connected": "Connected",
                  "failed": "Login failed", "error": "Connection error"}
        _heartbeat()
        dashboard.update(connection_status=status, bot_status=labels.get(status, status))

    _license_attempt = {'n': 0}
    _license_retry_delay = 60
    while True:
        _license_attempt['n'] += 1
        log_msg(f"      [license] check attempt {_license_attempt['n']}...")
        try:
            _lic_ok = call_with_timeout(license_client.check, timeout=15,
                                         default=False, name="license_check")
        except Exception as e:
            log_msg(f"      [license] EXCEPTION: {e}", "warning")
            _lic_ok = False

        if _lic_ok:
            log_msg(f"      [license] OK")
            break

        license_state = license_client.snapshot()
        dashboard.update(license=license_state, bot_running=False,
                          bot_status="License required")
        log_msg(f"      [license] failed - retry in {_license_retry_delay}s", "warning")
        if _license_attempt['n'] >= 30:
            log_msg(f"      [license] All attempts failed. Check internet/LICENSE_EMAIL.", "error")
        time.sleep(_license_retry_delay)
        _license_retry_delay = min(_license_retry_delay * 2, 900)

    dashboard.update(license=license_client.snapshot())
    dashboard.authorize_license_startup()

    credentials = dashboard.get_credentials() or dashboard.wait_for_credentials()
    initial_settings = dashboard.get_runtime_settings()
    iq = connect_iq_with_heartbeat(
        on_status=update_conn, credentials=credentials,
        credentials_provider=dashboard.get_credentials, account_type=ACCOUNT_TYPE,
        account_type_provider=dashboard.get_account_type,
    )
    applied_conn_rev = initial_settings["connection_revision"]
    api_key = initial_settings["api_key"]
    pv = PriceValidator(api_key) if (VALIDATE_CANDLE and _HAS_PV and api_key) else None

    _iq_holder['iq'] = iq
    threading.Thread(
        target=_chart_live_updater_thread,
        args=(dashboard,),
        daemon=True,
        name="ChartLiveUpdater",
    ).start()
    threading.Thread(target=_watchdog_thread, args=(dashboard,), daemon=True,
                     name="Watchdog").start()
    _heartbeat()

    risk_mgr = RiskManager()
    stats = StatsManager(account_type=initial_settings["account_type"])
    _stats_holder['stats'] = stats
    _risk_mgr_holder['manager'] = risk_mgr
    tracker = TradeTracker(
        broker=iq,
        balance_provider=lambda: _get_balance(
            _iq_holder.get('iq'), default=None, name="tracker_balance"
        ),
        journal_path='trades.jsonl',
        logger=lambda message: log_msg(message, "trade"),
        on_resolved=_handle_tracker_resolution,
        result_provider=get_trade_result,
        candles_provider=_get_trade_expiry_candles,
        call_timeout=TIMEOUT_GET_BALANCE,
        resolution_lock=_pending_results_lock,
    )
    _tracker_holder['tracker'] = tracker
    tracker.start()
    log_msg("🧾 trade_tracker: ACTIVE (executor + balance + candle fallback)", "info")

    threading.Thread(target=_balance_updater_thread, args=(dashboard,), daemon=True,
                     name="BalanceUpdater").start()

    def _bot_status_snapshot():
        return stats.status_snapshot(current_balance=_last_bal["value"])

    license_client.set_bot_status_stats_provider(_bot_status_snapshot)

    def _poll_results_during_countdown():
        now = time.time()
        if now - _countdown_poll['last'] < COUNTDOWN_POLL_INTERVAL:
            return
        _countdown_poll['last'] = now
        iq_client = _iq_holder.get("iq")
        if iq_client is not None:
            check_pending_results(iq_client, stats, risk_mgr)

    global _countdown_result_callback
    _countdown_result_callback = _poll_results_during_countdown

    log_msg("🔍 [STARTUP] Reading balance (retrying until a positive balance is available)...")
    startup_bal = None
    startup_attempt = 0
    while startup_bal is None or startup_bal <= 0:
        if _stop_event.is_set():
            break
        startup_attempt += 1
        startup_bal = _get_balance(iq, default=None, name="startup_balance",
                                    retries=2, retry_delay=2.0)
        if startup_bal is not None and startup_bal > 0:
            log_msg(f"   ✓ attempt {startup_attempt}: ${startup_bal:.2f}")
            break
        if startup_attempt == 1 or startup_attempt % 5 == 0:
            log_msg(
                f"   ✗ attempt {startup_attempt} failed; will keep retrying",
                "warning"
            )
        _stop_event.wait(3)

    log_msg(f"   Config account_type: {ACCOUNT_TYPE}")
    log_msg(f"   Runtime account_type: {initial_settings['account_type']}")

    if startup_bal is None or startup_bal <= 0:
        log_msg(
            "❌ [CRITICAL] Startup stopped before a positive balance was available — "
            "ALL TRADES BLOCKED until balance available",
            "error"
        )
        _balance_unavailable = True
        dashboard.update(current_balance=0.0,
                          bot_status="⚠️ Balance unavailable — NO TRADING")
    else:
        log_msg(f"✅ [STARTUP] Balance: ${startup_bal:.2f}")
        _balance_unavailable = False
        stats.set_start_balance(startup_bal)
        dashboard.update(current_balance=startup_bal, connection_status="connected")
        _last_bal['value'] = startup_bal

        try:
            stake_check = dashboard.get_invest_amount()
            if stake_check is not None:
                if startup_bal < float(stake_check):
                    log_msg(
                        f"⚠️ [CRITICAL] Balance ${startup_bal:.2f} < Stake ${float(stake_check):.2f} "
                        f"— trades will FAIL.",
                        "error"
                    )
                else:
                    log_msg(f"✅ Balance ${startup_bal:.2f} >= Stake ${float(stake_check):.2f}")
        except Exception:
            pass

    log_msg(f"Trading asset: {TRADING_ASSET}")
    log_msg(f"Option type: {OPTION_TYPE} | Expiry: {TRADE_EXPIRY_MINUTES}m")
    log_msg(
        f"Regime range sanity: "
        f"{'ON' if ENABLE_REGIME_RANGE_SANITY else 'OFF'} | "
        f"lookback={REGIME_RANGE_LOOKBACK} candles | "
        f"ATR scale={REGIME_RANGE_ATR_SCALE:.2f}"
    )
    log_msg(
        f"Market structure: {'ON' if ENABLE_MARKET_STRUCTURE else 'OFF'} "
        f"swing L/R={STRUCTURE_SWING_LEFT}/{STRUCTURE_SWING_RIGHT} | "
        f"prominence={STRUCTURE_MIN_PROMINENCE * 10000:.1f}p | "
        f"equal={STRUCTURE_EQUAL_TOLERANCE * 10000:.1f}p | "
        f"BOS buffer={STRUCTURE_BOS_BUFFER * 10000:.1f}p | "
        f"swing retest={'ON' if ENABLE_SWING_RETEST_CONFIRMATION else 'OFF'} "
        f"tol={STRUCTURE_RETEST_TOLERANCE * 10000:.1f}p/"
        f"{STRUCTURE_RETEST_MAX_BARS} bars | bonus={SWING_RETEST_STAR_BONUS}*"
    )
    log_msg(f"5-FILTER: F1>={MIN_STARS_FOR_OPPOSITE_M5}* | "
            f"F2>={MIN_STARS_FOR_VOLUME_BYPASS}* "
            f"(strict bypass>={F2_STRICT_MIN_SETUP_STARS}* + zone + strong/B&R) | "
            f"F3>={MIN_STARS_FOR_WEAK_PA}* | F4>={MIN_STARS_FOR_M5_M15_CONFLICT}* | "
            f"F5>={MIN_STARS_FOR_NO_ZONE_CONFLUENCE}* | F6>={MIN_STARS_FOR_SIDEWAYS}*")
    log_msg(
        f"PA Momentum pattern: "
        f"{'ON' if ENABLE_MOMENTUM_PA_PATTERN else 'OFF'} | "
        f"body>={MOMENTUM_PA_MIN_BODY_PIPS:.1f}p | "
        f"body/range>={MOMENTUM_PA_MIN_BODY_RATIO:.2f}"
    )
    log_msg(f"v8.2: M15 high severity requires {M15_HIGH_SEVERITY_STARS_REQUIRED}★")
    log_msg(f"Trade counter: {_trades_today['date']} count={_trades_today['count']}")
    log_msg("Press Ctrl+C to stop.\n")

    try:
        cur_bal = _get_balance(iq, default=0.0, name="initial_dashboard_balance")
        stats.display_dashboard(cur_bal)
    except Exception:
        pass

    consecutive_failures = 0
    last_cleanup = time.time()

    while True:
        try:
            dashboard.update(
                current_signal="SKIP",
                current_signal_reason="Preparing the next scan.",
            )
            if not license_client.check():
                dashboard.update(
                    current_signal_reason="License check failed; scanning is paused.",
                )
                license_state = license_client.snapshot()
                if dashboard.is_running():
                    dashboard.pause_for_license(license_state)
                else:
                    dashboard.update(license=license_state)
                _poll_results_during_countdown()
                _stop_event.wait(2)
                continue
            dashboard.update(license=license_client.snapshot())
            _heartbeat()

            if time.time() - last_cleanup > 600:
                cleanup_old_results(max_age_seconds=3600)
                last_cleanup = time.time()

            runtime_settings = dashboard.get_runtime_settings()
            if runtime_settings["connection_revision"] != applied_conn_rev:
                iq = connect_iq_with_heartbeat(
                    on_status=update_conn, credentials=runtime_settings["credentials"],
                    credentials_provider=dashboard.get_credentials,
                    account_type=runtime_settings["account_type"],
                    account_type_provider=dashboard.get_account_type,
                )
                _iq_holder["iq"] = iq
                tracker.broker = iq
                stats = StatsManager(account_type=runtime_settings["account_type"])
                _stats_holder['stats'] = stats
                _last_bal["value"] = None
                _invalidate_candle_cache()
                _invalidate_connection_cache()
                license_client.set_bot_status_stats_provider(_bot_status_snapshot)
                _last_processed_candle_from['ts'] = None
                try:
                    balance = _get_balance(iq, default=None, name="account_switch_balance")
                    if balance is not None:
                        stats.set_start_balance(balance)
                        dashboard.update(current_balance=balance, connection_status="connected")
                        _last_bal["value"] = float(balance)
                        _balance_unavailable = False
                except Exception:
                    logging.exception("balance update error")
                applied_conn_rev = runtime_settings["connection_revision"]
                continue

            if runtime_settings["api_key"] != api_key:
                api_key = runtime_settings["api_key"]
                pv = PriceValidator(api_key) if (VALIDATE_CANDLE and _HAS_PV and api_key) else None

            if not dashboard.is_running():
                dashboard.update(bot_status="Paused")
                dashboard.update(
                    current_signal_reason="Bot is paused; no signal scan is running.",
                )
                _poll_results_during_countdown()
                dashboard.wait_for_resume(timeout=2)
                continue

            if _meta_brain is not None:
                cb_ok, cb_reason = _meta_brain.can_trade_now()
                if not cb_ok:
                    dashboard.update(bot_status=f"Circuit Breaker: {cb_reason}")
                    dashboard.update(
                        current_signal_reason=(
                            f"Circuit breaker active: {cb_reason}"
                        ),
                    )
                    _poll_results_during_countdown()
                    _heartbeat()
                    _stop_event.wait(5)
                    continue

            now = datetime.now()
            scan_sequence += 1
            scan_context = {
                "run_id": run_id,
                "scan_id": f"{run_id}_{scan_sequence:06d}_{TRADING_ASSET}",
                "asset": TRADING_ASSET,
                "candle_ts": None,
                "m5_pos": None,
                "direction": None,
                "pattern": None,
                "patterns": [],
                "stars": None,
            }
            calendar_weekend = ENABLE_WEEKEND_GUARD and now.weekday() >= 5
            scan_started_late = not _is_live_entry_window_open(now, now)
            _emit_trade_debug_event(
                "scan_started",
                scan_context,
                started_at=now.astimezone().isoformat(timespec="seconds"),
                scan_started_late=scan_started_late,
            )
            _emit_gate_event(
                scan_context,
                "SCAN_LATE",
                "skip" if scan_started_late else "pass",
                (
                    f"scan started at second {now.second}; live order disabled"
                    if scan_started_late
                    else "scan started within the live-entry window"
                ),
            )
            dashboard.update(bot_status="Waiting for next candle")
            dashboard.update(
                current_signal="SKIP",
                current_signal_reason="Waiting for the next scan.",
            )

            if scan_started_late:
                log_msg(
                    f"      ⚠️ [SCAN-LATE] started at second {now.second}; "
                    "analyze latest closed candle, live order disabled",
                    "warning",
                )

            now_str = now.strftime('%H:%M:%S')
            session = detect_session(now)
            dashboard.update(
                bot_status="Scanning",
                last_scan=now_str,
                session=session,
                current_signal="SKIP",
                current_signal_reason="Analyzing the current candle.",
            )
            log_msg(f"\n[{now_str}] --- SCANNING (Session: {session}) ---")
            _flush_phase_times()

            with _phase("check_connect"):
                connected = _is_connected_cached(iq)
            if not connected:
                _emit_gate_event(
                    scan_context,
                    "CONNECTION",
                    "block",
                    "IQ Option connection is unavailable; reconnecting",
                )
                log_msg("Connection lost; reconnecting...", "warning")
                dashboard.update(connection_status="disconnected")
                iq = connect_iq_with_heartbeat(
                    on_status=update_conn, credentials=dashboard.get_credentials(),
                    credentials_provider=dashboard.get_credentials,
                    account_type=dashboard.get_account_type(),
                    account_type_provider=dashboard.get_account_type,
                )
                _iq_holder['iq'] = iq
                tracker.broker = iq
                _invalidate_candle_cache()
                _invalidate_connection_cache()
                _last_processed_candle_from['ts'] = None
                continue
            _emit_gate_event(
                scan_context,
                "CONNECTION",
                "pass",
                "IQ Option connection is available",
            )
            dashboard.update(connection_status="connected")

            if check_pending_results(iq, stats, risk_mgr):
                try:
                    with _phase("result_balance"):
                        bal_now = _get_balance(iq, default=0.0, name="result_balance")
                    stats.display_dashboard(bal_now)
                except Exception:
                    pass

            asset_status = "UNKNOWN"
            if ENABLE_ASSET_CHECK:
                asset_status, from_cache = check_asset_status(
                    iq, force_refresh=calendar_weekend
                )
                tag = "[cache]" if from_cache else "[fresh]"
                dashboard.update(asset=TRADING_ASSET, asset_status=asset_status,
                                  trade_availability=asset_status)
                if asset_status == "OPEN":
                    log_msg(f"      Asset: {TRADING_ASSET} OPEN {tag}")
                elif asset_status == "CLOSED":
                    log_msg(f"      Asset: {TRADING_ASSET} CLOSED {tag}", "warning")
                else:
                    log_msg(f"      Asset: {TRADING_ASSET} UNKNOWN {tag}")

            _emit_gate_event(
                scan_context,
                "ASSET_AVAILABILITY",
                "block" if ENABLE_ASSET_CHECK and asset_status != "OPEN" else "pass",
                (
                    f"{TRADING_ASSET} status is {asset_status}"
                    if ENABLE_ASSET_CHECK
                    else "asset availability check disabled"
                ),
                evaluated=ENABLE_ASSET_CHECK,
            )
            if asset_status == "CLOSED" and not calendar_weekend:
                log_msg(
                    f"      ⛔ [ASSET-CLOSED] {TRADING_ASSET} is closed outside "
                    "calendar weekend → skip",
                    "warning",
                )
                dashboard.update(bot_status="Asset closed")
                wait_until_next_minute(dashboard)
                continue

            weekend_observation = _is_weekend_observation(
                now, asset_status, asset_check_enabled=ENABLE_ASSET_CHECK
            )
            if weekend_observation:
                weekday_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][now.weekday()]
                log_msg(
                    f"      Weekend ({weekday_name}) - observation only "
                    f"(broker availability: {asset_status})",
                    "warning",
                )
                dashboard.update(bot_status=f"Weekend observation ({weekday_name})")
            elif calendar_weekend:
                log_msg(
                    f"      ✅ [WEEKEND-GUARD] broker reports {TRADING_ASSET} OPEN "
                    "on fresh check; trading allowed",
                    "trade",
                )
            _emit_gate_event(
                scan_context,
                "WEEKEND_GUARD",
                "skip" if weekend_observation else "pass",
                (
                    f"weekend observation only; broker status={asset_status}"
                    if weekend_observation
                    else "weekend trade guard permits this scan"
                    if calendar_weekend
                    else "not a weekend observation"
                ),
                evaluated=ENABLE_WEEKEND_GUARD,
            )

            will_trade = not weekend_observation and not scan_started_late
            if ENABLE_ASSET_CHECK and asset_status != "OPEN":
                will_trade = False

            risk_allows_trade = risk_mgr.can_trade()
            _emit_gate_event(
                scan_context,
                "RISK_MANAGER",
                "pass" if risk_allows_trade else "block",
                "risk manager permits order placement"
                if risk_allows_trade
                else "risk manager paused order placement",
            )
            if not risk_allows_trade:
                if will_trade:
                    log_msg("Risk manager paused order placement", "warning")
                will_trade = False

            cur_balance, (df, df_m5, df_m15) = _fetch_scan_inputs(iq)
            if cur_balance is not None and cur_balance > 0:
                dashboard.update(current_balance=cur_balance)

            if df is None or len(df) < 50:
                _emit_gate_event(
                    scan_context,
                    "CANDLE_DATA",
                    "block",
                    "M1 candle data missing or fewer than 50 candles",
                )
                consecutive_failures += 1
                log_msg(f"Unable to retrieve M1 data (attempt {consecutive_failures})", "error")
                if consecutive_failures >= 3:
                    countdown(60, "No M1 data", dashboard)
                else:
                    countdown(10, "Retrying", dashboard)
                continue
            _emit_gate_event(
                scan_context,
                "CANDLE_DATA",
                "pass",
                f"received {len(df)} M1 candles",
            )
            consecutive_failures = 0

            if 'from' in df.columns:
                if df.iloc[-1]['from'] + CANDLE_TIME > time.time():
                    df = df.iloc[:-1].reset_index(drop=True)

            if len(df) < 50:
                _emit_gate_event(
                    scan_context,
                    "CANDLE_DATA",
                    "block",
                    "fewer than 50 closed M1 candles",
                )
                log_msg("      Not enough closed candles")
                wait_until_next_minute(dashboard)
                continue

            last = df.iloc[-1]
            prev = df.iloc[-2]
            atr_params = get_atr_scaled_params(df)

            log_msg(
                f"      📏 [ATR14={atr_params['atr_pips']}p] "
                f"ZoneTol={atr_params['zone_tol_minor'] * 10000:.1f}p | "
                f"RR_min={atr_params['min_rr_edge_pips']}p | "
                f"NewsThr={atr_params['news_range_threshold'] * 10000:.0f}p",
                "trade"
            )
            current_from = float(last['from']) if 'from' in df.columns else None
            m5_position = _m5_position_for_scan_time(now)
            skip_live_trade_m5_slot = _should_skip_live_trade_for_m5_position(
                m5_position
            )
            try:
                candle_ts = (
                    datetime.fromtimestamp(current_from)
                    .astimezone()
                    .isoformat(timespec="seconds")
                    if current_from is not None
                    else None
                )
            except (OSError, OverflowError, ValueError) as error:
                candle_ts = str(current_from)
                logging.warning(
                    "Unable to format candle timestamp for run_id=%s scan_id=%s: %s",
                    run_id,
                    scan_context["scan_id"],
                    error,
                )
            scan_context["candle_ts"] = candle_ts
            scan_context["m5_pos"] = m5_position
            _emit_gate_event(
                scan_context,
                "M5_TIMING",
                "skip" if skip_live_trade_m5_slot else "pass",
                (
                    f"M{m5_position} candle is analysis-only"
                    if skip_live_trade_m5_slot
                    else f"M{m5_position} candle permits live timing"
                ),
            )
            if skip_live_trade_m5_slot:
                will_trade = False
                dashboard.update(
                    current_signal="SKIP",
                    current_signal_reason=(
                        f"M{m5_position} candle: analyze patterns; live trade disabled."
                    ),
                )

            if ENABLE_STALE_GUARD:
                try:
                    current_ohlc = (float(last['open']), float(last['high']),
                                    float(last['low']), float(last['close']))
                    if _stale_tracker['ohlc'] == current_ohlc:
                        _stale_tracker['count'] += 1
                        if _stale_tracker['count'] >= STALE_THRESHOLD:
                            _emit_gate_event(
                                scan_context,
                                "STALE_DATA",
                                "block",
                                f"unchanged OHLC for {_stale_tracker['count']} candles",
                            )
                            log_msg(f"      🛑 Frozen data for {_stale_tracker['count']} candles",
                                    "warning")
                            dashboard.update(bot_status="Frozen data")
                            countdown(STALE_SLEEP_SECONDS, "Stale data", dashboard)
                            _stale_tracker['count'] = 0
                            continue
                    else:
                        _stale_tracker['ohlc'] = current_ohlc
                        _stale_tracker['count'] = 0
                    _emit_gate_event(
                        scan_context,
                        "STALE_DATA",
                        "pass",
                        "candle OHLC changed or stale threshold not reached",
                    )
                except (KeyError, TypeError, ValueError) as error:
                    logging.warning("Stale-data check failed: %s", error)
                    _emit_gate_event(
                        scan_context,
                        "STALE_DATA",
                        "fallback_bypass",
                        f"{type(error).__name__}: {error}",
                        fallback=True,
                        evaluated=False,
                    )
            else:
                _emit_gate_event(
                    scan_context,
                    "STALE_DATA",
                    "pass",
                    "stale-data guard disabled",
                    evaluated=False,
                )

            chart_candles = prepare_chart_candles(df)
            if chart_candles:
                dashboard.update(chart_candles=chart_candles, last_candle=chart_candles[-1],
                                  last_price=chart_candles[-1]["close"])

            log_msg(f"      M1: O={last['open']:.5f} H={last['high']:.5f} "
                    f"L={last['low']:.5f} C={last['close']:.5f}")

            df_m5_closed = _closed_candles(df_m5, 300)
            if df_m5_closed is None or len(df_m5_closed) < 20:
                _emit_gate_event(
                    scan_context,
                    "M5_DATA",
                    "block",
                    "M5 data missing or fewer than 20 closed candles",
                )
                log_msg("      Not enough M5 data")
                wait_until_next_minute(dashboard)
                continue
            _emit_gate_event(
                scan_context,
                "M5_DATA",
                "pass",
                f"received {len(df_m5_closed)} closed M5 candles",
            )

            trend, sideways = trend_369(df_m5_closed, adx_threshold=ADX_THRESHOLD)
            log_msg(f"      M5: {_trend_str(trend, sideways)}")

            t15, s15 = 0, True
            df_m15_closed = None
            if (
                ENABLE_M15_TREND
                or ENABLE_M15_ZONE_SYSTEM
                or ENABLE_MARKET_STRUCTURE
            ):
                if df_m15 is not None:
                    df_m15_closed = _closed_candles(df_m15, 900)
                if (
                    ENABLE_M15_TREND
                    and df_m15_closed is not None
                    and len(df_m15_closed) >= 50
                ):
                    t15, s15 = trend_369(df_m15_closed)
                    log_msg(f"      M15: {_trend_str(t15, s15)}")
                elif ENABLE_M15_TREND:
                    log_msg("      M15: insufficient closed data for trend")

            market_structure = {}
            structure_m5_trend = None
            if df_m5_closed is not None and len(df_m5_closed) >= 20:
                structure_m5_trend, _ = trend_369(df_m5_closed)
            if ENABLE_MARKET_STRUCTURE:
                structure_frames = (
                    ("M1", df),
                    ("M5", df_m5_closed),
                    ("M15", df_m15_closed),
                )
                structure_log_parts = []
                for timeframe, structure_df in structure_frames:
                    if structure_df is None or structure_df.empty:
                        market_structure[timeframe] = {
                            "ready": False,
                            "structure": "UNAVAILABLE",
                            "swing_high": None,
                            "swing_high_label": None,
                            "swing_low": None,
                            "swing_low_label": None,
                            "bos": "NONE",
                            "bos_level": None,
                            "bos_kind": "NONE",
                            "structure_retest": "NONE",
                            "structure_retest_level": None,
                            "structure_retest_kind": "NONE",
                            "structure_event": "NONE",
                        }
                        continue

                    try:
                        structure_df = analyze_market_structure(
                            structure_df,
                            left=STRUCTURE_SWING_LEFT,
                            right=STRUCTURE_SWING_RIGHT,
                            min_prominence=STRUCTURE_MIN_PROMINENCE,
                            equal_tolerance=STRUCTURE_EQUAL_TOLERANCE,
                            min_break_buffer=STRUCTURE_BOS_BUFFER,
                            retest_tolerance=STRUCTURE_RETEST_TOLERANCE,
                            retest_max_bars=STRUCTURE_RETEST_MAX_BARS,
                        )
                        snapshot = market_structure_snapshot(structure_df)
                    except (KeyError, TypeError, ValueError, OverflowError) as exc:
                        log_msg(
                            f"      ⚠️ [STRUCTURE] {timeframe} analysis failed: {exc}",
                            "warning",
                        )
                        market_structure[timeframe] = {
                            "ready": False,
                            "structure": "ERROR",
                            "swing_high": None,
                            "swing_high_label": None,
                            "swing_low": None,
                            "swing_low_label": None,
                            "bos": "NONE",
                            "bos_level": None,
                            "bos_kind": "NONE",
                            "structure_retest": "NONE",
                            "structure_retest_level": None,
                            "structure_retest_kind": "NONE",
                            "structure_event": "NONE",
                        }
                        continue

                    market_structure[timeframe] = snapshot
                    last_structure_candle = structure_df.iloc[-1]
                    candle_key = (
                        last_structure_candle["from"]
                        if "from" in structure_df.columns
                        else tuple(
                            float(last_structure_candle[column])
                            for column in ("high", "low", "close")
                        )
                    )
                    if _market_structure_last_logged.get(timeframe) != candle_key:
                        high_label = snapshot["swing_high_label"] or "--"
                        low_label = snapshot["swing_low_label"] or "--"
                        high_value = snapshot["swing_high"]
                        low_value = snapshot["swing_low"]
                        high_text = (
                            f"{high_label}@{high_value:.5f}"
                            if high_value is not None else "--"
                        )
                        low_text = (
                            f"{low_label}@{low_value:.5f}"
                            if low_value is not None else "--"
                        )
                        bos_text = snapshot["bos"]
                        if snapshot["bos_level"] is not None:
                            bos_text += f"@{snapshot['bos_level']:.5f}"
                        retest_text = snapshot["structure_retest"]
                        if snapshot["structure_retest_level"] is not None:
                            retest_text += (
                                f"@{snapshot['structure_retest_level']:.5f}"
                            )
                        structure_log_parts.append(
                            f"{timeframe} {snapshot['structure']} "
                            f"H={high_text} L={low_text} BOS={bos_text} "
                            f"RETEST={retest_text} EVENT={snapshot['structure_event']}"
                        )
                        _market_structure_last_logged[timeframe] = candle_key

                if structure_log_parts:
                    log_msg(
                        "      [STRUCTURE] " + " | ".join(structure_log_parts),
                        "trade",
                    )

            zone_params_m5 = (
                get_atr_scaled_params(df_m5_closed)
                if len(df_m5_closed) >= 15
                else atr_params
            )
            zone_params_m15 = (
                get_atr_scaled_params(df_m15_closed)
                if df_m15_closed is not None and len(df_m15_closed) >= 15
                else atr_params
            )

            if _stale_tracker['count'] >= 2:
                cached_regime = _regime_tracker['last_stable']
                if cached_regime is None:
                    regime, _r_m5, _r_m15, _vol_ratio, _reg_conf = (
                        'MIXED', trend, t15, 1.0, 0.5
                    )
                else:
                    regime, _r_m5, _r_m15, _vol_ratio, _reg_conf = cached_regime
            elif ENABLE_REGIME_DETECT:
                regime_df_m15 = df_m15_closed if ENABLE_M15_TREND else None
                regime, _r_m5, _r_m15, _vol_ratio, _reg_conf = detect_market_regime(
                    df_m5_closed, regime_df_m15
                )
            else:
                regime, _r_m5, _r_m15, _vol_ratio, _reg_conf = 'MIXED', trend, t15, 1.0, 0.5
            if _stale_tracker['count'] == 0:
                _regime_tracker['last_stable'] = (
                    regime, _r_m5, _r_m15, _vol_ratio, _reg_conf
                )
            if _meta_brain is not None and _stale_tracker['count'] == 0:
                _meta_brain.observe_market_tick(regime)
            log_msg(f"      Regime: {regime} (score={_reg_conf:.2f}, vol={_vol_ratio:.2f}x) | Session: {session}")

            if current_from is not None and current_from == _last_processed_candle_from['ts']:
                log_msg("      Candle unchanged")
                wait_until_next_minute(dashboard)
                continue
            _last_processed_candle_from['ts'] = current_from

            # === v8.7.R2: ATR-Norm + Exhaustion detection ===
            m1_strength = (
                _get_trend_strength(df)
                if ENABLE_ATR_NORMALIZED_CONFLUENCE else 0.0
            )
            m5_strength = (
                _get_trend_strength(df_m5_closed)
                if ENABLE_ATR_NORMALIZED_CONFLUENCE else 0.0
            )
            m15_strength = (
                _get_trend_strength(df_m15_closed)
                if ENABLE_ATR_NORMALIZED_CONFLUENCE and df_m15_closed is not None
                else 0.0
            )

            if ENABLE_ATR_NORMALIZED_CONFLUENCE:
                log_msg(
                    f"      📊 [ATR-NORM] M1={m1_strength:.1f} "
                    f"M5={m5_strength:.1f} M15={m15_strength:.1f} ATR units",
                    "trade"
                )

            m15_extended = False
            m5_compressed = False
            compression_ratio = 0.0
            if ENABLE_EXHAUSTION_REVERSAL and df_m15_closed is not None:
                m15_extended = _is_m15_extended(df_m15_closed)
                m5_compressed, compression_ratio = _is_m5_compressed_in_m15(
                    df_m5_closed, df_m15_closed
                )
                if m15_extended and m5_compressed:
                    log_msg(
                        f"      🎯 [EXHAUSTION-REVERSAL] M15 extended + "
                        f"M5 compressed (ratio={compression_ratio:.2f}) "
                        f"→ reversal setup detected",
                        "trade"
                    )

            with _phase("zones"):
                zone_mgr.tolerance_minor = atr_params['zone_tol_minor']
                zone_mgr.tolerance_major = atr_params['zone_tol_major']
                zone = zone_mgr.update(df)
                zone_mgr_m5.tolerance_minor = zone_params_m5['zone_tol_minor']
                zone_mgr_m5.tolerance_major = zone_params_m5['zone_tol_major']
                zone_m5 = zone_mgr_m5.update(df_m5_closed)
                zone_m15 = None
                if (
                    ENABLE_M15_ZONE_SYSTEM
                    and df_m15_closed is not None
                    and len(df_m15_closed) >= 3
                ):
                    zone_mgr_m15.tolerance_minor = zone_params_m15['zone_tol_minor']
                    zone_mgr_m15.tolerance_major = zone_params_m15['zone_tol_major']
                    zone_m15 = zone_mgr_m15.update(df_m15_closed)

            _emit_trade_debug_event("market_observation", scan_context, asset=TRADING_ASSET,
                            timeframe_seconds=CANDLE_TIME, candle_timestamp=current_from,
                            market_status=asset_status, weekend_observation=weekend_observation,
                            orders_allowed=will_trade,
                            scan_started_late=scan_started_late,
                            scan_start_second=now.second,
                            m5_candle_position=m5_position,
                            m5_boundary_live_trade_skipped=skip_live_trade_m5_slot,
                            candle={k: last[k] for k in ("open","high","low","close") if k in last},
                            previous_candle={k: prev[k] for k in ("open","high","low","close") if k in prev},
                            trend_m5=trend, sideways_m5=sideways, trend_m15=t15, sideways_m15=s15,
                            regime=regime, volatility_ratio=_vol_ratio, session=session)

            with _phase("pa_scan"):
                pa_patterns = scan_all_pa_patterns(
                    df,
                    tweezer_tol=atr_params['tweezer_tol'],
                    strat_target_pips=atr_params['strat_target_pips'],
                )
            pa_summary = _format_pa_pattern_summary(pa_patterns)
            scan_context["patterns"] = list(pa_patterns)
            scan_context["pattern"] = next(iter(pa_patterns), None)

            if skip_live_trade_m5_slot:
                _emit_gate_event(
                    scan_context,
                    "CANDLE_QUALITY",
                    "skip",
                    "M0/M4 analysis-only; live candle-quality gate not applied",
                    evaluated=False,
                )
            elif not is_valid_signal_candle(df):
                _emit_gate_event(
                    scan_context,
                    "CANDLE_QUALITY",
                    "block",
                    "flat/Doji/Spinning Top candle",
                )
                reason = "flat/Doji/Spinning Top candle"
                log_msg(f"      ⛔ [CANDLE-QUALITY] {reason} → skip", "warning")
                log_msg(f"      [PA-ANALYSIS-ONLY] {pa_summary}", "trade")
                wait_until_next_minute(dashboard, signal_reason=reason)
                continue
            else:
                _emit_gate_event(
                    scan_context,
                    "CANDLE_QUALITY",
                    "pass",
                    "signal candle passed candle-quality checks",
                )

            if skip_live_trade_m5_slot:
                _emit_gate_event(
                    scan_context,
                    "GAP",
                    "skip",
                    "M0/M4 analysis-only; live gap gate not applied",
                    evaluated=False,
                )
            elif check_gap(df):
                _emit_gate_event(scan_context, "GAP", "block", "gap candle detected")
                reason = "gap candle detected"
                log_msg(f"      ⛔ [GAP] {reason} → skip", "warning")
                log_msg(f"      [PA-ANALYSIS-ONLY] {pa_summary}", "trade")
                wait_until_next_minute(dashboard, signal_reason=reason)
                continue
            else:
                _emit_gate_event(scan_context, "GAP", "pass", "no gap candle detected")

            has_news, news_pips, news_idx = is_after_news(
                df, threshold=atr_params['news_range_threshold'])
            if skip_live_trade_m5_slot:
                _emit_gate_event(
                    scan_context,
                    "NEWS",
                    "skip",
                    "M0/M4 analysis-only; news cooldown does not control live entry",
                    evaluated=False,
                    would_block=bool(has_news),
                )
            elif has_news:
                _emit_gate_event(
                    scan_context,
                    "NEWS",
                    "block",
                    f"news cooldown: -{news_idx}, range {news_pips:.0f} pips",
                )
                reason = f"news cooldown: -{news_idx}, range {news_pips:.0f} pips"
                log_msg(f"      ⛔ [NEWS-COOLDOWN] {reason} → skip", "warning")
                log_msg(f"      [PA-ANALYSIS-ONLY] {pa_summary}", "trade")
                wait_until_next_minute(dashboard, signal_reason=reason)
                continue
            else:
                _emit_gate_event(scan_context, "NEWS", "pass", "no active news cooldown")

            chart_zone_grid = zone_mgr.get_grid(float(last["close"]))
            m15_zone_price = (
                float(df_m15_closed.iloc[-1]['close'])
                if df_m15_closed is not None and len(df_m15_closed)
                else float(last["close"])
            )
            if ENABLE_M15_ZONE_SYSTEM:
                chart_zone_grid += zone_mgr_m15.get_grid(m15_zone_price)
            dashboard.update(chart_zones=[
                {"value": float(v), "label": lbl, "type": zt}
                for v, lbl, zt, _tol in chart_zone_grid
            ])
            nearest_lbl = zone.get('nearest_label') or '?'
            nearest_val = zone.get('nearest') or 0.0
            nearest_type = zone.get('nearest_type') or '?'
            tf_tag = "M1/M5" if nearest_type == 'minor' else "M15" if nearest_type == 'big' else "?"
            log_msg(f"      Zone: {zone['state']} | Nearest: {nearest_lbl} @ {nearest_val:.5f} [{tf_tag}]")

            touched_zones_list = zone.get('touched_zones', [])
            zone_for_stars = dict(zone)
            zone_for_stars['touched_zones'] = (
                list(touched_zones_list) + zone_m5.get('touched_zones', [])
            )
            zone_for_stars['at_m5_zone'] = zone_m5.get('at_m1_zone', False)
            zone_stars_early, zone_is_edge_early, m1_at_early, m5_at_early = \
                _get_zone_stars_early(zone_for_stars, current_from, _middle_zone_log_tracker)

            if touched_zones_list:
                for lbl, ltype, zval in touched_zones_list:
                    ztf = "M15" if ltype == 'big' else "M1"
                    log_msg(f"      Candle touched [{ztf}] {lbl} @ {zval:.5f}")
            else:
                log_msg("      Candle did not touch any zone (strict)")

            for lbl, ltype, zval in zone.get('near_zones', []):
                ztf = "M15" if ltype == 'big' else "M1"
                dist_pips = abs(last['close'] - zval) * 10000
                log_msg(f"      Candle near [{ztf}] {lbl} @ {zval:.5f} ({dist_pips:.1f} pips)")

            for timeframe, timeframe_zone in (('M5', zone_m5), ('M15', zone_m15)):
                if timeframe_zone:
                    for lbl, _ltype, zval in timeframe_zone.get('touched_zones', []):
                        log_msg(
                            f"      Candle touched [{timeframe}] {lbl} @ {zval:.5f}",
                            "trade",
                        )
                    timeframe_df = df_m5_closed if timeframe == 'M5' else df_m15_closed
                    timeframe_close = (
                        float(timeframe_df.iloc[-1]['close'])
                        if timeframe_df is not None and len(timeframe_df)
                        else float(last['close'])
                    )
                    for lbl, _ltype, zval in timeframe_zone.get('near_zones', []):
                        dist_pips = abs(timeframe_close - zval) * 10000
                        log_msg(
                            f"      Zone near [{timeframe}] {lbl} @ {zval:.5f} "
                            f"({dist_pips:.1f} pips)",
                            "trade",
                        )
                    if timeframe_zone.get('reason'):
                        log_msg(
                            f"      [{timeframe} zone] {timeframe_zone['reason']}",
                            "trade",
                        )

            if zone.get('reason'):
                log_msg(f"      {zone['reason']}")

            if pv:
                ok, reason = _validate_price_candle(
                    pv, last.to_dict(), CANDLE_TOLERANCE_PIPS
                )
                if not ok:
                    _emit_gate_event(
                        scan_context,
                        "PRICE_VALIDATOR",
                        "block",
                        reason,
                    )
                    log_msg(f"      ⛔ [PRICE-VALIDATOR] {reason} → skip", "warning")
                    log_msg(f"      [PA-ANALYSIS-ONLY] {pa_summary}", "trade")
                    wait_until_next_minute(
                        dashboard, signal_reason=f"Price validation failed: {reason}"
                    )
                    continue
                _emit_gate_event(
                    scan_context,
                    "PRICE_VALIDATOR",
                    "pass",
                    "candle price validation passed",
                )
            else:
                _emit_gate_event(
                    scan_context,
                    "PRICE_VALIDATOR",
                    "pass",
                    "price validator is unavailable or disabled",
                    evaluated=False,
                )

            sig369 = {}
            strategy_name_369 = ""
            reason369 = ""
            if sideways and ENABLE_FAKEY and len(df) >= 3:
                sig369, reason369 = evaluate_369_fakey(
                    cfg369, last.to_dict(), df.iloc[-2].to_dict(), df.iloc[-3].to_dict())
                strategy_name_369 = "369-Fakey"
            elif trend != 0:
                sig369, reason369 = evaluate_369_pinbar(
                    cfg369, last.to_dict(), prev.to_dict(), trend)
                strategy_name_369 = "369-Pinbar"
            else:
                reason369 = "M5 flat"

            dir369 = normalize_direction(sig369.get('Direction')) if sig369 else None
            if sig369 and not dir369:
                log_msg(f"      {strategy_name_369 or '369'}: {reason369 or 'no direction'}")
                sig369 = {}

            if skip_live_trade_m5_slot:
                _emit_gate_event(
                    scan_context,
                    "SPINNING_TOP",
                    "skip",
                    "M0/M4 analysis-only; live spinning-top gate not applied",
                    evaluated=False,
                )
            elif pattern_spinning_top(df) == 999:
                _emit_gate_event(
                    scan_context,
                    "SPINNING_TOP",
                    "block",
                    "Spinning Top candle",
                )
                reason = "Spinning Top candle"
                log_msg(f"      ⛔ [SPINNING-TOP] {reason} → skip", "warning")
                log_msg(f"      [PA-ANALYSIS-ONLY] {pa_summary}", "trade")
                wait_until_next_minute(dashboard, signal_reason=reason)
                continue
            else:
                _emit_gate_event(
                    scan_context,
                    "SPINNING_TOP",
                    "pass",
                    "no Spinning Top candle",
                )

            if skip_live_trade_m5_slot:
                log_msg(
                    f"      [M5-ANALYSIS-ONLY] M{m5_position} candle patterns scanned; "
                    "live trade remains disabled",
                    "trade",
                )
            if shadow is not None and shadow_loop is not None:
                shadow_signals = []
                for pattern_name, pattern_direction in pa_patterns.items():
                    if (
                        pattern_name not in SHADOW_ENABLED_PATTERNS
                        or pattern_name in NO_VOTE_PATTERNS
                    ):
                        continue
                    direction = normalize_direction(pattern_direction)
                    if direction is not None:
                        shadow_signals.append((pattern_name, direction))
                if strategy_name_369 == "369-Pinbar" and dir369:
                    direction = normalize_direction(dir369)
                    if direction is not None:
                        shadow_signals.append((strategy_name_369, direction))

                if _is_shadow_market_available(
                    asset_status,
                    weekend_observation,
                    ENABLE_ASSET_CHECK,
                ):
                    shadow_context = {
                        "session": session,
                        "regime": regime,
                        "zone_stars": zone_stars_early,
                        "zone_is_edge": zone_is_edge_early,
                        "trend_m5": trend,
                        "trend_m15": t15,
                        "sideways": sideways,
                        "sideways_m15": s15,
                        "vol_ratio": _vol_ratio,
                    }
                    for pattern_name, direction in shadow_signals:
                        same_direction_pa = [
                            name
                            for name, pattern_direction in pa_patterns.items()
                            if pattern_direction == direction
                            and name not in NO_VOTE_PATTERNS
                        ]
                        signal_context = {
                            **shadow_context,
                            "pa_count": _count_independent_pa(same_direction_pa),
                        }
                        shadow_loop.call_soon_threadsafe(
                            shadow.submit,
                            ShadowSignal(
                                pattern=pattern_name,
                                direction=direction,
                                entry_price=float(last["close"]),
                                expiry_sec=TRADE_EXPIRY_SECONDS,
                                zone=str(zone.get("nearest_label") or ""),
                                context=signal_context,
                            ),
                        )
                elif shadow_signals:
                    log_msg(
                        f"      ⛔ [SHADOW-MARKET] availability={asset_status}, "
                        f"weekend_observation={weekend_observation} → skip",
                        "warning",
                    )
            log_msg(f"      PA: {pa_summary}")

            all_signals = []
            if dir369:
                all_signals.append((strategy_name_369, dir369, '369'))
            for name, pdir in pa_patterns.items():
                all_signals.append((name, pdir, 'PA'))

            br_signal, br_reason = evaluate_break_and_retest(df, zone, trend)
            if br_signal:
                log_msg(f"      🎯 BREAK & RETEST: {br_reason} -> {br_signal.upper()}")
                all_signals.append(("BreakRetest", br_signal, 'PA'))

            if not all_signals:
                _emit_gate_event(
                    scan_context,
                    "SIGNAL_DETECTION",
                    "skip",
                    "no directional signals found",
                )
                log_msg("      No signals found")
                wait_until_next_minute(dashboard)
                continue

            voting_signals = [s for s in all_signals if s[0] not in NO_VOTE_PATTERNS]
            neutral_names = [s[0] for s in all_signals if s[0] in NO_VOTE_PATTERNS]

            if not voting_signals:
                reason = (
                    f"Only neutral pattern(s): {', '.join(neutral_names)}; "
                    "no directional vote"
                )
                _emit_gate_event(
                    scan_context,
                    "SIGNAL_DETECTION",
                    "skip",
                    reason,
                    patterns=neutral_names,
                )
                log_msg(f"      ⛔ [NO-VOTE] {reason} → skip", "warning")
                wait_until_next_minute(dashboard, signal_reason=reason)
                continue

            _emit_gate_event(
                scan_context,
                "SIGNAL_DETECTION",
                "pass",
                f"{len(voting_signals)} directional vote(s) available",
                patterns=[signal[0] for signal in voting_signals],
            )
            calls = [s for s in voting_signals if s[1] == 'call']
            puts = [s for s in voting_signals if s[1] == 'put']
            swing_retest_action = _get_swing_retest_action(
                market_structure, structure_m5_trend
            )

            _atr_for_quality = atr_params['atr_raw']

            def _weight(sigs):
                if _knowledge_brain is None or not (ENABLE_KNOWLEDGE_BRAIN and _knowledge_brain.is_ready()):
                    weighted_signals = [
                        (signal, 2 if signal[0] in STRONG_REVERSAL_PATTERNS else 1)
                        for signal in sigs
                    ]
                    if ENABLE_PA_CORRELATION_FILTER:
                        return _sum_independent_vote_weights(weighted_signals)
                    return sum(weight for _signal, weight in weighted_signals)

                pa_names_side = [s[0] for s in sigs if s[2] == 'PA' and s[0] != 'BreakRetest']
                if ENABLE_PA_CORRELATION_FILTER:
                    pa_eff_side = _count_independent_pa(pa_names_side)
                else:
                    pa_eff_side = len(pa_names_side)

                kb_ctx_side = {
                    'session': session,
                    'regime': regime,
                    'zone_stars': zone_stars_early,
                    'zone_is_edge': zone_is_edge_early,
                    'trend_m5': trend,
                    'trend_m15': t15,
                    'sideways': sideways,
                    'sideways_m15': s15,
                    'vol_ratio': _vol_ratio,
                    'pa_count': pa_eff_side,
                }

                weighted_signals = []
                for s in sigs:
                    base_w = 2 if s[0] in STRONG_REVERSAL_PATTERNS else 1
                    try:
                        q, kb_fallback, kb_error = _knowledge_brain.score_with_status(
                            s[0], s[1], kb_ctx_side, source="live"
                        )
                        if kb_fallback:
                            _emit_gate_event(
                                scan_context,
                                "KB_VOTE_WEIGHT",
                                "fallback_bypass",
                                f"{s[0]} scoring fallback: {kb_error}",
                                fallback=True,
                                evaluated=False,
                                pattern=s[0],
                            )
                        mult = _knowledge_brain.weight_multiplier(q)
                        weighted_w = base_w * mult
                        weighted_signals.append((s, weighted_w))
                        if KNOWLEDGE_BRAIN_VERBOSE:
                            log_msg(
                                f"      🧠 {s[0]} {s[1].upper()}: q={q:.3f} "
                                f"mult={mult:.2f} → w={weighted_w:.2f} (base={base_w})",
                                "trade")
                    except Exception as e:
                        logging.warning(f"KB weight {s[0]}: {e}")
                        _emit_gate_event(
                            scan_context,
                            "KB_VOTE_WEIGHT",
                            "fallback_bypass",
                            f"{s[0]}: {type(e).__name__}: {e}",
                            fallback=True,
                            evaluated=False,
                            pattern=s[0],
                        )
                        weighted_signals.append((s, base_w))
                if ENABLE_PA_CORRELATION_FILTER:
                    return _sum_independent_vote_weights(weighted_signals)
                return sum(weight for _signal, weight in weighted_signals)

            if calls and puts and ENABLE_WEIGHTED_CONFLICT:
                with _phase("kb_score"):
                    score_call = _weight(calls)
                    score_put = _weight(puts)
                diff = abs(score_call - score_put)
                if diff < CONFLICT_DIFF_MIN:
                    if swing_retest_action is None:
                        _emit_gate_event(
                            scan_context,
                            "CONFLICT_RESOLUTION",
                            "skip",
                            f"balanced votes: call={score_call:.2f}, "
                            f"put={score_put:.2f}, diff={diff:.2f}",
                        )
                        log_msg(f"      ⚠️ CONFLICT: call={score_call:.2f} vs put={score_put:.2f} "
                                f"(diff={diff:.2f} < {CONFLICT_DIFF_MIN}) -> skip")
                        wait_until_next_minute(dashboard)
                        continue
                    consensus_action = swing_retest_action
                    _emit_gate_event(
                        scan_context,
                        "CONFLICT_RESOLUTION",
                        "pass",
                        f"balanced votes resolved by M1 swing retest to "
                        f"{consensus_action.upper()}",
                    )
                    log_msg(
                        f"      ✅ [SWING-RETEST-CONFLICT] balanced votes resolved "
                        f"by M1 {consensus_action.upper()} BOS retest"
                    )
                else:
                    consensus_action = 'call' if score_call > score_put else 'put'
                    _emit_gate_event(
                        scan_context,
                        "CONFLICT_RESOLUTION",
                        "pass",
                        f"weighted votes resolved to {consensus_action.upper()}: "
                        f"call={score_call:.2f}, put={score_put:.2f}",
                    )
                    log_msg(f"      🔀 CONFLICT RESOLVED: call={score_call:.2f} vs put={score_put:.2f} "
                            f"-> {consensus_action.upper()}")
                agreeing = calls if consensus_action == 'call' else puts
            elif calls and puts:
                if swing_retest_action is None:
                    _emit_gate_event(
                        scan_context,
                        "CONFLICT_RESOLUTION",
                        "skip",
                        "strict directional conflict without swing-retet resolution",
                    )
                    log_msg(f"      CONFLICT (strict): skip")
                    wait_until_next_minute(dashboard)
                    continue
                consensus_action = swing_retest_action
                agreeing = calls if consensus_action == 'call' else puts
                _emit_gate_event(
                    scan_context,
                    "CONFLICT_RESOLUTION",
                    "pass",
                    f"strict conflict resolved by M1 swing retest to "
                    f"{consensus_action.upper()}",
                )
                log_msg(
                    f"      ✅ [SWING-RETEST-CONFLICT] conflict resolved by "
                    f"M1 {consensus_action.upper()} BOS retest"
                )
            else:
                consensus_action = 'call' if calls else 'put'
                agreeing = calls if calls else puts
                _emit_gate_event(
                    scan_context,
                    "CONFLICT_RESOLUTION",
                    "pass",
                    "only one directional side has votes",
                )

            dashboard.update(
                current_signal=consensus_action.upper(),
                current_signal_reason=(
                    f"{consensus_action.upper()} candidate detected; evaluating filters."
                ),
                last_signal=consensus_action.upper(),
            )
            agreeing_names = [s[0] for s in agreeing]
            scan_context["direction"] = consensus_action.upper()
            scan_context["patterns"] = agreeing_names
            scan_context["pattern"] = agreeing_names[0] if agreeing_names else None
            is_369_present = any(s[2] == '369' for s in agreeing)
            has_mau_M = any(s[0] == 'Mau-M' for s in agreeing)
            has_br = any(s[0] == 'BreakRetest' for s in agreeing)

            learning_filter_debug = {
                'enabled': ENABLE_LEARNING_FILTERS,
                'data_status': learning_filter_data_status,
                'status': 'disabled' if not ENABLE_LEARNING_FILTERS else 'unavailable',
                'matched_patterns': [],
            }
            if has_br:
                learning_filter_debug['status'] = 'break_retest_bypass'
                _emit_gate_event(
                    scan_context,
                    "LEARNING_FILTER",
                    "pass",
                    "valid BreakRetest bypass",
                    evaluated=False,
                )
            elif ENABLE_LEARNING_FILTERS and learning_filter_data_status == "active":
                blocked_patterns = [
                    {
                        'pattern': name,
                        **learning_blacklist[name],
                    }
                    for name in agreeing_names
                    if name in learning_blacklist
                ]
                learning_filter_debug['matched_patterns'] = blocked_patterns
                if blocked_patterns:
                    _emit_gate_event(
                        scan_context,
                        "LEARNING_FILTER",
                        "block",
                        ", ".join(item["pattern"] for item in blocked_patterns),
                    )
                    learning_filter_debug['status'] = 'blocked'
                    reason = ", ".join(
                        f"{item['pattern']} (n={item['n']}, "
                        f"WR={item['wr']:.1%})"
                        for item in blocked_patterns
                    )
                    log_msg(
                        f"      ⛔ [LEARNING-FILTER] {reason} → skip",
                        "warning",
                    )
                    _emit_trade_debug_event(
                        "learning_filter_block",
                        scan_context,
                        asset=TRADING_ASSET,
                        timeframe_seconds=CANDLE_TIME,
                        action=consensus_action,
                        agreed_patterns=agreeing_names,
                        learning_filter=learning_filter_debug,
                    )
                    wait_until_next_minute(dashboard)
                    continue
                learning_filter_debug['status'] = 'pass'
                _emit_gate_event(
                    scan_context,
                    "LEARNING_FILTER",
                    "pass",
                    "no learning blacklist match",
                )
            elif ENABLE_LEARNING_FILTERS:
                _emit_gate_event(
                    scan_context,
                    "LEARNING_FILTER",
                    "fallback_bypass",
                    f"learning report {learning_filter_data_status}; filter unavailable",
                    fallback=True,
                    evaluated=False,
                )
            else:
                _emit_gate_event(
                    scan_context,
                    "LEARNING_FILTER",
                    "pass",
                    "learning filter disabled",
                    evaluated=False,
                )

            pa_patterns_in_agree = [s[0] for s in agreeing
                                     if s[2] == 'PA' and s[0] != 'BreakRetest']
            if ENABLE_PA_CORRELATION_FILTER:
                pa_effective_count = _count_independent_pa(pa_patterns_in_agree)
                if pa_effective_count != len(pa_patterns_in_agree):
                    log_msg(
                        f"      🔗 PA correlation: {len(pa_patterns_in_agree)} detectors "
                        f"→ {pa_effective_count} independent",
                        "trade")
            else:
                pa_effective_count = len(pa_patterns_in_agree)
            # v8.7.R3.3: Block Star+MornStar alone (0/2 historical win rate)
            if set(pa_patterns_in_agree) == {'Star', 'MornStar'}:
                _emit_gate_event(
                    scan_context,
                    "PA_SUFFICIENCY",
                    "block",
                    "Star+MornStar alone (0/2 historical win rate) requires 3rd pattern",
                )
                log_msg(
                    "      ⛔ [PA-SUFFICIENCY] Star+MornStar alone — 0% win rate → skip",
                    "warning"
                )
                wait_until_next_minute(dashboard)
                continue

            m1_at = m1_at_early
            m5_at = m5_at_early
            minor_zone_bias_soft_pass = None
            minor_zone_entry_soft_passes = []
            minor_zone_soft_pass_reasons = []

            if zone['bias'] and zone_is_edge_early:
                z_action = 'call' if zone['bias'] == 'CALL' else 'put'
                if z_action == consensus_action:
                    _emit_gate_event(
                        scan_context,
                        "ZONE_BIAS",
                        "pass",
                        f"{zone['bias']} bias agrees with signal",
                    )
                    log_msg(f"      Zone bias ({zone['bias']}) + signals")
                    m1_at = True
                else:
                    zone_event = zone.get('zone_event') or {}
                    event_type = str(zone_event.get('type', '')).lower()
                    bias_zone_label = zone.get('bias_zone_label')
                    event_zone_label = zone_event.get('zone_label')
                    confirmed_override = (
                        event_type in {'retest', 'false_breakout'}
                        and bool(bias_zone_label)
                        and str(event_zone_label or '').strip().lower()
                        == str(bias_zone_label).strip().lower()
                        and is_counter_edge_breakout_confirmed(
                            bias_zone_label,
                            consensus_action,
                            zone.get('state'),
                            zone.get('reason'),
                            zone_value=zone_event.get('zone_value'),
                            previous_close=float(prev['close']),
                            candle_high=float(last['high']),
                            candle_low=float(last['low']),
                            candle_close=float(last['close']),
                            tolerance=zone_mgr.tolerance_minor,
                            breakout_margin=zone_mgr.U * 0.1,
                            event=zone_event,
                            active_breakout_label=zone.get('breakout_label'),
                            active_breakout_direction=zone.get('breakout_direction'),
                            active_breakout_value=zone.get('breakout_value'),
                        )
                    )
                    if confirmed_override:
                        _emit_gate_event(
                            scan_context,
                            "ZONE_BIAS",
                            "pass",
                            f"matching confirmed M1 {event_type} permits "
                            f"{consensus_action.upper()} against {zone['bias']} bias",
                        )
                        log_msg(
                            f"      ✅ [ZONE-BIAS-OVERRIDE] {event_type} "
                            f"{consensus_action.upper()} confirms "
                            f"{zone['bias']} zone {bias_zone_label}",
                            "trade",
                        )
                        m1_at = True
                    else:
                        can_defer_minor_zone_bias = (
                            ENABLE_MINOR_ZONE_TREND_OVERRIDE
                            and has_minor_zone_trend_alignment(
                                bias_zone_label,
                                consensus_action,
                                regime,
                                t15,
                                s15,
                            )
                        )
                        if can_defer_minor_zone_bias:
                            minor_zone_bias_soft_pass = (
                                f"M1 {bias_zone_label} opposes "
                                f"{consensus_action.upper()}"
                            )
                        else:
                            _emit_gate_event(
                                scan_context,
                                "ZONE_BIAS",
                                "block",
                                f"{zone['bias']} opposes {consensus_action.upper()} "
                                "without matching confirmed M1 event",
                            )
                            log_msg(
                                f"      ⛔ [ZONE-BIAS-OVERRIDE] {zone['bias']} "
                                f"opposes {consensus_action.upper()} without a "
                                "matching, confirmed M1 retest/false-breakout → skip",
                                "warning",
                            )
                            wait_until_next_minute(dashboard)
                            continue
            elif zone['bias'] and not zone_is_edge_early:
                _emit_gate_event(
                    scan_context,
                    "ZONE_BIAS",
                    "pass",
                    "middle-zone bias is informational only",
                    evaluated=False,
                )
                log_msg(
                    f"      Zone bias ({zone['bias']}) IGNORED — middle zone (v8.2)",
                    "trade"
                )
            else:
                _emit_gate_event(
                    scan_context,
                    "ZONE_BIAS",
                    "pass",
                    "no edge-zone directional bias to evaluate",
                    evaluated=False,
                )

            if ENABLE_MTF_ZONE_ENTRY_FILTER:
                # v8.7.R8: bỏ M1 khỏi zone entry filter — quá noisy.
                # M1 zone đã có zone_bias_override + zone_stars scoring xử lý riêng.
                zone_timeframes = [
                    ('M5', zone_mgr_m5, zone_m5, df_m5_closed),
                ]
                if ENABLE_M15_ZONE_SYSTEM and zone_m15 and df_m15_closed is not None:
                    zone_timeframes.append(
                        ('M15', zone_mgr_m15, zone_m15, df_m15_closed)
                    )
                zone_block_reason = None
                for timeframe, manager, timeframe_zone, timeframe_df in zone_timeframes:
                    if timeframe_df is None or len(timeframe_df) < 2:
                        continue
                    current = timeframe_df.iloc[-1]
                    previous = timeframe_df.iloc[-2]
                    for label, _zone_type, level in timeframe_zone.get('touched_zones', []):
                        preferred_direction = get_zone_direction(label)
                        if preferred_direction == 'both' or preferred_direction == consensus_action:
                            continue
                        if is_counter_edge_breakout_confirmed(
                            label,
                            consensus_action,
                            timeframe_zone.get('state'),
                            timeframe_zone.get('reason'),
                            zone_value=float(level),
                            previous_close=float(previous['close']),
                            candle_high=float(current['high']),
                            candle_low=float(current['low']),
                            candle_close=float(current['close']),
                            tolerance=(
                                manager.tolerance_major
                                if timeframe == 'M15'
                                else manager.tolerance_minor
                            ),
                            breakout_margin=manager.U * 0.1,
                            event=timeframe_zone.get('zone_event'),
                            active_breakout_label=timeframe_zone.get('breakout_label'),
                            active_breakout_direction=timeframe_zone.get('breakout_direction'),
                            active_breakout_value=timeframe_zone.get('breakout_value'),
                        ):
                            log_msg(
                                f"      [{timeframe} {label}] counter-direction "
                                f"{consensus_action.upper()} allowed by matching confirmation",
                                "trade",
                            )
                        else:
                            can_defer_minor_zone_entry = (
                                ENABLE_MINOR_ZONE_TREND_OVERRIDE
                                and timeframe in {'M1', 'M5'}
                                and has_minor_zone_trend_alignment(
                                    label,
                                    consensus_action,
                                    regime,
                                    t15,
                                    s15,
                                )
                            )
                            if can_defer_minor_zone_entry:
                                minor_zone_entry_soft_passes.append(
                                    f"{timeframe} {label}"
                                )
                            else:
                                zone_block_reason = (
                                    f"{timeframe} {label} prefers "
                                    f"{preferred_direction.upper()}; "
                                    f"{consensus_action.upper()} lacks matching "
                                    "breakout/retest"
                                )
                                break
                    if zone_block_reason:
                        break
                if zone_block_reason:
                    _emit_gate_event(
                        scan_context,
                        "ZONE_ENTRY",
                        "block",
                        zone_block_reason,
                    )
                    log_msg(f"      ⛔ [ZONE-ENTRY] {zone_block_reason} → skip", "warning")
                    wait_until_next_minute(dashboard)
                    continue
                if not minor_zone_entry_soft_passes:
                    _emit_gate_event(
                        scan_context,
                        "ZONE_ENTRY",
                        "pass",
                        "all touched M1/M5/M15 zones allow the signal direction",
                    )
            else:
                _emit_gate_event(
                    scan_context,
                    "ZONE_ENTRY",
                    "pass",
                    "MTF zone-entry filter disabled",
                    evaluated=False,
                )

            zone_stars = zone_stars_early
            zone_ext_applied = False
            if m1_at_early and not zone_is_edge_early and zone_stars_early == 0:
                _st = str(zone.get('state', '')).upper()
                _is_br = ('WAIT_RETEST' in _st) or ('BREAKOUT' in _st)
                if _is_br:
                    _m5al = ((consensus_action == 'call' and trend == 1) or
                             (consensus_action == 'put' and trend == -1))
                    _m15al = ((consensus_action == 'call' and t15 == 1) or
                              (consensus_action == 'put' and t15 == -1))
                    log_msg(
                        f"      ⚠️ z60 breakout but no +1★ "
                        f"(m5_at={m5_at_early}, m5_aligned={_m5al}, m15_aligned={_m15al})",
                        "trade"
                    )

            near_m1 = zone.get('near_m1_zone', False)
            near_m5 = zone.get('near_m5_zone', False)
            near_zones = zone.get('near_zones', [])
            min_dist_pips = min(
                (abs(float(last['close']) - zval) * 10000 for _lbl, _t, zval in near_zones),
                default=999.0)

            _effective_near_close = max(
                atr_params['near_very_close_pips'],
                0.4,
            )

            if (zone_stars == 0
                    and min_dist_pips <= _effective_near_close
                    and zone_is_edge_early):
                zone_stars = 1
                log_msg(
                    f"      🎯 Zone VERY NEAR ({min_dist_pips:.2f}p <= "
                    f"{_effective_near_close:.2f}p) → +1 zone*"
                )
            elif zone_stars == 0 and NEAR_BOTH_TF_BONUS and near_m1 and near_m5 and zone_is_edge_early:
                zone_stars = 1
                log_msg("      Candle near zones both TF -> +1 zone*")
            elif (zone_stars == 0 and zone_is_edge_early and min_dist_pips <= 2.0):
                zone_stars = 1
                log_msg(
                    f"      🎯 Edge zone within 2.0p ({min_dist_pips:.2f}p) → +1 zone* [ext]",
                    "trade"
                )
            elif zone_stars == 0 and (near_m1 or near_m5):
                log_msg(f"      Candle near zone ({min_dist_pips:.1f} pips, no bonus)")

            zone_bonus = zone_stars >= 1
            setup_stars = 0
            setup_reason = ""

            if has_mau_M:
                trend_ok = ((consensus_action == 'call' and trend == 1) or
                             (consensus_action == 'put' and trend == -1))
                if not trend_ok:
                    non_M = [s for s in agreeing if s[0] != 'Mau-M']
                    if non_M:
                        agreeing = non_M
                        agreeing_names = [s[0] for s in non_M]
                        pa_effective_count = _count_independent_pa(
                            [s[0] for s in non_M if s[2] == 'PA']) if ENABLE_PA_CORRELATION_FILTER \
                            else len([s for s in non_M if s[2] == 'PA'])
                        has_mau_M = False
                        log_msg(f"      Removed M-Pattern; remaining: {agreeing_names}")
                    else:
                        _emit_gate_event(
                            scan_context,
                            "SETUP_STARS",
                            "block",
                            "M-Pattern only and M5 trend is not aligned",
                        )
                        log_msg("      M-Pattern only, trend not aligned -> skipped")
                        wait_until_next_minute(dashboard)
                        continue

            setup_stars, setup_reason = _score_setup(
                pa_effective_count,
                is_369_present,
                zone_stars,
                has_br,
            )
            multi_pa = pa_effective_count >= 2
            near_zone = near_m1 or near_m5
            can_enter = (setup_stars >= 1 or zone_stars >= 1 or multi_pa
                          or (near_zone and setup_stars >= 0) or has_br)
            if not can_enter:
                scan_context["stars"] = setup_stars
                _emit_gate_event(
                    scan_context,
                    "SETUP_STARS",
                    "block",
                    f"setup too weak: {setup_reason}",
                )
                log_msg(f"      Setup too weak: {setup_reason}")
                wait_until_next_minute(dashboard)
                continue
            scan_context["stars"] = setup_stars

            if (
                swing_retest_action == consensus_action
                and not has_br
                and setup_stars > 0
            ):
                original_stars = setup_stars
                setup_stars = min(4, setup_stars + SWING_RETEST_STAR_BONUS)
                setup_reason += (
                    f" + M1 swing BOS retest (+"
                    f"{setup_stars - original_stars}★)"
                )
                log_msg(
                    f"      ✅ [SWING-RETEST] aligned M1 retest "
                    f"{consensus_action.upper()} → setup "
                    f"{original_stars}★ → {setup_stars}★",
                    "trade",
                )
                scan_context["stars"] = setup_stars

            continuation_reason = "no continuation-only downgrade required"
            if ENABLE_CONTINUATION_DOWNGRADE and setup_stars > 0:
                _cont_names = [n for n in agreeing_names if n in CONTINUATION_PATTERNS]
                _true_rev_names = [n for n in agreeing_names if n in TRUE_REVERSAL_PATTERNS]
                if _cont_names and not _true_rev_names and not has_br:
                    # Preserve continuation setups aligned with the strong trend.
                    _strong_trend_aligned = (
                        (regime == 'STRONG_TREND_UP' and consensus_action == 'call')
                        or (
                            regime == 'STRONG_TREND_DOWN'
                            and consensus_action == 'put'
                        )
                    )
                    if _strong_trend_aligned:
                        continuation_reason = (
                            f"continuation-only {_cont_names} aligned with {regime}"
                        )
                        log_msg(
                            f"      ✅ [CONT-SKIP] continuation in strong trend "
                            f"aligned ({regime}) → no downgrade",
                            "trade",
                        )
                    else:
                        _has_breakout_ctx = _has_valid_breakout_context(
                            zone, consensus_action
                        )
                        _aligned_edge = find_aligned_touched_edge_zone(
                            (
                                ("M1", touched_zones_list),
                                ("M5", zone_m5.get("touched_zones", [])),
                            ),
                            consensus_action,
                        )
                        if not _has_breakout_ctx and not _aligned_edge:
                            _orig_stars = setup_stars
                            setup_stars = max(0, setup_stars - 1)
                            setup_reason += (
                                f" [-cont {_orig_stars}→{setup_stars}]"
                            )
                            continuation_reason = (
                                f"continuation-only {_cont_names} has no breakout "
                                "context or aligned touched edge"
                            )
                            log_msg(
                                f"      ⚠️ [v8.3 CONT] continuation-only "
                                f"({_cont_names}) no breakout or aligned edge "
                                f"→ setup {_orig_stars}★ → {setup_stars}★",
                                "trade",
                            )
                            if setup_stars == 0:
                                _emit_gate_event(
                                    scan_context,
                                    "CONTINUATION",
                                    "block",
                                    continuation_reason,
                                )
                                log_msg(
                                    "      Setup downgraded to 0★ -> skipped",
                                    "warning",
                                )
                                wait_until_next_minute(dashboard)
                                continue
                        elif _aligned_edge:
                            continuation_reason = (
                                f"continuation-only {_cont_names} aligned with "
                                f"{_aligned_edge[1]} ({_aligned_edge[0]})"
                            )
                            log_msg(
                                f"      ✅ [CONT-EDGE] continuation "
                                f"({_cont_names}) + {_aligned_edge[1]} "
                                f"aligned → keep {setup_stars}★",
                                "trade",
                            )
                        else:
                            continuation_reason = (
                                f"continuation-only {_cont_names} has valid "
                                "breakout context"
                            )
            _emit_gate_event(
                scan_context,
                "CONTINUATION",
                "pass",
                continuation_reason,
                evaluated=ENABLE_CONTINUATION_DOWNGRADE,
            )

            if _should_downgrade_setup_4(setup_stars, agreeing_names):
                setup_stars = 3
                scan_context["stars"] = setup_stars
                log_msg(
                    "      ⚠️ [SETUP-4-QUALITY] No high-quality pattern "
                    "among Star/MornStar/Mau-M/3Inside/Marubozu/BeltHold "
                    "→ downgrade to 3★",
                    "trade",
                )


            if ENABLE_PA_369_CONTEXT and _needs_369_context(agreeing_names):
                context_touched_zones = (
                    list(touched_zones_list)
                    + list(zone_m5.get("touched_zones", []))
                )
                context_near_zones = (
                    list(near_zones)
                    + list(zone_m5.get("near_zones", []))
                )
                has_context, context_reason = _has_369_or_edge_context(
                    agreeing,
                    context_touched_zones,
                    setup_stars,
                    pa_effective_count,
                    context_near_zones,
                    float(last["close"]),
                )
                if not has_context:
                    reason = (
                        f"{agreeing_names} lacks PA context "
                        f"({context_reason})"
                    )
                    _emit_gate_event(
                        scan_context, "PA_369_CONTEXT", "block", reason
                    )
                    log_msg(
                        f"      ⛔ [PA-369-CONTEXT] {reason} → skip",
                        "warning",
                    )
                    wait_until_next_minute(dashboard)
                    continue
                _emit_gate_event(
                    scan_context,
                    "PA_369_CONTEXT",
                    "pass",
                    context_reason,
                )
                log_msg(
                    f"      ✅ [PA-369-CONTEXT] {context_reason} → pass",
                    "trade",
                )
            elif not ENABLE_PA_369_CONTEXT:
                _emit_gate_event(
                    scan_context,
                    "PA_369_CONTEXT",
                    "pass",
                    "filter disabled",
                    evaluated=False,
                )

            if minor_zone_bias_soft_pass:
                bias_zone_label = zone.get('bias_zone_label')
                if not is_minor_zone_soft_pass_eligible(
                    bias_zone_label,
                    consensus_action,
                    regime,
                    t15,
                    s15,
                    setup_stars,
                    MINOR_ZONE_TREND_OVERRIDE_MIN_STARS,
                ):
                    reason = (
                        f"{minor_zone_bias_soft_pass}; soft-pass requires "
                        f"{MINOR_ZONE_TREND_OVERRIDE_MIN_STARS}★, "
                        f"setup has {setup_stars}★"
                    )
                    _emit_gate_event(scan_context, "ZONE_BIAS", "block", reason)
                    log_msg(
                        f"      ⛔ [ZONE-BIAS-OVERRIDE] {reason} → skip",
                        "warning",
                    )
                    wait_until_next_minute(dashboard)
                    continue
                reason = (
                    f"{minor_zone_bias_soft_pass}; aligned STRONG_TREND and "
                    f"M15; setup {setup_stars}★"
                )
                _emit_gate_event(scan_context, "ZONE_BIAS", "pass", reason)
                log_msg(
                    f"      ✅ [ZONE-BIAS-OVERRIDE] minor zone soft-pass "
                    f"({setup_stars}★, M15 aligned)",
                    "trade",
                )
                minor_zone_soft_pass_reasons.append(minor_zone_bias_soft_pass)

            if minor_zone_entry_soft_passes:
                if setup_stars < MINOR_ZONE_TREND_OVERRIDE_MIN_STARS:
                    reason = (
                        f"{', '.join(minor_zone_entry_soft_passes)} counter-zone "
                        f"soft-pass requires "
                        f"{MINOR_ZONE_TREND_OVERRIDE_MIN_STARS}★; "
                        f"setup has {setup_stars}★"
                    )
                    _emit_gate_event(scan_context, "ZONE_ENTRY", "block", reason)
                    log_msg(
                        f"      ⛔ [ZONE-ENTRY] {reason} → skip",
                        "warning",
                    )
                    wait_until_next_minute(dashboard)
                    continue
                reason = (
                    f"minor zones {', '.join(minor_zone_entry_soft_passes)} "
                    f"soft-passed: aligned STRONG_TREND and M15; "
                    f"setup {setup_stars}★"
                )
                _emit_gate_event(scan_context, "ZONE_ENTRY", "pass", reason)
                log_msg(
                    f"      ✅ [ZONE-ENTRY] minor-zone trend soft-pass "
                    f"({setup_stars}★, M15 aligned)",
                    "trade",
                )
                minor_zone_soft_pass_reasons.extend(
                    minor_zone_entry_soft_passes
                )

            # v8.7.R8: cho phép solo-strong pattern nếu có near-zone
            if setup_stars < 2 and not has_br:
                _solo_strong = (
                    len(agreeing_names) == 1
                    and agreeing_names[0] in STRONG_TRUE_REVERSAL_PATTERNS
                    and (zone_stars >= 1 or near_m1 or near_m5)
                )
                if _solo_strong:
                    log_msg(
                        f"      ✅ [SOLO-STRONG] {agreeing_names[0]} + near zone "
                        f"→ allow (1★ bypass)", "trade"
                    )
                else:
                    log_msg(f"      ⛔ [v8.5] setup {setup_stars}★ < 2★ → skip", "warning")
                    wait_until_next_minute(dashboard)
                    continue
            _emit_gate_event(
                scan_context,
                "SETUP_STARS",
                "pass",
                (
                    "BreakRetest bypass"
                    if has_br
                    else f"setup {setup_stars} stars meets minimum 2"
                ),
                evaluated=not has_br,
            )

            log_msg(f"      {'*' * setup_stars} {setup_reason}")

            m5_same_direction = ((consensus_action == 'call' and trend == 1) or
                                  (consensus_action == 'put' and trend == -1))
            m5_opposite = (not m5_same_direction and trend != 0 and not sideways)
            countertrend_confirmation = "not_required"
            m1_flip_override_emitted = False
            # v8.9: M1 BOS_RETEST trend-flip override.
            # M5 EMA20 lags, so M1 BOS_RETEST confirms a trend flip.
            if m5_opposite and _get_m1_flip_override(
                market_structure, consensus_action
            ):
                log_msg(
                    f"   ✅ [M1-TREND-FLIP] M1 BOS_RETEST confirms "
                    f"{consensus_action.upper()} — bypass counter-M5 "
                    f"(M5 EMA20 lag ~100min)",
                    "trade",
                )
                _emit_gate_event(
                    scan_context,
                    "COUNTERTREND",
                    "pass",
                    "M1 BOS_RETEST trend-flip override",
                )
                m1_flip_override_emitted = True
                m5_opposite = False

            if ENABLE_COUNTERTREND_CONFIRMATION and m5_opposite:
                has_reversal_confirmation = any(
                    name in TRUE_REVERSAL_PATTERNS for name in agreeing_names
                )
                m1_zone_label = zone.get('m1_zone_label')
                z60_label = (
                    m1_zone_label[0]
                    if m1_zone_label
                    and is_middle_zone_label(m1_zone_label[0], 'minor')
                    else None
                )
                _near_zones_for_ct = zone.get('near_zones', [])
                _min_dist_ct = min(
                    (abs(float(last['close']) - zval) * 10000
                     for _lbl, _t, zval in _near_zones_for_ct),
                    default=999.0)
                _near_edge_countertrend_ok = (
                    zone_is_edge_early
                    and not has_br
                    and pa_effective_count >= 2
                    and has_reversal_confirmation
                    and _min_dist_ct <= 2.0
                    and setup_stars >= 2
                )
                if _near_edge_countertrend_ok:
                    countertrend_confirmation = (
                        f"near_edge_z({_min_dist_ct:.1f}p)+"
                        f"{pa_effective_count}PA_incl_reversal"
                    )
                    log_msg(
                        f"      ✅ [v8.7 NEAR-EDGE-CT] countertrend OK: near edge "
                        f"({_min_dist_ct:.1f}p) + {pa_effective_count} PA incl reversal",
                        "trade"
                    )
                elif (
                    ENABLE_Z60_COUNTERTREND_CONFIRMATION
                    and z60_label is not None
                ):
                    z60_confirmed = is_middle_zone_countertrend_confirmed(
                        z60_label,
                        consensus_action,
                        zone.get('zone_event'),
                        candle_close=float(last['close']),
                        independent_pa_count=pa_effective_count,
                        has_reversal_pattern=has_reversal_confirmation,
                        min_pa_count=COUNTERTREND_MIN_PA_CONFIRMATIONS,
                    )
                    if z60_confirmed:
                        countertrend_confirmation = (
                            "z60_retest_or_false_breakout+independent_reversal_pa"
                        )
                    else:
                        countertrend_confirmation = "blocked"
                        reason = (
                            f"M5 opposes {consensus_action.upper()} at {z60_label}; "
                            f"require matching z60 retest/false-breakout and "
                            f"{COUNTERTREND_MIN_PA_CONFIRMATIONS} independent PA "
                            "including a reversal pattern"
                        )
                        log_msg(
                            f"      ⛔ [Z60-COUNTERTREND] {reason} → skip",
                            "warning",
                        )
                        _emit_gate_event(
                            scan_context,
                            "COUNTERTREND",
                            "block",
                            reason,
                        )
                        wait_until_next_minute(dashboard)
                        continue
                elif is_countertrend_confirmation_sufficient(
                    has_break_retest=has_br,
                    zone_is_edge=zone_is_edge_early,
                    zone_stars=zone_stars,
                    independent_pa_count=pa_effective_count,
                    has_reversal_pattern=has_reversal_confirmation,
                    min_pa_count=COUNTERTREND_MIN_PA_CONFIRMATIONS,
                ):
                    countertrend_confirmation = (
                        "break_retest" if has_br else "edge_zone+independent_reversal_pa"
                    )
                else:
                    countertrend_confirmation = "blocked"
                    reason = (
                        f"M5 opposes {consensus_action.upper()}; require valid B&R "
                        f"or edge-zone confirmation + {COUNTERTREND_MIN_PA_CONFIRMATIONS} "
                        "independent PA including a reversal pattern"
                    )
                    log_msg(f"      ⛔ [COUNTERTREND] {reason} → skip", "warning")
                    _emit_gate_event(
                        scan_context,
                        "COUNTERTREND",
                        "block",
                        reason,
                    )
                    wait_until_next_minute(dashboard)
                    continue
                _emit_gate_event(
                    scan_context,
                    "COUNTERTREND",
                    "pass",
                    f"countertrend confirmation: {countertrend_confirmation}",
                )
            else:
                # v8.9: avoid a duplicate pass event after the flip override.
                if not m1_flip_override_emitted:
                    _emit_gate_event(
                        scan_context,
                        "COUNTERTREND",
                        "pass",
                        "countertrend confirmation not required or disabled",
                        evaluated=False,
                    )

            m5_m15_conflict = False
            if ENABLE_M15_TREND and t15 is not None and not s15 and t15 != 0:
                m5_m15_conflict = ((trend == 1 and t15 == -1) or (trend == -1 and t15 == 1))

            weak_only_pa = (len(pa_patterns_in_agree) == 1
                             and pa_patterns_in_agree[0] in WEAK_PA_PATTERNS
                             and not has_mau_M)
            no_zone_confirmation = (not is_369_present and zone_stars == 0
                                     and not zone.get('bias') and not has_mau_M and not has_br)
            f3_weak_pa_exception = (weak_only_pa and setup_stars >= 2
                                     and zone_stars >= 1 and m5_same_direction)

            override, override_reason = should_override_filter(
                agreeing_names, zone_stars, pa_effective_count,
                consensus_action, regime, session, 
                zone_is_edge=zone_is_edge_early,
                touched_zones=zone.get('touched_zones', []))   # ← THÊM
            if has_br:
                override = True
                override_reason = "Break_Retest_Bypass"
            if override:
                log_msg(f"      🟢 OVERRIDE: {override_reason}")

            _atr_pips_now = _atr_for_quality * 10000.0
            s_adj = session_threshold_adjust(session)
            th_f1 = max(1, MIN_STARS_FOR_OPPOSITE_M5 + s_adj)
            th_f3 = max(1, MIN_STARS_FOR_WEAK_PA + s_adj)
            th_f4 = max(1, MIN_STARS_FOR_M5_M15_CONFLICT + s_adj)
            th_f5 = max(1, MIN_STARS_FOR_NO_ZONE_CONFLUENCE + s_adj)
            th_f6 = max(1, MIN_STARS_FOR_SIDEWAYS + s_adj)

            if _meta_brain is not None:
                _utc_hour = datetime.now(timezone.utc).hour
                adapt = _meta_brain.get_adaptive_context(
                    current_hour=_utc_hour,
                    regime=regime,
                    regime_conf=_reg_conf,
                    atr_pips=_atr_pips_now,
                )
                adapt['min_rr_edge'] = atr_params['min_rr_edge_pips']

                # v8.7.R8 FIX: respect user's KB_SOFT_VETO_THRESHOLD (was ignored)
                _user_thr = KB_SOFT_VETO_THRESHOLD + KB_VETO_MARGIN
                adapt['kb_veto_thr'] = min(adapt['kb_veto_thr'], _user_thr)
                log_msg(
                    f"      🧬 [ADAPT] F1={th_f1}★ F5={th_f5}★ | "
                    f"KB_thr={adapt['kb_veto_thr']:.3f} "
                    f"(user cap {_user_thr:.3f}) | "
                    f"RR_min={adapt['min_rr_edge']}p ({adapt['reason']})",
                    "trade",
                )
            else:
                adapt = {
                    'kb_veto_thr': KB_SOFT_VETO_THRESHOLD,
                    'min_rr_edge': atr_params['min_rr_edge_pips'],
                }
                _emit_gate_event(
                    scan_context,
                    "META_REGIME",
                    "pass",
                    "adaptive meta-regime filter unavailable",
                    evaluated=False,
                )

            # === v8.7.R2: ATR-Norm effective thresholds ===
            effective_min_opposite_m5 = th_f1
            effective_min_m15_high = M15_HIGH_SEVERITY_STARS_REQUIRED

            if ENABLE_ATR_NORMALIZED_CONFLUENCE:
                if m1_strength > ATR_NORM_VERY_STRONG_THRESHOLD:
                    effective_min_opposite_m5 = max(2, th_f1 - 1)
                    effective_min_m15_high = max(
                        3, M15_HIGH_SEVERITY_STARS_REQUIRED - 2
                    )
                    log_msg(
                        f"      🚀 [ATR-NORM] M1 very strong "
                        f"({m1_strength:.1f} ATR) → "
                        f"relax F1={effective_min_opposite_m5}★ "
                        f"M15={effective_min_m15_high}★",
                        "trade"
                    )
                elif m1_strength > ATR_NORM_STRONG_THRESHOLD:
                    effective_min_opposite_m5 = max(2, th_f1 - 1)
                    if m15_strength < 1.0:
                        effective_min_m15_high = max(
                            4, M15_HIGH_SEVERITY_STARS_REQUIRED - 1
                        )
                    log_msg(
                        f"      ✅ [ATR-NORM] M1 strong "
                        f"({m1_strength:.1f} ATR) → "
                        f"relax F1={effective_min_opposite_m5}★",
                        "trade"
                    )
                elif (
                    m1_strength > 1.0
                    and m5_strength < ATR_NORM_WEAK_THRESHOLD
                ):
                    effective_min_opposite_m5 = max(2, th_f1 - 1)
                    log_msg(
                        f"      ℹ️ [ATR-NORM] M1 average + M5 noise → "
                        f"relax F1={effective_min_opposite_m5}★",
                        "trade"
                    )

            if ENABLE_EXHAUSTION_REVERSAL and m15_extended and m5_compressed:
                effective_min_m15_high = min(
                    effective_min_m15_high, M15_HIGH_RELAXED_STARS
                )
                log_msg(
                    f"      🎯 [EXHAUSTION-REVERSAL] relax M15-HIGH → "
                    f"{effective_min_m15_high}★",
                    "trade"
                )

            filter_reasons = []
            if m5_opposite and setup_stars < effective_min_opposite_m5:
                if override: log_msg(f"      ✅ [F1-OVERRIDE]")
                else:
                    _relaxed = (
                        " (relaxed)"
                        if effective_min_opposite_m5 < th_f1
                        else ""
                    )
                    filter_reasons.append(
                        f"[F1] opposing M5 + {setup_stars}* < "
                        f"{effective_min_opposite_m5}*{_relaxed}"
                    )

            if m5_m15_conflict and setup_stars < th_f4:
                if override: log_msg(f"      ✅ [F4-OVERRIDE]")
                else: filter_reasons.append(f"[F4] M5/M15 conflict + {setup_stars}* < {th_f4}*")

            if weak_only_pa and setup_stars < th_f3 and not f3_weak_pa_exception:
                if override: log_msg(f"      ✅ [F3-OVERRIDE]")
                else: filter_reasons.append(f"[F3] weak PA + {setup_stars}* < {th_f3}*")

            if no_zone_confirmation and setup_stars < th_f5:
                if override: log_msg(f"      ✅ [F5-OVERRIDE]")
                else: filter_reasons.append(f"[F5] no zone + {setup_stars}* < {th_f5}*")

            if sideways and setup_stars < th_f6:
                if override: log_msg(f"      ✅ [F6-OVERRIDE]")
                else: filter_reasons.append(f"[F6] SIDEWAYS + {setup_stars}* < {th_f6}*")

            if filter_reasons:
                _emit_gate_event(
                    scan_context,
                    "FIVE_FILTER",
                    "block",
                    " | ".join(filter_reasons),
                )
                log_msg(f"      ⛔ 5-FILTER BLOCK: {' | '.join(filter_reasons)}", "warning")
                wait_until_next_minute(dashboard)
                continue
            _emit_gate_event(
                scan_context,
                "FIVE_FILTER",
                "pass",
                "all applicable F1-F6 checks passed"
                if not override
                else f"filter override accepted: {override_reason}",
            )
            log_msg(f"      ✅ 5-FILTER PASS (setup {setup_stars}*)")

# Sửa M15 severity check (trong main loop):
            if ENABLE_M15_SEVERITY_BLOCK and ENABLE_M15_TREND:
                severity = _get_m15_severity(trend, sideways, t15, s15, consensus_action)
                if severity == 'high':
                    required = 3 if has_br else M15_HIGH_SEVERITY_STARS_REQUIRED
                    soft_override = _should_soft_override_m15_high(
                        setup_stars, required, _reg_conf
                    )
                    # === v8.7.R11: M1 BOS_RETEST = trend flip confirmation ===
                    # Khi M5 aligned nhưng M15 counter, và M1 vừa có BOS+RETEST
                    # cùng chiều consensus → M15 đang lag (M5 EMA20 ~100min),
                    # M1 là tín hiệu sớm hơn → cho phép setup 3★ pass
                    _m1_flip_confirm = _get_m1_flip_override(
                        market_structure, consensus_action
                    )
                    if _m1_flip_confirm and setup_stars >= 3:
                        log_msg(
                            f"      ✅ [M15-HIGH-M1-FLIP] M1 BOS_RETEST confirms "
                            f"{consensus_action.upper()} (setup {setup_stars}★) → allow",
                            "trade",
                        )
                        soft_override = True
                    # === END v8.7.R11 ===
                    if setup_stars < required and not soft_override:
                        log_msg(
                            f"      ⛔ [M15-HIGH] M15={_trend_str(t15, s15)} "
                            f"counter {consensus_action.upper()}, setup "
                            f"{setup_stars}* < {required}* → skip",
                            "warning")
                        wait_until_next_minute(dashboard)
                        continue
                    if soft_override:
                        log_msg(
                            f"      ✅ [M15-HIGH-SOFT] {setup_stars}★ + M15 conf "
                            f"{_reg_conf:.2f} < "
                            f"{M15_HIGH_SOFT_OVERRIDE_MAX_CONFIDENCE:.2f} "
                            "→ allow",
                            "trade",
                        )
                else:
                    m5_m15_confluent = (trend == t15 and trend != 0 and not sideways)
                    strong_setup = (has_br or setup_stars >= 3
                                    or (zone_stars >= 1 and setup_stars >= 2))
                    if not m5_m15_confluent and not strong_setup:
                        _emit_gate_event(
                            scan_context,
                            "M15_SEVERITY",
                            "block",
                            f"M5/M15 disagree and setup {setup_stars} is weak",
                        )
                        log_msg(f"      ⛔ M5/M15 disagree + weak setup {setup_stars}*", "warning")
                        wait_until_next_minute(dashboard)
                        continue
            elif ENABLE_M5_M15_CONFLUENCE and ENABLE_M15_TREND:
                m5_m15_confluent = (trend == t15 and trend != 0 and not sideways)
                strong_setup = (has_br or setup_stars >= 3
                                 or (zone_stars >= 1 and setup_stars >= 2))
                if not m5_m15_confluent and not strong_setup:
                    _emit_gate_event(
                        scan_context,
                        "M15_SEVERITY",
                        "block",
                        "M5/M15 disagree and setup is weak",
                    )
                    log_msg(f"      ⛔ M5/M15 disagree + weak setup", "warning")
                    wait_until_next_minute(dashboard)
                    continue
            if ENABLE_M15_SEVERITY_BLOCK and ENABLE_M15_TREND:
                _emit_gate_event(
                    scan_context,
                    "M15_SEVERITY",
                    "pass",
                    "M15 severity/confluence requirements satisfied",
                )
            elif ENABLE_M5_M15_CONFLUENCE and ENABLE_M15_TREND:
                _emit_gate_event(
                    scan_context,
                    "M15_SEVERITY",
                    "pass",
                    "M5/M15 confluence requirements satisfied",
                )
            else:
                _emit_gate_event(
                    scan_context,
                    "M15_SEVERITY",
                    "pass",
                    "M15 severity and confluence checks are disabled",
                    evaluated=False,
                )

            session_block = get_session_block_reason(
                session, regime, zone_stars, setup_stars,
                agreeing_names, pa_effective_count)
            if session_block:
                _emit_gate_event(
                    scan_context,
                    "SESSION",
                    "block",
                    session_block,
                )
                log_msg(f"      ⛔ {session_block}", "warning")
                wait_until_next_minute(dashboard)
                continue
            _emit_gate_event(
                scan_context,
                "SESSION",
                "pass",
                f"session {session} permits this setup",
            )
            log_msg(f"      ✅ Session OK ({session})")

            # === v8.7.R5: middle R:R phụ thuộc edge support ===
            _mid_has_edge = _check_middle_edge_support(
                zone_mgr, float(last["close"]), consensus_action, tol_pips=2.0
            )
            _rr_middle_final = compute_middle_rr_threshold(
                atr_params["atr_pips"], _mid_has_edge
            )
                        # === v8.7.R11: TURBO 60s R:R adjustment ===
            # TURBO 60s: giá chỉ cần close > entry (CALL) hoặc < entry (PUT) sau 60s
            # KHÔNG cần chạm zone → R:R yêu cầu không cần cao như CFD/Forex
            #
            # Bug fix log 24/11 (5 poor R:R blocks liên tiếp):
            #   - Edge R:R 1.3p < 1.68p → blocked (nên cho qua, vì 1.3p đủ cho 60s)
            #   - Edge R:R 1.1p < 1.75p → blocked (nên cho qua)
            # Giải pháp:
            #   - Edge: min = max(0.8, ATR × 0.6)
            #   - Middle: min = edge_min × 1.15 (thay vì 1.5)
            _atr_pips_for_rr = float(atr_params['atr_pips'])
            _rr_edge_final = atr_params['min_rr_edge_pips']
            _rr_middle_final = _rr_middle_final  # default
            if OPTION_TYPE == "TURBO" and TRADE_EXPIRY_SECONDS <= 60:
                _rr_edge_final = max(0.8, _atr * 0.5)
                _rr_middle_final = max(1.2, _atr * 0.7)

                log_msg(
                    f"      🎯 [v8.7.R11 TURBO-RR] ATR={_atr:.2f}p → "
                    f"edge={_rr_edge_final:.2f}p middle={_rr_middle_final:.2f}p "
                    f"(was edge={atr_params['min_rr_edge_pips']:.2f}p "
                    f"middle={_rr_middle_final_orig:.2f}p)",
                    "trade",
                )
            # === v8.7.R11: TURBO 60s R:R adjustment ===
            # TURBO 60s: close sau 60s chỉ cần > entry (CALL) hoặc < entry (PUT)
            # KHÔNG cần chạm zone → R:R không cần cao như CFD/Forex.
            # Log 25/11: 5 poor R:R blocks/26 phút với ATR ~1.96p:
            #   - Edge 1.3p < 1.68p (block) → thực tế WIN nếu trade
            #   - Middle 1.6p < 2.46p (block) → thực tế WIN nếu trade
            #   - Edge 1.1p < 1.75p (block) → thực tế LOSS nếu trade
            # Giải pháp:
            #   Edge = max(0.8, ATR × 0.5)
            #   Middle = max(1.2, ATR × 0.7)
            _rr_middle_final_orig = _rr_middle_final  # backup để log
            _rr_edge_final = atr_params['min_rr_edge_pips']
            if OPTION_TYPE == "TURBO" and TRADE_EXPIRY_SECONDS <= 60:
                _atr_for_rr = float(atr_params['atr_pips'])
                _rr_edge_final = max(0.8, _atr_for_rr * 0.5)
                _rr_middle_final = max(1.2, _atr_for_rr * 0.7)
                log_msg(
                    f"      🎯 [v8.7.R11 TURBO-RR] ATR={_atr_for_rr:.2f}p → "
                    f"edge={_rr_edge_final:.2f}p middle={_rr_middle_final:.2f}p "
                    f"(was edge={atr_params['min_rr_edge_pips']:.2f}p "
                    f"middle={_rr_middle_final_orig:.2f}p)",
                    "trade",
                )

            log_msg(
                "      Middle R:R: ATR=%.2f edge_support=%s -> %sp" % (
                    atr_params["atr_pips"], _mid_has_edge, _rr_middle_final),
                "trade",
            )
            opp_zone = _find_rr_target_with_fallback(
                zone_mgr,
                zone_mgr_m15,
                float(last['close']),
                consensus_action,
                rr_edge_min=_rr_edge_final,        # ← v8.7.R11
                rr_middle_min=_rr_middle_final,    # ← đã adjust ở trên
            )
            if opp_zone is not None:
                if len(opp_zone) == 5:
                    opp_lbl, opp_val, opp_dist, opp_is_edge, opp_src = opp_zone
                elif len(opp_zone) == 4:
                    opp_lbl, opp_val, opp_dist, opp_is_edge = opp_zone
                    opp_src = 'M1'
                else:
                    log_msg(
                        f"      ⛔ [R:R] Bad opp_zone shape: {opp_zone}",
                        "warning",
                    )
                    wait_until_next_minute(dashboard)
                    continue

                min_rr_req = (
                    atr_params['min_rr_edge_pips']
                    if opp_is_edge
                    else _rr_middle_final   # ← v8.7.R7: dùng dynamic, khớp với _find_rr_target
                )

                _rr_relax_applied = False
                if (
                    opp_dist < min_rr_req
                    and setup_stars >= 3
                    and zone_stars >= 1
                ):
                    _relaxed = min_rr_req * RR_RELAX_FACTOR
                    if opp_dist >= _relaxed:
                        _rr_relax_applied = True
                        log_msg(
                            f"      ⚠️ [R:R-RELAX] {opp_dist:.1f}p ≥ "
                            f"{RR_RELAX_FACTOR:.0%} of {min_rr_req:.1f}p "
                            f"— setup {setup_stars}★+zone{zone_stars}★ → allow",
                            "trade",
                        )

                if opp_dist < min_rr_req and not _rr_relax_applied:
                    reason = (
                        f"opposite zone {opp_lbl} at {opp_dist:.1f}p; "
                        f"minimum required is {min_rr_req:.1f}p"
                    )
                    _emit_gate_event(
                        scan_context, "RISK_REWARD", "block", reason
                    )
                    log_msg(f"      ⛔ [R:R] {reason} → skip", "warning")
                    wait_until_next_minute(dashboard)
                    continue
                _emit_gate_event(
                    scan_context,
                    "RISK_REWARD",
                    "pass",
                    (
                        f"opposite zone {opp_lbl} at {opp_dist:.1f}p"
                        + (
                            " using quality relaxation"
                            if _rr_relax_applied
                            else ""
                        )
                    ),
                )
            _emit_gate_event(
                scan_context,
                "CONTRA_M5_BREAKOUT",
                "pass",
                "M5 zone breakout direction matches action or no breakout",
            )

            if not check_zone_state_conflict(zone, consensus_action):
                state = str(zone.get('state', 'IDLE'))
                _emit_gate_event(
                    scan_context,
                    "ZONE_STATE_CONFLICT",
                    "block",
                    f"zone state={state} conflicts with action={consensus_action.upper()}",
                )
                log_msg(
                    f"      [BLOCK] [ZONE-STATE] {state} vs action "
                    f"{consensus_action.upper()} -> skip",
                    "warning"
                )
                wait_until_next_minute(dashboard)
                continue
            _emit_gate_event(
                scan_context,
                "ZONE_STATE_CONFLICT",
                "pass",
                "zone state aligns with action",
            )

            if has_mau_M:
                if not check_atr(df, last.to_dict(), max_ratio=2.5):
                    _emit_gate_event(
                        scan_context,
                        "ATR_QUALITY",
                        "block",
                        "M-Pattern candle ATR ratio exceeds 2.5",
                    )
                    try:
                        _r = float(last['high'] - last['low']) * 10000
                        _b = abs(float(last['close']) - float(last['open'])) * 10000
                        log_msg(f"      ATR reject M-Pattern | range={_r:.1f}pip body={_b:.1f}pip", "warning")
                    except Exception:
                        log_msg("      ATR reject M-Pattern", "warning")
                    wait_until_next_minute(dashboard)
                    continue
                _emit_gate_event(
                    scan_context,
                    "ATR_QUALITY",
                    "pass",
                    "M-Pattern candle ATR ratio is within range",
                )
            else:
                if not check_atr(df, last.to_dict(), max_ratio=2.0):
                    _emit_gate_event(
                        scan_context,
                        "ATR_QUALITY",
                        "block",
                        "candle ATR ratio exceeds 2.0",
                    )
                    try:
                        _r = float(last['high'] - last['low']) * 10000
                        _b = abs(float(last['close']) - float(last['open'])) * 10000
                        log_msg(f"      ATR reject | range={_r:.1f}pip body={_b:.1f}pip", "warning")
                    except Exception:
                        log_msg("      ATR reject", "warning")
                    wait_until_next_minute(dashboard)
                    continue
                _emit_gate_event(
                    scan_context,
                    "ATR_QUALITY",
                    "pass",
                    "candle ATR ratio is within the allowed range",
                )

            if has_mau_M:
                if not check_ema(df, last.to_dict(), consensus_action):
                    _emit_gate_event(
                        scan_context,
                        "EMA_TREND",
                        "block",
                        "M-Pattern opposes EMA50",
                    )
                    log_msg("      M-Pattern opposes EMA50")
                    wait_until_next_minute(dashboard)
                    continue
                _emit_gate_event(
                    scan_context,
                    "EMA_TREND",
                    "pass",
                    "M-Pattern is aligned with EMA50",
                )
                if not check_m5_trend(df_m5, consensus_action):
                    _emit_gate_event(
                        scan_context,
                        "M5_TREND",
                        "block",
                        "M-Pattern opposes M5 trend",
                    )
                    log_msg("      M-Pattern opposes M5 trend")
                    wait_until_next_minute(dashboard)
                    continue
                _emit_gate_event(
                    scan_context,
                    "M5_TREND",
                    "pass",
                    "M-Pattern is aligned with M5 trend",
                )
            else:
                _emit_gate_event(
                    scan_context,
                    "EMA_TREND",
                    "pass",
                    "EMA alignment gate only applies to M-Pattern",
                    evaluated=False,
                )
                _emit_gate_event(
                    scan_context,
                    "M5_TREND",
                    "pass",
                    "M5 trend gate only applies to M-Pattern",
                    evaluated=False,
                )

            volume_passed, volume_reason = _evaluate_f2_volume(
                df,
                session,
                setup_stars,
                zone_stars,
                agreeing_names,
                has_br,
                min_ratio_default=cfg369.MinVolumeRatio,
            )
            _emit_gate_event(
                scan_context,
                "F2_VOLUME",
                "pass" if volume_passed else "block",
                volume_reason,
            )
            if not volume_passed:
                log_msg(f"      ⛔ [F2] {volume_reason}", "warning")
                wait_until_next_minute(dashboard)
                continue
            log_msg(f"      ✅ [F2] {volume_reason}")


            _momentum_metrics = None
            if ENABLE_MOMENTUM_FILTER and _HAS_MOMENTUM:
                try:
                    with _phase("momentum"):
                        mom_cfg = get_adaptive_momentum_cfg(regime, _vol_ratio)
                        _m15_struct_raw = market_structure.get("M15", {}).get("structure", None)
                        if _m15_struct_raw in ('BULLISH', 'BEARISH'):
                            m15_struct_for_mom = _m15_struct_raw
                        else:
                            m15_struct_for_mom = None
                        momentum_result = detect_overextended(
                            df, consensus_action,
                            trend_m5=trend,
                            sideways=sideways,
                            has_br=has_br,
                            setup_stars=setup_stars,
                            m15_structure=m15_struct_for_mom,
                            regime_ctx=regime,
                            cfg_override=mom_cfg,
                        )
                        log_msg(f"      📊 Momentum: {format_metrics(momentum_result['metrics'])}", "trade")

                    if momentum_result.get('block'):
                        reason = f"[v8.5 MOMENTUM] {consensus_action.upper()} block → {momentum_result['reason']}"
                        log_msg(f"      ⛔ {reason}", "warning")
                        if momentum_result.get('hard_block'):
                            log_msg("      🛑 [HARD-BLOCK] ABSOLUTE exhaustion — không override được", "error")
                        wait_until_next_minute(dashboard, signal_reason=reason)
                        continue
                    elif momentum_result.get('warning'):
                        log_msg(
                            f"      ⚠️ [v8.5 MOMENTUM] {consensus_action.upper()} borderline → "
                            f"{momentum_result['reason']}",
                            "warning",
                        )
                    _emit_gate_event(
                        scan_context,
                        "MOMENTUM",
                        "pass",
                        momentum_result.get("reason", "momentum filter passed"),
                    )
                except Exception as e:
                    logging.warning(f"Momentum check error: {e}")
                    _emit_gate_event(
                        scan_context,
                        "MOMENTUM",
                        "fallback_bypass",
                        f"{type(e).__name__}: {e}",
                        fallback=True,
                        evaluated=False,
                    )
            else:
                _emit_gate_event(
                    scan_context,
                    "MOMENTUM",
                    "pass",
                    "momentum filter disabled or analyzer unavailable",
                    evaluated=False,
                )
            if ENABLE_KB_SOFT_VETO and _knowledge_brain is not None and _knowledge_brain.is_ready():
                primary_pattern = agreeing_names[0] if agreeing_names else None
                if primary_pattern:
                    log_msg(
                        f"      🧠 [KB] entering veto check for {primary_pattern}...",
                        "trade"
                    )
                    kb_ctx_veto = {
                        'session': session,
                        'regime': regime,
                        'zone_stars': zone_stars,
                        'zone_is_edge': zone_is_edge_early,
                        'trend_m5': trend,
                        'trend_m15': t15,
                        'sideways': sideways,
                        'sideways_m15': s15,
                        'vol_ratio': _vol_ratio,
                        'pa_count': pa_effective_count,
                    }
                    try:
                        kb_thr_now = adapt['kb_veto_thr']
                        kb_veto_limit = kb_thr_now + KB_VETO_MARGIN
                        q_veto, kb_fallback, kb_error = _knowledge_brain.score_blended(
                            primary_pattern, consensus_action, kb_ctx_veto,
                        )
                        log_msg(
                            f"      🧠 KB-VETO check: {primary_pattern} "
                            f"{consensus_action.upper()} q={q_veto:.3f} "
                            f"(thr={kb_thr_now:.3f}+{KB_VETO_MARGIN:.3f}"
                            f"={kb_veto_limit:.3f})",
                            "trade")
                        if q_veto < kb_veto_limit:
                            hard_veto = (
                                m5_opposite
                                and _atr_for_quality * 10000 < 3.0
                                and q_veto < 0.45
                                and not has_br
                            )
                            if hard_veto:
                                _emit_gate_event(
                                    scan_context,
                                    "KB_VETO",
                                    "block",
                                    f"countertrend + low ATR + q={q_veto:.3f} < 0.45"
                                    + (f"; scoring fallback: {kb_error}" if kb_fallback else ""),
                                    fallback=kb_fallback,
                                )
                                log_msg(
                                    f"      ⛔ [KB-HARD-VETO] countertrend + low-ATR "
                                    f"+ q={q_veto:.3f} < 0.45 -> skip",
                                    "warning")
                                wait_until_next_minute(dashboard)
                                continue
                            if has_br or override:
                                # v8.7.R6: KB hard floor — q < 0.45 = cấm override
                                if q_veto < 0.45:
                                    _emit_gate_event(
                                        scan_context,
                                        "KB_VETO",
                                        "block",
                                        f"q={q_veto:.3f} < HARD FLOOR 0.45 — override denied"
                                        + (f"; scoring fallback: {kb_error}" if kb_fallback else ""),
                                        fallback=kb_fallback,
                                    )
                                    log_msg(
                                        f"      ⛔ [KB-HARD-FLOOR] q={q_veto:.3f} < 0.45 "
                                        f"-> override DENIED -> skip",
                                        "warning")
                                    wait_until_next_minute(dashboard)
                                    continue
                                _emit_gate_event(
                                    scan_context,
                                    "KB_VETO",
                                    "pass",
                                    "low KB score accepted by BreakRetest/override"
                                    + (f"; scoring fallback: {kb_error}" if kb_fallback else ""),
                                    fallback=kb_fallback,
                                    evaluated=not (has_br or override),
                                )
                                log_msg(
                                    f"      ⚠️ KB q={q_veto:.3f} < {kb_veto_limit:.3f} "
                                    f"BUT override/B&R -> allowed",
                                    "trade")
                            else:
                                _emit_gate_event(
                                    scan_context,
                                    "KB_VETO",
                                    "block",
                                    f"q={q_veto:.3f} < threshold {kb_veto_limit:.3f}"
                                    + (f"; scoring fallback: {kb_error}" if kb_fallback else ""),
                                    fallback=kb_fallback,
                                )
                                log_msg(
                                    f"      ⛔ [KB-VETO] q={q_veto:.3f} < "
                                    f"{kb_veto_limit:.3f} → skip",
                                    "warning")
                                wait_until_next_minute(dashboard)
                                continue
                        else:
                            _emit_gate_event(
                                scan_context,
                                "KB_VETO",
                                "pass",
                                f"q={q_veto:.3f} >= threshold {kb_veto_limit:.3f}"
                                + (f"; scoring fallback: {kb_error}" if kb_fallback else ""),
                                fallback=kb_fallback,
                            )
                    except Exception as e:
                        logging.warning(f"KB veto error: {e}")
                        _emit_gate_event(
                            scan_context,
                            "KB_VETO",
                            "fallback_bypass",
                            f"{type(e).__name__}: {e}",
                            fallback=True,
                            evaluated=False,
                        )
                    except Exception as e:
                        logging.warning(f"KB veto error: {e}")
                        _emit_gate_event(
                            scan_context,
                            "KB_VETO",
                            "fallback_bypass",
                            f"{type(e).__name__}: {e}",
                            fallback=True,
                            evaluated=False,
                        )
            else:
                _emit_gate_event(
                    scan_context,
                    "KB_VETO",
                    "pass",
                    "KB soft veto disabled or KnowledgeBrain unavailable",
                    evaluated=False,
                )

            # ============ SUBMIT ORDER ============
            pattern_tag = "+".join("M-Pattern" if name == "Mau-M" else name
                                    for name in agreeing_names)
            if zone_bonus:
                pattern_tag += "+Z"
            pattern_tag = pattern_tag[:40]

            debug_id = uuid.uuid4().hex
            signal_debug = {
                "debug_id": debug_id, "asset": TRADING_ASSET,
                "timeframe_seconds": CANDLE_TIME, "option_type": OPTION_TYPE,
                "amount": dashboard.get_invest_amount(),
                "expiry_minutes": TRADE_EXPIRY_MINUTES, "action": consensus_action,
                "pattern_tag": pattern_tag,
                "signals": [{"pattern": n, "direction": d, "source": s}
                             for n, d, s in all_signals],
                "agreed_patterns": agreeing_names, "neutral_patterns": neutral_names,
                "setup_stars": setup_stars, "setup_reason": setup_reason,
                "pa_effective_count": pa_effective_count,
                "pa_detector_count": len(pa_patterns_in_agree),
                "trend": trend, "trend15": t15, "sideways": sideways, "sideways15": s15,
                "zone_stars": zone_stars, "zone_stars_early": zone_stars_early,
                "zone_is_edge": zone_is_edge_early,
                "zone_ext_applied": zone_ext_applied,
                "minor_zone_soft_pass": bool(minor_zone_soft_pass_reasons),
                "minor_zone_soft_pass_reasons": minor_zone_soft_pass_reasons,
                "minor_zone_soft_pass_min_stars": (
                    MINOR_ZONE_TREND_OVERRIDE_MIN_STARS
                ),
                "zone": zone, "regime": regime, "session": session,
                "scan_started_late": scan_started_late,
                "scan_start_second": now.second,
                "entry_check_second": None,
                "override": override, "override_reason": override_reason,
                "countertrend_confirmation": countertrend_confirmation,
                "signal_candle": last.to_dict(), "previous_candle": prev.to_dict(),
                "m5_candle_position": m5_position,
                "m5_boundary_live_trade_skipped": skip_live_trade_m5_slot,
                "strategy_369": sig369, "strategy_369_reason": reason369,
                "pa_patterns": pa_patterns,
                "momentum_pa_pattern_enabled": ENABLE_MOMENTUM_PA_PATTERN,
                "momentum": _momentum_metrics,
                "learning_filter": learning_filter_debug,
                "market_structure": market_structure,
                "swing_retest_action": swing_retest_action,
            }
            _emit_trade_debug_event("signal_ready", scan_context, **signal_debug)

            if skip_live_trade_m5_slot:
                reason = (
                    f"M{m5_position} candle analyzed; live trade is disabled "
                    "for M0/M4"
                )
                log_msg(
                    f"      ⛔ [M5-TRADE-TIMING] {reason} → skip",
                    "warning",
                )
                _emit_trade_debug_event(
                    "signal_skipped_m5_boundary", scan_context, **signal_debug
                )
                wait_until_next_minute(dashboard, signal_reason=reason)
                continue

            in_cd, cd_rem = _is_asset_in_cooldown(TRADING_ASSET)
            if in_cd:
                _emit_gate_event(
                    scan_context,
                    "COOLDOWN",
                    "block",
                    f"{TRADING_ASSET} cooldown has {cd_rem}s remaining",
                )
                log_msg(f"⚡ SIGNAL {consensus_action.upper()} but cooldown {cd_rem}s → NO ORDER",
                        "warning")
                wait_until_next_minute(dashboard)
                continue
            _emit_gate_event(
                scan_context,
                "COOLDOWN",
                "pass",
                f"{TRADING_ASSET} is not in cooldown",
            )

            entry_check_time = datetime.now()
            if will_trade and not _is_live_entry_window_open(now, entry_check_time):
                scan_started_late = True
                will_trade = False
                signal_debug["scan_started_late"] = True
                signal_debug["entry_check_second"] = entry_check_time.second
                log_msg(
                    f"      ⛔ [SCAN-LATE] analysis completed at second "
                    f"{entry_check_time.second}; live order disabled → skip",
                    "warning",
                )
                _emit_gate_event(
                    scan_context,
                    "SCAN_LATE",
                    "skip",
                    f"analysis completed at second {entry_check_time.second}; "
                    "live order disabled",
                )
                _emit_trade_debug_event(
                    "signal_skipped_late_scan", scan_context, **signal_debug
                )

            if not will_trade:
                if weekend_observation:
                    reason = "weekend observation guard"
                elif ENABLE_ASSET_CHECK and asset_status == "CLOSED":
                    reason = "asset CLOSED"
                elif ENABLE_ASSET_CHECK and asset_status != "OPEN":
                    reason = f"asset availability {asset_status}"
                elif scan_started_late:
                    reason = (
                        f"scan started at second {now.second}; "
                        "late scan is analysis-only"
                    )
                else:
                    reason = "risk manager paused orders"
                log_msg(
                    f"⚡ SIGNAL {consensus_action.upper()} but {reason} → NO ORDER",
                    "warning",
                )
                wait_until_next_minute(dashboard)
                continue

            log_msg(f"SIGNAL {consensus_action.upper()} ({pattern_tag}) on {TRADING_ASSET} - submit",
                    "trade")

            if _balance_unavailable:
                _emit_gate_event(
                    scan_context,
                    "BALANCE",
                    "block",
                    "balance unavailable; order placement blocked",
                )
                log_msg(
                    f"      ⛔ Balance unavailable — order blocked",
                    "warning"
                )
                wait_until_next_minute(dashboard)
                continue
            _emit_gate_event(
                scan_context,
                "BALANCE",
                "pass",
                "balance is available for order placement",
            )

            cold_start_pattern = None
            cold_start_decision = None
            cold_start_quality = None
            cold_start_fallback = False
            cold_start_error = None
            if ENABLE_COLD_START_GATE:
                if (
                    _knowledge_brain is None
                    or not _knowledge_brain.is_ready()
                ):
                    reason = "KnowledgeBrain is unavailable"
                    _emit_gate_event(
                        scan_context,
                        "COLD_START",
                        "block",
                        reason,
                    )
                    log_msg(f"      ⛔ [COLD-START] {reason} → skip", "warning")
                    wait_until_next_minute(dashboard)
                    continue
                cold_start_pattern = agreeing_names[0] if agreeing_names else None
                if cold_start_pattern is None:
                    reason = "no primary pattern is available"
                    _emit_gate_event(
                        scan_context,
                        "COLD_START",
                        "block",
                        reason,
                    )
                    log_msg(f"      ⛔ [COLD-START] {reason} → skip", "warning")
                    wait_until_next_minute(dashboard)
                    continue

                pattern_status = (
                    "active"
                    if cold_start_pattern in _knowledge_brain.PATTERN_PRIORS
                    else "candidate"
                )
                try:
                    cold_start_decision = cold_start_eval(
                        _knowledge_brain,
                        cold_start_pattern,
                        pattern_status,
                        max_samples=COLD_START_MAX_SAMPLES,
                        quality_floor=COLD_START_QUALITY_FLOOR,
                        active_quality_floor=COLD_START_ACTIVE_QUALITY_FLOOR,
                        hard_disable=COLD_START_HARD_DISABLE,
                    )
                    cold_start_context = {
                        "session": session,
                        "regime": regime,
                        "zone_stars": zone_stars,
                        "zone_is_edge": zone_is_edge_early,
                        "trend_m5": trend,
                        "trend_m15": t15,
                        "sideways": sideways,
                        "sideways_m15": s15,
                        "vol_ratio": _vol_ratio,
                        "pa_count": pa_effective_count,
                    }
                    (
                        cold_start_quality,
                        cold_start_fallback,
                        cold_start_error,
                    ) = _knowledge_brain.score_with_status(
                        cold_start_pattern,
                        consensus_action,
                        cold_start_context,
                        source="live",
                    )
                except Exception as error:
                    reason = (
                        f"unable to evaluate {cold_start_pattern}: "
                        f"{type(error).__name__}: {error}"
                    )
                    _emit_gate_event(
                        scan_context,
                        "COLD_START",
                        "block",
                        reason,
                        fallback=True,
                    )
                    log_msg(f"      ⛔ [COLD-START] {reason} → skip", "warning")
                    wait_until_next_minute(dashboard)
                    continue

                if not cold_start_decision.allowed:
                    reason = (
                        f"{cold_start_pattern} blocked: "
                        f"{cold_start_decision.reason}"
                    )
                    _emit_gate_event(
                        scan_context,
                        "COLD_START",
                        "block",
                        reason,
                        fallback=cold_start_fallback,
                    )
                    log_msg(f"      ⛔ [COLD-START] {reason} → skip", "warning")
                    _emit_trade_debug_event(
                        "cold_start_gate_block",
                        scan_context,
                        asset=TRADING_ASSET,
                        action=consensus_action,
                        pattern=cold_start_pattern,
                        quality=cold_start_quality,
                        decision=cold_start_decision.__dict__,
                    )
                    wait_until_next_minute(dashboard)
                    continue

                quality_valid = (
                    not isinstance(cold_start_quality, bool)
                    and isinstance(cold_start_quality, (int, float))
                    and math.isfinite(cold_start_quality)
                )
                if not quality_valid:
                    reason = f"{cold_start_pattern} returned invalid quality score"
                elif cold_start_quality < cold_start_decision.quality_floor:
                    reason = (
                        f"{cold_start_pattern} quality={cold_start_quality:.2f} "
                        f"< floor={cold_start_decision.quality_floor:.2f} "
                        f"({cold_start_decision.reason})"
                    )
                else:
                    reason = None
                if reason is not None:
                    _emit_gate_event(
                        scan_context,
                        "COLD_START",
                        "block",
                        reason,
                        fallback=cold_start_fallback,
                    )
                    log_msg(f"      ⛔ [COLD-START] {reason} → skip", "warning")
                    _emit_trade_debug_event(
                        "cold_start_gate_block",
                        scan_context,
                        asset=TRADING_ASSET,
                        action=consensus_action,
                        pattern=cold_start_pattern,
                        quality=cold_start_quality,
                        decision=cold_start_decision.__dict__,
                    )
                    wait_until_next_minute(dashboard)
                    continue
                _emit_gate_event(
                    scan_context,
                    "COLD_START",
                    "pass",
                    f"{cold_start_pattern} quality meets "
                    f"{cold_start_decision.quality_floor:.2f} floor"
                    + (f"; scoring fallback: {cold_start_error}" if cold_start_fallback else ""),
                    fallback=cold_start_fallback,
                )
            else:
                _emit_gate_event(
                    scan_context,
                    "COLD_START",
                    "pass",
                    "cold-start gate disabled",
                    evaluated=False,
                )

            trade_placed = False
            lock_acquired = dashboard.trade_lock.acquire(timeout=TRADE_LOCK_TIMEOUT)
            if not lock_acquired:
                _emit_gate_event(
                    scan_context,
                    "TRADE_LOCK",
                    "block",
                    "could not acquire trade lock before timeout",
                )
                log_msg("      Cannot acquire trade lock", "warning")
                wait_until_next_minute(dashboard)
                continue

            try:
                if not dashboard.is_running():
                    _emit_gate_event(
                        scan_context,
                        "BOT_STATE",
                        "block",
                        "bot paused before order placement",
                    )
                    log_msg("      Bot paused; cancel order")
                    wait_until_next_minute(dashboard)
                    continue
                if not license_client.check():
                    _emit_gate_event(
                        scan_context,
                        "LICENSE",
                        "block",
                        "license check failed before order placement",
                    )
                    license_state = license_client.snapshot()
                    dashboard.pause_for_license(license_state, trade_lock_held=True)
                    log_msg("      License invalid; blocked.", "warning")
                    continue
                _emit_gate_event(
                    scan_context,
                    "LICENSE",
                    "pass",
                    "license check passed before order placement",
                )

                try:
                    base_unit = dashboard.get_invest_amount()
                    if base_unit is None:
                        _emit_gate_event(
                            scan_context,
                            "STAKE",
                            "block",
                            "stake unavailable",
                        )
                        log_msg("      Stake unavailable; blocked.", "warning")
                        continue
                    stake_usd = base_unit
                    if not math.isfinite(float(stake_usd)) or stake_usd <= 0:
                        reason = f"invalid license stake {stake_usd}"
                        _emit_gate_event(
                            scan_context,
                            "STAKE",
                            "block",
                            reason,
                        )
                        log_msg(f"      ⛔ [STAKE] {reason} → skip", "warning")
                        continue
                    _emit_gate_event(
                        scan_context,
                        "STAKE",
                        "pass",
                        f"license stake is ${stake_usd:.2f}",
                    )
                    if cold_start_decision is not None:
                        log_msg(
                            f"      [GATE] {cold_start_pattern} TRADE | "
                            f"quality={cold_start_quality:.2f} | stake=${stake_usd:.2f} "
                            f"({cold_start_decision.reason}, "
                            f"samples={cold_start_decision.samples_so_far})",
                            "trade",
                        )
                    current_count = _get_trades_today()
                    log_msg(f"      [v8.3] Trades today: {current_count} -> checking {current_count + 1}")
                    license_client.assert_trade_allowed(stake_usd, current_count + 1)
                except LicenseLimitExceeded as error:
                    _emit_gate_event(
                        scan_context,
                        "LICENSE_TRADE_LIMIT",
                        "block",
                        str(error),
                    )
                    log_msg(f"      Trade blocked by license: {error}", "warning")
                    continue
                _emit_gate_event(
                    scan_context,
                    "LICENSE_TRADE_LIMIT",
                    "pass",
                    "stake and trade count allowed by license",
                )

                log_msg(f"      Option: {OPTION_TYPE} | Asset: {TRADING_ASSET} | Expiry: {TRADE_EXPIRY_MINUTES}m")
                with _phase("execute_trade"):
                    success, trade_id, uncertain = execute_trade(
                        iq, TRADING_ASSET, consensus_action, stake_usd, TRADE_EXPIRY_MINUTES,
                        pattern_tag, option_type=OPTION_TYPE, debug_id=debug_id)

                if not success:
                    _emit_trade_debug_event("signal_order_failed", scan_context, **signal_debug,
                                     success=False, trade_id=None, timed_out=False,
                                     submission_uncertain=(uncertain == 'unknown'))
                    if uncertain == 'unknown':
                        _increment_trades_today()
                    if uncertain == 'asset_closed':
                        log_msg(f"      Broker: asset closed -> cooldown", "warning")
                    else:
                        log_msg(f"      Broker rejected ({uncertain or 'unknown'})", "warning")
                    _set_asset_cooldown(TRADING_ASSET, ASSET_COOLDOWN_ON_FAILURE)
                    dashboard.update(bot_status="Broker rejected")
                else:
                    trade_placed = True
                    _last_trade_time['ts'] = time.time()
                    _emit_trade_debug_event(
                        "signal_order_succeeded",
                        scan_context,
                        debug_id=debug_id,
                        trade_id=trade_id,
                        option_type=OPTION_TYPE,
                    )

                    kb_contexts = [
                        {
                            'pattern': s[0], 'action': s[1],
                            'session': session,
                            'regime': regime,
                            'zone_stars': zone_stars_early,
                            'zone_is_edge': zone_is_edge_early,
                            'trend_m5': trend, 'trend_m15': t15,
                            'sideways': sideways, 'sideways_m15': s15,
                            'vol_ratio': _vol_ratio,
                            'pa_count': pa_effective_count,
                            'rsi': _momentum_metrics['rsi'] if _momentum_metrics else None,
                            'position_in_range': _momentum_metrics['pos'] if _momentum_metrics else None,
                        } for s in agreeing
                    ]

                    stats.add_pending(trade_id, consensus_action, pattern_tag,
                                       stake_usd, TRADE_EXPIRY_SECONDS,
                                       option_type=OPTION_TYPE, features=None,
                                       kb_contexts=kb_contexts)
                    tracker.track(
                        trade_id,
                        consensus_action,
                        stake_usd,
                        time.time() + TRADE_EXPIRY_SECONDS,
                        entry_price=float(last['close']),
                        balance_before=cur_balance,
                        asset=TRADING_ASSET,
                        pattern=pattern_tag,
                        kb_contexts=kb_contexts,
                    )
                    _increment_trades_today()
                    log_msg(f"      Trade ID: {trade_id} [today #{_get_trades_today()}] | "
                            f"KB tracking: {[s[0] for s in agreeing]}", "trade")
                    dashboard.update(
                        bot_status=f"Entered {consensus_action.upper()} {TRADING_ASSET}",
                        current_signal=consensus_action.upper(),
                        current_signal_reason=(
                            f"Order placed: {consensus_action.upper()} {TRADING_ASSET} "
                            f"({pattern_tag})."
                        ),
                    )
            finally:
                try:
                    dashboard.trade_lock.release()
                except Exception:
                    pass

            _heartbeat()
            try:
                with _phase("after_trade"):
                    bal_after = _get_balance(iq, default=cur_balance, name="after_trade")
                dashboard.update(current_balance=bal_after)
            except Exception:
                pass
            stats.display_compact(cur_balance)
            wait_until_next_minute(dashboard, mark_signal_skip=not trade_placed)

        except KeyboardInterrupt:
            _stop_shadow_mode()
            try:
                if _knowledge_brain is not None:
                    kb_stats = _knowledge_brain.get_stats()
                    log_msg(f"🧠 KB saved {kb_stats['n_keys']} keys", "trade")
            except Exception:
                pass
            dashboard.update(bot_running=False, bot_status="Stopped")
            try:
                final_bal = _get_balance(iq, default=0.0, name="final_balance")
                stats.display_dashboard(final_bal)
            except Exception:
                pass
            sys.exit(0)
        except Exception as e:
            dashboard.update(bot_status="Error")
            log_msg(f"Error: {e}", "error")
            import traceback
            logging.error(traceback.format_exc())
            _stop_event.wait(5)
            try:
                still_connected = call_with_timeout(
                    iq.check_connect,
                    timeout=TIMEOUT_CHECK_CONNECT,
                    default=False,
                    name="reconnect_check",
                )
                if not still_connected:
                    iq = connect_iq_with_heartbeat(
                        on_status=update_conn,
                        credentials=dashboard.get_credentials(),
                        account_type_provider=dashboard.get_account_type,
                    )
                    _iq_holder['iq'] = iq
                    tracker.broker = iq
                    _invalidate_candle_cache()
                    _invalidate_connection_cache()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
