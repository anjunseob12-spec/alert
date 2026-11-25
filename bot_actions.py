"""
[코스피200 · 코스닥200 올인원 스마트 알림 봇 (bot_actions.py - v9.2)]
- 유니버스: 코스피 시총 상위 200 + 코스닥 시총 상위 200 (보통주 400종목 + 기존 감시종목 추적 유지)
- 핵심 검증 반영:
  1. 네이버 금융 장중 시세 일괄 동기화로 KRX StockListing 장중 정체(전일종가 고정) 문제 해결
  2. MarketType 컬럼 명시 할당으로 KeyError 원천 차단
  3. 마감 브리핑(15:55~16:15) 재검증 강화: NEW_6 지수 -1.0% 이하 유지 확인 & NEW_4 마감 RVOL <= 120% 확인
  4. ETF/KR 등락률 스케일(% vs 소수점) 자동 검증, 실행 로그 출력 및 배치 시차 폴백 보완
  5. 005930 기본 일봉 기반 휴장일 가드 + KRX: 수정주가 런타임 자동 검증 + signal_history.csv 주간 집계
"""

import os
import csv
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd
import FinanceDataReader as fdr

KST = timezone(timedelta(hours=9))
STATE_FILE = "alert_state.json"
CSV_FILE = "signal_history.csv"

CSV_HEADERS = [
    "date", "code", "name", "market", "sig_id", "sig_label",
    "entry_price", "stop_price", "target_price",
    "days_elapsed", "d1_ret", "d5_ret", "max_high", "min_low", "outcome"
]

# ==============================================================================
# 사용자 알림 기준 설정
# ==============================================================================
CONFIG = {
    "TOP_N": 200,                        # 코스피 상위 200 + 코스닥 상위 200 (400종목)
    "EXCLUDE_PREFERRED": True,           # 우선주 제외 여부

    # [기존 4종 알림 기준]
    "THRESHOLDS": [-5.0, -10.0, -15.0],
    "PIVOT_WINDOW_DAYS": 10,
    "LOW_TOUCH_BUFFER_PCT": 0.5,
    "MIN_DROP_FOR_PREV_LOW": -1.5,
    "MIN_AMOUNT_FOR_PREV_LOW_UK": 150,
    "VOL_SURGE_PCT": 300.0,
    "MIN_TRADE_AMOUNT_UK": 200,

    # [신규 8종 공통 필터]
    "MIN_ADV20_KRW": 10_000_000_000,     # 20일 평균 거래대금 100억 이상
    "COOLDOWN_TRADING_DAYS": 5,          # 동일 종목·동일 시그널 5거래일 쿨다운
    "HIST_LOW_REFRESH_DAYS": 7,          # 액면분할·권리락 반영을 위한 전체 일봉 재조회 주기(일)
}


def load_state():
    today = datetime.now(KST).strftime("%Y-%m-%d")
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") == today and data.get("version") == 9:
            return data
        return {
            "date": today,
            "version": 9,
            "alerted": {},
            "cooldowns": data.get("cooldowns", {}),
            "hist_lows": data.get("hist_lows", {}),
            "hist_low_dates": data.get("hist_low_dates", {}),
            "active_setups": data.get("active_setups", {}),
            "trading_dates": data.get("trading_dates", []),
            "etf_history": [],
            "today_signals": {},
            "levels": {},
            "index_base": {},
            "health": {},
        }
    return {
        "date": today,
        "version": 9,
        "alerted": {},
        "cooldowns": {},
        "hist_lows": {},
        "hist_low_dates": {},
        "active_setups": {},
        "trading_dates": [],
        "etf_history": [],
        "today_signals": {},
        "levels": {},
        "index_base": {},
        "health": {},
    }


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def can_fire_once(state, code, event_key):
    already = state["alerted"].get(code, [])
    return event_key not in already


def commit_once(state, code, event_key):
    already = state["alerted"].setdefault(code, [])
    if event_key not in already:
        already.append(event_key)


def get_trading_day_diff(state, past_date, today_date):
    t_dates = state.get("trading_dates", [])
    if past_date in t_dates and today_date in t_dates:
        return t_dates.index(today_date) - t_dates.index(past_date)
    try:
        d1 = datetime.strptime(past_date, "%Y-%m-%d")
        d2 = datetime.strptime(today_date, "%Y-%m-%d")
        return max(0, int((d2 - d1).days * 5 / 7))
    except Exception:
        return 0


def can_fire_cooldown(state, code, sig_key):
    if not can_fire_once(state, code, sig_key):
        return False
    today = state["date"]
    last_date = state["cooldowns"].get(f"{code}:{sig_key}")
    if last_date and last_date != today:
        diff = get_trading_day_diff(state, last_date, today)
        if diff < CONFIG["COOLDOWN_TRADING_DAYS"]:
            return False
    return True


def commit_cooldown(state, code, sig_key):
    commit_once(state, code, sig_key)
    state["cooldowns"][f"{code}:{sig_key}"] = state["date"]


def record_today_signal(state, code, name, market_type, sig_id, sig_label, entry_price, stop_price, target_price, buy_zone):
    """15:55 장 마감 브리핑 TOP 3 및 성과 CSV 기록용 시그널 적재"""
    entry = state["today_signals"].setdefault(code, {
        "name": name,
        "market": market_type,
        "signals": {},
    })
    entry["signals"][sig_id] = {
        "label": sig_label,
        "entry": float(entry_price),
        "stop": float(stop_price),
        "target": float(target_price),
        "buy_zone": buy_zone,
    }
    append_signal_csv(
        date_str=state["date"],
        code=code,
        name=name,
        market=market_type,
        sig_id=sig_id,
        sig_label=sig_label,
        entry_price=entry_price,
        stop_price=stop_price,
        target_price=target_price,
    )


def register_active_setup(state, code, name, market_type, sig_id, sig_label, setup_high, setup_low, setup_hh120, stop_price, target_price):
    """#4·#6·#7 준비 신호를 최대 5거래일 후속 트리거 감시 목록에 등록"""
    setups = state.setdefault("active_setups", {})
    key = f"{code}:{sig_id}"
    setups[key] = {
        "code": code,
        "name": name,
        "market": market_type,
        "sig_id": sig_id,
        "sig_label": sig_label,
        "created_date": state["date"],
        "setup_high": float(setup_high),
        "setup_low": float(setup_low),
        "setup_hh120": float(setup_hh120),
        "stop_price": float(stop_price),
        "target_price": float(target_price),
    }


def ensure_csv_exists():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()


def get_open_csv_codes():
    """아직 5거래일 추적이 끝나지 않은 CSV 종목 코드 반환"""
    if not os.path.exists(CSV_FILE):
        return set()
    codes = set()
    try:
        with open(CSV_FILE, "r", newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                elapsed = int(float(r.get("days_elapsed") or 0))
                if r.get("outcome") == "OPEN" or elapsed < 5:
                    if r.get("code"):
                        codes.add(str(r["code"]).zfill(6))
    except Exception:
        pass
    return codes


def append_signal_csv(date_str, code, name, market, sig_id, sig_label, entry_price, stop_price, target_price):
    ensure_csv_exists()
    with open(CSV_FILE, "r", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    for r in rows:
        if r["date"] == date_str and r["code"] == code and r["sig_id"] == sig_id:
            return

    new_row = {
        "date": date_str,
        "code": code,
        "name": name,
        "market": market,
        "sig_id": sig_id,
        "sig_label": sig_label,
        "entry_price": round(float(entry_price), 2),
        "stop_price": round(float(stop_price), 2),
        "target_price": round(float(target_price), 2),
        "days_elapsed": 0,
        "d1_ret": "",
        "d5_ret": "",
        "max_high": round(float(entry_price), 2),
        "min_low": round(float(entry_price), 2),
        "outcome": "OPEN",
    }
    rows.append(new_row)
    with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def update_signal_csv_performance(state, live_snapshots, hhmm):
    """
    과거 발동 시그널 성과 추적:
    - 장중(hhmm < 1555): max_high, min_low 및 STOP_HIT/TARGET_HIT/BOTH_HIT만 갱신
    - 마감(hhmm >= 1555): D+1/D+5 종가 수익률, EXPIRED_5D, days_elapsed 최종 확정
    """
    ensure_csv_exists()
    with open(CSV_FILE, "r", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return

    today = state["date"]
    is_closing = (hhmm >= 1555)
    changed = False

    for r in rows:
        sig_date = r["date"]
        if sig_date >= today:
            continue

        diff = get_trading_day_diff(state, sig_date, today)
        if diff <= 0:
            continue

        prev_elapsed = int(float(r.get("days_elapsed") or 0))
        if prev_elapsed >= 5 and r.get("d5_ret") != "":
            continue

        snap = live_snapshots.get(str(r["code"]).zfill(6))
        if not snap:
            continue

        entry_p = float(r["entry_price"])
        stop_p = float(r["stop_price"])
        target_p = float(r["target_price"])
        c, h, l = snap["c"], snap["h"], snap["l"]

        if diff <= 5:
            max_h = max(float(r.get("max_high") or entry_p), h)
            min_l = min(float(r.get("min_low") or entry_p), l)
            if str(round(max_h, 2)) != r.get("max_high") or str(round(min_l, 2)) != r.get("min_low"):
                r["max_high"] = round(max_h, 2)
                r["min_low"] = round(min_l, 2)
                changed = True

            if r["outcome"] == "OPEN":
                hit_stop = (stop_p > 0 and l <= stop_p)
                hit_target = (target_p > 0 and h >= target_p)
                if hit_stop and hit_target:
                    r["outcome"] = "BOTH_HIT"
                    changed = True
                elif hit_stop:
                    r["outcome"] = "STOP_HIT"
                    changed = True
                elif hit_target:
                    r["outcome"] = "TARGET_HIT"
                    changed = True

        if is_closing:
            ret_pct = round(((c / entry_p) - 1.0) * 100.0, 2) if entry_p > 0 else 0.0
            if diff == 1 and r.get("d1_ret") == "":
                r["d1_ret"] = ret_pct
                changed = True
            if diff >= 5 and r.get("d5_ret") == "":
                r["d5_ret"] = ret_pct
                if r["outcome"] == "OPEN":
                    r["outcome"] = "EXPIRED_5D"
                changed = True

            new_elapsed = min(5, diff)
            if new_elapsed != prev_elapsed:
                r["days_elapsed"] = new_elapsed
                changed = True

    if changed:
        with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
            writer.writerows(rows)


def build_weekly_performance_summary():
    """금요일 마감 브리핑용 시그널 성과 집계 텍스트 생성"""
    if not os.path.exists(CSV_FILE):
        return ""
    with open(CSV_FILE, "r", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    aged = [r for r in rows if int(float(r.get("days_elapsed") or 0)) >= 1]
    if not aged:
        return ""

    by_sig = {}
    for r in aged:
        label = r["sig_label"]
        grp = by_sig.setdefault(label, {"cnt": 0, "d1": [], "d5": [], "target": 0, "stop": 0})
        grp["cnt"] += 1
        if r.get("d1_ret") != "":
            grp["d1"].append(float(r["d1_ret"]))
        if r.get("d5_ret") != "":
            grp["d5"].append(float(r["d5_ret"]))
        if r["outcome"] in ("TARGET_HIT", "BOTH_HIT"):
            grp["target"] += 1
        if r["outcome"] in ("STOP_HIT", "BOTH_HIT"):
            grp["stop"] += 1

    lines = ["📊 **주간 시그널 누적 성과 집계 (D+1 이상 표본 기준)**"]
    for label, g in sorted(by_sig.items()):
        d1_avg = f"{sum(g['d1'])/len(g['d1']):+.2f}%" if g["d1"] else "-"
        d5_avg = f"{sum(g['d5'])/len(g['d5']):+.2f}%" if g["d5"] else "-"
        t_rate = (g["target"] / g["cnt"]) * 100.0
        s_rate = (g["stop"] / g["cnt"]) * 100.0
        lines.append(
            f"• `{label}` ({g['cnt']}건): D+1 `{d1_avg}` | D+5 `{d5_avg}` | 목표도달 `{t_rate:.0f}%` / 손절 `{s_rate:.0f}%`"
        )
    return "\n".join(lines)


def send_discord_embed(webhook_url, title, description, color, fields=None, author_text=None):
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if author_text:
        embed["author"] = {"name": author_text}
    if fields:
        embed["fields"] = fields

    for _ in range(3):
        try:
            res = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
            if res.status_code == 429:
                retry_after = float(res.json().get("retry_after", 2.0))
                time.sleep(retry_after + 0.5)
                continue
            res.raise_for_status()
            time.sleep(1.2)
            return True
        except Exception as e:
            print(f"디스코드 전송 오류: {e}")
            time.sleep(1.5)
    return False


def send_individual_card(webhook_url, market_type, title, meaning, action_guide, color, fields):
    author_text = "🔵 코스피 200 (KOSPI)" if market_type == "KOSPI" else "🟠 코스닥 200 (KOSDAQ)"
    full_fields = list(fields) + [
        {
            "name": "🧭 지금 해야 할 행동 / 체크포인트",
            "value": action_guide,
            "inline": False,
        }
    ]
    return send_discord_embed(
        webhook_url,
        title=title,
        description=f"📖 **알림 의미:** {meaning}",
        color=color,
        fields=full_fields,
        author_text=author_text,
    )


def _verify_krx_adjusted():
    """
    KRX:005930이 수정주가로 내려오는지 런타임 자동 검증:
    2018-05-04(50:1 액면분할) 이전 종가가 20만원 초과이면 무수정주가이므로 False 반환
    """
    try:
        df = fdr.DataReader("KRX:005930", "2017-06-01")
        if df is None or len(df) < 100:
            return False
        pre_split = df[df.index < "2018-05-04"]["Close"].astype(float)
        if len(pre_split) == 0:
            return False
        if float(pre_split.max()) > 200_000:
            print("KRX: 소스 무수정주가 감지 -> 기본 수정주가 일봉 사용")
            return False
        return True
    except Exception:
        return False


def _fetch_daily_ohlcv(code, start_date=None, use_krx_prefix=False):
    """수정주가 검증된 KRX 정규장 일봉 우선 조회, 실패 시 기본 일봉 폴백"""
    if use_krx_prefix:
        try:
            df = fdr.DataReader(f"KRX:{code}", start_date) if start_date else fdr.DataReader(f"KRX:{code}")
            if df is not None and len(df) > 0:
                return df, "KRX"
        except Exception:
            pass

    try:
        df = fdr.DataReader(code, start_date) if start_date else fdr.DataReader(code)
        if df is not None and len(df) > 0:
            return df, "FALLBACK"
    except Exception:
        pass

    return None, "FAIL"


def check_trading_day_and_index_history(state, hhmm):
    """
    1) 09:00~09:19: KRX 20분 지연으로 당일 스냅샷 미생성 구간이므로 대기 종료
    2) hhmm >= 920: 기본 fdr.DataReader("005930") 마지막 날짜가 오늘이 아니면 휴장일로 즉시 종료 (상한 없음)
    3) 거래일 캘린더 및 KS11·KQ11 직전 거래일 종가 검증 수집
    """
    if 900 <= hhmm < 920:
        print("09:20 이전(KRX 20분 지연 대기 구간)이므로 종료합니다.")
        return False

    today_str = state["date"]
    start_date = (datetime.now(KST) - timedelta(days=430)).strftime("%Y-%m-%d")

    try:
        probe_df = fdr.DataReader("005930", start_date)
    except Exception as e:
        print(f"기준 종목(005930) 일봉 조회 실패로 안전 종료합니다: {e}")
        return False

    if probe_df is None or len(probe_df) < 130:
        print("기준 종목(005930) 데이터 부족으로 안전 종료합니다.")
        return False

    last_probe_date = str(probe_df.index[-1])[:10]
    if hhmm >= 920 and last_probe_date != today_str:
        print(f"오늘은 휴장일입니다 (최근 거래일: {last_probe_date}). 알림 없이 종료합니다.")
        return False

    probe_dates = [str(d)[:10] for d in probe_df.index]
    if hhmm >= 920:
        if probe_dates[-1] != today_str:
            probe_dates.append(today_str)
        state["trading_dates"] = probe_dates[-160:]
        prev_trading_day = state["trading_dates"][-2] if len(state["trading_dates"]) >= 2 else ""
    else:
        state["trading_dates"] = probe_dates[-160:]
        prev_trading_day = state["trading_dates"][-1] if state["trading_dates"] else ""

    if state.get("index_base") and state["index_base"].get("valid"):
        return True

    idx_base = {"valid": True}
    for idx_code in ["KS11", "KQ11"]:
        try:
            raw_df = fdr.DataReader(idx_code, start_date)
            if raw_df is None or len(raw_df) < 130:
                idx_base["valid"] = False
                continue
            df = raw_df.iloc[:-1] if str(raw_df.index[-1])[:10] == today_str else raw_df
            last_idx_date = str(df.index[-1])[:10]
            if prev_trading_day and last_idx_date != prev_trading_day:
                print(f"경고: {idx_code} 마지막 일봉({last_idx_date})이 직전 거래일({prev_trading_day})과 불일치")
                idx_base["valid"] = False
            closes = df["Close"].astype(float)
            dates = [str(d)[:10] for d in df.index]
            idx_base[idx_code] = {
                "prev_close": float(closes.iloc[-1]),
                "last_date": last_idx_date,
                "closes_map": {dates[i]: float(closes.iloc[i]) for i in range(max(0, len(dates) - 140), len(dates))},
            }
        except Exception as e:
            print(f"지수({idx_code}) 히스토리 조회 오류: {e}")
            idx_base["valid"] = False

    state["index_base"] = idx_base
    return True


def _parse_etf_pct(row, chg_col):
    """ETF/KR 등락률이 퍼센트(-1.50)인지 소수점(-0.015)인지 자동 검증해 퍼센트 단위로 반환"""
    val = pd.to_numeric(row.get(chg_col), errors="coerce")
    if pd.isna(val):
        return None
    rate = float(val)

    price = pd.to_numeric(row.get("Price", row.get("Close")), errors="coerce")
    change = pd.to_numeric(row.get("Change", row.get("Changes")), errors="coerce")
    if pd.notna(price) and pd.notna(change):
        prev_p = float(price) - float(change)
        if prev_p > 0:
            calc_pct = (float(change) / prev_p) * 100.0
            if abs(calc_pct) > 0.05 and abs(rate * 100.0 - calc_pct) < abs(rate - calc_pct):
                return round(rate * 100.0, 4)
    return rate


def fetch_naver_intraday_quotes(codes):
    """네이버 금융 장중 시세 API로 400종목 시가/고가/저가/현재가/거래량/거래대금 일괄 조회"""
    quotes = {}
    code_list = list(dict.fromkeys(str(c).zfill(6) for c in codes if c))
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/",
    }
    chunk_size = 80
    for i in range(0, len(code_list), chunk_size):
        chunk = code_list[i : i + chunk_size]
        query = ",".join(f"SERVICE_ITEM:{c}" for c in chunk)
        url = f"https://polling.finance.naver.com/api/realtime?query={query}"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            areas = res.json().get("result", {}).get("areas", [])
            for area in areas:
                for item in area.get("datas", []):
                    cd = str(item.get("cd", "")).zfill(6)
                    nv = float(item.get("nv") or 0)
                    if not cd or nv <= 0:
                        continue
                    ov = float(item.get("ov") or nv)
                    hv = float(item.get("hv") or nv)
                    lv = float(item.get("lv") or nv)
                    aq = float(item.get("aq") or 0)
                    aa = float(item.get("aa") or 0)
                    sv = float(item.get("sv") or 0)
                    cr = float(item.get("cr") or 0)

                    est_amt = ((hv + lv + nv) / 3.0) * aq
                    if aa > 0 and est_amt > 0:
                        if 0.2 <= (aa * 1_000_000.0) / est_amt <= 5.0:
                            amt = aa * 1_000_000.0
                        elif 0.2 <= aa / est_amt <= 5.0:
                            amt = aa
                        else:
                            amt = est_amt
                    else:
                        amt = est_amt

                    quotes[cd] = {
                        "Close": nv,
                        "Open": ov if ov > 0 else nv,
                        "High": hv if hv > 0 else nv,
                        "Low": lv if lv > 0 else nv,
                        "Volume": aq,
                        "Amount": amt,
                        "ChagesRatio": cr,
                        "NaverPrevClose": sv,
                    }
        except Exception as e:
            print(f"네이버 장중 시세 배치 조회 오류({i}~{i+len(chunk)}): {e}")
    return quotes


def fetch_top_universe_and_etf(state, hhmm):
    """
    코스피200 + 코스닥200 유니버스 조회 + 네이버 금융 장중 시세 일괄 동기화
    + ETF/KR(069500, 229200) 등락률 스케일 검증·로그 출력 및 20분 지연 시차 동기화
    """
    tracked_codes = {str(st["code"]).zfill(6) for st in state.get("active_setups", {}).values()} | get_open_csv_codes()

    frames = []
    for market in ["KOSPI", "KOSDAQ"]:
        df = fdr.StockListing(market).copy()
        df["Code"] = df["Code"].astype(str).str.zfill(6)
        df["MarketType"] = market
        if CONFIG["EXCLUDE_PREFERRED"]:
            df = df[df["Code"].str.endswith("0")]
        df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce")
        df = df.sort_values("Marcap", ascending=False).reset_index(drop=True)

        top_df = df.head(CONFIG["TOP_N"]).copy()
        top_df["InTop200"] = True
        top_df["MarketType"] = market

        extra_df = df[df["Code"].isin(tracked_codes) & ~df["Code"].isin(top_df["Code"])].copy()
        if not extra_df.empty:
            extra_df["InTop200"] = False
            extra_df["MarketType"] = market
            frames.append(pd.concat([top_df, extra_df], ignore_index=True))
        else:
            frames.append(top_df)

    combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Code"], keep="first").reset_index(drop=True)
    for col in ["Open", "High", "Close", "Low", "ChagesRatio", "Volume", "Amount"]:
        if col not in combined.columns:
            combined[col] = 0.0
        combined[col] = pd.to_numeric(combined[col], errors="coerce").astype("float64")

    # KRX StockListing의 장중 전일종가 고정을 방지하기 위해 네이버 금융 장중 시세로 덮어쓰기
    live_quotes = fetch_naver_intraday_quotes(combined["Code"].tolist())
    if live_quotes:
        for idx, row in combined.iterrows():
            cd = str(row["Code"]).zfill(6)
            q = live_quotes.get(cd)
            if q:
                combined.at[idx, "Close"] = q["Close"]
                combined.at[idx, "Open"] = q["Open"]
                combined.at[idx, "High"] = q["High"]
                combined.at[idx, "Low"] = q["Low"]
                combined.at[idx, "Volume"] = q["Volume"]
                combined.at[idx, "Amount"] = q["Amount"]
                combined.at[idx, "ChagesRatio"] = q["ChagesRatio"]
        sample_ss = live_quotes.get("005930")
        sample_txt = f" | 삼성전자 {sample_ss['ChagesRatio']:+.2f}%" if sample_ss else ""
        print(f"장중 시세 동기화: {len(live_quotes)}/{len(combined)}종목{sample_txt}")
    else:
        print("경고: 네이버 장중 시세 조회 실패 -> KRX 기본 목록 값 사용")

    etf_now = {}
    try:
        etf = fdr.StockListing("ETF/KR")
        sym_col = "Symbol" if "Symbol" in etf.columns else "Code"
        chg_col = "ChangeRate" if "ChangeRate" in etf.columns else "ChagesRatio"
        sub = etf[etf[sym_col].astype(str).isin(["069500", "229200"])]
        for _, r in sub.iterrows():
            pct_val = _parse_etf_pct(r, chg_col)
            if pct_val is not None:
                etf_now[str(r[sym_col])] = pct_val
        print(f"ETF/KR 등락률(%): {etf_now}")
    except Exception as e:
        print(f"ETF/KR 조회 오류: {e}")

    now_ts = time.time()
    etf_hist = state.setdefault("etf_history", [])
    if "069500" in etf_now and "229200" in etf_now:
        etf_hist.append({
            "ts": now_ts,
            "hhmm": hhmm,
            "069500": etf_now["069500"],
            "229200": etf_now["229200"],
        })
        state["etf_history"] = etf_hist[-50:]

    aligned_etf = {}
    if 920 <= hhmm <= 1550:
        best_item = None
        best_diff = 999999
        for item in etf_hist:
            age = now_ts - item["ts"]
            if 12 * 60 <= age <= 35 * 60:
                diff = abs(age - 20 * 60)
                if diff < best_diff:
                    best_diff = diff
                    best_item = item
        if best_item is None:
            for item in etf_hist:
                age = now_ts - item["ts"]
                if 4 * 60 <= age <= 50 * 60:
                    diff = abs(age - 20 * 60)
                    if diff < best_diff:
                        best_diff = diff
                        best_item = item
        if best_item:
            aligned_etf = {"069500": best_item["069500"], "229200": best_item["229200"]}
        elif "069500" in etf_now and "229200" in etf_now:
            aligned_etf = etf_now
    else:
        if "069500" in etf_now and "229200" in etf_now:
            aligned_etf = etf_now

    return combined, aligned_etf, bool(etf_now)


def _need_full_hist_refresh(today_str, last_refreshed_str, cached_hist_low):
    if cached_hist_low is None or cached_hist_low <= 0 or not last_refreshed_str:
        return True
    try:
        d1 = datetime.strptime(last_refreshed_str, "%Y-%m-%d")
        d2 = datetime.strptime(today_str, "%Y-%m-%d")
        return (d2 - d1).days >= CONFIG["HIST_LOW_REFRESH_DAYS"]
    except Exception:
        return True


def _calc_single_stock_levels(code, market_type, idx_closes_map, cached_hist_low, need_full_refresh, use_krx_prefix):
    """수정주가 일봉 기반 기준선(hh240 포함) + 주간 3,000봉 장기 역사적 저점 계산"""
    try:
        today_str = datetime.now(KST).strftime("%Y-%m-%d")

        if need_full_refresh:
            raw_hist, src = _fetch_daily_ohlcv(code, start_date=None, use_krx_prefix=use_krx_prefix)
            if raw_hist is None or len(raw_hist) < 130:
                return code, None, None, src
            if str(raw_hist.index[-1])[:10] == today_str:
                raw_hist = raw_hist.iloc[:-1]
            valid_all_lows = raw_hist["Low"][raw_hist["Low"] > 0]
            hist_low = float(valid_all_lows.min()) if len(valid_all_lows) > 0 else 0.0
            hist = raw_hist.tail(320)
        else:
            start_date = (datetime.now(KST) - timedelta(days=430)).strftime("%Y-%m-%d")
            hist, src = _fetch_daily_ohlcv(code, start_date=start_date, use_krx_prefix=use_krx_prefix)
            if hist is None or len(hist) < 130:
                return code, None, None, src
            if str(hist.index[-1])[:10] == today_str:
                hist = hist.iloc[:-1]
            valid_430_lows = hist["Low"][hist["Low"] > 0]
            min_430 = float(valid_430_lows.min()) if len(valid_430_lows) > 0 else float(cached_hist_low)
            hist_low = min(float(cached_hist_low), min_430)

        if len(hist) < 130:
            return code, None, None, src

        o = hist["Open"].astype(float)
        h = hist["High"].astype(float)
        l = hist["Low"].astype(float)
        c = hist["Close"].astype(float)
        v = hist["Volume"].astype(float)
        dates = [str(d)[:10] for d in hist.index]

        if "Amount" in hist.columns and hist["Amount"].notna().sum() >= 20:
            amt = pd.to_numeric(hist["Amount"], errors="coerce").fillna(((h + l + c) / 3.0) * v)
        else:
            amt = ((h + l + c) / 3.0) * v

        prev_close = float(c.iloc[-1])
        prev_high = float(h.iloc[-1])
        prev_vol = float(v.iloc[-1])
        avg_vol_20 = float(v.tail(20).mean())
        adv20 = float(amt.tail(20).mean())

        # 1) 기존 10거래일 전저점 (PREV_LOW)
        w = CONFIG["PIVOT_WINDOW_DAYS"]
        recent_150 = l.tail(150).iloc[:-3].values
        pivots_10 = []
        for i in range(w, len(recent_150) - w):
            win = recent_150[i - w : i + w + 1]
            if recent_150[i] > 0 and recent_150[i] == win.min():
                pivots_10.append(float(recent_150[i]))
        below_10 = [p for p in pivots_10 if p < prev_close * 0.99]
        prev_low = max(below_10) if below_10 else 0.0

        # 2) 신규 1번용: 6개월(120일) 내 깨진 적 없는 의미 있는 PL
        l_120 = l.tail(120).values
        h_120 = h.tail(120).values
        sweep_pl_candidates = []
        for i in range(5, len(l_120) - 10):
            win = l_120[i - 5 : i + 6]
            pl_val = float(l_120[i])
            if pl_val > 0 and pl_val == win.min():
                after_lows = l_120[i + 1 :]
                after_highs = h_120[i + 1 :]
                if len(after_lows) >= 10 and after_lows.min() >= pl_val and after_highs.max() >= pl_val * 1.08:
                    if pl_val < prev_close:
                        sweep_pl_candidates.append(pl_val)
        sweep_pl = max(sweep_pl_candidates) if sweep_pl_candidates else 0.0

        # 3) 이동평균 시리즈 및 누적합
        ma20_s = c.rolling(20).mean()
        ma60_s = c.rolling(60).mean()
        ma120_s = c.rolling(120).mean()

        sum_c_4 = float(c.tail(4).sum())
        sum_c_19 = float(c.tail(19).sum())
        sum_c_59 = float(c.tail(59).sum())
        sum_c_119 = float(c.tail(119).sum())

        ma20_5 = float(ma20_s.iloc[-5])
        ma60_10 = float(ma60_s.iloc[-10])
        ma120_20 = float(ma120_s.iloc[-20])
        c_5_ago = float(c.iloc[-5])
        c_26_ago = float(c.iloc[-26])

        # 신규 2번용: 최근 40일 중 32일 이상 Close < MA120 & 최근 20일간 R 돌파 없음
        r_s = pd.concat([ma60_s, ma120_s], axis=1).max(axis=1)
        below_120_cnt = int((c.tail(40) < ma120_s.tail(40)).sum())
        clear_r_20 = bool((c.tail(20) <= r_s.tail(20)).all())
        ma_base_ok = (below_120_cnt >= 32) and clear_r_20

        # 4) 신규 3번용: 일목균형표 구름대 상단 (오늘 구름 = raw_top.iloc[-26])
        tenkan_s = (h.rolling(9).max() + l.rolling(9).min()) / 2.0
        kijun_s = (h.rolling(26).max() + l.rolling(26).min()) / 2.0
        span_a = (tenkan_s + kijun_s) / 2.0
        span_b = (h.rolling(52).max() + l.rolling(52).min()) / 2.0
        raw_top = pd.concat([span_a, span_b], axis=1).max(axis=1)
        cloud_top_s = raw_top.shift(26)

        ichimoku_top_today = float(raw_top.iloc[-26]) if len(raw_top) >= 26 and pd.notna(raw_top.iloc[-26]) else 0.0
        ichimoku_clear_20 = bool((c.tail(20) <= cloud_top_s.tail(20)).all()) if cloud_top_s.tail(20).notna().all() else False

        hh8 = float(h.tail(8).max())
        ll8 = float(l.tail(8).min())
        hh25 = float(h.tail(25).max())
        ll25 = float(l.tail(25).min())

        # 5) 고점 및 정배열 20일선 눌림 사전 데이터
        hh19 = float(h.tail(19).max())
        hh60 = float(h.tail(60).max())
        hh120 = float(h.tail(120).max())
        hh240 = float(h.tail(240).max())
        vol_3d_avg = float(v.tail(3).mean())
        pct_chg_s = c.pct_change() * 100.0
        min_chg_4d = float(pct_chg_s.tail(4).min())
        ma20_touch_10d = bool((l.tail(10) <= ma20_s.tail(10) * 1.01).any())

        # 6) 신규 5번용: 2~10거래일 전 기준봉 B 탐색
        min_b_pct = 5.0 if market_type == "KOSPI" else 7.0
        candle_b = None
        for offset in range(2, 11):
            idx = len(c) - offset
            if idx < 20:
                continue
            b_o = float(o.iloc[idx])
            b_c = float(c.iloc[idx])
            b_h = float(h.iloc[idx])
            b_l = float(l.iloc[idx])
            b_v = float(v.iloc[idx])
            b_a = float(amt.iloc[idx])
            b_prev_c = float(c.iloc[idx - 1])
            b_avg_v20 = float(v.iloc[idx - 20 : idx].mean())

            b_chg = ((b_c - b_prev_c) / b_prev_c) * 100.0 if b_prev_c > 0 else 0.0
            b_clv = ((b_c - b_l) / (b_h - b_l)) if b_h > b_l else 0.5

            if (
                b_chg >= min_b_pct
                and b_v >= b_avg_v20 * 3.0
                and b_a >= 30_000_000_000
                and b_c > b_o
                and b_clv >= 0.6
            ):
                b_mid = (b_o + b_c) / 2.0
                subsequent_closes = c.iloc[idx + 1 :]
                if len(subsequent_closes) > 0 and (subsequent_closes >= b_mid).all():
                    candle_b = {
                        "open": b_o,
                        "close": b_c,
                        "high": b_h,
                        "mid": b_mid,
                        "vol": b_v,
                    }
                    break

        # 7) 신규 7번용: RS 라인
        rs_vals = []
        for i in range(max(0, len(dates) - 125), len(dates)):
            d_str = dates[i]
            idx_c = idx_closes_map.get(d_str)
            if idx_c and idx_c > 0:
                rs_vals.append(float(c.iloc[i]) / idx_c)
        max_rs_120 = max(rs_vals) if len(rs_vals) >= 60 else 0.0
        rs_20_ago = rs_vals[-20] if len(rs_vals) >= 20 else 0.0

        rs_new_10d = False
        if len(rs_vals) >= 120:
            for k in range(1, 11):
                cur_rs = rs_vals[-k]
                prior_max = max(rs_vals[-k - 120 : -k]) if len(rs_vals) >= k + 120 else max(rs_vals[:-k])
                if cur_rs > prior_max:
                    rs_new_10d = True
                    break

        return code, hist_low, {
            "prev_close": prev_close,
            "prev_high": prev_high,
            "avg_vol_20": avg_vol_20,
            "adv20": adv20,
            "prev_low": prev_low,
            "hist_low": hist_low,
            "sweep_pl": sweep_pl,
            "prev_vol": prev_vol,
            "sum_c_4": sum_c_4,
            "sum_c_19": sum_c_19,
            "sum_c_59": sum_c_59,
            "sum_c_119": sum_c_119,
            "ma20_5": ma20_5,
            "ma60_10": ma60_10,
            "ma120_20": ma120_20,
            "c_5_ago": c_5_ago,
            "c_26_ago": c_26_ago,
            "ma_base_ok": ma_base_ok,
            "ichimoku_top": ichimoku_top_today,
            "ichimoku_clear_20": ichimoku_clear_20,
            "hh8": hh8,
            "ll8": ll8,
            "hh25": hh25,
            "ll25": ll25,
            "hh19": hh19,
            "hh60": hh60,
            "hh120": hh120,
            "hh240": hh240,
            "vol_3d_avg": vol_3d_avg,
            "min_chg_4d": min_chg_4d,
            "ma20_touch_10d": ma20_touch_10d,
            "candle_b": candle_b,
            "max_rs_120": max_rs_120,
            "rs_new_10d": rs_new_10d,
            "rs_20_ago": rs_20_ago,
        }, src
    except Exception:
        return code, None, None, "FAIL"


def ensure_daily_levels(state, df):
    idx_base = state.get("index_base", {})
    ks_map = idx_base.get("KS11", {}).get("closes_map", {})
    kq_map = idx_base.get("KQ11", {}).get("closes_map", {})
    hist_lows_cache = state.setdefault("hist_lows", {})
    hist_low_dates = state.setdefault("hist_low_dates", {})
    today_str = state["date"]

    missing = []
    for _, row in df.iterrows():
        code = str(row["Code"]).zfill(6)
        if code not in state["levels"]:
            c_low = hist_lows_cache.get(code)
            need_full = _need_full_hist_refresh(today_str, hist_low_dates.get(code), c_low)
            missing.append((code, row["MarketType"], c_low, need_full))

    if not missing:
        return

    full_cnt = sum(1 for _, _, _, nf in missing if nf)
    workers = 4 if full_cnt >= 30 else 8
    print(f"금일 기준선 계산 중 ({len(missing)}종목, 전체재조회 {full_cnt}종목, workers={workers})...")
    use_krx_prefix = _verify_krx_adjusted()
    krx_cnt, fb_cnt, fail_cnt = 0, 0, 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _calc_single_stock_levels,
                code,
                m_type,
                ks_map if m_type == "KOSPI" else kq_map,
                c_low,
                need_full,
                use_krx_prefix,
            ): need_full
            for code, m_type, c_low, need_full in missing
        }
        for fut in as_completed(futures):
            was_full = futures[fut]
            code, h_low, info, src = fut.result()
            if src == "KRX":
                krx_cnt += 1
            elif src == "FALLBACK":
                fb_cnt += 1
            else:
                fail_cnt += 1

            if h_low and h_low > 0:
                hist_lows_cache[code] = h_low
                if was_full or code not in hist_low_dates:
                    hist_low_dates[code] = today_str
            if info:
                state["levels"][code] = info

    prev_hl = state.get("health") or {}
    top200_cnt = int(df["InTop200"].sum()) if "InTop200" in df.columns else len(df)
    state["health"] = {
        "target_cnt": top200_cnt,
        "levels_ok": len(state["levels"]),
        "krx_cnt": prev_hl.get("krx_cnt", 0) + krx_cnt,
        "fb_cnt": prev_hl.get("fb_cnt", 0) + fb_cnt,
        "fail_cnt": prev_hl.get("fail_cnt", 0) + fail_cnt,
    }


def get_market_elapsed_ratio():
    now = datetime.now(KST)
    hhmm = now.hour * 100 + now.minute
    if hhmm < 920 or hhmm >= 1550:
        return 1.0
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    elapsed_min = (now - market_open).total_seconds() / 60.0
    data_elapsed_min = elapsed_min - 20.0
    effective_min = min(390.0, max(15.0, data_elapsed_min))
    return effective_min / 390.0


def evaluate_follow_up_setups(webhook_url, state, live_snapshots, etf_valid, ks_chg, kq_chg, hhmm):
    """
    #4·#6·#7 준비 신호의 후속 진입 트리거 발동 및 손절 이탈 감시 (최대 5거래일)
    - #4·#7 손절 이탈은 종가 기준 전략에 맞춰 hhmm >= 1520 이후에만 판정
    """
    setups = state.get("active_setups", {})
    if not setups:
        return 0

    today = state["date"]
    sent = 0
    to_remove = []

    for key, st in list(setups.items()):
        created = st["created_date"]
        if created == today:
            continue

        diff = get_trading_day_diff(state, created, today)
        if diff > 5:
            to_remove.append(key)
            continue

        code = str(st["code"]).zfill(6)
        snap = live_snapshots.get(code)
        lv = state["levels"].get(code)
        if not snap or not lv:
            continue

        c, o, h, l = snap["c"], snap["o"], snap["h"], snap["l"]
        ma20 = snap["ma20"]
        rvol = snap["rvol"]
        sig_id = st["sig_id"]
        sig_label = st["sig_label"]
        name = st["name"]
        market_type = st["market"]

        invalid_reason = None
        if sig_id == "NEW_4" and hhmm >= 1520 and c < ma20 * 0.97:
            invalid_reason = f"종가 무렵 현재가({int(c):,}원)가 20일선 3% 손절선({int(ma20*0.97):,}원)을 이탈했습니다."
        elif sig_id == "NEW_6" and l < st["setup_low"]:
            invalid_reason = f"당일 저가({int(l):,}원)가 급락일 저가({int(st['setup_low']):,}원)를 하향 이탈했습니다."
        elif sig_id == "NEW_7" and hhmm >= 1520 and c < ma20:
            invalid_reason = f"종가 무렵 현재가({int(c):,}원)가 20일 이동평균선({int(ma20):,}원)을 이탈했습니다."

        if invalid_reason:
            ok = send_individual_card(
                webhook_url, market_type,
                title=f"❌ [후속 시나리오 이탈] {name} ({code}) · {sig_label}",
                meaning=f"{created} 포착된 준비 시그널의 지지 기준선이 무너졌습니다.",
                action_guide=f"• **이탈 사유:** {invalid_reason}\n• **대응:** 관심종목에서 제외하고 신규 진입 보류",
                color=9807270,
                fields=[
                    {"name": "현재가 / 등락률", "value": f"{int(c):,}원 (`{snap['chg']:+.2f}%`)", "inline": True},
                    {"name": "경과 거래일", "value": f"D+{diff}일", "inline": True},
                ],
            )
            if ok:
                to_remove.append(key)
                sent += 1
            continue

        trigger_reason = None
        if sig_id == "NEW_4":
            if c > ma20 and c > o and c > lv["prev_close"]:
                trigger_reason = f"20일선({int(ma20):,}원) 위 양봉 전환 및 전일 종가({int(lv['prev_close']):,}원) 돌파 확인"
            elif h > lv["prev_high"] and c > ma20:
                trigger_reason = f"20일선({int(ma20):,}원) 지지 속 직전 거래일 고가({int(lv['prev_high']):,}원) 돌파 확인"
        elif sig_id == "NEW_6":
            idx_chg = (ks_chg if market_type == "KOSPI" else kq_chg) if etf_valid else None
            if idx_chg is not None and idx_chg >= 0.3 and h > st["setup_high"] and c > o:
                trigger_reason = f"시장 지수 반등({idx_chg:+.2f}%)과 함께 급락일 고가({int(st['setup_high']):,}원) 돌파"
        elif sig_id == "NEW_7":
            if h > st["setup_hh120"] and rvol >= 150.0:
                trigger_reason = f"거래량({rvol:,.0f}%)을 동반해 120일 신고가({int(st['setup_hh120']):,}원) 돌파"

        if trigger_reason:
            ok = send_individual_card(
                webhook_url, market_type,
                title=f"🔔 [후속 진입 트리거 발동] {name} ({code}) · {sig_label}",
                meaning=f"{created} 포착된 주도주 후보가 실제 매수 진입 조건을 충족했습니다.",
                action_guide=(
                    f"• **발동 조건:** {trigger_reason}\n"
                    f"• **손절 / 목표:** 손절 `{int(st['stop_price']):,}원` / 1차 목표 `{int(st['target_price']):,}원`"
                ),
                color=3066993,
                fields=[
                    {"name": "현재가 / 등락률", "value": f"**{int(c):,}원** (`{snap['chg']:+.2f}%`)", "inline": True},
                    {"name": "경과 거래일", "value": f"D+{diff}일", "inline": True},
                ],
            )
            if ok:
                to_remove.append(key)
                sent += 1

    for k in to_remove:
        setups.pop(k, None)
    return sent


def main():
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook_url:
        raise ValueError("DISCORD_WEBHOOK_URL 시크릿이 설정되지 않았습니다.")

    state = load_state()
    now = datetime.now(KST)
    hhmm = now.hour * 100 + now.minute

    # 1) 09:20 이전 대기 및 평일 공휴일 가드
    if not check_trading_day_and_index_history(state, hhmm):
        return

    is_after_10am_data = (hhmm >= 1020) or (hhmm < 900)
    sent_count = 0
    batch_6_candidates = []
    batch_7_candidates = []
    live_snapshots = {}

    try:
        df, aligned_etf, raw_etf_ok = fetch_top_universe_and_etf(state, hhmm)
        ensure_daily_levels(state, df)

        idx_hist_ok = state.get("index_base", {}).get("valid", False)
        etf_valid = idx_hist_ok and ("069500" in aligned_etf) and ("229200" in aligned_etf)
        ks_chg = aligned_etf.get("069500")
        kq_chg = aligned_etf.get("229200")

        idx_base = state.get("index_base", {})
        ks_prev = idx_base.get("KS11", {}).get("prev_close", 0.0)
        kq_prev = idx_base.get("KQ11", {}).get("prev_close", 0.0)
        ks_live = ks_prev * (1.0 + ks_chg / 100.0) if (etf_valid and ks_prev > 0) else 0.0
        kq_live = kq_prev * (1.0 + kq_chg / 100.0) if (etf_valid and kq_prev > 0) else 0.0

        elapsed_ratio = get_market_elapsed_ratio()
        buffer_mult = 1.0 + (CONFIG["LOW_TOUCH_BUFFER_PCT"] / 100.0)

        for _, row in df.iterrows():
            code = str(row["Code"]).zfill(6)
            name = str(row["Name"])
            market_type = row["MarketType"]
            in_top200 = bool(row["InTop200"]) if "InTop200" in row else True

            c = float(row["Close"]) if pd.notna(row["Close"]) else 0.0
            if c <= 0:
                continue

            lv = state["levels"].get(code)
            if not lv:
                continue

            o = float(row["Open"]) if ("Open" in row and pd.notna(row["Open"]) and row["Open"] > 0) else c
            h_raw = float(row["High"]) if ("High" in row and pd.notna(row["High"]) and row["High"] > 0) else c
            l_raw = float(row["Low"]) if ("Low" in row and pd.notna(row["Low"]) and row["Low"] > 0) else c
            h = max(h_raw, c, o)
            l = min(l_raw, c, o)

            if lv["prev_close"] > 0:
                chg = ((c - lv["prev_close"]) / lv["prev_close"]) * 100.0
            else:
                chg = float(row["ChagesRatio"]) if ("ChagesRatio" in row and pd.notna(row["ChagesRatio"])) else 0.0

            v = float(row["Volume"]) if ("Volume" in row and pd.notna(row["Volume"])) else 0.0
            raw_amt = float(row["Amount"]) if ("Amount" in row and pd.notna(row["Amount"])) else 0.0
            amt = raw_amt if raw_amt > 0 else ((h + l + c) / 3.0) * v
            amount_uk = int(amt / 100_000_000)
            clv = ((c - l) / (h - l)) if h > l else 0.5

            avg_vol_20 = lv["avg_vol_20"]
            adv20 = lv["adv20"]
            expected_vol = avg_vol_20 * elapsed_ratio
            rvol = (v / expected_vol * 100.0) if expected_vol > 0 else 0.0

            ma5 = (lv["sum_c_4"] + c) / 5.0
            ma20 = (lv["sum_c_19"] + c) / 20.0
            ma60 = (lv["sum_c_59"] + c) / 60.0
            ma120 = (lv["sum_c_119"] + c) / 120.0
            hh20 = max(lv["hh19"], h)
            chg_5d = ((c / lv["c_5_ago"]) - 1.0) * 100.0 if lv["c_5_ago"] > 0 else 0.0

            idx_chg = (ks_chg if market_type == "KOSPI" else kq_chg) if etf_valid else None
            idx_live = (ks_live if market_type == "KOSPI" else kq_live) if etf_valid else 0.0
            rs_today = (c / idx_live) if idx_live > 0 else 0.0

            live_snapshots[code] = {
                "o": o, "h": h, "l": l, "c": c, "v": v, "amt": amt,
                "chg": chg, "chg_5d": chg_5d, "clv": clv, "rvol": rvol,
                "ma5": ma5, "ma20": ma20, "ma60": ma60, "ma120": ma120,
                "hh20": hh20, "rs_today": rs_today,
            }

            if not in_top200:
                continue

            # ==============================================================
            # [A] 기존 4종 기본 알림
            # ==============================================================
            hist_low = lv["hist_low"]
            prev_low = lv["prev_low"]

            if hist_low > 0 and l <= hist_low * buffer_mult:
                if can_fire_once(state, code, "HIST_LOW"):
                    status = "역사적 신저가 이탈!" if l < hist_low else "역사적 저점 근접!"
                    ok = send_individual_card(
                        webhook_url, market_type,
                        title=f"🌋 [역사적 저점] {name} ({code}) · {status}",
                        meaning="상장 이후 장기 최저가(최대 12년 기준) 영역에 도달했습니다. 장기 하락 추세의 마지막 바닥 지지 여부를 시험하는 구간입니다.",
                        action_guide=(
                            "1) 유상증자·실적 쇼크 등 기업 펀더멘털 훼손 공시 여부 먼저 확인\n"
                            "2) 떨어지는 칼날 즉시 매수 금지 — 거래량 동반 양봉 전환 확인 전까지 관망\n"
                            f"3) 종가 기준 장기 최저가(**{int(hist_low):,}원**) 이탈 시 추가 하방 열림 주의"
                        ),
                        color=9323693,
                        fields=[
                            {"name": "현재가 (장중저가)", "value": f"{int(c):,}원 ({int(l):,}원)", "inline": True},
                            {"name": "장기 역사적 저점", "value": f"**{int(hist_low):,}원**", "inline": True},
                            {"name": "등락률 / 거래대금", "value": f"{chg:+.2f}% / {amount_uk:,}억", "inline": True},
                        ],
                    )
                    if ok:
                        commit_once(state, code, "HIST_LOW")
                        sent_count += 1

            elif (
                prev_low > 0
                and l <= prev_low * buffer_mult
                and chg <= CONFIG["MIN_DROP_FOR_PREV_LOW"]
                and amount_uk >= CONFIG["MIN_AMOUNT_FOR_PREV_LOW_UK"]
            ):
                if can_fire_once(state, code, "PREV_LOW"):
                    status = "지지선 하향 이탈" if l < prev_low else "지지선 터치 근접"
                    ok = send_individual_card(
                        webhook_url, market_type,
                        title=f"⚓ [10일 전저점] {name} ({code}) · {status}",
                        meaning="최근 10거래일 기준 의미 있는 스윙 바닥(직전 지지선)에 닿았습니다. 기술적 반등과 추세 이탈이 갈리는 자리입니다.",
                        action_guide=(
                            f"1) 지지선(**{int(prev_low):,}원**) 부근에서 하락이 멈추고 아래꼬리·양봉이 나오는지 관찰\n"
                            "2) 반등 확인 시 단기 반등 노림 가능 (종가 기준 전저점 이탈 시 즉시 손절)\n"
                            "3) 거래량을 동반한 장대음봉으로 깰 경우 투매 확산 주의"
                        ),
                        color=15965202,
                        fields=[
                            {"name": "현재가 (장중저가)", "value": f"{int(c):,}원 ({int(l):,}원)", "inline": True},
                            {"name": "직전 전저점", "value": f"**{int(prev_low):,}원**", "inline": True},
                            {"name": "등락률 / 거래대금", "value": f"{chg:+.2f}% / {amount_uk:,}억", "inline": True},
                        ],
                    )
                    if ok:
                        commit_once(state, code, "PREV_LOW")
                        sent_count += 1

            if avg_vol_20 > 0 and amount_uk >= CONFIG["MIN_TRADE_AMOUNT_UK"] and rvol >= CONFIG["VOL_SURGE_PCT"]:
                if can_fire_once(state, code, "VOL_SURGE"):
                    if chg >= 0:
                        title = f"🚀 [매수 수급폭발] {name} ({code})"
                        meaning = f"평상시 동시간대 대비 **{rvol:,.0f}%**의 강한 매수 수급이 유입되며 주가를 끌어올리는 신호입니다."
                        guide = (
                            "1) 뉴스·공시·테마 등 수급이 쏠린 핵심 재료 확인\n"
                            "2) 급등 추격 매수보다 당일 양봉 중심값 지지 여부 확인 후 눌림목 대기\n"
                            "3) 장 후반 윗꼬리가 길어지며 상승폭을 반납하면 단기 차익실현 경계"
                        )
                        color = 3066993
                    else:
                        title = f"🌊 [투매 거래폭발] {name} ({code})"
                        meaning = f"평상시 동시간대 대비 **{rvol:,.0f}%**의 대량 매도 물량이 쏟아지며 투매가 발생하는 경보입니다."
                        guide = (
                            "1) 악재 공시(유증·실적 부진·외국인/기관 대량 이탈) 즉시 체크\n"
                            "2) 투매 진행 중 섣부른 물타기 금지 — 거래량 감소 및 바닥 다지기 확인\n"
                            "3) 보유 종목이라면 본인의 손절 기준선 이탈 여부 점검"
                        )
                        color = 3447003
                    ok = send_individual_card(
                        webhook_url, market_type,
                        title=title,
                        meaning=meaning,
                        action_guide=guide,
                        color=color,
                        fields=[
                            {"name": "현재가 (등락률)", "value": f"{int(c):,}원 (**{chg:+.2f}%**)", "inline": True},
                            {"name": "거래량 폭발률", "value": f"**{rvol:,.0f}%**", "inline": True},
                            {"name": "누적 거래대금", "value": f"{amount_uk:,}억 원", "inline": True},
                        ],
                    )
                    if ok:
                        commit_once(state, code, "VOL_SURGE")
                        sent_count += 1

            passed_drops = [t for t in sorted(CONFIG["THRESHOLDS"]) if chg <= t]
            if passed_drops:
                deepest = passed_drops[0]
                if can_fire_once(state, code, f"DROP_{deepest}"):
                    if deepest <= -15.0:
                        title, color = f"🧨 [-15% 초급락 경보] {name} ({code})", 10038562
                    elif deepest <= -10.0:
                        title, color = f"🔥 [-10% 폭락 경보] {name} ({code})", 15158332
                    else:
                        title, color = f"📉 [-5% 급락 알림] {name} ({code})", 15105570
                    idx_txt = f"{idx_chg:+.2f}%" if idx_chg is not None else "지연동기대기"
                    ok = send_individual_card(
                        webhook_url, market_type,
                        title=title,
                        meaning=f"전일 종가 대비 **{deepest:.0f}%** 이상 급락하며 단기 과매도 구간에 진입했습니다.",
                        action_guide=(
                            f"1) 소속 지수({idx_txt}) 동반 급락인지, 개별 기업 악재인지 먼저 구분\n"
                            "2) 과매도 반등을 노릴 경우 장 막판 투매 진정 확인 후 소액 분할만 고려\n"
                            "3) 반등 없이 당일 저가 부근으로 마감 시 익일 추가 갭하락 가능성 대비"
                        ),
                        color=color,
                        fields=[
                            {"name": "현재가", "value": f"{int(c):,}원", "inline": True},
                            {"name": "등락률", "value": f"**{chg:.2f}%**", "inline": True},
                            {"name": "거래대금", "value": f"{amount_uk:,}억 원", "inline": True},
                        ],
                    )
                    if ok:
                        for t in passed_drops:
                            commit_once(state, code, f"DROP_{t}")
                        sent_count += 1

            # ==============================================================
            # [B] 신규 8종 공통 노이즈 필터: 20일 평균 거래대금(ADV20) >= 100억
            # ==============================================================
            if adv20 < CONFIG["MIN_ADV20_KRW"]:
                continue

            # --- [신규 1] 전저점 스윕 리클레임 ---
            pl = lv["sweep_pl"]
            if is_after_10am_data and pl > 0:
                target_1 = ma20 if ma20 > c else hh20
                rr_1 = ((target_1 - c) / (c - l)) if c > l else 0.0
                if (
                    pl * 0.97 <= l < pl
                    and c >= pl * 1.005
                    and c > o
                    and clv >= 0.6
                    and rvol >= 150.0
                    and rr_1 >= 2.0
                ):
                    if can_fire_cooldown(state, code, "NEW_1_SWEEP"):
                        ok = send_individual_card(
                            webhook_url, market_type,
                            title=f"🪝 [전저점 스윕 리클레임] {name} ({code})",
                            meaning=f"주요 전저점(**{int(pl):,}원**)을 장중 일시 이탈해 손절 물량을 털어낸 뒤, 거래량을 실어 지지선 위로 강하게 말아 올린 반등 시그널입니다.",
                            action_guide=(
                                f"• **진입:** 알림 시점 계획 물량의 **1/2 진입** → 종가가 전저점(**{int(pl):,}원**) 위 마감 시 나머지 진입\n"
                                f"• **손절 / 목표:** 당일 저가(**{int(l):,}원**) 이탈 시 손절 / 1차 목표 **{int(target_1):,}원** (손익비 `1:{rr_1:.1f}`)\n"
                                "• **무효 조건:** 장 마감 전 전저점 아래로 다시 밀리면 시나리오 폐기"
                            ),
                            color=2067276,
                            fields=[
                                {"name": "현재가 / 전저점", "value": f"**{int(c):,}원** / {int(pl):,}원", "inline": True},
                                {"name": "손절 / 1차목표", "value": f"{int(l):,}원 / {int(target_1):,}원", "inline": True},
                                {"name": "RVOL / CLV", "value": f"{rvol:,.0f}% / {clv:.2f}", "inline": True},
                            ],
                        )
                        if ok:
                            commit_cooldown(state, code, "NEW_1_SWEEP")
                            record_today_signal(
                                state, code, name, market_type, "NEW_1", "1.스윕리클레임",
                                entry_price=c, stop_price=l, target_price=target_1,
                                buy_zone=f"{int(pl):,}~{int(c):,}원"
                            )
                            sent_count += 1

            # --- [신규 2 & 3] 60·120일선 첫 돌파 & 일목 구름대 상단 첫 돌파 ---
            r_line = max(ma60, ma120)
            cond_2 = (
                is_after_10am_data
                and lv["ma_base_ok"]
                and c > r_line * 1.01
                and chg >= 3.0
                and clv >= 0.7
                and rvol >= 200.0
                and amt >= 10_000_000_000
                and lv["ma120_20"] > 0
                and (ma120 / lv["ma120_20"]) >= 0.97
            )

            top_cloud = lv["ichimoku_top"]
            t_today = (max(lv["hh8"], h) + min(lv["ll8"], l)) / 2.0
            k_today = (max(lv["hh25"], h) + min(lv["ll25"], l)) / 2.0
            cond_3 = (
                is_after_10am_data
                and top_cloud > 0
                and lv["ichimoku_clear_20"]
                and c > top_cloud * 1.01
                and t_today >= k_today
                and c > lv["c_26_ago"]
                and chg >= 2.5
                and clv >= 0.6
                and rvol >= 180.0
                and amt >= 10_000_000_000
            )

            raw_target_23 = lv["hh120"] if lv["hh120"] > c * 1.05 else max(lv["hh240"], c * 1.15)
            target_23 = max(raw_target_23, c * 1.12)

            if cond_2 and cond_3:
                ok2 = can_fire_cooldown(state, code, "NEW_2_MABREAK")
                ok3 = can_fire_cooldown(state, code, "NEW_3_CLOUD")
                if ok2 or ok3:
                    stop_23 = min(r_line, top_cloud) * 0.99
                    sent_ok = send_individual_card(
                        webhook_url, market_type,
                        title=f"⚔️ [이중 돌파: 이평선+구름대] {name} ({code})",
                        meaning="장기 저항선(60·120일선)과 일목균형표 구름대 상단을 거래량을 동반해 동시에 돌파한 강력한 추세 전환 시그널입니다.",
                        action_guide=(
                            "• **진입:** 14:50 이후에도 돌파선 위 유지 시 **1/3 진입**, 나머지는 1~3일 내 돌파선 리테스트 때 분할\n"
                            f"• **손절:** 종가 기준 돌파선(**{int(stop_23):,}원**) 재이탈 또는 돌파일 저가 이탈 시 손절\n"
                            f"• **목표:** 1차 목표(**{int(target_23):,}원**) 절반 익절 후 나머지는 20일선 이탈까지 보유"
                        ),
                        color=15844367,
                        fields=[
                            {"name": "현재가 (등락률)", "value": f"**{int(c):,}원** (`{chg:+.2f}%`)", "inline": True},
                            {"name": "돌파 기준 (이평/구름)", "value": f"{int(r_line):,} / {int(top_cloud):,}원", "inline": True},
                            {"name": "손절 / 1차목표", "value": f"{int(stop_23):,} / {int(target_23):,}원", "inline": True},
                        ],
                    )
                    if sent_ok:
                        if ok2:
                            commit_cooldown(state, code, "NEW_2_MABREAK")
                            record_today_signal(
                                state, code, name, market_type, "NEW_2", "2.이평돌파",
                                entry_price=c, stop_price=r_line * 0.99, target_price=target_23,
                                buy_zone=f"{int(r_line):,}~{int(c):,}원"
                            )
                        if ok3:
                            commit_cooldown(state, code, "NEW_3_CLOUD")
                            record_today_signal(
                                state, code, name, market_type, "NEW_3", "3.구름돌파",
                                entry_price=c, stop_price=top_cloud * 0.99, target_price=target_23,
                                buy_zone=f"{int(top_cloud):,}~{int(c):,}원"
                            )
                        sent_count += 1
            elif cond_2:
                if can_fire_cooldown(state, code, "NEW_2_MABREAK"):
                    sent_ok = send_individual_card(
                        webhook_url, market_type,
                        title=f"📈 [60·120일선 첫 돌파] {name} ({code})",
                        meaning=f"오랜 기간 저항으로 작용하던 60·120일선(**{int(r_line):,}원**)을 거래량을 실어 처음 뚫어낸 추세 전환 초입 시그널입니다.",
                        action_guide=(
                            f"• **진입:** 14:50 이후에도 **{int(r_line):,}원** 위 유지 시 **1/3 진입**, 나머지는 1~3일 내 리테스트 때 분할\n"
                            f"• **손절:** 종가 기준 돌파선(**{int(r_line*0.99):,}원**) 재이탈 또는 돌파일 저가 이탈 시 손절\n"
                            f"• **목표:** 1차 목표(**{int(target_23):,}원**) 절반 익절 후 20일선 이탈까지 추세 추종"
                        ),
                        color=3066993,
                        fields=[
                            {"name": "현재가 (등락률)", "value": f"**{int(c):,}원** (`{chg:+.2f}%`)", "inline": True},
                            {"name": "RVOL / 거래대금", "value": f"{rvol:,.0f}% / {amount_uk:,}억", "inline": True},
                            {"name": "손절 / 1차목표", "value": f"{int(r_line*0.99):,} / {int(target_23):,}원", "inline": True},
                        ],
                    )
                    if sent_ok:
                        commit_cooldown(state, code, "NEW_2_MABREAK")
                        record_today_signal(
                            state, code, name, market_type, "NEW_2", "2.이평돌파",
                            entry_price=c, stop_price=r_line * 0.99, target_price=target_23,
                            buy_zone=f"{int(r_line):,}~{int(c):,}원"
                        )
                        sent_count += 1
            elif cond_3:
                if can_fire_cooldown(state, code, "NEW_3_CLOUD"):
                    sent_ok = send_individual_card(
                        webhook_url, market_type,
                        title=f"⛅ [일목 구름대 첫 돌파] {name} ({code})",
                        meaning=f"일목균형표 구름대 상단(**{int(top_cloud):,}원**)을 첫 돌파하고 전환선≥기준선·후행스팬 주가 위 조건까지 충족한 상승 진입 신호입니다.",
                        action_guide=(
                            f"• **진입:** 종가가 구름대 상단(**{int(top_cloud):,}원**) 위로 확정될 때 종가 매수 또는 익일 리테스트 진입\n"
                            f"• **손절:** 종가 기준 구름대 안으로 재진입(**{int(top_cloud*0.99):,}원** 하회) 시 손절\n"
                            f"• **목표 / 청산:** 1차 목표(**{int(target_23):,}원**) / 일목 기준선(**{int(k_today):,}원**) 이탈까지 보유"
                        ),
                        color=1752220,
                        fields=[
                            {"name": "현재가 (등락률)", "value": f"**{int(c):,}원** (`{chg:+.2f}%`)", "inline": True},
                            {"name": "RVOL / 거래대금", "value": f"{rvol:,.0f}% / {amount_uk:,}억", "inline": True},
                            {"name": "손절 / 1차목표", "value": f"{int(top_cloud*0.99):,} / {int(target_23):,}원", "inline": True},
                        ],
                    )
                    if sent_ok:
                        commit_cooldown(state, code, "NEW_3_CLOUD")
                        record_today_signal(
                            state, code, name, market_type, "NEW_3", "3.구름돌파",
                            entry_price=c, stop_price=top_cloud * 0.99, target_price=target_23,
                            buy_zone=f"{int(top_cloud):,}~{int(c):,}원"
                        )
                        sent_count += 1

            # --- [신규 4] 정배열 20일선 첫 눌림 ---
            dd_from_hh20 = ((c / hh20) - 1.0) * 100.0 if hh20 > 0 else 0.0
            if (
                ma20 > ma60 > ma120
                and ma20 > lv["ma20_5"]
                and ma60 > lv["ma60_10"]
                and abs(hh20 - lv["hh60"]) < 1.0
                and -12.0 <= dd_from_hh20 <= -4.0
                and l <= ma20 * 1.01
                and c >= ma20 * 0.985
                and lv["vol_3d_avg"] <= avg_vol_20 * 0.70
                and rvol <= 100.0
                and lv["min_chg_4d"] > -5.0
                and chg > -5.0
                and not lv["ma20_touch_10d"]
            ):
                if can_fire_cooldown(state, code, "NEW_4_MA20PULL"):
                    sent_ok = send_individual_card(
                        webhook_url, market_type,
                        title=f"🎯 [정배열 20일선 첫 눌림] {name} ({code})",
                        meaning=f"우상향 정배열 종목이 거래량을 줄이며 20일 생명선(**{int(ma20):,}원**)에 처음 닿은 건강한 조정 구간입니다.",
                        action_guide=(
                            "• **진입:** 지금은 **준비 신호** (5거래일간 자동 추적됨) — 20일선 위 양봉 전환 또는 전일 고가 돌파 시 트리거 알림 발송\n"
                            f"• **손절:** 종가 기준 20일선 3% 이탈(**{int(ma20*0.97):,}원**) 시 손절\n"
                            f"• **목표:** 직전 고점(**{int(hh20):,}원**)에서 절반 익절 후 돌파 시 나머지 보유"
                        ),
                        color=5763719,
                        fields=[
                            {"name": "현재가 / 20일선", "value": f"**{int(c):,}원** / {int(ma20):,}원", "inline": True},
                            {"name": "고점대비 / RVOL", "value": f"{dd_from_hh20:.1f}% / {rvol:,.0f}%", "inline": True},
                            {"name": "손절 / 전고점목표", "value": f"{int(ma20*0.97):,} / {int(hh20):,}원", "inline": True},
                        ],
                    )
                    if sent_ok:
                        commit_cooldown(state, code, "NEW_4_MA20PULL")
                        record_today_signal(
                            state, code, name, market_type, "NEW_4", "4.20일선눌림",
                            entry_price=c, stop_price=ma20 * 0.97, target_price=hh20,
                            buy_zone=f"{int(ma20*0.99):,}~{int(ma20*1.01):,}원"
                        )
                        register_active_setup(
                            state, code, name, market_type, "NEW_4", "4.20일선눌림",
                            setup_high=h, setup_low=l, setup_hh120=lv["hh120"],
                            stop_price=ma20 * 0.97, target_price=hh20
                        )
                        sent_count += 1

            # --- [신규 5] 기준봉 중심값 눌림 ---
            cb = lv.get("candle_b")
            if cb:
                mid = cb["mid"]
                if (
                    l <= mid * 1.02
                    and c >= mid * 0.99
                    and lv["prev_vol"] <= cb["vol"] * 0.40
                    and rvol <= 100.0
                    and c > ma60
                ):
                    if can_fire_cooldown(state, code, "NEW_5_MIDPULL"):
                        sent_ok = send_individual_card(
                            webhook_url, market_type,
                            title=f"🕯️ [기준봉 중심값 눌림] {name} ({code})",
                            meaning=f"최근 300억 이상 대량 거래를 동반한 장대양봉(기준봉)의 중심값(**{int(mid):,}원**) 지지선까지 거래량이 마르며 눌린 자리입니다.",
                            action_guide=(
                                f"• **진입:** 중심값 부근(**{int(mid):,} ~ {int(mid*1.02):,}원**)에서 **2회 분할 매수**\n"
                                f"• **손절:** 종가 기준 기준봉 시가(**{int(cb['open']):,}원**) 이탈 시 즉시 손절\n"
                                f"• **목표:** 기준봉 고가(**{int(cb['high']):,}원**) 재돌파 시 절반 익절, 나머지는 5일선 이탈까지 보유"
                            ),
                            color=10181046,
                            fields=[
                                {"name": "현재가 / 중심값", "value": f"**{int(c):,}원** / {int(mid):,}원", "inline": True},
                                {"name": "기준봉 시가(손절)", "value": f"{int(cb['open']):,}원", "inline": True},
                                {"name": "기준봉 고가(목표)", "value": f"{int(cb['high']):,}원", "inline": True},
                            ],
                        )
                        if sent_ok:
                            commit_cooldown(state, code, "NEW_5_MIDPULL")
                            record_today_signal(
                                state, code, name, market_type, "NEW_5", "5.기준봉중심눌림",
                                entry_price=c, stop_price=cb["open"], target_price=cb["high"],
                                buy_zone=f"{int(mid):,}~{int(mid*1.02):,}원"
                            )
                            sent_count += 1

            # --- [신규 6 & 7] 지수 ETF 시차 동기화 완료 시 후보 수집 ---
            if etf_valid and idx_chg is not None:
                last_cd_6 = state["cooldowns"].get(f"{code}:NEW_6_DEFIANT")
                cd_6_blocked = (
                    last_cd_6 is not None
                    and last_cd_6 != state["date"]
                    and get_trading_day_diff(state, last_cd_6, state["date"]) < CONFIG["COOLDOWN_TRADING_DAYS"]
                )
                if (
                    not cd_6_blocked
                    and idx_chg <= -1.5
                    and 2.0 <= chg < 15.0
                    and c > o
                    and clv >= 0.6
                    and rvol >= 130.0
                    and amt >= 15_000_000_000
                    and c > ma60
                ):
                    excess_ret = chg - idx_chg
                    batch_6_candidates.append({
                        "code": code, "name": name, "market": market_type,
                        "c": c, "l": l, "h": h, "hh120": lv["hh120"],
                        "chg": chg, "idx_chg": idx_chg,
                        "excess": excess_ret, "rvol": rvol, "amount_uk": amount_uk,
                    })

                if (
                    rs_today > 0
                    and lv["max_rs_120"] > 0
                    and rs_today > lv["max_rs_120"]
                    and not lv["rs_new_10d"]
                    and (0.90 * lv["hh120"]) <= c < lv["hh120"]
                    and c > ma60
                    and ma60 >= lv["ma60_10"]
                ):
                    rs_20_growth = ((rs_today / lv["rs_20_ago"]) - 1.0) * 100.0 if lv["rs_20_ago"] > 0 else 0.0
                    batch_7_candidates.append({
                        "code": code, "name": name, "market": market_type,
                        "c": c, "l": l, "h": h, "ma20": ma20, "hh120": lv["hh120"],
                        "chg": chg, "rs_growth": rs_20_growth, "amount_uk": amount_uk,
                    })

        # ==================================================================
        # [C] 후속 트리거 평가 (#4·#6·#7 5거래일 감시) & 성과 CSV 업데이트
        # ==================================================================
        sent_count += evaluate_follow_up_setups(webhook_url, state, live_snapshots, etf_valid, ks_chg, kq_chg, hhmm)
        update_signal_csv_performance(state, live_snapshots, hhmm)

        # ==================================================================
        # [D] 정시 배치 알림 발송 (#6 11:20/14:50, #7 15:20, #8 15:55~16:15)
        # ==================================================================
        slot_6 = None
        if 1120 <= hhmm <= 1140:
            slot_6 = "BATCH_6_1100"
        elif 1450 <= hhmm <= 1510:
            slot_6 = "BATCH_6_1430"

        if etf_valid and slot_6 and batch_6_candidates and can_fire_once(state, "MARKET", slot_6):
            top5_6 = sorted(batch_6_candidates, key=lambda x: x["excess"], reverse=True)[:5]
            lines = []
            for item in top5_6:
                m_badge = "🔵" if item["market"] == "KOSPI" else "🟠"
                lines.append(
                    f"{m_badge} **{item['name']} ({item['code']})** | 현재가 `{int(item['c']):,}원` (`{item['chg']:+.2f}%`)\n"
                    f"└ 지수대비 초과수익 **`+{item['excess']:.2f}%p`** | 급락일 고가 `{int(item['h']):,}원` / 저가 `{int(item['l']):,}원`"
                )
            sent_ok = send_discord_embed(
                webhook_url,
                title=f"🛡️ [지수 급락일 역행 강세주 TOP {len(top5_6)}] ({now.strftime('%H:%M')} 기준)",
                description=(
                    f"📖 **알림 의미:** 코스피200(`{ks_chg:+.2f}%`)·코스닥150(`{kq_chg:+.2f}%`) 급락 속 수급이 쏠리는 주도주 후보입니다.\n\n"
                    + "\n\n".join(lines)
                ),
                color=15844367,
                fields=[{
                    "name": "🧭 지금 해야 할 행동 / 체크포인트",
                    "value": (
                        "1) **당일 추격 매수 금지** (5거래일간 자동 추적됨) — 익일 이후 지수 양봉 전환 + 급락일 고가 돌파 시 트리거 발송\n"
                        "2) **손절 기준:** 급락일 당일 저가 이탈 시 시나리오 자동 폐기"
                    ),
                    "inline": False,
                }],
            )
            if sent_ok:
                commit_once(state, "MARKET", slot_6)
                for item in top5_6:
                    commit_cooldown(state, item["code"], "NEW_6_DEFIANT")
                    record_today_signal(
                        state, item["code"], item["name"], item["market"], "NEW_6", "6.지수역행강세",
                        entry_price=item["c"], stop_price=item["l"], target_price=item["h"] * 1.08,
                        buy_zone=f"{int(item['h']):,}원 돌파 시"
                    )
                    register_active_setup(
                        state, item["code"], item["name"], item["market"], "NEW_6", "6.지수역행강세",
                        setup_high=item["h"], setup_low=item["l"], setup_hh120=item["hh120"],
                        stop_price=item["l"], target_price=item["h"] * 1.08
                    )
                sent_count += 1

        if etf_valid and 1520 <= hhmm <= 1540 and batch_7_candidates and can_fire_once(state, "MARKET", "BATCH_7_1500"):
            valid_7 = [it for it in batch_7_candidates if can_fire_cooldown(state, it["code"], "NEW_7_RS")]
            top5_7 = sorted(valid_7, key=lambda x: x["rs_growth"], reverse=True)[:5]
            if top5_7:
                lines = []
                for item in top5_7:
                    m_badge = "🔵" if item["market"] == "KOSPI" else "🟠"
                    gap_hh = ((item["c"] / item["hh120"]) - 1.0) * 100.0
                    lines.append(
                        f"{m_badge} **{item['name']} ({item['code']})** | 현재가 `{int(item['c']):,}원` (`{item['chg']:+.2f}%`)\n"
                        f"└ RS 20일 상승률 **`+{item['rs_growth']:.1f}%`** | 120일고점 `{int(item['hh120']):,}원` (`{gap_hh:.1f}%`) | 20일선 `{int(item['ma20']):,}원`"
                    )
                sent_ok = send_discord_embed(
                    webhook_url,
                    title=f"👑 [RS 라인 신고가 선행 TOP {len(top5_7)}] ({now.strftime('%H:%M')} 기준)",
                    description=(
                        "📖 **알림 의미:** 주가는 아직 120일 전고점 아래에 있지만, 시장(지수) 대비 상대강도(RS)가 먼저 120일 신고가를 돌파한 차기 주도주 후보입니다.\n\n"
                        + "\n\n".join(lines)
                    ),
                    color=3447003,
                    fields=[{
                        "name": "🧭 지금 해야 할 행동 / 체크포인트",
                        "value": (
                            "1) **주도주 후보 편입 신호** (5거래일간 자동 추적됨) — 거래대금 동반 120일 고점 돌파 시 트리거 알림 발송\n"
                            "2) **손절 기준:** 종가 기준 20일 이동평균선 이탈 시 자동 폐기"
                        ),
                        "inline": False,
                    }],
                )
                if sent_ok:
                    commit_once(state, "MARKET", "BATCH_7_1500")
                    for item in top5_7:
                        commit_cooldown(state, item["code"], "NEW_7_RS")
                        record_today_signal(
                            state, item["code"], item["name"], item["market"], "NEW_7", "7.RS신고가선행",
                            entry_price=item["c"], stop_price=item["ma20"], target_price=item["hh120"],
                            buy_zone=f"20일선({int(item['ma20']):,}원) 눌림 or {int(item['hh120']):,}원 돌파"
                        )
                        register_active_setup(
                            state, item["code"], item["name"], item["market"], "NEW_7", "7.RS신고가선행",
                            setup_high=item["h"], setup_low=item["l"], setup_hh120=item["hh120"],
                            stop_price=item["ma20"], target_price=item["hh120"]
                        )
                    sent_count += 1

        # 8번: 15:55 ~ 16:15 장 마감 브리핑 TOP 3 + 당일 active_setups 종가 동기화
        if 1555 <= hhmm <= 1615 and can_fire_once(state, "MARKET", "BATCH_8_CLOSING"):
            base_scores = {
                "NEW_1": 30, "NEW_2": 30, "NEW_3": 25,
                "NEW_6": 25, "NEW_4": 20, "NEW_5": 20, "NEW_7": 15,
            }
            scored_list = []
            closing_validated_map = {}

            for code, info in state.get("today_signals", {}).items():
                snap = live_snapshots.get(code)
                lv = state["levels"].get(code)
                if not snap or not lv:
                    continue

                c, o, h, l = snap["c"], snap["o"], snap["h"], snap["l"]
                chg, chg_5d, clv = snap["chg"], snap["chg_5d"], snap["clv"]
                if chg <= -5.0:
                    continue

                idx_close_chg = (ks_chg if info["market"] == "KOSPI" else kq_chg) if etf_valid else None

                valid_sigs = []
                for sig_id, sig_meta in info["signals"].items():
                    ok = False
                    recalc_meta = dict(sig_meta)
                    if sig_id == "NEW_1":
                        pl = lv["sweep_pl"]
                        ok = (pl > 0 and pl * 0.97 <= l < pl and c >= pl * 1.005 and c > o and clv >= 0.6)
                        recalc_meta["stop"] = l
                        recalc_meta["target"] = snap["ma20"] if snap["ma20"] > c else snap["hh20"]
                        recalc_meta["buy_zone"] = f"{int(pl):,}~{int(c):,}원"
                    elif sig_id == "NEW_2":
                        r_line = max(snap["ma60"], snap["ma120"])
                        ok = (c > r_line * 1.01)
                        raw_t23 = lv["hh120"] if lv["hh120"] > c * 1.05 else max(lv["hh240"], c * 1.15)
                        recalc_meta["stop"] = r_line * 0.99
                        recalc_meta["target"] = max(raw_t23, c * 1.12)
                        recalc_meta["buy_zone"] = f"{int(r_line):,}~{int(c):,}원"
                    elif sig_id == "NEW_3":
                        top_c = lv["ichimoku_top"]
                        t_val = (max(lv["hh8"], h) + min(lv["ll8"], l)) / 2.0
                        k_val = (max(lv["hh25"], h) + min(lv["ll25"], l)) / 2.0
                        ok = (top_c > 0 and c > top_c * 1.01 and t_val >= k_val and c > lv["c_26_ago"])
                        raw_t23 = lv["hh120"] if lv["hh120"] > c * 1.05 else max(lv["hh240"], c * 1.15)
                        recalc_meta["stop"] = top_c * 0.99
                        recalc_meta["target"] = max(raw_t23, c * 1.12)
                        recalc_meta["buy_zone"] = f"{int(top_c):,}~{int(c):,}원"
                    elif sig_id == "NEW_4":
                        ok = (c >= snap["ma20"] * 0.985 and chg > -5.0 and snap["rvol"] <= 120.0)
                        recalc_meta["stop"] = snap["ma20"] * 0.97
                        recalc_meta["target"] = snap["hh20"]
                        recalc_meta["buy_zone"] = f"{int(snap['ma20']*0.99):,}~{int(snap['ma20']*1.01):,}원"
                    elif sig_id == "NEW_5":
                        cb = lv.get("candle_b")
                        ok = (cb is not None and c >= cb["mid"] * 0.99)
                        if cb:
                            recalc_meta["stop"] = cb["open"]
                            recalc_meta["target"] = cb["high"]
                            recalc_meta["buy_zone"] = f"{int(cb['mid']):,}~{int(cb['mid']*1.02):,}원"
                    elif sig_id == "NEW_6":
                        ok = (
                            idx_close_chg is not None
                            and idx_close_chg <= -1.0
                            and chg >= 2.0
                            and c > o
                            and clv >= 0.6
                        )
                        recalc_meta["stop"] = l
                        recalc_meta["target"] = h * 1.08
                        recalc_meta["buy_zone"] = f"{int(h):,}원 돌파 시"
                    elif sig_id == "NEW_7":
                        ok = (snap["rs_today"] > lv["max_rs_120"] and c < lv["hh120"] and c > snap["ma60"])
                        recalc_meta["stop"] = snap["ma20"]
                        recalc_meta["target"] = lv["hh120"]
                        recalc_meta["buy_zone"] = f"20일선({int(snap['ma20']):,}원) 눌림 or {int(lv['hh120']):,}원 돌파"

                    if ok:
                        valid_sigs.append((sig_id, recalc_meta))

                if not valid_sigs:
                    continue

                closing_validated_map[code] = {sid: meta for sid, meta in valid_sigs}

                valid_sigs.sort(key=lambda x: base_scores.get(x[0], 0), reverse=True)
                _, top_meta = valid_sigs[0]
                stop_p = top_meta["stop"]
                target_p = top_meta["target"]
                rr = ((target_p - c) / (c - stop_p)) if (c > stop_p and target_p > c) else 0.0
                if rr < 1.0:
                    continue

                score = max(base_scores.get(s[0], 0) for s in valid_sigs)
                if len(valid_sigs) >= 2:
                    score += 15
                if clv >= 0.8:
                    score += 10
                elif clv < 0.4:
                    score -= 15

                adv_mult = (snap["amt"] / lv["adv20"]) if lv["adv20"] > 0 else 0.0
                if adv_mult >= 3.0:
                    score += 10
                elif adv_mult >= 2.0:
                    score += 5

                if rr >= 2.0:
                    score += 10
                if chg >= 12.0 or chg_5d >= 25.0:
                    score -= 15

                scored_list.append({
                    "code": code,
                    "name": info["name"],
                    "market": info["market"],
                    "score": score,
                    "labels": " + ".join(s[1]["label"] for s in valid_sigs),
                    "c": c,
                    "chg": chg,
                    "adv_mult": adv_mult,
                    "buy_zone": top_meta["buy_zone"],
                    "stop": stop_p,
                    "target": target_p,
                    "rr": rr,
                })

            # 당일 생성된 active_setups 종가 재검증 동기화
            setups = state.get("active_setups", {})
            for key, st in list(setups.items()):
                if st.get("created_date") == state["date"]:
                    s_code = st["code"]
                    s_id = st["sig_id"]
                    snap = live_snapshots.get(s_code)
                    survived = (s_code in closing_validated_map) and (s_id in closing_validated_map[s_code])
                    if not survived or not snap:
                        setups.pop(key, None)
                    else:
                        meta = closing_validated_map[s_code][s_id]
                        st["setup_high"] = float(snap["h"])
                        st["setup_low"] = float(snap["l"])
                        st["stop_price"] = float(meta["stop"])
                        st["target_price"] = float(meta["target"])

            hl = state.get("health", {})
            etf_stat = "정상(시차동기화)" if etf_valid else ("조회됨(시차대기)" if raw_etf_ok else "미조회")
            health_line = (
                f"🩺 **시스템 헬스체크:** 스캔 `{hl.get('target_cnt', 400)}종목` | "
                f"기준선 성공 `{hl.get('levels_ok', len(state['levels']))}건` (KRX `{hl.get('krx_cnt', 0)}` / 폴백 `{hl.get('fb_cnt', 0)}`) | "
                f"지수ETF `{etf_stat}`"
            )

            weekly_summary = build_weekly_performance_summary() if now.weekday() == 4 else ""
            footer_block = f"\n\n{health_line}" + (f"\n\n{weekly_summary}" if weekly_summary else "")

            if not scored_list:
                sent_ok = send_discord_embed(
                    webhook_url,
                    title=f"📋 [{state['date']} 장 마감 브리핑]",
                    description=(
                        "📖 **알림 의미:** 장중 발생한 시그널을 15:30 정규장 확정 종가 기준으로 재검증한 결과, 내일 시초가 공략 기준(손익비 1:1 이상)을 통과한 종목이 없습니다.\n\n"
                        "🧭 **행동 지침:** 무리한 신규 진입 없이 현금을 보유하며 다음 거래일 장중 시그널을 대기하세요."
                        + footer_block
                    ),
                    color=9807270,
                )
            else:
                top3 = sorted(scored_list, key=lambda x: x["score"], reverse=True)[:3]
                lines = []
                for rank, item in enumerate(top3, 1):
                    m_badge = "🔵" if item["market"] == "KOSPI" else "🟠"
                    lines.append(
                        f"**{rank}위. {m_badge} {item['name']} ({item['code']})** — 종합 **{item['score']}점**\n"
                        f"• 시그널: `{item['labels']}`\n"
                        f"• 종가/등락/거래배율: **{int(item['c']):,}원** (`{item['chg']:+.2f}%`) · 평상시 대비 `{item['adv_mult']:.1f}배`\n"
                        f"• 내일 매수구간: `{item['buy_zone']}` | 손절 `{int(item['stop']):,}원` | 1차목표 `{int(item['target']):,}원` (손익비 `1:{item['rr']:.1f}`)"
                    )
                sent_ok = send_discord_embed(
                    webhook_url,
                    title=f"🏆 [{state['date']} 장 마감 브리핑 TOP {len(top3)}]",
                    description=(
                        "📖 **알림 의미:** 당일 포착 시그널을 15:30 정규장 확정 종가 기준으로 재검증·재계산하여 선별한 내일 시초가 최우선 관심종목입니다.\n\n"
                        + "\n\n".join(lines)
                        + footer_block
                    ),
                    color=15844367,
                    fields=[{
                        "name": "🧭 내일(익일) 시초가 대응 가이드",
                        "value": (
                            "• **+3% 이상 갭상승 출발 시:** 절대 시초가 추격 금지 — 제시된 매수구간까지 눌림 대기\n"
                            "• **매수구간 안에서 출발 시:** 시가 지지를 확인한 뒤 1차 분할 진입\n"
                            "• **손절가 아래에서 출발 시:** 해당 종목 시나리오 즉시 폐기(패스)"
                        ),
                        "inline": False,
                    }],
                )
            if sent_ok:
                commit_once(state, "MARKET", "BATCH_8_CLOSING")
                sent_count += 1

    finally:
        save_state(state)

    print(f"스캔 완료 (발송 알림: {sent_count}건)")


if __name__ == "__main__":
    main()
