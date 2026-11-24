import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd
import FinanceDataReader as fdr

KST = timezone(timedelta(hours=9))
STATE_FILE = "alert_state.json"

# ==============================================================================
# 사용자 알림 기준 설정 (필요시 숫자만 수정)
# ==============================================================================
CONFIG = {
    "THRESHOLDS": [-5.0, -10.0, -15.0],  # 일간 하락률 알림 기준 (%)
    "TOP_N": 100,                        # 코스피 상위 100 + 코스닥 상위 100
    "EXCLUDE_PREFERRED": True,           # 우선주 제외 여부
    "VOL_SURGE_PCT": 250.0,              # 동시간대 20일 평균 대비 거래량 250%(2.5배) 이상
    "MIN_TRADE_AMOUNT_UK": 100,          # 최소 거래대금 100억 원 이상 필터
    "LOW_TOUCH_BUFFER_PCT": 0.5,         # 전저점·역사적 저점 대비 +0.5% 이내 근접/이탈 시 발동
}


def load_state():
    today = datetime.now(KST).strftime("%Y-%m-%d")
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") == today:
            return data
    return {"date": today, "alerted": {}, "levels": {}}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def mark_and_check_once(state, code, event_key):
    already = state["alerted"].get(code, [])
    if event_key in already:
        return False
    already.append(event_key)
    state["alerted"][code] = already
    return True


def send_discord(webhook_url, title, description, color, fields):
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "fields": fields,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    for _ in range(3):
        res = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
        if res.status_code == 429:
            retry_after = float(res.json().get("retry_after", 2.0))
            print(f"디스코드 속도 제한 감지 - {retry_after}초 대기 후 재전송...")
            time.sleep(retry_after + 0.5)
            continue
        res.raise_for_status()
        time.sleep(1.2)  # 연속 발송 시 스팸 차단 방지 딜레이
        return


def fetch_top100():
    frames = []
    for market in ["KOSPI", "KOSDAQ"]:
        df = fdr.StockListing(market).copy()
        if CONFIG["EXCLUDE_PREFERRED"]:
            df = df[df["Code"].str.endswith("0")]
        df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce")
        df = df.sort_values("Marcap", ascending=False).head(CONFIG["TOP_N"]).copy()
        df["MarketLabel"] = "코스피100" if market == "KOSPI" else "코스닥100"
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    for col in ["Close", "Low", "ChagesRatio", "Volume", "Amount"]:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")
    return combined


def _calc_single_stock_levels(code):
    try:
        hist = fdr.DataReader(code)
        if hist is None or len(hist) < 10:
            return code, None

        today_str = datetime.now(KST).strftime("%Y-%m-%d")
        if str(hist.index[-1])[:10] == today_str:
            hist = hist.iloc[:-1]

        if len(hist) < 10:
            return code, None

        valid_lows = hist["Low"][hist["Low"] > 0]
        hist_low = float(valid_lows.min()) if len(valid_lows) > 0 else 0.0
        avg_vol_20 = float(hist["Volume"].tail(20).mean())
        prev_close = float(hist["Close"].iloc[-1])

        recent = hist.tail(120).iloc[:-3]
        lows = recent["Low"].values
        pivot_lows = []
        for i in range(5, len(lows) - 5):
            window = lows[i - 5 : i + 6]
            if lows[i] > 0 and lows[i] == window.min():
                pivot_lows.append(float(lows[i]))

        below_pivots = [p for p in pivot_lows if p < prev_close]
        if below_pivots:
            prev_low = max(below_pivots)
        elif len(recent) > 0:
            prev_low = float(recent["Low"][recent["Low"] > 0].min())
        else:
            prev_low = hist_low

        return code, {
            "avg_vol_20": avg_vol_20,
            "prev_low": prev_low,
            "hist_low": hist_low,
        }
    except Exception:
        return code, None


def ensure_daily_levels(state, codes):
    missing = [c for c in codes if c not in state["levels"]]
    if not missing:
        return

    print(f"금일 기준선(전저점·역사적저점·평균거래량) 최초 계산 중 ({len(missing)}종목)...")
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(_calc_single_stock_levels, code) for code in missing]
        for fut in as_completed(futures):
            code, info = fut.result()
            if info:
                state["levels"][code] = info


def get_market_elapsed_ratio():
    now = datetime.now(KST)
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    elapsed_min = (now - market_open).total_seconds() / 60.0
    if elapsed_min <= 0:
        return 1.0
    effective_min = min(390.0, max(15.0, elapsed_min))
    return effective_min / 390.0


def main():
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook_url:
        raise ValueError("DISCORD_WEBHOOK_URL 시크릿이 설정되지 않았습니다.")

    state = load_state()
    sent_count = 0

    try:
        df = fetch_top100()
        codes = df["Code"].astype(str).tolist()
        ensure_daily_levels(state, codes)

        elapsed_ratio = get_market_elapsed_ratio()
        buffer_mult = 1.0 + (CONFIG["LOW_TOUCH_BUFFER_PCT"] / 100.0)

        for _, row in df.iterrows():
            code = str(row["Code"])
            name = str(row["Name"])
            market_label = row["MarketLabel"]
            price = int(row["Close"]) if pd.notna(row["Close"]) else 0
            intraday_low = int(row["Low"]) if ("Low" in row and pd.notna(row["Low"]) and row["Low"] > 0) else price
            check_low = min(price, intraday_low)
            chg = float(row["ChagesRatio"]) if pd.notna(row["ChagesRatio"]) else 0.0
            curr_vol = float(row["Volume"]) if pd.notna(row["Volume"]) else 0.0
            amount_uk = int(row["Amount"] / 100_000_000) if pd.notna(row["Amount"]) else 0

            if price <= 0:
                continue

            levels = state["levels"].get(code, {})
            avg_vol_20 = levels.get("avg_vol_20", 0)
            prev_low = levels.get("prev_low", 0)
            hist_low = levels.get("hist_low", 0)

            # 1. 역사적 저점 터치 / 이탈
            if hist_low > 0 and check_low <= hist_low * buffer_mult:
                if mark_and_check_once(state, code, "HIST_LOW"):
                    status_txt = "역사적 신저가 이탈!" if check_low < hist_low else "역사적 저점 바짝 근접!"
                    send_discord(
                        webhook_url,
                        title=f"🆘 [역사적 저점] {name} ({code}) | {status_txt}",
                        description=f"**{market_label}** 종목이 상장 이후 역대 최저가 영역에 도달했습니다.",
                        color=10038562,
                        fields=[
                            {"name": "현재가 (장중저가)", "value": f"{price:,}원 ({check_low:,}원)", "inline": True},
                            {"name": "역사적 저점", "value": f"**{int(hist_low):,}원**", "inline": True},
                            {"name": "등락률 / 거래대금", "value": f"{chg:+.2f}% / {amount_uk:,}억", "inline": True},
                        ],
                    )
                    sent_count += 1

            # 2. 직전 전저점(Pivot Low) 터치 / 이탈
            elif prev_low > 0 and check_low <= prev_low * buffer_mult:
                if mark_and_check_once(state, code, "PREV_LOW"):
                    status_txt = "전저점 지지선 이탈" if check_low < prev_low else "전저점 지지선 터치"
                    send_discord(
                        webhook_url,
                        title=f"📍 [전저점 도달] {name} ({code}) | {status_txt}",
                        description=f"**{market_label}** 종목이 최근 6개월 주요 전저점 지지선에 닿았습니다.",
                        color=15105570,
                        fields=[
                            {"name": "현재가 (장중저가)", "value": f"{price:,}원 ({check_low:,}원)", "inline": True},
                            {"name": "직전 전저점", "value": f"**{int(prev_low):,}원**", "inline": True},
                            {"name": "등락률 / 거래대금", "value": f"{chg:+.2f}% / {amount_uk:,}억", "inline": True},
                        ],
                    )
                    sent_count += 1

            # 3. 실시간 거래량 급증 (동시간대 평균 대비 250% 이상 + 100억 이상)
            if avg_vol_20 > 0 and amount_uk >= CONFIG["MIN_TRADE_AMOUNT_UK"]:
                expected_vol = avg_vol_20 * elapsed_ratio
                vol_pace_pct = (curr_vol / expected_vol) * 100.0
                if vol_pace_pct >= CONFIG["VOL_SURGE_PCT"]:
                    if mark_and_check_once(state, code, "VOL_SURGE"):
                        direction = "🔥 매수세 수급 폭발" if chg >= 0 else "🌊 투매 거래량 폭발"
                        color = 3066993 if chg >= 0 else 15548997
                        send_discord(
                            webhook_url,
                            title=f"{direction} | {name} ({code})",
                            description=f"**{market_label}** 종목에 평상시 동시간대 대비 **{vol_pace_pct:,.0f}%** 거래량이 몰리고 있습니다.",
                            color=color,
                            fields=[
                                {"name": "현재가 (등락률)", "value": f"{price:,}원 (**{chg:+.2f}%**)", "inline": True},
                                {"name": "거래량 폭발률", "value": f"**{vol_pace_pct:,.0f}%**", "inline": True},
                                {"name": "누적 거래대금", "value": f"{amount_uk:,}억 원", "inline": True},
                            ],
                        )
                        sent_count += 1

            # 4. -5% / -10% / -15% 일간 급락 경보
            passed = [t for t in sorted(CONFIG["THRESHOLDS"]) if chg <= t]
            if passed:
                deepest_hit = passed[0]
                if mark_and_check_once(state, code, f"DROP_{deepest_hit}"):
                    for t in passed:
                        mark_and_check_once(state, code, f"DROP_{t}")

                    if deepest_hit <= -15.0:
                        badge, color = "🆘 -15% 폭락 경보", 10038562
                    elif deepest_hit <= -10.0:
                        badge, color = "🚨 -10% 급락 경보", 15548997
                    else:
                        badge, color = "⚠️ -5% 하락 알림", 15105570

                    send_discord(
                        webhook_url,
                        title=f"{badge} | {name} ({code})",
                        description=f"**{market_label}** 편입 종목이 **{deepest_hit:.0f}%** 기준선을 하회했습니다.",
                        color=color,
                        fields=[
                            {"name": "현재가", "value": f"{price:,}원", "inline": True},
                            {"name": "등락률", "value": f"**{chg:.2f}%**", "inline": True},
                            {"name": "거래대금", "value": f"{amount_uk:,}억 원", "inline": True},
                        ],
                    )
                    sent_count += 1
    finally:
        # 중간에 무슨 일이 생겨도 이미 보낸 알림 기록은 반드시 저장
        save_state(state)

    print(f"스캔 완료 (신규 발송 알림: {sent_count}건)")


if __name__ == "__main__":
    main()
