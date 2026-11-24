import os
import json
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd
import FinanceDataReader as fdr

KST = timezone(timedelta(hours=9))
STATE_FILE = "alert_state.json"

CONFIG = {
    "THRESHOLDS": [-5.0, -10.0, -15.0],
    "TOP_N": 100,
    "EXCLUDE_PREFERRED": True,
}


def load_state():
    today = datetime.now(KST).strftime("%Y-%m-%d")
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") == today:
            return data
    return {"date": today, "alerted": {}}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_new_threshold(state, code, change_pct):
    already = state["alerted"].get(code, [])
    passed = [t for t in sorted(CONFIG["THRESHOLDS"]) if change_pct <= t]
    if not passed:
        return None

    deepest_hit = passed[0]
    if deepest_hit in already:
        return None

    for t in passed:
        if t not in already:
            already.append(t)
    state["alerted"][code] = already
    return deepest_hit


def send_discord(webhook_url, title, description, color, fields):
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "fields": fields,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    res = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
    res.raise_for_status()


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
    combined["Close"] = pd.to_numeric(combined["Close"], errors="coerce")
    combined["ChagesRatio"] = pd.to_numeric(combined["ChagesRatio"], errors="coerce")
    combined["Amount"] = pd.to_numeric(combined["Amount"], errors="coerce")
    return combined


def main():
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook_url:
        raise ValueError("DISCORD_WEBHOOK_URL 시크릿이 설정되지 않았습니다.")

    state = load_state()
    df = fetch_top100()
    min_trigger = max(CONFIG["THRESHOLDS"])
    dropped = df[df["ChagesRatio"] <= min_trigger]

    sent_count = 0
    for _, row in dropped.iterrows():
        code = str(row["Code"])
        name = str(row["Name"])
        market_label = row["MarketLabel"]
        price = int(row["Close"])
        chg = float(row["ChagesRatio"])
        amount_uk = int(row["Amount"] / 100_000_000) if pd.notna(row["Amount"]) else 0

        hit_level = get_new_threshold(state, code, chg)
        if hit_level is None:
            continue

        if hit_level <= -15.0:
            color, badge = 10038562, "🆘 -15% 폭락 경보"
        elif hit_level <= -10.0:
            color, badge = 15548997, "🚨 -10% 급락 경보"
        else:
            color, badge = 15105570, "⚠️ -5% 하락 알림"

        fields = [
            {"name": "현재가", "value": f"{price:,}원", "inline": True},
            {"name": "등락률", "value": f"**{chg:.2f}%**", "inline": True},
            {"name": "거래대금", "value": f"{amount_uk:,}억 원", "inline": True},
        ]
        send_discord(
            webhook_url,
            title=f"{badge} | {name} ({code})",
            description=f"**{market_label}** 편입 종목이 **{hit_level:.0f}%** 기준선을 하회했습니다.",
            color=color,
            fields=fields,
        )
        sent_count += 1

    save_state(state)
    print(f"스캔 완료 (발송된 알림: {sent_count}건)")


if __name__ == "__main__":
    main()
