#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
"""
CS2 皮肤盯盘引擎（只「看」不「下单」）

职责：定时拉取行情 → 计算信号 → 生成看板数据 → 推送提醒
红线：本脚本不登录任何账号、不提交订单、不模拟点击。交易决策永远由人完成。

用法：
    python3.11 monitor.py --mock                 # 用内置演示数据跑通全流程
    python3.11 monitor.py --input prices.json    # 用你自己的行情源
    python3.11 monitor.py --input prices.json --config config.json --out .

定时（每 10 分钟）：
    */10 * * * * cd /workspace/cs2-monitor && /usr/bin/python3.11 monitor.py --input prices.json >> run.log 2>&1

行情输入格式（prices.json，可来自 steamDT / CS2Cap / cs2.sh / CSPriceAPI 等任意聚合接口）：
{
  "updated_at": "2026-11-18 22:00:00",
  "broad": {"index": 1284.6, "change_pct": 0.87, "volume_cny": 240000000,
            "advancers": 312, "decliners": 187},
  "items": [
    {
      "name": "AK-47 | 红线 (久经沙场)",
      "ma7": 182.0,
      "change_24h_pct": -3.1,
      "listings": 320,
      "listings_ma7": 290,
      "trend_7d": [186, 184, 183, 185, 181, 179, 168],
      "markets": [
        {"name": "BUFF",     "ask": 168.0, "bid": 166.0},
        {"name": "悠悠有品", "ask": 172.0, "bid": 181.0}
      ]
    }
  ]
}
说明：ask = 该平台最低挂单价，bid = 该平台最高求购价，单位统一为 CNY。
      只填 ask 不填 bid 也可以（bid 设为 null），跨市场信号会自动跳过。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# 演示数据：结构与真实行情源一致，方便先看到效果再接数据
# ---------------------------------------------------------------------------
MOCK_FEED = {
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "source": "演示数据",
    "broad": {
        "index": 1284.6,
        "change_pct": 0.87,
        "volume_cny": 241_800_000,
        "advancers": 312,
        "decliners": 187,
    },
    "items": [
        {
            "name": "AK-47 | 红线 (久经沙场)",
            "ma7": 182.0, "change_24h_pct": -3.1,
            "listings": 320, "listings_ma7": 290,
            "trend_7d": [186, 184, 183, 185, 181, 179, 168],
            "markets": [
                {"name": "BUFF", "ask": 168.0, "bid": 166.0},
                {"name": "悠悠有品", "ask": 172.0, "bid": 181.0},
                {"name": "C5GAME", "ask": 175.0, "bid": 170.0},
                # Steam 余额不可提现且费率 15%，只做估值参考，禁止参与搬砖计算
                {"name": "Steam(折算·仅参考)", "ask": 210.0, "bid": 198.0,
                 "cashout": False},
            ],
        },
        {
            "name": "M4A1-S | 印花集 (略有磨损)",
            "ma7": 338.0, "change_24h_pct": 2.4,
            "listings": 210, "listings_ma7": 205,
            "trend_7d": [329, 331, 334, 336, 337, 341, 352],
            "markets": [
                {"name": "悠悠有品", "ask": 352.0, "bid": 344.0},
                {"name": "BUFF", "ask": 355.0, "bid": 341.0},
                {"name": "CSFloat(折算)", "ask": 349.0, "bid": 339.0},
            ],
        },
        {
            "name": "AWP | 二西莫夫 (久经沙场)",
            "ma7": 705.0, "change_24h_pct": -6.8,
            "listings": 590, "listings_ma7": 310,
            "trend_7d": [712, 710, 708, 703, 695, 672, 648],
            "markets": [
                {"name": "BUFF", "ask": 648.0, "bid": 638.0},
                {"name": "悠悠有品", "ask": 655.0, "bid": 640.0},
                {"name": "C5GAME", "ask": 660.0, "bid": 642.0},
            ],
        },
        {
            "name": "沙漠之鹰 | 蓝色层压板 (崭新出厂)",
            "ma7": 13.2, "change_24h_pct": 64.5,
            "listings": 1200, "listings_ma7": 480,
            "trend_7d": [12.9, 13.0, 13.1, 13.4, 13.6, 14.8, 16.67],
            "markets": [
                {"name": "悠悠有品", "ask": 16.67, "bid": 14.90},
                {"name": "BUFF", "ask": 16.20, "bid": 15.10},
            ],
        },
        {
            "name": "★ 蝴蝶刀 | 多普勒 (崭新出厂)",
            "ma7": 4300.0, "change_24h_pct": -0.8,
            "listings": 88, "listings_ma7": 92,
            "trend_7d": [4320, 4315, 4308, 4302, 4296, 4288, 4210],
            "markets": [
                {"name": "BUFF", "ask": 4210.0, "bid": 4160.0},
                {"name": "悠悠有品", "ask": 4240.0, "bid": 4180.0},
            ],
        },
        {
            "name": "USP-S | 印花集 (崭新出厂)",
            "ma7": 278.0, "change_24h_pct": -4.2,
            "listings": 410, "listings_ma7": 390,
            "trend_7d": [281, 280, 279, 277, 274, 266, 258],
            "markets": [
                {"name": "悠悠有品", "ask": 258.0, "bid": 262.0},
                {"name": "BUFF", "ask": 261.0, "bid": 255.0},
            ],
        },
    ],
}

# ---------------------------------------------------------------------------
# 信号引擎
# ---------------------------------------------------------------------------

SIGNAL_META = {
    "arbitrage": {"label": "跨市场价差", "level": "opportunity", "rank": 5},
    "dip": {"label": "低吸机会", "level": "opportunity", "rank": 4},
    "peak": {"label": "高抛机会", "level": "opportunity", "rank": 4},
    "risk_dump": {"label": "放量下挫", "level": "risk", "rank": 3},
    "risk_spike": {"label": "高位放量", "level": "risk", "rank": 3},
    "watch": {"label": "观望", "level": "neutral", "rank": 0},
}


def analyze(item: dict, cfg: dict) -> dict:
    """对单件饰品计算所有指标与信号。"""
    s = cfg["strategy"]
    markets = [m for m in item.get("markets", []) if m.get("ask")]
    # 展示用：所有平台；计算用：仅可提现平台（cashout 默认 True）
    all_quotes = [{
        "name": m["name"],
        "price": float(m["ask"]),
        "bid": float(m["bid"]) if m.get("bid") is not None else None,
        "cashout": bool(m.get("cashout", True)),
    } for m in markets]
    cash_quotes = [q for q in all_quotes if q["cashout"]]

    cheapest = min(cash_quotes, key=lambda x: x["price"]) if cash_quotes else None
    cash_bids = [q for q in cash_quotes if q["bid"] is not None]
    best_bid = max(cash_bids, key=lambda x: x["bid"]) if cash_bids else None

    ma7 = float(item.get("ma7") or 0)
    ask = cheapest["price"] if cheapest else 0.0
    deviation = ((ask - ma7) / ma7 * 100) if ma7 else 0.0

    listings = float(item.get("listings") or 0)
    listings_ma7 = float(item.get("listings_ma7") or 0)
    volume_ratio = (listings / listings_ma7) if listings_ma7 else 1.0
    is_spike = volume_ratio >= s["volume_spike_ratio"]

    fee = s["fee_pct_total"] / 100.0
    # 跨市场：在最低挂单平台买，在最高求购平台卖，扣一次完整周转的综合费率
    net_margin = None
    if cheapest and best_bid:
        gross = best_bid["bid"] - cheapest["price"]
        net = best_bid["bid"] * (1 - fee) - cheapest["price"]
        net_margin = net / cheapest["price"] * 100
        gross_margin = gross / cheapest["price"] * 100
    else:
        gross_margin = 0.0

    change_24h = float(item.get("change_24h_pct") or 0)
    tags = []

    # 信号优先级：风险异动 > 确定性价差 > 偏离机会 > 观望
    if is_spike and change_24h <= -5:
        key = "risk_dump"
        tags.append(f"在售量 {volume_ratio:.1f}× 于 7 日均值，疑似集中出货")
    elif is_spike and change_24h >= 15:
        key = "risk_spike"
        tags.append(f"24h 拉升 {change_24h:+.1f}% 且挂单量 {volume_ratio:.1f}×，追高风险高")
    elif (net_margin is not None
          and net_margin >= s["min_net_margin_pct"]
          and gross_margin >= s["cross_market_min_spread_pct"]):
        key = "arbitrage"
        tags.append(f"{cheapest['name']} 买 ¥{cheapest['price']:,.2f} → "
                    f"{best_bid['name']} 求购 ¥{best_bid['bid']:,.2f}，"
                    f"扣费后净利 {net_margin:+.2f}%")
    elif deviation <= s["dip_threshold_pct"]:
        key = "dip"
        tags.append(f"现价低于 7 日均价 {abs(deviation):.1f}%，供给未异常放大")
    elif deviation >= s["peak_threshold_pct"]:
        key = "peak"
        tags.append(f"现价高于 7 日均价 {deviation:.1f}%，持仓可考虑分批止盈")
    else:
        key = "watch"

    if net_margin is not None and net_margin > 0 and key != "arbitrage":
        tags.append(f"存在 {net_margin:+.2f}% 跨市场净价差，但未达 {s['min_net_margin_pct']}% 阈值")
    if abs(change_24h) >= 10 and key == "watch":
        tags.append(f"24h 波动 {change_24h:+.1f}%，暂不满足开仓条件")

    meta = SIGNAL_META[key]
    return {
        "name": item["name"],
        "signal": key,
        "signal_label": meta["label"],
        "level": meta["level"],
        "rank": meta["rank"],
        "ask": ask,
        "ask_market": cheapest["name"] if cheapest else "—",
        "bid": best_bid["bid"] if best_bid else None,
        "bid_market": best_bid["name"] if best_bid else "—",
        "ma7": ma7,
        "deviation_pct": round(deviation, 2),
        "change_24h_pct": change_24h,
        "listings": int(listings),
        "listings_ma7": int(listings_ma7),
        "volume_ratio": round(volume_ratio, 2),
        "gross_margin_pct": round(gross_margin, 2) if cheapest and best_bid else None,
        "net_margin_pct": round(net_margin, 2) if net_margin is not None else None,
        "trend_7d": item.get("trend_7d", []),
        "markets": sorted(all_quotes, key=lambda x: x["price"]),
        "tags": tags,
    }


def build_alerts(results: list[dict], cfg: dict, now: datetime) -> list[dict]:
    """把非「观望」信号转成提醒流；带冷却去重。"""
    cooldown = timedelta(hours=cfg["strategy"]["alert_cooldown_hours"])
    state_path = BASE_DIR / ".alert_state.json"
    state = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            state = {}

    alerts = []
    for r in results:
        if r["signal"] == "watch":
            continue
        last = state.get(r["name"])
        if last and now - datetime.fromisoformat(last) < cooldown:
            continue
        state[r["name"]] = now.isoformat()

        if r["level"] == "risk":
            title = f"{r['signal_label']} · 建议回避"
            action = "先观察挂单消化情况，不要接飞刀 / 不要追高"
        elif r["signal"] == "arbitrage":
            title = "跨市场净价差达标"
            action = "人工核对磨损、相位与在售深度，确认后再手动下单"
        elif r["signal"] == "dip":
            title = "价格进入低吸区间"
            action = "对照目标买入价，人工确认后分批建仓"
        else:
            title = "价格进入止盈区间"
            action = "检查持仓成本，人工确认后分批挂单"

        alerts.append({
            "time": now.strftime("%H:%M"),
            "name": r["name"],
            "level": r["level"],
            "title": title,
            "detail": "；".join(r["tags"]),
            "action": action,
            "signal_label": r["signal_label"],
        })

    # 只保留最近 7 天状态，避免文件无限增长
    cutoff = now - timedelta(days=7)
    state = {k: v for k, v in state.items()
             if datetime.fromisoformat(v) > cutoff}
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return alerts


def push_webhook(alerts: list[dict], cfg: dict) -> None:
    """把提醒推到钉钉 / 企业微信 / 飞书群机器人（配置了才发）。

    云端部署（GitHub Actions / 服务器）时可用环境变量覆盖配置文件：
      NOTIFY_WEBHOOK_URL  机器人 Webhook 地址（设置后自动开启推送）
      BOARD_URL           看板网址（可选，附在消息末尾，方便手机点开）
    钉钉机器人若用「自定义关键词」校验，关键词填 CS2 即可，本消息必含。
    """
    n = dict(cfg.get("notify", {}))
    env_url = os.environ.get("NOTIFY_WEBHOOK_URL", "").strip()
    if env_url:
        n["webhook_url"] = env_url
        n["enabled"] = True
    if not n.get("enabled") or not n.get("webhook_url") or not alerts:
        return
    lines = [f"【CS2 盯盘】{len(alerts)} 条信号"]
    for a in alerts:
        lines.append(f"\n· [{a['signal_label']}] {a['name']}\n  {a['detail']}\n  → {a['action']}")
    board = os.environ.get("BOARD_URL", "").strip() or str(n.get("board_url") or "").strip()
    if board:
        lines.append(f"\n看板：{board}")
    channel = n.get("channel", "wecom")
    if channel == "feishu":
        payload = {"msg_type": "text", "content": {"text": "\n".join(lines)}}
    else:
        payload = {"msgtype": "text", "text": {"content": "\n".join(lines)}}
    try:
        req = urllib.request.Request(
            n["webhook_url"],
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[notify] 推送成功 HTTP {resp.status}")
    except Exception as exc:  # noqa: BLE001
        print(f"[notify] 推送失败：{exc}", file=sys.stderr)


def sparkline_svg(values: list[float], up_color: str, down_color: str) -> str:
    """生成内联迷你走势 SVG 字符串。"""
    if len(values) < 2:
        return ""
    lo, hi = min(values), max(values)
    span = (hi - lo) or 1.0
    w, h, pad = 108, 30, 3
    pts = []
    for i, v in enumerate(values):
        x = pad + i * (w - 2 * pad) / (len(values) - 1)
        y = pad + (h - 2 * pad) * (1 - (v - lo) / span)
        pts.append(f"{x:.1f},{y:.1f}")
    color = up_color if values[-1] >= values[0] else down_color
    area = f"{pad},{h - pad} " + " ".join(pts) + f" {w - pad},{h - pad}"
    return (f'<svg class="spark" viewBox="0 0 {w} {h}" preserveAspectRatio="none">'
            f'<polygon points="{area}" fill="{color}" opacity="0.12"/>'
            f'<polyline points="{" ".join(pts)}" fill="none" stroke="{color}" '
            f'stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"/>'
            f'</svg>')


def main() -> None:
    ap = argparse.ArgumentParser(description="CS2 皮肤盯盘引擎（只监控，不下单）")
    ap.add_argument("--mock", action="store_true", help="使用内置演示数据")
    ap.add_argument("--input", type=Path, help="行情 JSON 文件路径")
    ap.add_argument("--config", type=Path, default=BASE_DIR / "config.json")
    ap.add_argument("--out", type=Path, default=BASE_DIR)
    ap.add_argument("--no-notify", action="store_true", help="本次不推送")
    args = ap.parse_args()

    cfg = json.loads(args.config.read_text(encoding="utf-8"))
    if args.mock or not args.input:
        feed = MOCK_FEED
        if not args.mock:
            print("[warn] 未指定 --input，已自动使用演示数据", file=sys.stderr)
    else:
        feed = json.loads(args.input.read_text(encoding="utf-8"))

    now = datetime.now()
    results = [analyze(item, cfg) for item in feed.get("items", [])]
    results.sort(key=lambda r: (-r["rank"], r["deviation_pct"]))
    alerts = build_alerts(results, cfg, now)
    if not args.no_notify:
        push_webhook(alerts, cfg)

    up, down = "#f0464e", "#2ebd85"
    for r in results:
        r["spark_svg"] = sparkline_svg(r["trend_7d"], up, down)

    broad = feed.get("broad", {})
    sentiment = "偏强震荡" if broad.get("change_pct", 0) > 0.3 else (
        "偏弱整理" if broad.get("change_pct", 0) < -0.3 else "窄幅盘整")
    data = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "source": feed.get("source", "自定义行情源"),
        "is_mock": bool(args.mock or not args.input),
        "broad": {**broad, "sentiment": sentiment},
        "strategy": cfg["strategy"],
        "items": results,
        "alerts": alerts,
        "counts": {
            "total": len(results),
            "opportunity": sum(1 for r in results if r["level"] == "opportunity"),
            "risk": sum(1 for r in results if r["level"] == "risk"),
            "watch": sum(1 for r in results if r["level"] == "neutral"),
        },
    }

    out_dir = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    js = out_dir / "data.js"
    js.write_text("window.MONITOR_DATA = " +
                  json.dumps(data, ensure_ascii=False, indent=2) + ";\n",
                  encoding="utf-8")
    print(f"[ok] 已生成 {js}")
    for a in alerts:
        print(f"  [{a['signal_label']}] {a['name']} — {a['detail']}")
    if not alerts:
        print("  本次无新信号（可能处于冷却期内）")


if __name__ == "__main__":
    main()
