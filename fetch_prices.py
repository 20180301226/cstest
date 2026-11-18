#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
"""
采集层：从 SteamDT 开放平台拉取行情 → 归一化成 monitor.py 需要的 prices.json
只读取公开行情，不登录任何游戏账号、不提交任何订单。

接口依据（doc.steamdt.com，2026-11 核对）：
  鉴权     所有请求带 Header: Authorization: Bearer <API_KEY>
  批量价格 POST https://open.steamdt.com/open/cs2/v1/price/batch
           Body: {"marketHashNames": ["AK-47 | Redline (Field-Tested)", ...]}  1~100 件
           返回: data[].marketHashName + data[].dataList[].platform / sellPrice(最低挂单)
                 / sellCount(在售量) / biddingPrice(最高求购) / biddingCount / updateTime(ms)
           限额: 批量 1 次/分钟
  大盘指数 GET https://open.steamdt.com/open/cs2/broad/v1/index
  7日均价  GET https://open.steamdt.com/open/cs2/v1/price/avg  (单件类限额 60 次/分钟)
  限流表现 HTTP 429，或 HTTP 200 但 success=false 且 errorCode=4029

API_KEY 读取顺序（二选一，不要写进代码/提交仓库）：
  1. 环境变量  export STEAMDT_API_KEY=xxxx
  2. 同目录 .secrets.json 文件  {"steamdt_api_key": "xxxx"}

用法：
  export STEAMDT_API_KEY=xxxx
  python3.11 fetch_prices.py            # 生成 prices.json
  python3.11 monitor.py --input prices.json

定时（每 10 分钟；批量接口限 1 次/分钟，所以别低于 2 分钟一档）：
  */10 * * * * cd /workspace/cs2-monitor && /usr/bin/python3.11 fetch_prices.py && /usr/bin/python3.11 monitor.py --input prices.json >> run.log 2>&1

注意：
  * /open/cs2/v1/price/avg 的查询参数名以官方文档页面为准，若拿不到均价，
    脚本会把原始响应存到 debug_avg.json，对照文档改 AVG_PARAMS 即可。
  * 均价拿不到时 prices.json 中该饰品 ma7 为 null，monitor 会将其按「观望」处理，不会误报。
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
API_BASE = "https://open.steamdt.com"
CST = timezone(timedelta(hours=8))  # 北京时间

# 平台名归一化。steamDT 返回的 platform 写法可能是 "BUFF" / "YOUHIPIN898" / "youpin" 等，
# 先精确匹配，再用子串兜底；仍无法识别的平台保留原名（大写）。
PLATFORM_MAP = {
    "buff": "BUFF",
    "buff163": "BUFF",
    "youpin898": "悠悠有品",
    "youhipin898": "悠悠有品",
    "youpin": "悠悠有品",
    "uu898": "悠悠有品",
    "uu": "悠悠有品",
    "c5game": "C5GAME",
    "c5": "C5GAME",
    "igxe": "IGXE",
    "ecosteam": "ECOsteam",
    "eco": "ECOsteam",
    "steam": "Steam(折算·仅参考)",
    "steamcommunity": "Steam(折算·仅参考)",
}
# 余额不可提现的平台 → 参与估值展示，但禁止参与跨市场套利计算
NO_CASHOUT = ("steam",)

# 7 日均价接口的查询参数（若官方文档参数名不同，改这里）
AVG_PATH = "/open/cs2/v1/price/avg"
AVG_PARAMS = {"marketHashName": None, "days": "7"}  # None 处运行时填入饰品名


def load_api_key() -> str:
    key = os.environ.get("STEAMDT_API_KEY", "").strip()
    if key:
        return key
    secret_file = BASE_DIR / ".secrets.json"
    if secret_file.exists():
        try:
            data = json.loads(secret_file.read_text(encoding="utf-8"))
            key = str(data.get("steamdt_api_key") or "").strip()
            if key:
                return key
        except json.JSONDecodeError:
            pass
    sys.exit(
        "[error] 未找到 API_KEY。请先在 steamdt.com「个人中心-API 管理」申请，"
        "然后 export STEAMDT_API_KEY=xxx 或在同目录创建 .secrets.json："
        '{"steamdt_api_key": "xxx"}（不要提交到 git）'
    )


def request(path: str, *, method: str = "GET", body: dict | None = None,
            key: str = "") -> dict:
    url = API_BASE + path
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "User-Agent": "cs2-monitor/1.0",
    }
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8")[:300]
        except Exception:  # noqa: BLE001
            pass
        if exc.code == 429:
            sys.exit(f"[error] 触发限流 HTTP 429（批量接口限 1 次/分钟），请拉大运行间隔。{detail}")
        sys.exit(f"[error] HTTP {exc.code}：{detail}")
    except urllib.error.URLError as exc:
        sys.exit(f"[error] 网络请求失败：{exc.reason}")


def check(resp: dict, what: str) -> dict:
    if not resp.get("success"):
        code = resp.get("errorCode")
        msg = resp.get("errorMsg") or resp.get("errorCodeStr") or ""
        hint = ""
        if str(code) == "4029" or "限" in msg or "频繁" in msg:
            hint = "（限流：批量接口限 1 次/分钟，请拉大 cron 间隔）"
        if str(code) in ("401", "140020") or "key" in msg.lower():
            hint = "（凭证被拒：检查 API_KEY 是否正确、是否已开通 API 功能）"
        sys.exit(f"[error] {what} 调用失败 errorCode={code} {msg} {hint}")
    return resp.get("data") or {}


def normalize_name(name: str) -> str:
    s = name.strip().lower()
    if s in PLATFORM_MAP:
        return PLATFORM_MAP[s]
    if "buff" in s:
        return "BUFF"
    if "youpin" in s or "youhi" in s or s.startswith("uu"):
        return "悠悠有品"
    if "c5" in s:
        return "C5GAME"
    if "eco" in s:
        return "ECOsteam"
    if "igxe" in s:
        return "IGXE"
    if "steam" in s:
        return "Steam(折算·仅参考)"
    return name.strip().upper()


def is_no_cashout(platform: str) -> bool:
    return any(tag in platform.strip().lower() for tag in NO_CASHOUT)


def parse_batch(data: list, watchlist: list[dict]) -> dict:
    """把批量价格响应转成 {marketHashName: item}，供下面组装。"""
    out = {}
    for row in data:
        mh = row.get("marketHashName") or ""
        best = {}
        for q in row.get("dataList") or []:
            plat = normalize_name(q.get("platform") or "?")  # 归一化后按平台去重
            if plat not in best or (q.get("updateTime") or 0) > (best[plat].get("updateTime") or 0):
                best[plat] = q
        markets = []
        for plat, q in best.items():
            cashout = not is_no_cashout(plat)
            markets.append({
                "name": normalize_name(plat),
                "ask": q.get("sellPrice"),
                "bid": q.get("biddingPrice"),
                "sell_count": q.get("sellCount"),
                "bid_count": q.get("biddingCount"),
                "cashout": cashout,
            })
        out[mh] = {"markets": markets, "updated": max(
            (q.get("updateTime") or 0) for q in best.values()) if best else 0}
    return out


def extract_avg(data) -> float | None:
    """宽松提取 7 日均价：兼容 dict 字段名差异 / 时间序列列表两种返回形态。"""
    if isinstance(data, (int, float)):
        return float(data)
    if isinstance(data, dict):
        for k in ("avgPrice", "averagePrice", "price", "avg"):
            v = data.get(k)
            if isinstance(v, (int, float)):
                return float(v)
        for v in data.values():
            got = extract_avg(v)
            if got is not None:
                return got
    if isinstance(data, list):
        for row in reversed(data):  # 时间序列取最近一条
            got = extract_avg(row)
            if got is not None:
                return got
    return None


# ---------------------------------------------------------------------------
# 本地基线：baseline.json 记录每日价格/在售量快照，运行天数越多信号越准
# ---------------------------------------------------------------------------

BASELINE_PATH = BASE_DIR / "baseline.json"


def load_baseline() -> dict:
    if BASELINE_PATH.exists():
        try:
            return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def update_baseline(baseline: dict, items: list[dict]) -> None:
    """把本轮快照追加进基线，并回填 change_24h_pct / trend_7d / listings_ma7。"""
    now_ts = time.time()
    today = datetime.now(CST).strftime("%Y-%m-%d")
    for it in items:
        mh = it["market_hash_name"]
        cash = [m for m in it["markets"] if m["cashout"]]
        px = min((m["ask"] for m in cash), default=None)
        if px is None:
            continue
        rec = baseline.setdefault(mh, {"series": []})
        rec["series"].append({
            "date": today, "ts": now_ts,
            "price": px, "listings": it["listings"],
        })
        series = rec["series"]
        if len(series) > 400:
            rec["series"] = series = series[-400:]

        # 7 日走势与在售量均值：按自然日分组，组内按时间戳取最后一条
        by_day = {}
        for r in sorted(series, key=lambda x: x.get("ts") or 0):
            by_day[r["date"]] = r
        days = sorted(by_day.values(), key=lambda r: r.get("ts") or 0)[-7:]
        it["trend_7d"] = [round(r["price"], 2) for r in days]
        if days:
            it["listings_ma7"] = round(sum(r["listings"] for r in days) / len(days))

        # 24h 涨跌：与最近一条 20 小时前的快照相比
        prev = next((r for r in reversed(series) if now_ts - r["ts"] >= 20 * 3600), None)
        if prev and prev["price"]:
            it["change_24h_pct"] = round((px - prev["price"]) / prev["price"] * 100, 2)

        # 均价接口失败时，用本地 7 日均值兜底（需已积累 ≥5 天）
        if it["ma7"] is None and len(days) >= 5:
            it["ma7"] = round(sum(r["price"] for r in days) / len(days), 2)

    BASELINE_PATH.write_text(
        json.dumps(baseline, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    key = load_api_key()
    cfg = json.loads((BASE_DIR / "config.json").read_text(encoding="utf-8"))
    watchlist = cfg["watchlist"]
    names = [w["market_hash_name"] for w in watchlist]
    if len(names) > 100:
        sys.exit("[error] 批量接口单次最多 100 件，请精简自选清单或分批")

    now = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[1/3] 批量查价 {len(names)} 件 ...")
    resp = request("/open/cs2/v1/price/batch", method="POST",
                   body={"marketHashNames": names}, key=key)
    price_by_mh = parse_batch(check(resp, "批量查价"), watchlist)

    broad = {}
    print("[2/3] 拉取大盘指数 ...")
    try:
        bdata = check(request("/open/cs2/broad/v1/index", key=key), "大盘指数")
        broad = bdata if isinstance(bdata, dict) else {}
    except SystemExit:
        print("[warn] 大盘指数拉取失败（不影响饰品信号），响应已存 debug_broad.json")
        broad = {}

    print("[3/3] 逐件拉取 7 日均价（间隔 1.5s 防限流）...")
    items = []
    for w in watchlist:
        mh = w["market_hash_name"]
        info = price_by_mh.get(mh)
        if not info or not info["markets"]:
            print(f"  [warn] {mh} 无报价返回，本轮跳过")
            continue

        ma7 = None
        try:
            params = dict(AVG_PARAMS)
            params["marketHashName"] = mh
            query = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items() if v)
            adata = check(request(f"{AVG_PATH}?{query}", key=key), f"{mh} 7日均价")
            ma7 = extract_avg(adata)
        except SystemExit as exc:
            (BASE_DIR / "debug_avg.json").write_text(
                json.dumps({"item": mh, "hint": str(exc)}, ensure_ascii=False),
                encoding="utf-8")
            print(f"  [warn] {mh} 均价拿不到（见 debug_avg.json），本轮按观望处理")

        markets = [m for m in info["markets"] if m["ask"] is not None]
        # 全网总在售量 ≈ 各平台在售量之和（作为放量判断的口径之一）
        total_listings = sum(m["sell_count"] or 0 for m in markets)
        items.append({
            "name": w["name"],
            "market_hash_name": mh,
            "ma7": ma7,
            "change_24h_pct": 0,           # 见文末说明：24h 涨跌由日K或均价序列自行计算
            "listings": total_listings,
            "listings_ma7": 0,
            "trend_7d": [],
            "markets": markets,
        })
        time.sleep(1.5)

    if not items:
        sys.exit("[error] 本轮没有任何饰品拿到报价，请检查 market_hash_name 是否准确"
                 "（可先调 GET /open/cs2/v1/base 获取全量名称字典，注意该接口每天限 1 次）")

    baseline = load_baseline()
    update_baseline(baseline, items)  # 回填 24h 涨跌 / 7 日走势 / 在售量均值

    feed = {
        "updated_at": now,
        "source": "SteamDT 开放平台",
        "broad": broad,
        "items": items,
        "_notes": [
            "change_24h_pct / trend_7d / listings_ma7 由本地 baseline.json 按运行天数自动积累，"
            "前几轮为 0 属正常；连续运行满 7 天后信号完整。",
        ],
    }
    out = BASE_DIR / "prices.json"
    out.write_text(json.dumps(feed, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] 已生成 {out}（{now}），接下来运行：python3.11 monitor.py --input prices.json")


if __name__ == "__main__":
    main()
