# -*- coding: utf-8 -*-
"""Agentic Watch 网页版数据更新器：由 GitHub Actions 每 10 分钟运行一次。
只读链上公开数据，不签名、不花钱。"""
import json, os, re, time, sys, concurrent.futures as cf
from collections import Counter
import requests

D = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
REGISTRY = "0x60b9262456de011f7d15a54cdd8c48baaa8b022f"
T_REG = "0x663d34a86c6277aafbd5c6912c96cc5df091e6702ea3ea91b8f378adcbd8a5e5"
T_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO32 = "0x" + "0" * 64
MULTICALL3 = "0xca11bde05977b3631167028862be2a173976ca11"
CATALOG_URL = "https://agentidhub.eth.limo/project-catalog.js"
RPC_LOGS = os.environ.get("RPC_LOGS", "https://rpc-bsc.48.club").split(",")           # 支持不带地址的 getLogs
RPC_ADDR_LOGS = os.environ.get("RPC_ADDR_LOGS", "https://rpc-bsc.48.club,https://bsc-rpc.publicnode.com").split(",")
RPC = ["https://bsc-rpc.publicnode.com", "https://bsc-dataseed.bnbchain.org", "https://rpc-bsc.48.club",
       "https://bsc-dataseed1.binance.org", "https://bsc.drpc.org", "https://bsc-dataseed2.bnbchain.org"]
DEADLINE = time.time() + float(os.environ.get("MAX_SECONDS", "480"))
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0 AgenticWatch"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def rpc(method, params, urls=RPC, tries=8, timeout=30):
    last = None
    for k in range(tries):
        u = urls[k % len(urls)]
        try:
            j = S.post(u, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=(8, timeout)).json()
            if "result" in j:
                return j["result"]
            last = j.get("error")
            if isinstance(last, dict) and "revert" in str(last.get("message", "")).lower():
                raise ValueError("revert")
        except ValueError:
            raise
        except Exception as e:
            last = e
        time.sleep(min(0.5 + k * 0.5, 4))
    raise RuntimeError(f"{method} failed: {str(last)[:150]}")


def rpc_batch(calls, urls=RPC, tries=6):
    """calls: [(method, params)] -> results list (None on error)"""
    for k in range(tries):
        u = urls[k % len(urls)]
        try:
            body = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)]
            j = S.post(u, json=body, timeout=40).json()
            if isinstance(j, list):
                out = [None] * len(calls)
                for x in j:
                    if isinstance(x, dict) and "result" in x:
                        out[x["id"]] = x["result"]
                if sum(1 for x in out if x is not None) >= len(calls) * 0.8:
                    return out
        except Exception:
            pass
        time.sleep(1 + k)
    return [None] * len(calls)


def dec_str(h):
    try:
        b = bytes.fromhex(h[2:] if h.startswith("0x") else h)
        if len(b) >= 64:
            off = int.from_bytes(b[:32], "big")
            if off == 32:
                n = int.from_bytes(b[32:64], "big")
                return b[64:64 + n].decode("utf-8", "replace").strip("\x00")
        return b.rstrip(b"\x00").decode("utf-8", "replace")
    except Exception:
        return ""


def _w(n):
    return n.to_bytes(32, "big")


def multicall(calls):
    out = []
    for i in range(0, len(calls), 150):
        chunk = calls[i:i + 150]
        elems = []
        for t, cd in chunk:
            b = bytes.fromhex(cd[2:])
            elems.append(bytes.fromhex(t[2:].rjust(64, "0")) + _w(1) + _w(96) + _w(len(b)) + b + b"\0" * ((32 - len(b) % 32) % 32))
        head, tail, off = b"", b"", 32 * len(elems)
        for e in elems:
            head += _w(off); tail += e; off += len(e)
        data = "0x82ad56cb" + (_w(32) + _w(len(elems)) + head + tail).hex()
        try:
            res = bytes.fromhex(rpc("eth_call", [{"to": MULTICALL3, "data": data}, "latest"], tries=3, timeout=20)[2:])
        except Exception:
            out += [None] * len(chunk); continue
        n = int.from_bytes(res[32:64], "big")
        for k in range(n):
            o = 64 + int.from_bytes(res[64 + 32 * k:96 + 32 * k], "big")
            ok = int.from_bytes(res[o:o + 32], "big")
            ro = o + int.from_bytes(res[o + 32:o + 64], "big")
            ln = int.from_bytes(res[ro:ro + 32], "big")
            out.append(res[ro + 32:ro + 32 + ln] if ok else None)
    return out


def jload(name, default):
    try:
        with open(os.path.join(D, name), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def jsave(name, obj):
    p = os.path.join(D, name)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p + ".tmp", "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(p + ".tmp", p)


# ------------------------------------------------------------------ 载入
os.makedirs(os.path.join(D, "w"), exist_ok=True)
state = jload("state.json", {"block": 0, "pstat": {}, "info": {}, "xs": {}, "cat_t": 0})
holders = {}
try:
    with open(os.path.join(D, "holders.txt"), encoding="utf-8") as f:
        for line in f:
            a, _, n = line.rstrip("\n").partition(" ")
            if len(a) == 40:
                holders[a] = n
except FileNotFoundError:
    pass
contracts = jload("contracts.json", [])
cidx = {c: i for i, c in enumerate(contracts)}
W = {}  # wallet40 -> {ci: tmin}
for k in range(256):
    sh = jload(f"w/{k:02x}.json", {})
    for w, arr in sh.items():
        W[w] = {arr[i]: arr[i + 1] for i in range(0, len(arr), 2)}
feed = jload("feed.json", [])
sites = jload("sites.json", {})
log(f"载入：钱包 {len(holders):,}，mint 记录钱包 {len(W):,}，项目 {len(contracts)}")


def ci_of(c):
    if c not in cidx:
        cidx[c] = len(contracts); contracts.append(c)
    return cidx[c]


# ------------------------------------------------------------------ 扫链
head = int(rpc("eth_blockNumber", []), 16) - 2
frm = state["block"] + 1 if state["block"] else head - 1500
if head - frm > 60000:
    frm = head - 60000
if os.environ.get("SKIP_SCAN"):
    head = frm - 1
log(f"区块 {frm:,} → {head:,}（{head - frm + 1:,} 块）")

# 1) 新注册的 Agent ID
added = 0
b = frm
step = 4000
while b <= head and time.time() < DEADLINE - 200:
    e = min(b + step - 1, head)
    try:
        logs = rpc("eth_getLogs", [{"address": REGISTRY, "topics": [T_REG], "fromBlock": hex(b), "toBlock": hex(e)}], urls=RPC_ADDR_LOGS)
    except Exception as ex:
        if step > 500:
            step //= 2; continue
        log("注册事件读取失败：", ex); break
    for lg in logs:
        t = lg.get("topics") or []
        if len(t) > 2:
            a = t[2][-40:]
            nm = dec_str(lg.get("data", "0x"))
            nm = (nm if nm.endswith(".ai") else nm + ".ai") if nm else holders.get(a, "")
            if a not in holders:
                added += 1
            holders[a] = nm
    b = e + 1
log(f"新钱包 {added}")

# 2) mint 事件（ERC721 Transfer from 0x0 到 Agentic 钱包）
events = []  # (block, txhash, contract, wallet40)
bts = {}
done_to = frm - 1
CH = 100


def get_chunk(b0):
    e0 = min(b0 + CH - 1, head)
    logs = rpc("eth_getLogs", [{"topics": [T_TRANSFER, ZERO32], "fromBlock": hex(b0), "toBlock": hex(e0)}], urls=RPC_LOGS, tries=8, timeout=40)
    out = []
    for x in logs:
        t = x["topics"]
        if len(t) == 4:
            w = t[2][-40:]
            c = x["address"].lower()
            if w in holders and c != REGISTRY:
                out.append((int(x["blockNumber"], 16), x["transactionHash"], c, w))
    return b0, e0, out


starts = list(range(frm, head + 1, CH))
with cf.ThreadPoolExecutor(4) as ex:
    futs = []
    for s0 in starts:
        futs.append(ex.submit(get_chunk, s0))
    for f in futs:
        try:
            b0, e0, out = f.result()
        except Exception as e:
            log("mint 区块段读取失败，下次再补：", str(e)[:120]); break
        if b0 != done_to + 1:
            break
        events += out; done_to = e0
        if time.time() > DEADLINE - 120:
            log("时间快到了，剩下的下次补"); break
log(f"扫到 {len(events)} 条 Agentic 钱包 mint（到 {done_to:,}）")

# 区块时间：首尾两点插值
def btime(n):
    if n not in bts:
        bts[n] = int(rpc("eth_getBlockByNumber", [hex(n), False])["timestamp"], 16)
    return bts[n]


if events:
    t_a, t_b = btime(frm), btime(max(done_to, frm + 1))
    ts_of = lambda n: int(t_a + (n - frm) * (t_b - t_a) / max(1, (done_to - frm)))
else:
    ts_of = lambda n: int(time.time())

# 3) 交易详情：付了多少 BNB
last_blk = {}
for e in events:
    last_blk[e[1]] = max(last_blk.get(e[1], 0), e[0])
txs = sorted(last_blk, key=lambda h: -last_blk[h])[:2500]  # 补数据量大时只查最近的交易价格
txinfo = {}
for i in range(0, len(txs), 50):
    if time.time() > DEADLINE - 90:
        break
    part = txs[i:i + 50]
    res = rpc_batch([("eth_getTransactionByHash", [h]) for h in part])
    for h, r in zip(part, res):
        if r:
            txinfo[h] = int(r.get("value", "0x0"), 16) / 1e18
per_tx = Counter((e[1], e[2]) for e in events)
seen_tx = set()
pstat = state["pstat"]
for blk, h, c, w in events:
    ts = ts_of(blk)
    ci = ci_of(c)
    d = W.setdefault(w, {})
    d[ci] = max(d.get(ci, 0), ts // 60)
    if (h, c) in seen_tx:
        continue
    seen_tx.add((h, c))
    n = per_tx[(h, c)]
    if h not in txinfo:
        continue
    bnb = txinfo[h]
    st = pstat.setdefault(c, {"n": 0, "paid": 0, "paidn": 0, "bnb": 0.0, "tx": 0})
    st["tx"] += 1; st["n"] += n
    if bnb > 0:
        st["paid"] += 1; st["paidn"] += n; st["bnb"] = round(st["bnb"] + bnb, 8)
    feed.append({"t": ts, "w": w, "c": c, "k": n, "b": round(bnb, 6), "h": h})
feed = sorted(feed, key=lambda x: -x["t"])[:3000]
state["block"] = done_to

# ------------------------------------------------------------------ 官网目录
_BAD = ("x.com", "twitter.com", "bscscan.com", "sourcify.dev", "boltbook.fun", "binance.com", "t.me", "github.com",
        "agentidhub.eth.limo", "agentidhub.com", "dexscreener.com", "gmgn.ai", "opensea.io", "element.market",
        "google.com", "linktr.ee", "pancakeswap.finance", "ipfs.io", "arweave.net", "youtube.com", "medium.com",
        "hark.site", "hark.com", "bnbscan.com", "web3.bitget.com")


def ok_host(u):
    from urllib.parse import urlparse
    h = urlparse(u).netloc.lower().split(":")[0]
    return bool(h) and "." in h and not any(h == x or h.endswith("." + x) for x in _BAD)


def site_of(u):
    from urllib.parse import urlparse
    p = urlparse(u)
    if p.netloc.lower().endswith("github.io"):
        seg = [x for x in p.path.split("/") if x][:1]
        return f"{p.scheme}://{p.netloc}/" + (seg[0] + "/" if seg else "")
    return f"{p.scheme}://{p.netloc}/"


if time.time() - state.get("cat_t", 0) > 3000:
    try:
        s = S.get(CATALOG_URL, timeout=60).text
        j = s[s.index("{", s.index("AGENTID_PROJECT_CATALOG")):s.rstrip().rstrip(";").rfind("}") + 1]
        m = {}
        for p in json.loads(j)["projects"]:
            site = ""
            for so in p.get("sources", []):
                l = (so.get("label") or {}).get("zh", ""); u = so.get("url", "")
                if ("官网" in l or "铸造页" in l) and ok_host(u):
                    site = u; break
            if not site:
                for so in p.get("sources", []):
                    if ok_host(so.get("url", "")):
                        site = so["url"]; break
            if site:
                site = site_of(site)
            x = (p.get("officialX") or {}).get("url", "")
            for a in {a.lower() for a in re.findall(r"0x[a-fA-F0-9]{40}", json.dumps(p, ensure_ascii=False))}:
                if a not in m or (not m[a]["site"] and site):
                    m[a] = {"name": p.get("name", ""), "site": site, "x": x}
        if m:
            sites = m; state["cat_t"] = time.time()
            log(f"官网目录 {len(m)} 个合约")
    except Exception as e:
        log("官网目录更新失败：", str(e)[:100])

# ------------------------------------------------------------------ 每个项目的统计
P = {}
for w, d in W.items():
    for ci, t in d.items():
        x = P.get(ci)
        if x is None:
            P[ci] = x = [0, 0, 0]
        x[0] += 1
        if t:
            x[1] = min(x[1], t) if x[1] else t
            x[2] = max(x[2], t)

# 合约信息：名字、进度、价格、owner
SEL_SUPPLY = ["18160ddd", "a2309ff8", "75794a3c"]
SEL_MAX = ["d5abeb01", "32cb6b0c", "45c0f533", "355274ea", "f47c84c5", "e8315742", "902d55a5", "8f770ad0"]
SEL_PRICE = ["6817c76c", "a035b1fe", "c002d23d", "13faede6", "8d859f3e", "a945bf80", "13966db5", "d7bf81a3", "7b1b1de6", "f51f96dd", "9b6860c8"]
SEL_PER = ["453c2310", "0f2cdd6c", "b228d925", "29231c73", "0eb9e3cd", "17674c1f", "46668c09"]
SELS = ["06fdde03", "8da5cb5b"] + SEL_SUPPLY + SEL_MAX + SEL_PRICE + SEL_PER
info = state["info"]
now_min = int(time.time() // 60)
todo = [contracts[ci] for ci, v in P.items() if contracts[ci] not in info or now_min - v[2] < 3 * 1440]
todo.sort(key=lambda c: (c in info, -P[cidx[c]][2]))
todo = todo[:150]


def U(b):
    return int.from_bytes(b[:32], "big") if b and len(b) >= 32 else None


def read_infos(part):
    res = multicall([(c, "0x" + s) for c in part for s in SELS])
    out = {}
    for k, c in enumerate(part):
        r = res[k * len(SELS):(k + 1) * len(SELS)]
        if all(x is None for x in r):
            continue
        j = 2
        sup = next((U(x) for x in r[j:j + 3] if U(x) is not None), None); j += 3
        mx = next((U(x) for x in r[j:j + len(SEL_MAX)] if U(x) and U(x) < 10 ** 12), None); j += len(SEL_MAX)
        pr = next((U(x) for x in r[j:j + len(SEL_PRICE)] if U(x) is not None and U(x) < 10 ** 24), None); j += len(SEL_PRICE)
        per = next((U(x) for x in r[j:j + len(SEL_PER)] if U(x) and U(x) < 10 ** 6), None)
        ow = ("0x" + r[1][12:32].hex()) if r[1] and len(r[1]) >= 32 else ""
        if ow in ("0x" + "0" * 40, "0x000000000000000000000000000000000000dead"):
            ow = ""
        out[c] = {"nm": dec_str("0x" + r[0].hex())[:40] if r[0] else "", "o": ow, "s": sup, "m": mx, "p": pr, "pw": per}
    return out


if time.time() < DEADLINE - 30 and not os.environ.get("SKIP_INFO"):
    with cf.ThreadPoolExecutor(6) as ex:
        for out in ex.map(read_infos, [todo[i:i + 6] for i in range(0, len(todo), 6)]):
            info.update(out)
    log(f"读取合约信息 {len(todo)} 个")

# 官网里找推特和合约地址（给网页的「输入官网查」用）
xs = state.setdefault("xs", {})
sa = state.setdefault("sa", {})
XBAD = {"intent", "share", "home", "you", "i", "search", "hashtag", "login", "signup", "explore", "settings", "x", "twitter", "elonmusk", "binance", "bnbchain", "cz_binance"}


def scan_site(site):
    from urllib.parse import urljoin, urlparse
    texts = []
    try:
        r = S.get(site, timeout=12)
        html = r.text[:800000]; texts.append(html)
        host = urlparse(r.url).netloc
        for src in re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html)[:10]:
            u = urljoin(r.url, src)
            if urlparse(u).netloc == host:
                try:
                    texts.append(S.get(u, timeout=12).text[:3000000])
                except Exception:
                    pass
    except Exception:
        return site, None, None
    allt = "\n".join(texts)
    cnt = Counter(h for h in re.findall(r"https?://(?:www\.)?(?:x|twitter)\.com/(?:#!/)?([A-Za-z0-9_]{2,15})(?![A-Za-z0-9_])", allt) if h.lower() not in XBAD)
    x = ("https://x.com/" + cnt.most_common(1)[0][0]) if cnt else ""
    addrs = list(dict.fromkeys(a.lower() for a in re.findall(r"0x[a-fA-F0-9]{40}", allt)))[:40]
    return site, x, addrs


need = []
for ci, v in sorted(P.items(), key=lambda kv: -kv[1][2]):
    sv = sites.get(contracts[ci]) or {}
    if sv.get("site") and sv["site"] not in sa:
        need.append(sv["site"])
need = list(dict.fromkeys(need))[:40]
if need and time.time() < DEADLINE - 40 and not os.environ.get("SKIP_INFO"):
    with cf.ThreadPoolExecutor(8) as ex:
        for s0, x, addrs in ex.map(scan_site, need):
            if x is None:
                sa[s0] = {"a": [], "x": "", "fail": 1}
                continue
            xs[s0] = x
            sa[s0] = {"a": addrs, "x": x}
    log(f"扫描官网 {len(need)} 个")

# ------------------------------------------------------------------ 输出
def price_of(c):
    st = pstat.get(c)
    if st and st["tx"]:
        if st["paid"]:
            return "付费", round(st["bnb"] / max(1, st["paidn"]), 6), "实测"
        return "免费", 0, "实测"
    p = (info.get(c) or {}).get("p")
    if p is not None:
        return ("免费", 0, "合约") if p == 0 else ("付费", round(p / 1e18, 6), "合约")
    return "未知", None, ""


# ---- 开源检查（Sourcify v2）：已开源永久缓存；未开源 12 小时后复查；每轮最多 400 个，新项目优先
ver = state.get("ver", {})
def _sf(c):
    try:
        r = S.get(f"https://sourcify.dev/server/v2/contract/56/{c}", timeout=12)
        if r.status_code == 200:
            j = r.json()
            return c, 1 if (j.get("match") or j.get("runtimeMatch") or j.get("creationMatch")) else 0
        if r.status_code == 404:
            return c, 0
    except Exception:
        pass
    return c, None
_now = int(time.time())
_todo = [contracts[ci] for ci, v in sorted(P.items(), key=lambda kv: -kv[1][1])]
_todo = [c for c in _todo if not (ver.get(c) and (ver[c][0] == 1 or _now - ver[c][1] < 43200))][:400]
if _todo and time.time() < DEADLINE + 60:
    with cf.ThreadPoolExecutor(16) as ex:
        for c, v in ex.map(_sf, _todo):
            if v is not None:
                ver[c] = [v, _now]
    log(f"开源检查 {len(_todo)} 个，累计 {len(ver)}")
state["ver"] = ver

owners = Counter((info.get(contracts[ci]) or {}).get("o") for ci in P)
projects = []
for ci, v in P.items():
    c = contracts[ci]
    sv = sites.get(c) or {}
    inf = info.get(c) or {}
    fr, pr, src = price_of(c)
    sup, mx = inf.get("s"), inf.get("m")
    if sup is not None and sup > 10 ** 15:
        sup //= 10 ** 18
        if mx and mx > 10 ** 15:
            mx //= 10 ** 18
    ow = inf.get("o", None)
    site = sv.get("site", "")
    projects.append({"c": c, "n": sv.get("name") or inf.get("nm") or c[:10], "s": site,
                     "x": sv.get("x") or xs.get(site, ""), "w": v[0], "f": v[1], "l": v[2],
                     "fr": fr, "pr": pr, "ps": src, "su": sup, "mx": mx, "pw": inf.get("pw"),
                     "o": ow, "od": (owners[ow] - 1) if ow else None,
                     "v": (ver.get(c) or [None])[0]})
projects.sort(key=lambda x: -x["f"])
jsave("projects.json", projects)

nm_of = {c: p["n"] for c, p in [(p["c"], p) for p in projects]}
for e in feed:
    e["n"] = nm_of.get(e["c"], e.get("n", ""))
    e["wn"] = holders.get(e["w"], "")
jsave("feed.json", feed)

top = sorted(W.items(), key=lambda kv: -len(kv[1]))[:100]
jsave("topw.json", [{"w": w, "n": holders.get(w, ""), "k": len(d), "l": max(d.values()) if d else 0,
                     "p": [nm_of.get(contracts[ci], contracts[ci][:10]) for ci, _ in sorted(d.items(), key=lambda kv: -kv[1])[:6]]}
                    for w, d in top])

shards = {}
for w, d in W.items():
    shards.setdefault(w[:2], {})[w] = [x for ci, t in d.items() for x in (ci, t)]
for k in range(256):
    jsave(f"w/{k:02x}.json", shards.get(f"{k:02x}", {}))
jsave("contracts.json", contracts)
jsave("siteaddr.json", {k: {"a": v.get("a", []), "x": v.get("x", "")} for k, v in sa.items()})
jsave("sites.json", sites)
with open(os.path.join(D, "holders.txt.tmp"), "w", encoding="utf-8") as f:
    for a, n in holders.items():
        f.write(f"{a} {n}\n")
os.replace(os.path.join(D, "holders.txt.tmp"), os.path.join(D, "holders.txt"))
state["pstat"] = pstat; state["info"] = info
jsave("state.json", state)
jsave("meta.json", {"t": int(time.time()), "block": state["block"], "wallets": len(holders),
                    "minters": len(W), "projects": len(projects)})
log(f"完成：钱包 {len(holders):,}，项目 {len(projects)}，最新区块 {state['block']:,}")
