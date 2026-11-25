# -*- coding: utf-8 -*-
"""
astrbot_plugin_activity_watcher
用户活动追踪 + 记忆沉淀 + 时效性主动关心
配合 livingmemory / proactive_chat / private_companion 使用（可选）
"""
import asyncio
import json
import os
import random
import re
import sqlite3
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional

from astrbot.api.star import Context, Star, register
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.event.filter import EventMessageType, PermissionType
from astrbot.api.message_components import Plain
from astrbot.core.message.message_event_result import MessageChain

DATABASE_DIR = os.path.join("data", "plugin_data", "activity_watcher")
DB_PATH = os.path.join(DATABASE_DIR, "activity.db")


def _norm(text: str) -> str:
    """归一化文本用于复读合并"""
    if not text:
        return ""
    t = re.sub(r"\s+", "", text)
    # 全角转半角的常见标点/字母数字
    table = {ord(f): ord(t) for f, t in zip(
        "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")}
    return t.translate(table)


@register("astrbot_plugin_activity_watcher", "LazyTsukiko",
          "用户活动追踪：跨群/私聊活动感知、每日记忆沉淀、时效性主动关心与话题由头",
          "1.0.0", "https://github.com/LazyTsukiko/astrbot_plugin_activity_watcher")
class ActivityWatcher(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.cfg = config or {}
        self._db: Optional[sqlite3.Connection] = None
        self._db_lock = asyncio.Lock()
        self._bg_tasks: set[asyncio.Task] = set()
        self._inject_throttle: dict[str, float] = {}  # umo -> last inject ts
        self._semantic_buffer: dict[str, list] = {}  # uid -> [ {text, umo, at} ]
        self._companion_api = None
        self._ready = False

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def initialize(self):
        os.makedirs(DATABASE_DIR, exist_ok=True)
        self._db = sqlite3.connect(DB_PATH, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._init_tables()
        self._load_companion_api()
        self._spawn(self._daily_summary_loop())
        if self._cfg("semantic_care_detection", True):
            self._spawn(self._semantic_flush_loop())
        if self._cfg("enable_trigger_engine", True):
            self._spawn(self._trigger_loop())
        self._ready = True
        logger = getattr(self.context, "logger", None) or print
        logger("[activity_watcher] 初始化完成")

    async def terminate(self):
        self._ready = False
        for t in list(self._bg_tasks):
            t.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()
        if self._db:
            self._db.close()
            self._db = None
        # 注销 companion provider
        if getattr(self, "_companion_token", None) and self._companion_api:
            try:
                self._companion_api.unregister_memory_provider(self._companion_token)
            except Exception:
                pass

    def _spawn(self, coro):
        task = asyncio.get_event_loop().create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    def _cfg(self, key, default=None):
        v = self.cfg.get(key, default)
        return default if v is None else v

    # ------------------------------------------------------------------
    # 数据库
    # ------------------------------------------------------------------
    def _init_tables(self):
        cur = self._db.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            umo TEXT NOT NULL,
            is_private INTEGER NOT NULL,
            platform TEXT, group_id TEXT,
            sender_name TEXT,
            text TEXT,
            at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_user_time ON activities(user_id, at);
        CREATE INDEX IF NOT EXISTS idx_umo_time ON activities(umo, at);
        CREATE TABLE IF NOT EXISTS summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT, date TEXT NOT NULL,
            summary TEXT NOT NULL,
            pushed_lmem INTEGER DEFAULT 0,
            created_at REAL
        );
        CREATE TABLE IF NOT EXISTS care_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            detail TEXT,
            umo TEXT,
            urgency TEXT NOT NULL,
            expires_at REAL,
            status TEXT NOT NULL DEFAULT 'pending',
            last_triggered_at REAL,
            trigger_count INTEGER DEFAULT 0,
            next_retry_at REAL,
            created_at REAL
        );
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            summary TEXT NOT NULL,
            opened_at REAL, last_touched_at REAL,
            status TEXT NOT NULL DEFAULT 'open',
            close_reason TEXT
        );
        """)
        self._db.commit()

    def _exec(self, sql, params=(), fetch=False):
        cur = self._db.execute(sql, params)
        self._db.commit()
        if fetch:
            return cur.fetchall()
        return cur.lastrowid

    # ------------------------------------------------------------------
    # 消息采集
    # ------------------------------------------------------------------
    @filter.event_message_type(EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        if not self._ready:
            return
        message = event.message_obj if hasattr(event, "message_obj") else None
        if message is None:
            return
        # 跳过指令
        raw = (getattr(message, "raw_message", "") or "")
        message_str = (getattr(message, "message_str", "") or "").strip()
        if not message_str or message_str.startswith(("/", "!", "！")):
            return
        # 跳过机器人自身
        sender = event.get_sender_id()
        self_id = event.get_self_id()
        if sender and sender == self_id:
            return
        uid = f"{event.get_platform_name()}:{sender}"
        is_private = event.is_private_chat() if hasattr(event, "is_private_chat") \
            else not bool(event.get_group_id())
        self._exec(
            "INSERT INTO activities(user_id,umo,is_private,platform,group_id,sender_name,text,at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (uid, event.unified_msg_origin, int(is_private), event.get_platform_name(),
             event.get_group_id(), event.get_sender_name(), message_str[:self._cfg("record_max_text_len", 500)],
             time.time()))
        # 滚动清理
        if random.random() < 0.02:
            cutoff = time.time() - self._cfg("retention_days", 3) * 86400
            self._exec("DELETE FROM activities WHERE at < ?", (cutoff,))
        # 目标用户检测（关心事件 + 话题收口）
        if self._is_focus(uid):
            self._detect_care(event, uid, message_str, is_private)
            self._check_topic_close(uid, message_str)

    def _is_semantic_target(self, event, uid: str, is_private: bool) -> bool:
        """语义检测只针对目标用户，控制成本：
        focus_users 非空 → 仅白名单用户；为空 → 仅私聊对端用户"""
        if not self._cfg("semantic_care_detection", True):
            return False
        focus = self._cfg("focus_users", []) or []
        if focus:
            return uid in focus or any(uid.endswith(f":{u}") for u in focus)
        return is_private

    async def _semantic_check(self, event, uid: str, text: str):
        """将目标用户消息入队批量语义缓冲（时效要求不高，攒批降低调用成本）"""
        buf = self._semantic_buffer.setdefault(uid, [])
        buf.append({"text": text, "umo": getattr(event, "unified_msg_origin", ""),
                    "at": time.time()})
        if len(buf) >= self._cfg("semantic_batch_max", 10):
            await self._flush_semantic(uid)

    async def _flush_semantic(self, uid: str):
        """对缓冲的连续多条消息做一次批量语义判断（1 次调用覆盖整批）"""
        buf = self._semantic_buffer.pop(uid, None)
        if not buf:
            return
        try:
            topics = self._exec(
                "SELECT id,kind,summary FROM topics WHERE user_id=? AND status='open'",
                (uid,), fetch=True)
            topic_desc = "\n".join(
                f"  #{t['id']} [{t['kind']}] {t['summary']}" for t in topics) or "  （无）"
            msgs = "\n".join(f"{i+1}. {m['text'][:150]}"
                             for i, m in enumerate(buf[-self._cfg("semantic_batch_max", 10):]))
            prompt = (
                "以下是某用户的一组连续消息（按时间顺序），请整体分析后输出 JSON（不要其他文字）：\n"
                '{"care": "health"或"none", "detail": "若care为health，用不超过30字概括其状况，否则留空", '
                '"close_topic_ids": [已自然结束的话题编号数组]}\n'
                "care=health 的标准：消息中表达身体不适、生病、就医、情绪低落需要关心"
                "（不含关键词也能识别，如「头有点晕」「去看了医生」）。\n"
                "close_topic_ids 的标准：用户明确表达某话题已好转/结束/不再需要关心。\n\n"
                f"进行中话题：\n{topic_desc}\n\n用户消息组：\n{msgs}")
            ans = await self._llm_text(prompt)
            if not ans:
                return
            m = re.search(r"\{.*\}", ans, re.S)
            if not m:
                return
            data = json.loads(m.group())
            # 建/复用健康事件
            if data.get("care") == "health":
                existing = self._exec(
                    "SELECT id FROM care_events WHERE user_id=? AND kind='health'"
                    " AND status IN ('pending','delivered') LIMIT 1", (uid,), fetch=True)
                if not existing:
                    detail = data.get("detail") or buf[-1]["text"][:50]
                    self._exec(
                        "INSERT INTO care_events(user_id,kind,detail,umo,urgency,expires_at,"
                        "next_retry_at,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (uid, "health", f"语义识别：{detail}",
                         buf[-1]["umo"], "normal", None, time.time(), time.time()))
                    self._ensure_topic(uid, "health", detail)
            # 语义收口
            close_ids = data.get("close_topic_ids") or []
            if close_ids:
                for t in topics:
                    if t["id"] in close_ids:
                        self._close_topic(t["id"], "user_confirmed",
                                          f"语义识别话题结束：{t['summary']}")
        except Exception:
            pass

    async def _semantic_flush_loop(self):
        """定时冲刷语义缓冲，保证攒批消息最终都会被判断"""
        interval = self._cfg("semantic_batch_interval", 180)
        while True:
            try:
                await asyncio.sleep(interval)
                for uid in list(self._semantic_buffer.keys()):
                    await self._flush_semantic(uid)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    def _is_focus(self, uid: str) -> bool:
        focus = self._cfg("focus_users", []) or []
        if not focus:
            return True  # 空白名单=追踪所有人
        return uid in focus or any(uid.endswith(f":{u}") for u in focus)

    # ------------------------------------------------------------------
    # 关心事件检测
    # ------------------------------------------------------------------
    def _detect_care(self, event, uid: str, text: str, is_private: bool = False):
        now = time.time()
        hit = False
        # 健康关键词（零成本硬匹配优先）
        keywords = self._cfg("care_keywords", []) or []
        for kw in keywords:
            if kw and kw in text:
                hit = True
                # 去重：同用户同 kind 存在未关闭事件则不新建
                row = self._exec(
                    "SELECT id FROM care_events WHERE user_id=? AND kind='health'"
                    " AND status IN ('pending','delivered') LIMIT 1", (uid,), fetch=True)
                if not row:
                    eid = self._exec(
                        "INSERT INTO care_events(user_id,kind,detail,umo,urgency,expires_at,"
                        "next_retry_at,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (uid, "health", f"提到“{kw}”：{text[:80]}",
                         event.unified_msg_origin, "normal", None, now, now))
                    self._ensure_topic(uid, "health", f"身体不适（{text[:50]}）")
                break
        if not hit and self._is_semantic_target(event, uid, is_private):
            # 目标用户的轻量语义判断（限流 + 后台执行，不阻塞消息管道）
            self._spawn(self._semantic_check(event, uid, text))
        # 深夜活跃
        hours = self._cfg("late_night_hours", "0-5")
        try:
            h0, h1 = [int(x) for x in str(hours).split("-")]
        except Exception:
            h0, h1 = 0, 5
        cur_h = datetime.now().hour
        in_late = (cur_h >= h0 and cur_h < h1) if h0 <= h1 else (cur_h >= h0 or cur_h < h1)
        if in_late:
            day_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            row = self._exec(
                "SELECT id FROM care_events WHERE user_id=? AND kind='late_night' AND created_at>=?"
                " LIMIT 1", (uid, day_start), fetch=True)
            if not row:
                self._exec(
                    "INSERT INTO care_events(user_id,kind,detail,umo,urgency,expires_at,"
                    "next_retry_at,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (uid, "late_night", f"深夜 {cur_h} 点仍在活动",
                     event.unified_msg_origin, "urgent",
                     now + self._cfg("urgent_expire_minutes", 30) * 60, now, now))
                self._ensure_topic(uid, "late_night", "深夜未休息")

    def _ensure_topic(self, uid: str, kind: str, summary: str):
        row = self._exec(
            "SELECT id FROM topics WHERE user_id=? AND kind=? AND status='open' LIMIT 1",
            (uid, kind), fetch=True)
        if not row:
            now = time.time()
            self._exec(
                "INSERT INTO topics(user_id,kind,summary,opened_at,last_touched_at,status)"
                " VALUES(?,?,?,?,?,'open')", (uid, kind, summary, now, now))

    def _check_topic_close(self, uid: str, text: str):
        keywords = self._cfg("close_keywords", []) or []
        for kw in keywords:
            if kw and kw in text:
                rows = self._exec(
                    "SELECT id,kind,summary FROM topics WHERE user_id=? AND status='open'",
                    (uid,), fetch=True)
                for r in rows:
                    self._close_topic(r["id"], "user_confirmed",
                                      f"用户说“{kw}”，话题自然结束")
                break

    def _close_topic(self, topic_id, reason, note=""):
        row = self._exec("SELECT * FROM topics WHERE id=?", (topic_id,), fetch=True)
        if not row:
            return
        now = time.time()
        self._exec("UPDATE topics SET status='closed', close_reason=?, last_touched_at=? WHERE id=?",
                   (reason, now, topic_id))
        self._exec("UPDATE care_events SET status='closed' WHERE user_id=? AND kind=? AND status!='closed'",
                   (row["user_id"], row["kind"]))
        self._save_summary(row["user_id"], f"【话题收口】{row['summary']} — {note or reason}")

    # ------------------------------------------------------------------
    # 每日摘要
    # ------------------------------------------------------------------
    async def _daily_summary_loop(self):
        while True:
            try:
                cron = self._cfg("summarize_cron", "04:30")
                h, m = [int(x) for x in str(cron).split(":")]
                now = datetime.now()
                target = now.replace(hour=h, minute=m, second=0, microsecond=0)
                if target <= now:
                    target += timedelta(days=1)
                await asyncio.sleep((target - now).total_seconds() + random.uniform(0, 60))
                await self._run_daily_summary()
                await asyncio.sleep(86400)  # 之后逐日
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(600)

    async def _run_daily_summary(self):
        date_str = datetime.now().strftime("%Y-%m-%d")
        day_start = datetime.now().replace(hour=0, minute=0, second=0,
                                           microsecond=0).timestamp()
        rows = self._exec(
            "SELECT user_id, COUNT(*) c, MIN(at) t0, MAX(at) t1, SUM(is_private) pv"
            " FROM activities WHERE at>=? GROUP BY user_id HAVING c>0", (day_start,), fetch=True)
        if not rows:
            return
        # 每个目标用户生成摘要（最多处理 20 人，防开销）
        for r in rows[:20]:
            uid = r["user_id"]
            texts = self._exec(
                "SELECT text FROM activities WHERE user_id=? AND at>=? ORDER BY at LIMIT 80",
                (uid, day_start), fetch=True)
            joined = "\n".join(x["text"] for x in texts)
            prompt = (
                "以下是某用户今天的消息记录。分析后输出 JSON（不要其他文字）：\n"
                '{"summary": "2-4句总结值得记住的重要信息（健康、情绪、重要事件、约定等），忽略灌水", '
                '"interests": ["可选话题由头数组，0-2条：值得后续主动聊聊的兴趣点/'
                '新鲜事/未聊完的话头，如「今天去了爬山」「提到想看某部电影」；'
                '没有值得主动提的就给空数组"]}\n\n' + joined)
            ans = await self._llm_text(prompt)
            if not ans:
                continue
            summary, interests = None, []
            try:
                data = json.loads(re.search(r"\{.*\}", ans, re.S).group())
                summary = data.get("summary")
                interests = [i for i in (data.get("interests") or []) if isinstance(i, str)][:2]
            except Exception:
                summary = ans.strip()  # 兼容非 JSON 输出
            if not summary:
                continue
            # 兴趣类话题：放宽时间，低频触达 1-2 次后收口
            if interests:
                self._create_interest_events(uid, interests)
            # 顺带 LLM 复核话题收口
            await self._review_open_topics(uid, summary)
            self._save_summary(uid, summary)
            if self._cfg("push_to_livingmemory", True):
                await self._push_livingmemory(uid, summary)

    def _save_summary(self, uid, summary):
        self._exec("INSERT INTO summaries(user_id,date,summary,pushed_lmem,created_at) VALUES(?,?,?,0,?)",
                   (uid, datetime.now().strftime("%Y-%m-%d"), summary, time.time()))

    def _create_interest_events(self, uid: str, interests: list[str]):
        """每日摘要提炼的兴趣由头 → interest 话题 + 低频触达事件。
        限制：每用户每天最多 1 个新兴趣事件，同时 open 的兴趣话题不超过 2 个。"""
        now = time.time()
        day_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        made_today = self._exec(
            "SELECT COUNT(*) c FROM care_events WHERE user_id=? AND kind='interest' AND created_at>=?",
            (uid, day_start), fetch=True)[0]["c"]
        if made_today >= 1:
            return
        open_interest = self._exec(
            "SELECT COUNT(*) c FROM topics WHERE user_id=? AND kind='interest' AND status='open'",
            (uid,), fetch=True)[0]["c"]
        if open_interest >= 2:
            return
        topic = interests[0][:60]
        self._ensure_topic_kind(uid, "interest", topic)
        # 兴趣类只在私聊触达：无私聊记录则不建事件（话题仍 open，供注入/工具使用）
        row = self._exec(
            "SELECT umo FROM activities WHERE user_id=? AND is_private=1"
            " ORDER BY at DESC LIMIT 1", (uid,), fetch=True)
        if not row:
            return
        umo = row["umo"]
        # 放宽：延迟到下一个允许时段（默认 6 小时后），由触发引擎再经 quiet_hours 修正
        self._exec(
            "INSERT INTO care_events(user_id,kind,detail,umo,urgency,expires_at,"
            "next_retry_at,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (uid, "interest", f"可聊的兴趣点：{topic}",
             umo, "normal", None, now + self._cfg("interest_start_delay_hours", 6) * 3600, now))

    def _ensure_topic_kind(self, uid: str, kind: str, summary: str):
        row = self._exec(
            "SELECT id FROM topics WHERE user_id=? AND kind=? AND status='open' LIMIT 1",
            (uid, kind), fetch=True)
        if not row:
            now = time.time()
            self._exec(
                "INSERT INTO topics(user_id,kind,summary,opened_at,last_touched_at,status)"
                " VALUES(?,?,?,?,?,'open')", (uid, kind, summary, now, now))

    async def _review_open_topics(self, uid, daily_summary):
        rows = self._exec("SELECT id,kind,summary FROM topics WHERE user_id=? AND status='open'",
                          (uid,), fetch=True)
        if not rows:
            return
        desc = "\n".join(f"- [{r['kind']}] {r['summary']}" for r in rows)
        ans = await self._llm_text(
            f"用户近况摘要：{daily_summary}\n\n当前进行中的关心话题：\n{desc}\n\n"
            "判断哪些话题已经自然结束（如病已好转、话题已聊完）。"
            "只输出已结束话题的编号列表（JSON数组，如 [1,2]），若都未结束输出 []：")
        if not ans:
            return
        try:
            ids = json.loads(re.search(r"\[.*?\]", ans, re.S).group())
            for r in rows:
                if r["id"] in ids:
                    self._close_topic(r["id"], "user_confirmed", "每日复核判定话题已结束")
        except Exception:
            pass

    async def _llm_text(self, prompt: str) -> Optional[str]:
        """provider 直调（不经主链，不触发其他插件钩子）"""
        try:
            provider = self.context.get_using_provider()
            if provider is None:
                return None
            resp = await provider.text_chat(prompt=prompt, contexts=[])
            return getattr(resp, "completion_text", None) or None
        except Exception:
            return None

    async def _push_livingmemory(self, uid, summary):
        if not self._cfg("livingmemory_api_key", ""):
            return
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                await s.post(
                    self._cfg("livingmemory_api", "http://127.0.0.1:7801") + "/memories",
                    json={"content": f"[活动观察] {summary}", "user_id": uid, "source": "activity_watcher"},
                    headers={"Authorization": f"Bearer {self._cfg('livingmemory_api_key', '')}"},
                    timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 触发引擎
    # ------------------------------------------------------------------
    async def _trigger_loop(self):
        while True:
            try:
                await asyncio.sleep(self._cfg("trigger_check_interval", 300))
                await self._check_triggers()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    async def _check_triggers(self):
        now = time.time()
        # 过期的 urgent 作废
        self._exec(
            "UPDATE care_events SET status='closed' WHERE urgency='urgent'"
            " AND status='pending' AND expires_at IS NOT NULL AND expires_at<?", (now,))
        rows = self._exec(
            "SELECT * FROM care_events WHERE status IN ('pending','delivered')"
            " AND next_retry_at<=? AND urgency='normal'", (now,), fetch=True)
        max_triggers = self._cfg("max_repeat_triggers", 7)
        for ev in rows:
            # delivered 事件只在话题仍 open 时按周期重复；超上限则收口
            if ev["status"] == "delivered":
                if ev["trigger_count"] >= self._max_triggers(ev["kind"]):
                    self._exec("UPDATE care_events SET status='closed' WHERE id=?", (ev["id"],))
                    continue
                t = self._exec(
                    "SELECT id FROM topics WHERE user_id=? AND kind=? AND status='open'",
                    (ev["user_id"], ev["kind"]), fetch=True)
                if not t:
                    # 话题已收口，事件随之关闭
                    self._exec("UPDATE care_events SET status='closed' WHERE id=?", (ev["id"],))
                    continue
            # 主动触达一律走私聊：无私聊记录则不发送，事件直接关闭
            priv = self._private_umo(ev["user_id"])
            if not priv:
                self._exec("UPDATE care_events SET status='closed' WHERE id=?", (ev["id"],))
                continue
            umo = priv
            if not self._quiet_ok(ev):
                # 非深夜事件在免打扰时段顺延 1 小时
                if ev["urgency"] != "urgent":
                    self._exec("UPDATE care_events SET next_retry_at=? WHERE id=?",
                               (now + 3600, ev["id"]))
                    continue
            ok = await self._deliver(ev)
            if ok:
                retry = self._retry_interval(ev["kind"])
                self._exec(
                    "UPDATE care_events SET status='delivered',last_triggered_at=?,"
                    "trigger_count=trigger_count+1,next_retry_at=? WHERE id=?",
                    (now, now + retry, ev["id"]))
                if ev["kind"] == "health":
                    # 重复触达时顺带更新话题活跃时间，支撑 auto_close_days 判定
                    self._exec(
                        "UPDATE topics SET last_touched_at=? WHERE user_id=? AND kind=? AND status='open'",
                        (now, ev["user_id"], ev["kind"]))
            else:
                self._exec("UPDATE care_events SET next_retry_at=? WHERE id=?",
                           (now + self._cfg("trigger_check_interval", 300), ev["id"]))

    def _private_umo(self, uid: str) -> Optional[str]:
        """取该用户私聊会话 umo：优先用记录到的真实私聊 umo（最可靠），
        否则由 platform:FriendMessage:用户ID 直接构造。
        返回 None 表示平台不支持私聊直发。"""
        row = self._exec(
            "SELECT umo FROM activities WHERE user_id=? AND is_private=1"
            " ORDER BY at DESC LIMIT 1", (uid,), fetch=True)
        if row:
            return row["umo"]
        try:
            from astrbot.core.platform.message_type import MessageType
            platform, _, raw_id = uid.partition(":")
            if not raw_id:
                return None
            return f"{platform}:{MessageType.FRIEND_MESSAGE.value}:{raw_id}"
        except Exception:
            return None

    def _quiet_ok(self, ev) -> bool:
        if ev["kind"] == "late_night":
            return True  # 深夜关心豁免免打扰
        try:
            h0, h1 = [int(x) for x in str(self._cfg("quiet_hours", "1-8")).split("-")]
        except Exception:
            return True
        cur_h = datetime.now().hour
        in_quiet = (cur_h >= h0 and cur_h < h1) if h0 <= h1 else (cur_h >= h0 or cur_h < h1)
        return not in_quiet

    def _retry_interval(self, kind) -> int:
        if kind == "health":
            return self._cfg("health_retry_hours", 24) * 3600
        if kind == "interest":
            return self._cfg("interest_retry_hours", 12) * 3600
        return 86400

    def _max_triggers(self, kind) -> int:
        if kind == "interest":
            return 2  # 设计约定：兴趣由头触达 1-2 次后收口
        return self._cfg("max_repeat_triggers", 7)

    async def _deliver(self, ev) -> bool:
        """触达：agent 模式（主链生成，保人设）→ 兜底 provider 直调"""
        text = await self._generate_greeting(ev)
        if not text:
            return False
        try:
            ok = await self.context.send_message(
                ev["umo"], MessageChain(chain=[Plain(text)]))
            return bool(ok is not False)
        except Exception:
            return False

    async def _generate_greeting(self, ev) -> Optional[str]:
        ctx_snippet = self._render_activities(
            self._exec("SELECT * FROM activities WHERE user_id=? ORDER BY at DESC LIMIT 5",
                       (ev["user_id"],), fetch=True), with_context=False)
        prompt = (
            f"你是用户的陪伴者（人设基调：{self._cfg('persona_hint', '温柔、自然、不过度打扰')}）。\n"
            f"用户近况：{ev['detail']}。\n近期活动：\n{ctx_snippet}\n"
            f"请写一句自然、简短的问候/关心（不超过 60 字），直接输出内容，不要解释：")
        return await self._llm_text(prompt)

    # ------------------------------------------------------------------
    # 渲染（含复读合并）
    # ------------------------------------------------------------------
    def _merge_repeats(self, items: list[dict]) -> list[dict]:
        """相邻同人同文本合并为 text ×N"""
        out = []
        for it in items:
            key = (_norm(it.get("sender_name") or it.get("user_id", "")), _norm(it.get("text", "")))
            if out and _norm(out[-1].get("sender_name") or out[-1].get("user_id", "")) == key[0] \
                    and _norm(out[-1].get("text", "")) == key[1]:
                out[-1]["_repeat"] = out[-1].get("_repeat", 1) + 1
            else:
                out.append(dict(it))
        for it in out:
            if it.get("_repeat", 1) > 1:
                it["text"] = f"{it['text']} ×{it['_repeat']}"
        return out

    def _render_activities(self, rows, with_context=False) -> str:
        items = []
        for r in rows:
            items.append({"user_id": r["user_id"], "sender_name": r["sender_name"] or "",
                          "text": r["text"], "at": r["at"], "umo": r["umo"],
                          "is_private": r["is_private"]})
        items = self._merge_repeats(items)
        lines = []
        for it in items:
            t = datetime.fromtimestamp(it["at"]).strftime("%m-%d %H:%M")
            place = "私聊" if it.get("is_private") else (it.get("umo", "").split(":")[-1] or "群聊")
            name = it.get("sender_name") or it.get("user_id")
            line = f"[{t}][{place}] {name}: {it['text']}"
            if with_context:
                line += "\n" + self._render_context(it)
            lines.append(line)
        return "\n".join(lines)

    def _render_context(self, item, n: Optional[int] = None) -> str:
        n = n or self._cfg("context_window", 5)
        rows = self._exec(
            "SELECT * FROM activities WHERE umo=? AND at<=? ORDER BY at DESC LIMIT ?",
            (item["umo"], item["at"], n + 1), fetch=True)
        before = [r for r in reversed(rows) if r["at"] < item["at"]][-n:]
        rows_after = self._exec(
            "SELECT * FROM activities WHERE umo=? AND at>? ORDER BY at LIMIT ?",
            (item["umo"], item["at"], n), fetch=True)
        target_norm = _norm(item["text"])
        lines = ["  —— 上下文 ——"]
        for r in before + rows_after:
            if _norm(r["text"]) == target_norm:
                lines.append(f"  {r['sender_name'] or r['user_id']}: (重复)")
            else:
                lines.append(f"  {r['sender_name'] or r['user_id']}: {r['text']}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # prompt 注入
    # ------------------------------------------------------------------
    @filter.on_llm_request()
    async def inject_activity(self, event, req):
        if not self._cfg("enable_prompt_inject", True) or not self._ready:
            return
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            return
        now = time.time()
        if now - self._inject_throttle.get(umo, 0) < 600:
            return
        self._inject_throttle[umo] = now
        try:
            uid = f"{event.get_platform_name()}:{event.get_sender_id()}"
            stat_h = self._cfg("inject_stat_hours", 24)
            cutoff = now - stat_h * 3600
            stat = self._exec(
                "SELECT COUNT(*) c FROM activities WHERE user_id=? AND at>=?", (uid, cutoff),
                fetch=True)
            recent = self._exec(
                "SELECT * FROM activities WHERE user_id=? ORDER BY at DESC LIMIT ?",
                (uid, self._cfg("inject_max_recent_items", 10)), fetch=True)
            if not recent:
                return
            topics = self._exec(
                "SELECT kind,summary FROM topics WHERE user_id=? AND status='open'", (uid,),
                fetch=True)
            last_summary = self._exec(
                "SELECT summary FROM summaries WHERE user_id=? ORDER BY created_at DESC LIMIT 1",
                (uid,), fetch=True)
            parts = ["[近期活动观察 - 客观数据，仅供理解语境，不要复述此格式]",
                     f"（过去{stat_h}小时该用户发言 {stat[0]['c'] if stat else 0} 条）"]
            parts.append(self._render_activities(recent, with_context=False))
            if last_summary:
                parts.append(f"[昨日摘要] {last_summary[0]['summary']}")
            if topics:
                parts.append("[进行中的关心话题（可自然提及，勿生硬）] " +
                             "；".join(f"{t['kind']}: {t['summary']}" for t in topics))
            block = "\n".join(parts)
            req.system_prompt = (getattr(req, "system_prompt", "") or "") + "\n\n" + block
        except Exception:
            pass

    # ------------------------------------------------------------------
    # llm_tool
    # ------------------------------------------------------------------
    @filter.llm_tool(name="query_user_activity")
    async def query_user_activity(self, event: AstrMessageEvent,
                                  user_id: Optional[str] = None,
                                  hours: int = 72, limit: int = 20,
                                  with_context: bool = True):
        """查询某位用户近期的活动记录（发言时间、地点、内容，可带上下文语境）。
        Args:
            user_id: 目标用户的完整ID（platform:数字号）。不填则查询当前会话用户
            hours: 查询最近多少小时的活动，默认72
            limit: 最多返回多少条，默认20
            with_context: 是否附带每条发言前后的对话上下文，默认true
        """
        uid = user_id or f"{event.get_platform_name()}:{event.get_sender_id()}"
        rows = self._exec(
            "SELECT * FROM activities WHERE user_id=? AND at>=? ORDER BY at DESC LIMIT ?",
            (uid, time.time() - hours * 3600, max(1, min(limit, 50))), fetch=True)
        if not rows:
            yield event.plain_result(f"未查询到 {uid} 近 {hours} 小时的活动记录。")
            return
        yield event.plain_result(self._render_activities(rows, with_context=bool(with_context)))

    @filter.llm_tool(name="suggest_topic")
    async def suggest_topic(self, event: AstrMessageEvent, user_id: Optional[str] = None):
        """为主动会话寻找话题由头：基于目标用户未关闭的关心话题和待处理事件，给出话题建议。
        Args:
            user_id: 目标用户的完整ID。不填则查询当前会话用户
        """
        uid = user_id or f"{event.get_platform_name()}:{event.get_sender_id()}"
        topics = self._exec(
            "SELECT kind,summary,last_touched_at FROM topics WHERE user_id=? AND status='open'",
            (uid,), fetch=True)
        evs = self._exec(
            "SELECT kind,detail,urgency,created_at FROM care_events"
            " WHERE user_id=? AND status IN ('pending','delivered') ORDER BY created_at DESC LIMIT 5",
            (uid,), fetch=True)
        if not topics and not evs:
            yield event.plain_result("该用户近期暂无值得主动提及的话题。")
            return
        lines = ["可使用的话题由头（按建议优先级）："]
        for ev in evs:
            tag = "【时效性强，宜尽快提及】" if ev["urgency"] == "urgent" else ""
            lines.append(f"- {tag}{ev['kind']}：{ev['detail']}")
        for t in topics:
            lines.append(f"- 持续性话题（{t['kind']}）：{t['summary']}")
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # 管理指令
    # ------------------------------------------------------------------
    @filter.command("aw_status")
    async def cmd_status(self, event: AstrMessageEvent):
        """查看活动追踪状态"""
        uid = f"{event.get_platform_name()}:{event.get_sender_id()}"
        acts = self._exec("SELECT COUNT(*) c FROM activities WHERE user_id=?", (uid,), fetch=True)[0]["c"]
        open_topics = self._exec(
            "SELECT COUNT(*) c FROM topics WHERE user_id=? AND status='open'", (uid,), fetch=True)[0]["c"]
        pending = self._exec(
            "SELECT COUNT(*) c FROM care_events WHERE user_id=? AND status='pending'",
            (uid,), fetch=True)[0]["c"]
        yield event.plain_result(
            f"活动追踪：已记录 {acts} 条活动，{open_topics} 个进行中话题，{pending} 个待触达事件。")

    @filter.permission_type(PermissionType.ADMIN)
    @filter.command("aw_close")
    async def cmd_close(self, event: AstrMessageEvent, user_id: Optional[str] = None):
        """手动关闭某用户的话题。用法: /aw_close <platform:uid>"""
        uid = user_id or f"{event.get_platform_name()}:{event.get_sender_id()}"
        rows = self._exec("SELECT id FROM topics WHERE user_id=? AND status='open'", (uid,), fetch=True)
        for r in rows:
            self._close_topic(r["id"], "manual")
        yield event.plain_result(f"已关闭 {uid} 的 {len(rows)} 个话题。")

    # ------------------------------------------------------------------
    # private_companion 集成
    # ------------------------------------------------------------------
    def _load_companion_api(self):
        if not self._cfg("enable_companion_provider", True):
            return
        try:
            for star in self.context.get_all_stars():
                if getattr(star, "root_dir_name", "") == "astrbot_plugin_private_companion" \
                        and getattr(star, "activated", False):
                    api = getattr(star.star_cls, "extension_api", None)
                    if api and hasattr(api, "register_memory_provider"):
                        self._companion_api = api
                        self._companion_token = api.register_memory_provider(
                            "activity_watcher", plugin=self,
                            read=self._companion_read,
                            write=self._companion_write,
                            summary=self._companion_summary,
                            priority=5)
                    break
        except Exception:
            pass

    async def _companion_read(self, request):
        rows = self._exec(
            "SELECT date, summary FROM summaries ORDER BY created_at DESC LIMIT 5", fetch=True)
        return {"sections": [
            {"title": "近期活动观察", "content": "\n".join(r["summary"] for r in rows)}]}

    async def _companion_write(self, record):
        return True  # 只接收副本，不落库

    async def _companion_summary(self):
        return "活动观察摘要若干条"

    def _companion_is_active(self):
        return self._ready


# 兼容部分 AstrBot 版本的 logger 导入
try:
    from astrbot.api import logger  # noqa: E402
except Exception:
    import logging
    logger = logging.getLogger("activity_watcher")
