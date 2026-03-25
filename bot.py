"""
Хаус Мастер — Telegram Bot v3.0

+ SQLite база данных
+ Два типа заявок: разовый ремонт и регулярное обслуживание
+ Роль исполнителя (мастера) — принимает/отказывается от заявок
+ Распределение заявок между мастерами (кто первый принял)
+ Статусы заявок с уведомлением клиенту
+ Геолокация — клиент отправляет точку на карте
+ Выбор города — масштабирование по России
+ Выбор удобного времени визита мастера
+ Оплата через ЮКассу (Telegram Payments)
+ Фото до/после от мастера — автоматически клиенту
+ Отзыв после выполнения
+ Рассылка всем пользователям
+ Напоминание владельцу если заявка висит 2 часа
+ Кнопка "Перезвоните мне"
+ Еженедельная сводка по понедельникам
"""

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    LabeledPrice, PreCheckoutQuery
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import CommandStart, Command

# ─── НАСТРОЙКИ ────────────────────────────────────────────────────────────────
BOT_TOKEN       = "8437642100:AAF6NL71wkN77uctXCgTGLFHf1gITDD57-M"           # Токен от @BotFather
OWNER_ID        = 125380747              # Ваш Telegram ID от @userinfobot
DB_FILE         = "housemaster.db"
PHONE           = "+7 (992) 350-80-08"  # Телефон Хаус Мастер

# Оплата — токен от ЮКассы через @BotFather → Payments
# Для теста используй: "381764678:TEST:74896" (Telegram тестовый провайдер)
PAYMENT_TOKEN   = "ВАШ_PAYMENT_TOKEN"   # Токен платёжного провайдера
PAYMENT_ENABLED = False                  # True — включить оплату через бота

# ID мастеров — добавляй Telegram ID исполнителей
MASTER_IDS = [
    # 111111111,
    # 222222222,
]

# Города — добавляй по мере масштабирования
CITIES = [
    "Москва",
    "Санкт-Петербург",
    "Казань",
    "Екатеринбург",
    "Новосибирск",
    "Краснодар",
    "Другой город",
]
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

# ─── БАЗА ДАННЫХ ──────────────────────────────────────────────────────────────
def db_connect():
    return sqlite3.connect(DB_FILE)

def db_init():
    with db_connect() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id          INTEGER PRIMARY KEY,
                name        TEXT,
                username    TEXT,
                city        TEXT,
                first_seen  TEXT,
                last_seen   TEXT,
                visits      INTEGER DEFAULT 1
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER,
                order_type   TEXT,
                summary      TEXT,
                status       TEXT DEFAULT 'принята',
                master_id    INTEGER DEFAULT NULL,
                master_name  TEXT DEFAULT NULL,
                city         TEXT DEFAULT NULL,
                visit_time   TEXT DEFAULT NULL,
                paid         INTEGER DEFAULT 0,
                created_at   TEXT,
                updated_at   TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                order_id    INTEGER,
                rating      INTEGER,
                comment     TEXT,
                created_at  TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS masters (
                id            INTEGER PRIMARY KEY,
                name          TEXT,
                username      TEXT,
                phone         TEXT,
                city          TEXT,
                registered_at TEXT
            )
        """)
        con.commit()

def db_track_user(user, city: str = None) -> bool:
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db_connect() as con:
        existing = con.execute("SELECT id FROM users WHERE id=?", (user.id,)).fetchone()
        if not existing:
            con.execute(
                "INSERT INTO users (id, name, username, city, first_seen, last_seen, visits) VALUES (?,?,?,?,?,?,1)",
                (user.id, user.full_name, user.username or "", city or "", now, now)
            )
            con.commit()
            return True
        else:
            update_city = f", city='{city}'" if city else ""
            con.execute(
                f"UPDATE users SET last_seen=?, visits=visits+1, name=?, username=?{update_city} WHERE id=?",
                (now, user.full_name, user.username or "", user.id)
            )
            con.commit()
            return False

def db_add_order(user_id: int, order_type: str, summary: str, city: str = None, visit_time: str = None) -> int:
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db_connect() as con:
        cur = con.execute(
            "INSERT INTO orders (user_id, order_type, summary, status, city, visit_time, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (user_id, order_type, summary, "принята", city, visit_time, now, now)
        )
        con.commit()
        return cur.lastrowid

def db_update_status(order_id: int, status: str):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db_connect() as con:
        con.execute("UPDATE orders SET status=?, updated_at=? WHERE id=?", (status, now, order_id))
        con.commit()

def db_mark_paid(order_id: int):
    with db_connect() as con:
        con.execute("UPDATE orders SET paid=1 WHERE id=?", (order_id,))
        con.commit()

def db_assign_master(order_id: int, master_id: int, master_name: str) -> bool:
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db_connect() as con:
        row = con.execute("SELECT master_id, status FROM orders WHERE id=?", (order_id,)).fetchone()
        if not row or row[0] is not None or row[1] != "принята":
            return False
        con.execute(
            "UPDATE orders SET master_id=?, master_name=?, status='в работе', updated_at=? WHERE id=?",
            (master_id, master_name, now, order_id)
        )
        con.commit()
        return True

def db_get_order(order_id: int):
    with db_connect() as con:
        return con.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()

def db_add_review(user_id: int, order_id: int, rating: int, comment: str):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db_connect() as con:
        con.execute(
            "INSERT INTO reviews (user_id, order_id, rating, comment, created_at) VALUES (?,?,?,?,?)",
            (user_id, order_id, rating, comment, now)
        )
        con.commit()

def db_get_master_orders(master_id: int) -> list:
    with db_connect() as con:
        return con.execute(
            "SELECT id, order_type, status, created_at, summary, visit_time FROM orders WHERE master_id=? ORDER BY created_at DESC LIMIT 10",
            (master_id,)
        ).fetchall()

def db_get_active_orders() -> list:
    with db_connect() as con:
        return con.execute(
            """SELECT o.id, o.user_id, o.order_type, o.status, o.created_at,
                      u.name, u.username, o.master_name, o.city, o.visit_time, o.paid
               FROM orders o LEFT JOIN users u ON o.user_id = u.id
               WHERE o.status NOT IN ('выполнено', 'отменена')
               ORDER BY o.created_at DESC"""
        ).fetchall()

def db_get_client_orders(user_id: int) -> list:
    with db_connect() as con:
        return con.execute(
            "SELECT id, order_type, status, created_at, master_name, visit_time FROM orders WHERE user_id=? ORDER BY created_at DESC LIMIT 10",
            (user_id,)
        ).fetchall()

def db_get_all_user_ids() -> list:
    with db_connect() as con:
        return [r[0] for r in con.execute("SELECT id FROM users").fetchall()]

def db_get_pending_orders(minutes: int = 120) -> list:
    with db_connect() as con:
        rows = con.execute(
            "SELECT id, user_id, order_type, created_at FROM orders WHERE status='принята'"
        ).fetchall()
    result = []
    now = datetime.now()
    for row in rows:
        try:
            created = datetime.strptime(row[3], "%d.%m.%Y %H:%M")
            if (now - created).total_seconds() > minutes * 60:
                result.append({"id": row[0], "user_id": row[1], "type": row[2], "created_at": row[3]})
        except Exception:
            pass
    return result

def db_get_stats() -> str:
    with db_connect() as con:
        total_users  = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_orders = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        onetime_cnt  = con.execute("SELECT COUNT(*) FROM orders WHERE order_type='onetime'").fetchone()[0]
        regular_cnt  = con.execute("SELECT COUNT(*) FROM orders WHERE order_type='regular'").fetchone()[0]
        done_cnt     = con.execute("SELECT COUNT(*) FROM orders WHERE status='выполнено'").fetchone()[0]
        paid_cnt     = con.execute("SELECT COUNT(*) FROM orders WHERE paid=1").fetchone()[0]
        master_cnt   = con.execute("SELECT COUNT(*) FROM masters").fetchone()[0]
        avg_rating   = con.execute("SELECT AVG(rating) FROM reviews").fetchone()[0]
        review_cnt   = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
        cities       = con.execute(
            "SELECT city, COUNT(*) as cnt FROM users WHERE city != '' GROUP BY city ORDER BY cnt DESC LIMIT 5"
        ).fetchall()
    rating_str = f"{avg_rating:.1f} / 5 ({review_cnt} отзывов)" if avg_rating else "пока нет"
    cities_str = "\n".join([f"  {c[0]}: {c[1]} чел." for c in cities]) if cities else "  —"
    return (
        f"📊 Статистика Хаус Мастер\n\n"
        f"Пользователей:      {total_users}\n"
        f"Мастеров:           {master_cnt}\n"
        f"Всего заявок:       {total_orders}\n"
        f"  разовый ремонт:   {onetime_cnt}\n"
        f"  регулярное обсл.: {regular_cnt}\n"
        f"  выполнено:        {done_cnt}\n"
        f"  оплачено онлайн:  {paid_cnt}\n"
        f"Средний отзыв:      {rating_str}\n\n"
        f"Топ городов:\n{cities_str}"
    )

def db_get_weekly_stats() -> str:
    with db_connect() as con:
        week_ago    = (datetime.now() - timedelta(days=7)).strftime("%d.%m.%Y")
        new_users   = con.execute("SELECT COUNT(*) FROM users WHERE first_seen >= ?", (week_ago,)).fetchone()[0]
        new_orders  = con.execute("SELECT COUNT(*) FROM orders WHERE created_at >= ?", (week_ago,)).fetchone()[0]
        onetime_cnt = con.execute("SELECT COUNT(*) FROM orders WHERE order_type='onetime' AND created_at >= ?", (week_ago,)).fetchone()[0]
        regular_cnt = con.execute("SELECT COUNT(*) FROM orders WHERE order_type='regular' AND created_at >= ?", (week_ago,)).fetchone()[0]
        done_cnt    = con.execute("SELECT COUNT(*) FROM orders WHERE status='выполнено' AND updated_at >= ?", (week_ago,)).fetchone()[0]
        paid_cnt    = con.execute("SELECT COUNT(*) FROM orders WHERE paid=1 AND created_at >= ?", (week_ago,)).fetchone()[0]
        avg_rating  = con.execute("SELECT AVG(rating) FROM reviews WHERE created_at >= ?", (week_ago,)).fetchone()[0]
    rating_str = f"{avg_rating:.1f}/5" if avg_rating else "нет"
    return (
        f"📅 Сводка за 7 дней\n{datetime.now().strftime('%d.%m.%Y')}\n\n"
        f"Новых пользователей: {new_users}\n"
        f"Новых заявок:        {new_orders}\n"
        f"  разовый ремонт:    {onetime_cnt}\n"
        f"  регулярное обсл.:  {regular_cnt}\n"
        f"Выполнено:           {done_cnt}\n"
        f"Оплачено онлайн:     {paid_cnt}\n"
        f"Средний отзыв:       {rating_str}"
    )

# ─── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ──────────────────────────────────────────────────
def is_working_hours() -> bool:
    now = datetime.now()
    if now.weekday() == 6:
        return False
    return 8 <= now.hour < 21

def is_master(user_id: int) -> bool:
    return user_id in MASTER_IDS

def get_visit_slots() -> list:
    """Генерирует слоты на сегодня и завтра"""
    slots = []
    now   = datetime.now()
    for day_offset in range(3):
        day = now + timedelta(days=day_offset)
        label = ["Сегодня", "Завтра", day.strftime("%d.%m")][min(day_offset, 2)]
        for hour in [9, 11, 13, 15, 17, 19]:
            slot_dt = day.replace(hour=hour, minute=0)
            if slot_dt > now + timedelta(hours=1):
                slots.append({
                    "label": f"{label} {hour:02d}:00–{hour+2:02d}:00",
                    "value": slot_dt.strftime("%d.%m %H:00"),
                })
            if len(slots) >= 9:
                break
        if len(slots) >= 9:
            break
    return slots

# ─── УСЛУГИ ───────────────────────────────────────────────────────────────────
ONETIME_SERVICES = {
    "lamp":       {"name": "🔦 Замена ламп / люстр",          "price": "от 500 руб.",       "time": "30 мин",      "amount": 50000},
    "furniture":  {"name": "🪑 Сборка / ремонт мебели",       "price": "от 1 000 руб.",     "time": "1-3 часа",    "amount": 100000},
    "paint":      {"name": "🎨 Подкраска стен и поверхностей", "price": "от 1 500 руб.",     "time": "1-4 часа",    "amount": 150000},
    "plumbing":   {"name": "🚿 Сантехника (кран, унитаз)",     "price": "от 1 500 руб.",     "time": "1-2 часа",    "amount": 150000},
    "door":       {"name": "🚪 Установка / регулировка дверей","price": "от 1 000 руб.",     "time": "1-2 часа",    "amount": 100000},
    "shelf":      {"name": "📦 Полки, карнизы, крючки",        "price": "от 500 руб.",       "time": "30-60 мин",   "amount": 50000},
    "tv":         {"name": "📺 Крепление ТВ на стену",         "price": "от 1 500 руб.",     "time": "1 час",       "amount": 150000},
    "tile":       {"name": "🧱 Укладка / замена плитки",       "price": "от 3 000 руб.",     "time": "от 2 часов",  "amount": 300000},
    "electrical": {"name": "⚡ Электрика (розетки, выключат.)","price": "от 1 000 руб.",     "time": "1-2 часа",    "amount": 100000},
    "floor":      {"name": "🪵 Скрип пола / ламинат",          "price": "от 2 000 руб.",     "time": "от 2 часов",  "amount": 200000},
    "caulk":      {"name": "🪟 Герметизация окон / щелей",     "price": "от 800 руб.",       "time": "1 час",       "amount": 80000},
    "other":      {"name": "🔨 Другое — уточним",              "price": "по договорённости", "time": "уточним",     "amount": 0},
}

REGULAR_SERVICES = {
    "cafe_basic":   {"name": "☕ Кафе / ресторан — базовый",  "price": "от 8 000 руб./мес.",  "visits": "2 раза/мес.", "amount": 800000},
    "cafe_full":    {"name": "☕ Кафе / ресторан — полный",   "price": "от 15 000 руб./мес.", "visits": "еженедельно", "amount": 1500000},
    "office_basic": {"name": "🏢 Офис — базовый пакет",       "price": "от 5 000 руб./мес.",  "visits": "2 раза/мес.", "amount": 500000},
    "office_full":  {"name": "🏢 Офис — полный пакет",        "price": "от 10 000 руб./мес.", "visits": "еженедельно", "amount": 1000000},
    "apartment":    {"name": "🏠 Квартира / дом",             "price": "от 3 000 руб./мес.",  "visits": "1 раз/мес.",  "amount": 300000},
    "custom":       {"name": "📋 Индивидуальные условия",     "price": "по договорённости",   "visits": "по согл.",    "amount": 0},
}

URGENCY = {
    "standard": {"name": "📅 Стандарт (1-2 дня)",      "mult": "x1"},
    "urgent":   {"name": "⚡ Срочно (сегодня) +30%",   "mult": "+30%"},
    "express":  {"name": "🚨 Экстренно (2 часа) +60%", "mult": "+60%"},
}

PAYMENT = {
    "cash":       {"name": "💵 Наличные"},
    "card":       {"name": "💳 Карта / СБП"},
    "online":     {"name": "💳 Оплатить сейчас онлайн"},
    "bank_nds":   {"name": "🏦 Безнал с НДС"},
    "bank_nonds": {"name": "🏦 Безнал без НДС"},
}

ORDER_STATUSES = {
    "принята":   "Заявка принята, ищем мастера",
    "в работе":  "Мастер назначен и едет к вам",
    "выполнено": "Работа выполнена",
    "отменена":  "Заявка отменена",
}

# ─── СОСТОЯНИЯ ────────────────────────────────────────────────────────────────
class Order(StatesGroup):
    choosing_city     = State()
    choosing_service  = State()
    choosing_regular  = State()
    choosing_urgency  = State()
    choosing_time     = State()
    entering_address  = State()
    entering_phone    = State()
    choosing_payment  = State()
    entering_comment  = State()
    confirm           = State()

class OwnerReply(StatesGroup):
    waiting_message = State()

class Review(StatesGroup):
    waiting_rating  = State()
    waiting_comment = State()

# ─── ХЕЛПЕРЫ ──────────────────────────────────────────────────────────────────
def order_summary(data: dict) -> str:
    order_type = data.get("order_type", "onetime")
    lines = ["📋 Ваша заявка:\n"]
    if data.get("city"):
        lines.append(f"Город: {data['city']}")
    if order_type == "onetime":
        svc = ONETIME_SERVICES.get(data.get("service", ""), {})
        urg = URGENCY.get(data.get("urgency", "standard"), {})
        lines.append(f"Тип: Разовый ремонт")
        lines.append(f"Услуга: {svc.get('name', '—')}")
        lines.append(f"Срочность: {urg.get('name', '—')}")
    else:
        svc = REGULAR_SERVICES.get(data.get("regular_service", ""), {})
        lines.append(f"Тип: Регулярное обслуживание")
        lines.append(f"Пакет: {svc.get('name', '—')}")
        lines.append(f"Стоимость: {svc.get('price', '—')}")
        lines.append(f"Периодичность: {svc.get('visits', '—')}")
    if data.get("visit_time"):
        lines.append(f"Время визита: {data['visit_time']}")
    lines.append(f"Адрес: {data.get('address', '—')}")
    lines.append(f"Телефон: {data.get('phone', '—')}")
    pay_key = data.get("payment", "cash")
    if pay_key != "online":
        lines.append(f"Оплата: {PAYMENT.get(pay_key, {}).get('name', '—')}")
    else:
        lines.append(f"Оплата: онлайн ✅")
    comment = data.get("comment", "нет")
    if comment and comment != "нет":
        lines.append(f"Комментарий: {comment}")
    return "\n".join(lines)

async def notify_owner(data: dict, user, order_id: int):
    label = "РАЗОВЫЙ" if data.get("order_type") == "onetime" else "РЕГУЛЯРНОЕ"
    tag   = f"@{user.username}" if user.username else f"ID: {user.id}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ В работу",         callback_data=f"ss_{order_id}_{user.id}_inwork")],
        [InlineKeyboardButton(text="✅ Выполнено",         callback_data=f"ss_{order_id}_{user.id}_done")],
        [InlineKeyboardButton(text="💬 Написать клиенту",  callback_data=f"msg_{order_id}_{user.id}")],
        [InlineKeyboardButton(text="❌ Отменить",          callback_data=f"ss_{order_id}_{user.id}_cancel")],
    ])
    await bot.send_message(
        OWNER_ID,
        f"🆕 НОВАЯ ЗАЯВКА #{order_id} — {label}\n"
        f"Клиент: {user.full_name} ({tag})\n\n" + order_summary(data),
        reply_markup=kb
    )
    if data.get("lat") and data.get("lon"):
        await bot.send_location(OWNER_ID, latitude=data["lat"], longitude=data["lon"])

async def notify_masters(data: dict, user, order_id: int):
    if not MASTER_IDS:
        return
    label = "РАЗОВЫЙ" if data.get("order_type") == "onetime" else "РЕГУЛЯРНОЕ"
    tag   = f"@{user.username}" if user.username else f"ID: {user.id}"
    if data.get("order_type") == "onetime":
        svc     = ONETIME_SERVICES.get(data.get("service", ""), {})
        urg     = URGENCY.get(data.get("urgency", "standard"), {})
        details = f"Услуга: {svc.get('name','—')}\nСрочность: {urg.get('name','—')}"
    else:
        svc     = REGULAR_SERVICES.get(data.get("regular_service", ""), {})
        details = f"Пакет: {svc.get('name','—')}\nСтоимость: {svc.get('price','—')}"
    visit_str = f"\nВремя визита: {data['visit_time']}" if data.get("visit_time") else ""
    city_str  = f"\nГород: {data['city']}" if data.get("city") else ""
    text = (
        f"🔔 НОВАЯ ЗАЯВКА #{order_id} — {label}\n\n"
        f"{details}\n"
        f"Адрес: {data.get('address','—')}"
        f"{city_str}{visit_str}\n"
        f"Клиент: {tag}\n\nВозьмёшь заказ?"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять заявку", callback_data=f"take_{order_id}")],
        [InlineKeyboardButton(text="❌ Отказаться",     callback_data=f"skip_{order_id}")],
    ])
    for master_id in MASTER_IDS:
        try:
            await bot.send_message(master_id, text, reply_markup=kb)
            if data.get("lat") and data.get("lon"):
                await bot.send_location(master_id, latitude=data["lat"], longitude=data["lon"])
        except Exception as e:
            logging.warning(f"Не удалось уведомить мастера {master_id}: {e}")

# ─── КЛАВИАТУРЫ ───────────────────────────────────────────────────────────────
def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔧 Разовый ремонт",         callback_data="start_onetime")],
        [InlineKeyboardButton(text="🔄 Регулярное обслуживание", callback_data="start_regular")],
        [InlineKeyboardButton(text="📋 Мои заявки",              callback_data="my_orders")],
        [InlineKeyboardButton(text="📞 Перезвоните мне",         callback_data="callback_request")],
        [InlineKeyboardButton(text="💰 Прайс-лист",              callback_data="show_prices")],
        [InlineKeyboardButton(text="☎️ Позвонить нам",           callback_data="call_us")],
    ])

def kb_master_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Мои задачи",     callback_data="master_tasks")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="master_stats")],
    ])

def kb_cities():
    rows = []
    for i in range(0, len(CITIES), 2):
        row = []
        for city in CITIES[i:i+2]:
            row.append(InlineKeyboardButton(text=city, callback_data=f"city_{city}"))
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_onetime_services():
    rows = []
    items = list(ONETIME_SERVICES.items())
    for i in range(0, len(items), 2):
        row = []
        for key, val in items[i:i+2]:
            row.append(InlineKeyboardButton(text=val["name"], callback_data=f"svc_{key}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_regular_services():
    rows = []
    items = list(REGULAR_SERVICES.items())
    for i in range(0, len(items), 2):
        row = []
        for key, val in items[i:i+2]:
            row.append(InlineKeyboardButton(text=val["name"], callback_data=f"reg_{key}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_urgency():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Стандарт (1-2 дня)",      callback_data="urg_standard")],
        [InlineKeyboardButton(text="⚡ Срочно (сегодня) +30%",   callback_data="urg_urgent")],
        [InlineKeyboardButton(text="🚨 Экстренно (2 часа) +60%", callback_data="urg_express")],
        [InlineKeyboardButton(text="⬅️ Назад",                   callback_data="start_onetime")],
    ])

def kb_visit_time():
    slots = get_visit_slots()
    rows  = []
    for i in range(0, len(slots), 2):
        row = []
        for slot in slots[i:i+2]:
            row.append(InlineKeyboardButton(text=slot["label"], callback_data=f"vt_{slot['value']}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🕐 Уточним по телефону", callback_data="vt_call")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_address():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Отправить геолокацию", request_location=True)],
            [KeyboardButton(text="✏️ Ввести адрес вручную")],
        ],
        resize_keyboard=True, one_time_keyboard=True
    )

def kb_payment(with_online: bool = True, amount: int = 0):
    rows = []
    if with_online and PAYMENT_ENABLED and amount > 0:
        rows.append([InlineKeyboardButton(text="💳 Оплатить сейчас онлайн", callback_data="pay_online")])
    rows.append([InlineKeyboardButton(text="💵 Наличные",       callback_data="pay_cash")])
    rows.append([InlineKeyboardButton(text="💳 Карта / СБП",    callback_data="pay_card")])
    rows.append([InlineKeyboardButton(text="🏦 Безнал с НДС",   callback_data="pay_bank_nds")])
    rows.append([InlineKeyboardButton(text="🏦 Безнал без НДС", callback_data="pay_bank_nonds")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_phone():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить мой номер", request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True
    )

def kb_skip():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пропустить ➡️", callback_data="skip_comment")]
    ])

def kb_confirm():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить заявку", callback_data="confirm_yes")],
        [InlineKeyboardButton(text="✏️ Изменить",         callback_data="confirm_no")],
    ])

def kb_rating(order_id: int = 0):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐ 1", callback_data=f"rev_{order_id}_1"),
            InlineKeyboardButton(text="⭐ 2", callback_data=f"rev_{order_id}_2"),
            InlineKeyboardButton(text="⭐ 3", callback_data=f"rev_{order_id}_3"),
            InlineKeyboardButton(text="⭐ 4", callback_data=f"rev_{order_id}_4"),
            InlineKeyboardButton(text="⭐ 5", callback_data=f"rev_{order_id}_5"),
        ],
        [InlineKeyboardButton(text="Пропустить", callback_data=f"rev_{order_id}_skip")]
    ])

def kb_review_comment():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пропустить", callback_data="rev_comment_skip")]
    ])

# ─── СТАРТ ────────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id

    # Мастер
    if is_master(user_id):
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        with db_connect() as con:
            con.execute(
                "INSERT OR IGNORE INTO masters (id, name, username, registered_at) VALUES (?,?,?,?)",
                (user_id, message.from_user.full_name, message.from_user.username or "", now)
            )
            con.commit()
        await message.answer(
            f"👷 Привет, мастер {message.from_user.first_name}!\n\n"
            f"Как только появится новый заказ — пришлю уведомление.\nУдачной работы! 💪",
            reply_markup=kb_master_main()
        )
        return

    # Владелец
    if user_id == OWNER_ID:
        await message.answer(
            f"👑 Привет, шеф!\n\n"
            f"/orders — активные заявки\n"
            f"/stats — статистика\n"
            f"/week — сводка за 7 дней\n"
            f"/masters — список мастеров\n"
            f"/broadcast [текст] — рассылка\n",
            reply_markup=kb_main()
        )
        return

    # Клиент
    db_track_user(message.from_user)
    name = message.from_user.first_name or "Гость"
    off_hours_note = ""
    if not is_working_hours():
        off_hours_note = f"\n⚠️ Сейчас не работаем (Пн-Сб 8:00-21:00).\nЗаявку можно оставить — ответим утром!\n"
    await message.answer(
        f"Привет, {name}! 👋 Добро пожаловать в Хаус Мастер!\n\n"
        f"Быстро решаем бытовые задачи:\n"
        f"🔦 Лампы, мебель, сантехника, электрика\n"
        f"🎨 Подкраска, герметизация, плитка\n"
        f"🔄 Регулярное обслуживание кафе и офисов\n\n"
        f"✅ Выбор удобного времени\n"
        f"✅ Фиксированные цены без доплат\n"
        f"✅ Гарантия на все работы\n"
        + off_hours_note + "\nЧем могу помочь?",
        reply_markup=kb_main()
    )

# ─── ВЫБОР ГОРОДА ─────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "start_onetime")
async def start_onetime(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.update_data(order_type="onetime")
    await state.set_state(Order.choosing_city)
    await cb.message.answer("🏙️ Выберите ваш город:", reply_markup=kb_cities())
    await cb.answer()

@dp.callback_query(F.data == "start_regular")
async def start_regular(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.update_data(order_type="regular")
    await state.set_state(Order.choosing_city)
    await cb.message.answer("🏙️ Выберите ваш город:", reply_markup=kb_cities())
    await cb.answer()

@dp.callback_query(F.data.startswith("city_"), Order.choosing_city)
async def choose_city(cb: CallbackQuery, state: FSMContext):
    city = cb.data.replace("city_", "")
    await state.update_data(city=city)
    data = await state.get_data()
    db_track_user(cb.from_user, city=city)

    if data.get("order_type") == "onetime":
        await state.set_state(Order.choosing_service)
        await cb.message.answer(f"Город: {city} ✅\n\nВыберите услугу:", reply_markup=kb_onetime_services())
    else:
        await state.set_state(Order.choosing_regular)
        await cb.message.answer(
            f"Город: {city} ✅\n\nРегулярное обслуживание — мастер приезжает по расписанию.\n\nВыберите пакет:",
            reply_markup=kb_regular_services()
        )
    await cb.answer()

# ─── ВЫБОР УСЛУГИ ─────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("svc_"), Order.choosing_service)
async def choose_service(cb: CallbackQuery, state: FSMContext):
    key = cb.data.replace("svc_", "")
    svc = ONETIME_SERVICES[key]
    await state.update_data(service=key)
    await state.set_state(Order.choosing_urgency)
    await cb.message.answer(
        f"Выбрано: {svc['name']}\nЦена: {svc['price']} — {svc['time']}\n\nВыберите срочность:",
        reply_markup=kb_urgency()
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("urg_"), Order.choosing_urgency)
async def choose_urgency(cb: CallbackQuery, state: FSMContext):
    key = cb.data.replace("urg_", "")
    await state.update_data(urgency=key)
    await state.set_state(Order.choosing_time)
    await cb.message.answer(
        f"Срочность: {URGENCY[key]['name']}\n\n🕐 Выберите удобное время визита мастера:",
        reply_markup=kb_visit_time()
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("reg_"), Order.choosing_regular)
async def choose_regular(cb: CallbackQuery, state: FSMContext):
    key = cb.data.replace("reg_", "")
    svc = REGULAR_SERVICES[key]
    await state.update_data(regular_service=key)
    await state.set_state(Order.choosing_time)
    await cb.message.answer(
        f"Выбрано: {svc['name']}\nСтоимость: {svc['price']}\nПериодичность: {svc['visits']}\n\n"
        f"🕐 Выберите удобное время первого визита:",
        reply_markup=kb_visit_time()
    )
    await cb.answer()

# ─── ВЫБОР ВРЕМЕНИ ────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("vt_"), Order.choosing_time)
async def choose_visit_time(cb: CallbackQuery, state: FSMContext):
    value = cb.data.replace("vt_", "")
    if value == "call":
        visit_time = "Уточним по телефону"
    else:
        visit_time = value
    await state.update_data(visit_time=visit_time)
    await state.set_state(Order.entering_address)
    await cb.message.answer(
        f"Время визита: {visit_time} ✅\n\nУкажите адрес объекта:",
        reply_markup=kb_address()
    )
    await cb.answer()

# ─── АДРЕС ────────────────────────────────────────────────────────────────────
@dp.message(Order.entering_address)
async def enter_address(message: Message, state: FSMContext):
    if message.location:
        lat = message.location.latitude
        lon = message.location.longitude
        await state.update_data(
            address=f"📍 Геолокация: {lat:.5f}, {lon:.5f}",
            lat=lat, lon=lon
        )
        await message.answer("Геолокация получена ✅\n\nУкажите номер телефона:", reply_markup=ReplyKeyboardRemove())
        await message.answer("👇", reply_markup=kb_phone())
        await state.set_state(Order.entering_phone)
        return

    if message.text and message.text != "✏️ Ввести адрес вручную":
        await state.update_data(address=message.text)
        await message.answer("Адрес принят ✅\n\nУкажите номер телефона:", reply_markup=ReplyKeyboardRemove())
        await message.answer("👇", reply_markup=kb_phone())
        await state.set_state(Order.entering_phone)
        return

    await message.answer("Введите адрес текстом (улица, дом, квартира):", reply_markup=ReplyKeyboardRemove())

# ─── ТЕЛЕФОН ──────────────────────────────────────────────────────────────────
@dp.message(Order.entering_phone, F.contact)
async def enter_phone_contact(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("order_type") == "callback":
        await _handle_callback(message, message.contact.phone_number, state)
        return
    await state.update_data(phone=message.contact.phone_number)
    await _ask_payment(message, state)

@dp.message(Order.entering_phone)
async def enter_phone_text(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("order_type") == "callback":
        await _handle_callback(message, message.text, state)
        return
    await state.update_data(phone=message.text)
    await _ask_payment(message, state)

async def _ask_payment(message: Message, state: FSMContext):
    data = await state.get_data()
    # Определяем сумму для онлайн-оплаты
    amount = 0
    if data.get("order_type") == "onetime":
        amount = ONETIME_SERVICES.get(data.get("service", ""), {}).get("amount", 0)
    else:
        amount = REGULAR_SERVICES.get(data.get("regular_service", ""), {}).get("amount", 0)
    await state.update_data(payment_amount=amount)
    await state.set_state(Order.choosing_payment)
    await message.answer("Выберите форму оплаты:", reply_markup=ReplyKeyboardRemove())
    await message.answer("👇", reply_markup=kb_payment(with_online=True, amount=amount))

# ─── ОПЛАТА ───────────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("pay_"), Order.choosing_payment)
async def choose_payment(cb: CallbackQuery, state: FSMContext):
    key = cb.data.replace("pay_", "")
    await state.update_data(payment=key)

    if key == "online":
        # Онлайн-оплата через Telegram Payments
        data = await state.get_data()
        amount = data.get("payment_amount", 0)
        if amount == 0:
            await cb.message.answer(
                "Для этой услуги стоимость рассчитывается индивидуально.\n"
                "Выберите другой способ оплаты:",
                reply_markup=kb_payment(with_online=False)
            )
            await cb.answer()
            return
        svc_name = ""
        if data.get("order_type") == "onetime":
            svc_name = ONETIME_SERVICES.get(data.get("service", ""), {}).get("name", "Услуга")
        else:
            svc_name = REGULAR_SERVICES.get(data.get("regular_service", ""), {}).get("name", "Услуга")
        await cb.message.answer_invoice(
            title=f"Хаус Мастер — {svc_name}",
            description="Оплата услуги мастера",
            payload=f"order_payment",
            provider_token=PAYMENT_TOKEN,
            currency="RUB",
            prices=[LabeledPrice(label=svc_name, amount=amount)],
            need_name=False,
            need_phone_number=False,
        )
        await cb.answer()
        return

    await state.set_state(Order.entering_comment)
    await cb.message.answer(
        f"Оплата: {PAYMENT[key]['name']}\n\nДобавьте комментарий или нажмите Пропустить:",
        reply_markup=kb_skip()
    )
    await cb.answer()

# Telegram Payments — предварительная проверка
@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)

# Telegram Payments — успешная оплата
@dp.message(F.successful_payment)
async def successful_payment(message: Message, state: FSMContext):
    await state.update_data(payment="online", paid=True)
    await bot.send_message(
        OWNER_ID,
        f"💳 Получена онлайн-оплата!\n"
        f"Клиент: {message.from_user.full_name}\n"
        f"Сумма: {message.successful_payment.total_amount // 100} руб."
    )
    await state.set_state(Order.entering_comment)
    await message.answer(
        "✅ Оплата прошла успешно!\n\nДобавьте комментарий или нажмите Пропустить:",
        reply_markup=kb_skip()
    )

# ─── КОММЕНТАРИЙ И ПОДТВЕРЖДЕНИЕ ─────────────────────────────────────────────
@dp.callback_query(F.data == "skip_comment", Order.entering_comment)
async def skip_comment(cb: CallbackQuery, state: FSMContext):
    await state.update_data(comment="нет")
    data = await state.get_data()
    await state.set_state(Order.confirm)
    await cb.message.answer(order_summary(data) + "\n\nВсё верно?", reply_markup=kb_confirm())
    await cb.answer()

@dp.message(Order.entering_comment)
async def enter_comment(message: Message, state: FSMContext):
    await state.update_data(comment=message.text)
    data = await state.get_data()
    await state.set_state(Order.confirm)
    await message.answer(order_summary(data) + "\n\nВсё верно?", reply_markup=kb_confirm())

@dp.callback_query(F.data == "confirm_yes", Order.confirm)
async def confirm_order(cb: CallbackQuery, state: FSMContext):
    data       = await state.get_data()
    city       = data.get("city")
    visit_time = data.get("visit_time")
    order_id   = db_add_order(
        cb.from_user.id,
        data.get("order_type", "onetime"),
        order_summary(data),
        city=city,
        visit_time=visit_time
    )
    if data.get("paid"):
        db_mark_paid(order_id)
    await notify_owner(data, cb.from_user, order_id)
    await notify_masters(data, cb.from_user, order_id)
    await state.clear()

    if data.get("order_type") == "onetime":
        svc        = ONETIME_SERVICES.get(data.get("service", ""), {})
        urg        = URGENCY.get(data.get("urgency", "standard"), {})
        price_hint = f"\nПримерная стоимость: {svc.get('price','')} ({urg.get('mult','')})\n"
        extra      = "Можете прислать фото — поможет точнее назвать стоимость.\n\n"
    else:
        svc        = REGULAR_SERVICES.get(data.get("regular_service", ""), {})
        price_hint = f"\nСтоимость: {svc.get('price','')}\n"
        extra      = "Свяжемся для согласования расписания визитов.\n\n"

    visit_str = f"Мастер приедет: {visit_time}\n" if visit_time and visit_time != "Уточним по телефону" else ""
    await cb.message.answer(
        f"✅ Заявка #{order_id} принята!\n{price_hint}\n"
        f"{visit_str}Мастер свяжется с вами в ближайшее время.\n\n{extra}"
        f"Спасибо, что выбрали Хаус Мастер! 🏠",
        reply_markup=kb_main()
    )

@dp.callback_query(F.data == "confirm_no", Order.confirm)
async def cancel_order(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Хорошо, начнём заново:", reply_markup=kb_main())

# ─── МАСТЕР — ПРИНЯТЬ / ОТКАЗАТЬСЯ ───────────────────────────────────────────
@dp.callback_query(F.data.startswith("take_"))
async def master_take_order(cb: CallbackQuery):
    if not is_master(cb.from_user.id):
        await cb.answer("Только мастера могут принимать заявки.", show_alert=True)
        return
    order_id = int(cb.data.replace("take_", ""))
    success  = db_assign_master(order_id, cb.from_user.id, cb.from_user.full_name)
    if not success:
        await cb.answer("Заявка уже взята другим мастером!", show_alert=True)
        await cb.message.edit_reply_markup(reply_markup=None)
        return
    await cb.message.edit_text(
        cb.message.text + f"\n\n✅ Ты принял эту заявку!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📸 Фото ДО начала работ",    callback_data=f"photo_before_{order_id}")],
            [InlineKeyboardButton(text="✅ Отметить выполненной",     callback_data=f"mdone_{order_id}")],
        ])
    )
    await cb.answer("Заявка принята! Удачи 💪")
    order = db_get_order(order_id)
    if order:
        try:
            await bot.send_message(
                order[1],
                f"🔧 По вашей заявке #{order_id} назначен мастер!\n\n"
                f"Мастер: {cb.from_user.full_name}\nСкоро свяжется с вами.\n\nВопросы? Звоните: {PHONE}"
            )
        except Exception:
            pass
    await bot.send_message(OWNER_ID, f"✅ Мастер {cb.from_user.full_name} взял заявку #{order_id}")
    for master_id in MASTER_IDS:
        if master_id != cb.from_user.id:
            try:
                await bot.send_message(master_id, f"ℹ️ Заявка #{order_id} уже взята мастером {cb.from_user.full_name}.")
            except Exception:
                pass

@dp.callback_query(F.data.startswith("skip_"))
async def master_skip_order(cb: CallbackQuery):
    if not is_master(cb.from_user.id):
        return
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer("Понял, пропускаем.")

@dp.callback_query(F.data.startswith("mdone_"))
async def master_done_order(cb: CallbackQuery):
    if not is_master(cb.from_user.id):
        return
    order_id = int(cb.data.replace("mdone_", ""))
    order    = db_get_order(order_id)
    if not order or order[5] != cb.from_user.id:
        await cb.answer("Это не твоя заявка.", show_alert=True)
        return
    await cb.message.answer(
        "Пришли фото результата (после выполнения работ) — отправим клиенту автоматически.\n\n"
        "Или нажми если фото не нужно:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Пропустить фото", callback_data=f"done_nophoto_{order_id}")]
        ])
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("done_nophoto_"))
async def done_no_photo(cb: CallbackQuery):
    order_id = int(cb.data.replace("done_nophoto_", ""))
    order    = db_get_order(order_id)
    if not order:
        return
    await _apply_status(cb.message, order_id, "выполнено", user_id=order[1])
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer("Заявка выполнена ✅")

# ─── ФОТО ДО/ПОСЛЕ ────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("photo_before_"))
async def request_photo_before(cb: CallbackQuery, state: FSMContext):
    if not is_master(cb.from_user.id):
        return
    order_id = int(cb.data.replace("photo_before_", ""))
    await state.update_data(photo_order_id=order_id, photo_stage="before")
    await cb.message.answer("📸 Пришли фото ДО начала работ:")
    await cb.answer()

@dp.message(F.photo)
async def handle_photo(message: Message, state: FSMContext):
    # Мастер отправляет фото до/после
    if is_master(message.from_user.id):
        data  = await state.get_data()
        stage = data.get("photo_stage")
        order_id = data.get("photo_order_id")

        if stage == "before" and order_id:
            order = db_get_order(order_id)
            if order:
                # Пересылаем фото "до" владельцу
                await bot.forward_message(OWNER_ID, message.chat.id, message.message_id)
                await bot.send_message(OWNER_ID, f"📸 Фото ДО работ от мастера {message.from_user.full_name}, заявка #{order_id}")
                await message.answer(
                    "Фото ДО сохранено ✅\n\nПосле завершения работ нажми «Выполнено» и пришли фото ПОСЛЕ.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Отметить выполненной", callback_data=f"mdone_{order_id}")]
                    ])
                )
                await state.update_data(photo_stage=None, photo_order_id=None)
            return

        if stage == "after" and order_id:
            order = db_get_order(order_id)
            if order:
                client_id = order[1]
                # Отправляем фото "после" клиенту
                await bot.copy_message(client_id, message.chat.id, message.message_id)
                await bot.send_message(client_id, f"📸 Мастер прислал фото выполненной работы по заявке #{order_id}")
                # Пересылаем владельцу
                await bot.forward_message(OWNER_ID, message.chat.id, message.message_id)
                await bot.send_message(OWNER_ID, f"📸 Фото ПОСЛЕ работ от мастера {message.from_user.full_name}, заявка #{order_id}")
                await message.answer("Фото отправлено клиенту ✅")
                # Помечаем выполненной
                await _apply_status(message, order_id, "выполнено", user_id=client_id)
                await state.update_data(photo_stage=None, photo_order_id=None)
            return

        # Мастер прислал фото в режиме "выполнено" (после mdone)
        data2 = await state.get_data()
        if data2.get("awaiting_after_photo"):
            order_id2 = data2.get("after_photo_order_id")
            if order_id2:
                order = db_get_order(order_id2)
                if order:
                    client_id = order[1]
                    await bot.copy_message(client_id, message.chat.id, message.message_id)
                    await bot.send_message(client_id, f"📸 Фото выполненной работы по заявке #{order_id2}")
                    await bot.forward_message(OWNER_ID, message.chat.id, message.message_id)
                    await message.answer("Фото отправлено клиенту ✅")
                    await _apply_status(message, order_id2, "выполнено", user_id=client_id)
                    await state.update_data(awaiting_after_photo=False, after_photo_order_id=None)
                return
        return

    # Клиент отправляет фото
    await bot.forward_message(OWNER_ID, message.chat.id, message.message_id)
    with db_connect() as con:
        row = con.execute(
            "SELECT id FROM orders WHERE user_id=? AND status NOT IN ('выполнено','отменена') ORDER BY created_at DESC LIMIT 1",
            (message.from_user.id,)
        ).fetchone()
    if row:
        await bot.send_message(OWNER_ID, f"📸 Фото от клиента {message.from_user.full_name} к заявке #{row[0]}")
        await message.answer(
            f"Фото прикреплено к заявке #{row[0]}. Мастер свяжется в ближайшее время.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Главное меню", callback_data="back_main")]
            ])
        )
    else:
        await bot.send_message(OWNER_ID, f"📸 Фото от клиента {message.from_user.full_name} (без заявки)")
        await message.answer("Фото получено! Свяжемся в течение 15 минут.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Главное меню", callback_data="back_main")]
            ])
        )

# После нажатия "Выполнено" от мастера — просим фото после
@dp.message(Order.entering_comment)
async def enter_comment(message: Message, state: FSMContext):
    await state.update_data(comment=message.text)
    data = await state.get_data()
    await state.set_state(Order.confirm)
    await message.answer(order_summary(data) + "\n\nВсё верно?", reply_markup=kb_confirm())

# ─── МАСТЕР — ЗАДАЧИ ──────────────────────────────────────────────────────────
@dp.callback_query(F.data == "master_tasks")
async def master_tasks(cb: CallbackQuery):
    if not is_master(cb.from_user.id):
        return
    rows = db_get_master_orders(cb.from_user.id)
    if not rows:
        await cb.message.answer("У тебя пока нет заявок. Жди новых! 🔔")
        await cb.answer()
        return
    emoji_map = {"принята": "🆕", "в работе": "🔧", "выполнено": "✅", "отменена": "❌"}
    text = "📋 Твои заявки:\n\n"
    for row in rows:
        order_id, order_type, status, created_at, summary, visit_time = row
        label      = "Регулярное" if order_type == "regular" else "Разовый"
        visit_str  = f"\n   Время: {visit_time}" if visit_time else ""
        text      += f"{emoji_map.get(status,'📋')} #{order_id} — {label}\n   Статус: {status}{visit_str}\n   Дата: {created_at}\n\n"
    await cb.message.answer(text)
    await cb.answer()

@dp.callback_query(F.data == "master_stats")
async def master_stats(cb: CallbackQuery):
    if not is_master(cb.from_user.id):
        return
    with db_connect() as con:
        total = con.execute("SELECT COUNT(*) FROM orders WHERE master_id=?", (cb.from_user.id,)).fetchone()[0]
        done  = con.execute("SELECT COUNT(*) FROM orders WHERE master_id=? AND status='выполнено'", (cb.from_user.id,)).fetchone()[0]
        avg   = con.execute(
            "SELECT AVG(r.rating) FROM reviews r JOIN orders o ON r.order_id=o.id WHERE o.master_id=?",
            (cb.from_user.id,)
        ).fetchone()[0]
    rating_str = f"{avg:.1f}/5" if avg else "пока нет"
    await cb.message.answer(
        f"📊 Твоя статистика:\n\nВсего заявок: {total}\nВыполнено: {done}\nСредний отзыв: {rating_str}"
    )
    await cb.answer()

# ─── ПЕРЕЗВОНИТЕ МНЕ ──────────────────────────────────────────────────────────
@dp.callback_query(F.data == "callback_request")
async def callback_request(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.update_data(order_type="callback")
    await state.set_state(Order.entering_phone)
    await cb.message.answer("Оставьте номер — перезвоним в течение 15 минут:", reply_markup=kb_phone())
    await cb.answer()

async def _handle_callback(message: Message, phone: str, state: FSMContext):
    await state.clear()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    tag = f"@{message.from_user.username}" if message.from_user.username else f"ID: {message.from_user.id}"
    await bot.send_message(
        OWNER_ID,
        f"📞 ПЕРЕЗВОНИТЬ!\n\nКлиент: {message.from_user.full_name} ({tag})\nТелефон: {phone}\nВремя: {now}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать клиенту", callback_data=f"msg_0_{message.from_user.id}")]
        ])
    )
    await message.answer("Перезвоним в течение 15 минут! 📞", reply_markup=ReplyKeyboardRemove())
    await message.answer("Главное меню:", reply_markup=kb_main())

# ─── МОИ ЗАЯВКИ ───────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "my_orders")
async def my_orders(cb: CallbackQuery):
    rows = db_get_client_orders(cb.from_user.id)
    if not rows:
        await cb.message.answer(
            "У вас пока нет заявок.\n\nОставьте первую — ответим в течение 15 минут!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔧 Оставить заявку", callback_data="start_onetime")],
                [InlineKeyboardButton(text="⬅️ Главное меню",    callback_data="back_main")],
            ])
        )
        return
    emoji_map = {"принята": "🆕", "в работе": "🔧", "выполнено": "✅", "отменена": "❌"}
    text = "Ваши заявки:\n\n"
    for row in rows:
        order_id, order_type, status, created_at, master_name, visit_time = row
        label     = "Регулярное" if order_type == "regular" else "Разовый"
        master    = f"\n   Мастер: {master_name}" if master_name else ""
        visit_str = f"\n   Время визита: {visit_time}" if visit_time else ""
        text     += f"{emoji_map.get(status,'📋')} #{order_id} — {label}\n   Статус: {status}{master}{visit_str}\n   Дата: {created_at}\n\n"
    await cb.message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔧 Новая заявка",  callback_data="start_onetime")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_main")],
        ])
    )
    await cb.answer()

# ─── КОМАНДЫ ВЛАДЕЛЬЦА ────────────────────────────────────────────────────────
@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    await message.answer(db_get_stats())

@dp.message(Command("week"))
async def cmd_week(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    await message.answer(db_get_weekly_stats())

@dp.message(Command("orders"))
async def cmd_orders(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    rows = db_get_active_orders()
    if not rows:
        await message.answer("Нет активных заявок.")
        return
    for row in rows:
        order_id, user_id, order_type, status, created_at, name, username, master_name, city, visit_time, paid = row
        label      = "РЕГУЛЯРНОЕ" if order_type == "regular" else "РАЗОВЫЙ"
        tag        = f"@{username}" if username else f"ID {user_id}"
        emoji      = {"принята": "🆕", "в работе": "🔧"}.get(status, "📋")
        master_str = f"\nМастер: {master_name}" if master_name else "\nМастер: не назначен"
        city_str   = f"\nГород: {city}" if city else ""
        visit_str  = f"\nВремя визита: {visit_time}" if visit_time else ""
        paid_str   = "\n💳 Оплачено онлайн" if paid else ""
        await message.answer(
            f"{emoji} Заявка #{order_id} — {label}\n"
            f"Клиент: {name} ({tag})\n"
            f"Статус: {status}{master_str}{city_str}{visit_str}{paid_str}\n"
            f"Создана: {created_at}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="▶️ В работу",        callback_data=f"ss_{order_id}_{user_id}_inwork")],
                [InlineKeyboardButton(text="✅ Выполнено",        callback_data=f"ss_{order_id}_{user_id}_done")],
                [InlineKeyboardButton(text="💬 Написать клиенту", callback_data=f"msg_{order_id}_{user_id}")],
                [InlineKeyboardButton(text="❌ Отменить",         callback_data=f"ss_{order_id}_{user_id}_cancel")],
            ])
        )

@dp.message(Command("masters"))
async def cmd_masters(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    with db_connect() as con:
        rows = con.execute("SELECT id, name, username, registered_at FROM masters").fetchall()
    if not rows:
        await message.answer("Мастеров нет.\nДобавь их ID в MASTER_IDS в коде.")
        return
    text = f"👷 Мастера ({len(rows)}):\n\n"
    for row in rows:
        mid, name, username, reg = row
        tag  = f"@{username}" if username else f"ID: {mid}"
        with db_connect() as con:
            done = con.execute("SELECT COUNT(*) FROM orders WHERE master_id=? AND status='выполнено'", (mid,)).fetchone()[0]
        text += f"👤 {name} ({tag})\n   Выполнено: {done} заявок\n   С нами с: {reg}\n\n"
    await message.answer(text)

@dp.message(Command("status"))
async def cmd_set_status(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Формат: /status [номер] [статус]")
        return
    try:
        order_id = int(parts[1])
    except ValueError:
        await message.answer("Номер заявки должен быть числом")
        return
    status = parts[2].strip().lower()
    if status not in ORDER_STATUSES:
        await message.answer(f"Статусы: {', '.join(ORDER_STATUSES.keys())}")
        return
    await _apply_status(message, order_id, status)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Формат: /broadcast Текст сообщения")
        return
    user_ids = db_get_all_user_ids()
    sent, failed = 0, 0
    for uid in user_ids:
        try:
            await bot.send_message(uid, parts[1])
            sent += 1
        except Exception:
            failed += 1
    await message.answer(f"Рассылка завершена.\nОтправлено: {sent}\nНе доставлено: {failed}")

@dp.message(Command("prices"))
async def cmd_prices(message: Message):
    text = "💰 Прайс — Разовые работы\n\n"
    for svc in ONETIME_SERVICES.values():
        text += f"{svc['name']}\n{svc['price']} — {svc['time']}\n\n"
    await message.answer(text)

@dp.message(Command("contacts"))
async def cmd_contacts(message: Message):
    await message.answer(f"📍 Хаус Мастер\nТелефон: {PHONE}\nПн-Сб 8:00-21:00")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    if message.from_user.id == OWNER_ID:
        await message.answer(
            "Команды владельца:\n\n"
            "/orders — активные заявки\n"
            "/stats — статистика\n"
            "/week — сводка за 7 дней\n"
            "/masters — список мастеров\n"
            "/status [№] [статус] — сменить статус\n"
            "/broadcast [текст] — рассылка\n\n"
            "Статусы: принята, в работе, выполнено, отменена"
        )
    elif is_master(message.from_user.id):
        await message.answer("Меню мастера:", reply_markup=kb_master_main())
    else:
        await message.answer("/start — главное меню\n/prices — прайс\n/contacts — контакты", reply_markup=kb_main())

# ─── УПРАВЛЕНИЕ СТАТУСАМИ ─────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("ss_"))
async def cb_set_status(cb: CallbackQuery):
    if cb.from_user.id != OWNER_ID:
        return
    parts    = cb.data.split("_")
    order_id = int(parts[1])
    user_id  = int(parts[2])
    code     = parts[3]
    status_map = {"inwork": "в работе", "done": "выполнено", "cancel": "отменена"}
    status   = status_map.get(code, code)
    await _apply_status(cb.message, order_id, status, user_id=user_id)
    await cb.answer(f"Статус: {status}")

async def _apply_status(msg, order_id: int, status: str, user_id: int = None):
    order = db_get_order(order_id)
    if not order:
        await msg.answer(f"Заявка #{order_id} не найдена")
        return
    db_update_status(order_id, status)
    client_id   = user_id or order[1]
    status_text = ORDER_STATUSES.get(status, status)
    try:
        await bot.send_message(
            client_id,
            f"📋 Заявка #{order_id}\nСтатус: {status_text}\n\nВопросы? Звоните: {PHONE}"
        )
        if status == "выполнено":
            await bot.send_message(client_id, "Оцените работу мастера:", reply_markup=kb_rating(order_id))
    except Exception:
        pass
    await msg.answer(f"Статус заявки #{order_id} → «{status}», клиент уведомлён.")

@dp.callback_query(F.data.startswith("msg_"))
async def cb_message_client(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != OWNER_ID:
        return
    parts    = cb.data.split("_")
    order_id = int(parts[1])
    user_id  = int(parts[2])
    await state.update_data(reply_order_id=order_id, reply_user_id=user_id)
    await state.set_state(OwnerReply.waiting_message)
    await cb.message.answer(
        f"Введите сообщение для клиента по заявке #{order_id}:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_reply")]
        ])
    )
    await cb.answer()

@dp.callback_query(F.data == "cancel_reply")
async def cancel_reply(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Отменено.")
    await cb.answer()

@dp.message(OwnerReply.waiting_message)
async def send_message_to_client(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    data     = await state.get_data()
    order_id = data.get("reply_order_id")
    user_id  = data.get("reply_user_id")
    await state.clear()
    try:
        await bot.send_message(
            user_id,
            f"💬 Сообщение от Хаус Мастер по заявке #{order_id}:\n\n{message.text}\n\nЗвоните: {PHONE}"
        )
        await message.answer("Сообщение отправлено клиенту.")
    except Exception:
        await message.answer("Не удалось отправить — клиент заблокировал бота.")

# ─── ПРАЙС / КОНТАКТЫ / МЕНЮ ──────────────────────────────────────────────────
@dp.callback_query(F.data == "show_prices")
async def show_prices(cb: CallbackQuery):
    text = "💰 Прайс — Разовые работы\n\n"
    for svc in ONETIME_SERVICES.values():
        text += f"{svc['name']}\n{svc['price']} — {svc['time']}\n\n"
    text += "💡 Точная стоимость — после фото или описания"
    await cb.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔧 Оставить заявку", callback_data="start_onetime")],
        [InlineKeyboardButton(text="⬅️ Главное меню",   callback_data="back_main")],
    ]))
    await cb.answer()

@dp.callback_query(F.data == "call_us")
async def call_us(cb: CallbackQuery):
    await cb.message.answer(
        f"☎️ Позвоните нам:\n\n{PHONE}\n\nПн-Сб 8:00-21:00\nЭкстренные выезды — круглосуточно",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔧 Оставить заявку", callback_data="start_onetime")],
            [InlineKeyboardButton(text="⬅️ Главное меню",   callback_data="back_main")],
        ])
    )
    await cb.answer()

@dp.callback_query(F.data == "back_main")
async def back_main(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    if is_master(cb.from_user.id):
        await cb.message.answer("Меню мастера:", reply_markup=kb_master_main())
    else:
        await cb.message.answer("Главное меню:", reply_markup=kb_main())
    await cb.answer()

# ─── ОТЗЫВЫ ───────────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("rev_"))
async def handle_review(cb: CallbackQuery, state: FSMContext):
    parts    = cb.data.split("_")
    order_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    action   = parts[2] if len(parts) > 2 else "skip"
    if action == "skip":
        await cb.message.answer("Спасибо! Будем рады снова помочь. 🏠", reply_markup=kb_main())
        await cb.answer()
        return
    if action.isdigit():
        rating = int(action)
        await state.update_data(review_rating=rating, review_order_id=order_id)
        await state.set_state(Review.waiting_comment)
        await cb.message.answer(
            f"Спасибо за оценку {'⭐' * rating}!\n\nОставьте комментарий или нажмите Пропустить:",
            reply_markup=kb_review_comment()
        )
    await cb.answer()

@dp.callback_query(F.data == "rev_comment_skip", Review.waiting_comment)
async def review_comment_skip(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    db_add_review(cb.from_user.id, data.get("review_order_id", 0), data.get("review_rating", 5), "")
    await state.clear()
    await cb.message.answer("Спасибо за отзыв! 🙏", reply_markup=kb_main())
    await bot.send_message(OWNER_ID,
        f"⭐ Отзыв от {cb.from_user.full_name}:\n"
        f"Оценка: {'⭐' * data.get('review_rating', 5)}\nЗаявка #{data.get('review_order_id','?')}"
    )
    await cb.answer()

@dp.message(Review.waiting_comment)
async def review_comment(message: Message, state: FSMContext):
    data = await state.get_data()
    db_add_review(message.from_user.id, data.get("review_order_id", 0), data.get("review_rating", 5), message.text)
    await state.clear()
    await message.answer("Спасибо за отзыв! 🙏", reply_markup=kb_main())
    await bot.send_message(OWNER_ID,
        f"⭐ Отзыв от {message.from_user.full_name}:\n"
        f"Оценка: {'⭐' * data.get('review_rating', 5)}\n"
        f"Комментарий: {message.text}\nЗаявка #{data.get('review_order_id','?')}"
    )

# ─── FALLBACK ─────────────────────────────────────────────────────────────────
@dp.message()
async def fallback(message: Message, state: FSMContext):
    if await state.get_state():
        return
    if is_master(message.from_user.id):
        await message.answer("Меню мастера:", reply_markup=kb_master_main())
        return
    await message.answer("Воспользуйтесь меню:", reply_markup=kb_main())

# ─── ФОНОВЫЕ ЗАДАЧИ ───────────────────────────────────────────────────────────
async def reminder_task():
    await asyncio.sleep(60)
    while True:
        try:
            pending = db_get_pending_orders(minutes=120)
            for order in pending:
                label = "РЕГУЛЯРНОЕ" if order["type"] == "regular" else "РАЗОВЫЙ"
                await bot.send_message(
                    OWNER_ID,
                    f"⏰ НАПОМИНАНИЕ\n\nЗаявка #{order['id']} ({label}) без мастера уже 2 часа!\nСоздана: {order['created_at']}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="▶️ В работу", callback_data=f"ss_{order['id']}_{order['user_id']}_inwork")],
                        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"ss_{order['id']}_{order['user_id']}_cancel")],
                    ])
                )
        except Exception as e:
            logging.error(f"Reminder error: {e}")
        await asyncio.sleep(30 * 60)

async def weekly_report_task():
    while True:
        now          = datetime.now()
        days_ahead   = (7 - now.weekday()) % 7 or 7
        next_monday  = now.replace(hour=9, minute=0, second=0, microsecond=0)
        next_monday  = next_monday.replace(day=now.day + days_ahead)
        wait_seconds = (next_monday - now).total_seconds()
        if wait_seconds < 0:
            wait_seconds += 7 * 24 * 3600
        await asyncio.sleep(wait_seconds)
        try:
            await bot.send_message(OWNER_ID, db_get_weekly_stats())
        except Exception as e:
            logging.error(f"Weekly report error: {e}")

# ─── ЗАПУСК ───────────────────────────────────────────────────────────────────
async def main():
    db_init()
    print("🏠 Хаус Мастер Bot v3.0 запущен!")
    asyncio.create_task(reminder_task())
    asyncio.create_task(weekly_report_task())
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
