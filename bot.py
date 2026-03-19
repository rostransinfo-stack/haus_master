"""
Хаус Мастер — Telegram Bot v1.0
Архитектура на базе VTehnike 24 bot v10.0

+ SQLite база данных (не теряется при передеплое)
+ Два типа заявок: разовый ремонт и регулярное обслуживание
+ Статусы заявок с уведомлением клиенту
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
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import CommandStart, Command

# ─── НАСТРОЙКИ ────────────────────────────────────────────────────────────────
BOT_TOKEN = "8437642100:AAF6NL71wkN77uctXCgTGLFHf1gITDD57-M"          # Токен от @BotFather
OWNER_ID  = 125380747             # Ваш Telegram ID от @userinfobot
DB_FILE   = "housemaster.db"
PHONE     = "+7 (992) 350-80-08" # Телефон Хаус Мастер
CITY      = "по всей России"      # Зона охвата
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
                first_seen  TEXT,
                last_seen   TEXT,
                visits      INTEGER DEFAULT 1
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                order_type  TEXT,
                summary     TEXT,
                status      TEXT DEFAULT 'принята',
                created_at  TEXT,
                updated_at  TEXT
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
        con.commit()

def db_track_user(user) -> bool:
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db_connect() as con:
        existing = con.execute("SELECT id FROM users WHERE id=?", (user.id,)).fetchone()
        if not existing:
            con.execute(
                "INSERT INTO users (id, name, username, first_seen, last_seen, visits) VALUES (?,?,?,?,?,1)",
                (user.id, user.full_name, user.username or "", now, now)
            )
            con.commit()
            return True
        else:
            con.execute(
                "UPDATE users SET last_seen=?, visits=visits+1, name=?, username=? WHERE id=?",
                (now, user.full_name, user.username or "", user.id)
            )
            con.commit()
            return False

def db_add_order(user_id: int, order_type: str, summary: str) -> int:
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db_connect() as con:
        cur = con.execute(
            "INSERT INTO orders (user_id, order_type, summary, status, created_at, updated_at) VALUES (?,?,?,?,?,?)",
            (user_id, order_type, summary, "принята", now, now)
        )
        con.commit()
        return cur.lastrowid

def db_update_status(order_id: int, status: str):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db_connect() as con:
        con.execute(
            "UPDATE orders SET status=?, updated_at=? WHERE id=?",
            (status, now, order_id)
        )
        con.commit()

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

def db_get_stats() -> str:
    with db_connect() as con:
        total_users   = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_orders  = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        onetime_cnt   = con.execute("SELECT COUNT(*) FROM orders WHERE order_type='onetime'").fetchone()[0]
        regular_cnt   = con.execute("SELECT COUNT(*) FROM orders WHERE order_type='regular'").fetchone()[0]
        done_cnt      = con.execute("SELECT COUNT(*) FROM orders WHERE status='выполнено'").fetchone()[0]
        avg_rating    = con.execute("SELECT AVG(rating) FROM reviews").fetchone()[0]
        review_cnt    = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
        recent = con.execute(
            "SELECT name, username, last_seen FROM users ORDER BY last_seen DESC LIMIT 5"
        ).fetchall()
    rating_str = f"{avg_rating:.1f} / 5 ({review_cnt} отзывов)" if avg_rating else "пока нет"
    recent_str = ""
    for name, username, last_seen in recent:
        tag = f"@{username}" if username else ""
        recent_str += f"  {name} {tag} — был {last_seen}\n"
    return (
        f"📊 Статистика Хаус Мастер\n\n"
        f"Пользователей:       {total_users}\n"
        f"Всего заявок:        {total_orders}\n"
        f"  разовый ремонт:    {onetime_cnt}\n"
        f"  регулярное обсл.:  {regular_cnt}\n"
        f"  выполнено:         {done_cnt}\n"
        f"Средний отзыв:       {rating_str}\n\n"
        f"Последние 5 активных:\n{recent_str}"
    )

def db_get_weekly_stats() -> str:
    with db_connect() as con:
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%d.%m.%Y")
        new_users    = con.execute("SELECT COUNT(*) FROM users WHERE first_seen >= ?", (week_ago,)).fetchone()[0]
        new_orders   = con.execute("SELECT COUNT(*) FROM orders WHERE created_at >= ?", (week_ago,)).fetchone()[0]
        onetime_cnt  = con.execute("SELECT COUNT(*) FROM orders WHERE order_type='onetime' AND created_at >= ?", (week_ago,)).fetchone()[0]
        regular_cnt  = con.execute("SELECT COUNT(*) FROM orders WHERE order_type='regular' AND created_at >= ?", (week_ago,)).fetchone()[0]
        done_cnt     = con.execute("SELECT COUNT(*) FROM orders WHERE status='выполнено' AND updated_at >= ?", (week_ago,)).fetchone()[0]
        avg_rating   = con.execute("SELECT AVG(rating) FROM reviews WHERE created_at >= ?", (week_ago,)).fetchone()[0]
    rating_str = f"{avg_rating:.1f}/5" if avg_rating else "нет"
    return (
        f"📅 Сводка за 7 дней\n"
        f"{datetime.now().strftime('%d.%m.%Y')}\n\n"
        f"Новых пользователей: {new_users}\n"
        f"Новых заявок:        {new_orders}\n"
        f"  разовый ремонт:    {onetime_cnt}\n"
        f"  регулярное обсл.:  {regular_cnt}\n"
        f"Выполнено:           {done_cnt}\n"
        f"Средний отзыв:       {rating_str}"
    )

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

def db_get_active_orders() -> list:
    with db_connect() as con:
        rows = con.execute(
            """SELECT o.id, o.user_id, o.order_type, o.status, o.created_at,
                      u.name, u.username
               FROM orders o
               LEFT JOIN users u ON o.user_id = u.id
               WHERE o.status NOT IN ('выполнено', 'отменена')
               ORDER BY o.created_at DESC"""
        ).fetchall()
    return rows

def db_get_order_user_id(order_id: int) -> int:
    with db_connect() as con:
        row = con.execute("SELECT user_id FROM orders WHERE id=?", (order_id,)).fetchone()
    return row[0] if row else None

def db_get_client_orders(user_id: int) -> list:
    with db_connect() as con:
        rows = con.execute(
            """SELECT id, order_type, status, created_at
               FROM orders WHERE user_id=?
               ORDER BY created_at DESC LIMIT 10""",
            (user_id,)
        ).fetchall()
    return rows

def db_get_all_user_ids() -> list:
    with db_connect() as con:
        rows = con.execute("SELECT id FROM users").fetchall()
    return [r[0] for r in rows]

# ─── РАБОЧЕЕ ВРЕМЯ ────────────────────────────────────────────────────────────
def is_working_hours() -> bool:
    """Пн-Сб 8:00-21:00"""
    now = datetime.now()
    if now.weekday() == 6:
        return False
    return 8 <= now.hour < 21

# ─── ДАННЫЕ — УСЛУГИ ──────────────────────────────────────────────────────────
ONETIME_SERVICES = {
    "lamp":        {"name": "🔦 Замена ламп / люстр",         "price": "от 500 руб.",    "time": "30 мин"},
    "furniture":   {"name": "🪑 Сборка / ремонт мебели",      "price": "от 1 000 руб.",  "time": "1-3 часа"},
    "paint":       {"name": "🎨 Подкраска стен и поверхностей","price": "от 1 500 руб.",  "time": "1-4 часа"},
    "plumbing":    {"name": "🚿 Сантехника (кран, унитаз)",    "price": "от 1 500 руб.",  "time": "1-2 часа"},
    "door":        {"name": "🚪 Установка / регулировка дверей","price": "от 1 000 руб.", "time": "1-2 часа"},
    "shelf":       {"name": "📦 Полки, карнизы, крючки",       "price": "от 500 руб.",   "time": "30-60 мин"},
    "tv":          {"name": "📺 Крепление ТВ на стену",        "price": "от 1 500 руб.",  "time": "1 час"},
    "tile":        {"name": "🧱 Укладка / замена плитки",      "price": "от 3 000 руб.",  "time": "от 2 часов"},
    "electrical":  {"name": "⚡ Электрика (розетки, выключат.)","price": "от 1 000 руб.", "time": "1-2 часа"},
    "floor":       {"name": "🪵 Скрип пола / ламинат",         "price": "от 2 000 руб.",  "time": "от 2 часов"},
    "caulk":       {"name": "🪟 Герметизация окон / щелей",    "price": "от 800 руб.",    "time": "1 час"},
    "other":       {"name": "🔨 Другое — уточним",             "price": "по договорённости","time": "уточним"},
}

REGULAR_SERVICES = {
    "cafe_basic":    {"name": "☕ Кафе / ресторан — базовый пакет",  "price": "от 8 000 руб./мес.",  "visits": "2 раза/мес."},
    "cafe_full":     {"name": "☕ Кафе / ресторан — полный пакет",   "price": "от 15 000 руб./мес.", "visits": "еженедельно"},
    "office_basic":  {"name": "🏢 Офис — базовый пакет",             "price": "от 5 000 руб./мес.",  "visits": "2 раза/мес."},
    "office_full":   {"name": "🏢 Офис — полный пакет",              "price": "от 10 000 руб./мес.", "visits": "еженедельно"},
    "apartment":     {"name": "🏠 Квартира / дом",                   "price": "от 3 000 руб./мес.",  "visits": "1 раз/мес."},
    "custom":        {"name": "📋 Индивидуальные условия",           "price": "по договорённости",    "visits": "по согласованию"},
}

URGENCY = {
    "standard": {"name": "Стандарт (1-2 дня)",        "mult": "x1"},
    "urgent":   {"name": "Срочно (сегодня) +30%",     "mult": "+30%"},
    "express":  {"name": "Экстренно (2 часа) +60%",   "mult": "+60%"},
}

PAYMENT = {
    "cash":       {"name": "💵 Наличные"},
    "card":       {"name": "💳 Карта / СБП"},
    "bank_nds":   {"name": "🏦 Безнал с НДС"},
    "bank_nonds": {"name": "🏦 Безнал без НДС"},
}

ORDER_STATUSES = {
    "принята":   "Заявка принята, скоро свяжемся",
    "в работе":  "Мастер назначен, едет к вам",
    "выполнено": "Работа выполнена",
    "отменена":  "Заявка отменена",
}

# ─── СОСТОЯНИЯ ────────────────────────────────────────────────────────────────
class Order(StatesGroup):
    choosing_service  = State()   # выбор услуги разового ремонта
    choosing_regular  = State()   # выбор пакета регулярного обслуживания
    choosing_urgency  = State()   # срочность (только для разового)
    entering_address  = State()   # адрес
    entering_phone    = State()   # телефон
    choosing_payment  = State()   # способ оплаты
    entering_comment  = State()   # комментарий
    confirm           = State()   # подтверждение

class OwnerReply(StatesGroup):
    waiting_message = State()

class Review(StatesGroup):
    waiting_rating  = State()
    waiting_comment = State()

# ─── ХЕЛПЕРЫ ──────────────────────────────────────────────────────────────────
def order_summary(data: dict) -> str:
    order_type = data.get("order_type", "onetime")
    lines = ["📋 Ваша заявка:\n"]
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
    lines.append(f"Адрес: {data.get('address', '—')}")
    lines.append(f"Телефон: {data.get('phone', '—')}")
    lines.append(f"Оплата: {PAYMENT.get(data.get('payment','cash'), {}).get('name', '—')}")
    comment = data.get("comment", "нет")
    if comment and comment != "нет":
        lines.append(f"Комментарий: {comment}")
    return "\n".join(lines)

async def notify_owner(data: dict, user, order_id: int):
    order_type = data.get("order_type", "onetime")
    label = "РАЗОВЫЙ" if order_type == "onetime" else "РЕГУЛЯРНОЕ"
    tag = f"@{user.username}" if user.username else f"ID: {user.id}"
    header = (
        f"🆕 НОВАЯ ЗАЯВКА #{order_id} — {label}\n"
        f"Клиент: {user.full_name} ({tag})\n\n"
    )
    body = order_summary(data)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ В работу",         callback_data=f"ss_{order_id}_{user.id}_inwork")],
        [InlineKeyboardButton(text="✅ Выполнено",         callback_data=f"ss_{order_id}_{user.id}_done")],
        [InlineKeyboardButton(text="💬 Написать клиенту",  callback_data=f"msg_{order_id}_{user.id}")],
        [InlineKeyboardButton(text="❌ Отменить",          callback_data=f"ss_{order_id}_{user.id}_cancel")],
    ])
    await bot.send_message(OWNER_ID, header + body, reply_markup=kb)

# ─── КЛАВИАТУРЫ ───────────────────────────────────────────────────────────────
def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔧 Разовый ремонт",        callback_data="start_onetime")],
        [InlineKeyboardButton(text="🔄 Регулярное обслуживание",callback_data="start_regular")],
        [InlineKeyboardButton(text="📋 Мои заявки",             callback_data="my_orders")],
        [InlineKeyboardButton(text="📞 Перезвоните мне",        callback_data="callback_request")],
        [InlineKeyboardButton(text="💰 Прайс-лист",             callback_data="show_prices")],
        [InlineKeyboardButton(text="☎️ Позвонить нам",          callback_data="call_us")],
    ])

def kb_onetime_services():
    rows = []
    items = list(ONETIME_SERVICES.items())
    for i in range(0, len(items), 2):
        row = []
        for key, val in items[i:i+2]:
            row.append(InlineKeyboardButton(text=val["name"], callback_data=f"svc_{key}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_regular_services():
    rows = []
    items = list(REGULAR_SERVICES.items())
    for i in range(0, len(items), 2):
        row = []
        for key, val in items[i:i+2]:
            row.append(InlineKeyboardButton(text=val["name"], callback_data=f"reg_{key}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_urgency():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Стандарт (1-2 дня)",      callback_data="urg_standard")],
        [InlineKeyboardButton(text="⚡ Срочно (сегодня) +30%",   callback_data="urg_urgent")],
        [InlineKeyboardButton(text="🚨 Экстренно (2 часа) +60%", callback_data="urg_express")],
        [InlineKeyboardButton(text="⬅️ Назад",                   callback_data="start_onetime")],
    ])

def kb_payment():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Наличные",      callback_data="pay_cash")],
        [InlineKeyboardButton(text="💳 Карта / СБП",   callback_data="pay_card")],
        [InlineKeyboardButton(text="🏦 Безнал с НДС",  callback_data="pay_bank_nds")],
        [InlineKeyboardButton(text="🏦 Безнал без НДС",callback_data="pay_bank_nonds")],
    ])

def kb_skip():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пропустить ➡️", callback_data="skip_comment")]
    ])

def kb_confirm():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить заявку", callback_data="confirm_yes")],
        [InlineKeyboardButton(text="✏️ Изменить",         callback_data="confirm_no")],
    ])

def kb_phone():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить мой номер", request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True
    )

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
    db_track_user(message.from_user)
    name = message.from_user.first_name or "Гость"
    off_hours_note = ""
    if not is_working_hours():
        off_hours_note = (
            "\n⚠️ Сейчас мы не работаем (Пн-Сб 8:00-21:00).\n"
            f"Заявку можно оставить — ответим утром!\nЭкстренно: {PHONE}\n"
        )
    await message.answer(
        f"Привет, {name}! 👋 Добро пожаловать в Хаус Мастер!\n\n"
        "Мы быстро решаем бытовые задачи:\n"
        "🔦 Лампы, мебель, сантехника, электрика\n"
        "🎨 Подкраска, герметизация, плитка\n"
        "🔄 Регулярное обслуживание кафе и офисов\n\n"
        "✅ Приедем в удобное время\n"
        "✅ Фиксированные цены без скрытых доплат\n"
        "✅ Гарантия на все работы\n"
        + off_hours_note +
        "\nЧем могу помочь?",
        reply_markup=kb_main()
    )

# ─── КОМАНДЫ ВЛАДЕЛЬЦА ────────────────────────────────────────────────────────
@dp.message(Command("help"))
async def cmd_help(message: Message):
    if message.from_user.id == OWNER_ID:
        await message.answer(
            "Команды владельца:\n\n"
            "/orders — активные заявки\n"
            "/stats — статистика всего времени\n"
            "/week — сводка за 7 дней\n"
            "/status [№] [статус] — сменить статус\n"
            "/broadcast [текст] — рассылка всем\n"
            "/prices — прайс услуг\n"
            "/contacts — контакты\n\n"
            "Статусы: принята, в работе, выполнено, отменена"
        )
    else:
        await message.answer(
            "Доступные команды:\n\n"
            "/start — главное меню\n"
            "/prices — прайс на услуги\n"
            "/contacts — наши контакты",
            reply_markup=kb_main()
        )

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
        order_id, user_id, order_type, status, created_at, name, username = row
        label  = "РЕГУЛЯРНОЕ" if order_type == "regular" else "РАЗОВЫЙ"
        tag    = f"@{username}" if username else f"ID {user_id}"
        emoji  = {"принята": "🆕", "в работе": "🔧"}.get(status, "📋")
        await message.answer(
            f"{emoji} Заявка #{order_id} — {label}\n"
            f"Клиент: {name} ({tag})\n"
            f"Статус: {status}\n"
            f"Создана: {created_at}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="▶️ В работу",         callback_data=f"ss_{order_id}_{user_id}_inwork")],
                [InlineKeyboardButton(text="✅ Выполнено",         callback_data=f"ss_{order_id}_{user_id}_done")],
                [InlineKeyboardButton(text="💬 Написать клиенту",  callback_data=f"msg_{order_id}_{user_id}")],
                [InlineKeyboardButton(text="❌ Отменить",          callback_data=f"ss_{order_id}_{user_id}_cancel")],
            ])
        )

@dp.message(Command("status"))
async def cmd_set_status(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Формат: /status [номер заявки] [статус]")
        return
    try:
        order_id = int(parts[1])
    except ValueError:
        await message.answer("Номер заявки должен быть числом")
        return
    status = parts[2].strip().lower()
    if status not in ORDER_STATUSES:
        await message.answer(f"Неверный статус. Доступные: {', '.join(ORDER_STATUSES.keys())}")
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
    text     = parts[1]
    user_ids = db_get_all_user_ids()
    sent, failed = 0, 0
    for uid in user_ids:
        try:
            await bot.send_message(uid, text)
            sent += 1
        except Exception:
            failed += 1
    await message.answer(f"Рассылка завершена.\nОтправлено: {sent}\nНе доставлено: {failed}")

@dp.message(Command("prices"))
async def cmd_prices(message: Message):
    text = "💰 Прайс — Разовые работы\n\n"
    for svc in ONETIME_SERVICES.values():
        text += f"{svc['name']}\n{svc['price']} — {svc['time']}\n\n"
    text += "Точная стоимость — после фото или описания"
    await message.answer(text)

@dp.message(Command("contacts"))
async def cmd_contacts(message: Message):
    await message.answer(
        f"📍 Контакты Хаус Мастер\n\n"
        f"Телефон: {PHONE}\n"
        f"Зона работы: {CITY}\n\n"
        f"Пн-Сб 8:00-21:00\n"
        f"Экстренные вызовы — круглосуточно"
    )

# ─── УПРАВЛЕНИЕ СТАТУСАМИ (КНОПКИ) ───────────────────────────────────────────
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
            f"📋 Заявка #{order_id}\n\n"
            f"Статус изменён: {status_text}\n\n"
            f"Вопросы? Звоните: {PHONE}"
        )
        if status == "выполнено":
            await bot.send_message(
                client_id,
                "Оцените работу мастера — это займёт 30 секунд:",
                reply_markup=kb_rating(order_id)
            )
    except Exception:
        pass
    await msg.answer(f"Статус заявки #{order_id} изменён на «{status}», клиент уведомлён.")

# ─── СООБЩЕНИЯ КЛИЕНТУ ────────────────────────────────────────────────────────
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
            f"💬 Сообщение от Хаус Мастер по заявке #{order_id}:\n\n"
            f"{message.text}\n\n"
            f"Вопросы? Звоните: {PHONE}"
        )
        await message.answer(f"Сообщение отправлено клиенту по заявке #{order_id}.")
    except Exception:
        await message.answer("Не удалось отправить — клиент заблокировал бота.")

# ─── РАЗОВЫЙ РЕМОНТ ───────────────────────────────────────────────────────────
@dp.callback_query(F.data == "start_onetime")
async def start_onetime(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.update_data(order_type="onetime")
    await state.set_state(Order.choosing_service)
    await cb.message.answer("Выберите услугу:", reply_markup=kb_onetime_services())
    await cb.answer()

@dp.callback_query(F.data.startswith("svc_"), Order.choosing_service)
async def choose_service(cb: CallbackQuery, state: FSMContext):
    key = cb.data.replace("svc_", "")
    svc = ONETIME_SERVICES[key]
    await state.update_data(service=key)
    await state.set_state(Order.choosing_urgency)
    await cb.message.answer(
        f"Выбрано: {svc['name']}\n"
        f"Цена: {svc['price']} — {svc['time']}\n\n"
        "Выберите срочность:",
        reply_markup=kb_urgency()
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("urg_"), Order.choosing_urgency)
async def choose_urgency(cb: CallbackQuery, state: FSMContext):
    key = cb.data.replace("urg_", "")
    await state.update_data(urgency=key)
    await state.set_state(Order.entering_address)
    await cb.message.answer(
        f"Срочность: {URGENCY[key]['name']}\n\nУкажите адрес (улица, дом, квартира):"
    )
    await cb.answer()

# ─── РЕГУЛЯРНОЕ ОБСЛУЖИВАНИЕ ──────────────────────────────────────────────────
@dp.callback_query(F.data == "start_regular")
async def start_regular(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.update_data(order_type="regular")
    await state.set_state(Order.choosing_regular)
    await cb.message.answer(
        "Регулярное обслуживание — мастер приезжает по расписанию и устраняет всё, что накопилось.\n\n"
        "Выберите пакет:",
        reply_markup=kb_regular_services()
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("reg_"), Order.choosing_regular)
async def choose_regular(cb: CallbackQuery, state: FSMContext):
    key = cb.data.replace("reg_", "")
    svc = REGULAR_SERVICES[key]
    await state.update_data(regular_service=key)
    await state.set_state(Order.entering_address)
    await cb.message.answer(
        f"Выбрано: {svc['name']}\n"
        f"Стоимость: {svc['price']}\n"
        f"Периодичность: {svc['visits']}\n\n"
        "Укажите адрес объекта:"
    )
    await cb.answer()

# ─── ОБЩИЙ СБОР ДАННЫХ ────────────────────────────────────────────────────────
@dp.message(Order.entering_address)
async def enter_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text)
    await state.set_state(Order.entering_phone)
    await message.answer("Укажите номер телефона для связи:", reply_markup=kb_phone())

@dp.message(Order.entering_phone, F.contact)
async def enter_phone_contact(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("order_type") == "callback":
        await _handle_callback(message, message.contact.phone_number, state)
        return
    await state.update_data(phone=message.contact.phone_number)
    await state.set_state(Order.choosing_payment)
    await message.answer("Выберите форму оплаты:", reply_markup=ReplyKeyboardRemove())
    await message.answer("👇", reply_markup=kb_payment())

@dp.message(Order.entering_phone)
async def enter_phone_text(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("order_type") == "callback":
        await _handle_callback(message, message.text, state)
        return
    await state.update_data(phone=message.text)
    await state.set_state(Order.choosing_payment)
    await message.answer("Выберите форму оплаты:", reply_markup=ReplyKeyboardRemove())
    await message.answer("👇", reply_markup=kb_payment())

@dp.callback_query(F.data.startswith("pay_"), Order.choosing_payment)
async def choose_payment(cb: CallbackQuery, state: FSMContext):
    key = cb.data.replace("pay_", "")
    await state.update_data(payment=key)
    await state.set_state(Order.entering_comment)
    await cb.message.answer(
        f"Оплата: {PAYMENT[key]['name']}\n\nДобавьте комментарий или нажмите Пропустить:",
        reply_markup=kb_skip()
    )
    await cb.answer()

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

# ─── ПОДТВЕРЖДЕНИЕ ────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "confirm_yes", Order.confirm)
async def confirm_order(cb: CallbackQuery, state: FSMContext):
    data     = await state.get_data()
    summary  = order_summary(data)
    order_id = db_add_order(cb.from_user.id, data.get("order_type", "onetime"), summary)
    await notify_owner(data, cb.from_user, order_id)
    await state.clear()

    order_type = data.get("order_type", "onetime")
    if order_type == "onetime":
        svc = ONETIME_SERVICES.get(data.get("service", ""), {})
        urg = URGENCY.get(data.get("urgency", "standard"), {})
        price_hint = f"\nПримерная стоимость: {svc.get('price', '')} ({urg.get('mult', '')})\n"
        extra = "Можете прислать фото — поможет точнее назвать стоимость.\n\n"
    else:
        svc = REGULAR_SERVICES.get(data.get("regular_service", ""), {})
        price_hint = f"\nСтоимость: {svc.get('price', '')}\n"
        extra = "Мы свяжемся для согласования расписания визитов.\n\n"

    await cb.message.answer(
        f"✅ Заявка #{order_id} принята!\n"
        f"{price_hint}\n"
        f"Свяжемся с вами в течение 15 минут.\n\n"
        f"{extra}"
        f"Спасибо, что выбрали Хаус Мастер! 🏠",
        reply_markup=kb_main()
    )

@dp.callback_query(F.data == "confirm_no", Order.confirm)
async def cancel_order(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Хорошо, начнём заново:", reply_markup=kb_main())

# ─── ПЕРЕЗВОНИТЕ МНЕ ──────────────────────────────────────────────────────────
@dp.callback_query(F.data == "callback_request")
async def callback_request(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.update_data(order_type="callback")
    await state.set_state(Order.entering_phone)
    await cb.message.answer(
        "Оставьте номер — перезвоним в течение 15 минут:",
        reply_markup=kb_phone()
    )
    await cb.answer()

async def _handle_callback(message: Message, phone: str, state: FSMContext):
    await state.clear()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    tag = f"@{message.from_user.username}" if message.from_user.username else f"ID: {message.from_user.id}"
    await bot.send_message(
        OWNER_ID,
        f"📞 ПЕРЕЗВОНИТЬ!\n\n"
        f"Клиент: {message.from_user.full_name} ({tag})\n"
        f"Телефон: {phone}\n"
        f"Время: {now}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать клиенту", callback_data=f"msg_0_{message.from_user.id}")]
        ])
    )
    await message.answer(
        "Отлично! Перезвоним в течение 15 минут. 📞",
        reply_markup=ReplyKeyboardRemove()
    )
    await message.answer("Главное меню:", reply_markup=kb_main())

# ─── МОИ ЗАЯВКИ ───────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "my_orders")
async def my_orders(cb: CallbackQuery):
    rows = db_get_client_orders(cb.from_user.id)
    if not rows:
        await cb.message.answer(
            "У вас пока нет заявок.\n\nОставьте первую — ответим в течение 15 минут!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔧 Оставить заявку",  callback_data="start_onetime")],
                [InlineKeyboardButton(text="⬅️ Главное меню",     callback_data="back_main")],
            ])
        )
        return
    emoji_map = {"принята": "🆕", "в работе": "🔧", "выполнено": "✅", "отменена": "❌"}
    text = "Ваши заявки:\n\n"
    for row in rows:
        order_id, order_type, status, created_at = row
        label = "Регулярное" if order_type == "regular" else "Разовый"
        text += f"{emoji_map.get(status,'📋')} #{order_id} — {label}\n   Статус: {status}\n   Дата: {created_at}\n\n"
    await cb.message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔧 Новая заявка",  callback_data="start_onetime")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_main")],
        ])
    )
    await cb.answer()

# ─── ПРАЙС / КОНТАКТЫ ─────────────────────────────────────────────────────────
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
        f"☎️ Позвоните нам:\n\n"
        f"{PHONE}\n\n"
        f"Пн-Сб 8:00-21:00\n"
        f"Экстренные выезды — круглосуточно\n\n"
        f"Или оставьте заявку — перезвоним:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔧 Оставить заявку", callback_data="start_onetime")],
            [InlineKeyboardButton(text="⬅️ Главное меню",   callback_data="back_main")],
        ])
    )
    await cb.answer()

@dp.callback_query(F.data == "back_main")
async def back_main(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Главное меню:", reply_markup=kb_main())
    await cb.answer()

# ─── ФОТО ─────────────────────────────────────────────────────────────────────
@dp.message(F.photo)
async def handle_photo(message: Message):
    await bot.forward_message(OWNER_ID, message.chat.id, message.message_id)
    with db_connect() as con:
        row = con.execute(
            """SELECT id FROM orders
               WHERE user_id=? AND status NOT IN ('выполнено','отменена')
               ORDER BY created_at DESC LIMIT 1""",
            (message.from_user.id,)
        ).fetchone()
    if row:
        order_id = row[0]
        await bot.send_message(OWNER_ID, f"📸 Фото от {message.from_user.full_name} к заявке #{order_id}")
        await message.answer(
            f"Фото прикреплено к заявке #{order_id}.\nМастер свяжется в течение 15 минут.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Главное меню", callback_data="back_main")]
            ])
        )
    else:
        await bot.send_message(OWNER_ID, f"📸 Фото от {message.from_user.full_name} (без заявки)")
        await message.answer(
            "Фото получено! Оценим и свяжемся в течение 15 минут.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Главное меню", callback_data="back_main")]
            ])
        )

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
        stars = "⭐" * rating
        await cb.message.answer(
            f"Спасибо за оценку {stars}!\n\nОставьте комментарий или нажмите Пропустить:",
            reply_markup=kb_review_comment()
        )
    await cb.answer()

@dp.callback_query(F.data == "rev_comment_skip", Review.waiting_comment)
async def review_comment_skip(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    db_add_review(cb.from_user.id, data.get("review_order_id", 0), data.get("review_rating", 5), "")
    await state.clear()
    await cb.message.answer("Спасибо за отзыв! Это очень важно для нас. 🙏", reply_markup=kb_main())
    await bot.send_message(
        OWNER_ID,
        f"⭐ Новый отзыв от {cb.from_user.full_name}:\n"
        f"Оценка: {'⭐' * data.get('review_rating', 5)}\n"
        f"Заявка #{data.get('review_order_id', '?')}"
    )
    await cb.answer()

@dp.message(Review.waiting_comment)
async def review_comment(message: Message, state: FSMContext):
    data = await state.get_data()
    db_add_review(message.from_user.id, data.get("review_order_id", 0), data.get("review_rating", 5), message.text)
    await state.clear()
    await message.answer("Спасибо за отзыв! Это очень важно для нас. 🙏", reply_markup=kb_main())
    await bot.send_message(
        OWNER_ID,
        f"⭐ Новый отзыв от {message.from_user.full_name}:\n"
        f"Оценка: {'⭐' * data.get('review_rating', 5)}\n"
        f"Комментарий: {message.text}\n"
        f"Заявка #{data.get('review_order_id', '?')}"
    )

# ─── FALLBACK ─────────────────────────────────────────────────────────────────
@dp.message()
async def fallback(message: Message, state: FSMContext):
    if await state.get_state():
        return
    await message.answer("Воспользуйтесь меню:", reply_markup=kb_main())

# ─── ФОНОВЫЕ ЗАДАЧИ ───────────────────────────────────────────────────────────
async def reminder_task():
    """Каждые 30 минут проверяем заявки, висящие > 2 часов"""
    await asyncio.sleep(60)
    while True:
        try:
            pending = db_get_pending_orders(minutes=120)
            for order in pending:
                label = "РЕГУЛЯРНОЕ" if order["type"] == "regular" else "РАЗОВЫЙ"
                await bot.send_message(
                    OWNER_ID,
                    f"⏰ НАПОМИНАНИЕ\n\n"
                    f"Заявка #{order['id']} ({label}) висит без ответа!\n"
                    f"Создана: {order['created_at']}\n\n"
                    f"Свяжись с клиентом.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="▶️ В работу", callback_data=f"ss_{order['id']}_{order['user_id']}_inwork")],
                        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"ss_{order['id']}_{order['user_id']}_cancel")],
                    ])
                )
        except Exception as e:
            logging.error(f"Reminder error: {e}")
        await asyncio.sleep(30 * 60)

async def weekly_report_task():
    """Еженедельная сводка каждый понедельник в 9:00"""
    while True:
        now = datetime.now()
        days_ahead = (7 - now.weekday()) % 7 or 7
        next_monday = now.replace(hour=9, minute=0, second=0, microsecond=0)
        next_monday = next_monday.replace(day=now.day + days_ahead)
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
    print("🏠 Хаус Мастер Bot v1.0 запущен!")
    asyncio.create_task(reminder_task())
    asyncio.create_task(weekly_report_task())
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
