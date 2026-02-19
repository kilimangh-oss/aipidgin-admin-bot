import os
import asyncio
import logging
import sqlite3
from datetime import datetime
from typing import List, Tuple, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

load_dotenv()

# ====== ТОЛЬКО ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ======
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден! Укажите его в переменных окружения.")
# =========================================

DB_PATH = os.getenv('USERS_DB', '../users.db')
ADMIN_ID = 8444406750

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ==================== DATABASE FUNCTIONS ====================
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_users_table():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            registered INTEGER DEFAULT 0,
            reg_date TEXT,
            deposit_amount REAL DEFAULT 0,
            deposit_confirmed INTEGER DEFAULT 0,
            deposit_date TEXT,
            trader_id TEXT,
            click_id TEXT
        )
        """
    )
    conn.commit()
    conn.close()

def stats() -> Tuple[int, int, int, float]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM users')
    total = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM users WHERE registered=1')
    registered = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM users WHERE deposit_confirmed=1')
    deposited = cur.fetchone()[0]
    cur.execute('SELECT SUM(deposit_amount) FROM users WHERE deposit_confirmed=1')
    s = cur.fetchone()[0]
    total_deposits = float(s) if s is not None else 0.0
    conn.close()
    return total, registered, deposited, total_deposits

def fetch_users(offset: int = 0, limit: int = 10) -> List[sqlite3.Row]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT user_id, username, registered FROM users ORDER BY user_id LIMIT ? OFFSET ?', (limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows

def fetch_user(user_identifier: str) -> Optional[sqlite3.Row]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM users WHERE user_id=?', (user_identifier,))
    row = cur.fetchone()
    if row:
        conn.close()
        return row
    cur.execute('SELECT * FROM users WHERE username=?', (user_identifier,))
    row = cur.fetchone()
    conn.close()
    return row

def search_users(query: str, offset: int = 0, limit: int = 10) -> List[sqlite3.Row]:
    q = f"%{query}%"
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        'SELECT user_id, username, registered FROM users WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? OR trader_id LIKE ? ORDER BY user_id LIMIT ? OFFSET ?',
        (q, q, q, limit, offset)
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def confirm_registration(user_id: int) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET registered=1 WHERE user_id=?', (user_id,))
    conn.commit()
    updated = cur.rowcount
    conn.close()
    return updated > 0

def confirm_deposit(user_id: int, amount: float) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET deposit_confirmed=1, deposit_amount=?, deposit_date=? WHERE user_id=?', 
                (amount, datetime.now().isoformat(), user_id))
    conn.commit()
    updated = cur.rowcount
    conn.close()
    return updated > 0

# ==================== KEYBOARDS ====================
def build_admin_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='📊 Статистика', callback_data='admin:stats')],
        [InlineKeyboardButton(text='👥 Список пользователей', callback_data='admin:users:0')],
        [InlineKeyboardButton(text='🔍 Поиск пользователя', callback_data='admin:search')],
        [InlineKeyboardButton(text='📢 Рассылка', callback_data='admin:broadcast')],
        [InlineKeyboardButton(text='⚙️ Настройки', callback_data='admin:settings')],
    ])
    return kb

def build_users_keyboard(offset: int, has_more: bool) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    if offset > 0:
        row.append(InlineKeyboardButton(text='⬅️ Назад', callback_data=f'admin:users:{max(0, offset-10)}'))
    if has_more:
        row.append(InlineKeyboardButton(text='Далее ➡️', callback_data=f'admin:users:{offset+10}'))
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text='🔙 В меню', callback_data='admin:menu')])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ==================== HANDLERS ====================
async def is_admin(user_id):
    return user_id == ADMIN_ID

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply('Доступ запрещён.')
        return
    await message.answer('Админ-меню:', reply_markup=build_admin_menu())

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply('Доступ запрещён.')
        return
    total, registered, deposited, total_deposits = stats()
    text = (
        f"Всего пользователей: {total}\n"
        f"Зарегистрировано: {registered}\n"
        f"С депозитом: {deposited}\n"
        f"Сумма депозитов: {total_deposits}"
    )
    await message.reply(text)

@dp.message(Command("users"))
async def cmd_users(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply('Доступ запрещён.')
        return
    await show_users_page(message, 0)

async def show_users_page(message_or_call, offset: int):
    rows = fetch_users(offset, 10)
    text_lines = []
    for r in rows:
        text_lines.append(
            f"ID: {r['user_id']} | @{r['username'] or '—'} | Зарегистрирован: {'✅' if r['registered'] else '❌'} | Депозит: {'✅' if r.get('deposit_confirmed', 0) else '❌'}"
        )
    has_more = len(rows) == 10
    text = '\n'.join(text_lines) if text_lines else 'Пользователи не найдены.'
    if isinstance(message_or_call, Message):
        await message_or_call.reply(text, reply_markup=build_users_keyboard(offset, has_more))
    else:
        await message_or_call.message.edit_text(text, reply_markup=build_users_keyboard(offset, has_more))

@dp.message(Command("user"))
async def cmd_user(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply('Доступ запрещён.')
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply('Использование: /user <user_id или username>')
        return
    identifier = parts[1].strip()
    row = fetch_user(identifier)
    if not row:
        await message.reply('Пользователь не найден.')
        return
    txt = []
    txt.append(f"ID: {row['user_id']}")
    txt.append(f"Username: @{row['username'] or '—'}")
    txt.append(f"Зарегистрирован: {'Да' if row['registered'] else 'Нет'}")
    txt.append(f"Дата регистрации: {row['reg_date'] or '—'}")
    txt.append(f"Депозит подтверждён: {'Да' if row['deposit_confirmed'] else 'Нет'}")
    txt.append(f"Сумма депозита: {row['deposit_amount'] or 0}")
    txt.append(f"Дата депозита: {row['deposit_date'] or '—'}")
    txt.append(f"Trader ID: {row['trader_id'] or '—'}")
    txt.append(f"Click ID: {row['click_id'] or '—'}")
    await message.reply('\n'.join(txt))

@dp.message(Command("search"))
async def cmd_search(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply('Доступ запрещён.')
        return
    await message.answer('Введите user_id или username для поиска:')

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply('Доступ запрещён.')
        return
    await message.answer('Введите текст для рассылки всем пользователям:')

@dp.message(Command("confirm_reg"))
async def cmd_confirm_reg(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply('Доступ запрещён.')
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply('Использование: /confirm_reg <user_id>')
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await message.reply('Некорректный user_id.')
        return
    ok = confirm_registration(uid)
    await message.reply('Регистрация подтверждена.' if ok else 'Пользователь не найден или уже подтверждён.')

@dp.message(Command("confirm_dep"))
async def cmd_confirm_dep(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply('Доступ запрещён.')
        return
    parts = message.text.split()
    if len(parts) != 3:
        await message.reply('Использование: /confirm_dep <user_id> <сумма>')
        return
    try:
        uid = int(parts[1])
        amt = float(parts[2])
    except ValueError:
        await message.reply('Некорректные аргументы.')
        return
    ok = confirm_deposit(uid, amt)
    await message.reply('Депозит подтверждён.' if ok else 'Пользователь не найден или ошибка обновления.')

@dp.callback_query()
async def on_callback(call: CallbackQuery):
    if not await is_admin(call.from_user.id):
        await call.answer('Доступ запрещён.', show_alert=True)
        return
    data = call.data or ''
    if data.startswith('admin:stats'):
        total, registered, deposited, total_deposits = stats()
        text = (
            f"Всего пользователей: {total}\n"
            f"Зарегистрировано: {registered}\n"
            f"С депозитом: {deposited}\n"
            f"Сумма депозитов: {total_deposits}"
        )
        await call.message.edit_text(text, reply_markup=build_admin_menu())
    elif data.startswith('admin:users:'):
        try:
            offset = int(data.split(':')[-1])
        except Exception:
            offset = 0
        await show_users_page(call, offset)
    elif data.startswith('admin:search'):
        await call.message.edit_text('Введите user_id или username для поиска:', reply_markup=build_admin_menu())
    elif data.startswith('admin:broadcast'):
        await call.message.edit_text('Введите текст для рассылки всем пользователям:', reply_markup=build_admin_menu())
    elif data.startswith('admin:settings'):
        await call.message.edit_text('Настройки (заглушка):\n\nПока здесь ничего нет.', reply_markup=build_admin_menu())
    elif data.startswith('admin:menu'):
        await call.message.edit_text('Админ-меню:', reply_markup=build_admin_menu())
    else:
        await call.answer()

async def main():
    ensure_users_table()
    print("🚀 Admin бот запускается в режиме polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())