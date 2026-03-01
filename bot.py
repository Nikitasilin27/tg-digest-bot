import os
import asyncio
import logging
import json
import requests
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from gigachat import GigaChat
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import json

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GIGACHAT_CREDENTIALS = os.getenv("GIGACHAT_CREDENTIALS")
SHEET_ID = os.getenv("SHEET_ID")
CHAT_ID = int(os.getenv("CHAT_ID"))
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 1. Читаем каналы из Google Sheets
# ─────────────────────────────────────────────
def get_channels_from_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).worksheet("Мониторинг")
    values = sheet.col_values(3)  # столбец C
    channels = []
    for cell in values[1:]:  # пропускаем первую строку
        if cell and cell.strip():
            # Извлекаем логин из https://t.me/username
            username = cell.strip().rstrip("/").split("/")[-1]
            if username:
                channels.append(username)
    logger.info(f"Загружено каналов: {len(channels)}")
    return channels


# ─────────────────────────────────────────────
# 2. Парсим посты канала за последние 24 часа
# ─────────────────────────────────────────────
def fetch_channel_posts(username):
    url = f"https://t.me/s/{username}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
    except Exception as e:
        logger.warning(f"Ошибка запроса {username}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Название и подписчики
    title_tag = soup.find("div", class_="tgme_channel_info_header_title")
    title = title_tag.get_text(strip=True) if title_tag else username

    subs_tag = soup.find("div", class_="tgme_channel_info_counter")
    # Иногда подписчики в первом counter-блоке
    counters = soup.find_all("div", class_="tgme_channel_info_counter")
    subs = ""
    for c in counters:
        label = c.find("span", class_="counter_type")
        if label and "subscriber" in label.get_text(strip=True).lower():
            value = c.find("span", class_="counter_value")
            subs = value.get_text(strip=True) if value else ""
            break

    # Посты за последние 24 часа
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    messages = soup.find_all("div", class_="tgme_widget_message")
    posts_text = []
    max_views = 0

    for msg in messages:
        # Время поста
        time_tag = msg.find("time")
        if not time_tag or not time_tag.get("datetime"):
            continue
        try:
            post_time = datetime.fromisoformat(time_tag["datetime"].replace("Z", "+00:00"))
        except Exception:
            continue

        if post_time < cutoff:
            continue

        # Текст поста
        text_tag = msg.find("div", class_="tgme_widget_message_text")
        text = text_tag.get_text(separator=" ", strip=True) if text_tag else ""
        if text:
            posts_text.append(text[:300])  # обрезаем длинные посты

        # Просмотры
        views_tag = msg.find("span", class_="tgme_widget_message_views")
        if views_tag:
            views_str = views_tag.get_text(strip=True).replace("K", "000").replace("M", "000000")
            try:
                views_num = int("".join(filter(str.isdigit, views_str)))
                if views_num > max_views:
                    max_views = views_num
            except Exception:
                pass

    if not posts_text:
        return None  # нет постов за 24 часа

    return {
        "username": username,
        "title": title,
        "subs": subs,
        "max_views": max_views,
        "posts": posts_text,
    }


# ─────────────────────────────────────────────
# 3. Получаем AI-резюме через GigaChat
# ─────────────────────────────────────────────
def get_summaries(channels_data):
    if not channels_data:
        return {}

    # Собираем один большой промпт
    prompt_parts = []
    for i, ch in enumerate(channels_data, 1):
        posts_joined = " | ".join(ch["posts"][:5])  # максимум 5 постов
        prompt_parts.append(f"{i}. @{ch['username']}: {posts_joined}")

    prompt = (
            "Ты — редактор регионального политического дайджеста. "
            "Твоя задача: для каждого Telegram-канала ниже написать ОДНУ строку резюме на русском языке.\n\n"
            "Правила:\n"
            "— Максимум 80 символов на резюме\n"
            "— Только суть: кто + что сделал/сказал/решил\n"
            "— Без вводных слов ('канал сообщает', 'автор пишет' и т.д.)\n"
            "— Без имён пользователей и ссылок\n"
            "— Если постов нет или они нерелевантны — пропусти номер\n"
            "— Формат ответа: только пронумерованный список, ничего лишнего\n\n"
            "Каналы:\n"
            + "\n".join(prompt_parts)
    )

    try:
        with GigaChat(credentials=GIGACHAT_CREDENTIALS, verify_ssl_certs=False) as giga:
            response = giga.chat(prompt)
            text = response.choices[0].message.content
            logger.info(f"GigaChat ответил:\n{text}")
    except Exception as e:
        logger.error(f"Ошибка GigaChat: {e}")
        return {}

    # Парсим ответ: "1. текст", "2. текст" ...
    summaries = {}
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        for i, ch in enumerate(channels_data, 1):
            if line.startswith(f"{i}."):
                summary = line[len(f"{i}."):].strip()
                # Убираем @username если GigaChat добавил его в начало
                if summary.startswith(f"@{ch['username']}:"):
                    summary = summary[len(f"@{ch['username']}:"):].strip()
                summaries[ch["username"]] = summary
                break

    return summaries


# ─────────────────────────────────────────────
# 4. Форматируем дайджест
# ─────────────────────────────────────────────
def format_views(n):
    if n == 0:
        return "—"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1000:
        return f"{n/1000:.1f}K"
    return str(n)


def build_digest():
    channels = get_channels_from_sheet()
    if not channels:
        return "❌ Список каналов пуст."

    channels_data = []
    for username in channels:
        data = fetch_channel_posts(username)
        if data:
            channels_data.append(data)

    if not channels_data:
        return "📭 За последние 24 часа новых постов не найдено."

    summaries = get_summaries(channels_data)

    now_msk = datetime.now(timezone(timedelta(hours=3)))
    header = f"📰 Дайджест за {now_msk.strftime('%d.%m.%Y')}\n\n"

    # Сортируем по просмотрам — самые популярные первыми
    channels_data.sort(key=lambda x: x["max_views"], reverse=True)

    lines = []
    for ch in channels_data:
        username = ch["username"]
        subs = f" · {ch['subs']} подп." if ch["subs"] else ""
        summary = summaries.get(username, "")

        title = ch.get("title") or username
        line = f"📌 {title} (@{username}){subs}"
        if summary:
            line += f"\n└ {summary}"
        lines.append(line)

    body = "\n\n".join(lines)
    digest = header + body

    # Телеграм лимит 4096 символов
    if len(digest) > 4096:
        digest = digest[:4090] + "\n…"

    return digest


# ─────────────────────────────────────────────
# Праздники с calend.ru
# ─────────────────────────────────────────────
def get_holidays(days=1):
    now_msk = datetime.now(timezone(timedelta(hours=3)))
    day_names = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
    headers = {"User-Agent": "Mozilla/5.0"}
    lines = []

    for delta in range(days):
        target = (now_msk + timedelta(days=delta))
        url = f"https://www.calend.ru/day/{target.year}-{target.month}-{target.day}/"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Ищем заголовок страницы с праздниками — он в теге h1 или title
            items = []
            # Все ссылки ведущие на /holidays/ — это и есть праздники дня
            seen = set()
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "/holidays/" in href or "/events/" in href:
                    title = a.get_text(strip=True)
                    if title and len(title) > 4 and title not in seen:
                        seen.add(title)
                        items.append(f"  • {title}")

            if items:
                day_name = day_names[target.weekday()]
                date_str = target.strftime(f"%d.%m ({day_name})")
                lines.append(f"\n📅 {date_str}:")
                lines.extend(items[:10])  # максимум 10 праздников на день

        except Exception as e:
            logger.warning(f"Ошибка загрузки праздников {target.date()}: {e}")

    if not lines:
        return "🗓 Праздников не найдено."

    period = "сегодня" if days == 1 else f"ближайшие {days} дней"
    result = f"🎉 Праздники ({period}):\n" + "\n".join(lines)
    if len(result) > 4096:
        result = result[:4090] + "\n…"
    return result


# ─────────────────────────────────────────────
# 5. Команды бота
# ─────────────────────────────────────────────
async def cmd_digest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Собираю дайджест, подожди...")
    try:
        text = build_digest()
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Обновить", callback_data="refresh")]])
        await update.message.reply_text(text, reply_markup=keyboard)
    except Exception as e:
        logger.exception("Ошибка в cmd_digest")
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def callback_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Отправляем новое сообщение с индикатором загрузки
    loading_msg = await query.message.reply_text("⏳ Собираю дайджест, минутку...")
    try:
        text = build_digest()
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Обновить", callback_data="refresh")]])
        await loading_msg.edit_text(text, reply_markup=keyboard)
    except Exception as e:
        logger.exception("Ошибка в callback_refresh")
        await loading_msg.edit_text(f"❌ Ошибка: {e}")


async def callback_holidays_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    loading_msg = await query.message.reply_text("⏳ Загружаю праздники...")
    text = get_holidays(days=1)
    await loading_msg.edit_text(text)


async def callback_holidays_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    loading_msg = await query.message.reply_text("⏳ Загружаю праздники...")
    text = get_holidays(days=7)
    await loading_msg.edit_text(text)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📰 Получить дайджест", callback_data="refresh")],
        [
            InlineKeyboardButton("🎉 Праздники на день", callback_data="holidays_day"),
            InlineKeyboardButton("📆 Праздники на неделю", callback_data="holidays_week"),
        ]
    ])
    await update.message.reply_text(
        "Привет! Я бот-дайджест.\n"
        "/digest — получить дайджест прямо сейчас\n"
        "Каждый день в 09:00 МСК я шлю дайджест автоматически.",
        reply_markup=keyboard
    )


# ─────────────────────────────────────────────
# 6. Автоматическая отправка в 09:00 МСК
# ─────────────────────────────────────────────
async def scheduled_digest(app):
    try:
        text = build_digest()
        await app.bot.send_message(chat_id=CHAT_ID, text=text)
        logger.info("Дайджест отправлен по расписанию")
    except Exception as e:
        logger.error(f"Ошибка при отправке по расписанию: {e}")


# ─────────────────────────────────────────────
# 7. Запуск
# ─────────────────────────────────────────────
async def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("digest", cmd_digest))
    app.add_handler(CallbackQueryHandler(callback_refresh, pattern="^refresh$"))
    app.add_handler(CallbackQueryHandler(callback_holidays_day, pattern="^holidays_day$"))
    app.add_handler(CallbackQueryHandler(callback_holidays_week, pattern="^holidays_week$"))

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(
        scheduled_digest,
        trigger="cron",
        hour=9,
        minute=0,
        args=[app],
    )
    scheduler.start()

    logger.info("Бот запущен")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    # Держим бота живым
    try:
        await asyncio.Event().wait()
    finally:
        scheduler.shutdown()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


# ─────────────────────────────────────────────
# Health check сервер (чтобы Render не засыпал)
# ─────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass  # отключаем лишние логи


def run_health_server():
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()


if __name__ == "__main__":
    # Запускаем health check в фоне
    threading.Thread(target=run_health_server, daemon=True).start()
    asyncio.run(main())