import os
import asyncio
import logging
import json
import requests
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from gigachat import GigaChat
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from monitor import run_monitoring, format_monitoring_report
from db import init_db
from handlers_monitor import register_monitor_handlers

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


def _call_gigachat(prompt, batch_channels=None, attempt=1):
    """
    Отправляет один запрос к GigaChat с ретраем.
    batch_channels — список username'ов каналов в батче (для логов).
    Возвращает текст ответа или None при ошибке.
    """
    try:
        with GigaChat(credentials=GIGACHAT_CREDENTIALS, verify_ssl_certs=False) as giga:
            response = giga.chat(prompt)
            text = response.choices[0].message.content
            return text
    except Exception as e:
        ch_info = ""
        if batch_channels:
            ch_info = f" | каналы: {', '.join(batch_channels)}"
        logger.error(
            f"GigaChat ошибка (попытка {attempt}/{2}){ch_info} | "
            f"промпт ~{len(prompt)} симв. | {type(e).__name__}: {e}",
            exc_info=True,  # ← полный traceback в лог
        )
        if attempt < 2:
            import time
            time.sleep(3)
            return _call_gigachat(prompt, batch_channels, attempt + 1)
        return None


def get_summaries(channels_data):
    """
    Получает AI-резюме для каждого канала отдельным запросом.

    Почему по одному, а не батчами:
    - GigaChat путал содержание между каналами в батче
    - GigaChat галлюцинировал детали (города, названия) при сжатии
    - Цензура одного канала убивала весь батч
    22 канала × ~1.5 сек = ~33 сек — приемлемо для утреннего дайджеста.
    """
    if not channels_data:
        return {}

    summaries = {}

    for ch in channels_data:
        posts_joined = " | ".join(ch["posts"][:5])

        prompt = (
            "Напиши ОДНУ строку-резюме (максимум 80 символов) по постам Telegram-канала.\n\n"
            "СТРОГИЕ ПРАВИЛА:\n"
            "— Используй ТОЛЬКО факты из текста ниже. НЕ ДОДУМЫВАЙ города, имена, числа, детали.\n"
            "— Если в тексте не указан город — не добавляй город.\n"
            "— Только суть: кто + что сделал. Без вводных слов, без эмодзи.\n"
            "— Если не можешь определить тему — ответь: ПРОПУСК\n\n"
            f"Посты канала @{ch['username']}:\n{posts_joined}"
        )

        logger.info(f"GigaChat → @{ch['username']}, ~{len(prompt)} симв.")
        # Логируем содержимое промпта для отладки галлюцинаций
        logger.debug(f"GigaChat промпт @{ch['username']}:\n{prompt}")

        text = _call_gigachat(prompt, batch_channels=[ch["username"]])
        if not text:
            logger.warning(f"GigaChat не ответил для @{ch['username']}")
            continue

        logger.info(f"GigaChat ← @{ch['username']}: {text[:150]}")

        # Детекция цензуры
        censorship_markers = [
            "не обладают собственным мнением",
            "чувствительные темы",
            "ограничены",
            "не могу помочь",
            "не могу выполнить",
        ]
        if any(marker in text.lower() for marker in censorship_markers):
            logger.warning(f"⚠️ GigaChat ОТЦЕНЗУРИЛ @{ch['username']}: {text[:200]}")
            continue

        # Пропуск — GigaChat не смог определить тему
        if "ПРОПУСК" in text.strip().upper():
            logger.info(f"GigaChat пропустил @{ch['username']}")
            continue

        # Чистим ответ: убираем возможную нумерацию "1. " или тире в начале
        summary = text.strip()
        if len(summary) > 2 and summary[:2] in ("1.", "—", "–"):
            summary = summary[2:].strip()
        if summary.startswith("-"):
            summary = summary[1:].strip()

        # Убираем @username если GigaChat добавил
        if summary.lower().startswith(f"@{ch['username'].lower()}"):
            summary = summary[len(ch['username']) + 1:].strip().lstrip(":").strip()

        if summary:
            summaries[ch["username"]] = summary

    logger.info(f"Получено резюме: {len(summaries)} из {len(channels_data)} каналов")
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
        summary = summaries.get(username, "")

        # Нет резюме = не показываем канал (не путаем пользователей)
        if not summary:
            continue

        subs = f" · {ch['subs']} подп." if ch["subs"] else ""
        title = ch.get("title") or username
        line = f"📌 {title} (@{username}){subs}"
        line += f"\n└ {summary}"
        lines.append(line)

    if not lines:
        return "📭 Не удалось сформировать дайджест. Попробуйте позже."

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

            items = []
            seen = set()
            for block in soup.select("div.block.holidays:not(.famous-date) ul.itemsNet li span.title a"):
                title = block.get_text(strip=True)
                if title and title not in seen:
                    seen.add(title)
                    items.append(f"  • {title}")

            if items:
                day_name = day_names[target.weekday()]
                date_str = target.strftime(f"%d.%m ({day_name})")
                lines.append(f"\n📅 {date_str}:")
                lines.extend(items[:10])

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


async def cmd_holidays(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎉 На день", callback_data="holidays_day"),
            InlineKeyboardButton("📆 На неделю", callback_data="holidays_week"),
        ]
    ])
    await update.message.reply_text("Выберите период:", reply_markup=keyboard)


async def cmd_monitoring(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 По персоне", callback_data="mon_persons")],
        [InlineKeyboardButton("📊 Все за сегодня", callback_data="mon_today")],
    ])
    await update.message.reply_text("🔍 Мониторинг упоминаний\n\nВыберите режим:", reply_markup=keyboard)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📰 Получить дайджест", callback_data="refresh")],
        [InlineKeyboardButton("🔍 Мониторинг упоминаний", callback_data="mon_menu")],
        [
            InlineKeyboardButton("🎉 Праздники на день", callback_data="holidays_day"),
            InlineKeyboardButton("📆 Праздники на неделю", callback_data="holidays_week"),
        ]
    ])
    await update.message.reply_text(
        "Привет! Я бот-дайджест — ваш персональный цифровой сталкер.\n"
        "Каждый день в 09:00 МСК я врываюсь в чат с дайджестом, как утренний кофе без сахара.\n"
        "Каждые 2 часа я как Шерлок Холмс выискиваю упоминания ваших \"подопечных\".\n\n"
        "Нажми на кнопку - получишь результат:",
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

async def scheduled_monitoring(app):
    """Фоновый мониторинг: сбор постов + поиск фамилий каждые 2 часа. Без отправки."""
    try:
        result = await run_monitoring()
        logger.info(
            f"Мониторинг: {result['new_posts']} постов, "
            f"{result['new_mentions']} упоминаний"
        )
    except Exception as e:
        logger.error(f"Ошибка мониторинга по расписанию: {e}")

# ─────────────────────────────────────────────
# 7. Запуск
# ─────────────────────────────────────────────
async def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("digest", cmd_digest))
    app.add_handler(CommandHandler("holidays", cmd_holidays))
    app.add_handler(CommandHandler("monitoring", cmd_monitoring))
    app.add_handler(CallbackQueryHandler(callback_refresh, pattern="^refresh$"))
    app.add_handler(CallbackQueryHandler(callback_holidays_day, pattern="^holidays_day$"))
    app.add_handler(CallbackQueryHandler(callback_holidays_week, pattern="^holidays_week$"))
    register_monitor_handlers(app)

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(
        scheduled_digest,
        trigger="cron",
        hour=9,
        minute=0,
        args=[app],
    )
    scheduler.add_job(
        scheduled_monitoring,
        trigger="interval",
        hours=2,
        args=[app],
    )
    init_db()
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


if __name__ == "__main__":
    asyncio.run(main())