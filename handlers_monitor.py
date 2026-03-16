"""
handlers_monitor.py — хэндлеры бота для мониторинга упоминаний.

Отдельный файл, чтобы bot.py не разрастался.
Все callback_data начинаются с "mon_" — легко отличить от остальных.

User flow:
  Кнопка "🔍 Мониторинг" → меню
    ├── 👤 По персоне → список персон (кнопки)
    │     └── выбрал персону → выбор периода [24ч] [3 дня] [Неделя]
    │           └── результат с пагинацией ← 1/3 →
    └── 📊 Все за сегодня → сводка по всем персонам

Все переходы — edit_message_text (обновляем одно сообщение, не спамим новыми).

Как подключить в bot.py:
    from handlers_monitor import register_monitor_handlers
    register_monitor_handlers(app)
"""

from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, ContextTypes
from db import get_connection

# Сколько упоминаний на одну "страницу" в Telegram
MENTIONS_PER_PAGE = 5


# ─────────────────────────────────────────────
# Вспомогательная функция: стартовое меню бота
# ─────────────────────────────────────────────

def build_start_keyboard():
    """Клавиатура главного меню — такая же как в cmd_start."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📰 Получить дайджест", callback_data="refresh")],
        [InlineKeyboardButton("🔍 Мониторинг упоминаний", callback_data="mon_menu")],
        [
            InlineKeyboardButton("🎉 Праздники на день", callback_data="holidays_day"),
            InlineKeyboardButton("📆 Праздники на неделю", callback_data="holidays_week"),
        ]
    ])


START_TEXT = (
    "Привет! Я бот-дайджест.\n"
    "Каждый день в 09:00 МСК я шлю дайджест автоматически.\n"
    "Каждые 2 часа проверяю упоминания отслеживаемых персон.\n\n"
    "Нажми кнопку ниже:"
)


# ─────────────────────────────────────────────
# 0. Кнопка "Назад" → главное меню
# ─────────────────────────────────────────────

async def callback_back_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню — редактируем текущее сообщение."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(text=START_TEXT, reply_markup=build_start_keyboard())


# ─────────────────────────────────────────────
# 1. Главное меню мониторинга
# ─────────────────────────────────────────────

async def callback_monitor_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню мониторинга: По персоне / Все за сегодня."""
    query = update.callback_query
    await query.answer()

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 По персоне", callback_data="mon_persons")],
        [InlineKeyboardButton("📊 Все за сегодня", callback_data="mon_today")],
        [InlineKeyboardButton("◀️ Назад", callback_data="mon_back_start")],
    ])

    await query.edit_message_text(
        "🔍 Мониторинг упоминаний\n\nВыберите режим:",
        reply_markup=keyboard,
    )


# ─────────────────────────────────────────────
# 2. Список персон (кнопки)
# ─────────────────────────────────────────────

async def callback_persons_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает кнопки с именами отслеживаемых персон."""
    query = update.callback_query
    await query.answer()

    # Считаем упоминания за последние 24 часа — "что нового".
    now = datetime.now(timezone.utc)
    day_ago = (now - timedelta(hours=24)).isoformat()

    conn = get_connection()
    persons = conn.execute(
        """SELECT p.id, p.name, p.group_tag, COUNT(m.id) as cnt
           FROM persons p
           LEFT JOIN mentions m ON m.person_id = p.id AND m.post_date >= ?
           WHERE p.active = 1
           GROUP BY p.id
           ORDER BY cnt DESC""",
        (day_ago,),
    ).fetchall()
    conn.close()

    if not persons:
        await query.edit_message_text("📭 Нет отслеживаемых персон.")
        return

    # Формируем кнопки: по 2 в ряд.
    # Показываем имя + новые упоминания за 24ч.
    buttons = []
    row = []
    for p in persons:
        if p['cnt'] > 0:
            label = f"{p['name']} {p['cnt']}"
        else:
            label = f"{p['name']}"
        row.append(InlineKeyboardButton(label, callback_data=f"mon_p_{p['id']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton("◀️ Назад", callback_data="mon_menu")])

    await query.edit_message_text(
        "👤 Выберите персону:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# ─────────────────────────────────────────────
# 3. Выбор периода для персоны
# ─────────────────────────────────────────────

async def callback_person_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь выбрал персону → показываем кнопки периода."""
    query = update.callback_query
    await query.answer()

    # callback_data: "mon_p_{person_id}"
    person_id = int(query.data.split("_")[2])

    conn = get_connection()
    person = conn.execute(
        "SELECT name FROM persons WHERE id = ?", (person_id,)
    ).fetchone()
    conn.close()

    if not person:
        await query.edit_message_text("❌ Персона не найдена.")
        return

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("24 часа", callback_data=f"mon_r_{person_id}_1_0"),
            InlineKeyboardButton("3 дня", callback_data=f"mon_r_{person_id}_3_0"),
            InlineKeyboardButton("Неделя", callback_data=f"mon_r_{person_id}_7_0"),
        ],
        [InlineKeyboardButton("◀️ К списку персон", callback_data="mon_persons")],
    ])

    await query.edit_message_text(
        f"👤 {person['name']}\nВыберите период:",
        reply_markup=keyboard,
    )


# ─────────────────────────────────────────────
# 4. Результат: упоминания с пагинацией
# ─────────────────────────────────────────────

async def callback_show_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает упоминания персоны за выбранный период.

    callback_data: "mon_r_{person_id}_{days}_{page}"
    Пример: "mon_r_14_3_0" — персона #14, 3 дня, страница 0 (первая)
    """
    query = update.callback_query
    await query.answer()

    # Разбираем callback_data
    parts = query.data.split("_")
    person_id = int(parts[2])
    days = int(parts[3])
    page = int(parts[4])

    # Границы периода
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=days)).isoformat()

    conn = get_connection()

    # Имя персоны
    person = conn.execute(
        "SELECT name FROM persons WHERE id = ?", (person_id,)
    ).fetchone()
    if not person:
        conn.close()
        await query.edit_message_text("❌ Персона не найдена.")
        return

    # Считаем общее количество упоминаний за период
    total_count = conn.execute(
        """SELECT COUNT(*) FROM mentions
           WHERE person_id = ? AND post_date >= ?""",
        (person_id, cutoff),
    ).fetchone()[0]

    # Вычисляем пагинацию
    total_pages = max(1, (total_count + MENTIONS_PER_PAGE - 1) // MENTIONS_PER_PAGE)
    offset = page * MENTIONS_PER_PAGE

    # Загружаем упоминания для текущей страницы
    mentions = conn.execute(
        """SELECT m.snippet, m.post_url, m.post_date,
                  s.name as source_name, s.platform
           FROM mentions m
           JOIN sources s ON s.id = m.source_id
           WHERE m.person_id = ? AND m.post_date >= ?
           ORDER BY m.post_date DESC
           LIMIT ? OFFSET ?""",
        (person_id, cutoff, MENTIONS_PER_PAGE, offset),
    ).fetchall()

    conn.close()

    # Формируем текст
    period_label = {1: "24 часа", 3: "3 дня", 7: "неделю"}
    period_text = period_label.get(days, f"{days} дн.")

    if total_count == 0:
        text = f"👤 {person['name']}\n📭 За {period_text} упоминаний не найдено."
    else:
        lines = [
            f"👤 {person['name']} — {total_count} упом. за {period_text}",
            f"📄 Страница {page + 1}/{total_pages}",
            "",
        ]

        for m in mentions:
            platform = m["platform"].upper()
            # Дату показываем в МСК
            try:
                dt = datetime.fromisoformat(m["post_date"])
                date_str = (dt + timedelta(hours=3)).strftime("%d.%m %H:%M")
            except Exception:
                date_str = m["post_date"][:10]

            snippet = m["snippet"][:200].replace("\n", " ")

            lines.append(f"📌 {m['source_name'][:35]} ({platform}) · {date_str}")
            lines.append(f"└ {snippet}")
            lines.append(f"🔗 {m['post_url']}")
            lines.append("")

        text = "\n".join(lines)

    # Telegram лимит
    if len(text) > 4096:
        text = text[:4090] + "\n…"

    # Кнопки пагинации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton("◀️", callback_data=f"mon_r_{person_id}_{days}_{page - 1}")
        )
    nav_buttons.append(
        InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop")
    )
    if page < total_pages - 1:
        nav_buttons.append(
            InlineKeyboardButton("▶️", callback_data=f"mon_r_{person_id}_{days}_{page + 1}")
        )

    keyboard = InlineKeyboardMarkup([
        nav_buttons,
        [
            InlineKeyboardButton("🔄 Другой период", callback_data=f"mon_p_{person_id}"),
            InlineKeyboardButton("👤 Другая персона", callback_data="mon_persons"),
        ],
    ])

    await query.edit_message_text(text=text, reply_markup=keyboard)


# ─────────────────────────────────────────────
# 5. Все упоминания за сегодня (сводка)
# ─────────────────────────────────────────────

async def callback_today_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает сводку по всем персонам за последние 24 часа."""
    query = update.callback_query
    await query.answer()

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=24)).isoformat()

    conn = get_connection()

    rows = conn.execute(
        """SELECT p.name, p.id, COUNT(m.id) as cnt
           FROM persons p
           LEFT JOIN mentions m ON m.person_id = p.id AND m.post_date >= ?
           WHERE p.active = 1
           GROUP BY p.id
           ORDER BY cnt DESC""",
        (cutoff,),
    ).fetchall()

    # Последние 5 конкретных упоминаний для контекста
    recent = conn.execute(
        """SELECT p.name as person_name, s.name as source_name,
                  s.platform, m.snippet, m.post_url, m.post_date
           FROM mentions m
           JOIN persons p ON p.id = m.person_id
           JOIN sources s ON s.id = m.source_id
           WHERE m.post_date >= ?
           ORDER BY m.post_date DESC
           LIMIT 5""",
        (cutoff,),
    ).fetchall()

    conn.close()

    total = sum(r["cnt"] for r in rows)

    if total == 0:
        text = "📊 Сводка за 24 часа\n\n📭 Новых упоминаний не найдено."
    else:
        lines = [f"📊 Сводка за 24 часа — {total} упоминаний\n"]

        for r in rows:
            if r["cnt"] > 0:
                bar = "█" * min(r["cnt"], 15)
                lines.append(f"  👤 {r['name']} — {r['cnt']} {bar}")

        if recent:
            lines.append("\n── Последние ──")
            for m in recent:
                platform = m["platform"].upper()
                snippet = m["snippet"][:100].replace("\n", " ")
                lines.append(f"\n📌 {m['person_name']} в {m['source_name'][:30]} ({platform})")
                lines.append(f"└ {snippet}...")
                lines.append(f"🔗 {m['post_url']}")

        text = "\n".join(lines)

    if len(text) > 4096:
        text = text[:4090] + "\n…"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("◀️ Назад", callback_data="mon_menu")],
    ])

    await query.edit_message_text(text=text, reply_markup=keyboard)


# ─────────────────────────────────────────────
# 6. Заглушка для "noop" кнопки (номер страницы)
# ─────────────────────────────────────────────

async def callback_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Кнопка-заглушка (номер страницы). Ничего не делает."""
    await update.callback_query.answer()


# ─────────────────────────────────────────────
# Регистрация всех хэндлеров
# ─────────────────────────────────────────────

def register_monitor_handlers(app):
    """
    Регистрирует все callback-хэндлеры мониторинга.

    Вызывается из bot.py одной строкой:
        register_monitor_handlers(app)
    """
    app.add_handler(CallbackQueryHandler(callback_back_start, pattern="^mon_back_start$"))
    app.add_handler(CallbackQueryHandler(callback_monitor_menu, pattern="^mon_menu$"))
    app.add_handler(CallbackQueryHandler(callback_persons_list, pattern="^mon_persons$"))
    app.add_handler(CallbackQueryHandler(callback_person_selected, pattern=r"^mon_p_\d+$"))
    app.add_handler(CallbackQueryHandler(callback_show_results, pattern=r"^mon_r_\d+_\d+_\d+$"))
    app.add_handler(CallbackQueryHandler(callback_today_summary, pattern="^mon_today$"))
    app.add_handler(CallbackQueryHandler(callback_noop, pattern="^noop$"))