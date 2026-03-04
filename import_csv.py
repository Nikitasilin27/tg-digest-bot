"""
import_csv.py — одноразовый скрипт импорта данных из CSV в SQLite.

Запускаешь ОДИН раз после создания базы:
    python3 import_csv.py

Что он делает:
1. Читает CSV-файл (выгрузку из Google Sheets)
2. Из каждой строки вытаскивает VK и TG ссылки → таблица sources
3. Из строк с персонами (Главы, Коммунисты, оппонент) вытаскивает
   фамилии → таблица persons

Безопасен для повторного запуска: дубли пропускаются (INSERT OR IGNORE).
"""

import csv
import re
import os
from db import get_connection, init_db

# ─────────────────────────────────────────────
# Настройки — поправь путь если CSV лежит в другом месте
# ─────────────────────────────────────────────
CSV_PATH = "monitoring.csv"  # положи CSV рядом со скриптом и переименуй

# ─────────────────────────────────────────────
# Вспомогательные функции
# ─────────────────────────────────────────────

def extract_vk_id(url):
    """
    Из VK-ссылки вытаскивает идентификатор группы/пользователя.

    Примеры:
        https://vk.com/pohvistnevo.vkurse  →  pohvistnevo.vkurse
        https://vk.com/club208709480       →  club208709480
        https://vk.com/public87563767      →  public87563767
        https://vk.com/id640300607         →  id640300607

    Этот ID нужен для VK API: метод wall.get принимает
    домен группы (screen_name) или числовой ID.
    """
    if not url or url.strip().upper() == "НЕТ":
        return None
    # Убираем всё до последнего "/" — остаётся идентификатор
    clean = url.strip().rstrip("/")
    parts = clean.split("/")
    return parts[-1] if parts else None


def extract_tg_username(url):
    """
    Из TG-ссылки вытаскивает username канала.

    Примеры:
        https://t.me/pohvistnevo_news  →  pohvistnevo_news
        https://t.me/er_samara         →  er_samara
    """
    if not url or url.strip().upper() == "НЕТ":
        return None
    clean = url.strip().rstrip("/")
    parts = clean.split("/")
    return parts[-1] if parts else None


def extract_person_name(raw_name):
    """
    Из строки вроде "Глава района Игорь Рудаков" вытаскивает
    имя и фамилию.

    Логика:
    1. Убираем префиксы: "Глава района", "Глава Волжского района"
    2. Оставшиеся слова — это "Имя Фамилия"
    3. Последнее слово = фамилия, предпоследнее = имя

    Возвращает: ("Рудаков Игорь", "рудаков")
                 ^отображаемое имя   ^нормальная форма для поиска
    """
    # Убираем "Глава ... района" в начале
    cleaned = re.sub(
        r"^Глава\s+(\S+\s+)?района\s*",  # \S+ — любое слово (Волжского, и т.д.)
        "",
        raw_name.strip()
    )
    # Если осталось пусто — вернём как есть
    cleaned = cleaned.strip()
    if not cleaned:
        return None, None

    words = cleaned.split()
    if len(words) >= 2:
        # Стандартный случай: "Игорь Рудаков" или "Михаил Матвеев"
        first_name = words[-2]   # имя (предпоследнее слово)
        surname = words[-1]      # фамилия (последнее слово)
        display = f"{surname} {first_name}"
    elif len(words) == 1:
        # Только фамилия
        surname = words[0]
        display = surname
    else:
        return None, None

    # ВАЖНО: мы НЕ используем pymorphy для нормализации фамилий.
    # pymorphy не понимает, что "Казаков" — это фамилия, а не слово "казак".
    # Он выдаёт: Казаков → казак, Шарков → шарк — это мусор.
    #
    # Вместо этого храним фамилию как есть, в нижнем регистре.
    # А pymorphy будем использовать ПОЗЖЕ — при поиске по тексту поста:
    # берём каждое слово из поста, проверяем, не является ли оно формой
    # нашей фамилии (через совпадение основы).
    normal = surname.lower()

    return display, normal


# ─────────────────────────────────────────────
# Основная логика импорта
# ─────────────────────────────────────────────

def do_import():
    # Проверяем, что CSV на месте
    if not os.path.exists(CSV_PATH):
        print(f"❌ Файл не найден: {CSV_PATH}")
        print(f"   Положи CSV-файл рядом со скриптом и назови его '{CSV_PATH}'")
        print(f"   Или поправь переменную CSV_PATH в начале файла.")
        return

    # Убеждаемся, что таблицы существуют
    init_db()
    conn = get_connection()

    # Счётчики — чтобы в конце показать что импортировали
    sources_added = 0
    persons_added = 0
    skipped_dupes = 0

    # ─── Читаем CSV ─────────────────────────
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # Категории, которые содержат персоны (не организации).
    # Для них мы извлечём фамилии в таблицу persons.
    PERSON_CATEGORIES = {"Главы", "Коммунисты", "оппонент"}

    # CSV-файл устроен хитро: категория указана только в первой строке группы,
    # а дальше ячейка пустая. Поэтому мы "запоминаем" текущую категорию.
    current_category = ""

    for i, row in enumerate(rows):
        # Пропускаем строку 0 (пустая) и строку 1 (заголовок)
        if i < 2:
            continue

        # Если в первом столбце есть текст — это новая категория
        if row[0].strip():
            current_category = row[0].strip()

        name = row[3].strip() if len(row) > 3 else ""
        district = row[1].strip() if len(row) > 1 else ""
        vk_url = row[5].strip() if len(row) > 5 else ""
        tg_url = row[6].strip() if len(row) > 6 else ""

        if not name:
            continue

        # ─── Добавляем VK-источник ──────────
        vk_id = extract_vk_id(vk_url)
        if vk_id:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO sources
                       (category, name, district, platform, platform_id, url)
                       VALUES (?, ?, ?, 'vk', ?, ?)""",
                    (current_category, name, district, vk_id, vk_url)
                )
                # rowcount = 1 если вставка прошла, 0 если дубль (IGNORE)
                if conn.total_changes:
                    sources_added += 1
            except Exception as e:
                # Ловим ошибку, логируем, но не падаем —
                # один битый URL не должен сломать весь импорт
                print(f"  ⚠️  VK ошибка строка {i}: {e}")

        # ─── Добавляем TG-источник ──────────
        tg_username = extract_tg_username(tg_url)
        if tg_username:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO sources
                       (category, name, district, platform, platform_id, url)
                       VALUES (?, ?, ?, 'tg', ?, ?)""",
                    (current_category, name, district, tg_username, tg_url)
                )
            except Exception as e:
                print(f"  ⚠️  TG ошибка строка {i}: {e}")

        # ─── Извлекаем персону (если это персона, а не организация) ───
        if current_category in PERSON_CATEGORIES:
            display_name, normal_form = extract_person_name(name)
            if display_name and normal_form:
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO persons
                           (name, normal_form, group_tag)
                           VALUES (?, ?, ?)""",
                        (display_name, normal_form, current_category)
                    )
                except Exception as e:
                    print(f"  ⚠️  Person ошибка строка {i}: {e}")

    conn.commit()

    # ─── Отчёт ──────────────────────────────
    sources_count = conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
    persons_count = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]

    print(f"\n✅ Импорт завершён!")
    print(f"   Источников в базе: {sources_count}")
    print(f"   Персон в базе:     {persons_count}")

    # Покажем что получилось
    print(f"\n── Источники ({'─' * 40})")
    for row in conn.execute(
        "SELECT category, name, platform, platform_id FROM sources ORDER BY category, name"
    ):
        print(f"   [{row['category']:12s}] {row['platform'].upper():2s}  {row['name'][:40]:40s}  ({row['platform_id']})")

    print(f"\n── Персоны ({'─' * 40})")
    for row in conn.execute(
        "SELECT name, normal_form, group_tag FROM persons ORDER BY group_tag, name"
    ):
        print(f"   [{row['group_tag']:12s}] {row['name']:25s}  лемма: {row['normal_form']}")

    conn.close()


if __name__ == "__main__":
    do_import()
