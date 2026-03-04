"""
db.py — модуль инициализации базы данных.

Что здесь происходит:
1. При первом запуске создаётся файл bot.db с четырьмя таблицами
2. При повторных запусках ничего не ломается (CREATE TABLE IF NOT EXISTS)
3. Функция get_connection() — единая точка входа для работы с базой

Как использовать в других файлах:
    from db import get_connection
    conn = get_connection()
    cursor = conn.execute("SELECT * FROM sources")
"""

import sqlite3
import os

# Путь к файлу базы — лежит рядом с ботом.
# В продакшене (Render) это будет в рабочей директории проекта.
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.db")


def get_connection():
    """
    Открывает соединение с базой.

    row_factory = sqlite3.Row  — это магия, которая позволяет обращаться
    к колонкам по имени, а не по индексу:
        row["username"]  вместо  row[3]
    Так код читается намного лучше.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # WAL-режим: позволяет читать базу, пока другой процесс пишет.
    # Без этого при одновременном чтении/записи будут блокировки.
    conn.execute("PRAGMA journal_mode=WAL")
    # Включаем проверку внешних ключей (по умолчанию в SQLite выключена!)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """
    Создаёт все таблицы, если их ещё нет.
    Вызывается один раз при старте бота.
    """
    conn = get_connection()

    # ─────────────────────────────────────────
    # ТАБЛИЦА 1: sources — откуда мы мониторим
    # ─────────────────────────────────────────
    # Каждая строка — один источник (VK-паблик или TG-канал).
    # У одной газеты может быть И VK, И TG — это будут ДВЕ записи.
    # Почему? Потому что у них разные URL, разный контент, разный парсинг.
    #
    # category  — группа из таблицы: "СМИ", "ЕР", "Главы", "Коммунисты", "оппонент"
    # platform  — "vk" или "tg"
    # platform_id — для VK: числовой ID группы (нужен для API)
    #               для TG: username канала (нужен для парсинга t.me/s/)
    # url       — полная ссылка, как была в таблице (для отображения)
    # active    — 1 = мониторим, 0 = пропускаем (чтобы не удалять, а "выключать")
    # added_by  — Telegram user ID того, кто добавил источник через бота (NULL = импорт)
    #
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            category    TEXT    NOT NULL DEFAULT '',
            name        TEXT    NOT NULL,
            district    TEXT    NOT NULL DEFAULT '',
            platform    TEXT    NOT NULL CHECK (platform IN ('vk', 'tg')),
            platform_id TEXT    NOT NULL,
            url         TEXT    NOT NULL,
            active      INTEGER NOT NULL DEFAULT 1,
            added_by    INTEGER,
            created_at  TEXT    NOT NULL DEFAULT (datetime('now')),

            UNIQUE (platform, platform_id)
        )
    """)
    # UNIQUE (platform, platform_id) — защита от дублей.
    # Нельзя добавить один и тот же VK-паблик дважды.

    # ─────────────────────────────────────────
    # ТАБЛИЦА 2: persons — кого мы ищем
    # ─────────────────────────────────────────
    # Каждая строка — одна персона для мониторинга.
    #
    # name        — как отображаем: "Рудаков Игорь"
    # normal_form — лемма фамилии для pymorphy2: "рудаков"
    #               (это начальная форма слова, к которой pymorphy приводит любую
    #               падежную форму: Рудакова → рудаков, Рудаковым → рудаков)
    # group_tag   — роль: "клиент", "депутат", "оппонент" и т.д.
    #               Нужен для фильтрации: "покажи упоминания только клиентов"
    #
    conn.execute("""
        CREATE TABLE IF NOT EXISTS persons (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            normal_form TEXT    NOT NULL,
            group_tag   TEXT    NOT NULL DEFAULT '',
            active      INTEGER NOT NULL DEFAULT 1,
            created_at  TEXT    NOT NULL DEFAULT (datetime('now')),

            UNIQUE (normal_form)
        )
    """)

    # ─────────────────────────────────────────
    # ТАБЛИЦА 3: posts_cache — кэш постов
    # ─────────────────────────────────────────
    # Зачем кэш? Представь: сборщик запускается каждые 2 часа.
    # Без кэша он будет каждый раз заново скачивать те же посты
    # и находить те же упоминания (= дубли в mentions).
    #
    # platform_post_id — уникальный ID поста на платформе.
    #   VK: "{owner_id}_{post_id}" (например "-12345_678")
    #   TG: "{username}_{message_id}" (например "pohvistnevo_news_1234")
    #
    # post_text — полный текст поста (обрезаем до 2000 символов, чтобы база
    #             не разрасталась на гигабайты)
    # post_url  — прямая ссылка на пост
    # post_date — когда пост был опубликован (время автора)
    #
    conn.execute("""
        CREATE TABLE IF NOT EXISTS posts_cache (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id           INTEGER NOT NULL REFERENCES sources(id),
            platform_post_id    TEXT    NOT NULL,
            post_text           TEXT    NOT NULL,
            post_url            TEXT    NOT NULL DEFAULT '',
            post_date           TEXT    NOT NULL,
            fetched_at          TEXT    NOT NULL DEFAULT (datetime('now')),

            UNIQUE (platform_post_id)
        )
    """)
    # UNIQUE (platform_post_id) — если пост уже в кэше, повторно не вставляем.
    # При вставке используем INSERT OR IGNORE — тихо пропускает дубли.

    # ─────────────────────────────────────────
    # ТАБЛИЦА 4: mentions — найденные упоминания
    # ─────────────────────────────────────────
    # Это главная таблица, ради которой всё затевается.
    # Связывает: какую персону (person_id) нашли в каком посте (post_id)
    # из какого источника (source_id).
    #
    # snippet — фрагмент текста вокруг найденной фамилии (±100 символов).
    #           Чтобы в боте показать контекст, не загружая весь пост.
    # notified — отправили ли мы уже уведомление об этом упоминании.
    #            0 = нет, 1 = да. Чтобы не спамить одним и тем же.
    #
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mentions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id   INTEGER NOT NULL REFERENCES persons(id),
            source_id   INTEGER NOT NULL REFERENCES sources(id),
            post_id     INTEGER NOT NULL REFERENCES posts_cache(id),
            snippet     TEXT    NOT NULL,
            post_url    TEXT    NOT NULL DEFAULT '',
            post_date   TEXT    NOT NULL,
            found_at    TEXT    NOT NULL DEFAULT (datetime('now')),
            notified    INTEGER NOT NULL DEFAULT 0,

            UNIQUE (person_id, post_id)
        )
    """)
    # UNIQUE (person_id, post_id) — одна персона + один пост = максимум одно упоминание.
    # Даже если фамилия встречается в посте 5 раз — запись одна.

    # ─────────────────────────────────────────
    # ИНДЕКСЫ — ускоряют частые запросы
    # ─────────────────────────────────────────
    # Без индексов SQLite будет перебирать ВСЕ строки таблицы при каждом запросе.
    # Индекс — как алфавитный указатель в книге: вместо чтения всех страниц
    # сразу открываешь нужную.
    #
    # Этот индекс ускоряет запрос:
    #   "покажи все упоминания персоны X за последние 3 дня"
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_mentions_person_date
        ON mentions (person_id, post_date DESC)
    """)

    # Этот ускоряет:
    #   "есть ли этот пост уже в кэше?"  (при вставке новых постов)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_posts_cache_source
        ON posts_cache (source_id, post_date DESC)
    """)

    # Этот ускоряет:
    #   "покажи неотправленные уведомления"
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_mentions_not_notified
        ON mentions (notified) WHERE notified = 0
    """)

    conn.commit()
    conn.close()


# ─────────────────────────────────────────
# Если запустить файл напрямую: python db.py
# — создаст пустую базу с таблицами.
# Полезно для первичной настройки на сервере.
# ─────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    print(f"База создана: {DB_PATH}")

    # Проверяем, что всё на месте
    conn = get_connection()
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    print("Таблицы:", [t["name"] for t in tables])
    conn.close()
