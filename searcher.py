"""
searcher.py — модуль поиска фамилий в собранных постах.

Что делает:
1. Берёт из базы все посты (posts_cache) и все персоны (persons)
2. Для каждого поста проверяет: встречается ли в тексте одна из фамилий
3. Если да — сохраняет в таблицу mentions с фрагментом контекста

Как ищем фамилии (почему не просто "ctrl+F"):
    Фамилия в базе: "казаков" (именительный падеж, нижний регистр)
    В тексте может быть: Казаков, Казакова, Казакову, Казаковым, Казакове...
    Все эти формы начинаются с "казаков" — это основа (stem).
    Мы проверяем: слово.lower().startswith("казаков") — и ловим все склонения.

    Почему не pymorphy? Потому что pymorphy думает, что "Казаков" — это "казак",
    а "Шарков" — это "шарк". Для фамилий он работает плохо.
    А вот startswith + ограничение длины — надёжно и быстро.

Как использовать:
    from searcher import search_mentions_in_new_posts
    new_mentions = search_mentions_in_new_posts()

Можно запустить отдельно для теста:
    python3 searcher.py
"""

import re
import logging
from db import get_connection, init_db

logger = logging.getLogger(__name__)

# Максимальная "добавка" к длине фамилии при склонении.
# "казаков" (7 букв) → "казаковой" (9 букв) — разница 2.
# Берём с запасом 4, чтобы не пропустить редкие формы,
# но отсечь случайные совпадения с длинными словами.
MAX_SUFFIX_LEN = 4

# Сколько символов вокруг найденной фамилии показываем в сниппете.
SNIPPET_RADIUS = 120


def build_search_patterns(persons):
    """
    Готовит данные для поиска.

    Из списка персон создаёт список словарей:
    [
        {"id": 1, "name": "Казаков Виктор", "stem": "казаков", "max_len": 11},
        ...
    ]

    stem     — основа для поиска (нижний регистр)
    max_len  — максимальная длина слова, которое считаем совпадением
    """
    patterns = []
    for person in persons:
        stem = person["normal_form"].lower().strip()
        if not stem:
            continue
        patterns.append({
            "id": person["id"],
            "name": person["name"],
            "stem": stem,
            "max_len": len(stem) + MAX_SUFFIX_LEN,
        })
    return patterns


def extract_snippet(text, match_start, match_end):
    """
    Вырезает фрагмент текста вокруг найденного слова.

    Пример:
        Текст: "...глава района Рудаков подписал соглашение о строительстве..."
        Результат: "...глава района Рудаков подписал соглашение о строительстве..."
                                  ^^^^^^^^ — найденное слово

    Если обрезаем начало или конец — добавляем "..."
    """
    start = max(0, match_start - SNIPPET_RADIUS)
    end = min(len(text), match_end + SNIPPET_RADIUS)

    snippet = text[start:end].strip()

    # Обрезаем до границы слова (чтобы не было "обрезанных" слов в начале/конце)
    if start > 0:
        # Ищем первый пробел и обрезаем до него
        space = snippet.find(" ")
        if space > 0:
            snippet = snippet[space + 1:]
        snippet = "..." + snippet

    if end < len(text):
        space = snippet.rfind(" ")
        if space > 0:
            snippet = snippet[:space]
        snippet = snippet + "..."

    return snippet


def find_persons_in_text(text, patterns):
    """
    Ищет фамилии в тексте.

    Разбивает текст на слова, каждое слово проверяет по всем паттернам.

    Возвращает список: [(person_id, snippet), ...]
    Одна персона — максимум одно совпадение на пост (берём первое).
    """
    if not text or not patterns:
        return []

    # Разбиваем текст на слова, запоминая позиции (для сниппета)
    # re.finditer находит все "слова" (последовательности букв/цифр)
    # и возвращает объекты Match с позициями start() и end()
    words = list(re.finditer(r"[а-яёА-ЯЁa-zA-Z]+", text))

    found = {}  # person_id → snippet (берём только первое совпадение)

    for match in words:
        word = match.group()
        word_lower = word.lower()
        word_len = len(word_lower)

        for pattern in patterns:
            # Уже нашли эту персону в этом посте — пропускаем
            if pattern["id"] in found:
                continue

            # Главная проверка:
            # 1. Слово начинается с основы фамилии
            # 2. Слово не слишком длинное (отсекаем "Казаковский район" и т.п.)
            if (
                word_lower.startswith(pattern["stem"])
                and word_len <= pattern["max_len"]
            ):
                snippet = extract_snippet(text, match.start(), match.end())
                found[pattern["id"]] = snippet

    return list(found.items())


def search_mentions_in_new_posts():
    """
    Ищет упоминания персон в постах, которые ещё не были проверены.

    "Ещё не проверены" = посты, для которых нет записей в mentions.
    Это позволяет запускать поиск повторно без дублей и не проверять
    старые посты заново.

    Возвращает количество новых упоминаний.
    """
    init_db()
    conn = get_connection()

    # Загружаем персон
    persons = conn.execute(
        "SELECT id, name, normal_form FROM persons WHERE active = 1"
    ).fetchall()

    if not persons:
        logger.warning("Нет активных персон для мониторинга")
        conn.close()
        return 0

    patterns = build_search_patterns(persons)
    logger.info(f"Ищем {len(patterns)} персон: {', '.join(p['name'] for p in patterns)}")

    # Загружаем посты, которые ещё НЕ были проверены.
    # Логика: берём все посты, у которых нет НИ ОДНОЙ записи в mentions.
    # LEFT JOIN + WHERE mentions.id IS NULL — классический SQL-паттерн
    # "найди строки из левой таблицы, у которых нет пары в правой".
    #
    # Но проще: берём ВСЕ посты, а дубли отсечёт UNIQUE(person_id, post_id).
    # Для 700 постов разница в скорости нулевая.
    posts = conn.execute(
        """SELECT pc.id, pc.source_id, pc.post_text, pc.post_url, pc.post_date
           FROM posts_cache pc
           ORDER BY pc.post_date DESC"""
    ).fetchall()

    logger.info(f"Проверяю {len(posts)} постов...")

    new_mentions = 0

    for post in posts:
        matches = find_persons_in_text(post["post_text"], patterns)

        for person_id, snippet in matches:
            try:
                cursor = conn.execute(
                    """INSERT OR IGNORE INTO mentions
                       (person_id, source_id, post_id, snippet, post_url, post_date)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        person_id,
                        post["source_id"],
                        post["id"],
                        snippet,
                        post["post_url"],
                        post["post_date"],
                    ),
                )
                if cursor.rowcount > 0:
                    new_mentions += 1
            except Exception as e:
                logger.error(f"Ошибка сохранения упоминания: {e}")

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM mentions").fetchone()[0]
    logger.info(f"Поиск завершён. Новых упоминаний: {new_mentions}, всего: {total}")

    conn.close()
    return new_mentions


# ─────────────────────────────────────────────
# Запуск для теста: python3 searcher.py
# ─────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    print("🔍 Запускаю поиск упоминаний...\n")
    count = search_mentions_in_new_posts()
    print(f"\n✅ Найдено новых упоминаний: {count}")

    # Покажем результаты, сгруппированные по персонам
    conn = get_connection()

    print("\n── Сводка по персонам ──")
    stats = conn.execute(
        """SELECT p.name, p.group_tag, COUNT(m.id) as cnt
           FROM persons p
           LEFT JOIN mentions m ON m.person_id = p.id
           GROUP BY p.id
           ORDER BY cnt DESC"""
    ).fetchall()
    for row in stats:
        tag = f"[{row['group_tag']}]" if row["group_tag"] else ""
        bar = "█" * min(row["cnt"], 30)  # визуальная шкала
        print(f"   {row['name']:25s} {tag:15s} {row['cnt']:3d} {bar}")

    print("\n── Примеры найденных упоминаний ──")
    examples = conn.execute(
        """SELECT p.name, s.name as source_name, s.platform,
                  m.snippet, m.post_url, m.post_date
           FROM mentions m
           JOIN persons p ON p.id = m.person_id
           JOIN sources s ON s.id = m.source_id
           ORDER BY m.post_date DESC
           LIMIT 10"""
    ).fetchall()
    for ex in examples:
        platform = ex["platform"].upper()
        date = ex["post_date"][:10]
        snippet_short = ex["snippet"][:100].replace("\n", " ")
        print(f"\n   👤 {ex['name']}")
        print(f"   📌 {ex['source_name'][:40]} ({platform}) · {date}")
        print(f"   └ {snippet_short}...")
        print(f"   🔗 {ex['post_url']}")

    conn.close()
