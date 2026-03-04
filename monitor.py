"""
monitor.py — полный цикл мониторинга: сбор постов → поиск фамилий.

Это "конвейер", который запускается по расписанию.
Объединяет collector.py и searcher.py в одну цепочку.

Зачем отдельный файл, а не просто два вызова в bot.py?
1. Можно запустить вручную для теста: python3 monitor.py
2. Бот вызывает одну функцию, а не две
3. Здесь же живёт логика уведомлений (когда добавим)

Как использовать:
    from monitor import run_monitoring
    new_mentions = await run_monitoring()

Или из терминала:
    python3 monitor.py
"""

import asyncio
import logging
from collector import collect_all_posts
from searcher import search_mentions_in_new_posts
from db import get_connection

logger = logging.getLogger(__name__)


async def run_monitoring():
    """
    Запускает полный цикл мониторинга:
    1. Собрать новые посты из VK и TG
    2. Найти в них упоминания отслеживаемых персон
    3. Вернуть количество новых упоминаний

    Возвращает dict с результатами:
    {
        "new_posts": 42,
        "new_mentions": 5,
        "mentions_by_person": [("Казаков Виктор", 3), ("Рудаков Игорь", 2)]
    }
    """
    result = {
        "new_posts": 0,
        "new_mentions": 0,
        "mentions_by_person": [],
    }

    # ─── Шаг 1: Сбор постов ─────────────────
    try:
        new_posts = await collect_all_posts()
        result["new_posts"] = new_posts
        logger.info(f"Собрано новых постов: {new_posts}")
    except Exception as e:
        logger.error(f"Ошибка сбора постов: {e}")
        return result

    # Если новых постов нет — искать нечего, выходим
    if new_posts == 0:
        logger.info("Новых постов нет, поиск пропущен")
        return result

    # ─── Шаг 2: Поиск фамилий ───────────────
    # searcher — синхронная функция (SQLite не любит async).
    # Оборачиваем в run_in_executor, чтобы не блокировать event loop.
    #
    # Что это значит: asyncio крутит один поток. Если вызвать
    # синхронную функцию напрямую — весь бот "зависнет" на время поиска.
    # run_in_executor запускает функцию в отдельном потоке,
    # а основной поток продолжает обрабатывать команды бота.
    try:
        loop = asyncio.get_event_loop()
        new_mentions = await loop.run_in_executor(None, search_mentions_in_new_posts)
        result["new_mentions"] = new_mentions
        logger.info(f"Найдено новых упоминаний: {new_mentions}")
    except Exception as e:
        logger.error(f"Ошибка поиска упоминаний: {e}")
        return result

    # ─── Шаг 3: Сводка по персонам ──────────
    # Собираем статистику: кто сколько раз упоминается в НОВЫХ находках.
    # Это пригодится для уведомлений ("Найдено 3 новых упоминания Казакова").
    if new_mentions > 0:
        try:
            conn = get_connection()
            # Берём неотправленные упоминания (notified = 0)
            rows = conn.execute(
                """SELECT p.name, COUNT(m.id) as cnt
                   FROM mentions m
                   JOIN persons p ON p.id = m.person_id
                   WHERE m.notified = 0
                   GROUP BY p.id
                   ORDER BY cnt DESC"""
            ).fetchall()
            result["mentions_by_person"] = [(r["name"], r["cnt"]) for r in rows]
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка сводки: {e}")

    return result


def format_monitoring_report(result):
    """
    Форматирует результат мониторинга в текст для Telegram.

    Пример:
        🔍 Мониторинг: 42 новых поста, 5 упоминаний
        👤 Казаков Виктор — 3
        👤 Рудаков Игорь — 2
    """
    if result["new_mentions"] == 0:
        return None  # не шлём пустые отчёты

    lines = [
        f"🔍 Мониторинг: {result['new_posts']} новых постов, "
        f"{result['new_mentions']} упоминаний"
    ]

    for name, count in result["mentions_by_person"]:
        lines.append(f"  👤 {name} — {count}")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# Запуск для теста: python3 monitor.py
# ─────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    print("🔄 Запускаю полный цикл мониторинга...\n")
    result = asyncio.run(run_monitoring())

    report = format_monitoring_report(result)
    if report:
        print(f"\n{report}")
    else:
        print("\n📭 Новых упоминаний не найдено")
