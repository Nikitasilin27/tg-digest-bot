"""
collector.py — асинхронный сборщик постов из VK и Telegram.

Что делает:
1. Берёт из базы все активные источники (sources)
2. Параллельно (до 10 одновременно) запрашивает посты:
   - VK → через API (wall.get)
   - TG → через скрейпинг t.me/s/
3. Новые посты сохраняет в posts_cache
4. Возвращает список новых постов для дальнейшего поиска фамилий

Как использовать:
    from collector import collect_all_posts
    new_posts = await collect_all_posts()

Можно запустить отдельно для теста:
    python3 collector.py
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import aiohttp
from bs4 import BeautifulSoup
from db import get_connection, init_db

load_dotenv()

VK_SERVICE_TOKEN = os.getenv("VK_SERVICE_TOKEN")
VK_API_VERSION = "5.199"  # актуальная версия VK API

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Семафор — ограничитель параллельных запросов.
#
# Представь: у тебя 51 источник. Без ограничения aiohttp
# попытается открыть 51 соединение одновременно.
# VK забанит за спам, TG начнёт отдавать 429 (Too Many Requests).
#
# Семафор работает как турникет: пропускает максимум N запросов
# одновременно, остальные ждут в очереди.
# ─────────────────────────────────────────────
MAX_CONCURRENT = 10


# ─────────────────────────────────────────────
# VK: сбор постов через API
# ─────────────────────────────────────────────

def parse_vk_platform_id(platform_id):
    """
    Из platform_id определяет параметры для VK API.

    VK API метод wall.get принимает:
    - domain="pohvistnevo.vkurse"  (текстовый адрес)
    - owner_id=-208709480          (числовой ID, минус = группа)

    В нашей базе platform_id хранится как:
    - "pohvistnevo.vkurse"  → текстовый, передаём как domain
    - "club208709480"       → числовой, group → owner_id = -208709480
    - "public87563767"      → числовой, group → owner_id = -87563767
    - "id640300607"         → числовой, user  → owner_id = 640300607

    Возвращает dict с параметрами для API-запроса.
    """
    # Пробуем извлечь числовой ID из префикса
    match = re.match(r"^(club|public)(\d+)$", platform_id)
    if match:
        # Группа/паблик: ID со знаком минус
        return {"owner_id": -int(match.group(2))}

    match = re.match(r"^id(\d+)$", platform_id)
    if match:
        # Страница пользователя: ID без минуса
        return {"owner_id": int(match.group(2))}

    # Текстовый адрес (screen name): передаём как domain
    return {"domain": platform_id}


async def fetch_vk_posts(session, semaphore, source, vk_lock):
    """
    Запрашивает посты одного VK-источника.

    session   — aiohttp-сессия (переиспользуется для всех запросов)
    semaphore — ограничитель параллельности
    source    — строка из таблицы sources (dict-like)
    vk_lock   — блокировка, чтобы VK-запросы шли по одному с паузой

    Возвращает список dict-ов с постами или пустой список.
    """
    async with semaphore:  # ← ждём своей очереди через "турникет"
        # VK разрешает 3 запроса в секунду на сервисный токен.
        # Самый надёжный способ не нарваться — делать запросы
        # последовательно с паузой 0.35 сек (≈ 2.8 req/sec, с запасом).
        # vk_lock гарантирует, что в один момент идёт только один VK-запрос.
        try:
            # Формируем параметры запроса
            params = {
                "access_token": VK_SERVICE_TOKEN,
                "v": VK_API_VERSION,
                "count": 30,  # последние 30 постов (VK отдаёт макс 100)
            }
            params.update(parse_vk_platform_id(source["platform_id"]))

            url = "https://api.vk.com/method/wall.get"

            # Ждём очередь и делаем паузу — всё под lock-ом,
            # чтобы следующий VK-запрос не начался раньше времени.
            async with vk_lock:
                await asyncio.sleep(0.35)
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    data = await resp.json()

            # VK API возвращает ошибки в поле "error"
            if "error" in data:
                error_msg = data["error"].get("error_msg", "Unknown")
                logger.warning(f"VK API ошибка [{source['name']}]: {error_msg}")
                return []

            items = data.get("response", {}).get("items", [])
            posts = []

            for item in items:
                # item["date"] — Unix timestamp (секунды с 1970 года)
                post_date = datetime.fromtimestamp(item["date"], tz=timezone.utc)
                text = item.get("text", "").strip()

                if not text:
                    continue  # пропускаем посты без текста (репосты, фото и т.д.)

                # Уникальный ID поста в VK: "{owner_id}_{post_id}"
                # owner_id отрицательный для групп, положительный для пользователей
                owner_id = item.get("owner_id", 0)
                post_id = item.get("id", 0)
                platform_post_id = f"vk_{owner_id}_{post_id}"

                # Прямая ссылка на пост
                post_url = f"https://vk.com/wall{owner_id}_{post_id}"

                posts.append({
                    "source_id": source["id"],
                    "platform_post_id": platform_post_id,
                    "post_text": text[:2000],  # обрезаем, чтобы база не разрослась
                    "post_url": post_url,
                    "post_date": post_date.isoformat(),
                })

            logger.info(f"VK [{source['name'][:30]}]: {len(posts)} постов с текстом")
            return posts

        except asyncio.TimeoutError:
            logger.warning(f"VK таймаут [{source['name']}]")
            return []
        except Exception as e:
            logger.warning(f"VK ошибка [{source['name']}]: {e}")
            return []


# ─────────────────────────────────────────────
# Telegram: сбор постов через скрейпинг
# ─────────────────────────────────────────────
# Telegram не даёт API для чтения каналов (если бот не админ).
# Но у каждого публичного канала есть веб-превью: t.me/s/username
# Оттуда и парсим — ровно как в текущем bot.py, только через aiohttp.
# ─────────────────────────────────────────────

async def fetch_tg_posts(session, semaphore, source):
    """
    Парсит посты одного TG-канала через его публичное превью.
    """
    async with semaphore:
        try:
            url = f"https://t.me/s/{source['platform_id']}"
            headers = {"User-Agent": "Mozilla/5.0"}

            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning(f"TG [{source['name']}]: HTTP {resp.status}")
                    return []
                html = await resp.text()

            soup = BeautifulSoup(html, "html.parser")
            messages = soup.find_all("div", class_="tgme_widget_message")
            posts = []

            for msg in messages:
                # Извлекаем время поста
                time_tag = msg.find("time")
                if not time_tag or not time_tag.get("datetime"):
                    continue
                try:
                    post_date = datetime.fromisoformat(
                        time_tag["datetime"].replace("Z", "+00:00")
                    )
                except Exception:
                    continue

                # Извлекаем текст
                text_tag = msg.find("div", class_="tgme_widget_message_text")
                text = text_tag.get_text(separator=" ", strip=True) if text_tag else ""
                if not text:
                    continue

                # Уникальный ID: из атрибута data-post="channel/123"
                data_post = msg.get("data-post", "")
                if data_post:
                    platform_post_id = f"tg_{data_post.replace('/', '_')}"
                    # Ссылка на конкретное сообщение
                    post_url = f"https://t.me/{data_post}"
                else:
                    # Фоллбэк — используем username + дату
                    platform_post_id = f"tg_{source['platform_id']}_{post_date.isoformat()}"
                    post_url = url

                posts.append({
                    "source_id": source["id"],
                    "platform_post_id": platform_post_id,
                    "post_text": text[:2000],
                    "post_url": post_url,
                    "post_date": post_date.isoformat(),
                })

            logger.info(f"TG [{source['name'][:30]}]: {len(posts)} постов")
            return posts

        except asyncio.TimeoutError:
            logger.warning(f"TG таймаут [{source['name']}]")
            return []
        except Exception as e:
            logger.warning(f"TG ошибка [{source['name']}]: {e}")
            return []


# ─────────────────────────────────────────────
# Главная функция: собрать всё и сохранить
# ─────────────────────────────────────────────

async def collect_all_posts():
    """
    Обходит все активные источники, собирает посты,
    сохраняет новые в posts_cache.

    Возвращает количество НОВЫХ постов (которых раньше не было в базе).
    Это нужно, чтобы потом запускать поиск фамилий только по новым.
    """
    init_db()
    conn = get_connection()

    # Загружаем все активные источники
    sources = conn.execute(
        "SELECT id, name, platform, platform_id FROM sources WHERE active = 1"
    ).fetchall()

    if not sources:
        logger.warning("Нет активных источников в базе")
        conn.close()
        return 0

    logger.info(f"Начинаю сбор из {len(sources)} источников...")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    # vk_lock — чтобы VK-запросы шли строго по одному с паузой.
    # TG-запросы идут параллельно — у Telegram нет такого жёсткого лимита.
    vk_lock = asyncio.Lock()

    # ─── Запускаем все запросы параллельно ───
    # aiohttp.ClientSession — это "браузер", который переиспользует
    # TCP-соединения. Создаём один на весь сбор, а не по одному на запрос.
    #
    # ssl=False — отключаем проверку SSL-сертификатов.
    # На macOS Python часто не имеет актуальных корневых сертификатов,
    # и все HTTPS-запросы падают с CERTIFICATE_VERIFY_FAILED.
    # На сервере (Ubuntu VPS) это тоже безопаснее — меньше сюрпризов.
    # Для наших целей (чтение публичных данных) это абсолютно нормально.
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for source in sources:
            if source["platform"] == "vk":
                tasks.append(fetch_vk_posts(session, semaphore, source, vk_lock))
            elif source["platform"] == "tg":
                tasks.append(fetch_tg_posts(session, semaphore, source))

        # asyncio.gather запускает все задачи и ждёт завершения ВСЕХ.
        # return_exceptions=True — если одна задача упадёт, остальные
        # продолжат работу (а не упадут все вместе).
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # ─── Сохраняем в базу ────────────────────
    new_count = 0

    for result in results:
        # Если задача упала — result будет Exception, пропускаем
        if isinstance(result, Exception):
            logger.error(f"Ошибка сбора: {result}")
            continue

        for post in result:
            try:
                # INSERT OR IGNORE: если platform_post_id уже есть — пропускаем.
                # Это защита от дублей при повторных запусках.
                cursor = conn.execute(
                    """INSERT OR IGNORE INTO posts_cache
                       (source_id, platform_post_id, post_text, post_url, post_date)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        post["source_id"],
                        post["platform_post_id"],
                        post["post_text"],
                        post["post_url"],
                        post["post_date"],
                    ),
                )
                # rowcount = 1 если вставка прошла, 0 если дубль
                if cursor.rowcount > 0:
                    new_count += 1
            except Exception as e:
                logger.error(f"Ошибка сохранения поста: {e}")

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM posts_cache").fetchone()[0]
    logger.info(f"Сбор завершён. Новых постов: {new_count}, всего в базе: {total}")

    conn.close()
    return new_count


# ─────────────────────────────────────────────
# Запуск для теста: python3 collector.py
# ─────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    if not VK_SERVICE_TOKEN:
        print("❌ VK_SERVICE_TOKEN не задан в .env")
        print("   Добавь строку: VK_SERVICE_TOKEN=твой_токен")
        exit(1)

    print("🔄 Запускаю сбор постов...\n")
    new = asyncio.run(collect_all_posts())
    print(f"\n✅ Готово! Новых постов: {new}")

    # Покажем примеры собранных постов
    conn = get_connection()
    print("\n── Последние 5 постов в базе ──")
    rows = conn.execute(
        """SELECT pc.post_date, s.platform, s.name, pc.post_text
           FROM posts_cache pc
           JOIN sources s ON s.id = pc.source_id
           ORDER BY pc.post_date DESC
           LIMIT 5"""
    ).fetchall()
    for r in rows:
        platform = r["platform"].upper()
        name = r["name"][:30]
        text = r["post_text"][:80].replace("\n", " ")
        print(f"   [{platform}] {name}: {text}...")
    conn.close()
