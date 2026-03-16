# Селянин — руководство по эксплуатации

## Архитектура

```
bot.py                  — главный файл: команды, дайджест, GigaChat, расписание
handlers_monitor.py     — хэндлеры мониторинга упоминаний (кнопки, пагинация)
collector.py            — сборщик постов из VK (API) и Telegram (скрейпинг)
searcher.py             — поиск фамилий в собранных постах
monitor.py              — оркестратор: collector → searcher (вызывается по расписанию)
db.py                   — инициализация SQLite, схема таблиц
import_csv.py           — одноразовый импорт источников из CSV
bot.db                  — база данных (не в git)
.env                    — секреты (не в git)
```

## Расписание бота

| Что                  | Когда                | Что делает                                  |
|----------------------|----------------------|---------------------------------------------|
| Дайджест             | 09:00 МСК ежедневно  | Парсит каналы → GigaChat резюме → отправка  |
| Мониторинг           | Каждые 2 часа        | Сбор постов → поиск фамилий → сохранение    |

---

## Деплой (стандартный)

### Локально
```bash
cd ~/tg_digest_bot
# внести правки...
git add .
git commit -m "описание"
git push
```

### На сервере
```bash
ssh root@<IP>
cd /root/tg-digest-bot
git pull
systemctl restart tgbot
```

### Проверка
```bash
journalctl -u tgbot -f                         # логи в реальном времени
journalctl -u tgbot --since "5 min ago"         # последние 5 минут
journalctl -u tgbot --since "09:00" --no-pager  # с конкретного времени
```

---

## Частые операции

### Добавить персону для мониторинга

На сервере:
```bash
cd /root/tg-digest-bot && python3 -c "
from db import get_connection
conn = get_connection()
conn.execute('''INSERT OR IGNORE INTO persons (name, normal_form, group_tag) VALUES (?, ?, ?)''',
    ('Фамилия Имя', 'фамилия', 'группа'))
conn.commit()
conn.close()
print('Готово')
"
```

- `name` — отображаемое имя: `Рудаков Игорь`
- `normal_form` — фамилия в нижнем регистре: `рудаков` (поиск идёт через `startswith`, ловит все падежи)
- `group_tag` — произвольная метка: `персона`, `клиент`, `депутат`, `оппонент`

После добавления прогнать поиск по существующим постам:
```bash
python3 -c "
from searcher import search_mentions_in_new_posts
print(search_mentions_in_new_posts())
"
```

### Удалить/деактивировать персону

```bash
cd /root/tg-digest-bot && python3 -c "
from db import get_connection
conn = get_connection()
conn.execute('UPDATE persons SET active=0 WHERE normal_form=?', ('фамилия',))
conn.commit()
conn.close()
print('Деактивировано')
"
```

### Посмотреть текущих персон

```bash
cd /root/tg-digest-bot && python3 -c "
from db import get_connection
conn = get_connection()
for r in conn.execute('SELECT id, name, normal_form, group_tag, active FROM persons'):
    status = '✅' if r['active'] else '❌'
    print(f'  {status} {r[\"id\"]}. {r[\"name\"]} | {r[\"normal_form\"]} | {r[\"group_tag\"]}')
conn.close()
"
```

### Посмотреть последние упоминания персоны

```bash
cd /root/tg-digest-bot && python3 -c "
from db import get_connection
conn = get_connection()
for r in conn.execute('''
    SELECT m.post_date, s.name, substr(m.snippet, 1, 80)
    FROM mentions m JOIN persons p ON p.id=m.person_id JOIN sources s ON s.id=m.source_id
    WHERE p.normal_form=\"фамилия\"
    ORDER BY m.post_date DESC LIMIT 10
'''):
    print(f'  {r[0][:16]} | {r[1][:30]} | {r[2]}...')
conn.close()
"
```
Замени `фамилия` на нужную (в нижнем регистре).

### Добавить источник (канал/паблик)

```bash
cd /root/tg-digest-bot && python3 -c "
from db import get_connection
conn = get_connection()
conn.execute('''INSERT OR IGNORE INTO sources
    (category, name, district, platform, platform_id, url)
    VALUES (?, ?, ?, ?, ?, ?)''',
    ('СМИ', 'Название канала', 'Самарская обл', 'tg', 'username', 'https://t.me/username'))
conn.commit()
conn.close()
print('Источник добавлен')
"
```

- `platform` — `tg` или `vk`
- `platform_id` — для TG: username без @. Для VK: screen_name или `club123456`

### Посмотреть статистику базы

```bash
cd /root/tg-digest-bot && python3 -c "
from db import get_connection
conn = get_connection()
sources = conn.execute('SELECT COUNT(*) FROM sources WHERE active=1').fetchone()[0]
posts = conn.execute('SELECT COUNT(*) FROM posts_cache').fetchone()[0]
mentions = conn.execute('SELECT COUNT(*) FROM mentions').fetchone()[0]
persons = conn.execute('SELECT COUNT(*) FROM persons WHERE active=1').fetchone()[0]
print(f'Источников: {sources}')
print(f'Постов в кэше: {posts}')
print(f'Упоминаний: {mentions}')
print(f'Персон: {persons}')
conn.close()
"
```

---

## Дайджест: как работает GigaChat

### Батчинг
Каналы разбиваются на группы по 7 (`GIGACHAT_BATCH_SIZE`). Каждый батч — отдельный запрос. Это нужно потому что:
- Большой промпт (20+ каналов) переполняет контекстное окно GigaChat
- При ошибке одного батча остальные всё равно отработают

### Цензура
GigaChat срабатывает на политический контент. В промпте используется нейтральная формулировка ("новостной дайджест", "без оценок и мнений"). Если цензура сработала — в логах будет:
```
⚠️ GigaChat ОТЦЕНЗУРИЛ батч! Каналы: ...
```

### Каналы без резюме
Если GigaChat не выдал резюме для канала (цензура, пропуск номера, ненумерованная строка) — канал НЕ попадает в дайджест. Пользователи видят только каналы с резюме.

### Путаница каналов
GigaChat может приписать контент одного канала другому. В промпте есть инструкция "резюме каждого канала должно описывать ТОЛЬКО посты ЭТОГО канала". Если путаница повторяется — уменьшить `GIGACHAT_BATCH_SIZE` до 5 или 3.

---

## Мониторинг: как работает поиск фамилий

### Алгоритм
1. `collector.py` обходит все источники (VK API + TG скрейпинг)
2. Новые посты сохраняются в `posts_cache` (дубли пропускаются по `platform_post_id`)
3. `searcher.py` разбивает текст каждого поста на слова
4. Каждое слово проверяется: `слово.lower().startswith(фамилия)` и `len(слово) <= len(фамилия) + 4`
5. Совпадение → запись в `mentions` с фрагментом контекста (±120 символов)

### Почему не pymorphy
pymorphy3 считает "Казаков" формой слова "казак", а "Шарков" — формой "шарк". Для фамилий `startswith` надёжнее.

### Ограничение длины (+4 символа)
Отсекает ложные срабатывания вроде "Казаковский район" (слишком длинное слово для склонения фамилии).

---

## Известные проблемы

### python-dotenv could not parse statement
При старте бота в логах 11 предупреждений о парсинге `.env`. Бот работает — переменные подхватываются. Чтобы починить:
```bash
cat -n .env
```
Проверить: нет ли лишних пробелов вокруг `=`, переносов строк внутри значений, BOM-маркеров.

### .env с многострочными значениями (GOOGLE_CREDENTIALS)
Если JSON-ключ содержит переносы строк — обернуть в одинарные кавычки:
```
GOOGLE_CREDENTIALS='{"type":"service_account",...}'
```
Весь JSON в одну строку, без переносов.

---

## Структура базы данных

| Таблица       | Назначение                              |
|---------------|-----------------------------------------|
| `sources`     | Источники: VK-паблики и TG-каналы       |
| `persons`     | Персоны для мониторинга                 |
| `posts_cache` | Кэш собранных постов                   |
| `mentions`    | Найденные упоминания персон             |

### Ключевые поля persons
- `normal_form` — фамилия в нижнем регистре (основа для поиска)
- `active` — 1 = мониторим, 0 = пропускаем
- `group_tag` — метка для группировки

### Ключевые поля mentions
- `snippet` — фрагмент текста ±120 символов вокруг фамилии
- `notified` — 0 = не отправлено, 1 = отправлено (пока не используется)
- `UNIQUE(person_id, post_id)` — одна персона в одном посте = одна запись

---

## Полезные команды для сервера

```bash
# Статус бота
systemctl status tgbot

# Перезапуск
systemctl restart tgbot

# Логи в реальном времени
journalctl -u tgbot -f

# Логи за период
journalctl -u tgbot --since "09:00" --until "09:05" --no-pager

# Ручной запуск мониторинга
cd /root/tg-digest-bot && python3 monitor.py

# Ручной запуск только сборщика
python3 collector.py

# Ручной запуск только поиска
python3 searcher.py
```
