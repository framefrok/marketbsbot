import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

from aiogram.filters import Command

from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.enums import ParseMode
from aiogram import F
from tinydb import TinyDB, Query, where
from dotenv import load_dotenv
import os
import math

# Загрузка токена
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Инициализация базы данных
db = TinyDB('database.json')
market_table = db.table('market_data')
alerts_table = db.table('alerts')

# Состояния FSM
class AlertSetup(StatesGroup):
    choosing_resource = State()
    choosing_direction = State()  # "up" или "down"
    entering_target_price = State()

# Парсинг сообщения рынка
def parse_market_message(text: str) -> Optional[Dict[str, Dict[str, float]]]:
    """
    Парсит сообщение рынка и возвращает словарь:
    {
        "Дерево": {"buy": 11.0, "sell": 9.0},
        "Камень": {"buy": 9.45, "sell": 7.73},
        ...
    }
    """
    lines = text.strip().split('\n')
    resource_pattern = r"^(.+?):.*?([0-9,]+)([🪵🪨🍞🐴])$"
    price_pattern = r"([📈📉])?.*?Купить/продать:\s*([0-9.,]+)\s*/\s*([0-9.,]+).*?💰"

    resources = {}
    current_resource = None

    for line in lines:
        line = line.strip()
        if not line or line == "🎪 Рынок":
            continue

        # Проверка на строку ресурса (например: "Дерево: 2,580,444🪵")
        res_match = re.match(resource_pattern, line)
        if res_match:
            resource_name = res_match.group(1).strip()
            # Убираем смайлы и оставляем только название
            emoji_map = {"🪵": "Дерево", "🪨": "Камень", "🍞": "Провизия", "🐴": "Лошади"}
            emoji = res_match.group(3)
            normalized_name = emoji_map.get(emoji, resource_name)
            current_resource = normalized_name
            continue

        # Проверка на строку цены (например: "📈Купить/продать: 11/9💰")
        price_match = re.search(price_pattern, line)
        if price_match and current_resource:
            # trend = price_match.group(1)  # 📈 или 📉 (не используется в расчетах)
            buy_price = float(price_match.group(2).replace(',', '.'))
            sell_price = float(price_match.group(3).replace(',', '.'))
            resources[current_resource] = {
                "buy": buy_price,
                "sell": sell_price
            }
            current_resource = None  # Сбрасываем после записи

    return resources if resources else None

# Получение данных за последние N минут
def get_recent_data(resource: str, minutes: int = 15) -> List[Dict]:
    MarketData = Query()
    cutoff_time = int((datetime.now() - timedelta(minutes=minutes)).timestamp())
    records = market_table.search(
        (MarketData.resource == resource) & (MarketData.timestamp >= cutoff_time)
    )
    # Сортируем по времени
    records.sort(key=lambda x: x['timestamp'])
    return records

# Получение последней записи для ресурса
def get_latest_data(resource: str) -> Optional[Dict]:
    MarketData = Query()
    records = market_table.search(MarketData.resource == resource)
    if not records:
        return None
    records.sort(key=lambda x: x['timestamp'], reverse=True)
    return records[0]

# Расчет скорости изменения цены
def calculate_speed(records: List[Dict], price_type: str = "buy") -> Optional[float]:
    """
    Возвращает скорость изменения цены в минуту.
    Если цена падает — отрицательное число.
    """
    if len(records) < 2:
        return None

    first = records[0]
    last = records[-1]

    price_delta = last[price_type] - first[price_type]
    time_delta_minutes = (last['timestamp'] - first['timestamp']) / 60.0

    if time_delta_minutes == 0:
        return None

    speed = price_delta / time_delta_minutes
    return round(speed, 4)

# Проверка тренда
def get_trend(records: List[Dict], price_type: str = "buy") -> str:
    """Определяет тренд на основе последних данных"""
    if len(records) < 2:
        return "stable"
    
    first = records[0][price_type]
    last = records[-1][price_type]
    
    if last > first:
        return "up"
    elif last < first:
        return "down"
    else:
        return "stable"

# Обработчик форварда с рынком
@dp.message(F.text.startswith('🎪 Рынок') | F.forward_from)
async def handle_market_forward(message: types.Message):
    try:
        data = parse_market_message(message.text)
        if not data:
            await message.reply("❌ Не удалось распознать данные рынка. Убедитесь, что формат сообщения верный.")
            return

        timestamp = int(message.date.timestamp())
        saved_count = 0

        for resource, prices in data.items():
            # Проверяем, есть ли уже запись с таким timestamp и resource
            MarketData = Query()
            existing = market_table.get(
                (MarketData.resource == resource) & (MarketData.timestamp == timestamp)
            )
            
            if not existing:
                market_table.insert({
                    "user_id": message.from_user.id,
                    "resource": resource,
                    "buy": prices["buy"],
                    "sell": prices["sell"],
                    "timestamp": timestamp,
                    "date": datetime.fromtimestamp(timestamp).isoformat()
                })
                saved_count += 1

        if saved_count > 0:
            await message.reply(f"✅ Сохранено {saved_count} записей рынка.")
            
            # Проверяем, есть ли данные за последние 15 минут для хотя бы одного ресурса
            any_recent = False
            for resource in ["Дерево", "Камень", "Провизия", "Лошади"]:
                if len(get_recent_data(resource)) >= 2:
                    any_recent = True
                    break

            if any_recent:
                await send_resource_selection(message.from_user.id)
        else:
            await message.reply("ℹ️ Данные уже были сохранены ранее.")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения рынка: {e}")
        await message.reply("❌ Произошла ошибка при обработке данных рынка.")

# Отправка выбора ресурса
async def send_resource_selection(user_id: int):
    # Создаем список кнопок
    buttons = []
    for resource in ["Дерево", "Камень", "Провизия", "Лошади"]:
        btn = InlineKeyboardButton(text=resource, callback_data=f"resource_{resource}")
        buttons.append([btn])  # Каждая кнопка на своей строке

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

    await bot.send_message(user_id, "📊 Выберите ресурс для отслеживания:", reply_markup=keyboard)

# Обработчик выбора ресурса
@dp.callback_query(F.data.startswith('resource_'))
async def process_resource_selection(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    resource = callback_query.data.split('_')[1]

    # Получаем последние данные для ресурса
    records = get_recent_data(resource)
    if len(records) < 2:
        await bot.send_message(callback_query.from_user.id, 
                              f"⚠️ Для {resource} недостаточно данных. Пришлите еще обновления рынка.")
        return

    # Сохраняем выбранный ресурс
    await state.update_data(resource=resource)
    
    # Рассчитываем текущую скорость и тренд
    speed = calculate_speed(records, "buy")
    trend = get_trend(records, "buy")
    current_price = records[-1]["buy"]
    
    trend_emoji = "📈" if trend == "up" else "📉" if trend == "down" else "➡️"
    trend_text = "растёт" if trend == "up" else "падает" if trend == "down" else "стабильна"
    
    # Создаем кнопки выбора направления
    buttons = [
        [InlineKeyboardButton(text="📉 Падение цены", callback_data="direction_down")],
        [InlineKeyboardButton(text="📈 Рост цены", callback_data="direction_up")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await bot.send_message(
        callback_query.from_user.id, 
        f"{trend_emoji} Вы выбрали {resource}. Текущая цена: {current_price}\n"
        f"Тренд: {trend_text} ({abs(speed):.4f} в минуту)\n\n"
        f"Что вас интересует?", 
        reply_markup=keyboard
    )
    await state.set_state(AlertSetup.choosing_direction)

# Обработчик отмены действий
@dp.callback_query(F.data == 'cancel_action')
async def cancel_action(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    await state.clear()
    await bot.send_message(callback_query.from_user.id, "❌ Действие отменено.")

# Обработчик выбора направления
@dp.callback_query(F.data.startswith('direction_'), AlertSetup.choosing_direction)
async def process_direction_selection(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    direction = "down" if callback_query.data == "direction_down" else "up"
    
    user_data = await state.get_data()
    resource = user_data["resource"]
    
    # Получаем последние данные для ресурса
    records = get_recent_data(resource)
    current_price = records[-1]["buy"]
    trend = get_trend(records, "buy")
    
    # Проверяем логику выбора направления
    if (direction == "down" and trend != "down") or (direction == "up" and trend != "up"):
        trend_text = "падает" if trend == "down" else "растёт" if trend == "up" else "стабильна"
        await bot.send_message(
            callback_query.from_user.id,
            f"⚠️ Внимание! Цена {resource} сейчас {trend_text}, а вы выбрали "
            f"{'падение' if direction == 'down' else 'рост'}. Уверены, что хотите продолжить?"
        )
    
    await state.update_data(direction=direction)
    await bot.send_message(
        callback_query.from_user.id, 
        f"💰 Введите целевую цену для {resource} (например: {current_price * 0.9:.2f}):"
    )
    await state.set_state(AlertSetup.entering_target_price)

# Обработчик ввода целевой цены
@dp.message(AlertSetup.entering_target_price)
# Обработчик ввода целевой цены
@dp.message(AlertSetup.entering_target_price)
async def process_target_price(message: types.Message, state: FSMContext):
    try:
        target_price = float(message.text.strip().replace(',', '.'))
        if target_price <= 0:
            await message.reply("❌ Цена должна быть положительным числом.")
            return
    except ValueError:
        await message.reply("❌ Пожалуйста, введите корректное число (например: 0.55).")
        return

    user_data = await state.get_data()
    resource = user_data["resource"]
    direction = user_data["direction"]

    # Получаем последние данные для расчета скорости
    records = get_recent_data(resource)
    if len(records) < 2:
        await message.reply("⚠️ Недостаточно данных для расчета скорости. Пришлите еще обновления рынка.")
        await state.clear()
        return

    # Рассчитываем скорость по buy-цене
    speed = calculate_speed(records, "buy")
    if speed is None:
        await message.reply("⚠️ Не удалось рассчитать скорость изменения цены.")
        await state.clear()
        return

    current_price = records[-1]["buy"]
    price_diff = target_price - current_price

    # Проверка логики направления
    if (direction == "down" and target_price >= current_price) or \
       (direction == "up" and target_price <= current_price):
        await message.reply(f"⚠️ При {('падении' if direction == 'down' else 'росте')} целевая цена должна быть {('ниже' if direction == 'down' else 'выше')} текущей ({current_price}).")
        return

    # Проверка соответствия направления и тренда
    trend = get_trend(records, "buy")
    if (direction == "down" and trend == "up") or (direction == "up" and trend == "down"):
        await message.reply("⚠️ Внимание! Выбранное направление противоречит текущему тренду. "
                          "Оповещение может никогда не сработать.")
        # Не прерываем процесс, просто предупреждаем

    # --- ИСПРАВЛЕНИЕ НАЧАЛО ---
    # Используем абсолютное значение скорости для расчета времени
    # Проверяем, движется ли цена в нужную сторону
    if (direction == "down" and speed >= 0) or (direction == "up" and speed <= 0):
        await message.reply("⚠️ Цена движется не в ту сторону, чтобы достичь вашей цели. Оповещение не будет установлено.")
        await state.clear()
        return

    # Рассчитываем время на основе абсолютных значений
    time_minutes = abs(price_diff) / abs(speed)
    # --- ИСПРАВЛЕНИЕ КОНЕЦ ---

    alert_time = datetime.now() + timedelta(minutes=time_minutes)
    alert_id = alerts_table.insert({
        "user_id": message.from_user.id,
        "resource": resource,
        "target_price": target_price,
        "direction": direction,
        "speed": speed,  # Сохраняем реальную скорость со знаком для истории/аналитики
        "current_price": current_price,
        "alert_time": alert_time.isoformat(),
        "created_at": datetime.now().isoformat(),
        "status": "active"
    })

    # Форматируем время срабатывания
    alert_time_str = alert_time.strftime("%H:%M:%S")
    
    await message.reply(
        f"✅ Таймер установлен!\n"
        f"Ресурс: {resource}\n"
        f"Текущая цена: {current_price:.2f}\n"
        f"Цель: {target_price:.2f} ({'падение' if direction == 'down' else 'рост'})\n"
        f"Скорость: {speed:+.4f} в минуту\n"
        f"Осталось: ~{int(time_minutes)} мин.\n"
        f"Ожидаемое время: {alert_time_str}\n\n"
        f"Бот оповестит вас, когда цена достигнет цели."
    )

    await state.clear()

    # Запуск фоновой задачи для отслеживания
    asyncio.create_task(schedule_alert(alert_id, message.from_user.id, resource, target_price, alert_time))

# Фоновая задача для отправки уведомления
# Фоновая задача для отправки уведомления
async def schedule_alert(alert_id: int, user_id: int, resource: str, target_price: float, alert_time: datetime):
    now = datetime.now()
    sleep_seconds = (alert_time - now).total_seconds()

    if sleep_seconds > 0:
        await asyncio.sleep(sleep_seconds)

    # Проверяем, не был ли алерт удален или деактивирован
    Alert = Query()
    alert = alerts_table.get(doc_id=alert_id)
    if not alert or alert.get('status') != 'active':
        return

    try:
        # Получаем самую актуальную цену на момент срабатывания таймера
        latest_data = get_latest_data(resource)
        if not latest_data:
            raise ValueError(f"No latest data found for resource: {resource}")

        current_price = latest_data['buy']
        direction = alert['direction']

        # --- КРИТИЧЕСКАЯ ПРОВЕРКА: ДОСТИГНУТА ЛИ ЦЕЛЬ? ---
        is_target_reached = False
        if direction == "down" and current_price <= target_price:
            is_target_reached = True
        elif direction == "up" and current_price >= target_price:
            is_target_reached = True

        if is_target_reached:
            await bot.send_message(
                user_id,
                f"🔔 {resource} достигла целевой цены!\n"
                f"Цель: {target_price:.2f}\n"
                f"Текущая цена: {current_price:.2f}\n\n"
                f"Время {'покупать!' if direction == 'down' else 'продавать!'}"
            )
            alerts_table.update({'status': 'completed'}, doc_ids=[alert_id])
        else:
            # Цель не достигнута — возможно, рынок изменился
            await bot.send_message(
                user_id,
                f"⏰ Таймер для {resource} сработал, но цель ({target_price:.2f}) "
                f"еще не достигнута (текущая цена: {current_price:.2f}).\n"
                f"Скорость рынка, вероятно, изменилась."
            )
            alerts_table.update({'status': 'expired'}, doc_ids=[alert_id])
        # --- КОНЕЦ КРИТИЧЕСКОЙ ПРОВЕРКИ ---

    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
        alerts_table.update({'status': 'error'}, doc_ids=[alert_id])

async def cleanup_expired_alerts():
    """
    Фоновая задача для очистки просроченных алертов.
    Удаляет алерты, которые:
    - активны (status == 'active')
    - время срабатывания (alert_time) было более часа назад
    """
    while True:
        try:
            Alert = Query()
            now = datetime.now()
            # Находим все активные алерты, время которых уже прошло более часа назад
            cutoff_time = (now - timedelta(hours=1)).isoformat()
            expired_alerts = alerts_table.search(
                (Alert.status == 'active') &
                (Alert.alert_time < cutoff_time)
            )

            if expired_alerts:
                expired_ids = [alert.doc_id for alert in expired_alerts]
                # Помечаем их как 'cleanup_expired' вместо полного удаления для истории
                alerts_table.update({'status': 'cleanup_expired'}, doc_ids=expired_ids)
                logger.info(f"Очистка: деактивировано {len(expired_ids)} просроченных алертов.")

        except Exception as e:
            logger.error(f"Ошибка при выполнении очистки просроченных алертов: {e}")

        # Ждем 10 минут перед следующей проверкой
        await asyncio.sleep(600)  # 600 секунд = 10 минут

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.reply(
        "👋 Привет! Я бот для отслеживания цен на рынке в игре.\n"
        "Просто перешлите сюда сообщение с рынком (с эмодзи 🎪), и я начну анализ.\n"
        "Когда соберу достаточно данных — предложу настроить оповещение.\n\n"
        "Доступные команды:\n"
        "/status - показать активные оповещения\n"
        "/history - показать историю цен\n"
        "/cancel - отменить все оповещения\n"
        "/help - подробная инструкция по использованию"
        
    )

# Команда /status — показать активные алерты
@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    Alert = Query()
    alerts = alerts_table.search((Alert.user_id == message.from_user.id) & (Alert.status == 'active'))

    if not alerts:
        await message.reply("📭 У вас нет активных оповещений.")
        return

    text = "📋 Ваши активные оповещения:\n\n"
    for alert in alerts:
        direction = "падение" if alert["direction"] == "down" else "рост"
        alert_time = datetime.fromisoformat(alert["alert_time"])
        remaining = alert_time - datetime.now()
        mins = int(remaining.total_seconds() // 60)
        secs = int(remaining.total_seconds() % 60)
        
        if mins < 0:
            text += (
                f"• {alert['resource']} → {alert['target_price']:.2f} ({direction})\n"
                f"  Должно было сработать: {alert_time.strftime('%H:%M:%S')}\n\n"
            )
        else:
            text += (
                f"• {alert['resource']} → {alert['target_price']:.2f} ({direction})\n"
                f"  Осталось: {mins} мин. {secs} сек.\n"
                f"  Сработает в: {alert_time.strftime('%H:%M:%S')}\n\n"
            )

    await message.reply(text)

# Команда /history - показать историю цен
@dp.message(Command("history"))
async def cmd_history(message: types.Message):
    try:
        # Получаем аргументы команды
        args = message.text.split()[1:] if len(message.text.split()) > 1 else []
        resource = args[0] if len(args) > 0 else None
        hours = int(args[1]) if len(args) > 1 else 24
        
        if not resource:
            await message.reply(
                "Укажите ресурс для просмотра истории. Например:\n"
                "/history Дерево\n"
                "/history Камень 12  (за последние 12 часов)"
            )
            return
        
        # Получаем данные за указанный период
        MarketData = Query()
        cutoff_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
        records = market_table.search(
            (MarketData.resource == resource) & (MarketData.timestamp >= cutoff_time)
        )
        
        if not records:
            await message.reply(f"Нет данных по {resource} за последние {hours} часов.")
            return
            
        # Сортируем по времени
        records.sort(key=lambda x: x['timestamp'])
        
        # Формируем сообщение с историей
        text = f"📊 История цен на {resource} за последние {hours} часов:\n\n"
        
        # Группируем по часам для удобства чтения
        current_hour = None
        for record in records[-10:]:  # Показываем последние 10 записей
            record_time = datetime.fromtimestamp(record['timestamp'])
            hour_str = record_time.strftime("%H:00")
            
            if hour_str != current_hour:
                text += f"\n🕐 {hour_str}:\n"
                current_hour = hour_str
                
            time_str = record_time.strftime("%H:%M")
            text += f"  {time_str} - Купить: {record['buy']:.2f}, Продать: {record['sell']:.2f}\n"
        
        # Добавляем информацию о текущем тренде
        recent_records = get_recent_data(resource, minutes=60)  # За последний час
        if recent_records and len(recent_records) >= 2:
            speed = calculate_speed(recent_records, "buy")
            trend = get_trend(recent_records, "buy")
            trend_text = "растёт 📈" if trend == "up" else "падает 📉" if trend == "down" else "стабильна ➡️"
            text += f"\nТренд: {trend_text} ({speed:+.4f}/мин)"
        
        await message.reply(text)
        
    except ValueError:
        await message.reply("Некорректный формат команды. Используйте: /history <ресурс> [часы]")
    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /history: {e}")
        await message.reply("Произошла ошибка при получении истории цен.")

# Команда /cancel — отменить все алерты
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message):
    Alert = Query()
    alerts = alerts_table.search((Alert.user_id == message.from_user.id) & (Alert.status == 'active'))
    
    if not alerts:
        await message.reply("🗑️ Нет активных оповещений для отмены.")
        return
        
    # Помечаем оповещения как отмененные вместо удаления
    for alert in alerts:
        alerts_table.update({'status': 'cancelled'}, doc_ids=[alert.doc_id])
    
    await message.reply(f"🗑️ Отменено {len(alerts)} оповещений.")

# Команда /help — показать инструкцию по использованию
@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    help_text = (
        "📖 <b>Полная инструкция по использованию бота</b>\n\n"
        "<b>1. Как начать:</b>\n"
        "• Перешлите в чат любое сообщение с рынка, начинающееся с эмодзи 🎪.\n"
        "• Бот автоматически сохранит цены на ресурсы: Дерево, Камень, Провизия, Лошади.\n"
        "• Как только накопится достаточно данных (минимум 2 записи за 15 минут), бот предложит настроить оповещение.\n\n"
        "<b>2. Настройка оповещения:</b>\n"
        "• Выберите ресурс из списка.\n"
        "• Укажите направление: рост 📈 или падение 📉 цены.\n"
        "• Введите целевую цену.\n"
        "• Бот рассчитает примерное время срабатывания и оповестит вас, когда цена достигнет цели!\n\n"
        "<b>3. Доступные команды:</b>\n"
        "• /start — приветственное сообщение и список команд.\n"
        "• /help — эта инструкция.\n"
        "• /status — показать все ваши активные оповещения и время до их срабатывания.\n"
        "• /history <ресурс> [часы] — показать историю цен. Пример: `/history Дерево 6`.\n"
        "• /cancel — отменить все ваши активные оповещения.\n\n"
        "<b>4. Важно:</b>\n"
        "• Бот работает на основе <i>вашей личной</i> истории цен. Чем чаще вы присылаете данные рынка, тем точнее прогнозы.\n"
        "• Если цена резко изменила направление движения, оповещение может не сработать. Бот пришлет уведомление, если цель не будет достигнута в расчетное время.\n"
        "• Просроченные оповещения (которые не сработали вовремя) автоматически удаляются из списка активных через час."
    )
    await message.reply(help_text, parse_mode=ParseMode.HTML)

# Обработка ошибок
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    logger.error(f"Ошибка при обработке запроса: {exception}")
    return True

# Запуск бота
async def main():
    logger.info("Бот запущен...")
    asyncio.create_task(cleanup_expired_alerts())
    await dp.start_polling(bot)



if __name__ == '__main__':
    asyncio.run(main())

