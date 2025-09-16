import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackContext,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters
)
from tinydb import TinyDB, Query
from dotenv import load_dotenv
import os

# Загрузка токена
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Токен бота не найден. Убедитесь, что BOT_TOKEN установлен в .env")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация базы данных
db = TinyDB('database.json')
market_table = db.table('market_data')
alerts_table = db.table('alerts')
settings_table = db.table('settings')

# Простая система состояний
user_states = {}  # user_id -> state_name
user_data = {}    # user_id -> dict с данными (resource, direction и т.д.)

# Состояния
STATE_CHOOSING_RESOURCE = "choosing_resource"
STATE_CHOOSING_DIRECTION = "choosing_direction"
STATE_ENTERING_TARGET_PRICE = "entering_target_price"
STATE_SETTINGS_ANCHOR = "settings_anchor"
STATE_SETTINGS_TRADE_LEVEL = "settings_trade_level"

# Эмодзи → Название ресурса
EMOJI_TO_RESOURCE = {
    "🪵": "Дерево",
    "🪨": "Камень",
    "🍞": "Провизия",
    "🐴": "Лошади"
}

RESOURCE_EMOJI = {v: k for k, v in EMOJI_TO_RESOURCE.items()}

# Получение настроек пользователя
def get_user_settings(user_id: int) -> Dict[str, Union[bool, int]]:
    Settings = Query()
    setting = settings_table.get(Settings.user_id == user_id)
    if setting:
        return {
            "has_anchor": setting.get("has_anchor", False),
            "trade_level": setting.get("trade_level", 0),
            "push_interval": setting.get("push_interval", 30),
            "push_enabled": setting.get("push_enabled", True)
        }
    return {
        "has_anchor": False,
        "trade_level": 0,
        "push_interval": 30,
        "push_enabled": True
    }

# Сохранение настроек пользователя
def save_user_settings(user_id: int, has_anchor: bool, trade_level: int, push_interval: int = 30, push_enabled: bool = True):
    settings_table.upsert({
        "user_id": user_id,
        "has_anchor": has_anchor,
        "trade_level": trade_level,
        "push_interval": push_interval,
        "push_enabled": push_enabled
    }, Query().user_id == user_id)

# Расчет бонуса пользователя (процент выгоды)
def get_user_bonus(user_id: int) -> float:
    settings = get_user_settings(user_id)
    bonus = 0.02 if settings["has_anchor"] else 0.0
    bonus += 0.02 * settings["trade_level"]
    return bonus

# Корректировка цен для пользователя (базовые цены -> персональные)
def adjust_prices_for_user(user_id: int, buy: float, sell: float) -> Tuple[float, float]:
    bonus = get_user_bonus(user_id)
    adjusted_buy = buy / (1 + bonus)
    adjusted_sell = sell * (1 + bonus)
    return adjusted_buy, adjusted_sell

# Парсинг сообщения рынка
def parse_market_message(text: str) -> Optional[Dict[str, Dict[str, Union[float, int]]]]:
    lines = [line.strip() for line in text.strip().split('\n') if line.strip()]
    resources = {}
    current_resource = None
    current_quantity = 0

    resource_pattern = r"^(.+?):\s*([0-9,]*)\s*([🪵🪨🍞🐴])$"
    price_pattern = r"(?:[📈📉]?\s*)?Купить/продать:\s*([0-9.]+)\s*/\s*([0-9.]+)\s*💰"

    for i, line in enumerate(lines):
        if line == "🎪 Рынок":
            continue

        res_match = re.match(resource_pattern, line)
        if res_match:
            name_part = res_match.group(1).strip()
            qty_str = res_match.group(2).replace(',', '').strip()
            emoji = res_match.group(3)

            current_resource = EMOJI_TO_RESOURCE.get(emoji, name_part)
            current_quantity = int(qty_str) if qty_str.isdigit() else 0
            continue

        price_match = re.search(price_pattern, line)
        if price_match and current_resource:
            try:
                buy_price = float(price_match.group(1))
                sell_price = float(price_match.group(2))
                resources[current_resource] = {
                    "buy": buy_price,
                    "sell": sell_price,
                    "quantity": current_quantity
                }
                logger.info(f"Распознано: {current_resource} — покупка {buy_price}, продажа {sell_price}, кол-во {current_quantity}")
                current_resource = None
                current_quantity = 0
            except ValueError as e:
                logger.error(f"Ошибка конвертации цен: {e}")
                continue

    if not resources:
        logger.warning("Не удалось распознать ни одного ресурса.")
        return None

    return resources

# Получение данных за последние N минут
def get_recent_data(resource: str, minutes: int = 15) -> List[Dict]:
    MarketData = Query()
    cutoff_time = int((datetime.now() - timedelta(minutes=minutes)).timestamp())
    records = market_table.search(
        (MarketData.resource == resource) & (MarketData.timestamp >= cutoff_time)
    )
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
    if len(records) < 2:
        return None

    first = records[0]
    last = records[-1]

    price_delta = last[price_type] - first[price_type]
    time_delta_minutes = (last['timestamp'] - first['timestamp']) / 60.0

    if time_delta_minutes < 0.1:
        return None

    speed = price_delta / time_delta_minutes
    return round(speed, 4)

# Проверка тренда
def get_trend(records: List[Dict], price_type: str = "buy") -> str:
    if len(records) < 2:
        return "stable"

    first_price = records[0][price_type]
    last_price = records[-1][price_type]

    if last_price > first_price:
        return "up"
    elif last_price < first_price:
        return "down"
    else:
        return "stable"

# Универсальная функция отправки сообщений
async def send_to_user_and_group(context: CallbackContext, user_id: int, chat_id: Optional[int], text: str, 
                                reply_to_message_id: Optional[int] = None, reply_markup: Optional[InlineKeyboardMarkup] = None):
    try:
        if reply_to_message_id:
            await context.bot.send_message(chat_id=user_id, text=text, reply_to_message_id=reply_to_message_id, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup)

        if chat_id and chat_id != user_id:
            try:
                if reply_to_message_id:
                    await context.bot.send_message(chat_id=chat_id, text=text, reply_to_message_id=reply_to_message_id, reply_markup=reply_markup)
                else:
                    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
            except Exception as e:
                logger.error(f"Не удалось отправить сообщение в групповой чат {chat_id}: {e}")
                await context.bot.send_message(chat_id=user_id, text=f"⚠️ Не удалось отправить сообщение в групповой чат {chat_id}.")
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")

# Отправка выбора ресурса
async def send_resource_selection(context: CallbackContext, user_id: int, chat_id: Optional[int] = None):
    buttons = [
        [InlineKeyboardButton(text=res, callback_data=f"resource_{res}")]
        for res in EMOJI_TO_RESOURCE.values()
    ]
    keyboard = InlineKeyboardMarkup(buttons)

    try:
        user = await context.bot.get_chat(user_id)
        username = user.username or 'User'
    except:
        username = 'User'
    message_text = f"📊 @{username}, выберите ресурс для отслеживания:"

    await send_to_user_and_group(context, user_id, chat_id, message_text, reply_markup=keyboard)

# Обработчик форварда с рынком
async def handle_market_forward(update: Update, context: CallbackContext):
    try:
        message = update.message
        if not message.forward_from or not message.forward_from.is_bot:
            await message.reply_text("❌ Сообщение должно быть пересылкой от бота рынка.")
            return

        logger.info(f"Forwarded from: {message.forward_from.username} (ID: {message.forward_from.id})")

        current_time = datetime.now().timestamp()
        if current_time - message.date.timestamp() > 3600:
            await message.reply_text("❌ Сообщение слишком старое (более часа). Используйте свежие обновления.")
            return

        logger.info("Обработка сообщения рынка...")
        data = parse_market_message(message.text)
        if not data:
            await message.reply_text("❌ Не удалось распознать данные рынка. Проверьте формат сообщения.")
            return

        timestamp = int(message.date.timestamp())
        saved_count = 0
        user_id = message.from_user.id
        chat_id = message.chat.id if message.chat.type in ['group', 'supergroup'] else None

        for resource, prices in data.items():
            MarketData = Query()
            existing = market_table.get(
                (MarketData.resource == resource) &
                (MarketData.timestamp == timestamp) &
                (MarketData.buy == prices["buy"]) &
                (MarketData.sell == prices["sell"])
            )

            if not existing:
                try:
                    market_table.insert({
                        "resource": resource,
                        "buy": prices["buy"],
                        "sell": prices["sell"],
                        "quantity": prices.get("quantity", 0),
                        "timestamp": timestamp,
                        "date": datetime.fromtimestamp(timestamp).isoformat()
                    })
                    saved_count += 1
                    logger.info(f"Сохранено: {resource} - buy={prices['buy']}, sell={prices['sell']}, qty={prices['quantity']}")
                except Exception as db_e:
                    logger.error(f"Ошибка при записи в базу данных для {resource}: {db_e}")
                    await send_to_user_and_group(context, user_id, chat_id, f"❌ Ошибка при сохранении данных для {resource}.", message.message_id)
                    continue

        if saved_count > 0:
            await send_to_user_and_group(context, user_id, chat_id, f"✅ Сохранено {saved_count} записей рынка.", message.message_id)

            for resource in EMOJI_TO_RESOURCE.values():
                if len(get_recent_data(resource, 15)) >= 2:
                    await send_resource_selection(context, user_id, chat_id)
                    break

            context.application.create_task(update_dynamic_timers_once(context))

        else:
            await send_to_user_and_group(context, user_id, chat_id, "ℹ️ Данные уже были сохранены ранее.", message.message_id)

    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения рынка: {e}", exc_info=True)
        await send_to_user_and_group(context, update.effective_user.id, None, f"❌ Произошла ошибка: {str(e)}. Пожалуйста, попробуйте снова или свяжитесь с поддержкой.", update.message.message_id)

# Обработчик выбора ресурса
async def process_resource_selection(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    resource = query.data.split('_', 1)[1]

    records = get_recent_data(resource, 15)
    if len(records) < 2:
        chat_id = query.message.chat.id if query.message.chat.type in ['group', 'supergroup'] else None
        await send_to_user_and_group(context, query.from_user.id, chat_id, 
                                   f"⚠️ Для {resource} недостаточно данных. Пришлите еще обновления рынка.", query.message.message_id)
        return

    user_id = query.from_user.id
    chat_id = query.message.chat.id if query.message.chat.type in ['group', 'supergroup'] else None
    user_states[user_id] = STATE_CHOOSING_DIRECTION
    user_data[user_id] = {"resource": resource, "chat_id": chat_id}

    speed = calculate_speed(records, "buy")
    trend = get_trend(records, "buy")
    current_price = records[-1]["buy"]

    # ВАЖНО: данные уже содержат бонус игрока, поэтому корректировка не нужна
    bonus = get_user_bonus(user_id)
    # Используем цены как есть, без дополнительной корректировки
    adjusted_buy = current_price
    adj_speed = speed  # Скорость также не корректируем

    trend_emoji = "📈" if trend == "up" else "📉" if trend == "down" else "➡️"
    trend_text = "растёт" if trend == "up" else "падает" if trend == "down" else "стабильна"

    buttons = [
        [InlineKeyboardButton(text="📉 Падение цены", callback_data="direction_down")],
        [InlineKeyboardButton(text="📈 Рост цены", callback_data="direction_up")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ]
    keyboard = InlineKeyboardMarkup(buttons)

    speed_text = f"{adj_speed:+.4f}" if adj_speed is not None else "неизвестно"
    message_text = (
        f"📊 @{query.from_user.username or 'User'}, вы выбрали {resource}. "
        f"Текущая цена: {adjusted_buy:.2f}\n"
        f"Тренд: {trend_text} ({speed_text} в минуту)\n\n"
        f"Что вас интересует?"
    )

    await send_to_user_and_group(context, user_id, chat_id, message_text, query.message.message_id, keyboard)

# Обработчик отмены действий
async def cancel_action(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_states.pop(user_id, None)
    user_data.pop(user_id, None)
    chat_id = query.message.chat.id if query.message.chat.type in ['group', 'supergroup'] else None
    await send_to_user_and_group(context, user_id, chat_id, "❌ Действие отменено.", query.message.message_id)

# Обработчик выбора направления
async def process_direction_selection(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if user_states.get(user_id) != STATE_CHOOSING_DIRECTION:
        return

    direction = "down" if query.data == "direction_down" else "up"
    resource = user_data[user_id]["resource"]
    chat_id = user_data[user_id].get("chat_id")

    records = get_recent_data(resource, 15)
    current_price = records[-1]["buy"]
    trend = get_trend(records, "buy")

    # Используем цену как есть (уже с бонусом)
    adjusted_buy = current_price

    if (direction == "down" and trend != "down") or (direction == "up" and trend != "up"):
        trend_text = "падает" if trend == "down" else "растёт" if trend == "up" else "стабильна"
        message_text = (
            f"⚠️ @{query.from_user.username or 'User'}, внимание! Цена {resource} сейчас {trend_text}, "
            f"а вы выбрали {'падение' if direction == 'down' else 'рост'}. "
            f"Уверены, что хотите продолжить?"
        )
        await send_to_user_and_group(context, user_id, chat_id, message_text, query.message.message_id)

    user_data[user_id]["direction"] = direction
    user_states[user_id] = STATE_ENTERING_TARGET_PRICE

    message_text = f"💰 @{query.from_user.username or 'User'}, введите целевую цену для {resource} (например: {adjusted_buy * 0.9:.2f}):"
    await send_to_user_and_group(context, user_id, chat_id, message_text, query.message.message_id)

# Обработчик ввода целевой цены
async def process_target_price(update: Update, context: CallbackContext):
    message = update.message
    user_id = message.from_user.id
    
    if user_states.get(user_id) != STATE_ENTERING_TARGET_PRICE:
        return

    chat_id = user_data.get(user_id, {}).get("chat_id")
    try:
        target_price = float(message.text.strip().replace(',', '.'))
        if target_price <= 0:
            await send_to_user_and_group(context, user_id, chat_id, "❌ Цена должна быть положительным числом.", message.message_id)
            return
    except ValueError:
        await send_to_user_and_group(context, user_id, chat_id, "❌ Пожалуйста, введите корректное число (например: 0.55).", message.message_id)
        return

    resource = user_data[user_id]["resource"]
    direction = user_data[user_id]["direction"]

    records = get_recent_data(resource, 15)
    if len(records) < 2:
        await send_to_user_and_group(context, user_id, chat_id, "⚠️ Недостаточно данных для расчета скорости. Пришлите еще обновления рынка.", message.message_id)
        user_states.pop(user_id, None)
        user_data.pop(user_id, None)
        return

    speed = calculate_speed(records, "buy")
    if speed is None:
        await send_to_user_and_group(context, user_id, chat_id, "⚠️ Не удалось рассчитать скорость изменения цены.", message.message_id)
        user_states.pop(user_id, None)
        user_data.pop(user_id, None)
        return

    # Используем скорость и цену как есть (уже с бонусом)
    adj_speed = speed
    current_price = records[-1]["buy"]
    adjusted_buy = current_price
    price_diff = target_price - adjusted_buy

    if (direction == "down" and target_price >= adjusted_buy) or \
       (direction == "up" and target_price <= adjusted_buy):
        message_text = f"⚠️ @{message.from_user.username or 'User'}, при {('падении' if direction == 'down' else 'росте')} целевая цена должна быть {('ниже' if direction == 'down' else 'выше')} текущей ({adjusted_buy:.2f})."
        await send_to_user_and_group(context, user_id, chat_id, message_text, message.message_id)
        return

    trend = get_trend(records, "buy")
    if (direction == "down" and trend == "up") or (direction == "up" and trend == "down"):
        message_text = (
            f"⚠️ @{message.from_user.username or 'User'}, внимание! Выбранное направление противоречит текущему тренду. "
            f"Оповещение может никогда не сработать."
        )
        await send_to_user_and_group(context, user_id, chat_id, message_text, message.message_id)

    if (direction == "down" and adj_speed >= 0) or (direction == "up" and adj_speed <= 0):
        message_text = f"⚠️ @{message.from_user.username or 'User'}, цена движется не в ту сторону, чтобы достичь вашей цели. Оповещение не будет установлено."
        await send_to_user_and_group(context, user_id, chat_id, message_text, message.message_id)
        user_states.pop(user_id, None)
        user_data.pop(user_id, None)
        return

    time_minutes = abs(price_diff) / abs(adj_speed)
    alert_time = datetime.now() + timedelta(minutes=time_minutes)
    alert_id = alerts_table.insert({
        "user_id": user_id,
        "resource": resource,
        "target_price": target_price,
        "direction": direction,
        "speed": adj_speed,
        "current_price": adjusted_buy,
        "alert_time": alert_time.isoformat(),
        "created_at": datetime.now().isoformat(),
        "status": "active",
        "chat_id": chat_id,
        "message_id": None,
        "last_checked": datetime.now().isoformat()
    })

    alert_time_str = alert_time.strftime("%H:%M:%S")
    username = message.from_user.username or 'User'
    notification_text = (
        f"✅ @{username} установил таймер!\n"
        f"Ресурс: {resource}\n"
        f"Текущая цена: {adjusted_buy:.2f}\n"
        f"Цель: {target_price:.2f} ({'падение' if direction == 'down' else 'рост'})\n"
        f"Скорость: {adj_speed:+.4f} в минуту\n"
        f"Осталось: ~{int(time_minutes)} мин.\n"
        f"Ожидаемое время: {alert_time_str}"
    )

    sent_message = await message.reply_text(notification_text)

    message_id = None
    if chat_id and chat_id != user_id:
        try:
            chat = await context.bot.get_chat(chat_id)
            pinned_message = chat.pinned_message
            if not pinned_message:
                await context.bot.pin_chat_message(chat_id, sent_message.message_id, disable_notification=True)
                message_id = sent_message.message_id
            alerts_table.update({'message_id': message_id}, doc_ids=[alert_id])
            await context.bot.send_message(chat_id, notification_text)
        except Exception as e:
            logger.error(f"Не удалось отправить или закрепить сообщение в групповом чате {chat_id}: {e}")
            await context.bot.send_message(user_id, f"⚠️ @{username}, не удалось отправить или закрепить сообщение в групповом чате {chat_id}.")

    context.application.create_task(schedule_alert(context, alert_id, user_id, resource, target_price, alert_time, chat_id, message_id))

    user_states.pop(user_id, None)
    user_data.pop(user_id, None)

# Фоновая задача для отправки уведомления
async def schedule_alert(context: CallbackContext, alert_id: int, user_id: int, resource: str, target_price: float, 
                        alert_time: datetime, chat_id: Optional[int] = None, message_id: Optional[int] = None):
    now = datetime.now()
    sleep_seconds = max(0, (alert_time - now).total_seconds())

    await asyncio.sleep(sleep_seconds)

    Alert = Query()
    alert = alerts_table.get(doc_id=alert_id)
    if not alert or alert.get('status') != 'active':
        return

    try:
        latest_data = get_latest_data(resource)
        if not latest_data:
            raise ValueError(f"Нет последних данных для ресурса: {resource}")

        # Используем цену как есть (уже с бонусом)
        current_price = latest_data['buy']
        direction = alert['direction']

        is_target_reached = False
        if direction == "down" and current_price <= target_price:
            is_target_reached = True
        elif direction == "up" and current_price >= target_price:
            is_target_reached = True

        username = (await context.bot.get_chat(user_id)).username or 'User'
        notification_text = (
            f"🔔 @{username} {resource} достигла целевой цены!\n"
            f"Цель: {target_price:.2f}\n"
            f"Текущая цена: {current_price:.2f}\n\n"
            f"Время {'покупать!' if direction == 'down' else 'продавать!'}"
        ) if is_target_reached else (
            f"⏰ @{username} Таймер для {resource} сработал, но цель ({target_price:.2f}) "
            f"еще не достигнута (текущая цена: {current_price:.2f}).\n"
            f"Скорость рынка, вероятно, изменилась."
        )

        await context.bot.send_message(user_id, notification_text)

        if chat_id and chat_id != user_id:
            try:
                await context.bot.send_message(chat_id, notification_text)
                if message_id:
                    await context.bot.unpin_chat_message(chat_id, message_id)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление или открепить сообщение в групповом чате {chat_id}: {e}")
                await context.bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу или открепить сообщение.")

        alerts_table.update({'status': 'completed' if is_target_reached else 'expired'}, doc_ids=[alert_id])

    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
        alerts_table.update({'status': 'error'}, doc_ids=[alert_id])

# Команда /start
async def cmd_start(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    user_states.pop(user_id, None)
    user_data.pop(user_id, None)
    await update.message.reply_text(
        "👋 Привет! Я бот для отслеживания цен на рынке в игре BastionSiege.\n"
        "Просто перешлите сюда сообщение с рынком (с эмодзи 🎪), и я начну анализ.\n"
        "Когда соберу достаточно данных — предложу настроить оповещение.\n\n"
        "Доступные команды:\n"
        "/status - показать активные оповещения\n"
        "/history - показать историю цен\n"
        "/stat - текущая статистика рынка\n"
        "/cancel - отменить все оповещения\n"
        "/settings - настроить бонусы (Якорь и торговля)\n"
        "/help - подробная инструкция по использованию"
    )

# Команда /status
async def cmd_status(update: Update, context: CallbackContext):
    Alert = Query()
    alerts = alerts_table.search((Alert.user_id == update.effective_user.id) & (Alert.status == 'active'))

    if not alerts:
        await update.message.reply_text("📭 У вас нет активных оповещений.")
        return

    text = "📋 Ваши активные оповещения:\n\n"
    now = datetime.now()
    for alert in alerts:
        direction = "падение" if alert["direction"] == "down" else "рост"
        alert_time = datetime.fromisoformat(alert["alert_time"])
        remaining = alert_time - now
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

    await update.message.reply_text(text)

# Команда /history
async def cmd_history(update: Update, context: CallbackContext):
    try:
        args = context.args
        resource = args[0].capitalize() if len(args) > 0 else None
        hours = int(args[1]) if len(args) > 1 else 24

        if not resource or resource not in EMOJI_TO_RESOURCE.values():
            await update.message.reply_text(
                "Укажите ресурс для просмотра истории. Например:\n"
                "/history Дерево\n"
                "/history Камень 12  (за последние 12 часов)"
            )
            return

        user_id = update.effective_user.id
        MarketData = Query()
        cutoff_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
        records = market_table.search(
            (MarketData.resource == resource) & (MarketData.timestamp >= cutoff_time)
        )

        if not records:
            await update.message.reply_text(f"Нет данных по {resource} за последние {hours} часов.")
            return

        records.sort(key=lambda x: x['timestamp'])

        text = f"📊 История цен на {resource} за последние {hours} часов:\n\n"

        current_hour = None
        for record in records[-10:]:
            record_time = datetime.fromtimestamp(record['timestamp'])
            hour_str = record_time.strftime("%H:00")

            if hour_str != current_hour:
                text += f"\n🕐 {hour_str}:\n"
                current_hour = hour_str

            time_str = record_time.strftime("%H:%M")
            # Используем цены как есть (уже с бонусом)
            adj_buy = record['buy']
            adj_sell = record['sell']
            text += f"  {time_str} - Купить: {adj_buy:.2f}, Продать: {adj_sell:.2f}\n"

        recent_records = get_recent_data(resource, minutes=60)
        if recent_records and len(recent_records) >= 2:
            speed = calculate_speed(recent_records, "buy")
            adj_speed = speed  # Скорость не корректируем
            trend = get_trend(recent_records, "buy")
            trend_text = "растёт 📈" if trend == "up" else "падает 📉" if trend == "down" else "стабильна ➡️"
            speed_str = f"{adj_speed:+.4f}" if adj_speed is not None else "неизвестно"
            text += f"\nТренд: {trend_text} ({speed_str}/мин)"

        await update.message.reply_text(text)

    except ValueError:
        await update.message.reply_text("Некорректный формат команды. Используйте: /history <ресурс> [часы]")
    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /history: {e}")
        await update.message.reply_text("Произошла ошибка при получении истории цен.")

# Команда /cancel
async def cmd_cancel(update: Update, context: CallbackContext):
    Alert = Query()
    alerts = alerts_table.search((Alert.user_id == update.effective_user.id) & (Alert.status == 'active'))

    if not alerts:
        await update.message.reply_text("🗑️ Нет активных оповещений для отменя.")
        return

    for alert in alerts:
        alerts_table.update({'status': 'cancelled'}, doc_ids=[alert.doc_id])

    await update.message.reply_text(f"🗑️ Отменено {len(alerts)} оповещений.")

# Команда /settings
async def cmd_settings(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    user_states[user_id] = STATE_SETTINGS_ANCHOR
    user_data[user_id] = {}

    buttons = [
        [InlineKeyboardButton(text="✅ Да, есть Якорь", callback_data="anchor_yes")],
        [InlineKeyboardButton(text="❌ Нет", callback_data="anchor_no")]
    ]
    keyboard = InlineKeyboardMarkup(buttons)

    await update.message.reply_text("⚓️ Настройка бонусов: Есть ли у вас Якорь (выгода +2%)?", reply_markup=keyboard)

# Обработчик выбора якоря
async def process_anchor_selection(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if user_states.get(user_id) != STATE_SETTINGS_ANCHOR:
        return

    has_anchor = query.data == "anchor_yes"

    user_data[user_id]["has_anchor"] = has_anchor
    user_states[user_id] = STATE_SETTINGS_TRADE_LEVEL

    await context.bot.send_message(
        user_id,
        "⚖️ Теперь укажите уровень знания 'Основы торговли' (0-10, например: 3):"
    )

# Обработчик ввода уровня торговли
async def process_trade_level(update: Update, context: CallbackContext):
    message = update.message
    user_id = message.from_user.id
    
    if user_states.get(user_id) != STATE_SETTINGS_TRADE_LEVEL:
        return

    try:
        trade_level = int(message.text.strip())
        if trade_level < 0 or trade_level > 10:
            await message.reply_text("❌ Уровень должен быть от 0 до 10.")
            return
    except ValueError:
        await message.reply_text("❌ Пожалуйста, введите целое число (0-10).")
        return

    has_anchor = user_data[user_id]["has_anchor"]
    save_user_settings(user_id, has_anchor, trade_level)

    bonus = get_user_bonus(user_id)
    bonus_text = f"{bonus * 100:.0f}%" if bonus > 0 else "0%"

    await message.reply_text(
        f"✅ Настройки сохранены!\n"
        f"Якорь: {'✅' if has_anchor else '❌'}\n"
        f"Уровень торговли: {trade_level}\n"
        f"Общая выгода на цены: {bonus_text}"
    )

    user_states.pop(user_id, None)
    user_data.pop(user_id, None)

# Команда /help
async def cmd_help(update: Update, context: CallbackContext):
    help_text = (
        "📖 Полная инструкция по использованию бота\n\n"
        "1. Как начать:\n"
        "• Перешлите в чат (личный или групповой) любое сообщение с рынка, начинающееся с эмодзи 🎪.\n"
        "• Бот автоматически сохранит цены на ресурсы: Дерево, Камень, Провизия, Лошади.\n"
        "• Как только накопится достаточно данных (минимум 2 записи за 15 минут), бот предложит настроить оповещение с помощью кнопок (в личном и групповом чате, если применимо).\n\n"
        "2. Настройка оповещения:\n"
        "• Выберите ресурс из списка кнопок.\n"
        "• Укажите направление: рост 📈 или падение 📉 цены.\n"
        "• Введите целевую цену.\n"
        "• Бот рассчитает примерное время срабатывания и оповестит вас, когда цена достигнет цели!\n\n"
        "3. Доступные команды:\n"
        "• /start — приветственное сообщение и список команд.\n"
        "• /help — эта инструкция.\n"
        "• /status — показать все ваши активные оповещения и время до их срабатывания.\n"
        "• /history <ресурс> [часы] — показать историю цен. Пример: /history Дерево 6.\n"
        "• /stat — показать текущую статистику рынка и максимумы за неделю.\n"
        "• /timer <ресурс> <цена> — установить таймер на цену. Пример: /timer Дерево 8.50\n"
        "• /push — настроить напоминания об обновлении данных рынка.\n"
        "  - /push interval <минуты> — установить интервал напоминаний (5–120 минут)\n"
        "  - /push start — включить напоминания\n"
        "  - /push stop — отключить напоминания\n"
        "• /settings — настроить бонусы от Якоря и знания торговли.\n"
        "• /cancel — отменить все ваши активные оповещения.\n"
        "• /clear_group — удалить ссылки на групповые чаты из ваших таймеров.\n\n"
        "4. Важно:\n"
        "• Бот работает на основе вашей личной истории цен. Чем чаще вы присылаете данные рынка, тем точнее прогнозы.\n"
        "• Если цена резко изменила направление движения, оповещение может не сработать. Бот пришлет уведомление, если цель не будет достигнута в расчетное время.\n"
        "• Просроченные оповещения (которые не сработали вовремя) автоматически удаляются из списка активных через час.\n"
        "• В групповых чатах сообщения о таймерах закрепляются и открепляются по завершении. Кнопки выбора ресурса и направления доступны в группах, если бот имеет права на отправку сообщений."
    )
    await update.message.reply_text(help_text)

# Команда /stat — исправленная версия
async def cmd_stat(update: Update, context: CallbackContext):
    try:
        user_id = update.effective_user.id
        now = datetime.now()
        text = (
            f"<b>📊 Текущая статистика рынка</b>\n"
            f"🕗 Обновлено: {now.strftime('%d.%m.%Y %H:%M')}\n"
            f"{'─' * 22}\n\n"
        )

        resources = list(EMOJI_TO_RESOURCE.values())
        week_ago = int((now - timedelta(days=7)).timestamp())

        for resource in resources:
            emoji = RESOURCE_EMOJI.get(resource, "🔸")
            latest = get_latest_data(resource)
            if not latest:
                text += f"{emoji} <b>{resource}</b> — ❌ нет данных\n\n"
                continue

            # Используем цены как есть (уже с бонусом)
            last_buy = latest['buy']
            last_sell = latest['sell']
            last_timestamp = latest['timestamp']

            MarketData = Query()
            week_records = market_table.search(
                (MarketData.resource == resource) & (MarketData.timestamp >= week_ago)
            )

            if week_records:
                week_buy = [r['buy'] for r in week_records]
                week_sell = [r['sell'] for r in week_records]
                max_buy = max(week_buy)
                max_sell = max(week_sell)
                min_buy = min(week_buy)
                min_sell = min(week_sell)
                max_qty = max((r.get('quantity', 0) for r in week_records), default=0)
            else:
                max_buy = min_buy = last_buy
                max_sell = min_sell = last_sell
                max_qty = 0

            # Рассчитываем текущую цену на основе тренда
            recent = get_recent_data(resource, minutes=60)
            current_buy = last_buy
            current_sell = last_sell
            trend_desc = "неизвестен"
            trend_icon = "⏸️"

            if len(recent) >= 2:
                speed_buy = calculate_speed(recent, "buy")
                speed_sell = calculate_speed(recent, "sell")
                trend_buy = get_trend(recent, "buy")

                # Экстраполируем текущую цену
                elapsed_minutes = (now.timestamp() - last_timestamp) / 60.0
                if speed_buy is not None and elapsed_minutes > 0.1:
                    current_buy = last_buy + (speed_buy * elapsed_minutes)
                if speed_sell is not None and elapsed_minutes > 0.1:
                    current_sell = last_sell + (speed_sell * elapsed_minutes)

                # Описание тренда
                if trend_buy == "up":
                    trend_icon = "📈"
                    trend_desc = f"растёт ({speed_buy:+.4f}/мин)"
                elif trend_buy == "down":
                    trend_icon = "📉"
                    trend_desc = f"падает ({speed_buy:+.4f}/мин)"
                else:
                    trend_icon = "➖"
                    trend_desc = "стабилен"

            # Формат количества
            qty_str = f"{max_qty:,}".replace(",", " ") if max_qty > 0 else "не учтено"

            text += (
                f"{emoji} <b>{resource}</b>\n"
                f"├ 🕒 Последнее обновление: {datetime.fromtimestamp(last_timestamp).strftime('%H:%M')}\n"
                f"├ 💹 Покупка:   {current_buy:>7.3f} (было: {last_buy:.3f})\n"
                f"│   Диапазон за неделю: {min_buy:.3f} — {max_buy:.3f}\n"
                f"├ 💰 Продажа:  {current_sell:>7.3f} (было: {last_sell:.3f})\n"
                f"│   Диапазон за неделю: {min_sell:.3f} — {max_sell:.3f}\n"
                f"├ 📦 Макс. объём: {qty_str:>12} шт.\n"
                f"└ 📊 Тренд: {trend_icon} {trend_desc}\n\n"
            )

        text += f"{'─' * 22}\n"
        text += f"📈 — рост | 📉 — падение | ➖ — стабильно\n"
        text += f"Цены уже включают бонусы игрока."

        await update.message.reply_text(text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /stat: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при получении статистики.")

# Фоновая задача для обновления таймеров
async def update_dynamic_timers_once(context: CallbackContext):
    try:
        Alert = Query()
        active_alerts = alerts_table.search(Alert.status == 'active')

        for alert in active_alerts:
            user_id = alert['user_id']
            resource = alert['resource']
            direction = alert['direction']
            target_price = alert['target_price']
            chat_id = alert.get('chat_id')
            message_id = alert.get('message_id')
            last_checked = datetime.fromisoformat(alert['last_checked'])

            records = get_recent_data(resource, minutes=15)
            if not records or len(records) < 2:
                continue

            latest_data = get_latest_data(resource)
            if not latest_data:
                continue

            if latest_data['timestamp'] <= datetime.fromisoformat(alert['created_at']).timestamp():
                continue

            # Используем цену как есть (уже с бонусом)
            current_price = latest_data['buy']
            speed = calculate_speed(records, "buy")
            if speed is None:
                continue

            # Проверка изменения тренда
            current_trend = get_trend(records, "buy")
            username = (await context.bot.get_chat(user_id)).username or 'User'
            if (direction == "down" and current_trend == "up") or \
               (direction == "up" and current_trend == "down"):
                notification_text = (
                    f"⚠️ @{username} Внимание! Тренд для {resource} изменился!\n"
                    f"Вы ждете {'падение' if direction == 'down' else 'рост'} до {target_price:.2f}, "
                    f"но цена сейчас {'растет' if current_trend == 'up' else 'падает'}.\n"
                    f"Текущая цена: {current_price:.2f}\n"
                    f"Оповещение может не сработать."
                )

                await context.bot.send_message(user_id, notification_text)
                if chat_id and chat_id != user_id:
                    try:
                        await context.bot.send_message(chat_id, notification_text)
                        if message_id:
                            await context.bot.unpin_chat_message(chat_id, message_id)
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление или открепить сообщение в групповом чате {chat_id}: {e}")
                        await context.bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу или открепить сообщение.")

                alerts_table.update({'status': 'trend_changed'}, doc_ids=[alert.doc_id])
                continue

            # Проверка достижения цели
            if (direction == "down" and current_price <= target_price) or \
               (direction == "up" and current_price >= target_price):
                notification_text = (
                    f"🔔 @{username} {resource} достигла целевой цены!\n"
                    f"Цель: {target_price:.2f}\n"
                    f"Текущая цена: {current_price:.2f}\n\n"
                    f"Время {'покупать!' if direction == 'down' else 'продавать!'}"
                )
                await context.bot.send_message(user_id, notification_text)
                if chat_id and chat_id != user_id:
                    try:
                        await context.bot.send_message(chat_id, notification_text)
                        if message_id:
                            await context.bot.unpin_chat_message(chat_id, message_id)
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление или открепить сообщение в групповом чате {chat_id}: {e}")
                        await context.bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу или открепить сообщение.")
                alerts_table.update({'status': 'completed'}, doc_ids=[alert.doc_id])
                continue

            # Пересчет времени
            price_diff = target_price - current_price
            if (direction == "down" and speed >= 0) or (direction == "up" and speed <= 0):
                continue

            time_minutes = abs(price_diff) / abs(speed)
            new_alert_time = datetime.now() + timedelta(minutes=time_minutes)

            alerts_table.update({
                'alert_time': new_alert_time.isoformat(),
                'speed': speed,
                'current_price': current_price,
                'last_checked': datetime.now().isoformat()
            }, doc_ids=[alert.doc_id])

            old_alert_time = datetime.fromisoformat(alert['alert_time'])
            time_diff_minutes = abs((new_alert_time - old_alert_time).total_seconds() / 60.0)
            if time_diff_minutes > 5:
                notification_text = (
                    f"🔄 @{username} Таймер для {resource} обновлен!\n"
                    f"Цель: {target_price:.2f}\n"
                    f"Текущая цена: {current_price:.2f}\n"
                    f"Новая скорость: {speed:+.4f} в минуту\n"
                    f"Новое время: {new_alert_time.strftime('%H:%M:%S')} (~{int(time_minutes)} мин.)"
                )
                await context.bot.send_message(user_id, notification_text)
                if chat_id and chat_id != user_id:
                    try:
                        await context.bot.send_message(chat_id, notification_text)
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление в групповой чат {chat_id}: {e}")
                        await context.bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу.")

    except Exception as e:
        logger.error(f"Ошибка при обновлении таймеров: {e}")

# Фоновая задача для очистки просроченных алертов
async def cleanup_expired_alerts(context: CallbackContext):
    while True:
        try:
            Alert = Query()
            now = datetime.now()
            cutoff_time = (now - timedelta(hours=1)).isoformat()
            expired_alerts = alerts_table.search(
                (Alert.status == 'active') &
                (Alert.alert_time < cutoff_time)
            )

            if expired_alerts:
                expired_ids = [alert.doc_id for alert in expired_alerts]
                alerts_table.update({'status': 'cleanup_expired'}, doc_ids=expired_ids)
                logger.info(f"Очистка: деактивировано {len(expired_ids)} просроченных алертов.")

        except Exception as e:
            logger.error(f"Ошибка при выполнении очистки просроченных алертов: {e}")

        await asyncio.sleep(600)

# Основная функция
def main():
    application = Application.builder().token(BOT_TOKEN).build()

    # Регистрация обработчиков
    application.add_handler(MessageHandler(filters.TEXT & filters.FORWARD & filters.Regex(r"🎪 Рынок"), handle_market_forward))
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("history", cmd_history))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_handler(CommandHandler("settings", cmd_settings))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("stat", cmd_stat))
    
    application.add_handler(CallbackQueryHandler(process_resource_selection, pattern=r"^resource_"))
    application.add_handler(CallbackQueryHandler(cancel_action, pattern=r"^cancel_action"))
    application.add_handler(CallbackQueryHandler(process_direction_selection, pattern=r"^direction_"))
    application.add_handler(CallbackQueryHandler(process_anchor_selection, pattern=r"^anchor_"))
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_target_price))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_trade_level))

    # Запуск бота
    logger.info("🚀 Бот запущен и готов к работе!")
    application.run_polling()

if __name__ == '__main__':
    main()