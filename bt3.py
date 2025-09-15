import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

import telebot
from telebot import types
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup
from tinydb import TinyDB, Query
from dotenv import load_dotenv
import os
import threading
import time

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

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)

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

# Получение настроек пользователя
def get_user_settings(user_id: int) -> Dict[str, Union[bool, int]]:
    Settings = Query()
    setting = settings_table.get(Settings.user_id == user_id)
    if setting:
        return {
            "has_anchor": setting.get("has_anchor", False),
            "trade_level": setting.get("trade_level", 0)
        }
    return {"has_anchor": False, "trade_level": 0}

# Сохранение настроек пользователя
def save_user_settings(user_id: int, has_anchor: bool, trade_level: int):
    settings_table.upsert({
        "user_id": user_id,
        "has_anchor": has_anchor,
        "trade_level": trade_level
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
    adjusted_buy = buy / (1 + bonus)  # Покупка дешевле
    adjusted_sell = sell * (1 + bonus)  # Продажа дороже
    return adjusted_buy, adjusted_sell

# Парсинг сообщения рынка — ОБНОВЛЁННАЯ ВЕРСИЯ
def parse_market_message(text: str) -> Optional[Dict[str, Dict[str, Union[float, int]]]]:
    """
    Парсит сообщение рынка. Пример:
        Дерево: 96,342,449🪵
        📉Купить/продать: 8.31/6.80💰
    Возвращает словарь с buy, sell и quantity.
    """
    lines = [line.strip() for line in text.strip().split('\n') if line.strip()]
    resources = {}
    current_resource = None
    current_quantity = 0

    # Паттерн для строки ресурса: "Название: число Эмодзи"
    resource_pattern = r"^(.+?):\s*([0-9,]*)\s*([🪵🪨🍞🐴])$"
    # Паттерн для цен: "📈Купить/продать: 8.31/6.80💰"
    price_pattern = r"(?:[📈📉]?\s*)?Купить/продать:\s*([0-9.]+)\s*/\s*([0-9.]+)\s*💰"

    for i, line in enumerate(lines):
        if line == "🎪 Рынок":
            continue

        # Проверка на строку ресурса
        res_match = re.match(resource_pattern, line)
        if res_match:
            name_part = res_match.group(1).strip()
            qty_str = res_match.group(2).replace(',', '').strip()
            emoji = res_match.group(3)

            current_resource = EMOJI_TO_RESOURCE.get(emoji, name_part)
            current_quantity = int(qty_str) if qty_str.isdigit() else 0
            continue

        # Проверка на строку цен
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


# Получение данных за последние N минут (базовые цены)
def get_recent_data(resource: str, minutes: int = 15) -> List[Dict]:
    MarketData = Query()
    cutoff_time = int((datetime.now() - timedelta(minutes=minutes)).timestamp())
    records = market_table.search(
        (MarketData.resource == resource) & (MarketData.timestamp >= cutoff_time)
    )
    records.sort(key=lambda x: x['timestamp'])
    return records


# Получение последней записи для ресурса (базовые цены)
def get_latest_data(resource: str) -> Optional[Dict]:
    MarketData = Query()
    records = market_table.search(MarketData.resource == resource)
    if not records:
        return None
    records.sort(key=lambda x: x['timestamp'], reverse=True)
    return records[0]


# Расчет скорости изменения цены (на базовых ценах)
def calculate_speed(records: List[Dict], price_type: str = "buy") -> Optional[float]:
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


# Проверка тренда (на базовых ценах)
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


# Обработчик форварда с рынком
# Обработчик форварда с рынком
@bot.message_handler(func=lambda message: message.text and ("🎪 Рынок" in message.text))
def handle_market_forward(message):
    try:
        # Проверка на forward от бота
        if not message.forward_from:
            bot.reply_to(message, "❌ Сообщение должно быть пересылкой от бота рынка.")
            return

        # Логируем информацию о пересылке для отладки
        logger.info(f"Forwarded from: {message.forward_from.username} (ID: {message.forward_from.id}, is_bot: {message.forward_from.is_bot})")

        # Проверка времени: не старше часа
        current_time = time.time()
        if current_time - message.date > 3600:
            bot.reply_to(message, "❌ Сообщение слишком старое (более часа). Используйте свежие обновления.")
            return

        logger.info("Обработка сообщения рынка...")
        data = parse_market_message(message.text)
        if not data:
            bot.reply_to(message, "❌ Не удалось распознать данные рынка. Проверьте формат сообщения.")
            return

        timestamp = int(message.date)
        saved_count = 0

        for resource, prices in data.items():
            MarketData = Query()
            # Проверка на дубликат: совпадение ресурса, времени и цен
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
                    bot.reply_to(message, f"❌ Ошибка при сохранении данных для {resource}.")
                    continue

        if saved_count > 0:
            bot.reply_to(message, f"✅ Сохранено {saved_count} записей рынка.")
            
            # Проверяем, есть ли хотя бы у одного ресурса >=2 записей за 15 минут
            for resource in EMOJI_TO_RESOURCE.values():
                if len(get_recent_data(resource, 15)) >= 2:
                    send_resource_selection(message.from_user.id)
                    break
        else:
            bot.reply_to(message, "ℹ️ Данные уже были сохранены ранее.")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения рынка: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Произошла ошибка: {str(e)}. Пожалуйста, попробуйте снова или свяжитесь с поддержкой.")


# Отправка выбора ресурса
def send_resource_selection(user_id: int):
    buttons = [
        [InlineKeyboardButton(text=res, callback_data=f"resource_{res}")]
        for res in EMOJI_TO_RESOURCE.values()
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    bot.send_message(user_id, "📊 Выберите ресурс для отслеживания:", reply_markup=keyboard)


# Обработчик выбора ресурса
@bot.callback_query_handler(func=lambda call: call.data.startswith('resource_'))
def process_resource_selection(call):
    bot.answer_callback_query(call.id)
    resource = call.data.split('_', 1)[1]

    records = get_recent_data(resource, 15)
    if len(records) < 2:
        bot.send_message(call.from_user.id, 
                        f"⚠️ Для {resource} недостаточно данных. Пришлите еще обновления рынка.")
        return

    # Сохраняем состояние и данные
    user_states[call.from_user.id] = STATE_CHOOSING_DIRECTION
    user_data[call.from_user.id] = {"resource": resource}

    speed = calculate_speed(records, "buy")
    trend = get_trend(records, "buy")
    current_price = records[-1]["buy"]
    
    # Корректировка цены для пользователя
    user_id = call.from_user.id
    bonus = get_user_bonus(user_id)
    adjusted_buy, _ = adjust_prices_for_user(user_id, current_price, records[-1]["sell"])
    adj_speed = speed / (1 + bonus) if speed is not None else None
    
    trend_emoji = "📈" if trend == "up" else "📉" if trend == "down" else "➡️"
    trend_text = "растёт" if trend == "up" else "падает" if trend == "down" else "стабильна"
    
    buttons = [
        [InlineKeyboardButton(text="📉 Падение цены", callback_data="direction_down")],
        [InlineKeyboardButton(text="📈 Рост цены", callback_data="direction_up")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    
    speed_text = f"{adj_speed:+.4f}" if adj_speed is not None else "неизвестно"
    bot.send_message(
        call.from_user.id, 
        f"{trend_emoji} Вы выбрали {resource}. Текущая цена: {adjusted_buy:.2f}\n"
        f"Тренд: {trend_text} ({speed_text} в минуту)\n\n"
        f"Что вас интересует?", 
        reply_markup=keyboard
    )


# Обработчик отмены действий
@bot.callback_query_handler(func=lambda call: call.data == 'cancel_action')
def cancel_action(call):
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    user_states.pop(user_id, None)
    user_data.pop(user_id, None)
    bot.send_message(user_id, "❌ Действие отменено.")


# Обработчик выбора направления
@bot.callback_query_handler(func=lambda call: call.data.startswith('direction_') and user_states.get(call.from_user.id) == STATE_CHOOSING_DIRECTION)
def process_direction_selection(call):
    bot.answer_callback_query(call.id)
    direction = "down" if call.data == "direction_down" else "up"
    
    user_id = call.from_user.id
    resource = user_data[user_id]["resource"]
    
    records = get_recent_data(resource, 15)
    current_price = records[-1]["buy"]
    trend = get_trend(records, "buy")
    
    # Корректировка для пользователя
    adjusted_buy, _ = adjust_prices_for_user(user_id, current_price, records[-1]["sell"])
    
    if (direction == "down" and trend != "down") or (direction == "up" and trend != "up"):
        trend_text = "падает" if trend == "down" else "растёт" if trend == "up" else "стабильна"
        bot.send_message(
            user_id,
            f"⚠️ Внимание! Цена {resource} сейчас {trend_text}, а вы выбрали "
            f"{'падение' if direction == 'down' else 'рост'}. Уверены, что хотите продолжить?"
        )
    
    user_data[user_id]["direction"] = direction
    user_states[user_id] = STATE_ENTERING_TARGET_PRICE
    
    bot.send_message(
        user_id, 
        f"💰 Введите целевую цену для {resource} (например: {adjusted_buy * 0.9:.2f}):"
    )


# Обработчик ввода целевой цены
@bot.message_handler(func=lambda message: user_states.get(message.from_user.id) == STATE_ENTERING_TARGET_PRICE)
def process_target_price(message):
    user_id = message.from_user.id
    try:
        target_price = float(message.text.strip().replace(',', '.'))
        if target_price <= 0:
            bot.reply_to(message, "❌ Цена должна быть положительным числом.")
            return
    except ValueError:
        bot.reply_to(message, "❌ Пожалуйста, введите корректное число (например: 0.55).")
        return

    resource = user_data[user_id]["resource"]
    direction = user_data[user_id]["direction"]

    records = get_recent_data(resource, 15)
    if len(records) < 2:
        bot.reply_to(message, "⚠️ Недостаточно данных для расчета скорости. Пришлите еще обновления рынка.")
        user_states.pop(user_id, None)
        user_data.pop(user_id, None)
        return

    speed = calculate_speed(records, "buy")
    if speed is None:
        bot.reply_to(message, "⚠️ Не удалось рассчитать скорость изменения цены.")
        user_states.pop(user_id, None)
        user_data.pop(user_id, None)
        return

    bonus = get_user_bonus(user_id)
    adj_speed = speed / (1 + bonus)

    current_price = records[-1]["buy"]
    # Корректировка для пользователя
    adjusted_buy, _ = adjust_prices_for_user(user_id, current_price, records[-1]["sell"])
    price_diff = target_price - adjusted_buy

    if (direction == "down" and target_price >= adjusted_buy) or \
       (direction == "up" and target_price <= adjusted_buy):
        bot.reply_to(message, f"⚠️ При {('падении' if direction == 'down' else 'росте')} целевая цена должна быть {('ниже' if direction == 'down' else 'выше')} текущей ({adjusted_buy:.2f}).")
        return

    trend = get_trend(records, "buy")
    if (direction == "down" and trend == "up") or (direction == "up" and trend == "down"):
        bot.reply_to(message,
            "⚠️ Внимание! Выбранное направление противоречит текущему тренду. "
            "Оповещение может никогда не сработать.")

    if (direction == "down" and adj_speed >= 0) or (direction == "up" and adj_speed <= 0):
        bot.reply_to(message, "⚠️ Цена движется не в ту сторону, чтобы достичь вашей цели. Оповещение не будет установлено.")
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
        "current_price": adjusted_buy,  # Сохраняем скорректированную
        "alert_time": alert_time.isoformat(),
        "created_at": datetime.now().isoformat(),
        "status": "active"
    })

    alert_time_str = alert_time.strftime("%H:%M:%S")
    
    bot.reply_to(
        message,
        f"✅ Таймер установлен!\n"
        f"Ресурс: {resource}\n"
        f"Текущая цена: {adjusted_buy:.2f}\n"
        f"Цель: {target_price:.2f} ({'падение' if direction == 'down' else 'рост'})\n"
        f"Скорость: {adj_speed:+.4f} в минуту\n"
        f"Осталось: ~{int(time_minutes)} мин.\n"
        f"Ожидаемое время: {alert_time_str}\n\n"
        f"Бот оповестит вас, когда цена достигнет цели."
    )

    user_states.pop(user_id, None)
    user_data.pop(user_id, None)

    # Запуск фоновой задачи
    threading.Thread(target=schedule_alert, args=(alert_id, user_id, resource, target_price, alert_time), daemon=True).start()


# Фоновая задача для отправки уведомления
# Фоновая задача для отправки уведомления
def schedule_alert(alert_id: int, user_id: int, resource: str, target_price: float, alert_time: datetime, chat_id: Optional[int] = None):
    now = datetime.now()
    sleep_seconds = (alert_time - now).total_seconds()

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    Alert = Query()
    alert = alerts_table.get(doc_id=alert_id)
    if not alert or alert.get('status') != 'active':
        return

    try:
        latest_data = get_latest_data(resource)
        if not latest_data:
            raise ValueError(f"Нет последних данных для ресурса: {resource}")

        # Корректировка текущей цены для пользователя
        current_price, _ = adjust_prices_for_user(user_id, latest_data['buy'], latest_data['sell'])
        direction = alert['direction']

        is_target_reached = False
        if direction == "down" and current_price <= target_price:
            is_target_reached = True
        elif direction == "up" and current_price >= target_price:
            is_target_reached = True

        notification_text = (
            f"🔔 {resource} достигла целевой цены!\n"
            f"Цель: {target_price:.2f}\n"
            f"Текущая цена: {current_price:.2f}\n\n"
            f"Время {'покупать!' if direction == 'down' else 'продавать!'}"
        ) if is_target_reached else (
            f"⏰ Таймер для {resource} сработал, но цель ({target_price:.2f}) "
            f"еще не достигнута (текущая цена: {current_price:.2f}).\n"
            f"Скорость рынка, вероятно, изменилась."
        )

        # Отправка уведомления пользователю
        bot.send_message(user_id, notification_text)
        
        # Отправка в групповой чат, если указан chat_id
        if chat_id and chat_id != user_id:
            try:
                bot.send_message(chat_id, f"@{bot.get_chat_member(user_id, user_id).user.username or 'User'} {notification_text}")
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление в групповой чат {chat_id}: {e}")

        alerts_table.update({'status': 'completed' if is_target_reached else 'expired'}, doc_ids=[alert_id])

    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
        alerts_table.update({'status': 'error'}, doc_ids=[alert_id])


# Фоновая задача для очистки просроченных алертов
def cleanup_expired_alerts():
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

        time.sleep(600)  # 10 минут


# Команда /start
@bot.message_handler(commands=['start'])
def cmd_start(message):
    user_id = message.from_user.id
    user_states.pop(user_id, None)
    user_data.pop(user_id, None)
    bot.reply_to(
        message,
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
@bot.message_handler(commands=['status'])
def cmd_status(message):
    Alert = Query()
    alerts = alerts_table.search((Alert.user_id == message.from_user.id) & (Alert.status == 'active'))

    if not alerts:
        bot.reply_to(message, "📭 У вас нет активных оповещений.")
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

    bot.reply_to(message, text)


# Команда /history
@bot.message_handler(commands=['history'])
def cmd_history(message):
    try:
        args = message.text.split()[1:] if len(message.text.split()) > 1 else []
        resource = args[0] if len(args) > 0 else None
        hours = int(args[1]) if len(args) > 1 else 24
        
        if not resource:
            bot.reply_to(message,
                "Укажите ресурс для просмотра истории. Например:\n"
                "/history Дерево\n"
                "/history Камень 12  (за последние 12 часов)"
            )
            return
        
        user_id = message.from_user.id
        bonus = get_user_bonus(user_id)
        MarketData = Query()
        cutoff_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
        records = market_table.search(
            (MarketData.resource == resource) & (MarketData.timestamp >= cutoff_time)
        )
        
        if not records:
            bot.reply_to(message, f"Нет данных по {resource} за последние {hours} часов.")
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
            adj_buy, adj_sell = adjust_prices_for_user(user_id, record['buy'], record['sell'])
            text += f"  {time_str} - Купить: {adj_buy:.2f}, Продать: {adj_sell:.2f}\n"
        
        recent_records = get_recent_data(resource, minutes=60)
        if recent_records and len(recent_records) >= 2:
            speed = calculate_speed(recent_records, "buy")
            adj_speed = speed / (1 + bonus) if speed is not None else None
            trend = get_trend(recent_records, "buy")
            trend_text = "растёт 📈" if trend == "up" else "падает 📉" if trend == "down" else "стабильна ➡️"
            speed_str = f"{adj_speed:+.4f}" if adj_speed is not None else "неизвестно"
            text += f"\nТренд: {trend_text} ({speed_str}/мин)"
        
        bot.reply_to(message, text)
        
    except ValueError:
        bot.reply_to(message, "Некорректный формат команды. Используйте: /history <ресурс> [часы]")
    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /history: {e}")
        bot.reply_to(message, "Произошла ошибка при получении истории цен.")


# Команда /cancel
@bot.message_handler(commands=['cancel'])
def cmd_cancel(message):
    Alert = Query()
    alerts = alerts_table.search((Alert.user_id == message.from_user.id) & (Alert.status == 'active'))
    
    if not alerts:
        bot.reply_to(message, "🗑️ Нет активных оповещений для отмены.")
        return
        
    for alert in alerts:
        alerts_table.update({'status': 'cancelled'}, doc_ids=[alert.doc_id])
    
    bot.reply_to(message, f"🗑️ Отменено {len(alerts)} оповещений.")


# Команда /settings
@bot.message_handler(commands=['settings'])
def cmd_settings(message):
    user_id = message.from_user.id
    user_states[user_id] = STATE_SETTINGS_ANCHOR
    user_data[user_id] = {}
    
    buttons = [
        [InlineKeyboardButton(text="✅ Да, есть Якорь", callback_data="anchor_yes")],
        [InlineKeyboardButton(text="❌ Нет", callback_data="anchor_no")]
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    
    bot.reply_to(message, "⚓️ Настройка бонусов: Есть ли у вас Якорь (выгода +2%)?", reply_markup=keyboard)


# Обработчик выбора якоря
@bot.callback_query_handler(func=lambda call: call.data.startswith('anchor_') and user_states.get(call.from_user.id) == STATE_SETTINGS_ANCHOR)
def process_anchor_selection(call):
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    has_anchor = call.data == "anchor_yes"
    
    user_data[user_id]["has_anchor"] = has_anchor
    user_states[user_id] = STATE_SETTINGS_TRADE_LEVEL
    
    bot.send_message(
        user_id,
        "⚖️ Теперь укажите уровень знания 'Основы торговли' (0-10, например: 3):"
    )


# Обработчик ввода уровня торговли
@bot.message_handler(func=lambda message: user_states.get(message.from_user.id) == STATE_SETTINGS_TRADE_LEVEL)
def process_trade_level(message):
    user_id = message.from_user.id
    try:
        trade_level = int(message.text.strip())
        if trade_level < 0 or trade_level > 10:
            bot.reply_to(message, "❌ Уровень должен быть от 0 до 10.")
            return
    except ValueError:
        bot.reply_to(message, "❌ Пожалуйста, введите целое число (0-10).")
        return

    has_anchor = user_data[user_id]["has_anchor"]
    save_user_settings(user_id, has_anchor, trade_level)
    
    bonus = get_user_bonus(user_id)
    bonus_text = f"{bonus * 100:.0f}%" if bonus > 0 else "0%"
    
    bot.reply_to(
        message,
        f"✅ Настройки сохранены!\n"
        f"Якорь: {'✅' if has_anchor else '❌'}\n"
        f"Уровень торговли: {trade_level}\n"
        f"Общая выгода на цены: {bonus_text}"
    )
    
    user_states.pop(user_id, None)
    user_data.pop(user_id, None)


# Команда /help
@bot.message_handler(commands=['help'])
def cmd_help(message):
    help_text = (
        "📖 Полная инструкция по использованию бота\n\n"
        "1. Как начать:\n"
        "• Перешлите в чат любое сообщение с рынка, начинающееся с эмодзи 🎪.\n"
        "• Бот автоматически сохранит цены на ресурсы: Дерево, Камень, Провизия, Лошади.\n"
        "• Как только накопится достаточно данных (минимум 2 записи за 15 минут), бот предложит настроить оповещение.\n\n"
        "2. Настройка оповещения:\n"
        "• Выберите ресурс из списка.\n"
        "• Укажите направление: рост 📈 или падение 📉 цены.\n"
        "• Введите целевую цену.\n"
        "• Бот рассчитает примерное время срабатывания и оповестит вас, когда цена достигнет цели!\n\n"
        "3. Доступные команды:\n"
        "• /start — приветственное сообщение и список команд.\n"
        "• /help — эта инструкция.\n"
        "• /status — показать все ваши активные оповещения и время до их срабатывания.\n"
        "• /history <ресурс> [часы] — показать историю цен. Пример: /history Дерево 6.\n"
        "• /stat — показать текущую статистику рынка и максимумы за неделю.\n"
        "• /settings — настроить бонусы от Якоря и знания торговли.\n"
        "• /cancel — отменить все ваши активные оповещения.\n\n"
        "4. Важно:\n"
        "• Бот работает на основе вашей личной истории цен. Чем чаще вы присылаете данные рынка, тем точнее прогнозы.\n"
        "• Если цена резко изменила направление движения, оповещение может не сработать. Бот пришлет уведомление, если цель не будет достигнута в расчетное время.\n"
        "• Просроченные оповещения (которые не сработали вовремя) автоматически удаляются из списка активных через час."
    )
    bot.reply_to(message, help_text)


# Расчет скорости изменения цены (на базовых ценах)
def calculate_speed(records: List[Dict], price_type: str = "buy") -> Optional[float]:
    if len(records) < 2:
        return None

    first = records[0]
    last = records[-1]

    price_delta = last[price_type] - first[price_type]
    time_delta_minutes = (last['timestamp'] - first['timestamp']) / 60.0

    if time_delta_minutes < 0.1:  # Минимальный интервал для расчета скорости
        return None

    speed = price_delta / time_delta_minutes
    return round(speed, 4)


# Команда /stat — с прогнозом текущей цены и улучшенным дизайном
@bot.message_handler(commands=['stat'])
def cmd_stat(message):
    try:
        user_id = message.from_user.id
        bonus = get_user_bonus(user_id)
        now = datetime.now()
        text = (
            f"<b>📊 Текущая статистика рынка</b>\n"
            f"🕗 Обновлено: {now.strftime('%d.%m.%Y %H:%M')}\n"
            f"💎 Бонус игрока: {bonus * 100:.0f}%\n"
            f"{'─' * 22}\n\n"
        )

        resources = list(EMOJI_TO_RESOURCE.values())
        week_ago = int((now - timedelta(days=7)).timestamp())

        RESOURCE_EMOJI = {
            "Дерево": "🪵",
            "Камень": "🪨",
            "Провизия": "🍞",
            "Лошади": "🐴"
        }

        for resource in resources:
            emoji = RESOURCE_EMOJI.get(resource, "🔸")
            latest = get_latest_data(resource)
            if not latest:
                text += f"{emoji} <b>{resource}</b> — ❌ нет данных\n\n"
                continue

            # Последние сохранённые цены (базовые)
            last_buy = latest['buy']
            last_sell = latest['sell']
            last_timestamp = latest['timestamp']

            # Корректировка для пользователя
            adj_buy, adj_sell = adjust_prices_for_user(user_id, last_buy, last_sell)

            MarketData = Query()
            week_records = market_table.search(
                (MarketData.resource == resource) & (MarketData.timestamp >= week_ago)
            )

            if week_records:
                week_adj_buy = [adjust_prices_for_user(user_id, r['buy'], r['sell'])[0] for r in week_records]
                week_adj_sell = [adjust_prices_for_user(user_id, r['buy'], r['sell'])[1] for r in week_records]
                max_buy = max(week_adj_buy)
                max_sell = max(week_adj_sell)
                min_buy = min(week_adj_buy)
                min_sell = min(week_adj_sell)
                max_qty = max((r.get('quantity', 0) for r in week_records), default=0)
            else:
                max_buy = min_buy = adj_buy
                max_sell = min_sell = adj_sell
                max_qty = 0

            # 📈 Рассчитываем ТЕКУЩУЮ цену на основе тренда за последние 60 минут
            recent = get_recent_data(resource, minutes=60)
            current_buy = adj_buy  # по умолчанию
            current_sell = adj_sell
            trend_desc = "неизвестен"
            trend_icon = "⏸️"

            if len(recent) >= 2:
                speed_buy = calculate_speed(recent, "buy")
                speed_sell = calculate_speed(recent, "sell")
                trend_buy = get_trend(recent, "buy")
                adj_speed_buy = speed_buy / (1 + bonus) if speed_buy is not None else None
                adj_speed_sell = speed_sell * (1 + bonus) if speed_sell is not None else None

                # Экстраполируем текущую цену (скорректированную)
                elapsed_minutes = (now.timestamp() - last_timestamp) / 60.0
                if speed_buy is not None and elapsed_minutes > 0.1:
                    current_buy = adj_buy + (adj_speed_buy * elapsed_minutes)
                if speed_sell is not None and elapsed_minutes > 0.1:
                    current_sell = adj_sell + (adj_speed_sell * elapsed_minutes)

                # Описание тренда
                if trend_buy == "up":
                    trend_icon = "📈"
                    trend_desc = f"растёт ({adj_speed_buy:+.4f}/мин)"
                elif trend_buy == "down":
                    trend_icon = "📉"
                    trend_desc = f"падает ({adj_speed_buy:+.4f}/мин)"
                else:
                    trend_icon = "➖"
                    trend_desc = "стабилен"

            # Формат количества
            qty_str = f"{max_qty:,}".replace(",", " ") if max_qty > 0 else "не учтено"

            # 🖋️ Форматируем красиво и информативно
            text += (
                f"{emoji} <b>{resource}</b>\n"
                f"├ 🕒 Последнее обновление: {datetime.fromtimestamp(last_timestamp).strftime('%H:%M')}\n"
                f"├ 💹 Покупка:   {current_buy:>7.3f} (было: {adj_buy:.3f})\n"
                f"│   Диапазон за неделю: {min_buy:.3f} — {max_buy:.3f}\n"
                f"├ 💰 Продажа:  {current_sell:>7.3f} (было: {adj_sell:.3f})\n"
                f"│   Диапазон за неделю: {min_sell:.3f} — {max_sell:.3f}\n"
                f"├ 📦 Макс. объём: {qty_str:>12} шт.\n"
                f"└ 📊 Тренд: {trend_icon} {trend_desc}\n\n"
            )

        # Подвал
        text += f"{'─' * 22}\n"
        text += f"📈 — рост | 📉 — падение | ➖ — стабильно\n"
        text += f"Цены скорректированы с учетом бонусов игрока ({bonus * 100:.0f}%)."

        bot.reply_to(message, text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /stat: {e}", exc_info=True)
        bot.reply_to(message, "❌ Произошла ошибка при получении статистики.")

# Команда /timer [Вид Ресурса] [цель цены]
@bot.message_handler(commands=['timer'])
def cmd_timer(message):
    try:
        args = message.text.split()[1:] if len(message.text.split()) > 1 else []
        if len(args) != 2:
            bot.reply_to(message, "❌ Формат команды: /timer <ресурс> <целевая цена>\nПример: /timer Дерево 8.50")
            return

        resource = args[0].capitalize()
        if resource not in EMOJI_TO_RESOURCE.values():
            bot.reply_to(message, f"❌ Неверный ресурс. Доступны: {', '.join(EMOJI_TO_RESOURCE.values())}")
            return

        try:
            target_price = float(args[1].replace(',', '.'))
            if target_price <= 0:
                bot.reply_to(message, "❌ Цена должна быть положительным числом.")
                return
        except ValueError:
            bot.reply_to(message, "❌ Пожалуйста, введите корректное число для цены (например: 8.50).")
            return

        user_id = message.from_user.id
        bonus = get_user_bonus(user_id)
        records = get_recent_data(resource, minutes=15)
        if len(records) < 2:
            bot.reply_to(message, f"⚠️ Недостаточно данных для {resource}. Пришлите еще обновления рынка.")
            return

        latest = get_latest_data(resource)
        current_price = latest["buy"]
        adjusted_buy, _ = adjust_prices_for_user(user_id, current_price, latest["sell"])
        
        # Определяем направление
        direction = "down" if target_price < adjusted_buy else "up"
        
        speed = calculate_speed(records, "buy")
        if speed is None:
            bot.reply_to(message, "⚠️ Не удалось рассчитать скорость изменения цены.")
            return

        adj_speed = speed / (1 + bonus) if direction == "down" else speed / (1 + bonus)
        
        if (direction == "down" and target_price >= adjusted_buy) or \
           (direction == "up" and target_price <= adjusted_buy):
            bot.reply_to(message, f"⚠️ При {('падении' if direction == 'down' else 'росте')} целевая цена должна быть {('ниже' if direction == 'down' else 'выше')} текущей ({adjusted_buy:.2f}).")
            return

        trend = get_trend(records, "buy")
        if (direction == "down" and trend == "up") or (direction == "up" and trend == "down"):
            bot.reply_to(message,
                "⚠️ Внимание! Выбранное направление противоречит текущему тренду. "
                "Оповещение может никогда не сработать.")

        if (direction == "down" and adj_speed >= 0) or (direction == "up" and adj_speed <= 0):
            bot.reply_to(message, "⚠️ Цена движется не в ту сторону, чтобы достичь вашей цели. Оповещение не будет установлено.")
            return

        price_diff = target_price - adjusted_buy
        time_minutes = abs(price_diff) / abs(adj_speed)
        alert_time = datetime.now() + timedelta(minutes=time_minutes)
        
        # Сохраняем chat_id для группового чата, если команда отправлена из группы
        chat_id = message.chat.id if message.chat.type in ['group', 'supergroup'] else None
        
        # Проверяем доступность группового чата
        if chat_id and chat_id != user_id:
            try:
                bot.get_chat(chat_id)  # Проверка, что бот имеет доступ к чату
            except Exception as e:
                logger.error(f"Не удалось проверить доступ к групповому чату {chat_id}: {e}")
                bot.reply_to(message, "⚠️ Не удалось подтвердить доступ к групповому чату. Уведомления будут отправлены только в личный чат.")
                chat_id = None
        
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
            "last_checked": datetime.now().isoformat()  # Для отслеживания последнего пересчета
        })

        alert_time_str = alert_time.strftime("%H:%M:%S")
        
        bot.reply_to(
            message,
            f"✅ Таймер установлен!\n"
            f"Ресурс: {resource}\n"
            f"Текущая цена: {adjusted_buy:.2f}\n"
            f"Цель: {target_price:.2f} ({'падение' if direction == 'down' else 'рост'})\n"
            f"Скорость: {adj_speed:+.4f} в минуту\n"
            f"Осталось: ~{int(time_minutes)} мин.\n"
            f"Ожидаемое время: {alert_time_str}\n\n"
            f"Бот оповестит вас, когда цена достигнет цели."
        )

    except Exception as e:
        logger.error(f"Ошибка при установке таймера: {e}", exc_info=True)
        bot.reply_to(message, "❌ Произошла ошибка при установке таймера.")

# Фоновая задача для проверки изменения тренда
def check_trend_changes():
    while True:
        try:
            Alert = Query()
            active_alerts = alerts_table.search(Alert.status == 'active')
            
            for alert in active_alerts:
                user_id = alert['user_id']
                resource = alert['resource']
                direction = alert['direction']
                target_price = alert['target_price']
                chat_id = alert.get('chat_id')

                records = get_recent_data(resource, minutes=15)
                if len(records) < 2:
                    continue

                current_trend = get_trend(records, "buy")
                if (direction == "down" and current_trend == "up") or \
                   (direction == "up" and current_trend == "down"):
                    latest_data = get_latest_data(resource)
                    current_price, _ = adjust_prices_for_user(user_id, latest_data['buy'], latest_data['sell'])
                    
                    notification_text = (
                        f"⚠️ Внимание! Тренд для {resource} изменился!\n"
                        f"Вы ждете {'падение' if direction == 'down' else 'рост'} до {target_price:.2f}, "
                        f"но цена сейчас {'растет' if current_trend == 'up' else 'падает'}.\n"
                        f"Текущая цена: {current_price:.2f}\n"
                        f"Оповещение может не сработать."
                    )
                    
                    # Отправка уведомления пользователю
                    bot.send_message(user_id, notification_text)
                    
                    # Отправка в групповой чат, если указан
                    if chat_id and chat_id != user_id:
                        try:
                            bot.send_message(
                                chat_id,
                                f"@{bot.get_chat_member(user_id, user_id).user.username or 'User'} {notification_text}"
                            )
                        except Exception as e:
                            logger.error(f"Не удалось отправить уведомление в групповой чат {chat_id}: {e}")

                    # Деактивируем таймер, чтобы избежать повторных уведомлений
                    alerts_table.update({'status': 'trend_changed'}, doc_ids=[alert.doc_id])

        except Exception as e:
            logger.error(f"Ошибка при проверке изменения тренда: {e}")

        time.sleep(300)  # Проверка каждые 5 минут


# Запуск фоновых задач
def start_background_tasks():
    threading.Thread(target=cleanup_expired_alerts, daemon=True).start()
    threading.Thread(target=check_trend_changes, daemon=True).start()


# Запуск бота
if __name__ == '__main__':
    logger.info("🚀 Бот запущен и готов к работе!")
    start_background_tasks()
    bot.polling(none_stop=True, interval=0, timeout=20)
