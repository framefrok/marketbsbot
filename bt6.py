import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import telebot
from telebot import types
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv
import os
import threading
import time

# Импорт db.py для работы с MySQL
from db import (
    init_db,
    insert_market_data,
    get_recent_market_data,
    get_latest_market_data,
    insert_alert,
    get_alert_by_id,
    update_alert_status,
    get_active_alerts,
    get_alerts_by_user,
    insert_setting,
    get_user_settings,
    update_user_settings,
    search_market_data,
    remove_alert
)

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

# Инициализация базы данных MySQL
init_db()

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
    return get_user_settings(user_id)

# Сохранение настроек пользователя
def save_user_settings(user_id: int, has_anchor: bool, trade_level: int, push_interval: int = 30, push_enabled: bool = True):
    update_user_settings(user_id, has_anchor, trade_level, push_interval, push_enabled)

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
    return get_recent_market_data(resource, minutes)

# Получение последней записи для ресурса (базовые цены)
def get_latest_data(resource: str) -> Optional[Dict]:
    return get_latest_market_data(resource)

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
@bot.message_handler(func=lambda message: message.text and ("🎪 Рынок" in message.text))
def handle_market_forward(message):
    try:
        # Проверка на forward от бота
        if not message.forward_from:
            bot.reply_to(message, "❌ Сообщение должно быть пересылкой от бота рынка.")
            return

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
            # Проверка на дубликат
            existing = search_market_data(resource, timestamp, prices["buy"], prices["sell"])
            if not existing:
                insert_market_data(resource, prices["buy"], prices["sell"], prices.get("quantity", 0), timestamp)
                saved_count += 1
                logger.info(f"Сохранено: {resource} - buy={prices['buy']}, sell={prices['sell']}, qty={prices['quantity']}")

        if saved_count > 0:
            bot.reply_to(message, f"✅ Сохранено {saved_count} записей рынка.")
            
            # Проверяем, есть ли хотя бы у одного ресурса >=2 записей за 15 минут
            for resource in EMOJI_TO_RESOURCE.values():
                if len(get_recent_data(resource, 15)) >= 2:
                    # Передаем chat_id, если сообщение отправлено в группе
                    chat_id = message.chat.id if message.chat.type in ['group', 'supergroup'] else None
                    send_resource_selection(message.from_user.id, chat_id)
                    break
            
            # Запускаем пересчет таймеров
            threading.Thread(target=update_dynamic_timers_once, daemon=True).start()
            
        else:
            bot.reply_to(message, "ℹ️ Данные уже были сохранены ранее.")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения рынка: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Произошла ошибка: {str(e)}. Пожалуйста, попробуйте снова или свяжитесь с поддержкой.")


# Отправка выбора ресурса
def send_resource_selection(user_id: int, chat_id: Optional[int] = None):
    buttons = [
        [InlineKeyboardButton(text=res, callback_data=f"resource_{res}")]
        for res in EMOJI_TO_RESOURCE.values()
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    
    # Формируем текст с тегом пользователя
    username = bot.get_chat_member(user_id, user_id).user.username or 'User'
    message_text = f"📊 @{username}, выберите ресурс для отслеживания:"
    
    # Отправка в личный чат
    bot.send_message(user_id, message_text, reply_markup=keyboard)
    
    # Отправка в групповой чат, если указан
    if chat_id and chat_id != user_id:
        try:
            bot.send_message(chat_id, message_text, reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Не удалось отправить кнопки в групповой чат {chat_id}: {e}")
            bot.send_message(user_id, f"⚠️ Не удалось отправить сообщение в групповой чат {chat_id}.")

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
    alert_id = insert_alert(user_id, resource, target_price, direction, adj_speed, adjusted_buy, alert_time)
    
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
def schedule_alert(alert_id: int, user_id: int, resource: str, target_price: float, alert_time: datetime):
    now = datetime.now()
    sleep_seconds = (alert_time - now).total_seconds()

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    alert = get_alert_by_id(alert_id)
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

        username = bot.get_chat_member(user_id, user_id).user.username or 'User'
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

        # Отправка уведомления пользователю
        bot.send_message(user_id, notification_text)
        
        # Отправка в групповой чат и открепление сообщения
        chat_id = alert.get('chat_id')
        if chat_id and chat_id != user_id:
            try:
                bot.send_message(chat_id, notification_text)
                message_id = alert.get('message_id')
                if message_id:
                    bot.unpin_chat_message(chat_id, message_id)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление или открепить сообщение в групповом чате {chat_id}: {e}")
                bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу или открепить сообщение.")

        update_alert_status(alert_id, 'completed' if is_target_reached else 'expired')

    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
        update_alert_status(alert_id, 'error')


# Фоновая задача для очистки просроченных алертов
def cleanup_expired_alerts():
    while True:
        try:
            now = datetime.now()
            cutoff_time = (now - timedelta(hours=1)).isoformat()
            active_alerts = get_active_alerts()
            expired_alerts = [alert for alert in active_alerts if alert['alert_time'] < cutoff_time]
            
            for alert in expired_alerts:
                update_alert_status(alert['id'], 'cleanup_expired')
                logger.info(f"Очистка: деактивировано оповещение {alert['id']}")

            time.sleep(600)  # 10 минут

        except Exception as e:
            logger.error(f"Ошибка при выполнении очистки просроченных алертов: {e}")

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
    user_id = message.from_user.id
    alerts = get_alerts_by_user(user_id)
    active_alerts = [alert for alert in alerts if alert['status'] == 'active']

    if not active_alerts:
        bot.reply_to(message, "📭 У вас нет активных оповещений.")
        return

    text = "📋 Ваши активные оповещения:\n\n"
    now = datetime.now()
    for alert in active_alerts:
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
        cutoff_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
        records = search_market_data(resource, cutoff_time, None, None)
        
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
# In bt6.py, modify cmd_cancel if needed
@bot.message_handler(commands=['cancel'])
def cmd_cancel(message):
    user_id = message.from_user.id
    alerts = get_alerts_by_user(user_id)
    active_alerts = [alert for alert in alerts if alert['status'] == 'active']
    
    if not active_alerts:
        bot.reply_to(message, "🗑️ Нет активных оповещений для отмены.")
        return
        
    for alert in active_alerts:
        remove_alert(alert['id'])  # Use remove_alert instead of update_alert_status
    
    bot.reply_to(message, f"🗑️ Отменено {len(active_alerts)} оповещений.")


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
        "• /timer <ресурс> <цена> — установить таймер на цену. Пример: /timer Дерево 8.50\n"
        "• /push — настроить напоминания об обновлении данных рынка.\n"
        "  - /push interval <минуты> — задать интервал напоминаний (5–120 минут).\n"
        "  - /push start — включить напоминания.\n"
        "  - /push stop — отключить напоминания.\n"
        "• /settings — настроить бонусы от Якоря и знания торговли.\n"
        "• /cancel — отменить все ваши активные оповещения.\n\n"
        "4. Важно:\n"
        "• Бот работает на основе вашей личной истории цен. Чем чаще вы присылаете данные рынка, тем точнее прогнозы.\n"
        "• Если цена резко изменила направление движения, оповещение может не сработать. Бот пришлет уведомление, если цель не будет достигнута в расчетное время.\n"
        "• Просроченные оповещения (которые не сработали вовремя) автоматически удаляются из списка активных через час."
    )
    bot.reply_to(message, help_text)


# Фоновая задача для проверки изменения тренда
def check_trend_changes():
    while True:
        try:
            active_alerts = get_active_alerts()
            
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
                    update_alert_status(alert['id'], 'trend_changed')

        except Exception as e:
            logger.error(f"Ошибка при проверке изменения тренда: {e}")

        time.sleep(300)  # Проверка каждые 5 минут


# Фоновая задача для динамического пересчета таймеров и проверки тренда
def update_dynamic_timers():
    while True:
        try:
            active_alerts = get_active_alerts()
            
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
                
                bonus = get_user_bonus(user_id)
                current_price, _ = adjust_prices_for_user(user_id, latest_data['buy'], latest_data['sell'])
                speed = calculate_speed(records, "buy")
                if speed is None:
                    continue
                
                adj_speed = speed / (1 + bonus) if direction == "down" else speed / (1 + bonus)
                
                # Проверка изменения тренда
                current_trend = get_trend(records, "buy")
                username = bot.get_chat_member(user_id, user_id).user.username or 'User'
                if (direction == "down" and current_trend == "up") or \
                   (direction == "up" and current_trend == "down"):
                    notification_text = (
                        f"⚠️ @{username} Внимание! Тренд для {resource} изменился!\n"
                        f"Вы ждете {'падение' if direction == 'down' else 'рост'} до {target_price:.2f}, "
                        f"но цена сейчас {'растет' if current_trend == 'up' else 'падает'}.\n"
                        f"Текущая цена: {current_price:.2f}\n"
                        f"Оповещение может не сработать."
                    )
                    
                    bot.send_message(user_id, notification_text)
                    if chat_id and chat_id != user_id:
                        try:
                            bot.send_message(chat_id, notification_text)
                            if message_id:
                                bot.unpin_chat_message(chat_id, message_id)
                        except Exception as e:
                            logger.error(f"Не удалось отправить уведомление или открепить сообщение в групповом чате {chat_id}: {e}")
                            bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу или открепить сообщение.")
                    
                    update_alert_status(alert['id'], 'trend_changed')
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
                    bot.send_message(user_id, notification_text)
                    if chat_id and chat_id != user_id:
                        try:
                            bot.send_message(chat_id, notification_text)
                            if message_id:
                                bot.unpin_chat_message(chat_id, message_id)
                        except Exception as e:
                            logger.error(f"Не удалось отправить уведомление или открепить сообщение в групповом чате {chat_id}: {e}")
                            bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу или открепить сообщение.")
                    update_alert_status(alert['id'], 'completed')
                    continue
                
                # Пересчет времени
                price_diff = target_price - current_price
                if (direction == "down" and adj_speed >= 0) or (direction == "up" and adj_speed <= 0):
                    continue
                
                time_minutes = abs(price_diff) / abs(adj_speed)
                new_alert_time = datetime.now() + timedelta(minutes=time_minutes)
                
                update_alert(alert['id'], new_alert_time, adj_speed, current_price)
                
                old_alert_time = datetime.fromisoformat(alert['alert_time'])
                time_diff_minutes = abs((new_alert_time - old_alert_time).total_seconds() / 60.0)
                if time_diff_minutes > 5:
                    notification_text = (
                        f"🔄 @{username} Таймер для {resource} обновлен!\n"
                        f"Цель: {target_price:.2f}\n"
                        f"Текущая цена: {current_price:.2f}\n"
                        f"Новая скорость: {adj_speed:+.4f} в минуту\n"
                        f"Новое время: {new_alert_time.strftime('%H:%M:%S')} (~{int(time_minutes)} мин.)"
                    )
                    bot.send_message(user_id, notification_text)
                    if chat_id and chat_id != user_id:
                        try:
                            bot.send_message(chat_id, notification_text)
                        except Exception as e:
                            logger.error(f"Не удалось отправить уведомление в групповой чат {chat_id}: {e}")
                            bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу.")
        
        except Exception as e:
            logger.error(f"Ошибка при обновлении таймеров: {e}")
        
        time.sleep(60)  # Проверка каждую минуту


# Однократный пересчет таймеров после новых данных рынка
def update_dynamic_timers_once():
    try:
        active_alerts = get_active_alerts()
        
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
            
            bonus = get_user_bonus(user_id)
            current_price, _ = adjust_prices_for_user(user_id, latest_data['buy'], latest_data['sell'])
            speed = calculate_speed(records, "buy")
            if speed is None:
                continue
            
            adj_speed = speed / (1 + bonus) if direction == "down" else speed / (1 + bonus)
            
            # Проверка изменения тренда
            current_trend = get_trend(records, "buy")
            username = bot.get_chat_member(user_id, user_id).user.username or 'User'
            if (direction == "down" and current_trend == "up") or \
               (direction == "up" and current_trend == "down"):
                notification_text = (
                    f"⚠️ @{username} Внимание! Тренд для {resource} изменился!\n"
                    f"Вы ждете {'падение' if direction == 'down' else 'рост'} до {target_price:.2f}, "
                    f"но цена сейчас {'растет' if current_trend == 'up' else 'падает'}.\n"
                    f"Текущая цена: {current_price:.2f}\n"
                    f"Оповещение может не сработать."
                )
                
                bot.send_message(user_id, notification_text)
                if chat_id and chat_id != user_id:
                    try:
                        bot.send_message(chat_id, notification_text)
                        if message_id:
                            bot.unpin_chat_message(chat_id, message_id)
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление или открепить сообщение в групповом чате {chat_id}: {e}")
                        bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу или открепить сообщение.")
                
                update_alert_status(alert['id'], 'trend_changed')
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
                bot.send_message(user_id, notification_text)
                if chat_id and chat_id != user_id:
                    try:
                        bot.send_message(chat_id, notification_text)
                        if message_id:
                            bot.unpin_chat_message(chat_id, message_id)
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление или открепить сообщение в групповом чате {chat_id}: {e}")
                        bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу или открепить сообщение.")
                update_alert_status(alert['id'], 'completed')
                continue
            
            # Пересчет времени
            price_diff = target_price - current_price
            if (direction == "down" and adj_speed >= 0) or (direction == "up" and adj_speed <= 0):
                continue
            
            time_minutes = abs(price_diff) / abs(adj_speed)
            new_alert_time = datetime.now() + timedelta(minutes=time_minutes)
            
            update_alert(alert['id'], new_alert_time, adj_speed, current_price)
            
            old_alert_time = datetime.fromisoformat(alert['alert_time'])
            time_diff_minutes = abs((new_alert_time - old_alert_time).total_seconds() / 60.0)
            if time_diff_minutes > 5:
                notification_text = (
                    f"🔄 @{username} Таймер для {resource} обновлен!\n"
                    f"Цель: {target_price:.2f}\n"
                    f"Текущая цена: {current_price:.2f}\n"
                    f"Новая скорость: {adj_speed:+.4f} в минуту\n"
                    f"Новое время: {new_alert_time.strftime('%H:%M:%S')} (~{int(time_minutes)} мин.)"
                )
                bot.send_message(user_id, notification_text)
                if chat_id and chat_id != user_id:
                    try:
                        bot.send_message(chat_id, notification_text)
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление в групповой чат {chat_id}: {e}")
                        bot.send_message(user_id, "⚠️ Не удалось отправить уведомление в группу.")
    
    except Exception as e:
        logger.error(f"Ошибка при однократном обновлении таймеров: {e}")


# Запуск фоновых задач
def start_background_tasks():
    threading.Thread(target=cleanup_expired_alerts, daemon=True).start()
    threading.Thread(target=update_dynamic_timers, daemon=True).start()
    threading.Thread(target=remind_market_update, daemon=True).start()


# Запуск бота
if __name__ == '__main__':
    logger.info("🚀 Бот запущен и готов к работе!")
    start_background_tasks()
    bot.polling(none_stop=True, interval=0, timeout=20)
