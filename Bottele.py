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
import math
import threading
import time

# Загрузка токена
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

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

# Простая система состояний (вместо aiogram FSM)
user_states = {}  # user_id -> state_name
user_data = {}    # user_id -> dict с данными (resource, direction и т.д.)

# Состояния (как строки)
STATE_CHOOSING_RESOURCE = "choosing_resource"
STATE_CHOOSING_DIRECTION = "choosing_direction"
STATE_ENTERING_TARGET_PRICE = "entering_target_price"

# Парсинг сообщения рынка
def parse_market_message(text: str) -> Optional[Dict[str, Dict[str, float]]]:
    lines = text.strip().split('\n')
    resource_pattern = r"^(.+?):.*?([0-9,]+)([🪵🪨🍞🐴])$"
    price_pattern = r"([📈📉])?.*?Купить/продать:\s*([0-9.,]+)\s*/\s*([0-9.,]+).*?💰"

    resources = {}
    current_resource = None

    for line in lines:
        line = line.strip()
        if not line or line == "🎪 Рынок":
            continue

        res_match = re.match(resource_pattern, line)
        if res_match:
            resource_name = res_match.group(1).strip()
            emoji_map = {"🪵": "Дерево", "🪨": "Камень", "🍞": "Провизия", "🐴": "Лошади"}
            emoji = res_match.group(3)
            normalized_name = emoji_map.get(emoji, resource_name)
            current_resource = normalized_name
            continue

        price_match = re.search(price_pattern, line)
        if price_match and current_resource:
            buy_price = float(price_match.group(2).replace(',', '.'))
            sell_price = float(price_match.group(3).replace(',', '.'))
            resources[current_resource] = {
                "buy": buy_price,
                "sell": sell_price
            }
            current_resource = None

    return resources if resources else None

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

    if time_delta_minutes == 0:
        return None

    speed = price_delta / time_delta_minutes
    return round(speed, 4)

# Проверка тренда
def get_trend(records: List[Dict], price_type: str = "buy") -> str:
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
@bot.message_handler(func=lambda message: message.text and (message.text.startswith('🎪 Рынок') or message.forward_from))
def handle_market_forward(message):
    try:
        data = parse_market_message(message.text)
        if not data:
            bot.reply_to(message, "❌ Не удалось распознать данные рынка. Убедитесь, что формат сообщения верный.")
            return

        timestamp = int(message.date)
        saved_count = 0

        for resource, prices in data.items():
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
            bot.reply_to(message, f"✅ Сохранено {saved_count} записей рынка.")
            
            any_recent = False
            for resource in ["Дерево", "Камень", "Провизия", "Лошади"]:
                if len(get_recent_data(resource)) >= 2:
                    any_recent = True
                    break

            if any_recent:
                send_resource_selection(message.from_user.id)
        else:
            bot.reply_to(message, "ℹ️ Данные уже были сохранены ранее.")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения рынка: {e}")
        bot.reply_to(message, "❌ Произошла ошибка при обработке данных рынка.")

# Отправка выбора ресурса
def send_resource_selection(user_id: int):
    buttons = []
    for resource in ["Дерево", "Камень", "Провизия", "Лошади"]:
        btn = InlineKeyboardButton(text=resource, callback_data=f"resource_{resource}")
        buttons.append([btn])

    keyboard = InlineKeyboardMarkup(buttons)
    bot.send_message(user_id, "📊 Выберите ресурс для отслеживания:", reply_markup=keyboard)

# Обработчик выбора ресурса
@bot.callback_query_handler(func=lambda call: call.data.startswith('resource_'))
def process_resource_selection(call):
    bot.answer_callback_query(call.id)
    resource = call.data.split('_')[1]

    records = get_recent_data(resource)
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
    
    trend_emoji = "📈" if trend == "up" else "📉" if trend == "down" else "➡️"
    trend_text = "растёт" if trend == "up" else "падает" if trend == "down" else "стабильна"
    
    buttons = [
        [InlineKeyboardButton(text="📉 Падение цены", callback_data="direction_down")],
        [InlineKeyboardButton(text="📈 Рост цены", callback_data="direction_up")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    
    bot.send_message(
        call.from_user.id, 
        f"{trend_emoji} Вы выбрали {resource}. Текущая цена: {current_price}\n"
        f"Тренд: {trend_text} ({abs(speed):.4f} в минуту)\n\n"
        f"Что вас интересует?", 
        reply_markup=keyboard
    )

# Обработчик отмены действий
@bot.callback_query_handler(func=lambda call: call.data == 'cancel_action')
def cancel_action(call):
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    if user_id in user_states:
        del user_states[user_id]
    if user_id in user_data:
        del user_data[user_id]
    bot.send_message(user_id, "❌ Действие отменено.")

# Обработчик выбора направления
@bot.callback_query_handler(func=lambda call: call.data.startswith('direction_') and user_states.get(call.from_user.id) == STATE_CHOOSING_DIRECTION)
def process_direction_selection(call):
    bot.answer_callback_query(call.id)
    direction = "down" if call.data == "direction_down" else "up"
    
    user_id = call.from_user.id
    resource = user_data[user_id]["resource"]
    
    records = get_recent_data(resource)
    current_price = records[-1]["buy"]
    trend = get_trend(records, "buy")
    
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
        f"💰 Введите целевую цену для {resource} (например: {current_price * 0.9:.2f}):"
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

    records = get_recent_data(resource)
    if len(records) < 2:
        bot.reply_to(message, "⚠️ Недостаточно данных для расчета скорости. Пришлите еще обновления рынка.")
        del user_states[user_id]
        del user_data[user_id]
        return

    speed = calculate_speed(records, "buy")
    if speed is None:
        bot.reply_to(message, "⚠️ Не удалось рассчитать скорость изменения цены.")
        del user_states[user_id]
        del user_data[user_id]
        return

    current_price = records[-1]["buy"]
    price_diff = target_price - current_price

    if (direction == "down" and target_price >= current_price) or \
       (direction == "up" and target_price <= current_price):
        bot.reply_to(message, f"⚠️ При {('падении' if direction == 'down' else 'росте')} целевая цена должна быть {('ниже' if direction == 'down' else 'выше')} текущей ({current_price}).")
        return

    trend = get_trend(records, "buy")
    if (direction == "down" and trend == "up") or (direction == "up" and trend == "down"):
        bot.reply_to(message,
            "⚠️ Внимание! Выбранное направление противоречит текущему тренду. "
            "Оповещение может никогда не сработать.")

    if (direction == "down" and speed >= 0) or (direction == "up" and speed <= 0):
        bot.reply_to(message, "⚠️ Цена движется не в ту сторону, чтобы достичь вашей цели. Оповещение не будет установлено.")
        del user_states[user_id]
        del user_data[user_id]
        return

    time_minutes = abs(price_diff) / abs(speed)
    alert_time = datetime.now() + timedelta(minutes=time_minutes)
    alert_id = alerts_table.insert({
        "user_id": user_id,
        "resource": resource,
        "target_price": target_price,
        "direction": direction,
        "speed": speed,
        "current_price": current_price,
        "alert_time": alert_time.isoformat(),
        "created_at": datetime.now().isoformat(),
        "status": "active"
    })

    alert_time_str = alert_time.strftime("%H:%M:%S")
    
    bot.reply_to(
        message,
        f"✅ Таймер установлен!\n"
        f"Ресурс: {resource}\n"
        f"Текущая цена: {current_price:.2f}\n"
        f"Цель: {target_price:.2f} ({'падение' if direction == 'down' else 'рост'})\n"
        f"Скорость: {speed:+.4f} в минуту\n"
        f"Осталось: ~{int(time_minutes)} мин.\n"
        f"Ожидаемое время: {alert_time_str}\n\n"
        f"Бот оповестит вас, когда цена достигнет цели."
    )

    del user_states[user_id]
    del user_data[user_id]

    # Запуск фоновой задачи
    threading.Thread(target=schedule_alert, args=(alert_id, user_id, resource, target_price, alert_time), daemon=True).start()

# Фоновая задача для отправки уведомления
def schedule_alert(alert_id: int, user_id: int, resource: str, target_price: float, alert_time: datetime):
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
            raise ValueError(f"No latest data found for resource: {resource}")

        current_price = latest_data['buy']
        direction = alert['direction']

        is_target_reached = False
        if direction == "down" and current_price <= target_price:
            is_target_reached = True
        elif direction == "up" and current_price >= target_price:
            is_target_reached = True

        if is_target_reached:
            bot.send_message(
                user_id,
                f"🔔 {resource} достигла целевой цены!\n"
                f"Цель: {target_price:.2f}\n"
                f"Текущая цена: {current_price:.2f}\n\n"
                f"Время {'покупать!' if direction == 'down' else 'продавать!'}"
            )
            alerts_table.update({'status': 'completed'}, doc_ids=[alert_id])
        else:
            bot.send_message(
                user_id,
                f"⏰ Таймер для {resource} сработал, но цель ({target_price:.2f}) "
                f"еще не достигнута (текущая цена: {current_price:.2f}).\n"
                f"Скорость рынка, вероятно, изменилась."
            )
            alerts_table.update({'status': 'expired'}, doc_ids=[alert_id])

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
    if user_id in user_states:
        del user_states[user_id]
    if user_id in user_data:
        del user_data[user_id]
    bot.reply_to(
        message,
        "👋 Привет! Я бот для отслеживания цен на рынке в игре.\n"
        "Просто перешлите сюда сообщение с рынком (с эмодзи 🎪), и я начну анализ.\n"
        "Когда соберу достаточно данных — предложу настроить оповещение.\n\n"
        "Доступные команды:\n"
        "/status - показать активные оповещения\n"
        "/history - показать историю цен\n"
        "/stat - текущая статистика рынка\n"
        "/cancel - отменить все оповещения\n"
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
            text += f"  {time_str} - Купить: {record['buy']:.2f}, Продать: {record['sell']:.2f}\n"
        
        recent_records = get_recent_data(resource, minutes=60)
        if recent_records and len(recent_records) >= 2:
            speed = calculate_speed(recent_records, "buy")
            trend = get_trend(recent_records, "buy")
            trend_text = "растёт 📈" if trend == "up" else "падает 📉" if trend == "down" else "стабильна ➡️"
            text += f"\nТренд: {trend_text} ({speed:+.4f}/мин)"
        
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

# Команда /help (исправленная)
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
        "• /cancel — отменить все ваши активные оповещения.\n\n"
        "4. Важно:\n"
        "• Бот работает на основе вашей личной истории цен. Чем чаще вы присылаете данные рынка, тем точнее прогнозы.\n"
        "• Если цена резко изменила направление движения, оповещение может не сработать. Бот пришлет уведомление, если цель не будет достигнута в расчетное время.\n"
        "• Просроченные оповещения (которые не сработали вовремя) автоматически удаляются из списка активных через час."
    )
    bot.reply_to(message, help_text)

# Команда /stat — новая команда для статистики
@bot.message_handler(commands=['stat'])
def cmd_stat(message):
    try:
        now = datetime.now()
        text = f"📊 <b>Текущая статистика рынка</b>\n"
        text += f"🕒 Время: {now.strftime('%d.%m.%Y %H:%M:%S')}\n\n"

        resources = ["Дерево", "Камень", "Провизия", "Лошади"]
        week_ago = int((now - timedelta(days=7)).timestamp())

        for resource in resources:
            # Текущие цены
            latest = get_latest_data(resource)
            if not latest:
                text += f"🔸 {resource}: <i>нет данных</i>\n"
                continue

            current_buy = latest['buy']
            current_sell = latest['sell']

            # Максимумы за неделю
            MarketData = Query()
            week_records = market_table.search(
                (MarketData.resource == resource) & (MarketData.timestamp >= week_ago)
            )

            if week_records:
                max_buy = max(r['buy'] for r in week_records)
                max_sell = max(r['sell'] for r in week_records)
                min_buy = min(r['buy'] for r in week_records)
                min_sell = min(r['sell'] for r in week_records)
            else:
                max_buy = current_buy
                max_sell = current_sell
                min_buy = current_buy
                min_sell = current_sell

            # Тренд за последние 60 минут
            recent = get_recent_data(resource, minutes=60)
            trend_text = "неизвестно"
            if len(recent) >= 2:
                speed = calculate_speed(recent, "buy")
                trend = get_trend(recent, "buy")
                if trend == "up":
                    trend_text = f"растёт 📈 ({speed:+.4f}/мин)"
                elif trend == "down":
                    trend_text = f"падает 📉 ({speed:+.4f}/мин)"
                else:
                    trend_text = "стабильна ➡️"

            text += (
                f"🔸 {resource}:\n"
                f"   Покупка: {current_buy:.2f} (max: {max_buy:.2f}, min: {min_buy:.2f})\n"
                f"   Продажа: {current_sell:.2f} (max: {max_sell:.2f}, min: {min_sell:.2f})\n"
                f"   Тренд: {trend_text}\n\n"
            )

        bot.reply_to(message, text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /stat: {e}")
        bot.reply_to(message, "❌ Произошла ошибка при получении статистики.")

# Запуск фоновых задач
def start_background_tasks():
    threading.Thread(target=cleanup_expired_alerts, daemon=True).start()

# Запуск бота
if __name__ == '__main__':
    logger.info("Бот запущен...")
    start_background_tasks()
    bot.polling(none_stop=True)
