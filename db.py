# db.py
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import os
from dotenv import load_dotenv

load_dotenv()

def remove_alert(alert_id: int):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor()
        cursor.execute("DELETE FROM alerts WHERE id = %s", (alert_id,))
        connection.commit()
        cursor.close()
        connection.close()
    except Error as e:
        print(f"Ошибка при удалении алерта: {e}")

def init_db():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor()
        
        # Создание таблиц, если их нет
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                resource VARCHAR(50),
                buy DECIMAL(10,4),
                sell DECIMAL(10,4),
                quantity BIGINT,
                timestamp BIGINT,
                date TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                resource VARCHAR(50),
                target_price DECIMAL(10,4),
                direction VARCHAR(10),
                speed DECIMAL(10,4),
                current_price DECIMAL(10,4),
                alert_time TIMESTAMP,
                created_at TIMESTAMP,
                status VARCHAR(20),
                chat_id BIGINT,
                message_id BIGINT,
                last_checked TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                user_id BIGINT PRIMARY KEY,
                has_anchor BOOLEAN DEFAULT FALSE,
                trade_level INT DEFAULT 0,
                push_interval INT DEFAULT 30,
                push_enabled BOOLEAN DEFAULT TRUE
            )
        """)
        
        connection.commit()
        cursor.close()
        connection.close()
        print("MySQL база данных инициализирована успешно.")
    except Error as e:
        print(f"Ошибка при инициализации базы данных: {e}")

def insert_market_data(resource: str, buy: float, sell: float, quantity: int, timestamp: int):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO market_data (resource, buy, sell, quantity, timestamp, date) VALUES (%s, %s, %s, %s, %s, %s)",
            (resource, buy, sell, quantity, timestamp, datetime.fromtimestamp(timestamp))
        )
        connection.commit()
        cursor.close()
        connection.close()
    except Error as e:
        print(f"Ошибка при вставке данных рынка: {e}")

def get_recent_market_data(resource: str, minutes: int = 15) -> List[Dict]:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor(dictionary=True)
        cutoff_time = int((datetime.now() - timedelta(minutes=minutes)).timestamp())
        cursor.execute(
            "SELECT * FROM market_data WHERE resource = %s AND timestamp >= %s ORDER BY timestamp",
            (resource, cutoff_time)
        )
        records = cursor.fetchall()
        cursor.close()
        connection.close()
        return records
    except Error as e:
        print(f"Ошибка при получении недавних данных рынка: {e}")
        return []

def get_latest_market_data(resource: str) -> Optional[Dict]:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM market_data WHERE resource = %s ORDER BY timestamp DESC LIMIT 1",
            (resource,)
        )
        record = cursor.fetchone()
        cursor.close()
        connection.close()
        return record
    except Error as e:
        print(f"Ошибка при получении последних данных рынка: {e}")
        return None

def search_market_data(resource: str, timestamp: int, buy: float, sell: float) -> Optional[Dict]:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM market_data WHERE resource = %s AND timestamp = %s AND buy = %s AND sell = %s LIMIT 1",
            (resource, timestamp, buy, sell)
        )
        record = cursor.fetchone()
        cursor.close()
        connection.close()
        return record
    except Error as e:
        print(f"Ошибка при поиске данных рынка: {e}")
        return None

def insert_alert(user_id: int, resource: str, target_price: float, direction: str, speed: float, current_price: float, alert_time: datetime) -> int:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO alerts (user_id, resource, target_price, direction, speed, current_price, alert_time, created_at, status, last_checked) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active', %s)",
            (user_id, resource, target_price, direction, speed, current_price, alert_time, datetime.now(), datetime.now())
        )
        alert_id = cursor.lastrowid
        connection.commit()
        cursor.close()
        connection.close()
        return alert_id
    except Error as e:
        print(f"Ошибка при вставке алерта: {e}")
        return None

def get_alert_by_id(alert_id: int) -> Optional[Dict]:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM alerts WHERE id = %s", (alert_id,))
        alert = cursor.fetchone()
        cursor.close()
        connection.close()
        return alert
    except Error as e:
        print(f"Ошибка при получении алерта: {e}")
        return None

def update_alert_status(alert_id: int, status: str):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor()
        cursor.execute("UPDATE alerts SET status = %s WHERE id = %s", (status, alert_id))
        connection.commit()
        cursor.close()
        connection.close()
    except Error as e:
        print(f"Ошибка при обновлении статуса алерта: {e}")

def get_active_alerts() -> List[Dict]:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM alerts WHERE status = 'active'")
        alerts = cursor.fetchall()
        cursor.close()
        connection.close()
        return alerts
    except Error as e:
        print(f"Ошибка при получении активных алертов: {e}")
        return []

def get_alerts_by_user(user_id: int) -> List[Dict]:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM alerts WHERE user_id = %s", (user_id,))
        alerts = cursor.fetchall()
        cursor.close()
        connection.close()
        return alerts
    except Error as e:
        print(f"Ошибка при получении алертов пользователя: {e}")
        return []

def update_alert(alert_id: int, alert_time: datetime, speed: float, current_price: float):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE alerts SET alert_time = %s, speed = %s, current_price = %s, last_checked = %s WHERE id = %s",
            (alert_time, speed, current_price, datetime.now(), alert_id)
        )
        connection.commit()
        cursor.close()
        connection.close()
    except Error as e:
        print(f"Ошибка при обновлении алерта: {e}")

def insert_setting(user_id: int, has_anchor: bool, trade_level: int, push_interval: int = 30, push_enabled: bool = True):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO settings (user_id, has_anchor, trade_level, push_interval, push_enabled) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE has_anchor = VALUES(has_anchor), trade_level = VALUES(trade_level), push_interval = VALUES(push_interval), push_enabled = VALUES(push_enabled)",
            (user_id, has_anchor, trade_level, push_interval, push_enabled)
        )
        connection.commit()
        cursor.close()
        connection.close()
    except Error as e:
        print(f"Ошибка при вставке настроек: {e}")

def get_user_settings(user_id: int) -> Dict[str, Union[bool, int]]:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM settings WHERE user_id = %s", (user_id,))
        setting = cursor.fetchone()
        cursor.close()
        connection.close()
        if setting:
            return {
                "has_anchor": setting['has_anchor'],
                "trade_level": setting['trade_level'],
                "push_interval": setting['push_interval'],
                "push_enabled": setting['push_enabled']
            }
        return {
            "has_anchor": False,
            "trade_level": 0,
            "push_interval": 30,
            "push_enabled": True
        }
    except Error as e:
        print(f"Ошибка при получении настроек пользователя: {e}")
        return {
            "has_anchor": False,
            "trade_level": 0,
            "push_interval": 30,
            "push_enabled": True
        }

def update_user_settings(user_id: int, has_anchor: bool, trade_level: int, push_interval: int = 30, push_enabled: bool = True):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO settings (user_id, has_anchor, trade_level, push_interval, push_enabled) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE has_anchor = VALUES(has_anchor), trade_level = VALUES(trade_level), push_interval = VALUES(push_interval), push_enabled = VALUES(push_enabled)",
            (user_id, has_anchor, trade_level, push_interval, push_enabled)
        )
        connection.commit()
        cursor.close()
        connection.close()
    except Error as e:
        print(f"Ошибка при обновлении настроек пользователя: {e}")

# Фоновая задача для напоминаний об обновлении данных рынка
def remind_market_update():
    while True:
        try:
            active_alerts = get_active_alerts()
            if not active_alerts:
                time.sleep(60)
                continue
            
            user_alerts = {}
            for alert in active_alerts:
                user_id = alert['user_id']
                if user_id not in user_alerts:
                    user_alerts[user_id] = []
                user_alerts[user_id].append(alert)
            
            for user_id, alerts in user_alerts.items():
                settings = get_user_settings(user_id)
                if not settings['push_enabled']:
                    continue
                
                push_interval = settings['push_interval']
                resources = set(alert['resource'] for alert in alerts)
                data_updated = False
                cutoff_time = int((datetime.now() - timedelta(minutes=15)).timestamp())
                
                for resource in resources:
                    recent_records = get_recent_market_data(resource, 15)
                    if recent_records:
                        data_updated = True
                        break
                
                if data_updated:
                    continue
                
                resources_text = ', '.join(resources)
                username = bot.get_chat_member(user_id, user_id).user.username or 'User'
                reminder_text = (
                    f"📢 @{username}, данные рынка для {resources_text} не обновлялись более 15 минут.\n"
                    f"Пожалуйста, перешлите новое сообщение с рынка (🎪), чтобы улучшить точность таймеров."
                )
                
                bot.send_message(user_id, reminder_text)
                
                group_chats = set(alert.get('chat_id') for alert in alerts if alert.get('chat_id') and alert.get('chat_id') != user_id)
                for chat_id in group_chats:
                    if check_bot_permissions(chat_id):
                        try:
                            bot.send_message(chat_id, reminder_text)
                        except Exception as e:
                            logger.error(f"Не удалось отправить напоминание в групповой чат {chat_id}: {e}")
                            bot.send_message(user_id, f"⚠️ @{username}, не удалось отправить напоминание в групповой чат {chat_id}.")
                    else:
                        logger.error(f"Бот не имеет прав для отправки сообщений в групповой чат {chat_id}")
                        bot.send_message(user_id, f"⚠️ @{username}, бот не имеет прав для отправки сообщений в групповой чат {chat_id}. Пожалуйста, добавьте бота как администратора.")
                
            min_interval = min(settings['push_interval'] for settings in (get_user_settings(user_id) for user_id in user_alerts.keys()) if settings['push_enabled'])
            time.sleep(min_interval * 60)
        
        except Exception as e:
            logger.error(f"Ошибка в задаче напоминания об обновлении данных: {e}")
            time.sleep(60)




# Запуск бота
if __name__ == '__main__':
    logger.info("🚀 Бот запущен и готов к работе!")
    start_background_tasks()
    bot.polling(none_stop=True, interval=0, timeout=20)
