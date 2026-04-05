import json
import os
import time
import logging
import threading

from flask import Flask, jsonify
from firebase_admin import credentials, initialize_app, db, messaging

# --- 1. Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. Инициализация Firebase Admin SDK ---
cred_json_str = os.environ.get("FIREBASE_CREDENTIALS_JSON")
if not cred_json_str:
    logging.error("Критическая ошибка: не найдена переменная окружения FIREBASE_CREDENTIALS_JSON")
    exit(1)

db_url = os.environ.get("FIREBASE_DB_URL")
if not db_url:
    logging.error("Критическая ошибка: не найдена переменная окружения FIREBASE_DB_URL")
    exit(1)

try:
    cred_dict = json.loads(cred_json_str)
    cred = credentials.Certificate(cred_dict)
    initialize_app(cred, {'databaseURL': db_url})
    logging.info("Firebase Admin SDK успешно инициализирован.")
except Exception as e:
    logging.error(f"Ошибка инициализации Firebase Admin SDK: {e}")
    exit(1)

# --- 3. Функция обработки сообщений из очереди ---
def process_messages():
    logging.info("Фоновый обработчик очереди запущен.")
    while True:
        try:
            queue_ref = db.reference('fcm_queue')
            pending_messages = queue_ref.order_by_child('processed').equal_to(False).get()
            if pending_messages:
                logging.info(f"Найдено {len(pending_messages)} необработанных сообщений.")
                for msg_key, msg_data in pending_messages.items():
                    logging.info(f"Обработка сообщения {msg_key}: {msg_data.get('title')}")
                    try:
                        message = messaging.Message(
                            notification=messaging.Notification(
                                title=msg_data.get('title'),
                                body=msg_data.get('body'),
                            ),
                            token=msg_data.get('token'),
                            data=msg_data.get('data', {}),
                        )
                        response = messaging.send(message)
                        logging.info(f"Сообщение {msg_key} успешно отправлено. ID: {response}")
                        queue_ref.child(msg_key).update({
                            'processed': True,
                            'processed_at': int(time.time() * 1000)
                        })
                    except Exception as e:
                        logging.error(f"Ошибка при отправке сообщения {msg_key}: {e}")
                        queue_ref.child(msg_key).update({
                            'processed': True,
                            'error': str(e),
                            'processed_at': int(time.time() * 1000)
                        })
            else:
                logging.debug("Новых сообщений нет.")
        except Exception as e:
            logging.error(f"Ошибка при чтении очереди сообщений: {e}")
        time.sleep(5)

# --- 4. Создание Flask-приложения и эндпоинтов ---
app = Flask(__name__)

@app.route('/')
def index():
    return "FCM Pusher is running!"

@app.route('/ping')
def ping():
    return jsonify({"status": "alive"}), 200

# --- 5. Запуск фонового потока (для production под Gunicorn) ---
background_thread = threading.Thread(target=process_messages, daemon=True)
background_thread.start()

# --- 6. Локальный запуск (если скрипт выполняется напрямую) ---
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Запуск Flask сервера на порту {port}")
    app.run(host='0.0.0.0', port=port)
