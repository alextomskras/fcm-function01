import os
import time
import threading
from flask import Flask
from firebase_admin import credentials, initialize_app, db, messaging
import firebase_admin

# Инициализация Flask (нужна для того, чтобы Render держал процесс живым)
app = Flask(__name__)

# Простая ручка для пинга. 
# Сюда будет стучаться крон раз в 10-14 минут, чтобы сервис не уснул.
@app.route('/')
def health_check():
    return "Bot is alive! Firebase FCM worker running."

def process_fcm_queue():
    """
    Фоновый процесс: опрашивает Firebase Realtime Database,
    забирает задачи из очереди 'fcm_queue' и отправляет пуши.
    """
    print(">>> Запуск воркера обработки очереди...")
    
    while True:
        try:
            # Ссылка на узел очереди в БД
            queue_ref = db.reference('fcm_queue')
            
            # Получаем все текущие задачи (ограничимся, например, первыми 10, чтобы не зависнуть)
            # В реальном проекте лучше использовать listen() для событий, но для начала подойдет опрос
            tasks = queue_ref.limit_to_first(10).get()

            if not tasks:
                # Если задач нет, ждем немного перед следующим опросом
                time.sleep(5)
                continue

            for task_id, task_data in tasks.items():
                if not isinstance(task_data, dict):
                    continue
                
                token = task_data.get('token')
                title = task_data.get('title', 'Уведомление')
                body = task_data.get('body', 'Новое сообщение')
                
                if not token:
                    print(f"Задача {task_id}: нет токена, пропускаем.")
                    queue_ref.child(task_id).delete()
                    continue

                # Формируем сообщение для FCM
                message = messaging.Message(
                    notification=messaging.Notification(
                        title=title,
                        body=body,
                    ),
                    token=token,
                )

                try:
                    # Отправка
                    response = messaging.send(message)
                    print(f"Задача {task_id}: Успешно отправлено: {response}")
                    
                    # Удаляем задачу из очереди после успешной отправки
                    queue_ref.child(task_id).delete()
                    
                except Exception as e:
                    print(f"Задача {task_id}: Ошибка отправки: {e}")
                    # Можно переместить в узел 'errors' или пометить флагом, а не удалять
                    queue_ref.child(task_id).update({'error': str(e), 'processed': True})

        except Exception as e:
            print(f"Критическая ошибка в цикле: {e}")
            time.sleep(10) # Пауза перед перезапуском цикла при ошибке

if __name__ == '__main__':
    # 1. Настройка Firebase
    # Ожидаем переменную окружения с содержимым JSON ключа или путь к файлу
    firebase_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    db_url = os.getenv('FIREBASE_DATABASE_URL')
    
    if not firebase_creds or not db_url:
        raise ValueError("Не найдены переменные окружения GOOGLE_APPLICATION_CREDENTIALS_JSON или FIREBASE_DATABASE_URL")

    # Сохраняем JSON в временный файл, так как SDK часто ожидает путь к файлу
    # (Альтернатива: использовать credentials.Certificate.from_json_keyfile_dict)
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        f.write(firebase_creds)
        cred_path = f.name

    cred = credentials.Certificate(cred_path)
    initialize_app(cred, {
        'databaseURL': db_url
    })
    print(">>> Firebase подключен успешно")

    # 2. Запускаем воркер обработки очередей в отдельном потоке
    worker_thread = threading.Thread(target=process_fcm_queue, daemon=True)
    worker_thread.start()

    # 3. Запускаем Flask сервер
    # Render выделяет порт через переменную PORT
    port = int(os.environ.get('PORT', 8080))
    print(f">>> Сервер запущен на порту {port}. Жду пингов и задач...")
    app.run(host='0.0.0.0', port=port)
