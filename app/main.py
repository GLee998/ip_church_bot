"""
FastAPI приложение для Telegram бота
"""
import os
import logging
import json
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

from app.config import settings
from app.bot import bot
from app.sheets import sheets_client
from app.sessions import session_manager

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=getattr(logging, settings.log_level.upper())
)
logger = logging.getLogger(__name__)

# Глобальные переменные для Telegram бота
telegram_app: Application = None

# Pydantic модели
class TelegramWebhook(BaseModel):
    """Модель для вебхука Telegram"""
    update_id: int
    message: Dict[str, Any] = None
    callback_query: Dict[str, Any] = None
    edited_message: Dict[str, Any] = None
    channel_post: Dict[str, Any] = None
    edited_channel_post: Dict[str, Any] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения
    """
    global telegram_app
    
    # Startup
    logger.info("🚀 Starting Church Telegram Bot (FastAPI)")
    logger.info(f"📁 Environment: {settings.environment}")
    logger.info(f"🤖 Telegram Bot Token: {settings.telegram_token[:10]}...")
    logger.info(f"📊 Google Sheet ID: {settings.sheet_id}")
    
    try:
        # Инициализация Telegram бота
        telegram_app = Application.builder().token(settings.telegram_token).build()
        
        # Регистрация обработчиков
        telegram_app.add_handler(CommandHandler("start", bot._send_main_menu))
        telegram_app.add_handler(CommandHandler("menu", bot._send_main_menu))
        telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
        telegram_app.add_handler(CallbackQueryHandler(bot.handle_callback))
        
        # Инициализация бота
        await telegram_app.initialize()
        await telegram_app.start()
        
        # Проверка бота
        bot_info = await telegram_app.bot.get_me()
        logger.info(f"✅ Bot initialized: @{bot_info.username}")
        
        # Проверка подключения к Google Sheets
        try:
            headers = await sheets_client.get_headers()
            logger.info(f"📊 Connected to Google Sheets: {len(headers)} columns")
        except Exception as e:
            logger.warning(f"⚠️ Google Sheets connection issue: {e}")
        
        logger.info("✅ Application startup completed")
        
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        raise
    
    yield  # Приложение работает
    
    # Shutdown
    logger.info("🛑 Shutting down application...")
    
    if telegram_app:
        try:
            await telegram_app.stop()
            await telegram_app.shutdown()
            logger.info("✅ Telegram bot stopped")
        except Exception as e:
            logger.error(f"Error stopping bot: {e}")
    
    # Очистка сессий
    await session_manager.cleanup_expired_sessions()
    logger.info("✅ Cleanup completed")


# Создание FastAPI приложения
app = FastAPI(
    title="Church Telegram Bot",
    description="Быстрый Telegram бот для управления церковной базой данных",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.is_development else None,
    redoc_url="/redoc" if settings.is_development else None
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Эндпоинты
@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {
        "service": "Church Telegram Bot",
        "version": "2.0.0",
        "environment": settings.environment,
        "status": "online",
        "timestamp": datetime.now().isoformat(),
        "features": [
            "FastAPI backend",
            "Google Sheets integration",
            "Telegram Bot API",
            "User sessions",
            "Admin panel"
        ]
    }


@app.get("/health")
async def health_check():
    """Health check для Cloud Run и мониторинга"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "api": "ok",
            "telegram_bot": "initialized" if telegram_app else "not_initialized",
            "google_sheets": "unknown",
            "session_storage": settings.session_storage
        }
    }
    
    # Проверка Google Sheets
    try:
        await sheets_client.get_headers()
        health_status["components"]["google_sheets"] = "connected"
    except Exception as e:
        health_status["components"]["google_sheets"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    return health_status


@app.post("/webhook")
async def telegram_webhook(webhook_data: TelegramWebhook):
    """
    Основной эндпоинт для вебхука Telegram
    Обрабатывает за 1-10ms
    """
    start_time = datetime.now()
    
    if not telegram_app:
        raise HTTPException(status_code=503, detail="Telegram bot not initialized")
    
    try:
        # Конвертируем данные в объект Update
        update = Update.de_json(webhook_data.dict(), telegram_app.bot)
        
        # Создаем простой контекст
        class SimpleContext:
            def __init__(self):
                self.bot = telegram_app.bot
                self.application = telegram_app
        
        context = SimpleContext()
        
        # Определяем тип обновления и вызываем соответствующий обработчик
        if update.message:
            await bot.handle_message(update, context)
        elif update.callback_query:
            await bot.handle_callback(update, context)
        
        # Измеряем время обработки
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        logger.debug(f"✅ Webhook processed in {processing_time:.2f}ms")
        
        return {
            "ok": True,
            "processing_time_ms": round(processing_time, 2),
            "update_id": update.update_id
        }
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {str(e)}", exc_info=True)
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": str(e),
                "processing_time_ms": round(processing_time, 2)
            }
        )


@app.get("/setup-webhook")
async def setup_webhook(request: Request):
    """Установка вебхука Telegram"""
    if not telegram_app:
        raise HTTPException(status_code=503, detail="Telegram bot not initialized")
    
    try:
        import httpx
        
        # Определяем URL вебхука
        if settings.is_production and settings.service_url:
            base_url = settings.service_url.rstrip("/")
        else:
            base_url = str(request.base_url).rstrip("/")
        
        webhook_url = f"{base_url}/webhook"
        
        logger.info(f"Setting webhook to: {webhook_url}")
        
        # Устанавливаем вебхук через Telegram API
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"https://api.telegram.org/bot{settings.telegram_token}/setWebhook",
                json={
                    "url": webhook_url,
                    "drop_pending_updates": True,
                    "max_connections": 100
                }
            )
            
            result = response.json()
            
            if result.get("ok"):
                logger.info(f"✅ Webhook set successfully")
                return {
                    "success": True,
                    "webhook_url": webhook_url,
                    "result": result
                }
            else:
                logger.error(f"❌ Failed to set webhook: {result}")
                raise HTTPException(
                    status_code=400,
                    detail=result.get("description", "Unknown error")
                )
                
    except Exception as e:
        logger.error(f"Webhook setup error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/webhook-info")
async def get_webhook_info():
    """Получение информации о вебхуке"""
    try:
        import httpx
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"https://api.telegram.org/bot{settings.telegram_token}/getWebhookInfo"
            )
            
            return response.json()
            
    except Exception as e:
        logger.error(f"Error getting webhook info: {e}")
        return {"error": str(e)}


@app.get("/bot-info")
async def get_bot_info():
    """Информация о боте"""
    if not telegram_app:
        raise HTTPException(status_code=503, detail="Bot not initialized")
    
    try:
        bot_info = await telegram_app.bot.get_me()
        
        return {
            "id": bot_info.id,
            "username": f"@{bot_info.username}" if bot_info.username else None,
            "first_name": bot_info.first_name,
            "is_bot": bot_info.is_bot,
            "can_join_groups": bot_info.can_join_groups,
            "can_read_all_group_messages": bot_info.can_read_all_group_messages,
            "supports_inline_queries": bot_info.supports_inline_queries
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Статистика системы"""
    try:
        stats = await bot.auth.get_stats()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "statistics": stats,
            "environment": settings.environment,
            "session_storage": settings.session_storage
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin")
async def admin_panel():
    """Админ панель (веб-интерфейс)"""
    html_content = """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Church Bot Admin</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    </head>
    <body class="bg-gray-50">
        <div class="min-h-screen">
            <!-- Header -->
            <header class="bg-white shadow">
                <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
                    <div class="flex items-center justify-between">
                        <div>
                            <h1 class="text-3xl font-bold text-gray-900">
                                <i class="fas fa-church text-blue-600 mr-3"></i>
                                Church Bot Admin
                            </h1>
                            <p class="text-gray-600 mt-2">Управление Telegram ботом для церковной базы данных</p>
                        </div>
                        <div class="bg-green-100 text-green-800 px-4 py-2 rounded-lg">
                            <i class="fas fa-check-circle mr-2"></i>
                            Система активна
                        </div>
                    </div>
                </div>
            </header>

            <!-- Main Content -->
            <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    
                    <!-- Bot Status Card -->
                    <div class="bg-white rounded-lg shadow p-6">
                        <div class="flex items-center mb-4">
                            <div class="bg-blue-100 p-3 rounded-full mr-4">
                                <i class="fas fa-robot text-blue-600 text-xl"></i>
                            </div>
                            <h3 class="text-xl font-semibold">Статус бота</h3>
                        </div>
                        <div id="bot-status" class="text-gray-600">
                            Загрузка...
                        </div>
                        <button onclick="getBotInfo()" class="mt-4 bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition">
                            <i class="fas fa-sync-alt mr-2"></i>Обновить
                        </button>
                    </div>

                    <!-- Webhook Status -->
                    <div class="bg-white rounded-lg shadow p-6">
                        <div class="flex items-center mb-4">
                            <div class="bg-green-100 p-3 rounded-full mr-4">
                                <i class="fas fa-link text-green-600 text-xl"></i>
                            </div>
                            <h3 class="text-xl font-semibold">Вебхук</h3>
                        </div>
                        <div id="webhook-status" class="text-gray-600">
                            Загрузка...
                        </div>
                        <div class="mt-4 space-y-2">
                            <button onclick="setupWebhook()" class="w-full bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition">
                                <i class="fas fa-plug mr-2"></i>Установить вебхук
                            </button>
                            <button onclick="getWebhookInfo()" class="w-full bg-gray-200 text-gray-800 px-4 py-2 rounded-lg hover:bg-gray-300 transition">
                                <i class="fas fa-info-circle mr-2"></i>Информация
                            </button>
                        </div>
                    </div>

                    <!-- System Stats -->
                    <div class="bg-white rounded-lg shadow p-6">
                        <div class="flex items-center mb-4">
                            <div class="bg-purple-100 p-3 rounded-full mr-4">
                                <i class="fas fa-chart-bar text-purple-600 text-xl"></i>
                            </div>
                            <h3 class="text-xl font-semibold">Статистика</h3>
                        </div>
                        <div id="system-stats" class="text-gray-600">
                            Загрузка...
                        </div>
                        <button onclick="getStats()" class="mt-4 bg-purple-600 text-white px-4 py-2 rounded-lg hover:bg-purple-700 transition">
                            <i class="fas fa-chart-line mr-2"></i>Обновить статистику
                        </button>
                    </div>

                    <!-- Database Info -->
                    <div class="bg-white rounded-lg shadow p-6 md:col-span-2 lg:col-span-3">
                        <div class="flex items-center mb-4">
                            <div class="bg-yellow-100 p-3 rounded-full mr-4">
                                <i class="fas fa-database text-yellow-600 text-xl"></i>
                            </div>
                            <h3 class="text-xl font-semibold">База данных (Google Sheets)</h3>
                        </div>
                        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                            <div class="bg-gray-50 p-4 rounded-lg">
                                <h4 class="font-medium text-gray-700">ID таблицы</h4>
                                <p class="text-lg font-mono break-all">{{SHEET_ID}}</p>
                            </div>
                            <div class="bg-gray-50 p-4 rounded-lg">
                                <h4 class="font-medium text-gray-700">Токен бота</h4>
                                <p class="text-lg font-mono">{{TELEGRAM_TOKEN_SHORT}}...</p>
                            </div>
                            <div class="bg-gray-50 p-4 rounded-lg">
                                <h4 class="font-medium text-gray-700">Админ ID</h4>
                                <p class="text-lg">{{MAIN_ADMIN_ID}}</p>
                            </div>
                        </div>
                    </div>

                    <!-- Quick Actions -->
                    <div class="bg-white rounded-lg shadow p-6 md:col-span-2 lg:col-span-3">
                        <h3 class="text-xl font-semibold mb-4">Быстрые действия</h3>
                        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                            <a href="/health" target="_blank" class="bg-blue-50 text-blue-700 p-4 rounded-lg hover:bg-blue-100 transition text-center">
                                <i class="fas fa-heartbeat text-2xl mb-2"></i>
                                <div class="font-medium">Health Check</div>
                            </a>
                            <a href="/docs" target="_blank" class="bg-green-50 text-green-700 p-4 rounded-lg hover:bg-green-100 transition text-center">
                                <i class="fas fa-book text-2xl mb-2"></i>
                                <div class="font-medium">API Документация</div>
                            </a>
                            <a href="https://console.cloud.google.com/run" target="_blank" class="bg-red-50 text-red-700 p-4 rounded-lg hover:bg-red-100 transition text-center">
                                <i class="fas fa-cloud text-2xl mb-2"></i>
                                <div class="font-medium">Cloud Console</div>
                            </a>
                            <div onclick="showTelegramBot()" class="bg-purple-50 text-purple-700 p-4 rounded-lg hover:bg-purple-100 transition text-center cursor-pointer">
                                <i class="fab fa-telegram text-2xl mb-2"></i>
                                <div class="font-medium">Открыть бота</div>
                            </div>
                        </div>
                    </div>

                </div>
            </main>

            <!-- Footer -->
            <footer class="bg-white border-t mt-8">
                <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
                    <div class="flex flex-col md:flex-row justify-between items-center">
                        <div class="text-gray-600">
                            <p>Church Telegram Bot v2.0.0 • Powered by FastAPI & Google Cloud Run</p>
                            <p class="text-sm mt-1">Время ответа API: 1-10ms • Обработка запросов: асинхронная</p>
                        </div>
                        <div class="mt-4 md:mt-0">
                            <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-blue-100 text-blue-800">
                                <span class="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                                Высокая производительность
                            </span>
                        </div>
                    </div>
                </div>
            </footer>
        </div>

        <script>
            // Конфигурация
            const CONFIG = {
                SHEET_ID: "{{SHEET_ID}}",
                TELEGRAM_TOKEN_SHORT: "{{TELEGRAM_TOKEN_SHORT}}",
                MAIN_ADMIN_ID: "{{MAIN_ADMIN_ID}}"
            };

            // Функции API
            async function apiCall(endpoint, method = 'GET', data = null) {
                try {
                    const options = {
                        method,
                        headers: {
                            'Content-Type': 'application/json',
                        }
                    };
                    
                    if (data) {
                        options.body = JSON.stringify(data);
                    }
                    
                    const response = await fetch(endpoint, options);
                    return await response.json();
                } catch (error) {
                    console.error('API Error:', error);
                    return { error: error.message };
                }
            }

            // Обновление статуса бота
            async function getBotInfo() {
                const statusEl = document.getElementById('bot-status');
                statusEl.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Загрузка...';
                
                const data = await apiCall('/bot-info');
                
                if (data.id) {
                    statusEl.innerHTML = `
                        <div class="space-y-2">
                            <div><strong>ID:</strong> <code>${data.id}</code></div>
                            <div><strong>Username:</strong> ${data.username || 'N/A'}</div>
                            <div><strong>Имя:</strong> ${data.first_name}</div>
                            <div><strong>Бот:</strong> ${data.is_bot ? 'Да' : 'Нет'}</div>
                        </div>
                    `;
                } else {
                    statusEl.innerHTML = `<div class="text-red-600">Ошибка: ${data.error || 'Неизвестная ошибка'}</div>`;
                }
            }

            // Информация о вебхуке
            async function getWebhookInfo() {
                const statusEl = document.getElementById('webhook-status');
                statusEl.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Загрузка...';
                
                const data = await apiCall('/webhook-info');
                
                if (data.ok) {
                    const url = data.result.url || 'Не установлен';
                    const hasCerts = data.result.has_custom_certificate ? 'Да' : 'Нет';
                    const pending = data.result.pending_update_count;
                    
                    statusEl.innerHTML = `
                        <div class="space-y-2">
                            <div><strong>URL:</strong> <code class="break-all">${url}</code></div>
                            <div><strong>Сертификат:</strong> ${hasCerts}</div>
                            <div><strong>Ожидающих обновлений:</strong> ${pending}</div>
                        </div>
                    `;
                } else {
                    statusEl.innerHTML = `<div class="text-red-600">Ошибка: ${data.description || 'Неизвестная ошибка'}</div>`;
                }
            }

            // Установка вебхука
            async function setupWebhook() {
                const statusEl = document.getElementById('webhook-status');
                statusEl.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Установка...';
                
                const data = await apiCall('/setup-webhook');
                
                if (data.success) {
                    statusEl.innerHTML = `
                        <div class="space-y-2 text-green-600">
                            <div><i class="fas fa-check-circle mr-2"></i><strong>Успешно установлен!</strong></div>
                            <div><strong>URL:</strong> <code class="break-all">${data.webhook_url}</code></div>
                        </div>
                    `;
                } else {
                    statusEl.innerHTML = `<div class="text-red-600">Ошибка: ${data.detail || 'Неизвестная ошибка'}</div>`;
                }
            }

            // Получение статистики
            async function getStats() {
                const statsEl = document.getElementById('system-stats');
                statsEl.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Загрузка...';
                
                const data = await apiCall('/stats');
                
                if (data.statistics) {
                    const stats = data.statistics;
                    let html = '<div class="space-y-2">';
                    
                    if (stats.database) {
                        html += `<div><strong>База данных:</strong> ${stats.database.records || 0} записей, ${stats.database.columns || 0} колонок</div>`;
                    }
                    
                    if (stats.users) {
                        html += `<div><strong>Пользователи:</strong> ${stats.users.total || 0} всего (${stats.users.admins || 0} админов)</div>`;
                    }
                    
                    if (stats.logs) {
                        html += `<div><strong>Логи доступа:</strong> ${stats.logs.granted || 0} успешно, ${stats.logs.denied || 0} отказано</div>`;
                    }
                    
                    html += `<div><strong>Хранилище сессий:</strong> ${data.session_storage}</div>`;
                    html += '</div>';
                    
                    statsEl.innerHTML = html;
                } else {
                    statsEl.innerHTML = `<div class="text-red-600">Ошибка: ${data.detail || 'Неизвестная ошибка'}</div>`;
                }
            }

            // Открытие Telegram бота
            function showTelegramBot() {
                const token = "{{TELEGRAM_TOKEN}}".replace('...', '');
                const botUsername = token.split(':')[0];
                window.open(`https://t.me/${botUsername}`, '_blank');
            }

            // Инициализация при загрузке
            document.addEventListener('DOMContentLoaded', function() {
                // Заменяем плейсхолдеры
                document.body.innerHTML = document.body.innerHTML
                    .replace(/{{SHEET_ID}}/g, CONFIG.SHEET_ID)
                    .replace(/{{TELEGRAM_TOKEN_SHORT}}/g, CONFIG.TELEGRAM_TOKEN_SHORT)
                    .replace(/{{MAIN_ADMIN_ID}}/g, CONFIG.MAIN_ADMIN_ID)
                    .replace(/{{TELEGRAM_TOKEN}}/g, CONFIG.TELEGRAM_TOKEN_SHORT);
                
                // Загружаем начальные данные
                getBotInfo();
                getWebhookInfo();
                getStats();
                
                // Авто-обновление каждые 30 секунд
                setInterval(getStats, 30000);
            });
        </script>
    </body>
    </html>
    """
    
    # Заменяем плейсхолдеры реальными значениями
    html_content = html_content.replace("{{SHEET_ID}}", settings.sheet_id)
    html_content = html_content.replace("{{TELEGRAM_TOKEN_SHORT}}", settings.telegram_token[:10])
    html_content = html_content.replace("{{MAIN_ADMIN_ID}}", str(settings.main_admin_id))
    
    return HTMLResponse(content=html_content)


# Обработчик ошибок
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Глобальный обработчик исключений"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=200,
        content={
            "error": "Internal server error",
            "message": str(exc) if settings.debug else "Something went wrong",
            "path": request.url.path,
            "timestamp": datetime.now().isoformat()
        }
    )


# Запуск сервера для разработки
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8080))
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=settings.is_development,
        workers=4 if settings.is_production else 1,
        log_level="info"
    )