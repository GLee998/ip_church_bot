"""
Управление сессиями пользователей
"""
import json
import time
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging

import redis.asyncio as redis

from app.config import settings

logger = logging.getLogger(__name__)


class SessionManager:
    """Менеджер сессий"""
    
    def __init__(self):
        self._storage = None
        self._memory_sessions = {}
        self._lock = asyncio.Lock()
    
    async def _get_storage(self):
        """Получение хранилища сессий"""
        if self._storage is None:
            if settings.session_storage == "redis" and settings.redis_url:
                try:
                    self._storage = redis.from_url(
                        settings.redis_url,
                        decode_responses=True,
                        socket_connect_timeout=5,
                        socket_keepalive=True
                    )
                    # Проверяем подключение
                    await self._storage.ping()
                    logger.info("Redis session storage initialized")
                except Exception as e:
                    logger.warning(f"Redis connection failed: {e}. Falling back to memory storage.")
                    self._storage = "memory"
            else:
                self._storage = "memory"
                logger.info("Using in-memory session storage")
        
        return self._storage
    
    def _get_session_key(self, chat_id: int) -> str:
        """Генерация ключа сессии"""
        return f"session:{chat_id}"
    
    async def get_session(self, chat_id: int) -> Dict[str, Any]:
        """Получение сессии"""
        storage = await self._get_storage()
        
        # Redis storage
        if isinstance(storage, redis.Redis):
            try:
                key = self._get_session_key(chat_id)
                data = await storage.get(key)
                
                if data:
                    session = json.loads(data)
                    
                    # Обновляем время доступа
                    session['last_access'] = time.time()
                    await storage.setex(
                        key,
                        int(settings.session_timeout.total_seconds()),
                        json.dumps(session)
                    )
                    
                    return session
            except Exception as e:
                logger.error(f"Redis session error: {e}")
        
        # Memory storage
        async with self._lock:
            if chat_id in self._memory_sessions:
                session = self._memory_sessions[chat_id]
                # Проверяем не истекла ли сессия
                if time.time() - session.get('last_access', 0) < settings.session_timeout.total_seconds():
                    session['last_access'] = time.time()
                    return session
                else:
                    del self._memory_sessions[chat_id]
        
        # Новая сессия
        return self._create_new_session(chat_id)
    
    async def save_session(self, chat_id: int, session_data: Dict[str, Any]):
        """Сохранение сессии"""
        storage = await self._get_storage()
        session_data['last_access'] = time.time()
        
        # Redis storage
        if isinstance(storage, redis.Redis):
            try:
                key = self._get_session_key(chat_id)
                await storage.setex(
                    key,
                    int(settings.session_timeout.total_seconds()),
                    json.dumps(session_data)
                )
                return
            except Exception as e:
                logger.error(f"Redis save error: {e}")
        
        # Memory storage
        async with self._lock:
            self._memory_sessions[chat_id] = session_data
    
    async def clear_session(self, chat_id: int):
        """Очистка сессии"""
        storage = await self._get_storage()
        
        # Redis storage
        if isinstance(storage, redis.Redis):
            try:
                key = self._get_session_key(chat_id)
                await storage.delete(key)
                return
            except Exception as e:
                logger.error(f"Redis delete error: {e}")
        
        # Memory storage
        async with self._lock:
            if chat_id in self._memory_sessions:
                del self._memory_sessions[chat_id]
    
    async def cleanup_expired_sessions(self):
        """Очистка устаревших сессий"""
        if settings.session_storage == "memory":
            current_time = time.time()
            timeout = settings.session_timeout.total_seconds()
            
            async with self._lock:
                expired = [
                    chat_id for chat_id, session in self._memory_sessions.items()
                    if current_time - session.get('last_access', 0) > timeout
                ]
                
                for chat_id in expired:
                    del self._memory_sessions[chat_id]
                
                if expired:
                    logger.info(f"Cleaned up {len(expired)} expired sessions")
    
    def _create_new_session(self, chat_id: int) -> Dict[str, Any]:
        """Создание новой сессии"""
        return {
            'state': 'IDLE',
            'mode': None,
            'draft': {},
            'step': None,
            'last_letter': None,
            'people_list': [],
            'viewing_row': None,
            'editing_row': None,
            'user_id': None,
            'last_access': time.time(),
            'created_at': datetime.now().isoformat()
        }


# Глобальный экземпляр менеджера сессий
session_manager = SessionManager()