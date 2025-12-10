"""
Аутентификация и управление доступом
"""
import logging
from typing import Dict, Any
from datetime import datetime

from app.config import settings
from app.sheets import sheets_client

logger = logging.getLogger(__name__)


class AuthManager:
    """Менеджер аутентификации и доступа"""
    
    def __init__(self):
        self._users_cache = None
        self._logs_cache = None
    
    async def check_access(self, user_id: int, user_info: Dict[str, Any]) -> bool:
        """Проверка доступа пользователя"""
        # Главный администратор всегда имеет доступ
        if user_id == settings.main_admin_id:
            await self._log_access(user_info, "GRANTED (Admin)")
            return True
        
        # Проверяем в белом списке
        has_access = await self._is_user_in_whitelist(user_id)
        
        # Логируем попытку
        status = "GRANTED" if has_access else "DENIED"
        await self._log_access(user_info, status)
        
        return has_access
    
    async def is_admin(self, user_id: int) -> bool:
        """Проверка, является ли пользователь администратором"""
        # Главный администратор
        if user_id == settings.main_admin_id:
            return True
        
        # Проверяем в базе
        try:
            users = await self._get_users_data()
            for user in users[1:]:  # Пропускаем заголовок
                if len(user) >= 4:
                    stored_id = int(user[0]) if user[0] else 0
                    if stored_id == user_id and user[3] == "admin":
                        return True
        except Exception as e:
            logger.error(f"Error checking admin status: {e}")
        
        return False
    
    async def add_user(self, user_id: int, username: str, 
                      first_name: str, last_name: str, 
                      user_type: str = "user") -> str:
        """Добавление пользователя в белый список"""
        try:
            # Проверяем, нет ли уже такого пользователя
            users = await self._get_users_data()
            for user in users[1:]:
                if user and len(user) > 0 and user[0] and int(user[0]) == user_id:
                    return f"⚠️ Пользователь с ID {user_id} уже существует"
            
            # Добавляем пользователя
            await sheets_client.append_row([
                user_id,
                username or "",
                f"{first_name or ''} {last_name or ''}".strip(),
                "admin" if user_type == "admin" else "user"
            ], "Users")
            
            # Сбрасываем кэш
            self._users_cache = None
            
            role = "👑 Админ" if user_type == "admin" else "👤 Пользователь"
            return f"✅ Пользователь добавлен\nID: {user_id}\nРоль: {role}"
            
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            return f"❌ Ошибка: {str(e)}"
    
    async def remove_user(self, user_id: int) -> str:
        """Удаление пользователя из белого списка"""
        try:
            # Нельзя удалить главного администратора
            if user_id == settings.main_admin_id:
                return "❌ Нельзя удалить главного администратора!"
            
            users = await self._get_users_data()
            found = False
            
            # Ищем пользователя для удаления
            for i in range(len(users) - 1, 0, -1):
                if users[i] and len(users[i]) > 0 and users[i][0]:
                    if int(users[i][0]) == user_id:
                        # Удаляем строку
                        worksheet = await sheets_client.get_worksheet("Users")
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, worksheet.delete_rows, i + 1)
                        found = True
                        break
            
            # Сбрасываем кэш
            if found:
                self._users_cache = None
                return "✅ Пользователь удален"
            else:
                return "❌ Пользователь не найден"
                
        except Exception as e:
            logger.error(f"Error removing user: {e}")
            return f"❌ Ошибка: {str(e)}"
    
    async def get_users_list(self) -> str:
        """Получение списка пользователей"""
        try:
            users = await self._get_users_data()
            
            if len(users) <= 1:
                return "📭 Список пользователей пуст"
            
            result = "👥 <b>Список пользователей</b>\n\n"
            
            for i, user in enumerate(users[1:], start=1):
                if len(user) >= 4:
                    user_id = user[0] or "N/A"
                    username = user[1] or "Не указано"
                    name = user[2] or "Не указано"
                    role = "👑 Админ" if user[3] == "admin" else "👤 Пользователь"
                    
                    result += f"{i}. ID: <code>{user_id}</code>\n"
                    result += f"   👤: {name}\n"
                    result += f"   📱: {username}\n"
                    result += f"   🏷️: {role}\n\n"
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting users list: {e}")
            return f"❌ Ошибка: {str(e)}"
    
    async def get_stats(self) -> Dict[str, Any]:
        """Получение статистики"""
        stats = {}
        
        try:
            # Статистика базы данных
            main_data = await sheets_client.get_all_data()
            if main_data:
                stats['database'] = {
                    'records': len(main_data) - 1,
                    'columns': len(main_data[0]) if main_data[0] else 0
                }
            
            # Статистика пользователей
            try:
                users = await self._get_users_data()
                if users:
                    admin_count = sum(1 for u in users[1:] if len(u) >= 4 and u[3] == "admin")
                    user_count = len(users) - 1 - admin_count
                    
                    stats['users'] = {
                        'total': len(users) - 1,
                        'admins': admin_count,
                        'regular': user_count
                    }
            except:
                pass
            
            # Статистика логов
            try:
                logs = await self._get_logs_data()
                if logs:
                    granted = sum(1 for l in logs[1:] if len(l) >= 6 and l[5] == "GRANTED")
                    denied = sum(1 for l in logs[1:] if len(l) >= 6 and l[5] == "DENIED")
                    
                    stats['logs'] = {
                        'total': len(logs) - 1,
                        'granted': granted,
                        'denied': denied
                    }
            except:
                pass
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
        
        return stats
    
    async def _get_users_data(self):
        """Получение данных пользователей с кэшированием"""
        if self._users_cache is None:
            try:
                self._users_cache = await sheets_client.get_all_data("Users")
            except Exception as e:
                logger.error(f"Error loading users data: {e}")
                self._users_cache = []
        
        return self._users_cache
    
    async def _get_logs_data(self):
        """Получение данных логов"""
        if self._logs_cache is None:
            try:
                self._logs_cache = await sheets_client.get_all_data("AccessLog")
            except Exception as e:
                logger.error(f"Error loading logs data: {e}")
                self._logs_cache = []
        
        return self._logs_cache
    
    async def _is_user_in_whitelist(self, user_id: int) -> bool:
        """Проверка наличия пользователя в белом списке"""
        users = await self._get_users_data()
        
        for user in users[1:]:  # Пропускаем заголовок
            if user and len(user) > 0 and user[0]:
                try:
                    if int(user[0]) == user_id:
                        return True
                except (ValueError, TypeError):
                    continue
        
        return False
    
    async def _log_access(self, user_info: Dict[str, Any], status: str):
        """Логирование попытки доступа"""
        try:
            await sheets_client.append_row([
                datetime.now().isoformat(),
                user_info.get('id', ''),
                f"@{user_info.get('username', '')}" if user_info.get('username') else "",
                user_info.get('first_name', ''),
                user_info.get('last_name', ''),
                status
            ], "AccessLog")
            
            # Сбрасываем кэш логов
            self._logs_cache = None
            
        except Exception as e:
            logger.error(f"Error logging access: {e}")


# Глобальный экземпляр менеджера аутентификации
auth_manager = AuthManager()