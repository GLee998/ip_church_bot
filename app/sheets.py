"""
Асинхронный клиент для Google Sheets с КЭШИРОВАНИЕМ
"""
import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime

import gspread
from google.oauth2 import service_account
from google.auth import default as google_default

from app.config import settings

logger = logging.getLogger(__name__)


class GoogleSheetsClient:
    """Клиент для работы с Google Sheets с кэшированием в памяти"""
    
    def __init__(self):
        self._client = None
        self._spreadsheet = None
        self._worksheets = {}
        
        # КЭШ: храним данные в памяти
        # Структура: {'worksheet_title': [[row1], [row2], ...]}
        self._cache: Dict[str, List[List[Any]]] = {}
        self._cache_lock = asyncio.Lock()
    
    async def _get_client(self):
        """Получение или создание клиента (без изменений)"""
        if self._client is None:
            try:
                if settings.google_credentials_file:
                    credentials = service_account.Credentials.from_service_account_file(
                        settings.google_credentials_file,
                        scopes=['https://www.googleapis.com/auth/spreadsheets',
                               'https://www.googleapis.com/auth/drive']
                    )
                else:
                    credentials, _ = google_default()
                
                loop = asyncio.get_event_loop()
                self._client = await loop.run_in_executor(
                    None, 
                    lambda: gspread.authorize(credentials)
                )
                logger.info("✅ Google Sheets client authorized")
            except Exception as e:
                logger.error(f"Failed to initialize Google Sheets client: {e}")
                raise
        return self._client
    
    async def _get_spreadsheet(self):
        """Получение таблицы"""
        if self._spreadsheet is None:
            client = await self._get_client()
            loop = asyncio.get_event_loop()
            self._spreadsheet = await loop.run_in_executor(
                None,
                lambda: client.open_by_key(settings.sheet_id)
            )
        return self._spreadsheet
    
    async def get_worksheet(self, title: str = None):
        """Получение листа (gspread object)"""
        spreadsheet = await self._get_spreadsheet()
        loop = asyncio.get_event_loop()
        
        # Определяем имя листа для ключа кэша
        target_title = title if title else "MainSheet" 
        
        if target_title not in self._worksheets:
            try:
                if title is None:
                    worksheet = await loop.run_in_executor(None, lambda: spreadsheet.sheet1)
                else:
                    worksheet = await loop.run_in_executor(None, lambda: spreadsheet.worksheet(title))
                self._worksheets[target_title] = worksheet
            except gspread.exceptions.WorksheetNotFound:
                worksheet = await loop.run_in_executor(
                    None,
                    lambda: spreadsheet.add_worksheet(title=title, rows=1000, cols=26)
                )
                self._worksheets[target_title] = worksheet
        
        return self._worksheets[target_title]

    async def refresh_cache(self, worksheet_title: str = None):
        """Принудительное обновление кэша из Google Sheets"""
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        logger.info(f"🔄 Refreshing cache for {cache_key}...")
        
        # Скачиваем данные
        data = await loop.run_in_executor(None, worksheet.get_all_values)
        
        async with self._cache_lock:
            self._cache[cache_key] = data
            
        logger.info(f"✅ Cache updated for {cache_key}: {len(data)} rows")
        return len(data)

    async def get_all_data(self, worksheet_title: str = None) -> List[List[Any]]:
        """Получение всех данных (сначала из кэша)"""
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        
        # Если данных нет в кэше, загружаем их
        if cache_key not in self._cache:
            await self.refresh_cache(worksheet_title)
        
        # Возвращаем данные из памяти (МГНОВЕННО)
        return self._cache.get(cache_key, [])
    
    async def get_headers(self, worksheet_title: str = None) -> List[str]:
        """Получение заголовков"""
        data = await self.get_all_data(worksheet_title)
        return data[0] if data else []
    
    async def append_row(self, data: List[Any], worksheet_title: str = None) -> int:
        """Добавление строки (обновляет кэш и отправляет в Google)"""
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # 1. Отправляем в Google (это займет время, около 1 сек)
        await loop.run_in_executor(None, worksheet.append_row, data)
        
        # 2. Обновляем локальный кэш (чтобы пользователь сразу увидел изменения)
        async with self._cache_lock:
            if cache_key in self._cache:
                self._cache[cache_key].append([str(x) for x in data])
            else:
                # Если кэша не было, загружаем всё
                await self.refresh_cache(worksheet_title)
                
        # Получаем новое количество строк
        row_count = len(self._cache[cache_key])
        logger.info(f"Row appended to {cache_key}, total rows: {row_count}")
        
        return row_count
    
    async def update_row(self, row_number: int, data: List[Any], worksheet_title: str = None):
        """Обновление строки (кэш + Google)"""
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # 1. Обновляем в Google
        range_start = f"A{row_number}"
        await loop.run_in_executor(
            None,
            lambda: worksheet.update(range_start, [data])
        )
        
        # 2. Обновляем в кэше
        async with self._cache_lock:
            if cache_key in self._cache:
                # Индекс в списке = номер строки - 1 (так как нумерация в sheets с 1)
                list_index = row_number - 1
                if 0 <= list_index < len(self._cache[cache_key]):
                    # Сохраняем длину строки, дополняем если нужно
                    current_len = len(self._cache[cache_key][list_index])
                    new_row = [str(x) for x in data]
                    # Если новая строка короче, дополняем пустыми строками, чтобы не сломать структуру
                    if len(new_row) < current_len:
                         new_row.extend([""] * (current_len - len(new_row)))
                    
                    self._cache[cache_key][list_index] = new_row
            else:
                 await self.refresh_cache(worksheet_title)
                 
        logger.info(f"Row {row_number} updated in {cache_key}")
    
    async def add_column(self, column_name: str, worksheet_title: str = None) -> bool:
        """Добавление колонки"""
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        
        # Проверяем заголовки через кэш
        headers = await self.get_headers(worksheet_title)
        if column_name in headers:
            return False
        
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # Обновляем в Google
        col_index = len(headers) + 1
        cell = worksheet.cell(1, col_index)
        await loop.run_in_executor(
            None,
            lambda: cell.__setattr__('value', column_name)
        )
        await loop.run_in_executor(None, worksheet.update_cells, [cell])

        # Сбрасываем кэш целиком, так как изменилась структура
        await self.refresh_cache(worksheet_title)
        
        return True
    
    @staticmethod
    def format_date(date_value: Any) -> str:
        """Форматирование даты (без изменений)"""
        if not date_value: return ""
        if isinstance(date_value, datetime): return date_value.strftime("%d.%m.%Y")
        if isinstance(date_value, str):
            formats = ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_value, fmt).strftime("%d.%m.%Y")
                except ValueError: continue
        return str(date_value)

# Глобальный экземпляр
sheets_client = GoogleSheetsClient()