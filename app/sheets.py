"""
Асинхронный клиент для Google Sheets
"""
import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import gspread
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.auth import default as google_default

from app.config import settings

logger = logging.getLogger(__name__)


class GoogleSheetsClient:
    """Клиент для работы с Google Sheets"""
    
    def __init__(self):
        self._client = None
        self._spreadsheet = None
        self._lock = asyncio.Lock()
        self._worksheets = {}
    
    async def _get_client(self):
        """Получение или создание клиента"""
        async with self._lock:
            if self._client is None:
                try:
                    logger.info("Initializing Google Sheets client...")
                    
                    # Способ 1: Используем сервисный аккаунт из файла (для разработки)
                    if settings.google_credentials_file:
                        try:
                            # В Cloud Run файл может быть в секретах
                            credentials = service_account.Credentials.from_service_account_file(
                                settings.google_credentials_file,
                                scopes=['https://www.googleapis.com/auth/spreadsheets',
                                       'https://www.googleapis.com/auth/drive']
                            )
                            logger.info(f"Using service account file: {settings.google_credentials_file}")
                        except Exception as file_error:
                            logger.warning(f"Cannot load credentials file: {file_error}. Trying default credentials.")
                            # Fallback to default credentials
                            credentials, _ = google_default()
                    
                    # Способ 2: Используем default credentials (для Cloud Run)
                    else:
                        logger.info("Using default Google credentials")
                        credentials, _ = google_default()
                    
                    # Создаем клиент в thread pool
                    loop = asyncio.get_event_loop()
                    self._client = await loop.run_in_executor(
                        None, 
                        lambda: gspread.authorize(credentials)
                    )
                    
                    logger.info("Google Sheets client authorized")
                    
                except Exception as e:
                    logger.error(f"Failed to initialize Google Sheets client: {e}")
                    raise
        
        return self._client
    
    async def _get_spreadsheet(self):
        """Получение таблицы"""
        if self._spreadsheet is None:
            client = await self._get_client()
            loop = asyncio.get_event_loop()
            
            try:
                self._spreadsheet = await loop.run_in_executor(
                    None,
                    lambda: client.open_by_key(settings.sheet_id)
                )
                logger.info(f"Spreadsheet opened: {self._spreadsheet.title}")
            except Exception as e:
                logger.error(f"Failed to open spreadsheet: {e}")
                raise
        
        return self._spreadsheet
    
    async def get_worksheet(self, title: str = None):
        """Получение листа"""
        spreadsheet = await self._get_spreadsheet()
        loop = asyncio.get_event_loop()
        
        if title is None:
            # Главный лист - используем свойство sheet1
            return await loop.run_in_executor(
                None,
                lambda: spreadsheet.sheet1  # Убрали скобки!
            )
        
        # Кэшируем листы
        if title not in self._worksheets:
            try:
                worksheet = await loop.run_in_executor(
                    None,
                    lambda: spreadsheet.worksheet(title)
                )
                self._worksheets[title] = worksheet
            except gspread.exceptions.WorksheetNotFound:
                # Создаем новый лист
                worksheet = await loop.run_in_executor(
                    None,
                    lambda: spreadsheet.add_worksheet(title=title, rows=1000, cols=26)
                )
                self._worksheets[title] = worksheet
                logger.info(f"📄 Created new worksheet: {title}")
        
        return self._worksheets[title]
    
    async def get_all_data(self, worksheet_title: str = None) -> List[List[Any]]:
        """Получение всех данных с листа"""
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        return await loop.run_in_executor(None, worksheet.get_all_values)
    
    async def get_headers(self, worksheet_title: str = None) -> List[str]:
        """Получение заголовков"""
        data = await self.get_all_data(worksheet_title)
        return data[0] if data else []
    
    async def find_rows(self, column: str, value: str, worksheet_title: str = None) -> List[Dict[str, Any]]:
        """Поиск строк по значению в колонке"""
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # Получаем все данные
        all_data = await self.get_all_data(worksheet_title)
        if not all_data or len(all_data) < 2:
            return []
        
        headers = all_data[0]
        
        # Ищем индекс колонки
        try:
            col_index = headers.index(column)
        except ValueError:
            logger.warning(f"Column '{column}' not found in worksheet")
            return []
        
        # Фильтруем строки
        results = []
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) > col_index and str(row[col_index]).strip().lower() == value.strip().lower():
                result = {"row_number": i, "data": {}}
                for j, header in enumerate(headers):
                    if j < len(row):
                        result["data"][header] = row[j]
                results.append(result)
        
        return results
    
    async def append_row(self, data: List[Any], worksheet_title: str = None) -> int:
        """Добавление новой строки"""
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        await loop.run_in_executor(None, worksheet.append_row, data)
        
        # Получаем обновленное количество строк
        row_count = await loop.run_in_executor(None, worksheet.row_count)
        logger.info(f"Row appended to {worksheet_title or 'main sheet'}, total rows: {row_count}")
        
        return row_count
    
    async def update_row(self, row_number: int, data: List[Any], worksheet_title: str = None):
        """Обновление строки"""
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # Обновляем строку
        range_start = f"A{row_number}"
        await loop.run_in_executor(
            None,
            lambda: worksheet.update(range_start, [data])
        )
        logger.info(f"Row {row_number} updated in {worksheet_title or 'main sheet'}")
    
    async def add_column(self, column_name: str, worksheet_title: str = None) -> bool:
        """Добавление новой колонки"""
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # Получаем текущие заголовки
        headers = await self.get_headers(worksheet_title)
        
        # Проверяем, нет ли уже такой колонки
        if column_name in headers:
            logger.warning(f"Column '{column_name}' already exists")
            return False
        
        # Добавляем новую колонку
        col_index = len(headers) + 1  # +1 потому что столбцы начинаются с 1
        cell = worksheet.cell(1, col_index)
        
        await loop.run_in_executor(
            None,
            lambda: cell.__setattr__('value', column_name)
        )
        
        logger.info(f"Column '{column_name}' added at position {col_index}")
        return True
    
    @staticmethod
    def format_date(date_value: Any) -> str:
        """Форматирование даты для отображения"""
        if not date_value:
            return ""
        
        # Если это объект datetime
        if isinstance(date_value, datetime):
            return date_value.strftime("%d.%m.%Y")
        
        # Если это строка
        if isinstance(date_value, str):
            # Пробуем разные форматы
            date_formats = [
                "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y",
                "%Y/%m/%d", "%d-%m-%Y", "%Y.%m.%d"
            ]
            
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    return dt.strftime("%d.%m.%Y")
                except ValueError:
                    continue
        
        # Если не удалось распарсить, возвращаем как есть
        return str(date_value)


# Глобальный экземпляр клиента
sheets_client = GoogleSheetsClient()