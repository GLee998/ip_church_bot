"""
Основная логика Telegram бота
"""
import logging
import re
from typing import Dict, Any, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import BadRequest

from app.config import settings
from app.sheets import sheets_client
from app.sessions import session_manager
from app.auth import auth_manager

logger = logging.getLogger(__name__)


class TelegramBot:
    """Основной класс бота"""
    
    def __init__(self):
        self.sheets = sheets_client
        self.sessions = session_manager
        self.auth = auth_manager
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Основной обработчик сообщений"""
        if not update.message:
            return
        
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        text = update.message.text
        
        logger.info(f"Message from {user_id}: {text}")
        
        # Проверка доступа
        user_info = {
            'id': user_id,
            'username': update.effective_user.username,
            'first_name': update.effective_user.first_name,
            'last_name': update.effective_user.last_name
        }
        
        if not await self.auth.check_access(user_id, user_info):
            await update.message.reply_text(
                "⛔ <b>Доступ запрещен</b>\n\n"
                "У вас нет прав для использования этого бота.\n\n"
                f"Ваш ID: {user_id}\n"
                "Обратитесь к администратору @Gosha_Lee, чтобы получить доступ.",
                parse_mode='HTML'
            )
            return
        
        # Получаем сессию
        session = await self.sessions.get_session(chat_id)
        session['user_id'] = user_id
        
        # Обработка команд
        if text and text.startswith('/admin'):
            if not await self.auth.is_admin(user_id):
                await update.message.reply_text("❌ У вас нет прав администратора.")
                return
            await self._handle_admin_command(update, context, text, chat_id)
            return
        
        if text in ('/start', '/menu', 'В главное меню'):
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
            return
        
        # Обработка по состоянию сессии
        state = session.get('state', 'IDLE')
        
        if state == 'IDLE':
            await self._handle_idle_state(update, chat_id, text, session)
        elif state == 'ADMIN_MENU':
            await self._handle_admin_menu(update, chat_id, text)
        elif state == 'SELECTING_LETTER':
            await self._handle_letter_selection(update, chat_id, text, session)
        elif state == 'SELECTING_PERSON':
            await self._handle_person_selection(update, chat_id, text, session)
        elif state == 'VIEWING_CARD':
            await self._handle_viewing_card(update, chat_id, text, session)
        elif state == 'BUILDER_MODE':
            await self._handle_builder_mode(update, chat_id, text, session)
        else:
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback-запросов"""
        query = update.callback_query
        try:
            # Строка 95
            await query.answer() 
        except BadRequest as e:
            # Логируем ошибку и игнорируем, если запрос слишком старый.
            # Это предотвратит сбой 500, который мы видели в логах.
            logger.warning(f"⚠️ Failed to answer expired callback query: {e}")
            pass # Продолжаем выполнение, чтобы попытаться обновить сообщение, если это возможно.
        
        chat_id = query.message.chat.id
        data = query.data
        
        logger.info(f"Callback from {chat_id}: {data}")
        
        # Получаем сессию
        session = await self.sessions.get_session(chat_id)
        
        # Обработка различных действий
        if data == "back_to_main":
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        
        elif data.startswith("letter_"):
            letter = data.replace("letter_", "")
            await self._show_people_by_letter(update, chat_id, letter)
        
        elif data.startswith("person_"):
            row_index = int(data.replace("person_", ""))
            
            if session.get('mode') == 'VIEW_ONLY':
                await self._show_read_only_card(update, chat_id, row_index)
            elif session.get('mode') == 'EDIT':
                await self._start_editing(update, chat_id, row_index)
        
        elif data == "back_to_letters":
            await self._show_alphabet(update, chat_id)
        
        elif data == "back_to_people":
            if session.get('last_letter'):
                await self._show_people_by_letter(update, chat_id, session['last_letter'])
            else:
                await self._show_alphabet(update, chat_id)
        
        elif data == "view":
            session['mode'] = 'VIEW_ONLY'
            await self.sessions.save_session(chat_id, session)
            await self._show_alphabet(update, chat_id)
        
        elif data == "edit":
            session['mode'] = 'EDIT'
            await self.sessions.save_session(chat_id, session)
            await self._show_alphabet(update, chat_id)
        
        elif data == "create":
            await self._start_creation(update, chat_id)
        
        elif data == "admin_panel":
            if not await self.auth.is_admin(session.get('user_id', 0)):
                await query.edit_message_text("❌ У вас нет прав администратора.")
                return
            await self._show_admin_menu(update, chat_id)
        
        elif data == "admin_users":
            await self._show_users_list(update, chat_id)
        
        elif data == "admin_stats":
            await self._show_admin_stats(update, chat_id)
        
        elif data == "admin_logs":
            await self._show_access_logs(update, chat_id)
        
        elif data == "back_to_admin":
            await self._show_admin_menu(update, chat_id)
        
        elif data.startswith("edit_field_"):
            field_name = data.replace("edit_field_", "")
            session['step'] = 'WAITING_VALUE'
            session['current_field'] = field_name
            await self.sessions.save_session(chat_id, session)
            
            current_value = session['draft'].get(field_name, "")
            if field_name in settings.date_columns and current_value:
                current_value = self.sheets.format_date(current_value)
            
            message = f"Введите значение для **{self._escape_html(field_name)}**:\n"
            if field_name in settings.date_columns:
                message += "Формат: ДД.ММ.ГГГГ (например: 04.05.1998)\n"
            if current_value:
                message += f"(Текущее: {self._escape_html(str(current_value))})"
            
            await query.edit_message_text(message, parse_mode='HTML')
        
        elif data == "add_category":
            session['step'] = 'WAITING_NEW_CAT'
            await self.sessions.save_session(chat_id, session)
            await query.edit_message_text("Напишите название новой категории:")
        
        elif data == "save_card":
            await self._save_card(update, chat_id, session)
        
        elif data == "cancel_builder":
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        
        else:
            await query.edit_message_text("Неизвестная команда")
    
    # Вспомогательные методы
    async def _send_main_menu(self, update: Update, chat_id: int):
        """Отправка главного меню"""
        session = await self.sessions.get_session(chat_id)
        user_id = session.get('user_id', 0)
        
        keyboard = [
            [InlineKeyboardButton("🔍 Найти / Просмотреть", callback_data="view")],
            [InlineKeyboardButton("✏️ Редактировать карточку", callback_data="edit")],
            [InlineKeyboardButton("➕ Создать карточку", callback_data="create")]
        ]
        
        # Добавляем админ-панель для администраторов
        if await self.auth.is_admin(user_id):
            keyboard.append([InlineKeyboardButton("🛡️ Админ панель", callback_data="admin_panel")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сессию
        session['state'] = 'IDLE'
        await self.sessions.save_session(chat_id, session)
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                "⛪ <b>Церковная база данных</b>\nВыберите действие:",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                "⛪ <b>Церковная база данных</b>\nВыберите действие:",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    
    async def _show_alphabet(self, update: Update, chat_id: int):
        """Показать алфавит для поиска"""
        try:
            data = await self.sheets.get_all_data()
            headers = data[0] if data else []
            name_index = headers.index(settings.col_first_name) if settings.col_first_name in headers else -1
            
            if name_index == -1:
                error_msg = f"⚠️ Ошибка: Нет колонки '{settings.col_first_name}'"
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(error_msg)
                else:
                    await update.message.reply_text(error_msg)
                return
            
            # Собираем буквы
            letters = set()
            for row in data[1:]:
                if name_index < len(row):
                    name = row[name_index]
                    if name and isinstance(name, str):
                        first_char = name[0].upper()
                        if re.match(r'[А-ЯA-Z]', first_char):
                            letters.add(first_char)
            
            if not letters:
                msg = "В базе нет данных. Создайте первую карточку."
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(msg)
                else:
                    await update.message.reply_text(msg)
                await self.sessions.clear_session(chat_id)
                await self._send_main_menu(update, chat_id)
                return
            
            # Создаем клавиатуру
            sorted_letters = sorted(letters)
            keyboard = []
            row = []
            
            for letter in sorted_letters:
                row.append(InlineKeyboardButton(letter, callback_data=f"letter_{letter}"))
                if len(row) == 5:
                    keyboard.append(row)
                    row = []
            
            if row:
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Обновляем сессию
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'SELECTING_LETTER'
            await self.sessions.save_session(chat_id, session)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    "🔤 Выберите первую букву имени:",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text(
                    "🔤 Выберите первую букву имени:",
                    reply_markup=reply_markup
                )
                
        except Exception as e:
            logger.error(f"Error showing alphabet: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_people_by_letter(self, update: Update, chat_id: int, letter: str):
        """Показать людей на выбранную букву"""
        try:
            data = await self.sheets.get_all_data()
            headers = data[0] if data else []
            
            name_idx = headers.index(settings.col_first_name) if settings.col_first_name in headers else -1
            surname_idx = headers.index(settings.col_last_name) if settings.col_last_name in headers else -1
            birth_idx = headers.index(settings.col_birth_date) if settings.col_birth_date in headers else -1
            
            if name_idx == -1:
                error_msg = "❌ Ошибка: не найдена колонка с именами"
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(error_msg)
                else:
                    await update.message.reply_text(error_msg)
                return
            
            # Собираем людей
            people = []
            name_counts = {}
            
            # Считаем тезок
            for i, row in enumerate(data[1:], start=2):
                if name_idx < len(row):
                    name = str(row[name_idx] or "").strip()
                    if name and name.upper().startswith(letter.upper()):
                        surname = str(row[surname_idx] or "").strip() if surname_idx != -1 and surname_idx < len(row) else ""
                        key = f"{name.lower()}_{surname.lower()}"
                        name_counts[key] = name_counts.get(key, 0) + 1
            
            # Формируем список
            for i, row in enumerate(data[1:], start=2):
                if name_idx < len(row):
                    name = str(row[name_idx] or "").strip()
                    if name and name.upper().startswith(letter.upper()):
                        surname = str(row[surname_idx] or "").strip() if surname_idx != -1 and surname_idx < len(row) else ""
                        key = f"{name.lower()}_{surname.lower()}"
                        
                        # Формируем отображаемое имя
                        display_name = f"{name} {surname}".strip()
                        
                        # Добавляем дату рождения если есть тезки
                        if name_counts.get(key, 0) > 1 and birth_idx != -1 and birth_idx < len(row) and row[birth_idx]:
                            birth_date = self.sheets.format_date(row[birth_idx])
                            if birth_date:
                                display_name = f"{name} {surname} (р. {birth_date})"
                        
                        people.append({
                            'text': display_name,
                            'row': i,
                            'display': f"{display_name} [#{i}]"
                        })
            
            if not people:
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(f"Нет имен на букву {letter}")
                else:
                    await update.message.reply_text(f"Нет имен на букву {letter}")
                await self._show_alphabet(update, chat_id)
                return
            
            # Создаем клавиатуру
            keyboard = []
            for person in people:
                keyboard.append([InlineKeyboardButton(person['display'], callback_data=f"person_{person['row']}")])
            
            keyboard.append([InlineKeyboardButton("⬅️ Назад к буквам", callback_data="back_to_letters")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Обновляем сессию
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'SELECTING_PERSON'
            session['last_letter'] = letter
            session['people_list'] = people
            await self.sessions.save_session(chat_id, session)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    "👤 Выберите человека:",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text(
                    "👤 Выберите человека:",
                    reply_markup=reply_markup
                )
                
        except Exception as e:
            logger.error(f"Error showing people by letter: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_read_only_card(self, update: Update, chat_id: int, row_index: int):
        """Показать карточку только для чтения"""
        try:
            data = await self.sheets.get_all_data()
            if row_index > len(data):
                error_msg = "❌ Запись не найдена"
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(error_msg)
                else:
                    await update.message.reply_text(error_msg)
                return
            
            headers = data[0]
            row_data = data[row_index - 1]
            
            message = "📋 <b>Информация о прихожанине:</b>\n\n"
            has_data = False
            
            for i, header in enumerate(headers):
                if i < len(row_data):
                    value = row_data[i]
                    if value and str(value).strip():
                        # Форматируем дату если нужно
                        if header in settings.date_columns:
                            value = self.sheets.format_date(value)
                        
                        message += f"🔹 <b>{header}:</b> {self._escape_html(str(value))}\n"
                        has_data = True
            
            if not has_data:
                message += "(Нет данных)"
            
            keyboard = [
                [InlineKeyboardButton("⬅️ К списку имен", callback_data="back_to_people")],
                [InlineKeyboardButton("🏠 В главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Обновляем сессию
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'VIEWING_CARD'
            session['viewing_row'] = row_index
            await self.sessions.save_session(chat_id, session)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing card: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _start_creation(self, update: Update, chat_id: int):
        """Начать создание новой карточки"""
        session = {
            'state': 'BUILDER_MODE',
            'mode': 'CREATE',
            'draft': {},
            'step': 'MENU',
            'editing_row': None
        }
        await self.sessions.save_session(chat_id, session)
        await self._show_builder_menu(update, chat_id, session)
    
    async def _start_editing(self, update: Update, chat_id: int, row_index: int):
        """Начать редактирование существующей карточки"""
        try:
            data = await self.sheets.get_all_data()
            if row_index > len(data):
                await update.callback_query.edit_message_text("❌ Запись не найдена")
                return
            
            headers = data[0]
            row_data = data[row_index - 1]
            
            # Создаем черновик из текущих данных
            draft = {}
            for i, header in enumerate(headers):
                if i < len(row_data) and row_data[i] and str(row_data[i]).strip():
                    draft[header] = row_data[i]
            
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'BUILDER_MODE'
            session['mode'] = 'EDIT'
            session['draft'] = draft
            session['step'] = 'MENU'
            session['editing_row'] = row_index
            await self.sessions.save_session(chat_id, session)
            
            await self._show_builder_menu(update, chat_id, session)
            
        except Exception as e:
            logger.error(f"Error starting edit: {e}")
            await update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
    
    async def _show_builder_menu(self, update: Update, chat_id: int, session: Dict[str, Any]):
        """Показать меню конструктора"""
        try:
            headers = await self.sheets.get_headers()
            keyboard = []
            
            for header in headers:
                label = header
                if header in session['draft']:
                    value = session['draft'][header]
                    if header in settings.date_columns:
                        value = self.sheets.format_date(value)
                    label = f"✅ {header}: {self._escape_html(str(value))}"
                
                keyboard.append([InlineKeyboardButton(label, callback_data=f"edit_field_{header}")])
            
            keyboard.append([InlineKeyboardButton("➕ Доб. категорию", callback_data="add_category")])
            keyboard.append([
                InlineKeyboardButton("💾 СОХРАНИТЬ", callback_data="save_card"),
                InlineKeyboardButton("❌ Отмена", callback_data="cancel_builder")
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            mode_text = "создания" if session['mode'] == 'CREATE' else "редактирования"
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    f"📝 <b>Режим {mode_text}</b>\nНажмите на категорию, чтобы изменить её:",
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    f"📝 <b>Режим {mode_text}</b>\nНажмите на категорию, чтобы изменить её:",
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing builder menu: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _save_card(self, update: Update, chat_id: int, session: Dict[str, Any]):
        """Сохранение карточки"""
        try:
            headers = await self.sheets.get_headers()
            row_data = []
            
            for header in headers:
                value = session['draft'].get(header, "")
                
                # Форматируем даты для Google Sheets
                if header in settings.date_columns and value:
                    # Если дата в формате ДД.ММ.ГГГГ, конвертируем
                    if isinstance(value, str) and re.match(r'^\d{1,2}\.\d{1,2}\.\d{4}$', value):
                        try:
                            day, month, year = map(int, value.split('.'))
                            value = f"{year}-{month:02d}-{day:02d}"
                        except:
                            pass
                
                row_data.append(value)
            
            if session['mode'] == 'CREATE':
                await self.sheets.append_row(row_data)
                message = "✅ Карточка успешно создана!"
            else:
                row_index = session['editing_row']
                await self.sheets.update_row(row_index, row_data)
                message = "✅ Данные обновлены!"
            
            await self.sessions.clear_session(chat_id)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(message)
                await self._send_main_menu(update, chat_id)
                
        except Exception as e:
            logger.error(f"Error saving card: {e}")
            error_msg = f"❌ Ошибка при сохранении: {e}"
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _handle_admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                  text: str, chat_id: int):
        """Обработка административных команд"""
        if text == '/admin':
            await self._show_admin_menu(update, chat_id)
        elif text == '/admin users':
            await self._show_users_list(update, chat_id)
        elif text == '/admin logs':
            await self._show_access_logs(update, chat_id)
        elif text == '/admin stats':
            await self._show_admin_stats(update, chat_id)
        elif text == '/admin reload':
            await update.message.reply_text("🔄 Обновляю кэш базы данных...")
            try:
                # Сбрасываем кэш основного листа
                count = await self.sheets.refresh_cache()
                # Сбрасываем кэш пользователей и логов
                auth_manager._users_cache = None 
                auth_manager._logs_cache = None
                
                await update.message.reply_text(f"✅ База данных обновлена!\nЗагружено записей: {count}")
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка обновления: {e}")
            return
        elif text.startswith('/admin add '):
            args = text.split()
            if len(args) < 3:
                await update.message.reply_text("Использование: /admin add USER_ID [admin/user]")
                return
            
            new_user_id = args[2]
            user_type = args[3] if len(args) > 3 else "user"
            
            # Получаем информацию о пользователе
            try:
                user_info = await context.bot.get_chat(new_user_id)
                result = await self.auth.add_user(
                    int(new_user_id),
                    user_info.username,
                    user_info.first_name,
                    user_info.last_name,
                    user_type
                )
            except Exception as e:
                logger.error(f"Error getting user info: {e}")
                result = await self.auth.add_user(int(new_user_id), "", "", "", user_type)
            
            await update.message.reply_text(result)
        elif text.startswith('/admin remove '):
            args = text.split()
            if len(args) < 3:
                await update.message.reply_text("Использование: /admin remove USER_ID")
                return
            
            remove_user_id = args[2]
            result = await self.auth.remove_user(int(remove_user_id))
            await update.message.reply_text(result)
        else:
            await update.message.reply_text(
                "📋 <b>Доступные команды админа:</b>\n\n"
                "<code>/admin</code> - Админ панель\n"
                "<code>/admin users</code> - Список пользователей\n"
                "<code>/admin logs</code> - Логи доступа\n"
                "<code>/admin stats</code> - Статистика\n"
                "<code>/admin add USER_ID</code> - Добавить пользователя\n"
                "<code>/admin remove USER_ID</code> - Удалить пользователя"
                "<code>/admin reload</code> - Обновить базу из Google",
                parse_mode='HTML'
            )
    
    async def _show_admin_menu(self, update: Update, chat_id: int):
        """Показать админ-меню"""
        keyboard = [
            [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")],
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("📋 Последние логи", callback_data="admin_logs")],
            [InlineKeyboardButton("➕ Добавить пользователя", callback_data="admin_add_user")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сессию
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'ADMIN_MENU'
        await self.sessions.save_session(chat_id, session)
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                "🛡️ <b>Админ панель</b>\nВыберите действие:",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                "🛡️ <b>Админ панель</b>\nВыберите действие:",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    
    async def _show_users_list(self, update: Update, chat_id: int):
        """Показать список пользователей"""
        try:
            users_list = await self.auth.get_users_list()
            
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад в админ-панель", callback_data="back_to_admin")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    users_list,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    users_list,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing users list: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_admin_stats(self, update: Update, chat_id: int):
        """Показать статистику"""
        try:
            stats = await self.auth.get_stats()
            
            message = "📊 <b>Статистика системы</b>\n\n"
            
            if 'database' in stats:
                message += "📁 <b>База данных:</b>\n"
                message += f"   📝 Записей: {stats['database'].get('records', 0)}\n"
                message += f"   🏷️ Категорий: {stats['database'].get('columns', 0)}\n\n"
            
            if 'users' in stats:
                message += "👥 <b>Пользователи:</b>\n"
                message += f"   👑 Админов: {stats['users'].get('admins', 0)}\n"
                message += f"   👤 Пользователей: {stats['users'].get('regular', 0)}\n\n"
            
            if 'logs' in stats:
                message += "📋 <b>Логи доступа:</b>\n"
                message += f"   ✅ Успешных: {stats['logs'].get('granted', 0)}\n"
                message += f"   ❌ Отказов: {stats['logs'].get('denied', 0)}\n"
            
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад в админ-панель", callback_data="back_to_admin")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing admin stats: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_access_logs(self, update: Update, chat_id: int):
        """Показать логи доступа"""
        try:
            logs_data = await self.sheets.get_all_data("AccessLog")
            
            if not logs_data or len(logs_data) <= 1:
                message = "📭 Логи доступа отсутствуют."
            else:
                message = "📋 <b>Последние 10 попыток доступа</b>\n\n"
                
                # Берем последние 10 записей
                start = max(1, len(logs_data) - 10)
                
                for i in range(start, len(logs_data)):
                    log = logs_data[i]
                    try:
                        from datetime import datetime
                        date_str = log[0]
                        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        formatted_date = date_obj.strftime("%d.%m.%y %H:%M")
                        
                        message += f"<b>{formatted_date}</b>\n"
                        message += f"ID: <code>{log[1] if len(log) > 1 else 'N/A'}</code>\n"
                        message += f"Имя: {log[3] if len(log) > 3 else 'Не указано'}\n"
                        status = log[5] if len(log) > 5 else ""
                        message += f"Статус: {'❌ Отказано' if status == 'DENIED' else '✅ Разрешено'}\n"
                        message += "---\n"
                    except:
                        continue
            
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад в админ-панель", callback_data="back_to_admin")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing access logs: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    # Обработчики состояний
    async def _handle_idle_state(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка состояния IDLE"""
        if text == '🛡️ Админ панель':
            if not await self.auth.is_admin(session['user_id']):
                await update.message.reply_text("❌ У вас нет прав администратора.")
                return
            await self._show_admin_menu(update, chat_id)
        elif 'Создать карточку' in text or text == '/create':
            await self._start_creation(update, chat_id)
        elif 'Найти' in text or 'Просмотреть' in text or text == '/view':
            session['mode'] = 'VIEW_ONLY'
            await self.sessions.save_session(chat_id, session)
            await self._show_alphabet(update, chat_id)
        elif 'Редактировать' in text or text == '/edit':
            session['mode'] = 'EDIT'
            await self.sessions.save_session(chat_id, session)
            await self._show_alphabet(update, chat_id)
        else:
            await self._send_main_menu(update, chat_id)
    
    async def _handle_admin_menu(self, update: Update, chat_id: int, text: str):
        """Обработка админ меню"""
        if text == '👥 Список пользователей':
            await self._show_users_list(update, chat_id)
        elif text == '📊 Статистика':
            await self._show_admin_stats(update, chat_id)
        elif text == '📋 Последние логи':
            await self._show_access_logs(update, chat_id)
        elif text == '🏠 Главное меню':
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        else:
            await self._show_admin_menu(update, chat_id)
    
    async def _handle_letter_selection(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка выбора буквы"""
        if text == '⬅️ Назад':
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
            return
        
        # Проверяем, что текст - это одна буква
        if text and len(text) == 1 and re.match(r'^[А-Яа-яA-Za-z]$', text):
            await self._show_people_by_letter(update, chat_id, text.upper())
        else:
            await self._show_alphabet(update, chat_id)
    
    async def _handle_person_selection(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка выбора человека"""
        if text == '⬅️ Назад к буквам':
            await self._show_alphabet(update, chat_id)
            return
        
        # Извлекаем ID из текста
        match = re.search(r'\[#(\d+)\]$', text)
        if match:
            row_index = int(match.group(1))
            if session.get('mode') == 'VIEW_ONLY':
                await self._show_read_only_card(update, chat_id, row_index)
            elif session.get('mode') == 'EDIT':
                await self._start_editing(update, chat_id, row_index)
        else:
            await update.message.reply_text("❌ Человек не найден (возможно, удален).")
            if session.get('last_letter'):
                await self._show_people_by_letter(update, chat_id, session['last_letter'])
    
    async def _handle_viewing_card(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка просмотра карточки"""
        if text == '⬅️ К списку имен':
            if session.get('last_letter'):
                await self._show_people_by_letter(update, chat_id, session['last_letter'])
            else:
                await self._show_alphabet(update, chat_id)
        elif text == '🏠 В главное меню':
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        else:
            # Показываем ту же карточку
            if session.get('viewing_row'):
                await self._show_read_only_card(update, chat_id, session['viewing_row'])
    
    async def _handle_builder_mode(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка режима конструктора"""
        if session['step'] == 'MENU':
            if text == '❌ Отмена':
                await self.sessions.clear_session(chat_id)
                await self._send_main_menu(update, chat_id)
            elif text == '➕ Доб. категорию':
                session['step'] = 'WAITING_NEW_CAT'
                await self.sessions.save_session(chat_id, session)
                await update.message.reply_text("Напишите название новой категории:")
            else:
                # Проверяем, является ли текст названием поля
                headers = await self.sheets.get_headers()
                for header in headers:
                    if text.startswith(header) or text.startswith(f"✅ {header}"):
                        session['step'] = 'WAITING_VALUE'
                        session['current_field'] = header
                        await self.sessions.save_session(chat_id, session)
                        
                        current_value = session['draft'].get(header, "")
                        if header in settings.date_columns and current_value:
                            current_value = self.sheets.format_date(current_value)
                        
                        message = f"Введите значение для **{self._escape_html(header)}**:\n"
                        if header in settings.date_columns:
                            message += "Формат: ДД.ММ.ГГГГ (например: 04.05.1998)\n"
                        if current_value:
                            message += f"(Текущее: {self._escape_html(str(current_value))})"
                        
                        await update.message.reply_text(message, parse_mode='HTML')
                        return
                
                await self._show_builder_menu(update, chat_id, session)
        
        elif session['step'] == 'WAITING_VALUE':
            field_name = session.get('current_field')
            if field_name:
                session['draft'][field_name] = text
                session['step'] = 'MENU'
                session['current_field'] = None
                await self.sessions.save_session(chat_id, session)
                await self._show_builder_menu(update, chat_id, session)
        
        elif session['step'] == 'WAITING_NEW_CAT':
            if text and text.strip():
                # Проверяем, нет ли уже такой категории
                headers = await self.sheets.get_headers()
                if text.strip() in headers:
                    await update.message.reply_text(f"❌ Категория '{text}' уже существует!")
                else:
                    await self.sheets.add_column(text.strip())
                    await update.message.reply_text(f"✅ Категория '{text}' добавлена!")
                
                session['step'] = 'MENU'
                await self.sessions.save_session(chat_id, session)
                await self._show_builder_menu(update, chat_id, session)
    
    @staticmethod
    def _escape_html(text: str) -> str:
        """Экранирование HTML-символов"""
        if not text:
            return ""
        
        return (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#039;')
                .replace('\t', '    ')
                .replace('\n', '<br>'))


# Глобальный экземпляр бота
bot = TelegramBot()