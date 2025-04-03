import sys
import asyncio
from asyncio import Queue, Semaphore, Lock
import aiohttp
import asyncpg
import ctypes
from datetime import datetime, timedelta, timezone
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QDateTimeEdit, QPushButton, QLineEdit, QTableWidget,
                             QDialog, QFormLayout, QMessageBox, QTableWidgetItem, QHeaderView,
                             QStyledItemDelegate, QStyleOptionProgressBar, QStyle, QProgressBar,
                             QStatusBar)
from PyQt5.QtCore import Qt, QSettings, QTimer, pyqtSignal
from PyQt5.QtGui import QKeyEvent, QIntValidator
from qasync import QEventLoop, asyncSlot
import logging
logging.basicConfig(filename='downloader.log', level=logging.INFO)

# Настройки для Windows
if sys.platform == 'win32':
    ctypes.windll.kernel32.SetDllDirectoryW(None)

class ProgressBarDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        progress_data = index.data(Qt.DisplayRole)
        end_date = index.data(Qt.UserRole)
        
        if progress_data is not None and isinstance(progress_data, dict):
            progress = progress_data.get('progress', 0)
            completed = progress_data.get('completed', 0)
            total = progress_data.get('total', 1)
            
            opt = QStyleOptionProgressBar()
            opt.rect = option.rect
            opt.minimum = 0
            opt.maximum = 100
            opt.progress = progress
            opt.textVisible = False
            QApplication.style().drawControl(QStyle.CE_ProgressBar, opt, painter)
            
            painter.save()
            text = f"{progress}%\n{completed:,}/{total:,} min"
            if end_date:
                text += f"\n{end_date.strftime('%d.%m.%y %H:%M')}"
            
            font = painter.font()
            font.setPointSize(8)
            painter.setFont(font)
            painter.setPen(Qt.black)
            text_rect = option.rect.adjusted(2, 2, -2, -2)
            flags = Qt.AlignCenter | Qt.TextWordWrap
            painter.drawText(text_rect, flags, text)
            painter.restore()
        else:
            super().paint(painter, option, index)

    def sizeHint(self, option, index):
        size = super().sizeHint(option, index)
        size.setHeight(size.height() * 2)
        return size
    
class NumericTableWidgetItem(QTableWidgetItem):
    def __lt__(self, other):
        try:
            return float(self.text().replace(" ", "")) < float(other.text().replace(" ", ""))
        except ValueError:
            return super().__lt__(other)

class SortableTableWidget(QTableWidget):
    headerClicked = pyqtSignal(int)
    shiftSelectionRequested = pyqtSignal(int, int)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.horizontalHeader().sectionClicked.connect(self.headerClicked.emit)
        self.setSortingEnabled(True)
        self.sort_order = {}
        self.last_selected_row = -1
        self.setSelectionMode(QTableWidget.MultiSelection)
        self.setSelectionBehavior(QTableWidget.SelectRows)
        self.loop = asyncio.get_event_loop()

    def mousePressEvent(self, event):
        if event.modifiers() & Qt.ShiftModifier:
            row = self.rowAt(event.y())
            if row >= 0 and self.last_selected_row >= 0:
                from_row = min(row, self.last_selected_row)
                to_row = max(row, self.last_selected_row)
                self.shiftSelectionRequested.emit(from_row, to_row)
                return
        else:
            row = self.rowAt(event.y())
            if row >= 0:
                self.last_selected_row = row
        
        super().mousePressEvent(event)

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки")
        self.setModal(True)
        
        layout = QFormLayout()
        
        self.host_edit = QLineEdit()
        self.port_edit = QLineEdit()
        self.user_edit = QLineEdit()
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.database_edit = QLineEdit()
        self.schema_edit = QLineEdit()
        self.threads_edit = QLineEdit()
        self.threads_edit.setValidator(QIntValidator(1, 100, self))
        
        layout.addRow("PostgreSQL Хост:", self.host_edit)
        layout.addRow("PostgreSQL Порт:", self.port_edit)
        layout.addRow("PostgreSQL Пользователь:", self.user_edit)
        layout.addRow("PostgreSQL Пароль:", self.password_edit)
        layout.addRow("PostgreSQL База данных:", self.database_edit)
        layout.addRow("Схема для данных:", self.schema_edit)
        layout.addRow("Число потоков скачивания:", self.threads_edit)
        
        buttons = QHBoxLayout()
        save_btn = QPushButton("Сохранить")
        save_btn.clicked.connect(self.save_settings)
        buttons.addWidget(save_btn)
        
        cancel_btn = QPushButton("Отмена")
        cancel_btn.clicked.connect(self.reject)
        buttons.addWidget(cancel_btn)
        
        layout.addRow(buttons)
        self.setLayout(layout)
        
        self.load_settings()
    
    def load_settings(self):
        settings = QSettings("settings.ini", QSettings.IniFormat)
        self.host_edit.setText(settings.value("postgres/host", ""))
        self.port_edit.setText(settings.value("postgres/port", "5432"))
        self.user_edit.setText(settings.value("postgres/user", ""))
        self.password_edit.setText(settings.value("postgres/password", ""))
        self.database_edit.setText(settings.value("postgres/database", ""))
        self.schema_edit.setText(settings.value("settings/schema", "bybit_data"))
        self.threads_edit.setText(settings.value("settings/threads", "5"))
    
    def save_settings(self):
        settings = QSettings("settings.ini", QSettings.IniFormat)
        settings.setValue("postgres/host", self.host_edit.text())
        settings.setValue("postgres/port", self.port_edit.text())
        settings.setValue("postgres/user", self.user_edit.text())
        settings.setValue("postgres/password", self.password_edit.text())
        settings.setValue("postgres/database", self.database_edit.text())
        settings.setValue("settings/schema", self.schema_edit.text())
        settings.setValue("settings/threads", self.threads_edit.text())
        self.accept()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Загрузчик данных ByBit")
        self.setGeometry(100, 100, 1000, 600)
        
        self.settings = QSettings("settings.ini", QSettings.IniFormat)
        self.tickers = []
        self.selected_tickers = set()
        self.shutdown = False
        self.all_tickers_data = []
        self.current_sort_column = 3
        self.current_sort_order = Qt.DescendingOrder
        self.download_progress = {}
        self.total_tasks = 0
        self.completed_tasks = 0
        self.timers = []
        self.pool = None
        self.download_threads = int(self.settings.value("settings/threads", "5"))
        self.total_minutes = 0
        self.completed_minutes = 0
        self.progress_update_timer = None
        self.calculated_tickers = 0
        self.total_tickers_to_calculate = 0
        self.active_threads = 0
       
        self.init_ui()
        self.refresh_tickers()
        self.load_selected_tickers()
        
        self.add_timer(1000, self.update_progress_bars)
    
    def update_status_bar(self, message):
        """Обновление строки состояния"""
        self.status_bar.showMessage(message)
        
    def update_calculation_progress(self):
        """Обновление прогресса расчета в строке состояния"""
        if self.total_tickers_to_calculate > 0:
            self.update_status_bar(
                f"Расчет минут: {self.calculated_tickers}/{self.total_tickers_to_calculate} тикеров | "
                f"Активных потоков: {self.active_threads}"
            )
        else:
            self.update_status_bar(f"Активных потоков: {self.active_threads}")

    def update_progress_bars(self):
        """Обновление прогресс-баров"""
        for row in range(self.tickers_table.rowCount()):
            item = self.tickers_table.item(row, 0)
            if item is not None:  # Добавляем проверку на None
                symbol = item.text()
                if symbol in self.download_progress:
                    progress_item = self.tickers_table.item(row, 4)
                    if progress_item is not None:  # Добавляем проверку на None
                        progress = self.download_progress[symbol]
                        progress_item.setData(Qt.DisplayRole, progress)
        
        self.update_global_progress()
        self.tickers_table.viewport().update()

    def add_timer(self, interval, callback):
        """Создание таймера"""
        timer = QTimer(self)
        timer.timeout.connect(callback)
        timer.start(interval)
        self.timers.append(timer)
        return timer
    
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout()
        
        # Период загрузки
        period_layout = QHBoxLayout()
        period_layout.addWidget(QLabel("С:"))
        self.from_datetime = QDateTimeEdit()
        self.from_datetime.setDateTime(datetime.now() - timedelta(days=7))
        self.from_datetime.setDisplayFormat("yyyy-MM-dd HH:mm")
        period_layout.addWidget(self.from_datetime)
        
        period_layout.addWidget(QLabel("По:"))
        self.to_datetime = QDateTimeEdit()
        self.to_datetime.setDateTime(datetime.now())
        self.to_datetime.setDisplayFormat("yyyy-MM-dd HH:mm")
        period_layout.addWidget(self.to_datetime)
        
        layout.addLayout(period_layout)
        
        # Фильтр тикеров
        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("Фильтр тикеров:"))
        
        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("Введите часть тикера для фильтрации")
        filter_layout.addWidget(self.filter_edit)
        
        self.filter_btn = QPushButton("Фильтр")
        self.filter_btn.clicked.connect(self.apply_filter)
        filter_layout.addWidget(self.filter_btn)
        
        layout.addLayout(filter_layout)
        
        # Таблица тикеров
        tickers_layout = QHBoxLayout()
        
        left_side = QVBoxLayout()
        left_side.addWidget(QLabel("Тикеры:"))
        
        self.tickers_table = SortableTableWidget()
        self.tickers_table.setColumnCount(5)
        self.tickers_table.setHorizontalHeaderLabels(["Тикер", "Объем", "Изменение (%)", "Оборот (24h)", "Прогресс"])
        self.tickers_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tickers_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tickers_table.headerClicked.connect(self.on_header_clicked)
        self.tickers_table.shiftSelectionRequested.connect(self.handle_shift_selection)
        self.tickers_table.setItemDelegateForColumn(4, ProgressBarDelegate())
        left_side.addWidget(self.tickers_table)
        
        buttons_layout = QHBoxLayout()
        self.refresh_btn = QPushButton("Обновить")
        self.refresh_btn.clicked.connect(lambda: asyncio.create_task(self.refresh_tickers()))
        buttons_layout.addWidget(self.refresh_btn)
        
        self.save_tickers_btn = QPushButton("Сохранить тикеры")
        self.save_tickers_btn.clicked.connect(self.save_selected_tickers)
        buttons_layout.addWidget(self.save_tickers_btn)
        
        self.filter_selected_btn = QPushButton("Только выделенное")
        self.filter_selected_btn.clicked.connect(self.filter_selected_rows)
        buttons_layout.addWidget(self.filter_selected_btn)
        
        left_side.addLayout(buttons_layout)
        tickers_layout.addLayout(left_side)
        
        # Кнопки управления выбором
        right_side = QVBoxLayout()
        right_side.addStretch()
        
        self.select_all_btn = QPushButton("Пометить все")
        self.select_all_btn.clicked.connect(self.select_all)
        right_side.addWidget(self.select_all_btn)
        
        self.deselect_all_btn = QPushButton("Снять все")
        self.deselect_all_btn.clicked.connect(self.deselect_all)
        right_side.addWidget(self.deselect_all_btn)
        
        self.invert_selection_btn = QPushButton("Инверсия")
        self.invert_selection_btn.clicked.connect(self.invert_selection)
        right_side.addWidget(self.invert_selection_btn)
        
        right_side.addStretch()
        tickers_layout.addLayout(right_side)
        
        layout.addLayout(tickers_layout)
        
        # Общий прогресс бар
        self.global_progress = QProgressBar()
        self.global_progress.setRange(0, 100)
        self.global_progress.setTextVisible(True)
        layout.addWidget(self.global_progress)
        
        # Основные кнопки
        buttons_layout = QHBoxLayout()
        self.load_btn = QPushButton("Загрузить")
        self.load_btn.clicked.connect(lambda: asyncio.create_task(self.start_loading()))
        buttons_layout.addWidget(self.load_btn)
        
        self.stop_btn = QPushButton("Остановить")
        self.stop_btn.clicked.connect(self.stop_loading)
        self.stop_btn.setEnabled(False)
        buttons_layout.addWidget(self.stop_btn)
        
        self.settings_btn = QPushButton("Настройка")
        self.settings_btn.clicked.connect(self.open_settings)
        buttons_layout.addWidget(self.settings_btn)
        
        self.close_btn = QPushButton("Закрыть")
        self.close_btn.clicked.connect(self.close)
        buttons_layout.addWidget(self.close_btn)
        
        layout.addLayout(buttons_layout)
        
        # Строка состояния
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.update_status_bar("Готово")
        
        central_widget.setLayout(layout)

        self.progress_update_timer = self.add_timer(1000, self.update_progress_bars)
    
    def closeEvent(self, event):
        self.shutdown = True
        
        for timer in self.timers:
            timer.stop()
            timer.deleteLater()
        self.timers.clear()
        
        if self.pool is not None:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.pool.close())
            except:
                pass
        
        super().closeEvent(event)
    
    def stop_loading(self):
        self.shutdown = True
        self.stop_btn.setEnabled(False)
        self.load_btn.setEnabled(True)
        
        for task in asyncio.all_tasks():
            if not task.done():
                task.cancel()
        
        self.update_status_bar("Загрузка остановлена")
        QMessageBox.information(self, "Остановлено", "Загрузка данных была остановлена")
    
    def handle_shift_selection(self, from_row, to_row):
        self.tickers_table.clearSelection()
        for row in range(from_row, to_row + 1):
            for col in range(self.tickers_table.columnCount()):
                item = self.tickers_table.item(row, col)
                if item:
                    item.setSelected(True)
    
    def on_header_clicked(self, logical_index):
        if logical_index == self.current_sort_column:
            self.current_sort_order = (
                Qt.AscendingOrder if self.current_sort_order == Qt.DescendingOrder 
                else Qt.DescendingOrder
            )
        else:
            self.current_sort_column = logical_index
            self.current_sort_order = Qt.DescendingOrder
        
        self.display_tickers(self.all_tickers_data)
    
    def select_all(self):
        self.tickers_table.selectAll()
    
    def deselect_all(self):
        self.tickers_table.clearSelection()
    
    def invert_selection(self):
        for row in range(self.tickers_table.rowCount()):
            if self.tickers_table.item(row, 0).isSelected():
                self.tickers_table.item(row, 0).setSelected(False)
            else:
                self.tickers_table.item(row, 0).setSelected(True)
    
    def apply_filter(self):
        filter_text = self.filter_edit.text().strip().upper()
        
        if not filter_text:
            self.display_tickers(self.all_tickers_data)
            return
        
        filtered_tickers = [
            ticker for ticker in self.all_tickers_data 
            if filter_text in ticker['symbol']
        ]
        self.display_tickers(filtered_tickers)
    
    def filter_selected_rows(self):
        selected_rows = set()
        
        for row in range(self.tickers_table.rowCount()):
            if any(self.tickers_table.item(row, col).isSelected() for col in range(self.tickers_table.columnCount())):
                selected_rows.add(row)
        
        if not selected_rows:
            QMessageBox.information(self, "Информация", "Нет выделенных строк")
            return
        
        filtered_data = []
        for row in selected_rows:
            symbol = self.tickers_table.item(row, 0).text()
            original_data = next((t for t in self.all_tickers_data if t['symbol'] == symbol), None)
            if original_data:
                filtered_data.append(original_data)
        
        self.display_tickers(filtered_data)
        
        QMessageBox.information(self, "Фильтр", f"Оставлено {len(filtered_data)} выделенных тикеров")
    
    def display_tickers(self, tickers_data):
        self.tickers_table.setRowCount(len(tickers_data))
        
        for row, ticker in enumerate(tickers_data):
            symbol = ticker['symbol']
            volume = ticker['volume24h']
            change = ticker['price24hPcnt']
            turnover = ticker.get('turnover24h', '0')
            
            self.tickers_table.setItem(row, 0, QTableWidgetItem(symbol))
            
            try:
                volume_rounded = int(round(float(volume)))
                volume_formatted = "{:,}".format(volume_rounded).replace(",", " ")
            except (ValueError, TypeError):
                volume_formatted = volume
                
            volume_item = NumericTableWidgetItem(volume_formatted)
            volume_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.tickers_table.setItem(row, 1, volume_item)
            
            change_item = QTableWidgetItem(f"{float(change)*100:.2f}%")
            change_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)  # Исправлено здесь
            self.tickers_table.setItem(row, 2, change_item)
            
            try:
                turnover_rounded = int(round(float(turnover)))
                turnover_formatted = "{:,}".format(turnover_rounded).replace(",", " ")
            except (ValueError, TypeError):
                turnover_formatted = turnover
                
            turnover_item = NumericTableWidgetItem(turnover_formatted)
            turnover_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.tickers_table.setItem(row, 3, turnover_item)
            
            progress_item = QTableWidgetItem()
            progress_data = self.download_progress.get(symbol, {'progress': 0, 'completed': 0, 'total': 1})
            progress_item.setData(Qt.DisplayRole, progress_data)
            self.tickers_table.setItem(row, 4, progress_item)

            if symbol in self.selected_tickers:
                for col in range(5):
                    item = self.tickers_table.item(row, col)
                    if item is not None:
                        item.setSelected(True)
        
        self.tickers_table.sortItems(self.current_sort_column, self.current_sort_order)
        
    def load_selected_tickers(self):
        """Загружает сохраненные тикеры из настроек"""
        selected = self.settings.value("selected_tickers", [])
        if isinstance(selected, str):
            selected = [selected] if selected else []
        self.selected_tickers = set(selected)    
    
    def save_selected_tickers(self):
        """Сохраняет выбранные тикеры в настройки"""
        selected = []
        for row in range(self.tickers_table.rowCount()):
            if self.tickers_table.item(row, 0).isSelected():
                selected.append(self.tickers_table.item(row, 0).text())
        
        self.selected_tickers = set(selected)
        self.settings.setValue("selected_tickers", selected)
        self.update_status_bar(f"Сохранено {len(selected)} тикеров")
        QMessageBox.information(self, "Сохранено", "Выбранные тикеры сохранены")    

    def open_settings(self):
        dialog = SettingsDialog(self)
        dialog.exec_()
    
    def update_progress(self, symbol, end_date=None):
        for row in range(self.tickers_table.rowCount()):
            if self.tickers_table.item(row, 0).text() == symbol:
                progress_item = self.tickers_table.item(row, 4)
                progress_data = self.download_progress.get(symbol, {'progress': 0, 'completed': 0, 'total': 0})
                progress_item.setData(Qt.DisplayRole, progress_data)
                if end_date:
                    progress_item.setData(Qt.UserRole, end_date)
                self.tickers_table.viewport().update()
                break
    
    def update_global_progress(self):
        if self.total_minutes > 0:
            progress = int((self.completed_minutes / self.total_minutes) * 100)
            self.global_progress.setValue(progress)
            self.global_progress.setFormat(f"{progress}% ({self.completed_minutes:,}/{self.total_minutes:,} минут)")

    @asyncSlot()
    async def refresh_tickers(self):
        try:
            self.refresh_btn.setEnabled(False)
            self.tickers_table.clear()
            self.tickers_table.setRowCount(1)
            self.tickers_table.setItem(0, 0, QTableWidgetItem("Загрузка тикеров..."))
            
            async with aiohttp.ClientSession() as session:
                url = "https://api.bybit.com/v5/market/tickers"
                params = {'category': 'spot'}
                
                async with session.get(url, params=params) as response:
                    data = await response.json()
                    
                    if not isinstance(data, dict):
                        raise ValueError("Invalid API response format")
                    
                    if data.get('retCode') != 0:
                        ret_msg = data.get('retMsg', 'Unknown error')
                        raise ValueError(f"API error: {ret_msg}")
                    
                    if not isinstance(data.get('result', {}).get('list'), list):
                        raise ValueError("Invalid tickers data format")
                    
                    self.all_tickers_data = data['result']['list']
                    self.display_tickers(self.all_tickers_data)
                    
        except Exception as e:
            logging.error(f"Ошибка при обновлении тикеров: {str(e)}")
            self.update_status_bar(f"Ошибка: {str(e)}")
        finally:
            self.refresh_btn.setEnabled(True)
        
    async def start_loading(self):
        selected_tickers = []
        for row in range(self.tickers_table.rowCount()):
            if self.tickers_table.item(row, 0).isSelected():
                selected_tickers.append(self.tickers_table.item(row, 0).text())
        
        if not selected_tickers:
            self.update_status_bar("Не выбраны тикеры для загрузки")
            QMessageBox.warning(self, "Ошибка", "Не выбраны тикеры для загрузки")
            return
        
        # Инициализация параметров загрузки
        start_date = self.from_datetime.dateTime().toPyDateTime()
        end_date = self.to_datetime.dateTime().toPyDateTime()
        
        self.shutdown = False
        self.load_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.setCursor(Qt.WaitCursor)
        
        # Сброс счетчиков прогресса
        self.total_minutes = 0
        self.completed_minutes = 0
        self.calculated_tickers = 0
        self.total_tickers_to_calculate = len(selected_tickers)
        self.active_threads = 0
        
        try:
            settings = QSettings("settings.ini", QSettings.IniFormat)
            self.download_threads = int(settings.value("settings/threads", "5"))
            
            pool = await asyncpg.create_pool(
                host=settings.value("postgres/host"),
                port=settings.value("postgres/port", "5432"),
                user=settings.value("postgres/user"),
                password=settings.value("postgres/password"),
                database=settings.value("postgres/database"),
                min_size=1,
                max_size=self.download_threads
            )
            
            async with pool:
                schema = settings.value("settings/schema", "bybit_data")
                await self.create_schema_if_not_exists(pool, schema)
                
                semaphore = asyncio.Semaphore(self.download_threads)
                download_tasks = []
                calculation_tasks = []
                
                # Создаем очередь для тикеров, готовых к загрузке
                ticker_queue = asyncio.Queue()
                
                async def calculate_missing_periods(symbol):
                    """Асинхронно рассчитывает недостающие периоды для символа"""
                    try:
                        missing_periods = await self.check_missing_data(pool, schema, symbol, start_date, end_date)
                        if missing_periods:
                            total_minutes = sum((end - start).total_seconds() / 60 
                                            for start, end in missing_periods)
                            self.download_progress[symbol] = {
                                'progress': 0,
                                'total': int(total_minutes),
                                'completed': 0
                            }
                            # Добавляем в очередь для загрузки
                            await ticker_queue.put((symbol, missing_periods))
                            # Атомарно увеличиваем счетчик минут
                            async with asyncio.Lock():
                                self.total_minutes += int(total_minutes)
                        else:
                            self.download_progress[symbol] = {
                                'progress': 100,
                                'total': 0,
                                'completed': 0
                            }
                        
                        self.calculated_tickers += 1
                        self.update_calculation_progress()
                    except Exception as e:
                        logging.error(f"Ошибка расчета для {symbol}: {str(e)}")
                        self.update_status_bar(f"Ошибка расчета для {symbol}")
                
                async def download_worker():
                    """Рабочий процесс для загрузки данных"""
                    while not self.shutdown:
                        try:
                            symbol, periods = await asyncio.wait_for(
                                ticker_queue.get(), 
                                timeout=1.0
                            )
                            
                            for period_start, period_end in periods:
                                if self.shutdown:
                                    break
                                task = asyncio.create_task(
                                    self.download_symbol_data(
                                        pool, schema, symbol, 
                                        period_start, period_end, 
                                        semaphore
                                    )
                                )
                                download_tasks.append(task)
                                
                            ticker_queue.task_done()
                        except asyncio.TimeoutError:
                            if ticker_queue.empty() and all(t.done() for t in calculation_tasks):
                                break
                
                # Запускаем рабочие процессы для загрузки
                download_workers = [
                    asyncio.create_task(download_worker()) 
                    for _ in range(self.download_threads)
                ]
                
                # Запускаем расчет недостающих периодов для всех тикеров
                for symbol in selected_tickers:
                    calculation_tasks.append(asyncio.create_task(
                        calculate_missing_periods(symbol)
                    ))
                
                # Ждем завершения всех задач
                await asyncio.gather(*calculation_tasks)
                await ticker_queue.join()
                
                # Отменяем рабочие процессы загрузки
                for worker in download_workers:
                    worker.cancel()
                
                # Ждем завершения оставшихся задач загрузки
                if download_tasks and not self.shutdown:
                    await asyncio.gather(*download_tasks, return_exceptions=True)
                    
                    # Проверяем ошибки
                    failed = any(isinstance(task, Exception) for task in download_tasks)
                    if failed and not self.shutdown:
                        self.update_status_bar("Некоторые задачи завершились с ошибками")
                        QMessageBox.warning(self, "Предупреждение", 
                                        "Некоторые задачи завершились с ошибками. Проверьте логи.")
                    elif not self.shutdown:
                        self.update_status_bar("Данные успешно загружены")
                        QMessageBox.information(self, "Успех", "Данные успешно загружены")
                
        except Exception as e:
            self.update_status_bar(f"Ошибка: {str(e)}")
            if not self.shutdown:
                QMessageBox.critical(self, "Ошибка", f"Ошибка при загрузке данных: {str(e)}")
        finally:
            self.load_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.setCursor(Qt.ArrowCursor)
            self.global_progress.setValue(100)

    async def check_missing_data(self, pool, schema, symbol, start_date, end_date):
        self.update_status_bar(f"Проверка данных для {symbol}")
        try:
            table_name = f"klines_{symbol.lower()}"
            missing_periods = []
            
            async with pool.acquire() as conn:
                table_exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
                    schema, table_name
                )
                
                if not table_exists:
                    if (end_date - start_date) > timedelta(days=30):
                        current_start = start_date
                        while current_start < end_date:
                            next_month = datetime(current_start.year, current_start.month, 1) + timedelta(days=32)
                            month_end = min(datetime(next_month.year, next_month.month, 1) - timedelta(seconds=1), end_date)
                            missing_periods.append((current_start, month_end))
                            current_start = month_end + timedelta(seconds=1)
                    else:
                        missing_periods.append((start_date, end_date))
                    return missing_periods
                
                current_month_start = datetime(start_date.year, start_date.month, 1)
                while current_month_start < end_date:
                    next_month = datetime(current_month_start.year, current_month_start.month, 1) + timedelta(days=32)
                    month_end = min(datetime(next_month.year, next_month.month, 1) - timedelta(seconds=1), end_date)
                    
                    month_start = max(current_month_start, start_date)
                    month_end = min(month_end, end_date)
                    
                    gaps = await conn.fetch(
                        f"""
                        WITH time_range AS (
                            SELECT generate_series(
                                $1::timestamp,
                                $2::timestamp,
                                interval '1 minute'
                            ) AS time_point
                        ),
                        existing_data AS (
                            SELECT timestamp FROM {schema}.{table_name}
                            WHERE timestamp BETWEEN $1 AND $2
                        )
                        SELECT time_point FROM time_range
                        WHERE NOT EXISTS (
                            SELECT 1 FROM existing_data
                            WHERE timestamp = time_point
                        )
                        ORDER BY time_point
                        """,
                        month_start, month_end
                    )
                    
                    if gaps:
                        current_start = gaps[0]['time_point']
                        prev_time = current_start
                        
                        for gap in gaps[1:]:
                            if (gap['time_point'] - prev_time) > timedelta(minutes=1):
                                missing_periods.append((current_start, prev_time))
                                current_start = gap['time_point']
                            prev_time = gap['time_point']
                        
                        missing_periods.append((current_start, prev_time))
                    
                    current_month_start = month_end + timedelta(seconds=1)
            
            self.calculated_tickers += 1
            self.update_calculation_progress()
            return missing_periods
        except Exception as e:
            self.update_status_bar(f"Ошибка при проверке данных для {symbol}: {str(e)}")
            raise
    
    async def create_schema_if_not_exists(self, pool, schema):
        async with pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    async def download_symbol_data(self, pool, schema, symbol, start_date, end_date, semaphore=None):
        if self.shutdown:
            return
                
        self.active_threads += 1
        self.update_calculation_progress()
        
        try:
            async with semaphore:
                table_name = f"klines_{symbol.lower()}"
                total_minutes = (end_date - start_date).total_seconds() / 60
                processed_minutes = 0
                
                self.update_progress(symbol, start_date)
                
                async with pool.acquire() as conn:
                    await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                        timestamp TIMESTAMP PRIMARY KEY,
                        open DECIMAL,
                        high DECIMAL,
                        low DECIMAL,
                        close DECIMAL,
                        volume DECIMAL,
                        turnover DECIMAL
                    )
                    """)
                
                async with aiohttp.ClientSession() as session:
                    current_start = start_date
                    
                    while current_start < end_date and not self.shutdown:
                        current_end = min(current_start + timedelta(minutes=600), end_date)
                        
                        klines = await self.fetch_klines(session, symbol, start_time=current_start, end_time=current_end)
                        
                        if klines is None:  # Если fetch_klines вернул None после всех попыток
                            self.shutdown = True
                            break
                            
                        if klines:  # Если данные получены успешно
                            await self.save_klines(pool, schema, table_name, klines)
                            last_timestamp = datetime.utcfromtimestamp(int(klines[0][0]) / 1000)
                            minutes_processed = (last_timestamp - current_start).total_seconds() / 60
                            processed_minutes += minutes_processed
                            self.completed_minutes += minutes_processed
                            current_start = last_timestamp + timedelta(minutes=1)
                            
                            progress = int((processed_minutes / total_minutes) * 100)
                            self.download_progress[symbol]['progress'] = min(progress, 100)
                            self.download_progress[symbol]['completed'] = processed_minutes
                            self.update_progress(symbol, current_start)
                            self.update_global_progress()
                        else:
                            minutes_processed = (current_end - current_start).total_seconds() / 60
                            processed_minutes += minutes_processed
                            self.completed_minutes += minutes_processed
                            current_start = current_end
                        
                        await asyncio.sleep(0.1)
                    
                    if not self.shutdown:
                        self.download_progress[symbol]['progress'] = 100
                        self.download_progress[symbol]['completed'] = self.download_progress[symbol]['total']
                        self.update_progress(symbol, end_date)
        except Exception as e:
            error_msg = f"Ошибка при загрузке {symbol}: {str(e)}"
            logging.error(error_msg)
            self.update_status_bar(error_msg)
        finally:
            self.active_threads -= 1
            self.update_calculation_progress()

    async def fetch_klines(self, session, symbol, start_time=None, end_time=None):
        """Запрашивает данные с биржи с повторными попытками при ошибках"""
        max_retries = 3
        retry_delay = 2  # секунды
        last_error = None
        
        for attempt in range(max_retries):
            if self.shutdown:
                return None
                
            url = "https://api.bybit.com/v5/market/kline"
            params = {
                'category': 'spot',
                'symbol': symbol,
                'interval': '1',
                'limit': 600
            }
            
            if start_time:
                params['start'] = int(start_time.timestamp() * 1000)
            if end_time:
                params['end'] = int(end_time.timestamp() * 1000)
            
            headers = {
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            
            try:
                async with session.get(url, params=params, headers=headers) as response:
                    # Проверка статуса ответа
                    if response.status != 200:
                        error_text = await response.text()
                        last_error = f"HTTP {response.status}: {error_text}"
                        logging.error(f"Attempt {attempt + 1} failed for {symbol}: {last_error}")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    # Проверка формата данных
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/json' not in content_type:
                        error_text = await response.text()
                        last_error = f"Invalid content type: {content_type}, response: {error_text}"
                        logging.error(f"Attempt {attempt + 1} failed for {symbol}: {last_error}")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    # Парсинг JSON
                    try:
                        data = await response.json()
                    except Exception as e:
                        last_error = f"JSON parse error: {str(e)}"
                        logging.error(f"Attempt {attempt + 1} failed for {symbol}: {last_error}")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    # Проверка структуры ответа
                    if not isinstance(data, dict):
                        last_error = "Response is not a dictionary"
                        logging.error(f"Attempt {attempt + 1} failed for {symbol}: {last_error}")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    # Проверка кода ошибки
                    if data.get('retCode') != 0:
                        ret_msg = data.get('retMsg', 'Unknown error')
                        last_error = f"API error: {ret_msg}"
                        logging.error(f"Attempt {attempt + 1} failed for {symbol}: {last_error}")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    # Проверка наличия данных
                    if not isinstance(data.get('result', {}).get('list'), list):
                        last_error = "Invalid data format in response"
                        logging.error(f"Attempt {attempt + 1} failed for {symbol}: {last_error}")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    # Успешный запрос
                    return data['result']['list']
                    
            except Exception as e:
                last_error = f"Request failed: {str(e)}"
                logging.error(f"Attempt {attempt + 1} failed for {symbol}: {last_error}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
        
        # Все попытки неудачны
        error_msg = f"Не удалось получить данные для {symbol} после {max_retries} попыток. Последняя ошибка: {last_error}"
        logging.error(error_msg)
        self.update_status_bar(error_msg)
        QMessageBox.critical(self, "Ошибка загрузки", error_msg)
        return None
    
    async def save_klines(self, pool, schema, table_name, klines):
        if self.shutdown or not klines:
            return
        
        async with pool.acquire() as conn:
            values = []
            for kline in reversed(klines):
                timestamp = datetime.fromtimestamp(int(kline[0]) / 1000)
                values.append((
                    timestamp,
                    float(kline[1]),
                    float(kline[2]),
                    float(kline[3]),
                    float(kline[4]),
                    float(kline[5]),
                    float(kline[6])
                ))
            
            await conn.executemany(
                f"""
                INSERT INTO {schema}.{table_name} 
                (timestamp, open, high, low, close, volume, turnover)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (timestamp) DO NOTHING
                """,
                values
            )

def run_app():
    app = QApplication(sys.argv)
    
    if sys.platform == 'win32':
        app.setAttribute(Qt.AA_DisableWindowContextHelpButton)
        app.setStyle('Fusion')
    
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)
    
    window = MainWindow()
    window.show()
    
    with loop:
        loop.run_forever()

if __name__ == "__main__":
    try:
        from qasync import QEventLoop
        run_app()
    except ImportError as e:
        sys.exit(1)