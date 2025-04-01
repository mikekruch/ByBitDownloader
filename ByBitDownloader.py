import sys
import asyncio
import aiohttp
import asyncpg
import ctypes
from datetime import datetime, timedelta, timezone
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QDateTimeEdit, QPushButton, QLineEdit, QTableWidget,
                             QDialog, QFormLayout, QMessageBox, QTableWidgetItem, QHeaderView,
                             QStyledItemDelegate, QStyleOptionProgressBar, QStyle, QProgressBar)
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
        self.timers = []  # Для хранения всех таймеров
        self.pool = None  # Для хранения пула подключений
        self.download_threads = int(self.settings.value("settings/threads", "5"))
        self.total_minutes = 0
        self.completed_minutes = 0
        self.progress_update_timer = None  # Single timer for progress updates
       
        self.init_ui()
        self.refresh_tickers()
        self.load_selected_tickers()
        
        # Используем единый таймер для обновлений
        self.add_timer(1000, self.update_progress_bars)  # Обновление прогресс-баров раз в секунду
    
    def update_progress_bars(self):
        """Обновляет только прогресс-бары без перезагрузки тикеров"""
        for row in range(self.tickers_table.rowCount()):
            symbol = self.tickers_table.item(row, 0).text()
            if symbol in self.download_progress:
                progress_item = self.tickers_table.item(row, 4)
                progress = self.download_progress[symbol]
                progress_item.setData(Qt.DisplayRole, progress)
    
        # Обновляем общий прогресс
        self.update_global_progress()
        self.tickers_table.viewport().update() 

    def add_timer(self, interval, callback):
        """Централизованное создание таймеров"""
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
        self.refresh_btn.clicked.connect(self.refresh_tickers)
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
        
        central_widget.setLayout(layout)

        self.progress_update_timer = self.add_timer(1000, self.update_progress_bars)
    
    def closeEvent(self, event):
        """Очистка ресурсов при закрытии окна"""
        self.shutdown = True
        
        # Остановка всех таймеров
        for timer in self.timers:
            timer.stop()
            timer.deleteLater()
        self.timers.clear()
        
        # Закрытие пула подключений
        if self.pool is not None:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.pool.close())
            except:
                pass
        
        super().closeEvent(event)
    
    def stop_loading(self):
        """Остановка всех загрузок"""
        self.shutdown = True
        self.stop_btn.setEnabled(False)
        self.load_btn.setEnabled(True)
        
        # Cancel all running tasks
        for task in asyncio.all_tasks():
            if not task.done():
                task.cancel()
        
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
            change_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
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
                    self.tickers_table.item(row, col).setSelected(True)
        
        self.tickers_table.sortItems(self.current_sort_column, self.current_sort_order)
    
    def load_selected_tickers(self):
        selected = self.settings.value("selected_tickers", [])
        if isinstance(selected, str):
            selected = [selected] if selected else []
        self.selected_tickers = set(selected)
    
    def save_selected_tickers(self):
        selected = []
        for row in range(self.tickers_table.rowCount()):
            if self.tickers_table.item(row, 0).isSelected():
                selected.append(self.tickers_table.item(row, 0).text())
        self.selected_tickers = set(selected)
        self.settings.setValue("selected_tickers", selected)
        QMessageBox.information(self, "Сохранено", "Выбранные тикеры сохранены")
    
    def open_settings(self):
        dialog = SettingsDialog(self)
        dialog.exec_()
    
    def update_progress(self, symbol, end_date=None):
        """Обновление прогресса для конкретного тикера"""
        for row in range(self.tickers_table.rowCount()):
            if self.tickers_table.item(row, 0).text() == symbol:
                progress_item = self.tickers_table.item(row, 4)
                progress_data = self.download_progress.get(symbol, {'progress': 0, 'completed': 0, 'total': 1})
                progress_item.setData(Qt.DisplayRole, progress_data)
                if end_date:
                    progress_item.setData(Qt.UserRole, end_date)
                self.tickers_table.viewport().update()
                break
    
    def update_global_progress(self):
        """Обновление общего прогрессбара на основе минут"""
        if self.total_minutes > 0:
            progress = int((self.completed_minutes / self.total_minutes) * 100)
            self.global_progress.setValue(progress)
            self.global_progress.setFormat(f"{progress}% ({self.completed_minutes:,}/{self.total_minutes:,} минут)")


    @asyncSlot()
    async def refresh_tickers(self):
        try:
            self.refresh_btn.setEnabled(False)
            self.tickers_table.setRowCount(1)
            self.tickers_table.setItem(0, 0, QTableWidgetItem("Загрузка тикеров..."))
            
            async with aiohttp.ClientSession() as session:
                url = "https://api.bybit.com/v5/market/tickers"
                params = {'category': 'spot'}
                
                async with session.get(url, params=params) as response:
                    data = await response.json()
                    
                    # More robust error handling
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
            QMessageBox.critical(self, "Ошибка", f"Ошибка при загрузке тикеров: {str(e)}")
        finally:
            self.refresh_btn.setEnabled(True)

    async def start_loading(self):
        selected_tickers = []
        for row in range(self.tickers_table.rowCount()):
            if self.tickers_table.item(row, 0).isSelected():
                selected_tickers.append(self.tickers_table.item(row, 0).text())
        
        if not selected_tickers:
            QMessageBox.warning(self, "Ошибка", "Не выбраны тикеры для загрузки")
            return
        
        start_date_ = self.from_datetime.dateTime().toPyDateTime()
        start_date = datetime(start_date_.year, start_date_.month, start_date_.day, start_date_.hour, start_date_.minute)
        end_date_ = self.to_datetime.dateTime().toPyDateTime()
        end_date = datetime(end_date_.year, end_date_.month, end_date_.day, end_date_.hour, end_date_.minute)
        
        if start_date >= end_date:
            QMessageBox.warning(self, "Ошибка", "Дата начала должна быть раньше даты окончания")
            return
        
        self.shutdown = False
        self.load_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.setCursor(Qt.WaitCursor)
        
        # Calculate total minutes for progress tracking
        self.total_minutes = int((end_date - start_date).total_seconds() / 60) * len(selected_tickers)
        self.completed_minutes = 0
        
        # Reset progress
        for symbol in selected_tickers:
            self.download_progress[symbol] = {
                'progress': 0,
                'total': int((end_date - start_date).total_seconds() / 60),
                'completed': 0
            }
            self.update_progress(symbol)
        
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
                
                tasks = []
                for symbol in selected_tickers:
                    missing_periods = await self.check_missing_data(pool, schema, symbol, start_date, end_date)
                    
                    if not missing_periods:
                        self.download_progress[symbol]['progress'] = 100
                        self.download_progress[symbol]['completed'] = self.download_progress[symbol]['total']
                        self.completed_minutes += self.download_progress[symbol]['total']
                        self.update_progress(symbol)
                        continue
                    
                    # Create semaphore to limit concurrent downloads
                    semaphore = asyncio.Semaphore(self.download_threads)
                    
                    for period_start, period_end in missing_periods:
                        tasks.append(
                            self.download_symbol_data(pool, schema, symbol, period_start, period_end, semaphore)
                        )
                
                if tasks:
                    # Create a task for gathering to allow proper cleanup
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Check if any tasks failed
                    failed = any(isinstance(task, Exception) for task in tasks)
                    if failed and not self.shutdown:
                        QMessageBox.warning(self, "Предупреждение", 
                                        "Некоторые задачи завершились с ошибками. Проверьте логи.")
                    elif not self.shutdown:
                        QMessageBox.information(self, "Успех", "Данные успешно загружены")
                    
        except Exception as e:
            if not self.shutdown:
                QMessageBox.critical(self, "Ошибка", f"Ошибка при загрузке данных: {str(e)}")
        finally:
            self.load_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.setCursor(Qt.ArrowCursor)
            self.global_progress.setValue(100)

            for timer in list(self.timers):
                if timer != self.progress_update_timer:
                    timer.stop()
                    timer.deleteLater()
                    self.timers.remove(timer)

    async def check_missing_data(self, pool, schema, symbol, start_date, end_date):
        table_name = f"klines_{symbol.lower()}"
        missing_periods = []
        
        async with pool.acquire() as conn:
            table_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
                schema, table_name
            )
            
            if not table_exists:
                # Split by months if period is large
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
            
            # Check for missing data month by month to avoid huge queries
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
            
            return missing_periods
    
    async def create_schema_if_not_exists(self, pool, schema):
        async with pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    async def download_symbol_data(self, pool, schema, symbol, start_date, end_date, semaphore=None):
        if self.shutdown:
            return
        
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
                    
                    if klines:
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

    async def fetch_klines(self, session, symbol, start_time=None, end_time=None):
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
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' not in content_type:
                    text = await response.text()
                    print(f"Unexpected content type: {content_type}, response: {text}")
                    return None
                
                data = await response.json()
                
                if data.get('retCode') == 0:
                    return data['result']['list']
                else:
                    print(f"Ошибка получения данных для {symbol}: {data.get('retMsg', 'Unknown error')}")
                    return None
        except Exception as e:
            print(f"Ошибка при запросе данных для {symbol}: {str(e)}")
            return None
    
    async def save_klines(self, pool, schema, table_name, klines):
        if self.shutdown or not klines:
            return
        
        async with pool.acquire() as conn:
            values = []
            for kline in reversed(klines):
                timestamp = datetime.utcfromtimestamp(int(kline[0]) / 1000)
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
    
    # Настройки для Windows
    if sys.platform == 'win32':
        app.setAttribute(Qt.AA_DisableWindowContextHelpButton)
        app.setStyle('Fusion')
    
    # Настройка event loop
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
        print(f"Ошибка: {e}. Требуется установить qasync (pip install qasync)")
        sys.exit(1)