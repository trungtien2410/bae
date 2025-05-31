from PyQt6 import QtCore, QtGui, QtWidgets
import pandas as pd
from datetime import timedelta
import sys
from pathlib import Path

def resource_path(relative_path):
    """
    Lấy đường dẫn tuyệt đối đến một tài nguyên, xử lý cả môi trường phát triển và môi trường đã đóng gói bằng PyInstaller.
    """
    if hasattr(sys, '_MEIPASS'):
        return str(Path(sys._MEIPASS) / relative_path)
    else:
        return str(Path(__file__).parent / relative_path)

class Worker(QtCore.QThread):
    """
    Lớp con của QThread để thực hiện việc cạo dữ liệu web và tạo tài liệu Word trong một luồng riêng biệt.
    Phát tín hiệu để cập nhật tiến độ, thông báo nhật ký và trạng thái hoàn thành.
    """
    progress = QtCore.pyqtSignal(int)
    log = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(object)

    def __init__(self, input_file_path, output_file_path):
        """
        Khởi tạo luồng Worker.
        Args:
            input_file_path (str): Đường dẫn đến file Excel đầu vào.
            output_file_path (str): Đường dẫn để lưu file Excel kết quả.
        """
        super().__init__()
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path

    def run(self):
        try:
            self.log.emit("ℹ️ Đang đọc dữ liệu từ file Excel...")
            self.data = pd.read_excel(self.input_file_path, engine='openpyxl')
            self.df = pd.DataFrame(self.data)

            # Validate required columns
            required_columns = ['N3', 'registration_time', 'buyer_id']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            # Xử lý cột restristration_time xoá các dòng NaT và chuyển đổi kiểu dữ liệu
            self.df['registration_time'].dropna(inplace=True) 
            # Đảm bảo cột 'registration_time' là kiểu datetime
            self.df['registration_time'] = pd.to_datetime(self.df['registration_time'], errors='coerce')
            
            # Filter out rows where registration_time is NaT (invalid date)
            df_filtered = self.df.dropna(subset=['registration_time']).copy()
            
            # Sort by phone number and time for optimized searching
            df_sorted = df_filtered.sort_values(by=['N3', 'registration_time']).reset_index(drop=True)

            final_grouped_ids = set() # To store all unique IDs that belong to any valid group
            
            total_phone_nums = len(df_sorted['N3'].unique())
            processed_phone_nums = 0

            # Iterate through each unique phone number group
            for phone_num, group in df_sorted.groupby('N3'):
                processed_phone_nums += 1
                self.progress.emit(int((processed_phone_nums / total_phone_nums) * 100))

                # Convert group to a list of dictionaries for easier indexing and manipulation
                phone_records = group.to_dict('records')
                
                i = 0 # 'i' is the starting index of the current potential group
                while i < len(phone_records):
                    current_group_ids = []
                    current_group_times = []

                    # Start a new potential group with the current record (the "mốc đầu tiên")
                    start_record = phone_records[i]
                    current_group_ids.append(start_record['buyer_id'])
                    current_group_times.append(start_record['registration_time'])
                    
                    # 'last_time_in_group' tracks the time of the latest record added to the current group.
                    # This is the "mốc" for checking the 1-hour window for subsequent records.
                    last_time_in_group = start_record['registration_time']

                    # 'j' iterates through subsequent records to expand the current group
                    j = i + 1
                    while j < len(phone_records):
                        next_record = phone_records[j]
                        next_id = next_record['buyer_id']
                        next_time = next_record['registration_time']

                        # Check if the 'next_time' is within 1 hour of the 'last_time_in_group'
                        if (next_time - last_time_in_group) <= timedelta(hours=1):
                            current_group_ids.append(next_id)
                            current_group_times.append(next_time)
                            last_time_in_group = next_time # Update the reference point for the 1-hour window
                            j += 1
                        else:
                            # If the time gap is too large, stop extending the current group
                            break 
                    
                    # After extending the group as much as possible, evaluate if it meets the criteria
                    # "ít nhất 3 con riêng biệt" -> len(set(current_group_ids)) >= 3
                    unique_ids_in_group = set(current_group_ids)
                    if len(unique_ids_in_group) >= 3:
                        final_grouped_ids.update(unique_ids_in_group)
                    
                    # Move 'i' to 'j'. This ensures that the next potential group starts
                    # from the record immediately following the last one considered in the current group.
                    # This fulfills "sau đó nó mới dò tiếp con số 3 sẽ là mốc đầu tiên và dò với các con kế tiếp."
                    # The record at index 'j' is the first one that was NOT included in the current group.
                    i = j

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách ID nhóm tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (ít nhất 3 ID riêng biệt trong 1 giờ).")
            
            self.finished.emit(True) # Indicate successful completion
            
        except FileNotFoundError:
            self.log.emit(f"❌ Lỗi: Không tìm thấy file Excel tại đường dẫn: {self.input_file_path}")
            self.finished.emit(None)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
            self.finished.emit(None)

class Worker2(QtCore.QThread):
    """
    Lớp con của QThread để thực hiện việc cạo dữ liệu web và tạo tài liệu Word trong một luồng riêng biệt.
    Phát tín hiệu để cập nhật tiến độ, thông báo nhật ký và trạng thái hoàn thành.
    """
    progress = QtCore.pyqtSignal(int)
    log = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(object)

    def __init__(self, input_file_path, output_file_path):
        """
        Khởi tạo luồng Worker.
        Args:
            input_file_path (str): Đường dẫn đến file Excel đầu vào.
            output_file_path (str): Đường dẫn để lưu file Excel kết quả.
        """
        super().__init__()
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path

    def run(self):
        try:
            self.log.emit("ℹ️ Đang đọc dữ liệu từ file Excel...")
            self.data = pd.read_excel(self.input_file_path, engine='openpyxl')
            self.df = pd.DataFrame(self.data)

            # Validate required columns
            required_columns = ['recipient_phone', 'pv_promotion_id', 'buyer_id']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return
            # Ensure 'recipient_phone' and 'pv_promotion_id' are strings to handle mixed types consistently
            self.df['recipient_phone'] = self.df['recipient_phone'].astype(str)
            self.df['pv_promotion_id'] = self.df['pv_promotion_id'].astype(str)
            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            # Get all the group of > or more unique id have the same recipient_phone and the same pv_promotion_id
            # Ensure 'pv_promotion_id' is string to handle mixed types consistently
            self.df['pv_promotion_id'] = self.df['pv_promotion_id'].astype(str)

            # Group by recipient_phone and pv_promotion_id
            # Then, for each group, find the number of unique buyer_id's
            grouped_df = self.df.groupby(['recipient_phone', 'pv_promotion_id'])['buyer_id'].nunique().reset_index(name='unique_buyer_ids_count')

            # Filter for groups with 3 or more unique buyer_id's
            filtered_groups = grouped_df[grouped_df['unique_buyer_ids_count'] >= 3]

            final_grouped_ids = set()

            # For each filtered group (that has 3 or more unique buyer_ids),
            # get all buyer_id's from the original DataFrame that belong to these groups.
            # This is more efficient than iterating through rows.
            if not filtered_groups.empty:
                # Merge original df with filtered groups to get all buyer_ids
                merged_df = pd.merge(
                    self.df,
                    filtered_groups[['recipient_phone', 'pv_promotion_id']],
                    on=['recipient_phone', 'pv_promotion_id'],
                    how='inner'
                )
                final_grouped_ids.update(merged_df['buyer_id'].unique())

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách ID nhóm theo khuyến mãi tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (recipient_phone, Promotion ID, >= 3 ID).")
            
            self.finished.emit(self.df)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)

class Worker3(QtCore.QThread):
    """
    Lớp con của QThread để thực hiện việc cạo dữ liệu web và tạo tài liệu Word trong một luồng riêng biệt.
    Phát tín hiệu để cập nhật tiến độ, thông báo nhật ký và trạng thái hoàn thành.
    """
    progress = QtCore.pyqtSignal(int)
    log = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(object)

    def __init__(self, input_file_path, output_file_path):
        """
        Khởi tạo luồng Worker.
        Args:
            input_file_path (str): Đường dẫn đến file Excel đầu vào.
            output_file_path (str): Đường dẫn để lưu file Excel kết quả.
        """
        super().__init__()
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path

    def run(self):
        try:
            self.log.emit("ℹ️ Đang đọc dữ liệu từ file Excel...")
            self.data = pd.read_excel(self.input_file_path, engine='openpyxl')
            self.df = pd.DataFrame(self.data)

            # Validate required columns
            required_columns = ['recipient_phone', 'fsv_voucher_code', 'buyer_id']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            # Get all the group of > or more unique id have the same recipient_phone and the same pv_promotion_id
            # Ensure 'pv_promotion_id' is string to handle mixed types consistently
            self.df['fsv_voucher_code'] = self.df['fsv_voucher_code'].astype(str)
            # Ensure 'recipient_phone' and 'pv_promotion_id' are strings to handle mixed types consistently
            self.df['recipient_phone'] = self.df['recipient_phone'].astype(str)
            # Group by recipient_phone and pv_promotion_id
            # Then, for each group, find the number of unique buyer_id's
            grouped_df = self.df.groupby(['recipient_phone', 'fsv_voucher_code'])['buyer_id'].nunique().reset_index(name='unique_buyer_ids_count')

            # Filter for groups with 3 or more unique buyer_id's
            filtered_groups = grouped_df[grouped_df['unique_buyer_ids_count'] >= 5]

            final_grouped_ids = set()

            # For each filtered group (that has 3 or more unique buyer_ids),
            # get all buyer_id's from the original DataFrame that belong to these groups.
            # This is more efficient than iterating through rows.
            if not filtered_groups.empty:
                # Merge original df with filtered groups to get all buyer_ids
                merged_df = pd.merge(
                    self.df,
                    filtered_groups[['recipient_phone', 'fsv_voucher_code']],
                    on=['recipient_phone', 'fsv_voucher_code'],
                    how='inner'
                )
                final_grouped_ids.update(merged_df['buyer_id'].unique())

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách ID nhóm theo khuyến mãi tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (recipient_phone, fsv_voucher_code, >= 5 ID).")
            
            self.finished.emit(self.df)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)


class Ui_MainWindow(object):
    """
    Lớp UI chính cho ứng dụng PyQt6.
    Thiết lập cửa sổ chính, các widget, bố cục và kết nối tín hiệu/khe.
    """
    def setupUi(self, MainWindow):
        """
        Thiết lập giao diện người dùng cho cửa sổ chính.
        Args:
            MainWindow (QtWidgets.QMainWindow): Đối tượng cửa sổ chính.
        """
        MainWindow.setObjectName("MainWindow")
        MainWindow.setMinimumSize(QtCore.QSize(620, 720))
        MainWindow.showMaximized()
        self.main_window = MainWindow
        
        # Initial stylesheet (light mode)
        self.light_mode_stylesheet = """
            QWidget {
                background-color: #fce4ec; /* Light Pink - Background */
                font-family: 'Segoe UI';
                color: #ad1457; /* Darker Pink - Default Text */
            }
            QPushButton {
                background-color: #e91e63; /* Deep Pink - Buttons */
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 8px;
                font-size: 14px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #c2185b; /* Darker Deep Pink - Button Hover */
            }
            QLineEdit, QTextEdit {
                background-color: #ffffff; /* White - Input Fields */
                border: 2px solid #f8bbd0; /* Light Pink Border */
                border-radius: 8px;
                padding: 6px;
                font-size: 13px;
                color: #4a148c; /* Dark Purple for text in light mode inputs */
            }
            QLabel {
                color: #ad1457; /* Darker Pink - Labels */
                font-size: 13pt;
            }
            QProgressBar {
                height: 24px;
                border-radius: 8px;
                background: #f8bbd0; /* Light Pink - Progress Bar Background */
            }
            QProgressBar::chunk {
                background-color: #e91e63; /* Deep Pink - Progress Bar Chunk */
                border-radius: 8px;
                transition: all 0.5s ease-in-out;
            }
            QTabWidget::pane {
                border: 2px solid #f8bbd0; /* Light Pink Border - Tab Pane */
                border-radius: 8px;
                background: #ffffff; /* White - Tab Pane Background */
                margin-top: 10px;
            }
            QTabBar::tab {
                background: #f8bbd0; /* Light Pink - Tab Background */
                border: 2px solid #f8bbd0;
                border-radius: 8px;
                padding: 8px 20px;
                font-size: 13px;
                color: #ad1457; /* Darker Pink - Tab Text */
            }
            QTabBar::tab:selected {
                background: #e91e63; /* Deep Pink - Selected Tab */
                color: white;
                font-weight: bold;
            }
            QTabBar::tab:hover {
                background: #f48fb1; /* Medium Pink - Tab Hover */
            }
            QTableWidget {
                background-color: #ffffff; /* White - Table Background */
                border: 2px solid #f8bbd0; /* Light Pink Border */
                border-radius: 8px;
                gridline-color: #f8bbd0; /* Light Pink Grid Lines */
                font-size: 13px;
                selection-background-color: #f48fb1; /* Medium Pink - Selection */
                selection-color: #ad1457; /* Darker Pink - Selected Text */
            }
            QTableWidget::item {
                padding: 6px;
            }
            QHeaderView::section {
                background-color: #f8bbd0; /* Light Pink - Header Background */
                color: #ad1457; /* Darker Pink - Header Text */
                padding: 6px;
                font-weight: bold;
                font-size: 13px;
                border: 1px solid #f48fb1; /* Medium Pink Border */
            }
            QTableCornerButton::section {
                background-color: #f8bbd0; /* Light Pink - Corner Button */
                border: 1px solid #f48fb1;
            }
            QScrollBar:vertical, QScrollBar:horizontal {
                background: #fce4ec; /* Light Pink - Scrollbar Background */
                border: none;
                width: 12px;
                height: 12px;
            }
            QScrollBar::handle:vertical, QScrollBar::handle:horizontal {
                background: #e91e63; /* Deep Pink - Scrollbar Handle */
                border-radius: 6px;
            }
            QScrollBar::add-line, QScrollBar::sub-line {
                background: none;
                border: none;
            }
            /* Style for the title label */
            #titleLabel {
                font-size: 28pt;
                font-weight: bold;
                color: #880e4f; /* Even darker pink */
                padding-bottom: 10px;
            }
            /* Styles for the dark mode toggle switch (checkbox) */
            QCheckBox::indicator {
                width: 25px;
                height: 25px;
                border-radius: 12px;
            }
            QCheckBox::indicator:unchecked {
                background-color: #f48fb1; /* Medium Pink - Unchecked */
                border: 1px solid #e91e63;
            }
            QCheckBox::indicator:unchecked:hover {
                background-color: #e91e63;
            }
            QCheckBox::indicator:checked {
                background-color: #4a148c; /* Dark Purple - Checked */
                border: 1px solid #ad1457;
            }
            QCheckBox::indicator:checked:hover {
                background-color: #6a1b9a;
            }
        """

        self.dark_mode_stylesheet = """
            QWidget {
                background-color: #263238; /* Dark Blue Grey - Background */
                font-family: 'Segoe UI';
                color: #eceff1; /* Light Grey - Default Text */
            }
            QPushButton {
                background-color: #8e24aa; /* Dark Purple - Buttons */
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 8px;
                font-size: 14px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #6a1b9a; /* Darker Purple - Button Hover */
            }
            QLineEdit, QTextEdit {
                background-color: #37474f; /* Even Darker Blue Grey - Input Fields */
                border: 2px solid #546e7a; /* Grey Blue Border */
                border-radius: 8px;
                padding: 6px;
                font-size: 13px;
                color: #e0f2f7; /* Lighter Blue for text in dark mode inputs */
            }
            QLabel {
                color: #b39ddb; /* Light Purple - Labels */
                font-size: 13pt;
            }
            QProgressBar {
                height: 24px;
                border-radius: 8px;
                background: #546e7a; /* Grey Blue - Progress Bar Background */
            }
            QProgressBar::chunk {
                background-color: #ab47bc; /* Medium Purple - Progress Bar Chunk */
                border-radius: 8px;
                transition: all 0.5s ease-in-out;
            }
            QTabWidget::pane {
                border: 2px solid #546e7a; /* Grey Blue Border - Tab Pane */
                border-radius: 8px;
                background: #37474f; /* Darker Blue Grey - Tab Pane Background */
                margin-top: 10px;
            }
            QTabBar::tab {
                background: #546e7a; /* Grey Blue - Tab Background */
                border: 2px solid #546e7a;
                border-radius: 8px;
                padding: 8px 20px;
                font-size: 13px;
                color: #cfd8dc; /* Light Grey - Tab Text */
            }
            QTabBar::tab:selected {
                background: #8e24aa; /* Dark Purple - Selected Tab */
                color: white;
                font-weight: bold;
            }
            QTabBar::tab:hover {
                background: #7b1fa2; /* Slightly Darker Purple - Tab Hover */
            }
            QTableWidget {
                background-color: #37474f; /* Darker Blue Grey - Table Background */
                border: 2px solid #546e7a; /* Grey Blue Border */
                border-radius: 8px;
                gridline-color: #546e7a; /* Grey Blue Grid Lines */
                font-size: 13px;
                selection-background-color: #ab47bc; /* Medium Purple - Selection */
                selection-color: white; /* White - Selected Text */
            }
            QTableWidget::item {
                padding: 6px;
            }
            QHeaderView::section {
                background-color: #546e7a; /* Grey Blue - Header Background */
                color: #b39ddb; /* Light Purple - Header Text */
                padding: 6px;
                font-weight: bold;
                font-size: 13px;
                border: 1px solid #78909c; /* Medium Grey Blue Border */
            }
            QTableCornerButton::section {
                background-color: #546e7a; /* Grey Blue - Corner Button */
                border: 1px solid #78909c;
            }
            QScrollBar:vertical, QScrollBar:horizontal {
                background: #263238; /* Dark Blue Grey - Scrollbar Background */
                border: none;
                width: 12px;
                height: 12px;
            }
            QScrollBar::handle:vertical, QScrollBar::handle:horizontal {
                background: #8e24aa; /* Dark Purple - Scrollbar Handle */
                border-radius: 6px;
            }
            QScrollBar::add-line, QScrollBar::sub-line {
                background: none;
                border: none;
            }
            /* Style for the title label */
            #titleLabel {
                font-size: 28pt;
                font-weight: bold;
                color: #d1c4e9; /* Lighter purple */
                padding-bottom: 10px;
            }
            /* Styles for the dark mode toggle switch (checkbox) */
            QCheckBox::indicator {
                width: 25px;
                height: 25px;
                border-radius: 12px;
            }
            QCheckBox::indicator:unchecked {
                background-color: #546e7a; /* Grey Blue - Unchecked */
                border: 1px solid #8e24aa;
            }
            QCheckBox::indicator:unchecked:hover {
                background-color: #6a1b9a;
            }
            QCheckBox::indicator:checked {
                background-color: #b39ddb; /* Light Purple - Checked */
                border: 1px solid #8e24aa;
            }
            QCheckBox::indicator:checked:hover {
                background-color: #d1c4e9;
            }
        """
        self.is_dark_mode = False # Track current mode
        MainWindow.setStyleSheet(self.light_mode_stylesheet) # Apply initial stylesheet

        self.centralwidget = QtWidgets.QWidget(MainWindow)
        main_layout = QtWidgets.QVBoxLayout(self.centralwidget)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)


        # --- Dark Mode Toggle ---
        dark_mode_layout = QtWidgets.QHBoxLayout()
        dark_mode_layout.addStretch() # Push toggle to the right
        self.dark_mode_checkbox = QtWidgets.QCheckBox("Dark Mode")
        self.dark_mode_checkbox.setFont(QtGui.QFont("Segoe UI", 11))
        self.dark_mode_checkbox.setStyleSheet("QCheckBox { color: #ad1457; }") # Initial color for light mode
        self.dark_mode_checkbox.stateChanged.connect(self.toggle_dark_mode)
        dark_mode_layout.addWidget(self.dark_mode_checkbox)
        main_layout.addLayout(dark_mode_layout)

        self.tabWidget = QtWidgets.QTabWidget()
        main_layout.addWidget(self.tabWidget)

        self.tab1 = QtWidgets.QWidget()
        tab1_layout = QtWidgets.QVBoxLayout(self.tab1)
        tab1_layout.setSpacing(15)

        # --- Input/Action Grouping (Optional: Can wrap in QGroupBox if desired) ---
        # For now, just keeping the existing layouts but mentioning the concept
        label_mnv_layout = QtWidgets.QHBoxLayout()
        self.label_mnv = QtWidgets.QLabel("Hãy chọn file gốc:")
        self.mnv = QtWidgets.QLineEdit()
        self.mnv.setPlaceholderText("Đường dẫn đến file Excel gốc...")
        self.chose_file_btn = QtWidgets.QPushButton("Chọn file gốc")
        self.chose_file_btn.clicked.connect(self.choose_file)
        self.mnv.setFont(QtGui.QFont("Segoe UI", 11))
        label_mnv_layout.addWidget(self.label_mnv)
        label_mnv_layout.addWidget(self.mnv)
        label_mnv_layout.addWidget(self.chose_file_btn)

        btn_layout = QtWidgets.QHBoxLayout()
        self.create_btn = QtWidgets.QPushButton("Tạo báo cáo N3")
        self.create_btn.clicked.connect(self.generate_report)
        self.clear_btn = QtWidgets.QPushButton("Tạo báo cáo same promotion")
        self.clear_btn.clicked.connect(self.clear_input)
        self.fsv_btn = QtWidgets.QPushButton("Tạo báo cáo same fsv")
        self.fsv_btn.clicked.connect(self.same_fsv_input)
        btn_layout.addWidget(self.create_btn)
        btn_layout.addWidget(self.clear_btn)
        btn_layout.addWidget(self.fsv_btn)

        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setValue(0)

        self.log_output = QtWidgets.QTextEdit()
        self.log_output.setReadOnly(True)
        # Apply style to log output to allow rich text (for icons)
        self.log_output.document().setDefaultStyleSheet("p { margin-bottom: 2px; }")


        tab1_layout.addLayout(label_mnv_layout)
        tab1_layout.addLayout(btn_layout)
        tab1_layout.addWidget(self.progress_bar)
        tab1_layout.addWidget(self.log_output)

        self.tabWidget.addTab(self.tab1, "Nhóm Dữ Liệu")

        MainWindow.setCentralWidget(self.centralwidget)

    def choose_file(self):
        """
        Mở hộp thoại để người dùng chọn file Excel gốc.
        Lưu đường dẫn file vào ô nhập mã nhân viên.
        """
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            None, "Chọn file gốc", "", "Excel Files (*.xlsx);;All Files (*)")
        if file_path:
            self.mnv.setText(file_path)

    def on_report_finished(self, df):
        """
        Hàm được gọi khi luồng Worker hoàn thành việc tạo báo cáo.
        Hiển thị thông báo và ẩn spinner.
        """
        self.progress_bar.setValue(100)
        if df is not None:
            self.log_output.append("✅ Xử lý hoàn tất!")
        else:
            self.log_output.append("⚠️ Quá trình xử lý không thành công hoặc không có dữ liệu để nhóm.")
        self.create_btn.setEnabled(True)
        self.clear_btn.setEnabled(True)
        self.fsv_btn.setEnabled(True) # Re-enable the fsv button too


    def generate_report(self):
        """
        Bắt đầu quá trình tạo báo cáo N3.
        Yêu cầu người dùng chọn vị trí lưu và khởi động luồng Worker.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self.create_btn.setEnabled(False) # Disable button during processing
        self.clear_btn.setEnabled(False) # Disable other buttons too
        self.fsv_btn.setEnabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Dữ Liệu Nhóm", "du_lieu_nhom_N3.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self.create_btn.setEnabled(True) # Re-enable if cancelled
            self.clear_btn.setEnabled(True)
            self.fsv_btn.setEnabled(True)
            return

        self.thread = Worker(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()

    def clear_input(self):
        """
        Tạo báo cáo same promotion.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self.create_btn.setEnabled(False)
        self.clear_btn.setEnabled(False)
        self.fsv_btn.setEnabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "du_lieu_same_promotion.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self.create_btn.setEnabled(True)
            self.clear_btn.setEnabled(True)
            self.fsv_btn.setEnabled(True)
            return

        self.thread = Worker2(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()

    def same_fsv_input(self):
        """
        Tạo báo cáo same fsv.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self.create_btn.setEnabled(False)
        self.clear_btn.setEnabled(False)
        self.fsv_btn.setEnabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "same_fsv.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self.create_btn.setEnabled(True)
            self.clear_btn.setEnabled(True)
            self.fsv_btn.setEnabled(True)
            return

        self.thread = Worker3(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()

    def toggle_dark_mode(self, state):
        if state == QtCore.Qt.CheckState.Checked.value: # Dark mode is ON
            self.is_dark_mode = True
            self.main_window.setStyleSheet(self.dark_mode_stylesheet)
            # Update specific widget colors for dark mode
            self.dark_mode_checkbox.setStyleSheet("QCheckBox { color: #b39ddb; }")
            # Using objectName to target the title label specifically
            self.label_mnv.setStyleSheet("QLabel { color: #b39ddb; }")
            self.mnv.setStyleSheet("QLineEdit { color: #e0f2f7; }")
            self.log_output.setStyleSheet("QTextEdit { color: #eceff1; }") # Log output text color in dark mode

        else: # Light mode is ON
            self.is_dark_mode = False
            self.main_window.setStyleSheet(self.light_mode_stylesheet)
            # Update specific widget colors for light mode
            self.dark_mode_checkbox.setStyleSheet("QCheckBox { color: #ad1457; }")
            self.label_mnv.setStyleSheet("QLabel { color: #ad1457; }")
            self.mnv.setStyleSheet("QLineEdit { color: #4a148c; }")
            self.log_output.setStyleSheet("QTextEdit { color: #ad1457; }") # Log output text color in light mode

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    MainWindow.setWindowTitle("Grouping Tool")
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec())