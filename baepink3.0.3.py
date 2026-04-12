from PyQt6 import QtCore, QtGui, QtWidgets
import pandas as pd
from datetime import timedelta
import sys
import os
from pathlib import Path
import unicodedata
import re
from fuzzywuzzy import fuzz
import subprocess
import requests
import semver
import tempfile
import time
def read_and_map_data(file_path, log_emitter):
    """
    Reads data from an Excel or CSV file and maps columns based on a predefined dictionary.

    Args:
        file_path (str): The path to the input file.
        log_emitter (pyqtSignal): The signal to emit log messages.

    Returns:
        pd.DataFrame or None: The pandas DataFrame with mapped columns if successful, otherwise None.
    """
    try:
        log_emitter.emit("ℹ️ Đang đọc dữ liệu từ file...")
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension in ['.xlsx', '.xls']:
            log_emitter.emit("ℹ️ Phát hiện file Excel. Đang đọc...")
            data = pd.read_excel(file_path, engine='openpyxl')
        elif file_extension == '.csv':
            log_emitter.emit("ℹ️ Phát hiện file CSV. Đang đọc...")
            data = pd.read_csv(file_path)
        else:
            log_emitter.emit(f"❌ Lỗi: Định dạng file không được hỗ trợ: {file_extension}. Vui lòng chọn file Excel (.xlsx, .xls) hoặc CSV (.csv).")
            return None
        
        df = pd.DataFrame(data)

        # 1. Define the column mapping
        column_mapping = {
            'order_id': 'Order ID',
            'create_time': 'Order Creation Time',
            'buyer_id': 'Buyer User ID',
            'registration_time': 'Buyer Registration Time',
            'buyer_shipping_address': 'Buyer Recipient Address',
            'buyer_shipping_address_state': 'Buyer Recipient Address State',
            'buyer_shipping_address_city': 'Buyer Recipient Address City',
            'buyer_shipping_address_district': 'Buyer Recipient Address District',
            'recipient_phone_': 'Buyer Recipient Phone',
            'pv_promotion_id': 'PV Promotion ID',
            'ip_checkout': 'Checkout IP Address',
            'item_amount': '# Items',
            'gmv_vnd': 'Order Value (Checkout Amount)'
        }

        # 2. Check and rename columns
        for new_col, old_col in column_mapping.items():
            if old_col in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)
                log_emitter.emit(f"✅ Đã đổi tên cột '{old_col}' thành '{new_col}'.")
        
        # 3. Check for required columns after mapping
        required_columns = list(column_mapping.keys())
        if not all(col in df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in df.columns]
            log_emitter.emit(f"❗❗❗ Cảnh báo: File Excel đầu vào thiếu các cột sau: {', '.join(missing_cols)} để mapping.")
            # return None

        return df

    except FileNotFoundError:
        log_emitter.emit(f"❌ Lỗi: Không tìm thấy file Excel tại đường dẫn: {file_path}")
        return None
    except Exception as e:
        log_emitter.emit(f"❌ Đã xảy ra lỗi khi đọc file: {e}")
        return None
# --- APPLICATION VERSION & UPDATE CONFIGURATION ---
# IMPORTANT: Update this version with each new release!
APP_VERSION = "3.0.3" 

# URL to your version.txt file on GitHub (raw content)
VERSION_URL = "https://raw.githubusercontent.com/trungtien2410/bae/main/version.txt"

# This assumes your executable name is consistent in GitHub releases.
UPDATE_EXECUTABLE_NAME_TEMPLATE = "baepink{}.exe" 
#post
# Helper function to construct the download URL based on the latest version tag.
# This assumes your GitHub releases follow the pattern:
# https://github.com/trungtien2410/hang-ra-quay/releases/download/{TAG_NAME}/{ASSET_NAME}
# where TAG_NAME might be 'V1.2.0' if version.txt contains '1.2.0'.
# We will prepend 'V' to the version from version.txt to form the tag name.
def get_download_url(latest_version_from_txt):
    tag_name = f"V{latest_version_from_txt}"
    # --- CẬP NHẬT DÒNG NÀY ---
    asset_name = UPDATE_EXECUTABLE_NAME_TEMPLATE.format(latest_version_from_txt) # Sử dụng mẫu tên
    # -------------------------
    repo_owner = "trungtien2410"
    repo_name = "bae"
    return f"https://github.com/{repo_owner}/{repo_name}/releases/download/{tag_name}/{asset_name}"

# --- Update-specific QThreads ---

class CheckUpdateThread(QtCore.QThread):
    """Checks for the latest version from VERSION_URL."""
    check_finished = QtCore.pyqtSignal(bool, str) # success, latest_version_string or error_message

    def run(self):
        try:
            response = requests.get(VERSION_URL, timeout=30)
            response.raise_for_status() # Raise an exception for HTTP errors
            latest_version = response.text.strip()
            self.check_finished.emit(True, latest_version)
        except requests.exceptions.RequestException as e:
            self.check_finished.emit(False, f"Lỗi mạng khi kiểm tra cập nhật: {e}")
        except Exception as e:
            self.check_finished.emit(False, f"Lỗi không mong muốn khi kiểm tra cập nhật: {e}")

class DownloadUpdateThread(QtCore.QThread):
    """Downloads the update file and emits progress and status."""
    download_progress = QtCore.pyqtSignal(int, str) # percentage, status_text
    download_finished = QtCore.pyqtSignal(str) # Path to downloaded file
    download_error = QtCore.pyqtSignal(str) # Error message

    def __init__(self, latest_version_tag):
        super().__init__()
        self.latest_version_tag = latest_version_tag
        self._is_canceled = False

    def run(self):
        try:
            update_url = get_download_url(self.latest_version_tag)
            
            # This initial message will show up in the progress dialog
            self.download_progress.emit(0, f"🚀 Đang tải bản cập nhật...\n"
                                           f"⬇ Chuẩn bị tải từ: {update_url.split('//')[1].split('/')[0]}...")

            r = requests.get(update_url, stream=True, timeout=300) # 5-minute timeout
            r.raise_for_status()

            total_size = int(r.headers.get('content-length', 0))
            block_size = 8192 # 8KB chunks (common buffer size)
            temp_dir = tempfile.gettempdir()
            file_path = Path(temp_dir) / UPDATE_EXECUTABLE_NAME_TEMPLATE.format(self.latest_version_tag)

            downloaded = 0
            start_time = time.time()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(block_size):
                    if self._is_canceled: # Check for cancellation request
                        raise UserCancelledDownload("Download was cancelled by user.")
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        percent = int(downloaded * 100 / total_size) if total_size > 0 else 0
                        
                        elapsed = time.time() - start_time
                        speed_bps = downloaded / elapsed if elapsed > 0 else 0
                        speed_mbps = speed_bps / (1024 * 1024)
                        
                        remaining_bytes = total_size - downloaded
                        remaining_time_sec = remaining_bytes / speed_bps if speed_bps > 0 else 0

                        status_text = (
                            f"🚀 Đang tải... {percent}%\n"
                            f"⬇ {downloaded // (1024 * 1024)} MB / {total_size // (1024 * 1024)} MB "
                            f"({speed_mbps:.2f} MB/s)\n"
                            f"⏳ Còn lại: {int(remaining_time_sec)}s"
                        )
                        self.download_progress.emit(percent, status_text)
            
            self.download_finished.emit(str(file_path))

        except UserCancelledDownload:
            self.download_error.emit("Đã hủy cập nhật.")
            if 'file_path' in locals() and os.path.exists(file_path):
                os.remove(file_path) # Clean up partial download
        except requests.exceptions.RequestException as e:
            self.download_error.emit(f"Lỗi mạng khi tải bản cập nhật: {e}")
            if 'file_path' in locals() and os.path.exists(file_path):
                os.remove(file_path) # Clean up partial download
        except Exception as e:
            self.download_error.emit(f"Lỗi không mong muốn khi tải bản cập nhật: {e}")
            if 'file_path' in locals() and os.path.exists(file_path):
                os.remove(file_path) # Clean up partial download

    def requestInterruption(self):
        self._is_canceled = True

class UserCancelledDownload(Exception):
    """Custom exception for user canceling download."""
    pass


def resource_path(relative_path):
    """
    Lấy đường dẫn tuyệt đối đến một tài nguyên, xử lý cả môi trường phát triển và môi trường đã đóng gói bằng PyInstaller.
    """
    if hasattr(sys, '_MEIPASS'):
        return str(Path(sys._MEIPASS) / relative_path)
    else:
        return str(Path(__file__).parent / relative_path)



class Worker1(QtCore.QThread):
    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same promotion
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
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            drop_column = ["grass_hour","create_time","order_id","item_name","seller_id","shop_name","status_b","buyer_user_name","buyer_email","recipient_name","buyer_shipping_address","buyer_shipping_address_district","buyer_shipping_address_city","buyer_shipping_address_state","address_modified_time_latest","sz_device","ip_checkout","gmv_vnd","pv_promotion_cap","pv_promotion_name","pv_voucher_code","pv_rebate_by_shopee_vnd","is_nuv","sv_promotion_id","sv_voucher_code","coin_earn","coin_used_cash_amt","fsv_voucher_code","is_fsv_nuv","origin_shipping_fee_vnd","item_rebate_vnd","item_id","is_buyer_legit","is_seller_cb_seller","is_seller_official_shop","is_seller_preferred_seller","order_sn","buyer_cancel_reason"]
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            # Validate required columns
            required_columns = ['recipient_phone_', 'pv_promotion_id', 'buyer_id']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return
            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            df_processed = self.df.dropna(subset=['recipient_phone_', 'pv_promotion_id', 'buyer_id']).copy()

            grouped_df = df_processed.groupby(['recipient_phone_', 'pv_promotion_id'])['buyer_id'].nunique().reset_index(name='unique_buyer_ids_count')

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
                    filtered_groups[['recipient_phone_', 'pv_promotion_id']],
                    on=['recipient_phone_', 'pv_promotion_id'],
                    how='inner'
                )
                final_grouped_ids.update(merged_df['buyer_id'].unique())

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách{len(final_grouped_ids)} ID nhóm theo khuyến mãi tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (recipient_phone_, Promotion ID, >= 3 ID).")
            
            self.finished.emit(self.df)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)

class Worker2(QtCore.QThread):

    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu same FSV
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
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            drop_column = ["grass_hour","create_time","order_id","item_name","seller_id","shop_name","status_b","buyer_user_name","buyer_email","recipient_name","buyer_shipping_address","buyer_shipping_address_city","buyer_shipping_address_state","address_modified_time_latest","sz_device","ip_checkout","gmv_vnd","pv_promotion_cap","pv_promotion_name","pv_voucher_code","pv_rebate_by_shopee_vnd","is_nuv","sv_promotion_id","sv_voucher_code","coin_earn","coin_used_cash_amt","is_fsv_nuv","origin_shipping_fee_vnd","item_rebate_vnd","item_id","is_buyer_legit","is_seller_cb_seller","is_seller_official_shop","is_seller_preferred_seller","order_sn","buyer_cancel_reason"]
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            # Validate required columns
            required_columns = ['recipient_phone_', 'fsv_voucher_code', 'buyer_id', 'buyer_shipping_address_district']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            # Get all the group of > or more unique id have the same recipient_phone_ and the same pv_promotion_id
            # Ensure 'pv_promotion_id' is string to handle mixed types consistently
            # self.df['fsv_voucher_code'] = self.df['fsv_voucher_code'].astype(str)
            # Ensure 'recipient_phone_' and 'pv_promotion_id' are strings to handle mixed types consistently
            # self.df['recipient_phone_'] = self.df['recipient_phone_'].astype(str)
            # Group by recipient_phone_ and pv_promotion_id
            # Then, for each group, find the number of unique buyer_id's
            df_processed = self.df.dropna(subset=['recipient_phone_', 'fsv_voucher_code', 'buyer_id', 'buyer_shipping_address_district']).copy()

            grouped_df = df_processed.groupby(['recipient_phone_', 'fsv_voucher_code', 'buyer_shipping_address_district'])['buyer_id'].nunique().reset_index(name='unique_buyer_ids_count')

            # Filter for groups with 5 or more unique buyer_id's
            filtered_groups = grouped_df[grouped_df['unique_buyer_ids_count'] >= 5]

            final_grouped_ids = set()

            # For each filtered group (that has 5 or more unique buyer_ids),
            # get all buyer_id's from the original DataFrame that belong to these groups.
            # This is more efficient than iterating through rows.
            if not filtered_groups.empty:
                # Merge original df with filtered groups to get all buyer_ids
                merged_df = pd.merge(
                    self.df,
                    filtered_groups[['recipient_phone_', 'fsv_voucher_code', 'buyer_shipping_address_district']],
                    on=['recipient_phone_', 'fsv_voucher_code', 'buyer_shipping_address_district'],
                    how='inner'
                )
                final_grouped_ids.update(merged_df['buyer_id'].unique())

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách{len(final_grouped_ids)} ID nhóm theo khuyến mãi tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (recipient_phone_, fsv_voucher_code, buyer_shipping_address_district >= 5 ID).")
            
            self.finished.emit(self.df)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)
class Worker3(QtCore.QThread):

    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu same IP and create_time
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
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            self.df = self.df[self.df['ip_checkout'] != '-'].copy()
            # Keep 'registration_time' since it's now a condition
            drop_column = ["grass_hour", "order_id", "item_name", "seller_id", "shop_name", "status_b", 
                           "buyer_user_name", "buyer_email", "recipient_phone_", "recipient_name", 
                           "buyer_shipping_address", "buyer_shipping_address_district", 
                           "buyer_shipping_address_city", "buyer_shipping_address_state", 
                           "address_modified_time_latest", "sz_device", "N3", "gmv_vnd", 
                           "pv_promotion_id", "pv_promotion_cap", "pv_promotion_name", 
                           "pv_voucher_code", "pv_rebate_by_shopee_vnd", "is_nuv", "sv_promotion_id", 
                           "sv_voucher_code", "coin_earn", "coin_used_cash_amt", "fsv_voucher_code", 
                           "is_fsv_nuv", "origin_shipping_fee_vnd", "item_rebate_vnd", "item_id", 
                           "is_buyer_legit", "is_seller_cb_seller", "is_seller_official_shop", 
                           "is_seller_preferred_seller", "order_sn", "buyer_cancel_reason", 'registration_time']
            
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            
            required_columns = ['ip_checkout', 'create_time', 'buyer_id']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            # Đảm bảo cột 'create_time' và 'registration_time' là kiểu datetime
            self.df['create_time'] = pd.to_datetime(self.df['create_time'], errors='coerce')
            
            # Filter out rows where create_time is NaT
            # If a record has NaT for create_time, it can't be part of a time-constrained group
            df_filtered = self.df.dropna(subset=['create_time']).copy()
            # Sort by ip_checkout and create_time for optimized searching
            # Sorting by create_time primarily and then registration_time secondarily
            # ensures that for each ip_checkout, records are processed chronologically
            # by create_time, and then by registration_time if create_times are identical.
            # This is important for the 'start_time' and 'start_reg_time' logic.
            df_sorted = df_filtered.sort_values(by=['ip_checkout', 'create_time']).reset_index(drop=True)

            final_grouped_ids = set() # To store all unique IDs that belong to any valid group
            
            total_unique_ips = len(df_sorted['ip_checkout'].unique())
            processed_ips = 0

            # Iterate through each unique ip_checkout group
            for ip_checkout_val, group in df_sorted.groupby('ip_checkout'):
                processed_ips += 1
                self.progress.emit(int((processed_ips / total_unique_ips) * 100))

                records_for_ip = group.to_dict('records')
                
                i = 0
                while i < len(records_for_ip):
                    start_record = records_for_ip[i]
                    start_create_time = start_record['create_time']

                    current_potential_group_ids = [start_record['buyer_id']]
                    
                    j = i + 1
                    while j < len(records_for_ip):
                        next_record = records_for_ip[j]
                        next_create_time = next_record['create_time']

                        # Check if BOTH create_time are within 1 hour of their respective start times
                        if (next_create_time - start_create_time) <= timedelta(hours=1) <= timedelta(hours=1):
                            current_potential_group_ids.append(next_record['buyer_id'])
                            j += 1
                        else:
                            # If either time condition fails, stop extending the current group
                            break 
                    
                    unique_ids_in_group = set(current_potential_group_ids)
                    if len(unique_ids_in_group) >= 3:
                        final_grouped_ids.update(unique_ids_in_group)
                    
                    i += 1 # Move to the next potential starting record

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(final_grouped_ids)} ID nhóm tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (ít nhất 3 ID riêng biệt trong 1 giờ cho create_time).")
            
            self.finished.emit(True) # Indicate successful completion
            
        except FileNotFoundError:
            self.log.emit(f"❌ Lỗi: Không tìm thấy file tại đường dẫn: {self.input_file_path}")
            self.finished.emit(None)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
            self.finished.emit(None)
class Worker4(QtCore.QThread):

    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same promotion
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
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            drop_column = ["grass_hour","create_time","order_id","item_name","seller_id","shop_name","status_b","buyer_user_name","buyer_email","recipient_name","buyer_shipping_address","buyer_shipping_address_city","buyer_shipping_address_state","address_modified_time_latest","sz_device","ip_checkout","gmv_vnd","pv_promotion_cap","pv_promotion_name","pv_voucher_code","pv_rebate_by_shopee_vnd","is_nuv","sv_promotion_id","sv_voucher_code","coin_earn","coin_used_cash_amt","fsv_voucher_code","is_fsv_nuv","origin_shipping_fee_vnd","item_rebate_vnd","item_id","is_buyer_legit","is_seller_cb_seller","is_seller_official_shop","is_seller_preferred_seller","order_sn","buyer_cancel_reason"]
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            # Validate required columns
            required_columns = ['recipient_phone_', 'pv_promotion_id', 'buyer_id', 'buyer_shipping_address_district']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return
            # Ensure 'recipient_phone_' and 'pv_promotion_id' are strings to handle mixed types consistently
            # self.df['recipient_phone_'] = self.df['recipient_phone_'].astype(str)
            # self.df['pv_promotion_id'] = self.df['pv_promotion_id'].astype(str)
            # self.df['buyer_id'] = self.df['buyer_id'].astype(str)
            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            # Get all the group of > or more unique id have the same recipient_phone_ and the same pv_promotion_id
            # Ensure 'pv_promotion_id' is string to handle mixed types consistently
            # Group by recipient_phone_ and pv_promotion_id
            # Then, for each group, find the number of unique buyer_id's
            df_processed = self.df.dropna(subset=['recipient_phone_', 'pv_promotion_id', 'buyer_id']).copy()

            grouped_df = df_processed.groupby(['recipient_phone_', 'pv_promotion_id',"buyer_shipping_address_district"])['buyer_id'].nunique().reset_index(name='unique_buyer_ids_count')

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
                    filtered_groups[['recipient_phone_', 'pv_promotion_id',"buyer_shipping_address_district"]],
                    on=['recipient_phone_', 'pv_promotion_id',"buyer_shipping_address_district"],
                    how='inner'
                )
                final_grouped_ids.update(merged_df['buyer_id'].unique())

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(final_grouped_ids)} ID nhóm theo khuyến mãi tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (recipient_phone_, Promotion ID, buyer_shipping_address_district >= 3 ID).")
            
            self.finished.emit(self.df)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)
class Worker5(QtCore.QThread):
    progress = QtCore.pyqtSignal(int)
    log = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(object)

    def __init__(self, input_file_path, output_file_path):
        super().__init__()
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path

    def run(self):
        try:
            # Giả định hàm read_and_map_data đã được định nghĩa
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            
            # Giữ lại gmv_vnd để tính toán
            drop_column = ["grass_hour", "order_id", "item_name", "seller_id", "shop_name", "status_b", 
                            "buyer_user_name", "buyer_email", "recipient_phone_", "recipient_name", 
                            "buyer_shipping_address", "buyer_shipping_address_district", 
                            "buyer_shipping_address_city",  
                            "address_modified_time_latest", "sz_device", "N3", 
                            "pv_promotion_id", "pv_promotion_cap", "pv_promotion_name", 
                            "pv_voucher_code", "pv_rebate_by_shopee_vnd", "is_nuv", "sv_promotion_id", 
                            "sv_voucher_code", "coin_earn", "coin_used_cash_amt", "fsv_voucher_code", 
                            "is_fsv_nuv", "origin_shipping_fee_vnd", "item_rebate_vnd", "item_id", 
                            "is_buyer_legit", "is_seller_cb_seller", "is_seller_official_shop", 
                            "is_seller_preferred_seller", "order_sn", "buyer_cancel_reason"]
            
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            
            # Các cột bắt buộc cho logic mới
            required_columns = ["buyer_shipping_address_state", "buyer_id", "item_amount", "gmv_vnd"]
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: Thiếu cột: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang phân tích dữ liệu theo nhóm Address và Amount...")
            
            # Loại bỏ NaN và lọc item_amount <= 3 (nếu cần)
            df_filtered = self.df.dropna(subset=required_columns).copy()
            df_filtered = df_filtered[df_filtered['item_amount'] >= 3]
            # Sắp xếp theo gmv_vnd để tối ưu việc tìm kiếm chênh lệch 300k
            df_sorted = df_filtered.sort_values(by=['buyer_shipping_address_state', 'item_amount', 'gmv_vnd']).reset_index(drop=True)

            final_grouped_ids = set()
            
            # Nhóm theo Tỉnh thành và Số lượng sản phẩm
            grouped = df_sorted.groupby(['buyer_shipping_address_state', 'item_amount'])
            total_groups = len(grouped)
            processed_groups = 0

            for name, group in grouped:
                processed_groups += 1
                self.progress.emit(int((processed_groups / total_groups) * 100))
                
                records = group.to_dict('records')
                if len(records) < 4:
                    continue
                
                # Duyệt tìm các bản ghi có gmv_vnd chênh lệch <= 300,000
                for i in range(len(records)):
                    current_val = records[i]['gmv_vnd']
                    potential_group = [records[i]['buyer_id']]
                    
                    for j in range(i + 1, len(records)):
                        next_val = records[j]['gmv_vnd']
                        
                        # Vì đã sort theo gmv_vnd, nếu hiệu số > 300k thì các dòng sau cũng sẽ > 300k
                        if (next_val - current_val) <= 300000:
                            potential_group.append(records[j]['buyer_id'])
                        else:
                            break
                    
                    # Kiểm tra nếu có ít nhất 3 buyer_id khác nhau trong cụm này
                    if len(set(potential_group)) >= 4:
                        final_grouped_ids.update(set(potential_group))

            self.log.emit("ℹ️ Đang xuất file kết quả...")
            
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Thành công: Tìm thấy {len(final_grouped_ids)} ID thỏa mãn điều kiện.")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào thỏa mãn các tiêu chí so sánh giá trị đơn hàng.")
            
            self.finished.emit(True)
            
        except Exception as e:
            self.log.emit(f"❌ Lỗi hệ thống: {str(e)}")
            self.finished.emit(None)
class Worker6(QtCore.QThread):

    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same Recipient_Phone_
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
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            drop_column = ["grass_hour","create_time","order_id","item_name","seller_id","shop_name","status_b","buyer_user_name","buyer_email","recipient_name","buyer_shipping_address","buyer_shipping_address_city","buyer_shipping_address_state","address_modified_time_latest","sz_device","ip_checkout","gmv_vnd","pv_promotion_cap","pv_promotion_name","pv_voucher_code","pv_rebate_by_shopee_vnd","is_nuv","sv_promotion_id","sv_voucher_code","coin_earn","coin_used_cash_amt","fsv_voucher_code","is_fsv_nuv","origin_shipping_fee_vnd","item_rebate_vnd","item_id","is_buyer_legit","is_seller_cb_seller","is_seller_official_shop","is_seller_preferred_seller","order_sn","buyer_cancel_reason"]
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            # Validate required columns
            required_columns = ['recipient_phone_']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return
            self.log.emit("ℹ️ Đang xử lý dữ liệu...")

            df_processed = self.df.dropna(subset=['recipient_phone_']).copy()

            grouped_df = df_processed.groupby(['recipient_phone_'])['buyer_id'].nunique().reset_index(name='unique_buyer_ids_count')

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
                    filtered_groups[['recipient_phone_']],
                    on=['recipient_phone_'],
                    how='inner'
                )
                final_grouped_ids.update(merged_df['buyer_id'].unique())

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(final_grouped_ids)} ID nhóm theo khuyến mãi tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (recipient_phone_ >= 3 ID).")
            
            self.finished.emit(self.df)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)
class Worker7(QtCore.QThread):

    """
    QThread subclass to generate a Same_Similar_address document in a separate thread.
    This document identifies groups of at least 3 unique buyer_ids that share
    similar delivery address and same order value checkout (using fuzzy matching)
    and leverages a blocking technique for improved performance.
    Emits signals for progress, log messages, and completion status.
    """
    progress = QtCore.pyqtSignal(int)
    log = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(object) # Emits True on success, None on error/no data

    # Configuration parameters
    SIMILARITY_THRESHOLD = 85  # Adjust this value (0-100) for both name and address
    NAME_BLOCKING_WORDS = 7   # Number of words for name blocking
    ADDRESS_BLOCKING_WORDS = 2 # Number of words for address blocking (after cleaning)

    def __init__(self, input_file_path, output_file_path):
        """
        Initializes the Worker10 thread.
        
        Args:
            input_file_path (str): Path to the input Excel or CSV file.
            output_file_path (str): Path to save the output Excel file.
        """
        super().__init__()
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.df = None # To store the DataFrame
    # New helper function based on your VBA RemoveDiacritics
    def _remove_diacritics(self, text):
        """Removes Vietnamese diacritics (accents) from a string."""
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        # Use unicodedata for general diacritic removal
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
        return text

    def _clean_address_for_fuzzy_match(self, address):
        """
        Cleans address strings for more accurate fuzzy matching,
        incorporating VBA-like normalization (diacritics removal, specific replacements).
        """
        if pd.isna(address):
            return None
        text = str(address).lower()
        
        # --- Start of VBA-like normalization ---
        # 1. Remove diacritics (using a more robust method)
        text = self._remove_diacritics(text)
        
        # 2. Specific word replacements (as in your VBA)
        text = text.replace("phuong", "p")
        text = text.replace("quan", "q")
        text = text.replace("duong", "") # Removed "duong" as per your VBA logic
        # --- End of VBA-like normalization ---

        # Regex for common noise words (more specific patterns first)
        noise_words = [
            r'\bsố\s+nhà\b', r'\bngõ\b', r'\bđường\b', r'\bthôn\b', r'\btổ\b', r'\bkhu\s+phố\b',
            r'\bấp\b', r'\bkdc\b', r'\bchợ\b', r'\btrường\b', r'\bquán\b', r'\bhội\s+trường\b',
            r'\bnhà\s+văn\s+hoá\b', r'\bđội\b', r'\bbản\b', r'\bkhu\s+dân\s+cư\b',
            r'\bchân\s+dốc\b', r'\bđèo\b', r'\bngã\s+ba\b', r'\btoà\s+nhà\b', r'\bphường\b',
            r'\btownship\b', r'\bvillage\b', r'\bhamlet\b', r'\bstreet\b', r'\bhouse\b',
            r'\bxóm\b', r'\bkp\b', r'\bcty\b', r'\bcông\s+ty\b', r'\bchi\s+nhánh\b',
            r'\bchi\s+cục\b', r'\bcông\s+viên\b', r'\bkho\b', r'\bxưởng\b', r'\bkcn\b', # Industrial park
            r'\bkhu\s+công\s+nghiệp\b', r'\bthành\s+phố\b', r'\bquận\b', r'\bhuyện\b',
            r'\btỉnh\b'
        ]
        
        # Remove common prefixes like "so 42," "s 8a"
        text = re.sub(r'^\s*(so|s)\s+\d+[a-z]?\s*,?\s*', '', text)
        # Remove text within parentheses
        text = re.sub(r'\([^)]*\)', '', text)
        # Remove common separators
        text = re.sub(r'[.,;]', '', text)

        for noise in noise_words:
            text = re.sub(noise, ' ', text)

        # Consolidate spaces and strip leading/trailing spaces (Trim in VBA)
        text = re.sub(r'\s+', ' ', text).strip()
        # Final non-alphanumeric removal (after noise words are gone)
        text = re.sub(r'[^a-z0-9\s]', '', text)
        # Final consolidation of spaces
        text = re.sub(r'\s+', ' ', text).strip()

        return text if text else None

    def run(self):
        """
        Main method that executes the data processing logic in the thread.
        """
        try:
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            required_columns = ['buyer_id', 'gmv_vnd', 'buyer_shipping_address_district']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File thiếu các cột bắt buộc cho báo cáo này: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            # Drop rows with any NaN in required columns before further processing
            initial_rows = len(self.df)
            self.df.dropna(subset=required_columns, inplace=True)
            if len(self.df) < initial_rows:
                self.log.emit(f"ℹ️ Đã loại bỏ {initial_rows - len(self.df)} hàng có giá trị thiếu trong các cột bắt buộc.")

            if self.df.empty:
                self.log.emit("ℹ️ Không có dữ liệu hợp lệ sau khi loại bỏ các hàng thiếu thông tin bắt buộc.")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang chuẩn hóa địa chỉ...")
            self.df['cleaned_address'] = self.df['buyer_shipping_address_district'].apply(self._clean_address_for_fuzzy_match)

            # Drop rows where normalization/cleaning resulted in None
            initial_rows_after_norm = len(self.df)
            self.df.dropna(subset=['cleaned_address'], inplace=True)
            if len(self.df) < initial_rows_after_norm:
                self.log.emit(f"ℹ️ Đã loại bỏ {initial_rows_after_norm - len(self.df)} hàng có tên hoặc địa chỉ không hợp lệ sau chuẩn hóa.")
            
            if self.df.empty:
                self.log.emit("ℹ️ Không có dữ liệu hợp lệ sau khi chuẩn hóa địa chỉ.")
                self.finished.emit(None)
                return
            
            # --- Blocking Step for improved performance ---
            self.log.emit("ℹ️ Đang tạo các khối (block) dữ liệu để so sánh hiệu quả hơn...")
            
            # Create a unique ID for each original row, to easily refer back to it
            self.df['original_index'] = self.df.index 
            
            # Use 'records' for efficient iteration in Python loop
            records = self.df[['original_index', 'cleaned_address', 'buyer_id', 'gmv_vnd']].to_dict('records')

            # The hashmap/dictionary for blocking
            # Key: (address_block, order_value) -> Value: list of record_dicts
            blocks = {}

            for record in records:
                address_cleaned = record['cleaned_address']
                order_value = record['gmv_vnd']

                if address_cleaned is None or pd.isna(order_value):
                    continue # Skip records that couldn't be normalized/cleaned or have no value
                
                address_words = address_cleaned.split()
                address_block_key = " ".join(address_words[:self.ADDRESS_BLOCKING_WORDS])

                blocking_key = (address_block_key, order_value)
                
                if blocking_key not in blocks:
                    blocks[blocking_key] = []
                blocks[blocking_key].append(record)

            self.log.emit(f"ℹ️ Đã tạo {len(blocks)} khối dữ liệu.")
            # --- End Blocking Step ---

            final_grouped_buyer_ids = set()
            
            # To keep track of which original_indices have been added to a final group
            processed_original_indices = set() 

            total_blocks = len(blocks)
            processed_blocks_count = 0

            self.log.emit("ℹ️ Bắt đầu phân tích nhóm trong từng khối...")

            for blocking_key, block_records in blocks.items():
                processed_blocks_count += 1
                # Update progress, ensuring it doesn't go over 100%
                self.progress.emit(min(99, int((processed_blocks_count / total_blocks) * 100)))

                # If a block is too small, it can't meet the >=3 unique buyer_id criteria anyway
                if len(block_records) < 3:
                    continue

                # Within each block, perform pairwise fuzzy comparison
                block_processed_record_indices = set() 

                for i in range(len(block_records)):
                    current_record_in_block = block_records[i]
                    current_original_index = current_record_in_block['original_index']

                    if current_original_index in block_processed_record_indices or \
                       current_original_index in processed_original_indices:
                        continue
                    
                    current_address = current_record_in_block['cleaned_address']
                    current_order_value = current_record_in_block['gmv_vnd']

                    current_cluster_original_indices = [current_original_index]
                    current_cluster_buyer_ids = [current_record_in_block['buyer_id']]

                    for j in range(i + 1, len(block_records)):
                        other_record_in_block = block_records[j]
                        other_original_index = other_record_in_block['original_index']

                        if other_original_index in block_processed_record_indices or \
                           other_original_index in processed_original_indices:
                            continue

                        other_address = other_record_in_block['cleaned_address']
                        other_order_value = other_record_in_block['gmv_vnd']

                        # Ensure addresses and values are not None before comparing
                        if current_address is None or other_address is None or \
                           pd.isna(current_order_value) or pd.isna(other_order_value):
                            continue 

                        address_similarity = fuzz.token_sort_ratio(current_address, other_address)
                        is_exact_address_match = (current_address == other_address)
                        is_same_order_value = (current_order_value == other_order_value)
                        
                        if (is_exact_address_match or address_similarity >= self.SIMILARITY_THRESHOLD) and is_same_order_value:
                            current_cluster_original_indices.append(other_original_index)
                            current_cluster_buyer_ids.append(other_record_in_block['buyer_id'])
                    
                    # After comparing current_record with all others in the block, evaluate the cluster
                    unique_ids_in_cluster = set(current_cluster_buyer_ids)
                    
                    if len(unique_ids_in_cluster) >= 3:
                        final_grouped_buyer_ids.update(unique_ids_in_cluster)
                        processed_original_indices.update(current_cluster_original_indices)
                        block_processed_record_indices.update(current_cluster_original_indices)
                        
                        self.log.emit(f"✅ Tìm thấy nhóm hợp lệ trong khối '{blocking_key}' (địa chỉ và giá trị đơn hàng khớp). {len(unique_ids_in_cluster)} ID duy nhất.")

            self.log.emit("ℹ️ Đang lưu kết quả...")
            self.progress.emit(100) # Ensure progress is 100% at the end

            if final_grouped_buyer_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_buyer_ids), columns=['buyer_id'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(final_grouped_buyer_ids)} ID nhóm tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (ít nhất 3 ID riêng biệt với địa chỉ tương đồng và giá trị đơn hàng giống nhau).")
            
            self.finished.emit(True) # Indicate successful completion
            
        except FileNotFoundError:
            self.log.emit(f"❌ Lỗi: Không tìm thấy file tại đường dẫn: {self.input_file_path}")
            self.finished.emit(None)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
            self.finished.emit(None)
class Worker8(QtCore.QThread):

    """
    Tolerant Address Report
    
    QThread subclass to generate a Same_Similar_address document in a separate thread.
    This document identifies groups of at least 3 unique buyer_ids that share
    similar delivery address (using fuzzy matching) and an order value checkout
    with a difference of no more than 300,000 VND.
    """
    progress = QtCore.pyqtSignal(int)
    log = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(object) # Emits True on success, None on error/no data

    # Configuration parameters
    SIMILARITY_THRESHOLD = 85  # Adjust this value (0-100) for address
    ADDRESS_BLOCKING_WORDS = 2 # Number of words for address blocking (after cleaning)
    ORDER_VALUE_TOLERANCE = 300000 # Max difference for Order Value (Checkout Amount)

    def __init__(self, input_file_path, output_file_path):
        """
                
        Args:
            input_file_path (str): Path to the input Excel or CSV file.
            output_file_path (str): Path to save the output Excel file.
        """
        super().__init__()
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.df = None # To store the DataFrame

    # Helper function based on your VBA RemoveDiacritics
    def _remove_diacritics(self, text):
        """Removes Vietnamese diacritics (accents) from a string."""
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        # Use unicodedata for general diacritic removal
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
        return text

    def _clean_address_for_fuzzy_match(self, address):
        """
        Cleans address strings for more accurate fuzzy matching,
        incorporating VBA-like normalization (diacritics removal, specific replacements).
        """
        if pd.isna(address):
            return None
        text = str(address).lower()
        
        # --- Start of VBA-like normalization ---
        # 1. Remove diacritics (using a more robust method)
        text = self._remove_diacritics(text)
        
        # 2. Specific word replacements (as in your VBA)
        text = text.replace("phuong", "p")
        text = text.replace("quan", "q")
        text = text.replace("duong", "") # Removed "duong" as per your VBA logic
        # --- End of VBA-like normalization ---

        # Regex for common noise words (more specific patterns first)
        noise_words = [
            r'\bsố\s+nhà\b', r'\bngõ\b', r'\bđường\b', r'\bthôn\b', r'\btổ\b', r'\bkhu\s+phố\b',
            r'\bấp\b', r'\bkdc\b', r'\bchợ\b', r'\btrường\b', r'\bquán\b', r'\bhội\s+trường\b',
            r'\bnhà\s+văn\s+hoá\b', r'\bđội\b', r'\bbản\b', r'\bkhu\s+dân\s+cư\b',
            r'\bchân\s+dốc\b', r'\bđèo\b', r'\bngã\s+ba\b', r'\btoà\s+nhà\b', r'\bphường\b',
            r'\btownship\b', r'\bvillage\b', r'\bhamlet\b', r'\bstreet\b', r'\bhouse\b',
            r'\bxóm\b', r'\bkp\b', r'\bcty\b', r'\bcông\s+ty\b', r'\bchi\s+nhánh\b',
            r'\bchi\s+cục\b', r'\bcông\s+viên\b', r'\bkho\b', r'\bxưởng\b', r'\bkcn\b', # Industrial park
            r'\bkhu\s+công\s+nghiệp\b', r'\bthành\s+phố\b', r'\bquận\b', r'\bhuyện\b',
            r'\btỉnh\b'
        ]
        
        # Remove common prefixes like "so 42," "s 8a"
        text = re.sub(r'^\s*(so|s)\s+\d+[a-z]?\s*,?\s*', '', text)
        # Remove text within parentheses
        text = re.sub(r'\([^)]*\)', '', text)
        # Remove common separators
        text = re.sub(r'[.,;]', '', text)

        for noise in noise_words:
            text = re.sub(noise, ' ', text)

        # Consolidate spaces and strip leading/trailing spaces (Trim in VBA)
        text = re.sub(r'\s+', ' ', text).strip()
        # Final non-alphanumeric removal (after noise words are gone)
        text = re.sub(r'[^a-z0-9\s]', '', text)
        # Final consolidation of spaces
        text = re.sub(r'\s+', ' ', text).strip()

        return text if text else None

    def run(self):
        """
        Main method that executes the data processing logic in the thread.
        """
        try:
            # Assuming read_and_map_data is defined elsewhere
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            required_columns = ['buyer_id', 'Order Value (Checkout Amount)', 'buyer_shipping_address_district']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File thiếu các cột bắt buộc cho báo cáo này: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            # Drop rows with any NaN in required columns before further processing
            initial_rows = len(self.df)
            self.df.dropna(subset=required_columns, inplace=True)
            if len(self.df) < initial_rows:
                self.log.emit(f"ℹ️ Đã loại bỏ {initial_rows - len(self.df)} hàng có giá trị thiếu trong các cột bắt buộc.")

            if self.df.empty:
                self.log.emit("ℹ️ Không có dữ liệu hợp lệ sau khi loại bỏ các hàng thiếu thông tin bắt buộc.")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang chuẩn hóa địa chỉ...")
            self.df['cleaned_address'] = self.df['buyer_shipping_address_district'].apply(self._clean_address_for_fuzzy_match)

            # Drop rows where normalization/cleaning resulted in None
            initial_rows_after_norm = len(self.df)
            self.df.dropna(subset=['cleaned_address'], inplace=True)
            if len(self.df) < initial_rows_after_norm:
                self.log.emit(f"ℹ️ Đã loại bỏ {initial_rows_after_norm - len(self.df)} hàng có tên hoặc địa chỉ không hợp lệ sau chuẩn hóa.")
            
            if self.df.empty:
                self.log.emit("ℹ️ Không có dữ liệu hợp lệ sau khi chuẩn hóa địa chỉ.")
                self.finished.emit(None)
                return
            
            # --- Blocking Step for improved performance ---
            self.log.emit("ℹ️ Đang tạo các khối (block) dữ liệu để so sánh hiệu quả hơn...")
            
            # Create a unique ID for each original row, to easily refer back to it
            self.df['original_index'] = self.df.index 
            
            # Use 'records' for efficient iteration in Python loop
            records = self.df[['original_index', 'cleaned_address', 'buyer_id', 'Order Value (Checkout Amount)']].to_dict('records')

            # The hashmap/dictionary for blocking
            # Key: (address_block) -> Value: list of record_dicts
            blocks = {}

            for record in records:
                address_cleaned = record['cleaned_address']
                
                if address_cleaned is None:
                    continue # Skip records that couldn't be normalized/cleaned
                
                address_words = address_cleaned.split()
                address_block_key = " ".join(address_words[:self.ADDRESS_BLOCKING_WORDS])

                blocking_key = (address_block_key,)
                
                if blocking_key not in blocks:
                    blocks[blocking_key] = []
                blocks[blocking_key].append(record)

            self.log.emit(f"ℹ️ Đã tạo {len(blocks)} khối dữ liệu.")
            # --- End Blocking Step ---

            final_grouped_buyer_ids = set()
            
            # To keep track of which original_indices have been added to a final group
            processed_original_indices = set() 

            total_blocks = len(blocks)
            processed_blocks_count = 0

            self.log.emit("ℹ️ Bắt đầu phân tích nhóm trong từng khối...")

            for blocking_key, block_records in blocks.items():
                processed_blocks_count += 1
                # Update progress, ensuring it doesn't go over 100%
                self.progress.emit(min(99, int((processed_blocks_count / total_blocks) * 100)))

                # If a block is too small, it can't meet the >=3 unique buyer_id criteria anyway
                if len(block_records) < 3:
                    continue

                # Within each block, perform pairwise fuzzy comparison
                block_processed_record_indices = set() 

                for i in range(len(block_records)):
                    current_record_in_block = block_records[i]
                    current_original_index = current_record_in_block['original_index']

                    if current_original_index in block_processed_record_indices or \
                       current_original_index in processed_original_indices:
                        continue
                    
                    current_address = current_record_in_block['cleaned_address']
                    current_order_value = current_record_in_block['Order Value (Checkout Amount)']
                    
                    current_cluster_original_indices = [current_original_index]
                    current_cluster_buyer_ids = [current_record_in_block['buyer_id']]

                    for j in range(i + 1, len(block_records)):
                        other_record_in_block = block_records[j]
                        other_original_index = other_record_in_block['original_index']

                        if other_original_index in block_processed_record_indices or \
                           other_original_index in processed_original_indices:
                            continue

                        other_address = other_record_in_block['cleaned_address']
                        other_order_value = other_record_in_block['Order Value (Checkout Amount)']

                        # Ensure addresses and values are not None before comparing
                        if current_address is None or other_address is None or \
                           pd.isna(current_order_value) or pd.isna(other_order_value):
                            continue 

                        address_similarity = fuzz.token_sort_ratio(current_address, other_address)
                        is_exact_address_match = (current_address == other_address)
                        is_within_tolerance = abs(current_order_value - other_order_value) <= self.ORDER_VALUE_TOLERANCE
                        
                        if (is_exact_address_match or address_similarity >= self.SIMILARITY_THRESHOLD) and is_within_tolerance:
                            current_cluster_original_indices.append(other_original_index)
                            current_cluster_buyer_ids.append(other_record_in_block['buyer_id'])
                    
                    # After comparing current_record with all others in the block, evaluate the cluster
                    unique_ids_in_cluster = set(current_cluster_buyer_ids)
                    
                    if len(unique_ids_in_cluster) >= 3:
                        final_grouped_buyer_ids.update(unique_ids_in_cluster)
                        processed_original_indices.update(current_cluster_original_indices)
                        block_processed_record_indices.update(current_cluster_original_indices)
                        
                        self.log.emit(f"✅ Tìm thấy nhóm hợp lệ trong khối '{blocking_key}' (địa chỉ tương đồng và giá trị đơn hàng chênh lệch không quá {self.ORDER_VALUE_TOLERANCE:,} VND). {len(unique_ids_in_cluster)} ID duy nhất.")

            self.log.emit("ℹ️ Đang lưu kết quả...")
            self.progress.emit(100) # Ensure progress is 100% at the end

            if final_grouped_buyer_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_buyer_ids), columns=['buyer_id'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(final_grouped_buyer_ids)} ID nhóm tại: {self.output_file_path}")
            else:
                self.log.emit(f"ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (ít nhất 3 ID riêng biệt với địa chỉ tương đồng và giá trị đơn hàng chênh lệch không quá {self.ORDER_VALUE_TOLERANCE:,} VND).")
            
            self.finished.emit(True) # Indicate successful completion
            
        except FileNotFoundError:
            self.log.emit(f"❌ Lỗi: Không tìm thấy file tại đường dẫn: {self.input_file_path}")
            self.finished.emit(None)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
            self.finished.emit(None)
class Worker9(QtCore.QThread):

    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu RSL
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
    def _normalize_phone_number(self, phone):
        """Normalizes phone numbers to a consistent format (digits only)."""
        if pd.isna(phone):
            return None
        normalized_phone = re.sub(r'\D', '', str(phone))
        return normalized_phone if normalized_phone else None
    
    
    def run(self):
        try:
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return

            drop_column = ["grass_hour", "order_id", "item_name", "seller_id", "shop_name", "status_b", 
                           "buyer_user_name", "buyer_email", "recipient_name", 
                           "buyer_shipping_address", "buyer_shipping_address_district", 
                           "buyer_shipping_address_city", "buyer_shipping_address_state", 
                           "address_modified_time_latest", "sz_device", "N3", "gmv_vnd", 
                           "pv_promotion_id", "pv_promotion_cap", "pv_promotion_name", 
                           "pv_voucher_code", "pv_rebate_by_shopee_vnd", "is_nuv", "sv_promotion_id", 
                           "sv_voucher_code", "coin_earn", "coin_used_cash_amt", "fsv_voucher_code", 
                           "is_fsv_nuv", "origin_shipping_fee_vnd", "item_rebate_vnd", "item_id", 
                           "is_buyer_legit", "is_seller_cb_seller", "is_seller_official_shop", 
                           "is_seller_preferred_seller", "order_sn", "buyer_cancel_reason"]
            
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            
            # Add 'registration_time' to required columns
            required_columns = ['recipient_phone_', 'buyer_id']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang chuẩn hóa số điện thoại...")
            self.df['normalized_phone'] = self.df['recipient_phone_'].apply(self._normalize_phone_number)
            
            self.df.dropna(subset=['normalized_phone'], inplace=True)
            if self.df.empty:
                self.log.emit("ℹ️ Không có dữ liệu hợp lệ sau khi chuẩn hóa số điện thoại.")
                self.finished.emit(None)
                return
            
            final_grouped_ids = set()
            
            # Group by normalized phone number
            total_unique_phones = len(self.df['normalized_phone'].unique())
            processed_phones = 0

            # self.log.emit("ℹ️ Bắt đầu nhóm theo số điện thoại...")

            for phone_number, group in self.df.groupby('normalized_phone'):
                processed_phones += 1
                self.progress.emit(int((processed_phones / total_unique_phones) * 100))

                unique_buyer_ids_in_group = set(group['buyer_id'].tolist())
                
                # Check if the group has 4 or more unique buyer IDs
                if len(unique_buyer_ids_in_group) >= 4:
                    final_grouped_ids.update(unique_buyer_ids_in_group)
                    # self.log.emit(f"✅ Tìm thấy nhóm hợp lệ cho số điện thoại '{phone_number}': {len(unique_buyer_ids_in_group)} ID duy nhất.")

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['buyer_id'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(final_grouped_ids)} ID nhóm tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (ít nhất 4 ID riêng biệt có cùng số điện thoại).")
            
            self.finished.emit(True) 
            
        except FileNotFoundError:
            self.log.emit(f"❌ Lỗi: Không tìm thấy file tại đường dẫn: {self.input_file_path}")
            self.finished.emit(None)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
            self.finished.emit(None)
class Worker10(QtCore.QThread):

    """
    Similar address report
    QThread subclass to generate a Same_Similar_address document in a separate thread.
    This document identifies groups of at least 3 unique buyer_ids that share
    similar recipient names and similar delivery address districts (using fuzzy matching)
    and leverages a blocking technique for improved performance.
    Emits signals for progress, log messages, and completion status.
    """
    progress = QtCore.pyqtSignal(int)
    log = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(object) # Emits True on success, None on error/no data

    # Configuration parameters
    SIMILARITY_THRESHOLD = 85  # Adjust this value (0-100) for both name and address
    NAME_BLOCKING_LENGTH = 3   # Number of characters for name blocking
    NAME_BLOCKING_WORDS = 7   # Number of words for name blocking
    ADDRESS_BLOCKING_WORDS = 2 # Number of words for address blocking (after cleaning)

    def __init__(self, input_file_path, output_file_path):
        """
        Initializes the Worker6 thread.
        
        Args:
            input_file_path (str): Path to the input Excel or CSV file.
            output_file_path (str): Path to save the output Excel file.
        """
        super().__init__()
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.df = None # To store the DataFrame

    def _normalize_recipient_name(self, text):
        """
        Normalizes recipient names for better fuzzy matching.
        Converts to lowercase, removes diacritics, and cleans non-alphanumeric chars.
        """
        if pd.isna(text):
            return None
        text = str(text).lower()
        # Remove Vietnamese diacritics
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
        # Remove special characters, keep letters, numbers, and spaces
        text = re.sub(r'[^a-z0-9\s]', '', text)
        # Consolidate multiple spaces and strip leading/trailing spaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text if text else None # Return None if string becomes empty after cleaning

    # New helper function based on your VBA RemoveDiacritics
    def _remove_diacritics(self, text):
        """Removes Vietnamese diacritics (accents) from a string."""
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        # Use unicodedata for general diacritic removal
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
        return text

    def _clean_address_for_fuzzy_match(self, address):
        """
        Cleans address strings for more accurate fuzzy matching,
        incorporating VBA-like normalization (diacritics removal, specific replacements).
        """
        if pd.isna(address):
            return None
        text = str(address).lower()
        
        # --- Start of VBA-like normalization ---
        # 1. Remove diacritics (using a more robust method)
        text = self._remove_diacritics(text)
        
        # 2. Specific word replacements (as in your VBA)
        text = text.replace("phuong", "p")
        text = text.replace("quan", "q")
        text = text.replace("duong", "") # Removed "duong" as per your VBA logic
        # --- End of VBA-like normalization ---

        # Regex for common noise words (more specific patterns first)
        noise_words = [
            r'\bsố\s+nhà\b', r'\bngõ\b', r'\bđường\b', r'\bthôn\b', r'\btổ\b', r'\bkhu\s+phố\b',
            r'\bấp\b', r'\bkdc\b', r'\bchợ\b', r'\btrường\b', r'\bquán\b', r'\bhội\s+trường\b',
            r'\bnhà\s+văn\s+hoá\b', r'\bđội\b', r'\bbản\b', r'\bkhu\s+dân\s+cư\b',
            r'\bchân\s+dốc\b', r'\bđèo\b', r'\bngã\s+ba\b', r'\btoà\s+nhà\b', r'\bphường\b',
            r'\btownship\b', r'\bvillage\b', r'\bhamlet\b', r'\bstreet\b', r'\bhouse\b',
            r'\bxóm\b', r'\bkp\b', r'\bcty\b', r'\bcông\s+ty\b', r'\bchi\s+nhánh\b',
            r'\bchi\s+cục\b', r'\bcông\s+viên\b', r'\bkho\b', r'\bxưởng\b', r'\bkcn\b', # Industrial park
            r'\bkhu\s+công\s+nghiệp\b', r'\bthành\s+phố\b', r'\bquận\b', r'\bhuyện\b',
            r'\btỉnh\b'
        ]
        
        # Remove common prefixes like "so 42," "s 8a"
        text = re.sub(r'^\s*(so|s)\s+\d+[a-z]?\s*,?\s*', '', text)
        # Remove text within parentheses
        text = re.sub(r'\([^)]*\)', '', text)
        # Remove common separators
        text = re.sub(r'[.,;]', '', text)

        for noise in noise_words:
            text = re.sub(noise, ' ', text)

        # Consolidate spaces and strip leading/trailing spaces (Trim in VBA)
        text = re.sub(r'\s+', ' ', text).strip()
        # Final non-alphanumeric removal (after noise words are gone)
        text = re.sub(r'[^a-z0-9\s]', '', text)
        # Final consolidation of spaces
        text = re.sub(r'\s+', ' ', text).strip()

        return text if text else None

    def run(self):
        """
        Main method that executes the data processing logic in the thread.
        """
        try:
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return


            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            required_columns = ['buyer_id', 'item_name', 'buyer_shipping_address_district']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File thiếu các cột bắt buộc cho báo cáo này: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            # Drop rows with any NaN in required columns before further processing
            initial_rows = len(self.df)
            self.df.dropna(subset=required_columns, inplace=True)
            if len(self.df) < initial_rows:
                self.log.emit(f"ℹ️ Đã loại bỏ {initial_rows - len(self.df)} hàng có giá trị thiếu trong các cột bắt buộc.")

            if self.df.empty:
                self.log.emit("ℹ️ Không có dữ liệu hợp lệ sau khi loại bỏ các hàng thiếu thông tin bắt buộc.")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang chuẩn hóa tên người nhận và địa chỉ...")
            self.df['normalized_recipient_name'] = self.df['item_name'].apply(self._normalize_recipient_name)
            self.df['cleaned_address'] = self.df['buyer_shipping_address_district'].apply(self._clean_address_for_fuzzy_match)

            # Drop rows where normalization/cleaning resulted in None
            initial_rows_after_norm = len(self.df)
            self.df.dropna(subset=['normalized_recipient_name', 'cleaned_address'], inplace=True)
            if len(self.df) < initial_rows_after_norm:
                self.log.emit(f"ℹ️ Đã loại bỏ {initial_rows_after_norm - len(self.df)} hàng có tên hoặc địa chỉ không hợp lệ sau chuẩn hóa.")
            
            if self.df.empty:
                self.log.emit("ℹ️ Không có dữ liệu hợp lệ sau khi chuẩn hóa tên và địa chỉ.")
                self.finished.emit(None)
                return

            # --- Blocking Step for improved performance ---
            self.log.emit("ℹ️ Đang tạo các khối (block) dữ liệu để so sánh hiệu quả hơn...")
            
            # Create a unique ID for each original row, to easily refer back to it
            self.df['original_index'] = self.df.index 
            
            # Use 'records' for efficient iteration in Python loop
            records = self.df[['original_index', 'normalized_recipient_name', 'cleaned_address', 'buyer_id']].to_dict('records')
            
            # The hashmap/dictionary for blocking
            # Key: (name_block, address_block) -> Value: list of record_dicts
            blocks = {}

            for record in records:
                # Ensure blocking keys are created, handle potential None after cleaning
                name_block = record['normalized_recipient_name']
                address_cleaned = record['cleaned_address']

                if name_block is None or address_cleaned is None:
                    continue # Skip records that couldn't be normalized/cleaned
                name_block_words = name_block.split()
                name_block_key = " ".join(name_block_words[:self.NAME_BLOCKING_WORDS])
                address_words = address_cleaned.split()
                address_block_key = " ".join(address_words[:self.ADDRESS_BLOCKING_WORDS])

                blocking_key = (name_block_key, address_block_key)
                
                if blocking_key not in blocks:
                    blocks[blocking_key] = []
                blocks[blocking_key].append(record)

            self.log.emit(f"ℹ️ Đã tạo {len(blocks)} khối dữ liệu.")
            # --- End Blocking Step ---

            final_grouped_buyer_ids = set()
            
            # To keep track of which original_indices have been added to a final group
            processed_original_indices = set() 

            total_blocks = len(blocks)
            processed_blocks_count = 0

            self.log.emit("ℹ️ Bắt đầu phân tích nhóm trong từng khối...")

            for blocking_key, block_records in blocks.items():
                processed_blocks_count += 1
                # Update progress, ensuring it doesn't go over 100%
                self.progress.emit(min(99, int((processed_blocks_count / total_blocks) * 100)))

                # If a block is too small, it can't meet the >=3 unique buyer_id criteria anyway
                if len(block_records) < 3:
                    continue

                # Within each block, perform pairwise fuzzy comparison
                # We need to ensure we don't re-process records that were already grouped *in this block*
                # and efficiently find all members of a cluster.
                
                # Use a local set for this block to manage processed records
                block_processed_record_indices = set() 

                for i in range(len(block_records)):
                    current_record_in_block = block_records[i]
                    current_original_index = current_record_in_block['original_index']

                    # Skip if this record has already been part of a group formed in this block, or a global group
                    if current_original_index in block_processed_record_indices or \
                       current_original_index in processed_original_indices:
                        continue
                    
                    current_name = current_record_in_block['normalized_recipient_name']
                    current_address = current_record_in_block['cleaned_address']
                    
                    # This list will hold the original_indices of records belonging to the current cluster
                    current_cluster_original_indices = [current_original_index]
                    current_cluster_buyer_ids = [current_record_in_block['buyer_id']]

                    # Compare this record with all subsequent records in the block
                    for j in range(i + 1, len(block_records)):
                        other_record_in_block = block_records[j]
                        other_original_index = other_record_in_block['original_index']

                        if other_original_index in block_processed_record_indices or \
                           other_original_index in processed_original_indices:
                            continue

                        other_name = other_record_in_block['normalized_recipient_name']
                        other_address = other_record_in_block['cleaned_address']

                        # Ensure names and addresses are not None before comparing
                        if current_name is None or other_name is None or \
                           current_address is None or other_address is None:
                            continue # Should be handled by dropna earlier, but as a safeguard

                        name_similarity = fuzz.ratio(current_name, other_name)
                        address_similarity = fuzz.token_sort_ratio(current_address, other_address)

                        # Check for fuzzy similarity
                        is_fuzzy_similar = (name_similarity >= self.SIMILARITY_THRESHOLD and \
                                            address_similarity >= self.SIMILARITY_THRESHOLD)
                        
                        # Check for exact address match after cleaning (inspired by your VBA logic)
                        # This would implicitly pass a high SIMILARITY_THRESHOLD, but it highlights the intent.
                        is_exact_address_match = (current_address == other_address)

                        if is_fuzzy_similar or is_exact_address_match: # Combine criteria
                            current_cluster_original_indices.append(other_original_index)
                            current_cluster_buyer_ids.append(other_record_in_block['buyer_id'])
                    
                    # After comparing current_record with all others in the block, evaluate the cluster
                    unique_ids_in_cluster = set(current_cluster_buyer_ids)
                    
                    if len(unique_ids_in_cluster) >= 3:
                        # Add these buyer IDs to the final set
                        final_grouped_buyer_ids.update(unique_ids_in_cluster)
                        
                        # Mark these records as processed to avoid re-clustering them as a starting point
                        # or processing them again in other blocks if they happen to fall into multiple blocks
                        processed_original_indices.update(current_cluster_original_indices)
                        block_processed_record_indices.update(current_cluster_original_indices)
                        
                        # Log message based on the type of match found
                        if is_exact_address_match: # This condition might be true for the last comparison
                             self.log.emit(f"✅ Tìm thấy nhóm hợp lệ trong khối '{blocking_key}' (địa chỉ chuẩn hóa chính xác): {len(unique_ids_in_cluster)} ID duy nhất.")
                        else:
                             self.log.emit(f"✅ Tìm thấy nhóm hợp lệ trong khối '{blocking_key}' (tên và địa chỉ tương đồng): {len(unique_ids_in_cluster)} ID duy nhất.")


            self.log.emit("ℹ️ Đang lưu kết quả...")
            self.progress.emit(100) # Ensure progress is 100% at the end

            if final_grouped_buyer_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_buyer_ids), columns=['buyer_id'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(final_grouped_buyer_ids)} ID nhóm tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (ít nhất 3 ID riêng biệt với tên/địa chỉ tương đồng).")
            
            self.finished.emit(True) # Indicate successful completion
            
        except FileNotFoundError:
            self.log.emit(f"❌ Lỗi: Không tìm thấy file tại đường dẫn: {self.input_file_path}")
            self.finished.emit(None)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
            self.finished.emit(None)
class Worker11(QtCore.QThread):
    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu N3 6 - 9
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
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            drop_column = ["grass_hour","create_time","order_id","item_name","seller_id","shop_name","status_b","buyer_user_name","buyer_email","recipient_phone_","recipient_name","buyer_shipping_address","buyer_shipping_address_district","buyer_shipping_address_city","buyer_shipping_address_state","address_modified_time_latest","sz_device","ip_checkout","gmv_vnd","pv_promotion_id","pv_promotion_cap","pv_promotion_name","pv_voucher_code","pv_rebate_by_shopee_vnd","is_nuv","sv_promotion_id","sv_voucher_code","coin_earn","coin_used_cash_amt","fsv_voucher_code","is_fsv_nuv","origin_shipping_fee_vnd","item_rebate_vnd","item_id","is_buyer_legit","is_seller_cb_seller","is_seller_official_shop","is_seller_preferred_seller","order_sn","buyer_cancel_reason"]
            # Validate required columns
            self.df = self.df.drop(columns=drop_column, errors='ignore')
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
            
            # Xử lý các dòng NaT trong cột 'registration_time'
            df_filtered = self.df.dropna(subset=['registration_time']).copy()
            
            # Sắp xếp dữ liệu theo N3 và registration_time để tối ưu hóa việc tìm kiếm
            df_sorted = df_filtered.sort_values(by=['N3', 'registration_time']).reset_index(drop=True)

            final_grouped_ids_ten = set() # Set lưu trữ tất cả các ID duy nhất thuộc về bất kỳ nhóm hợp lệ
            final_grouped_ids_six_to_nine = set() # Set lưu trữ tất cả các ID duy nhất thuộc về nhóm có từ 6 đến 9 ID

            total_phone_nums = len(df_sorted['N3'].unique())
            processed_phone_nums = 0

            # Lặp qua từng nhóm N3 duy nhất
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
                            j += 1
                        else:
                            # If the time gap is too large, stop extending the current group
                            break
                    # After attempting to build the group, evaluate its size
                    unique_ids_in_group = set(current_group_ids)
                    # If the group has more than or equal to 10 unique IDs, add them to the final set
                    if len(unique_ids_in_group) >= 10:
                        final_grouped_ids_ten.update(unique_ids_in_group)
                    # If the group has 6-9 unique IDs, we can choose to include them as well
                    if len(unique_ids_in_group) >= 6:
                        final_grouped_ids_six_to_nine.update(unique_ids_in_group)
                    # Move 'i' to the next record after the current group
                    i += 1
            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Khởi tạo df_output_ids với giá trị mặc định là None hoặc DataFrame rỗng
            # Initialization to handle the case where either set might be empty
            df_output_ids = None 
            df_six_to_nine = None
            
            # --- Bắt đầu logic xử lý và kết hợp DataFrame ---
            
            # 1. Xử lý nhóm >=10
            if final_grouped_ids_ten:
                df_output_ids = pd.DataFrame(list(final_grouped_ids_ten), columns=['ID >=10'])
            
            # 2. Xử lý nhóm 6-9, loại bỏ ID đã có trong nhóm >=10
            # Note: The set operation is 'final_grouped_ids_six_to_nine - final_grouped_ids_ten'
            ids_six_to_nine_only = final_grouped_ids_six_to_nine - final_grouped_ids_ten
            
            if ids_six_to_nine_only:
                df_six_to_nine = pd.DataFrame(list(ids_six_to_nine_only), columns=['ID 6-9'])
                
                # Nếu df_output_ids đã được tạo (tức là có ID >= 10), thì nối nó với df_six_to_nine
                if df_output_ids is not None:
                    df_output_ids = pd.concat([df_output_ids, df_six_to_nine], axis=1)
                else:
                    # Nếu chưa có ID >= 10, thì df_six_to_nine chính là DataFrame đầu ra
                    df_output_ids = df_six_to_nine
                    
            # --- Kết thúc logic xử lý và kết hợp DataFrame ---
                    
            # 3. Lưu kết quả nếu có bất kỳ ID nào được nhóm
            if df_output_ids is not None:
                # Nếu đã tạo được df_output_ids (dù chỉ từ nhóm >=10 hoặc chỉ từ nhóm 6-9 hoặc cả hai)
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(df_output_ids)} ID nhóm tại: {self.output_file_path}")
            else:
                # Nếu không tìm thấy bất kỳ ID nào trong cả hai nhóm
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (ít nhất 6 ID riêng biệt trong 1 giờ).")
            
            self.finished.emit(True) # Indicate successful completion
            
        except FileNotFoundError:
            self.log.emit(f"❌ Lỗi: Không tìm thấy file Excel tại đường dẫn: {self.input_file_path}")
            self.finished.emit(None)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
            self.finished.emit(None)

class Worker12(QtCore.QThread):
    
    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same phone NUV with threshold 6 unique buyer_id's
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
            # 1. Read Data
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            
            # 2. Drop Columns
            drop_column = ["grass_hour","create_time","order_id","item_name","seller_id","shop_name","status_b","buyer_user_name","buyer_email","recipient_name","buyer_shipping_address","buyer_shipping_address_city","buyer_shipping_address_state","address_modified_time_latest","sz_device","ip_checkout","gmv_vnd","pv_promotion_cap","pv_promotion_name","pv_voucher_code","pv_rebate_by_shopee_vnd","is_nuv","sv_promotion_id","sv_voucher_code","coin_earn","coin_used_cash_amt","fsv_voucher_code","is_fsv_nuv","origin_shipping_fee_vnd","item_rebate_vnd","item_id","is_buyer_legit","is_seller_cb_seller","is_seller_official_shop","is_seller_preferred_seller","order_sn","buyer_cancel_reason"]
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            
            # 3. Validate required columns
            required_columns = ['recipient_phone_', 'buyer_id']
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            # 4. Data Preparation: Drop rows with missing key values
            df_processed = self.df.dropna(subset=required_columns).copy()

            # ******************************************************
            # 5. Core Logic Adjustment: Group by Phone count UNIQUE Buyer IDs
            # ******************************************************
            grouped_summary = df_processed.groupby(
                ['recipient_phone_']
            )['buyer_id'].nunique().reset_index(name='unique_buyer_ids_count')

            # ******************************************************
            # 6. Filter Groups
            # Get the groups (phone) that meet the criteria
            # ******************************************************       
            # Groups with more than 6 unique buyer_id's
            filtered_groups_six = grouped_summary[
                (grouped_summary['unique_buyer_ids_count'] >= 6)]

            # ******************************************************
            # 7. Optimized ID Retrieval using Merging 🚀
            # ******************************************************
            ids_more_than_six = set()
            if not filtered_groups_six.empty:
                # Merge original df with the 6-9 filtered groups summary
                merged_six_to_nine = pd.merge(
                    df_processed,
                    filtered_groups_six['recipient_phone_'],
                    on=['recipient_phone_'],
                    how='inner'
                )
                ids_more_than_six.update(merged_six_to_nine['buyer_id'].unique())

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # 8. Create Output DataFrame
            
            # Create a list of Series (columns) to concatenate
            output_columns = []
            

            if ids_more_than_six:
                output_columns.append(pd.Series(list(ids_more_than_six), name='ID >=6'))

            if output_columns:
                # Concatenate all available Series into a single DataFrame
                df_output_ids = pd.concat(output_columns, axis=1)
                
                # Save to Excel
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Hoàn thành! Đã lưu {len(ids_more_than_six)} ID vào file.")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (recipient_phone_>= 6 ID).")
            
            self.finished.emit(self.df)
            
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)
class Worker13(QtCore.QThread):
    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu N3 -4
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
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            drop_column = ["grass_hour","create_time","order_id","item_name","seller_id","shop_name","status_b","buyer_user_name","buyer_email","recipient_phone_","recipient_name","buyer_shipping_address","buyer_shipping_address_district","buyer_shipping_address_city","buyer_shipping_address_state","address_modified_time_latest","sz_device","ip_checkout","gmv_vnd","pv_promotion_id","pv_promotion_cap","pv_promotion_name","pv_voucher_code","pv_rebate_by_shopee_vnd","is_nuv","sv_promotion_id","sv_voucher_code","coin_earn","coin_used_cash_amt","fsv_voucher_code","is_fsv_nuv","origin_shipping_fee_vnd","item_rebate_vnd","item_id","is_buyer_legit","is_seller_cb_seller","is_seller_official_shop","is_seller_preferred_seller","order_sn","buyer_cancel_reason"]
            # Validate required columns
            self.df = self.df.drop(columns=drop_column, errors='ignore')
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
            
            # Xử lý các dòng NaT trong cột 'registration_time'
            df_filtered = self.df.dropna(subset=['registration_time']).copy()
            """
            # ... (Phần tiền xử lý dữ liệu giữ nguyên cho đến đoạn df_sorted)

            # Đảm bảo dữ liệu đã sắp xếp đúng
            df_sorted = df_filtered.sort_values(by=['N3', 'registration_time']).reset_index(drop=True)

            # --- BẮT ĐẦU TỐI ƯU VECTORIZATION ---

            # 1. Tính khoảng cách thời gian giữa các dòng liên tiếp (đơn vị: giây)
            # .diff() sẽ lấy dòng sau trừ dòng trước
            df_sorted['time_diff'] = df_sorted.groupby('N3')['registration_time'].diff().dt.total_seconds()

            # 2. Xác định điểm bắt đầu của một nhóm mới (nếu khoảng cách > 3600 giây hoặc là dòng đầu của N3)
            # Kết quả là một mảng Boolean: True nếu là điểm khởi đầu chuỗi mới
            is_new_group = (df_sorted['time_diff'] > 3600) | (df_sorted['time_diff'].isna())

            # 3. Đánh số ID cho từng cụm (Sub-group) bằng hàm cộng dồn tích lũy (cumsum)
            df_sorted['sub_group_id'] = is_new_group.cumsum()

            # 4. Gom nhóm theo (N3 + sub_group_id) và đếm số lượng ID duy nhất
            group_counts = df_sorted.groupby(['N3', 'sub_group_id'])['buyer_id'].nunique()

            # 5. Lọc các nhóm có từ 4 ID duy nhất trở lên
            valid_groups = group_counts[group_counts >= 4].index

            # 6. Lấy danh sách buyer_id thuộc các nhóm thỏa mãn
            final_ids = df_sorted[
                df_sorted.set_index(['N3', 'sub_group_id']).index.isin(valid_groups)
            ]['buyer_id'].unique()

            # Tạo DataFrame kết quả
            df_more_than_four = pd.DataFrame(final_ids, columns=['ID >=4'])

            # --- KẾT THÚC TỐI ƯU --- TỐI ƯU VECTORIZATION CHO BÁO CÁO N3 -4 CÓ THỜI GIAN THÌ ĐỌC LẠI
            """


            # Sắp xếp dữ liệu theo N3 và registration_time để tối ưu hóa việc tìm kiếm
            df_sorted = df_filtered.sort_values(by=['N3', 'registration_time']).reset_index(drop=True)


            final_grouped_ids_more_than_four = set() # Set lưu trữ tất cả các ID duy nhất thuộc về nhóm có từ 4 ID trở lên

            total_phone_nums = len(df_sorted['N3'].unique())
            processed_phone_nums = 0

            # Lặp qua từng nhóm N3 duy nhất
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
                            j += 1
                        else:
                            # If the time gap is too large, stop extending the current group
                            break
                    # After attempting to build the group, evaluate its size
                    unique_ids_in_group = set(current_group_ids)

                    if len(unique_ids_in_group) >= 4:
                        final_grouped_ids_more_than_four.update(unique_ids_in_group)
                    # Move 'i' to the next record after the current group
                    i += 1
            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Khởi tạo df_output_ids với giá trị mặc định là None hoặc DataFrame rỗng
            # Initialization to handle the case where either set might be empty
            df_more_than_four = None
            
            # --- Bắt đầu logic xử lý và kết hợp DataFrame ---
            

            
            if final_grouped_ids_more_than_four:
                df_more_than_four = pd.DataFrame(list(final_grouped_ids_more_than_four), columns=['ID >=4'])
                   
            # --- Kết thúc logic xử lý và kết hợp DataFrame ---
                    
            # 3. Lưu kết quả nếu có bất kỳ ID nào được nhóm
            if df_more_than_four is not None:
                # Nếu đã tạo được df_output_ids (dù chỉ từ nhóm >=10 hoặc chỉ từ nhóm 6-9 hoặc cả hai)
                df_more_than_four.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(df_more_than_four)} ID nhóm tại: {self.output_file_path}")
            else:
                # Nếu không tìm thấy bất kỳ ID nào trong cả hai nhóm
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (ít nhất 4 ID riêng biệt trong 1 giờ).")
            
            self.finished.emit(True) # Indicate successful completion
            
        except FileNotFoundError:
            self.log.emit(f"❌ Lỗi: Không tìm thấy file Excel tại đường dẫn: {self.input_file_path}")
            self.finished.emit(None)
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
            self.finished.emit(None)

class Worker14(QtCore.QThread):
    
    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same IP and Create time within 01 hour Report with threshold 6 unique buyer_id's
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
            # 1. Read Data and Initial Cleaning
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return

            self.df = self.df[self.df['ip_checkout'] != '-'].copy()
            
            # 2. Define Columns and Validate
            drop_column = ["grass_hour", "order_id", "item_name", "seller_id", "shop_name", "status_b", 
                           "buyer_user_name", "buyer_email", "recipient_phone_", "recipient_name", 
                           "buyer_shipping_address", "address_modified_time_latest", "sz_device", "N3", "gmv_vnd", 
                           "pv_promotion_id", "pv_promotion_cap", "pv_promotion_name", 
                           "pv_voucher_code", "pv_rebate_by_shopee_vnd", "is_nuv", "sv_promotion_id", 
                           "sv_voucher_code", "coin_earn", "coin_used_cash_amt", "fsv_voucher_code", 
                           "is_fsv_nuv", "origin_shipping_fee_vnd", "item_rebate_vnd", "item_id", 
                           "is_buyer_legit", "is_seller_cb_seller", "is_seller_official_shop", 
                           "is_seller_preferred_seller", "order_sn", "buyer_cancel_reason", 'registration_time']
            
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            
            GROUPING_KEYS = ['ip_checkout']
            REQUIRED_COLUMNS = ['create_time', 'buyer_id'] + GROUPING_KEYS
            
            if not all(col in self.df.columns for col in REQUIRED_COLUMNS):
                missing_cols = [col for col in REQUIRED_COLUMNS if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            # 3. Data Preparation
            self.df['create_time'] = pd.to_datetime(self.df['create_time'], errors='coerce')
            df_processed = self.df.dropna(subset=REQUIRED_COLUMNS).copy()
            
            # Sort by keys AND time for correct sequential processing
            df_processed.sort_values(by=GROUPING_KEYS + ['create_time'], inplace=True)
            
            # 4. Core Logic: Fixed Start Time Grouping
            ids_more_than_six = set()
            
            total_unique_groups = len(df_processed.groupby(GROUPING_KEYS))
            processed_groups = 0

            # Iterate through each unique combined key group
            for key, group in df_processed.groupby(GROUPING_KEYS, sort=False):
                processed_groups += 1
                self.progress.emit(int((processed_groups / total_unique_groups) * 100))

                group_list = group.to_dict('records')
                i = 0
                
                while i < len(group_list):
                    start_record = group_list[i]
                    start_create_time = start_record['create_time']
                    
                    # Find the index of the last record within 1 hour of the start_create_time
                    # We can leverage the fact that the group is already sorted by create_time
                    j = i + 1
                    while j < len(group_list):
                        if (group_list[j]['create_time'] - start_create_time) <= timedelta(hours=1):
                            j += 1
                        else:
                            break
                    
                    # The cluster includes records from index i up to (but not including) j
                    current_cluster = group_list[i:j]
                    
                    # Get the unique buyer IDs in this fixed 1-hour window
                    unique_ids_in_cluster = set(record['buyer_id'] for record in current_cluster)
                    count = len(unique_ids_in_cluster)

                    if count >= 6:
                        # Get the matching threshold group
                        ids_more_than_six.update(unique_ids_in_cluster)
                        # The next window MUST start after this cluster ends (index j)
                        i = j
                    else:
                        # If the cluster is < 6, the next potential starting record is the next one
                        i += 1 
            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # 5. Create Output DataFrame (same as before)
            output_columns = []
            
            if ids_more_than_six:
                output_columns.append(pd.Series(list(ids_more_than_six), name='ID >=6'))

            if output_columns:
                df_output_ids = pd.concat(output_columns, axis=1)
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Hoàn thành! Đã lưu {len(ids_more_than_six)} ID vào file.")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (Same IP >= 6 ID trong 1 giờ).")
            
            self.finished.emit(self.df)
            
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)
class Worker15(QtCore.QThread):
   
    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same IP and Create time within 01 hour Report with threshold 4 unique buyer_id's
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
            # 1. Read Data and Initial Cleaning
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return

            self.df = self.df[self.df['ip_checkout'] != '-'].copy()
            
            # 2. Define Columns and Validate
            drop_column = ["grass_hour", "order_id", "item_name", "seller_id", "shop_name", "status_b", 
                           "buyer_user_name", "buyer_email", "recipient_phone_", "recipient_name", 
                           "buyer_shipping_address", "address_modified_time_latest", "sz_device", "N3", "gmv_vnd", 
                           "pv_promotion_id", "pv_promotion_cap", "pv_promotion_name", 
                           "pv_voucher_code", "pv_rebate_by_shopee_vnd", "is_nuv", "sv_promotion_id", 
                           "sv_voucher_code", "coin_earn", "coin_used_cash_amt", "fsv_voucher_code", 
                           "is_fsv_nuv", "origin_shipping_fee_vnd", "item_rebate_vnd", "item_id", 
                           "is_buyer_legit", "is_seller_cb_seller", "is_seller_official_shop", 
                           "is_seller_preferred_seller", "order_sn", "buyer_cancel_reason", 'registration_time']
            
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            
            GROUPING_KEYS = ['ip_checkout']
            REQUIRED_COLUMNS = ['create_time', 'buyer_id'] + GROUPING_KEYS
            
            if not all(col in self.df.columns for col in REQUIRED_COLUMNS):
                missing_cols = [col for col in REQUIRED_COLUMNS if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            # 3. Data Preparation
            self.df['create_time'] = pd.to_datetime(self.df['create_time'], errors='coerce')
            df_processed = self.df.dropna(subset=REQUIRED_COLUMNS).copy()
            
            # Sort by keys AND time for correct sequential processing
            df_processed.sort_values(by=GROUPING_KEYS + ['create_time'], inplace=True)
            
            # 4. Core Logic: Fixed Start Time Grouping
            ids_more_than_four = set()
            
            total_unique_groups = len(df_processed.groupby(GROUPING_KEYS))
            processed_groups = 0

            # Iterate through each unique combined key group
            for key, group in df_processed.groupby(GROUPING_KEYS, sort=False):
                processed_groups += 1
                self.progress.emit(int((processed_groups / total_unique_groups) * 100))

                group_list = group.to_dict('records')
                i = 0
                
                while i < len(group_list):
                    start_record = group_list[i]
                    start_create_time = start_record['create_time']
                    
                    # Find the index of the last record within 1 hour of the start_create_time
                    # We can leverage the fact that the group is already sorted by create_time
                    j = i + 1
                    while j < len(group_list):
                        if (group_list[j]['create_time'] - start_create_time) <= timedelta(hours=1):
                            j += 1
                        else:
                            break
                    
                    # The cluster includes records from index i up to (but not including) j
                    current_cluster = group_list[i:j]
                    
                    # Get the unique buyer IDs in this fixed 1-hour window
                    unique_ids_in_cluster = set(record['buyer_id'] for record in current_cluster)
                    count = len(unique_ids_in_cluster)

                    if count >= 4:
                        # Get the matching threshold group
                        ids_more_than_four.update(unique_ids_in_cluster)
                        # The next window MUST start after this cluster ends (index j)
                        i = j
                    else:
                        # If the cluster is < 4, the next potential starting record is the next one
                        i += 1 
            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # 5. Create Output DataFrame (same as before)
            output_columns = []
            
            if ids_more_than_four:
                output_columns.append(pd.Series(list(ids_more_than_four), name='ID >=4'))

            if output_columns:
                df_output_ids = pd.concat(output_columns, axis=1)
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Hoàn thành! Đã lưu {len(ids_more_than_four)} ID vào file.")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (Same IP >= 4 ID trong 1 giờ).")
            
            self.finished.emit(self.df)
            
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)

class Worker16(QtCore.QThread):
   
    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same Domain and Registration time within 01 hour Report with threshold 6  unique buyer_id's
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
            # 1. Read Data and Initial Cleaning
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return

            # 1. Định nghĩa danh sách các domain cần loại bỏ
            excluded_domains = ['gmail.com', 'yahoo.com.vn', 'yahoo.com', 'icloud.com', 'privaterelay.appleid.com']

            # 2. Lọc bỏ các dòng có domain nằm trong danh sách trên
            self.df = self.df[~self.df['domain'].isin(excluded_domains)].copy()

            # 2. Define Columns and Validate
            drop_column = ["grass_hour", "order_id", "item_name", "seller_id", "shop_name", "status_b", 
                           "buyer_user_name", "buyer_email", "recipient_phone_", "recipient_name", 
                           "buyer_shipping_address", "address_modified_time_latest", "sz_device", "N3", "gmv_vnd", 
                           "pv_promotion_id", "pv_promotion_cap", "pv_promotion_name", 
                           "pv_voucher_code", "pv_rebate_by_shopee_vnd", "is_nuv", "sv_promotion_id", 
                           "sv_voucher_code", "coin_earn", "coin_used_cash_amt", "fsv_voucher_code", 
                           "is_fsv_nuv", "origin_shipping_fee_vnd", "item_rebate_vnd", "item_id", 
                           "is_buyer_legit", "is_seller_cb_seller", "is_seller_official_shop", 
                           "is_seller_preferred_seller", "order_sn", "buyer_cancel_reason", 'ip_checkout']
            
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            
            GROUPING_KEYS = ['domain']
            REQUIRED_COLUMNS = ['registration_time', 'buyer_id'] + GROUPING_KEYS
            
            if not all(col in self.df.columns for col in REQUIRED_COLUMNS):
                missing_cols = [col for col in REQUIRED_COLUMNS if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            # 3. Data Preparation
            self.df['registration_time'] = pd.to_datetime(self.df['registration_time'], errors='coerce')
            df_processed = self.df.dropna(subset=REQUIRED_COLUMNS).copy()
            
            # Sort by keys AND time for correct sequential processing
            df_processed.sort_values(by=GROUPING_KEYS + ['registration_time'], inplace=True)
            
            # 4. Core Logic: Fixed Start Time Grouping
            ids_more_than_four = set()
            
            total_unique_groups = len(df_processed.groupby(GROUPING_KEYS))
            processed_groups = 0

            # Iterate through each unique combined key group
            for key, group in df_processed.groupby(GROUPING_KEYS, sort=False):
                processed_groups += 1
                self.progress.emit(int((processed_groups / total_unique_groups) * 100))

                group_list = group.to_dict('records')
                i = 0
                
                while i < len(group_list):
                    start_record = group_list[i]
                    start_registration_time = start_record['registration_time']
                    
                    # Find the index of the last record within 1 hour of the start_registration_time
                    # We can leverage the fact that the group is already sorted by registration_time
                    j = i + 1
                    while j < len(group_list):
                        if (group_list[j]['registration_time'] - start_registration_time) <= timedelta(hours=1):
                            j += 1
                        else:
                            break
                    
                    # The cluster includes records from index i up to (but not including) j
                    current_cluster = group_list[i:j]
                    
                    # Get the unique buyer IDs in this fixed 1-hour window
                    unique_ids_in_cluster = set(record['buyer_id'] for record in current_cluster)
                    count = len(unique_ids_in_cluster)

                    if count >= 6:
                        # Get the matching threshold group
                        ids_more_than_four.update(unique_ids_in_cluster)
                        # The next window MUST start after this cluster ends (index j)
                        i = j
                    else:
                        # If the cluster is < 6, the next potential starting record is the next one
                        i += 1 
            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # 5. Create Output DataFrame (same as before)
            output_columns = []
            
            if ids_more_than_four:
                output_columns.append(pd.Series(list(ids_more_than_four), name='ID >=6'))

            if output_columns:
                df_output_ids = pd.concat(output_columns, axis=1)
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Hoàn thành! Đã lưu {len(ids_more_than_four)} ID vào file.")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (Same Domain >= 6 ID trong 1 giờ).")
            
            self.finished.emit(self.df)
            
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)
class Worker17(QtCore.QThread):
    """
    Same city and district + reg time
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same State + City and Create time within 01 hour Report with threshold 6 unique buyer_id's và create_time - registration_time <= 20 phút
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
            # 1. Read Data and Initial Cleaning
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            # Chỉ lấy các dòng có email là rỗng
            self.df['buyer_email'] = self.df['buyer_email'].fillna('')
            self.df = self.df[self.df['buyer_email'] == ''].copy()
            
            # 2. Define Columns and Validate
            drop_column = ["grass_hour", "order_id", "item_name", "seller_id", "shop_name", "status_b", 
                           "buyer_user_name",  "recipient_phone_", "recipient_name", 
                           "buyer_shipping_address", "address_modified_time_latest", "sz_device", "N3", "gmv_vnd", 
                           "pv_promotion_id", "pv_promotion_cap", "pv_promotion_name", 
                           "pv_voucher_code", "pv_rebate_by_shopee_vnd", "is_nuv", "sv_promotion_id", 
                           "sv_voucher_code", "coin_earn", "coin_used_cash_amt", "fsv_voucher_code", 
                           "is_fsv_nuv", "origin_shipping_fee_vnd", "item_rebate_vnd", "item_id", 
                           "is_buyer_legit", "is_seller_cb_seller", "is_seller_official_shop", 
                           "is_seller_preferred_seller", "order_sn", "buyer_cancel_reason"]
            
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            
            GROUPING_KEYS = ['buyer_shipping_address_state', 
                             'buyer_shipping_address_city']
            REQUIRED_COLUMNS = ['create_time', 'buyer_id','registration_time'] + GROUPING_KEYS
            
            if not all(col in self.df.columns for col in REQUIRED_COLUMNS):
                missing_cols = [col for col in REQUIRED_COLUMNS if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            
            # 3. Data Preparation
            self.df['create_time'] = pd.to_datetime(self.df['create_time'], errors='coerce')
            self.df['registration_time'] = pd.to_datetime(self.df['registration_time'], errors='coerce')
            self.df['time_diff_minutes'] = (self.df['create_time'] - self.df['registration_time']).dt.total_seconds() / 60

            # Filter out records where time_diff_minutes > 20
            self.df = self.df[self.df['time_diff_minutes'] <= 20]
            df_processed = self.df.dropna(subset=REQUIRED_COLUMNS).copy()
            
            # Sort by keys AND time for correct sequential processing
            df_processed.sort_values(by=GROUPING_KEYS + ['create_time'], inplace=True)
            
            # 4. Core Logic: Fixed Start Time Grouping
            ids_more_than_six = set()
            
            total_unique_groups = len(df_processed.groupby(GROUPING_KEYS))
            processed_groups = 0

            # Iterate through each unique combined key group
            for key, group in df_processed.groupby(GROUPING_KEYS, sort=False):
                processed_groups += 1
                self.progress.emit(int((processed_groups / total_unique_groups) * 100))

                group_list = group.to_dict('records')
                i = 0
                
                while i < len(group_list):
                    start_record = group_list[i]
                    start_create_time = start_record['create_time']
                    
                    # Find the index of the last record within 1 hour of the start_create_time
                    # We can leverage the fact that the group is already sorted by create_time
                    j = i + 1
                    while j < len(group_list):
                        if (group_list[j]['create_time'] - start_create_time) <= timedelta(hours=1):
                            j += 1
                        else:
                            break
                    
                    # The cluster includes records from index i up to (but not including) j
                    current_cluster = group_list[i:j]
                    
                    # Get the unique buyer IDs in this fixed 1-hour window
                    unique_ids_in_cluster = set(record['buyer_id'] for record in current_cluster)
                    count = len(unique_ids_in_cluster)

                    if count >= 6:
                        # Get the matching threshold group
                        ids_more_than_six.update(unique_ids_in_cluster)
                        # The next window MUST start after this cluster ends (index j)
                        i = j
                    else:
                        # If the cluster is < 6, the next potential starting record is the next one
                        i += 1 
            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # 5. Create Output DataFrame (same as before)
            output_columns = []
            
            if ids_more_than_six:
                output_columns.append(pd.Series(list(ids_more_than_six), name='ID >=6'))

            if output_columns:
                df_output_ids = pd.concat(output_columns, axis=1)
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Hoàn thành! Đã lưu {len(ids_more_than_six)} ID vào file.")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí Same State + City and Create time within 01 hour Report with threshold 6 unique buyer_id's và create_time - registration_time <= 20 phút).")
            
            self.finished.emit(self.df)
            
        except Exception as e:
            self.log.emit(f"❌ Đã xảy ra lỗi: {str(e)}")
            self.finished.emit(None)
class Worker18(QtCore.QThread):

    """
    Lớp con của QThread để thực hiện việc nhóm dữ liệu Same Name + District + City + State
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
            self.df = read_and_map_data(self.input_file_path, self.log)
            if self.df is None:
                self.finished.emit(None)
                return
            drop_column = ["grass_hour","create_time","order_id","item_name","seller_id","shop_name","status_b","buyer_user_name","buyer_email","buyer_shipping_address","address_modified_time_latest","sz_device","ip_checkout","gmv_vnd","pv_promotion_cap","pv_promotion_name","pv_voucher_code","pv_rebate_by_shopee_vnd","is_nuv","sv_promotion_id","sv_voucher_code","coin_earn","coin_used_cash_amt","is_fsv_nuv","origin_shipping_fee_vnd","item_rebate_vnd","item_id","is_buyer_legit","is_seller_cb_seller","is_seller_official_shop","is_seller_preferred_seller","order_sn","buyer_cancel_reason"]
            self.df = self.df.drop(columns=drop_column, errors='ignore')
            # Validate required columns
            required_columns = ['buyer_id', 'buyer_shipping_address_district', "buyer_shipping_address_city", "buyer_shipping_address_state", "recipient_name"]
            if not all(col in self.df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in self.df.columns]
                self.log.emit(f"❌ Lỗi: File Excel thiếu các cột bắt buộc: {', '.join(missing_cols)}")
                self.finished.emit(None)
                return

            self.log.emit("ℹ️ Đang xử lý dữ liệu...")
            # Get all the group of > or more unique id have the same recipient_phone_ and the same pv_promotion_id
            # Ensure 'pv_promotion_id' is string to handle mixed types consistently
            # self.df['fsv_voucher_code'] = self.df['fsv_voucher_code'].astype(str)
            # Ensure 'recipient_phone_' and 'pv_promotion_id' are strings to handle mixed types consistently
            # self.df['recipient_phone_'] = self.df['recipient_phone_'].astype(str)
            # Group by recipient_phone_ and pv_promotion_id
            # Then, for each group, find the number of unique buyer_id's
            df_processed = self.df.dropna(subset=['buyer_id', 'buyer_shipping_address_district', "buyer_shipping_address_city", "buyer_shipping_address_state", "recipient_name"]).copy()

            grouped_df = df_processed.groupby([ 'buyer_shipping_address_district', "buyer_shipping_address_city", "buyer_shipping_address_state", "recipient_name"])['buyer_id'].nunique().reset_index(name='unique_buyer_ids_count')

            # Filter for groups with 6 or more unique buyer_id's
            filtered_groups = grouped_df[grouped_df['unique_buyer_ids_count'] >= 6]

            final_grouped_ids = set()

            # For each filtered group (that has 6 or more unique buyer_ids),
            # get all buyer_id's from the original DataFrame that belong to these groups.
            # This is more efficient than iterating through rows.
            if not filtered_groups.empty:
                # Merge original df with filtered groups to get all buyer_ids
                merged_df = pd.merge(
                    self.df,
                    filtered_groups[[ 'buyer_shipping_address_district', "buyer_shipping_address_city", "buyer_shipping_address_state", "recipient_name"]],
                    on=['buyer_shipping_address_district', "buyer_shipping_address_city", "buyer_shipping_address_state", "recipient_name"],
                    how='inner'
                )
                final_grouped_ids.update(merged_df['buyer_id'].unique())

            self.log.emit("ℹ️ Đang lưu kết quả...")
            
            # Create a DataFrame for the final grouped IDs (single column)
            if final_grouped_ids:
                df_output_ids = pd.DataFrame(list(final_grouped_ids), columns=['ID'])
                df_output_ids.to_excel(self.output_file_path, index=False, engine='openpyxl')
                self.log.emit(f"✅ Đã lưu danh sách {len(final_grouped_ids)} ID nhóm theo khuyến mãi tại: {self.output_file_path}")
            else:
                self.log.emit("ℹ️ Không tìm thấy ID nào để nhóm theo tiêu chí (buyer_shipping_address_district, buyer_shipping_address_city, buyer_shipping_address_state, recipient_name >= 6 ID).")
            
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
                color: black;
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
            QCheckBox::text {
                color: #ad1457; /* Darker Pink - Checkbox Text */
                font-size: 13px;
                padding-left: 8px;
            }
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
                background-color: #00aeef; /* Blue Pastel - Buttons */
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 8px;
                font-size: 14px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #0281b0; /* Darker Blue Pastel - Button Hover */
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
                color: #00aeef; /* Light Purple - Labels */
                font-size: 13pt;
            }
            QProgressBar {
                height: 24px;
                border-radius: 8px;
                background: #546e7a; /* Grey Blue - Progress Bar Background */
            }
            QProgressBar::chunk {
                background-color: #00aeef; /* Medium Purple - Progress Bar Chunk */
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
                background: #00aeef; /* Dark Purple - Selected Tab */
                color: white;
                font-weight: bold;
            }
            QTabBar::tab:hover {
                background: #0281b0; /* Slightly Darker Purple - Tab Hover */
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
                background: #00aeef; /* Dark Blue Grey - Scrollbar Background */
                border: none;
                width: 12px;
                height: 12px;
            }
            QScrollBar::handle:vertical, QScrollBar::handle:horizontal {
                background: #00aeef; /* Dark Purple - Scrollbar Handle */
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
                color: #00aeef; /* Lighter purple */
                padding-bottom: 10px;
            }
            /* --- Styles for the Pure QSS Sliding Switch (No Images) --- */
            QCheckBox {
                /* Ensure the text color is correct for the light/dark mode state */
                color: #263238;
                font-weight: bold;
                spacing: 5px; /* Adjust spacing between indicator and text */
            }

            QCheckBox::indicator {
                /* 1. Define the wide track size and shape */
                width: 25px; 
                height: 25px;
                border-radius: 12px; /* Perfect pill shape */
                border: 1px solid #546e7a; /* Default border */
            }

            /* --- UNCHECKED STATE (Switch OFF - Grey Track, Dark Thumb on Left) --- */
            QCheckBox::indicator:unchecked {
                /* Track color: Grey Blue */
                background-color: #546e7a;
                border-color: #546e7a;
                
                /* 2. Create the Dark Thumb on the LEFT using a CONIC gradient trick */
                /* This creates a small dark circle on the left edge */
                background: qconicalgradient(
                    cx:0.3, cy:0.5, angle:0, 
                    stop:0.0 #37474f,      /* Darker color for the thumb */
                    stop:0.35 #37474f,
                    stop:0.35 #546e7a,     /* Transition back to track color */
                    stop:1.0 #546e7a
                );
                
                /* Optional: Small inner shadow for depth (cannot replicate inset perfectly) */
                /* box-shadow is not supported, so rely on the gradient */
            }

            QCheckBox::indicator:unchecked:hover {
                /* Hover effect */
                border-color: #00aeef;
            }
            QCheckBox::checked {
                color: #ffffff; /* Light Grey text when checked */
            }
            /* --- CHECKED STATE (Switch ON - Theme Blue Track, Light Thumb on Right) --- */
            QCheckBox::indicator:checked {
                /* Track color: Your theme's primary blue */
                background-color: #00aeef; 
                border-color: #00aeef;
                
                /* 2. Create the Light Thumb on the RIGHT using a CONIC gradient trick */
                /* We adjust the cx/cy and color stops to position the thumb on the right */
                background: qconicalgradient(
                    cx:0.7, cy:0.5, angle:0, 
                    stop:0.0 #00aeef,       /* Start with track color */
                    stop:0.65 #00aeef,
                    stop:0.65 #eceff1,      /* Transition to Light Grey/White for the thumb */
                    stop:1.0 #eceff1
                );
            }

            # QCheckBox::indicator:checked:hover {
            #     /* Hover effect */
            #     background-color: #0281b0;
            #     border-color: #0281b0;
            # }
        """
        self.is_dark_mode = False # Track current mode
        MainWindow.setStyleSheet(self.dark_mode_stylesheet) # Apply initial stylesheet
        # Interactive Buttons
        
        # --- Central Widget & Main Layout ---
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        main_layout = QtWidgets.QVBoxLayout(self.centralwidget)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)

        # --- 1. Dark Mode Toggle (Top Right) ---
        dark_mode_layout = QtWidgets.QHBoxLayout()
        dark_mode_layout.addStretch()
        self.dark_mode_checkbox = QtWidgets.QCheckBox("Dark Mode")
        self.dark_mode_checkbox.setFont(QtGui.QFont("Segoe UI", 11))
        self.dark_mode_checkbox.setStyleSheet("QCheckBox { color: #0281b0; }")
        self.dark_mode_checkbox.stateChanged.connect(self.toggle_dark_mode)
        dark_mode_layout.addWidget(self.dark_mode_checkbox)
        main_layout.addLayout(dark_mode_layout)

        # --- 2. Input Section (Shared for all tabs) ---
        input_group = QtWidgets.QVBoxLayout()
        
        # File gốc
        label_mnv_layout = QtWidgets.QHBoxLayout()
        self.label_mnv = QtWidgets.QLabel("File gốc:")
        self.mnv = QtWidgets.QLineEdit()
        self.mnv.setPlaceholderText("Đường dẫn đến file Excel gốc...")
        self.chose_file_btn = QtWidgets.QPushButton("Chọn file gốc")
        self.chose_file_btn.clicked.connect(self.choose_file)
        label_mnv_layout.addWidget(self.label_mnv)
        label_mnv_layout.addWidget(self.mnv)
        label_mnv_layout.addWidget(self.chose_file_btn)
        
        # Thư mục đích
        label_mnv_layout2 = QtWidgets.QHBoxLayout()
        self.label_mnv2 = QtWidgets.QLabel("Thư mục đích:")
        self.destination_folder = QtWidgets.QLineEdit()
        self.destination_folder.setPlaceholderText("Đường dẫn đến thư mục đích...")
        self.chose_folder_btn = QtWidgets.QPushButton("Chọn thư mục đích")
        self.chose_folder_btn.clicked.connect(self.choose_folder)
        label_mnv_layout2.addWidget(self.label_mnv2)
        label_mnv_layout2.addWidget(self.destination_folder)
        label_mnv_layout2.addWidget(self.chose_folder_btn)

        input_group.addLayout(label_mnv_layout)
        input_group.addLayout(label_mnv_layout2)
        main_layout.addLayout(input_group)

        # --- 3. Tab Widget ---
        self.tabWidget = QtWidgets.QTabWidget()
        main_layout.addWidget(self.tabWidget)

        # --- TAB 1: NHÓM 1 ---
        self.tab1 = QtWidgets.QWidget()
        self.tab1_layout = QtWidgets.QVBoxLayout(self.tab1)
        
        t1_content_layout = QtWidgets.QHBoxLayout()
        t1_btn_col = QtWidgets.QVBoxLayout()
        t1_btn_col2 = QtWidgets.QVBoxLayout()
        t1_btn_col3 = QtWidgets.QVBoxLayout()
        # Khởi tạo nút Tab 1
        self.same_promotion_phone_btn = QtWidgets.QPushButton("Same Promotion + Phone Report") #ok
        self.ip_create_time_btn = QtWidgets.QPushButton("Same IP and Create Time Report") #ok
        self.same_prm_phone_district_btn = QtWidgets.QPushButton("Same Promotion + Phone + District Report")#ok
        self.rsl_item_btn = QtWidgets.QPushButton("Same RSL Item Report") #ok
        self.same_recipient_phone_btn = QtWidgets.QPushButton("Same Recipient Phone Report") #ok
        self.same_order_value_check_out_and_similar_address_btn = QtWidgets.QPushButton("Same Order Value + Similar Address")#ok
        self.tolerant_address_btn = QtWidgets.QPushButton("Tolerant Address Report")#ok
        self.rsl_btn = QtWidgets.QPushButton("RSL Report")#ok
        self.similiar_address_btn = QtWidgets.QPushButton("Similar Address Report")#ok

        # Kết nối sự kiện Tab 1
        self.same_promotion_phone_btn.clicked.connect(self.same_promotion_phone)
        self.ip_create_time_btn.clicked.connect(self.same_ip_check_out)
        self.same_prm_phone_district_btn.clicked.connect(self.same_prm_phone_district)
        self.rsl_item_btn.clicked.connect(self.rsl_item)
        self.same_recipient_phone_btn.clicked.connect(self.same_recipient_phone)
        self.same_order_value_check_out_and_similar_address_btn.clicked.connect(self.same_order_value_check_out_and_similar_address)
        self.tolerant_address_btn.clicked.connect(self.tolerant_address)
        self.rsl_btn.clicked.connect(self.rsl_report)
        self.similiar_address_btn.clicked.connect(self.similiar_address)
        
        for btn in [self.same_promotion_phone_btn,
                    self.ip_create_time_btn,
                    self.same_prm_phone_district_btn
                    ]:
            t1_btn_col.addWidget(btn)
        for btn in [self.rsl_item_btn,
                    self.same_recipient_phone_btn,
                    self.same_order_value_check_out_and_similar_address_btn]:
            t1_btn_col2.addWidget(btn)

        for btn in [
                    self.tolerant_address_btn,
                    self.rsl_btn,
                    self.similiar_address_btn]:
            t1_btn_col3.addWidget(btn)
        # Cột Checkbox Tab 1
        t1_check_col = QtWidgets.QVBoxLayout()
        t1_check_col2 = QtWidgets.QVBoxLayout()
        t1_check_col3 = QtWidgets.QVBoxLayout()
        self.same_promotion_phone_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.ip_create_time_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.same_prm_phone_district_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.rsl_item_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.same_recipient_phone_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.same_order_value_check_out_and_similar_address_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.tolerant_address_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.rsl_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.similiar_address_btn_checkbox = QtWidgets.QCheckBox('Checked')
        
        for cb in [self.same_promotion_phone_btn_checkbox, self.ip_create_time_btn_checkbox, self.same_prm_phone_district_btn_checkbox]:
            t1_check_col.addWidget(cb)
            
        for cb in [self.rsl_item_btn_checkbox, self.same_recipient_phone_btn_checkbox, self.same_order_value_check_out_and_similar_address_btn_checkbox]:
            t1_check_col2.addWidget(cb)
        for cb in [self.tolerant_address_btn_checkbox,self.rsl_btn_checkbox,self.similiar_address_btn_checkbox]:
            t1_check_col3.addWidget(cb)
            

        t1_content_layout.addLayout(t1_btn_col)
        t1_content_layout.addLayout(t1_check_col)

        t1_content_layout.addLayout(t1_btn_col2)
        t1_content_layout.addLayout(t1_check_col2)

        t1_content_layout.addLayout(t1_btn_col3)
        t1_content_layout.addLayout(t1_check_col3)

        self.tab1_layout.addLayout(t1_content_layout)
        self.tab1_layout.addStretch()

        # --- TAB 2: NHÓM 2 ---
        self.tab2 = QtWidgets.QWidget()
        self.tab2_layout = QtWidgets.QVBoxLayout(self.tab2)
        
        t2_content_layout = QtWidgets.QHBoxLayout()
        t2_btn_col = QtWidgets.QVBoxLayout()
        t2_btn_col2 = QtWidgets.QVBoxLayout()
        
        # Khởi tạo nút Tab 2
        self.fsv_btn = QtWidgets.QPushButton("Same FSV Report")
        self.N3_btn = QtWidgets.QPushButton("N3 Report - 6 - 9")
        self.N3_btn_4 = QtWidgets.QPushButton("N3 Report - 4")
        self.same_phone_btn = QtWidgets.QPushButton("Same Phone -6 Report")
        self.same_name_district_city_state_btn = QtWidgets.QPushButton("Same Name + District + City + State")
        self.same_ip_create_reg_time_4_btn = QtWidgets.QPushButton("Same IP and Create + RegTime 4 Report")
        self.same_ip_create_reg_time_6_btn = QtWidgets.QPushButton("Same IP and Create + RegTime 6 Report")
        self.same_domain_reg_time_report_btn = QtWidgets.QPushButton("Same Domain + Reg Time Report")
        self.same_city_district_reg_time_report_btn = QtWidgets.QPushButton("Same City + State + Reg Time Report")

        # Kết nối sự kiện Tab 2
        self.fsv_btn.clicked.connect(self.same_fsv_input)
        self.N3_btn.clicked.connect(self.N3_report)
        self.N3_btn_4.clicked.connect(self.N3_report_4)
        self.same_phone_btn.clicked.connect(self.same_phone_report)
        self.same_name_district_city_state_btn.clicked.connect(self.same_name_district_city_state_report)
        self.same_ip_create_reg_time_4_btn.clicked.connect(self.same_ip_create_time_4_report)
        self.same_ip_create_reg_time_6_btn.clicked.connect(self.same_ip_create_reg_time_6_report)
        self.same_domain_reg_time_report_btn.clicked.connect(self.same_domain_reg_time_report)
        self.same_city_district_reg_time_report_btn.clicked.connect(self.same_city_district_reg_time_report)
        
        for btn in [self.fsv_btn,
                    self.N3_btn,
                    self.same_phone_btn,
                    self.same_name_district_city_state_btn,
                    self.N3_btn_4
                    ]:
            t2_btn_col.addWidget(btn)

        for btn in [self.same_ip_create_reg_time_4_btn,
                    self.same_ip_create_reg_time_6_btn,
                    self.same_domain_reg_time_report_btn,
                    self.same_city_district_reg_time_report_btn]:
            t2_btn_col2.addWidget(btn)
        # Cột Checkbox Tab 2 
        t2_check_col = QtWidgets.QVBoxLayout()
        t2_check_col2 = QtWidgets.QVBoxLayout()
        self.fsv_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.N3_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.N3_btn_4_checkbox = QtWidgets.QCheckBox('Checked')
        self.same_phone_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.same_name_district_city_state_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.same_ip_create_reg_time_4_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.same_domain_reg_time_report_btn_checkbox = QtWidgets.QCheckBox('Checked')
        self.same_city_district_reg_time_report_btn_checkbox = QtWidgets.QCheckBox('Checked')
        
        for cb in [self.fsv_btn_checkbox, self.N3_btn_checkbox, self.N3_btn_4_checkbox, self.same_phone_btn_checkbox,
                   self.same_name_district_city_state_btn_checkbox]:
            t2_check_col.addWidget(cb)

        for cb in [self.same_ip_create_reg_time_4_btn_checkbox,
                   self.same_domain_reg_time_report_btn_checkbox,
                   self.same_city_district_reg_time_report_btn_checkbox]:
            t2_check_col2.addWidget(cb)

        t2_content_layout.addLayout(t2_btn_col)
        t2_content_layout.addLayout(t2_check_col)

        t2_content_layout.addLayout(t2_btn_col2)
        t2_content_layout.addLayout(t2_check_col2)

        self.tab2_layout.addLayout(t2_content_layout)
        self.tab2_layout.addStretch()

        # Add Tabs to Widget
        self.tabWidget.addTab(self.tab1, "HC function")
        self.tabWidget.addTab(self.tab2, "NUV, FSV function")

        # --- 4. Bottom Section (Progress & Logs) ---
        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setValue(0)
        main_layout.addWidget(self.progress_bar)

        self.log_output = QtWidgets.QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.document().setDefaultStyleSheet("p { margin-bottom: 2px; }")
        main_layout.addWidget(self.log_output)
        # Store references to all interactive buttons for easy enabling/disabling
        self.interactive_button = [
            self.N3_btn, 
            self.same_promotion_phone_btn, 
            self.fsv_btn, 
            self.ip_create_time_btn, 
            self.rsl_item_btn,
            self.rsl_btn,
            self.similiar_address_btn,
            self.same_prm_phone_district_btn,
            self.same_recipient_phone_btn,
            self.same_order_value_check_out_and_similar_address_btn,
            self.tolerant_address_btn,
            self.same_phone_btn,
            self.same_name_district_city_state_btn,
            # self.same_ip_create_time_district_city_state_btn,
            self.same_ip_create_reg_time_4_btn
        ]
        MainWindow.setCentralWidget(self.centralwidget)
    
    def _set_buttons_enabled(self, enabled: bool):
        """
        Helper function to enable or disable all interactive buttons.
        Args:
            enabled (bool): True to enable buttons, False to disable.
        """
        for button in self.interactive_button:
            button.setEnabled(enabled)
    def choose_file(self):
        """
        Mở hộp thoại để người dùng chọn file Excel gốc.
        Lưu đường dẫn file vào ô nhập mã nhân viên.
        """
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            None, "Chọn file gốc", "", ";;All Files (*)")
        if file_path:
            self.mnv.setText(file_path)

    def choose_folder(self):
        """
        Mở hộp thoại để người dùng chọn thư mục đích.
        Lưu đường dẫn thư mục vào ô nhập thư mục đích.
        """
        folder_path = QtWidgets.QFileDialog.getExistingDirectory(
            None, "Chọn thư mục đích", "")
        if folder_path:
            self.destination_folder.setText(folder_path)

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
        self._set_buttons_enabled(True)  # Re-enable buttons after processing

    def same_promotion_phone(self):
        """
        Tạo báo cáo same promotion.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        # self.movie.start()  # Start the spinner animation
        # self.movie.stop()  # Stop the spinner animation
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "du_lieu_same_promotion_phone.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker1(input_file_path, output_file_path)
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
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "same_fsv.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker2(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()

    def same_ip_check_out(self):
        """
        Tạo báo cáo same ip check out và create time.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "same_ip_check_out.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker3(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()   
             
    def same_prm_phone_district(self):
        """
        Tạo báo cáo same promotion + phone + district.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        # self.movie.start()  # Start the spinner animation
        # self.movie.stop()  # Stop the spinner animation
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "du_lieu_same_promotion_phone_district.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker4(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()

    def rsl_item(self):
        """
        Tạo báo cáo same RSL items.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "same_item_amount.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker5(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
        
    def same_recipient_phone(self):
        """
        Tạo báo cáo same recipient phone.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "same_recipient_phone.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker6(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()


    def same_order_value_check_out_and_similar_address(self):
        """
        Tạo báo cáo same order value check out và similar address.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "same_order_value_check_out_and_similar_address.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker7(input_file_path, output_file_path) # Worker7
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
    def tolerant_address(self):
        """
        Tạo báo cáo tolerant address.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "tolerant_address.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker8(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
        
    def rsl_report(self):
        """
        Tạo báo cáo RSL.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "rsl.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker9(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()  
                      
    def similiar_address(self):
        """
        Tạo báo cáo similiar address report.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "similiar_address_report.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker10(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
        
    def N3_report(self):
        """
        Bắt đầu quá trình tạo báo cáo N3 6 - 9 .
        Yêu cầu người dùng chọn vị trí lưu và khởi động luồng Worker.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)  # Disable buttons during processing

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Dữ Liệu Nhóm", "du_lieu_nhom_N3.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)  # Re-enable if cancelled
            return

        self.thread = Worker11(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
        
    def N3_report_4(self):
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
        self._set_buttons_enabled(False)  # Disable buttons during processing

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Dữ Liệu Nhóm", "du_lieu_nhom_N3.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)  # Re-enable if cancelled
            return

        self.thread = Worker13(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()    
    def same_phone_report(self):
        """
        Tạo báo cáo same phone -6.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "same_phone.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker12(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
    def same_name_district_city_state_report(self):
        """
        Tạo báo cáo same name, district, city và state.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "same_name_district_city_state.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker18(input_file_path, output_file_path) # Đang sửa
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
    def same_ip_create_reg_time_6_report(self):
        """
        Tạo báo cáo same IP và create time với threhold >= 6.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "Same IP and Create Time 6.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker14(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
    def same_ip_create_time_4_report(self):
        """
        Tạo báo cáo same IP và create time với threshold 4.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "Same IP and Create Time 4.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker15(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
    def same_domain_reg_time_report(self):
        """
        Tạo báo cáo same domain và registration time với threshold 6.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "Same domain.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker16(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()
    def same_city_district_reg_time_report(self):
        """
        Tạo báo cáo same city district và registration time với threshold 6.
        """
        input_file_path = self.mnv.text()
        if not input_file_path:
            QtWidgets.QMessageBox.warning(None, "Lỗi", "Vui lòng chọn file Excel gốc.")
            return

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.log_output.append("🚀 Bắt đầu xử lý...")
        self._set_buttons_enabled(False)

        output_file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            None, "Lưu File Same", "Same city district + registration time.xlsx", "Excel Files (*.xlsx)")
        if not output_file_path:
            self.log_output.append("❌ Đã hủy lưu file.")
            self._set_buttons_enabled(True)
            return

        self.thread = Worker17(input_file_path, output_file_path)
        self.thread.progress.connect(self.progress_bar.setValue)
        self.thread.log.connect(self.log_output.append)
        self.thread.finished.connect(self.on_report_finished)
        self.thread.start()    

    def toggle_dark_mode(self, state):
        if state == QtCore.Qt.CheckState.Checked.value: # Dark mode is ON
            self.is_dark_mode = True
            self.main_window.setStyleSheet(self.dark_mode_stylesheet)
            # Update specific widget colors for dark mode
            self.dark_mode_checkbox.setStyleSheet("QCheckBox { color: #00aeef; }")
            # Using objectName to target the title label specifically
            self.label_mnv.setStyleSheet("QLabel { color: #00aeef; }")
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
def get_download_url(latest_version_from_txt):
    tag_name = f"V{latest_version_from_txt}"
    asset_name = UPDATE_EXECUTABLE_NAME_TEMPLATE.format(latest_version_from_txt) 
    repo_owner = "trungtien2410"
    repo_name = "bae"
    return f"https://github.com/{repo_owner}/{repo_name}/releases/download/{tag_name}/{asset_name}"


# --- CÁC LỚP THREAD CẬP NHẬT ĐƯỢC CHỈNH SỬA ---
class CheckUpdateThread(QtCore.QThread):
    update_found = QtCore.pyqtSignal(dict)
    no_update = QtCore.pyqtSignal(dict)
    error = QtCore.pyqtSignal(dict)

    def __init__(self, version_url, app_version, parent=None): 
        super().__init__(parent) 
        self.version_url = version_url
        self.app_version = app_version
        self.latest_version_info = None 

    def run(self):
        try:
            response = requests.get(self.version_url, timeout=5)
            response.raise_for_status() 
            latest_version_str = response.text.strip()

            current_semver = semver.Version.parse(self.app_version.lstrip('v'))
            latest_semver = semver.Version.parse(latest_version_str.lstrip('v'))
            self.latest_version_info = latest_version_str 

            if latest_semver > current_semver:
                self.update_found.emit({"status": "update_found", "latest_version": latest_version_str})
            else:
                self.no_update.emit({"status": "no_update"})
        except requests.exceptions.RequestException as req_e:
            self.error.emit({"status": "error", "error_message": f"Kết nối mạng lỗi: {req_e}"})
        except semver.ValueError as sv_e:
            self.error.emit({"status": "error", "error_message": f"Lỗi phân tích phiên bản: {sv_e}"})
        except Exception as e:
            self.error.emit({"status": "error", "error_message": f"Lỗi không xác định: {e}"})


class DownloadUpdateThread(QtCore.QThread):
    progress = QtCore.pyqtSignal(int)
    finished = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)

    def __init__(self, download_url, temp_file_path, parent=None): 
        super().__init__(parent) 
        self.download_url = download_url
        self.temp_file_path = temp_file_path

    def run(self):
        try:
            r = requests.get(self.download_url, stream=True, timeout=10) 
            r.raise_for_status() 

            total_size = int(r.headers.get('content-length', 0))
            block_size = 8192 

            downloaded = 0
            with open(self.temp_file_path, 'wb') as f:
                for chunk in r.iter_content(block_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        percent = int(downloaded * 100 / total_size)
                        self.progress.emit(percent)
            self.finished.emit()
        except requests.exceptions.RequestException as req_e:
            if os.path.exists(self.temp_file_path):
                os.remove(self.temp_file_path) 
            self.error.emit(f"Kết nối mạng hoặc tải xuống lỗi: {req_e}")
        except Exception as e:
            if os.path.exists(self.temp_file_path):
                os.remove(self.temp_file_path) 
            self.error.emit(f"Lỗi không xác định khi tải xuống: {e}")
# --- LỚP QUẢN LÝ KHỞI ĐỘNG CŨNG ĐƯỢC CẬP NHẬT ---
class StartupUpdateManager(QtCore.QObject):
    finished_startup = QtCore.pyqtSignal(bool) 

    def __init__(self, progress_dialog, parent=None):
        super().__init__(parent)
        self.progress_dialog = progress_dialog
        self.progress_dialog.setWindowFlags(QtCore.Qt.WindowType.SplashScreen | QtCore.Qt.WindowType.FramelessWindowHint)
        self.progress_dialog.setWindowModality(QtCore.Qt.WindowModality.ApplicationModal)
        self.progress_dialog.setValue(0)
        self.progress_dialog.setCancelButton(None) 

        self.check_thread = None
        self.download_thread = None
        self.download_temp_file = None

    def start_initial_check(self):
        self.is_bundled = getattr(sys, 'frozen', False)

        if not self.is_bundled:
            print("Running from source, skipping update check.")
            self.progress_dialog.setLabelText("Ứng dụng đã sẵn sàng. Đang khởi động...")
            self.progress_dialog.setValue(100)
            QtCore.QTimer.singleShot(1000, lambda: self.finished_startup.emit(True))
            return

        self.progress_dialog.setLabelText("Kiểm tra cập nhật...")
        self.progress_dialog.setValue(10)
        self.progress_dialog.show()

        self.check_thread = CheckUpdateThread(VERSION_URL, APP_VERSION, parent=self) 
        self.check_thread.update_found.connect(self._on_update_check_finished)
        self.check_thread.no_update.connect(self._on_update_check_finished)
        self.check_thread.error.connect(self._on_update_check_finished)
        self.check_thread.start()

    def _on_update_check_finished(self, result):
        if result["status"] == "update_found":
            latest_version_str = result["latest_version"]
            current_semver = semver.Version.parse(APP_VERSION.lstrip('v'))
            latest_semver = semver.Version.parse(latest_version_str.lstrip('v'))

            if latest_semver > current_semver:
                self.progress_dialog.setLabelText(f"🚀 Phát hiện phiên bản mới: {latest_version_str}! Đang tải xuống...")
                self.progress_dialog.setValue(30)
                self._download_and_install(latest_version_str)
            else:
                self.progress_dialog.setLabelText("Không có bản cập nhật mới. Đang khởi động ứng dụng...")
                self.progress_dialog.setValue(100)
                QtCore.QTimer.singleShot(1000, lambda: self.finished_startup.emit(True))
        elif result["status"] == "no_update":
            self.progress_dialog.setLabelText("Bạn đang sử dụng phiên bản mới nhất. Đang khởi động ứng dụng...")
            self.progress_dialog.setValue(100)
            QtCore.QTimer.singleShot(1000, lambda: self.finished_startup.emit(True))
        else: 
            error_message = result.get("error_message", "Không xác định")
            self.progress_dialog.setLabelText(f"❌ Lỗi kiểm tra cập nhật: {error_message}. Sử dụng phiên bản hiện tại.")
            QtWidgets.QMessageBox.warning(self.progress_dialog, "Lỗi cập nhật", 
                                          f"Không thể kiểm tra cập nhật: {error_message}\n"
                                          "Vui lòng đảm bảo bạn có kết nối Internet hoặc thử lại sau.")
            self.progress_dialog.setValue(100)
            QtCore.QTimer.singleShot(1000, lambda: self.finished_startup.emit(True))

    def _download_and_install(self, latest_version_str):
        self.download_url = get_download_url(latest_version_str)
        self.download_temp_file = os.path.join(tempfile.gettempdir(), f"baepink_update_{latest_version_str}.exe")
        
        self.download_thread = DownloadUpdateThread(self.download_url, self.download_temp_file, parent=self)
        self.download_thread.progress.connect(self._update_progress_dialog)
        self.download_thread.finished.connect(self._on_download_finished_no_admin) 
        self.download_thread.error.connect(self._on_download_error)
        self.download_thread.start()

    def _update_progress_dialog(self, progress):
        self.progress_dialog.setValue(30 + int(progress * 0.7)) 
        
    def _on_download_error(self, message):
        self.progress_dialog.setLabelText(f"❌ Lỗi tải xuống cập nhật: {message}. Sử dụng phiên bản hiện tại.")
        QtWidgets.QMessageBox.critical(self.progress_dialog, "Lỗi tải xuống", 
                                      f"Không thể tải xuống bản cập nhật: {message}\n"
                                      "Vui lòng thử lại hoặc tải phiên bản mới thủ công.")
        if os.path.exists(self.download_temp_file):
            os.remove(self.download_temp_file)
        self.finished_startup.emit(True) 

    def _on_download_finished_no_admin(self):
        self.progress_dialog.setLabelText(f"✅ Tải xuống hoàn tất! Đang chuẩn bị cài đặt...")
        
        current_app_path = sys.executable 
        app_dir = os.path.dirname(current_app_path)
        
        new_exe_name = UPDATE_EXECUTABLE_NAME_TEMPLATE.format(self.check_thread.latest_version_info)
        new_exe_path_in_app_dir = os.path.join(app_dir, new_exe_name)

        downloaded_temp_path = self.download_temp_file

        update_script_name = "update_temp_script.bat" 
        update_script_path = os.path.join(tempfile.gettempdir(), update_script_name) 

        # --- NỘI DUNG SCRIPT BATCH ĐƯỢC CẢI TIẾN VỚI TIMEOUT VÀ GHI LOG CHI TIẾT HƠN ---
        script_content = f"""
        @echo off
        set "LOG_FILE=%TEMP%\\baepink_update_log_{int(time.time())}.txt"
        echo --- Update Script Started: %DATE% %TIME% --- > "%LOG_FILE%"
        echo Current working directory: %CD% >> "%LOG_FILE%"
        echo Update script path: "{update_script_path}" >> "%LOG_FILE%"

        echo Waiting for application to close (5 seconds)... >> "%LOG_FILE%"
        timeout /t 5 /nobreak > nul
        if errorlevel 1 (echo Timeout command failed or was interrupted! >> "%LOG_FILE%")

        echo Current app path: "{current_app_path}" >> "%LOG_FILE%"
        echo New app name in app dir: "{new_exe_name}" >> "%LOG_FILE%"
        echo New app path in app dir: "{new_exe_path_in_app_dir}" >> "%LOG_FILE%"
        echo Downloaded temp path: "{downloaded_temp_path}" >> "%LOG_FILE%"

        rem Step 1: Attempt to rename the currently running executable to .old
        echo Attempting to rename... >> "%LOG_FILE%"
        if exist "{current_app_path}" (
            rename "{current_app_path}" "{os.path.basename(current_app_path)}.old"
            if errorlevel 1 (
                echo Failed to rename old executable! Errorlevel: %errorlevel%. It might still be in use. >> "%LOG_FILE%"
                goto :EOF_WITH_ERROR
            ) else (
                echo Successfully renamed old executable. >> "%LOG_FILE%"
            )
        ) else (
            echo Old executable not found at "{current_app_path}". Proceeding with copy. >> "%LOG_FILE%"
        )
        
        rem Step 2: Copy the new executable from temp to the application directory
        echo Attempting to copy new executable... >> "%LOG_FILE%"
        copy /Y "{downloaded_temp_path}" "{new_exe_path_in_app_dir}"
        if errorlevel 1 (
            echo Error copying new executable! Errorlevel: %errorlevel%. >> "%LOG_FILE%"
            goto :EOF_WITH_ERROR
        ) else (
            echo Successfully copied new executable. >> "%LOG_FILE%"
        )

        rem Step 3: Delete the downloaded temporary file
        echo Deleting temporary file... >> "%LOG_FILE%"
        del /F /Q "{downloaded_temp_path}"
        if errorlevel 1 (echo Error deleting temporary file! Errorlevel: %errorlevel%. >> "%LOG_FILE%")

        echo Relaunching application... >> "%LOG_FILE%"
        rem Step 4: Start the newly copied executable
        start "" "{new_exe_path_in_app_dir}"
        if errorlevel 1 (echo Error starting new executable! Errorlevel: %errorlevel%. >> "%LOG_FILE%")
        
        rem Step 5: Delete the old renamed executable in the background after a short delay
        (
            echo Waiting 3 seconds to delete old renamed executable... >> "%LOG_FILE%"
            timeout /t 3 /nobreak > nul
            del /F /Q "{current_app_path}.old"
            if errorlevel 1 (echo Error deleting old renamed executable! Errorlevel: %errorlevel%. >> "%LOG_FILE%")
        ) > nul 2>&1

        echo --- Update Script Finished Successfully --- >> "%LOG_FILE%"
        exit /b 0

        :EOF_WITH_ERROR
        echo --- Update Script Finished with Error --- >> "%LOG_FILE%"
        exit /b 1
        """

        try:
            with open(update_script_path, 'w', encoding='utf-8') as f: 
                f.write(script_content)
            
            self.progress_dialog.setLabelText("ℹ️ Chuẩn bị cài đặt cập nhật. Ứng dụng sẽ tự động khởi động lại.")
            
            if sys.platform == "win32":
                subprocess.Popen(
                    ['cmd.exe', '/c', update_script_path], 
                    creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW, 
                    close_fds=True,
                    cwd=tempfile.gettempdir() # <--- THAY ĐỔI MỚI: Đặt thư mục làm việc
                )
            else:
                QtWidgets.QMessageBox.warning(self.progress_dialog, "Cảnh báo", 
                                              "Chức năng cập nhật tự động chỉ hỗ trợ đầy đủ trên Windows.")
                self.finished_startup.emit(True)

            self.finished_startup.emit(False) 
            QtWidgets.QApplication.quit() 

        except Exception as e:
            self.progress_dialog.setLabelText(f"❌ Lỗi trong quá trình chuẩn bị cài đặt: {e}. Sử dụng phiên bản hiện tại.")
            QtWidgets.QMessageBox.critical(self.progress_dialog, "Lỗi cài đặt", 
                                          f"Không thể hoàn tất cài đặt bản cập nhật: {e}\n"
                                          "Vui lòng thử lại hoặc tải phiên bản mới thủ công.")
            if os.path.exists(downloaded_temp_path):
                os.remove(downloaded_temp_path)
            self.finished_startup.emit(True)
class MainWindowApp(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.ui = Ui_MainWindow() # Đảm bảo Ui_MainWindow được định nghĩa ở đâu đó trong code của bạn
        self.ui.setupUi(self)
        
        self.setWindowTitle(f"GROUPING TOOL {APP_VERSION}")
        # Log này chỉ cần thiết cho cửa sổ chính, không phải cho quá trình khởi động/update
        self.ui.log_output.append(f"Current version: {APP_VERSION}")
    

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)

    # 1. Tạo Progress Dialog (Cửa sổ cập nhật) và áp dụng style
    # Đây là cửa sổ duy nhất người dùng thấy lúc ban đầu
    update_progress_dialog = QtWidgets.QProgressDialog(
        "🚀 Đang kiểm tra cập nhật...", "❌ Hủy", 0, 100
    )
    update_progress_dialog.setWindowModality(QtCore.Qt.WindowModality.ApplicationModal)
    update_progress_dialog.setWindowTitle("Cập nhật ứng dụng")
    update_progress_dialog.setFixedSize(400, 120)
    update_progress_dialog.setStyleSheet("""
        QProgressBar {
            height: 24px;
            border-radius: 12px;
            background-color: #eeeeee;
            border: 2px solid #cccccc;
        }
        QProgressBar::chunk {
            background: qlineargradient(
                x1: 0, y1: 0, x2: 1, y2: 0,
                stop: 0 #4facfe, stop: 1 #00f2fe
            );
            border-radius: 12px;
        }
        QProgressDialog {
            font-size: 14px;
            font-family: "Segoe UI";
        }
    """)
    # update_progress_dialog.show() # Sẽ show trong StartupUpdateManager.start_initial_check()

    # 2. Tạo instance của MainWindowApp nhưng CHƯA hiển thị
    main_app_instance = MainWindowApp()

    # 3. Tạo và khởi chạy StartupUpdateManager
    startup_manager = StartupUpdateManager(update_progress_dialog)

    # Hàm xử lý khi quá trình khởi động (kiểm tra/cài đặt update) hoàn tất
    def handle_startup_finish(show_main_app):
        if update_progress_dialog.isVisible():
            update_progress_dialog.close() # Đóng dialog cập nhật

        if show_main_app:
            main_app_instance.show() # Hiển thị cửa sổ chính nếu cần
        else:
            # Nếu show_main_app là False, có nghĩa là ứng dụng đang tự khởi động lại sau update,
            # hoặc đã có lỗi nghiêm trọng dẫn đến việc thoát.
            # Trong trường hợp này, QApplication.quit() đã được gọi trong StartupUpdateManager.
            pass

    # Kết nối signal finished_startup của manager với hàm xử lý
    startup_manager.finished_startup.connect(handle_startup_finish)

    # Bắt đầu quá trình kiểm tra cập nhật ban đầu
    startup_manager.start_initial_check()

    sys.exit(app.exec())