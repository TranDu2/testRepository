import pandas as pd
import os
import hl7
import chardet  # pip install chardet
import re

# -------------------------
# Hàm đọc file với auto detect encoding
# -------------------------
def read_file_with_encoding(file_path):
    with open(file_path, "rb") as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result["encoding"] or "utf-8"
    return raw_data.decode(encoding, errors="ignore")

# 1. Đọc file CSV
def read_csv_file(file_path):
    return pd.read_csv(file_path)

# 2. Đọc file .log (trích phần sau 'kết quả:')
def read_log_file(file_path):
    content = read_file_with_encoding(file_path)
    records = []

    for line in content.splitlines():
        match = re.search(r'kết quả:\s*(.*)', line, re.IGNORECASE)
        if match:
            ket_qua = match.group(1).strip()
            # Bỏ qua dòng trống hoặc toàn ký tự '?'
            if not ket_qua or all(ch in ['?', ''] for ch in ket_qua):
                continue
            # Tách dữ liệu theo dấu phẩy
            fields = [x.strip() for x in ket_qua.split(",")]
            records.append(fields)

    # Nếu không tìm thấy dữ liệu thì trả về DataFrame rỗng
    if not records:
        return pd.DataFrame()

    # Tạo tiêu đề cột tạm
    max_cols = max(len(r) for r in records)
    col_names = [f"Col{i+1}" for i in range(max_cols)]
    return pd.DataFrame(records, columns=col_names)

# 3. Đọc file HL7/ASTM
def read_hl7_file(file_path):
    content = read_file_with_encoding(file_path)
    records = []
    messages = content.strip().split("\r")
    for msg in messages:
        try:
            h = hl7.parse(msg)
            pid_segment = h.segment("PID")
            patient_id = pid_segment[3][0] if len(pid_segment) > 3 else None

            obr_segment = h.segment("OBR")
            test_name = obr_segment[4][1] if len(obr_segment) > 4 else None

            for obx in h.segments("OBX"):
                result_value = obx[5][0]
                unit = obx[6][0] if len(obx) > 6 else None
                ref_range = obx[7][0] if len(obx) > 7 else None

                records.append({
                    "ID": patient_id,
                    "Test": obx[3][1] if len(obx) > 3 else test_name,
                    "Result": result_value,
                    "Unit": unit,
                    "Ref": ref_range
                })
        except Exception:
            pass
    return pd.DataFrame(records)

# 4. Lưu sang Excel
def save_to_excel(df, output_excel):
    df.to_excel(output_excel, index=False)
    print(f"Đã lưu file excel: {output_excel}")

# 5. Hàm chính
def process_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        df = read_csv_file(file_path)
    elif ext == ".log":
        df = read_log_file(file_path)
    elif ext in [".hl7", ".astm", ".txt"]:
        df = read_hl7_file(file_path)
    else:
        raise ValueError("Định dạng file không hỗ trợ.")

    print("\nDữ liệu đọc được:")
    print(df.head())

    if df.empty:
        print("⚠️ Không có dữ liệu nào được đọc từ file.")
        return

    save_to_excel(df, "ket_qua.xlsx")

# 6. Chạy thử
if __name__ == "__main__":
    file_path = r"D:\Company\Python\ConvertToExcel\test.log"
    process_file(file_path)
