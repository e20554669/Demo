import pandas as pd
import requests
from datetime import datetime, timedelta
import pymysql
from airflow import DAG
from airflow.decorators import task

#  MySQL 連線設定
DB_CONFIG = {
    "host": "35.221.176.159",
    "port": 3306,
    "user": "fruit-weather",
    "password": "1qaz@WSX",
    "database": "fruit",
    "charset": "utf8mb4"
}
#匯入的資料表名稱
TABLE_NAME = "volume"

# ✅ API 對應表
url = "https://data.moa.gov.tw/Service/OpenData/FromM/FarmTransData.aspx"

column_name = {
    "交易日期": "TransDate",
    "市場代號": "MarketCode",
    "市場名稱": "MarketName",
    "作物代號": "CropCode",
    "上價": "UpperPrice",
    "中價": "MiddlePrice",
    "下價": "LowerPrice",
    "平均價": "AveragePrice",
    "交易量": "TransVolume",
    "種類代碼": "TypeCode"
}

fruit_name = {
    "72","I1","51","T1","N3","R1","L1","H1","H2","Z4","W1","A1","Y1","45",
    "J1","D1","41","O10","V1","E1","22","C1","P1","11","M3","C5","S1","H4",
    "B2","Q1","G7","K3","F1","X69","31"
}

MARKET_TO_CITY_ID = {
    "台北一": "TPE", "台北二": "TPE",
    "板橋區": "NTP", "三重區": "NTP",
    "桃農": "TYN", "宜蘭市": "ILA",
    "台中市": "TXG", "豐原區": "TXG", "東勢鎮": "TXG",
    "嘉義市": "CYI", "高雄市": "KHH", "鳳山區": "KHH",
    "台東市": "TTT", "南投市": "NTO", "屏東市": "PIF"
}


#民國轉西元
def roc_to_ad(date_str):
    if pd.isna(date_str):
        return None
    date_str = str(date_str).replace(".", "").replace("/", "")
    if len(date_str) != 7:
        return None
    y = int(date_str[:3]) + 1911
    m = int(date_str[3:5])
    d = int(date_str[5:7])
    return f"{y:04d}-{m:02d}-{d:02d}"

# 抓取水果API 資料
def fetch_data(start, end, page_top=2000):
    
    all_data = []
    valid_codes = set(fruit_name.keys())

    params = {
        "StartDate": f"{start.year - 1911:03d}.{start.month:02d}.{start.day:02d}",
        "EndDate": f"{end.year - 1911:03d}.{end.month:02d}.{end.day:02d}",
        "TcType": "N05",
        "$top": page_top,
        "$skip": 0
    }

    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break

        filtered = [i for i in data if i.get("作物代號") in valid_codes]
        all_data.extend(filtered)

        if len(data) < page_top:
            break
        params["$skip"] += page_top

    return all_data

#查資料庫中最後的日期
def get_last_date():
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result


def insert_to_mysql(df, batch_size=500):
    """逐筆匯入 MySQL（進度條已移除）"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    sql = f"""
    INSERT INTO {TABLE_NAME}
    (date, city_id, crop_id, avg_price, trans_volume)
    VALUES (%s, %s, %s, %s, %s)
    """

    data_to_insert = [
        (
            row["date"],
            row["city_id"],
            row["crop_id"],
            float(row["avg_price"]),
            float(row["trans_volume"])
        )
        for _, row in df.iterrows()
    ]

    total = len(data_to_insert)
    print(f"開始匯入 MySQL，共 {total} 筆資料")

    # 逐批匯入（無進度條）
    for i in range(0, total, batch_size):
        batch = data_to_insert[i:i + batch_size]
        cursor.executemany(sql, batch)
        conn.commit()

    cursor.close()
    conn.close()
    print("匯入完成！")



# Airflow DAG with API
with DAG(
    dag_id="fruit_price_daily",
    description="每日抓取台灣水果行情（API）",
    start_date=datetime(2020, 1, 1),
    schedule="36 16 * * *",
    catchup=False,
    tags=["fruit", "moa", "mysql"]
) as dag:

    @task()
    def prepare_date_range():
        """偵測 MySQL 最後日期 → 決定抓取範圍"""
        last_date = get_last_date()
        if last_date:
            start_date = last_date + timedelta(days=1)
            print(f"從 {start_date} 開始抓取新資料")
        else:
            start_date = datetime(2020, 1, 1).date()
            print("第一次執行，從 2025-11-01 開始")

        end_date = datetime.today().date()
        if start_date > end_date:
            print("已是最新資料，無需更新")
            return None
        return (start_date, end_date)

    @task()
    def fetch_and_transform(date_range):
        if not date_range:
            return None

        start_date, end_date = date_range
        records = []
        cursor_date = start_date

        while cursor_date <= end_date:
            print(f"抓取日期：{cursor_date}")
            day_data = fetch_data(cursor_date, cursor_date)
            if day_data:
                records.extend(day_data)
            cursor_date += timedelta(days=1)

        if not records:
            print("沒有抓到任何資料")
            return None

        df = pd.DataFrame(records)
        df = df.rename(columns={col: column_name.get(col, col) for col in df.columns})
        df["TransDate"] = df["TransDate"].apply(roc_to_ad)
        df["TransDate"] = pd.to_datetime(df["TransDate"], errors="coerce")
        df["city_id"] = df["MarketName"].map(MARKET_TO_CITY_ID)

        grouped = df.groupby(
            ["TransDate", "CropCode", "city_id"],
            as_index=False
        ).agg({
            "AveragePrice": "mean",
            "TransVolume": "sum"
        })

        grouped["AveragePrice"] = grouped["AveragePrice"].round(2)
        grouped = grouped.rename(columns={
            "TransDate": "date",
            "CropCode": "crop_id",
            "AveragePrice": "avg_price",
            "TransVolume": "trans_volume"
        })
        grouped["date"] = grouped["date"].astype(str)

        print(f"整理完成 {len(grouped)} 筆資料")
        return grouped.to_dict(orient="records")

    @task()
    def insert_data(records):
        """匯入 MySQL"""
        if not records:
            print("無新資料可匯入")
            return
        df = pd.DataFrame(records)
        insert_to_mysql(df)

    # DAG 任務流程
    date_range = prepare_date_range()
    data = fetch_and_transform(date_range)
    insert_data(data)
