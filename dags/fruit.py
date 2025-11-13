import pandas as pd
import requests
from datetime import datetime, timedelta
import pymysql
from airflow import DAG
from airflow.decorators import task

# MySQL 連線設定
DB_CONFIG = {
    "host": "35.221.176.159",
    "port": 3306,
    "user": "fruit-weather",
    "password": "1qaz@WSX",
    "database": "fruit",
    "charset": "utf8mb4"
}

# 匯入的資料表名稱
TABLE_NAME = "volume"

# API URL
url = "https://data.moa.gov.tw/Service/OpenData/FromM/FarmTransData.aspx"

# API 欄位映射
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

# 35 種水果代碼
fruit_name = {
    "72": "番茄", "I1": "木瓜", "51": "百香果", "T1": "西瓜", "N3": "李",
    "R1": "芒果", "L1": "枇杷", "H1": "文旦柚", "H2": "白柚", "Z4": "柿",
    "W1": "洋香瓜", "A1": "香蕉", "Y1": "桃", "45": "草莓", "J1": "荔枝",
    "D1": "楊桃", "41": "梅", "O10": "梨", "V1": "香瓜", "E1": "柳橙",
    "22": "蓮霧", "C1": "椪柑", "P1": "番石榴", "11": "可可椰子", "M3": "楊桃",
    "C5": "溫州蜜柑", "S1": "葡萄", "H4": "葡萄柚", "B2": "鳳梨",
    "Q1": "蓮霧", "G7": "龍眼", "K3": "棗", "F1": "蘋果",
    "X69": "釋迦", "31": "番茄枝"
}

# 市場 → 城市 ID
MARKET_TO_CITY_ID = {
    "台北一": "TPE", "台北二": "TPE",
    "板橋區": "NTP", "三重區": "NTP",
    "桃農": "TYN", "宜蘭市": "ILA",
    "台中市": "TXG", "豐原區": "TXG", "東勢鎮": "TXG",
    "嘉義市": "CYI", "高雄市": "KHH", "鳳山區": "KHH",
    "台東市": "TTT", "南投市": "NTO", "屏東市": "PIF"
}

# 民國 → 西元
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

# API 抓取資料
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

# MySQL 找最大日期
def get_last_date():
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result


# 匯入MySQL
def insert_to_mysql(df):
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
            str(row["crop_id"]),      # 避免 72 → 72.0
            float(row["avg_price"]),
            float(row["trans_volume"])
        )
        for _, row in df.iterrows()
    ]

    print(f"開始匯入 MySQL，共 {len(data_to_insert)} 筆資料")

    # 一次插入
    cursor.executemany(sql, data_to_insert)
    conn.commit()

    cursor.close()
    conn.close()
    print("匯入完成！")


# Airflow DAG
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
        last_date = get_last_date()
        if last_date:
            start_date = last_date + timedelta(days=1)
            print(f"從 {start_date} 開始抓資料")
        else:
            start_date = datetime(2020, 1, 1).date()
            print("第一次執行，從 2020-01-01 開始")

        end_date = datetime.today().date()

        if start_date > end_date:
            print("資料已最新，不需更新")
            return None

        return (start_date, end_date)

    @task()
    def fetch_and_transform(date_range):
        if not date_range:
            return None

        start_date, end_date = date_range
        records = []
        d = start_date

        while d <= end_date:
            print(f"抓取：{d}")
            day_data = fetch_data(d, d)
            if day_data:
                records.extend(day_data)
            d += timedelta(days=1)

        if not records:
            return None

        df = pd.DataFrame(records)
        df = df.rename(columns={col: column_name.get(col, col) for col in df.columns})

        df["TransDate"] = df["TransDate"].apply(roc_to_ad)
        df["TransDate"] = pd.to_datetime(df["TransDate"], errors="coerce")
        df["city_id"] = df["MarketName"].map(MARKET_TO_CITY_ID)

        grouped = df.groupby(["TransDate", "CropCode", "city_id"], as_index=False).agg({
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
        return grouped.to_dict(orient="records")

    @task()
    def insert_data(records):
        if not records:
            print("沒有新資料可匯入")
            return
        df = pd.DataFrame(records)
        insert_to_mysql(df)

    # DAG pipeline
    dr = prepare_date_range()
    data = fetch_and_transform(dr)
    insert_data(data)
