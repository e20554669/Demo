import pandas as pd
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone
import pymysql
from sqlalchemy import create_engine

# ==========================================================
# MySQL 設定
# ==========================================================
DB_USER = "root"
DB_PASS = "1qaz@WSX"
DB_HOST = "host.docker.internal"   # ✅ Docker 必用
DB_PORT = 3310
DB_NAME = "fruit_weather"
TABLE_NAME = "volume"

def get_engine():
    return create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        echo=False
    )

# ==========================================================
# 1. API網址 + 對應表
# ==========================================================
url = "https://data.moa.gov.tw/Service/OpenData/FromM/FarmTransData.aspx"

column_name = {
    "交易日期": "TransDate",
    "市場代號": "MarketCode",
    "市場名稱": "MarketName",
    "作物代號": "CropCode",
    "作物名稱": "CropName",
    "上價": "UpperPrice",
    "中價": "MiddlePrice",
    "下價": "LowerPrice",
    "平均價": "AveragePrice",
    "交易量": "TransVolume",
    "種類代碼": "TypeCode"
}

fruit_name = {
    "72": "番茄", "I1": "木瓜", "51": "百香果", "T1": "西瓜", "N3": "李",
    "R1": "芒果", "L1": "枇杷", "H1": "文旦柚", "H2": "白柚", "Z4": "柿",
    "W1": "洋香瓜", "A1": "香蕉", "Y1": "桃", "45": "草莓", "J1": "荔枝",
    "D1": "楊桃", "41": "梅", "O10": "梨", "V1": "香瓜", "E1": "柳橙",
    "22": "蓮霧", "C1": "椪柑", "P1": "番石榴", "11": "可可椰子",
    "C5": "溫州蜜柑", "S1": "葡萄", "H4": "葡萄柚", "B2": "鳳梨",
    "G7": "龍眼", "K3": "棗", "F1": "蘋果", "X69": "釋迦",
}

market_to_city = {
    "台北一": "台北市", "台北二": "台北市",
    "板橋區": "新北市", "三重區": "新北市",
    "桃農": "桃園市", "宜蘭市": "宜蘭縣",
    "台中市": "台中市", "豐原區": "台中市", "東勢鎮": "台中市",
    "嘉義市": "嘉義市",
    "高雄市": "高雄市", "鳳山區": "高雄市",
    "台東市": "台東縣", "南投市": "南投縣",
    "屏東市": "屏東縣"
}

# ==========================================================
# 2. 資料抓取
# ==========================================================
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

# ==========================================================
# ✅ 主程式（被 Airflow 呼叫）
# ==========================================================
def run_pipeline():
    start_date = datetime(2025, 11, 1).date()
    end_date = datetime.today().date()

    records = []
    cursor = start_date

    while cursor <= end_date:
        print(f"抓取：{cursor}")
        day_data = fetch_data(cursor, cursor)
        if day_data:
            records.extend(day_data)
        cursor += timedelta(days=1)

    if not records:
        print("沒有資料")
        return

    df = pd.DataFrame(records)

    df = df.rename(columns={col: column_name.get(col, col) for col in df.columns})
    df["TransDate"] = df["TransDate"].apply(roc_to_ad)
    df["TransDate"] = pd.to_datetime(df["TransDate"], errors="coerce")
    df["CropName"] = df["CropCode"].map(fruit_name)
    df["city_name"] = df["MarketName"].map(market_to_city)

    city_group_df = df.groupby(
        ["TransDate", "TypeCode", "CropCode", "CropName", "city_name"],
        as_index=False
    ).agg({
        "AveragePrice": "mean",
        "TransVolume": "sum"
    })

    print(f"處理筆數：{len(city_group_df)}")

    engine = get_engine()
    city_group_df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    print("✅ MySQL 匯入成功")

# ==========================================================
# ✅ Airflow DAG
# ==========================================================
tz = timezone("Asia/Taipei")

with DAG(
    dag_id="fruit_price_daily",
    start_date=datetime(2024, 11, 1, tzinfo=tz),
    schedule="00 15 * * *",
    catchup=False,
    tags=["fruit", "moa"]
) as dag:

    task_run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline
    )
