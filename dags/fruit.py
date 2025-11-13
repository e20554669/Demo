import pandas as pd
import requests
from datetime import datetime, timedelta
import pymysql
from airflow import DAG
from airflow.decorators import task

# MySQL 連線設定
DB = {
    "host": "35.221.176.159",
    "port": 3306,
    "user": "fruit-weather",
    "password": "1qaz@WSX",
    "database": "fruit",
    "charset": "utf8mb4"
}

TABLE = "volume"

# API 對應設定，不可修改
URL = "https://data.moa.gov.tw/Service/OpenData/FromM/FarmTransData.aspx"

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
    "72":"番茄","I1":"木瓜","51":"百香果","T1":"西瓜","N3":"李","R1":"芒果","L1":"枇杷",
    "H1":"文旦柚","H2":"白柚","Z4":"柿","W1":"洋香瓜","A1":"香蕉","Y1":"桃","45":"草莓",
    "J1":"荔枝","D1":"楊桃","41":"梅","O10":"梨","V1":"香瓜","E1":"柳橙","22":"蓮霧","C1":"椪柑",
    "P1":"番石榴","11":"可可椰子","M3":"楊桃","C5":"溫州蜜柑","S1":"葡萄","H4":"葡萄柚",
    "B2":"鳳梨","Q1":"蓮霧","G7":"龍眼","K3":"棗","F1":"蘋果","X69":"釋迦","31":"番茄枝"
}

MARKET_TO_CITY_ID = {
    "台北一":"TPE","台北二":"TPE","板橋區":"NTP","三重區":"NTP","桃農":"TYN","宜蘭市":"ILA",
    "台中市":"TXG","豐原區":"TXG","東勢鎮":"TXG","嘉義市":"CYI","高雄市":"KHH",
    "鳳山區":"KHH","台東市":"TTT","南投市":"NTO","屏東市":"PIF"
}

# 民國轉西元
def roc_to_ad(s):
    if pd.isna(s):
        return None
    s = str(s).replace(".", "").replace("/", "")
    if len(s) != 7:
        return None
    return f"{int(s[:3]) + 1911}-{s[3:5]}-{s[5:7]}"

# 抓 API 資料
def fetch_data(start, end):
    params = {
        "StartDate": f"{start.year - 1911:03d}.{start.month:02d}.{start.day:02d}",
        "EndDate": f"{end.year - 1911:03d}.{end.month:02d}.{end.day:02d}",
        "TcType": "N05",
        "$top": 2000,
        "$skip": 0
    }

    rows = []
    valid = set(fruit_name.keys())

    while True:
        data = requests.get(URL, params=params, timeout=30).json()
        if not data:
            break

        rows += [i for i in data if i.get("作物代號") in valid]

        if len(data) < 2000:
            break
        params["$skip"] += 2000

    return rows

# 查 MySQL 最後日期
def get_last_date():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(date) FROM {TABLE}")
    d = cur.fetchone()[0]
    cur.close()
    conn.close()
    return d

# 匯入 MySQL（符合你的表結構）
def insert_mysql(df):
    # 避免 NULL 導致外鍵錯誤
    df = df.dropna(subset=["date", "city_id", "crop_id", "avg_price", "trans_volume"])

    # 轉成字串避免 crop_id 被變浮點數
    df["crop_id"] = df["crop_id"].astype(str)

    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    sql = f"""
        INSERT INTO {TABLE} (date, city_id, crop_id, avg_price, trans_volume)
        VALUES (%s, %s, %s, %s, %s)
    """

    cur.executemany(sql, df.values.tolist())
    conn.commit()
    cur.close()
    conn.close()

# Airflow DAG
with DAG(
    dag_id="fruit_price_daily",
    start_date=datetime(2020, 1, 1),
    schedule="00 4 * * *",
    catchup=False
) as dag:

    @task()
    def date_range():
        last = get_last_date()
        start = (last + timedelta(days=1)) if last else datetime(2020, 1, 1).date()
        end = datetime.today().date()
        return None if start > end else (start, end)

    @task()
    def process(dr):
        if not dr:
            return None

        start, end = dr
        records = []
        d = start

        # 逐日抓資料
        while d <= end:
            records += fetch_data(d, d)
            d += timedelta(days=1)

        if not records:
            return None

        df = pd.DataFrame(records)

        # rename 欄位
        df = df.rename(columns={c: column_name.get(c, c) for c in df.columns})

        # 清洗資料
        df["TransDate"] = df["TransDate"].apply(roc_to_ad)
        df["TransDate"] = pd.to_datetime(df["TransDate"], errors="coerce")
        df["city_id"] = df["MarketName"].map(MARKET_TO_CITY_ID)
        df = df.dropna(subset=["TransDate", "CropCode", "city_id"])

        # group by
        g = df.groupby(
            ["TransDate", "CropCode", "city_id"],
            as_index=False
        ).agg({
            "AveragePrice": "mean",
            "TransVolume": "sum"
        })

        # 最後清理型別
        g["AveragePrice"] = g["AveragePrice"].round(2)
        g = g.rename(columns={
            "TransDate": "date",
            "CropCode": "crop_id",
            "AveragePrice": "avg_price",
            "TransVolume": "trans_volume"
        })
        g["date"] = g["date"].astype(str)
        g["crop_id"] = g["crop_id"].astype(str)

        return g

    @task()
    def load(df):
        if df is not None and len(df) > 0:
            insert_mysql(df)

    load(process(date_range()))
