import pandas as pd
import requests
from datetime import datetime, timedelta
import pymysql
from airflow import DAG
from airflow.decorators import task

DB = {
    "host": "35.221.176.159",
    "port": 3306,
    "user": "fruit-weather",
    "password": "1qaz@WSX",
    "database": "fruit",
    "charset": "utf8mb4"
}

TABLE = "volume"
API_URL = "https://data.moa.gov.tw/Service/OpenData/FromM/FarmTransData.aspx"

COLUMN_MAP = {
    "交易日期": "date",
    "市場名稱": "MarketName",
    "作物代號": "crop_id",
    "平均價": "avg_price",
    "交易量": "trans_volume"
}

FRUIT = {
    "72","I1","51","T1","N3","R1","L1","H1","H2","Z4",
    "W1","A1","Y1","45","J1","D1","41","O10","V1","E1",
    "22","C1","P1","11","M3","C5","S1","H4","B2","Q1",
    "G7","K3","F1","X69","31"
}

CITY = {
    "台北一":"TPE","台北二":"TPE","板橋區":"NTP","三重區":"NTP",
    "桃農":"TYN","宜蘭市":"ILA","台中市":"TXG","豐原區":"TXG",
    "東勢鎮":"TXG","嘉義市":"CYI","高雄市":"KHH","鳳山區":"KHH",
    "台東市":"TTT","南投市":"NTO","屏東市":"PIF"
}

def roc_to_ad(s):
    s = str(s).replace(".","").replace("/","")
    return f"{int(s[:3])+1911}-{s[3:5]}-{s[5:7]}" if len(s)==7 else None

def fetch_moa(start, end):
    params = {
        "StartDate": f"{start.year-1911:03d}.{start.month:02d}.{start.day:02d}",
        "EndDate":   f"{end.year-1911:03d}.{end.month:02d}.{end.day:02d}",
        "TcType": "N05",
        "$top": 2000,
        "$skip": 0
    }
    rows = []
    while True:
        r = requests.get(API_URL, params=params, timeout=30).json()
        if not r:
            break
        rows += [i for i in r if i.get("作物代號") in FRUIT]
        if len(r) < 2000:
            break
        params["$skip"] += 2000
    return rows

def get_last_date():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(date) FROM {TABLE}")
    d = cur.fetchone()[0]
    cur.close()
    conn.close()
    return d

def insert_mysql(df):
    df = df.dropna(subset=["date","city_id","crop_id","avg_price","trans_volume"])

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


with DAG(
    dag_id="fruit_price_daily",
    start_date=datetime(2020,1,1),
    schedule="45 3 * * *",
    catchup=False
) as dag:

    @task()
    def date_range():
        last = get_last_date()
        s = (last + timedelta(days=1)) if last else datetime(2020,1,1).date()
        e = datetime.today().date()
        return None if s > e else (s, e)

    @task()
    def process(dr):
        if not dr:
            return None

        s, e = dr
        rec = []
        d = s

        while d <= e:
            rec += fetch_moa(d, d)
            d += timedelta(days=1)

        if not rec:
            return None

        df = pd.DataFrame(rec)
        df = df.rename(columns={k: COLUMN_MAP[k] for k in COLUMN_MAP if k in df.columns})

        # 🔥 修正關鍵：移除缺資料避免 NULL
        df = df.dropna(subset=["date", "crop_id", "MarketName"])

        df["date"] = pd.to_datetime(df["date"].apply(roc_to_ad), errors="coerce")
        df["city_id"] = df["MarketName"].map(CITY)

        df = df.dropna(subset=["date","crop_id","city_id"])

        g = df.groupby(
            ["date","crop_id","city_id"],
            as_index=False
        ).agg({
            "avg_price":"mean",
            "trans_volume":"sum"
        })

        g["avg_price"] = g["avg_price"].round(2)
        g["date"] = g["date"].astype(str)
        g["crop_id"] = g["crop_id"].astype(str)

        return g

    @task()
    def load(df):
        if df is not None and len(df) > 0:
            insert_mysql(df)

    load(process(date_range()))
