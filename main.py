import json
import pathlib
import re
import shutil
import time

import pandas as pd
import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
}

rakuten = {
    # 1:免許情報検索  2: 登録情報検索
    "ST": 1,
    # 詳細情報付加 0:なし 1:あり
    "DA": 1,
    # スタートカウント
    "SC": 1,
    # 取得件数
    "DC": 1,
    # 出力形式 1:CSV 2:JSON 3:XML
    "OF": 2,
    # 無線局の種別
    "OW": "FB_H",
    # 免許人名称/登録人名称
    "NA": "楽天モバイル",
}


def musen_api(d, it):

    d["IT"] = it

    r = requests.get("https://www.tele.soumu.go.jp/musen/list", params=d, headers=headers)
    r.raise_for_status()

    time.sleep(1)

    return r.json()
    
def fetch_cities(s):

    lst = re.findall("(\S+)\s*\(([0-9,]+)\)", s)

    df0 = pd.DataFrame(lst, columns=["city", "count"])
    df0["count"] = df0["count"].str.strip().str.replace(",", "").astype(int)

    flag = df0["city"].str.endswith(("都", "道", "府", "県"))

    df0["pref"] = df0["city"].where(flag).fillna(method="ffill")
    df1 = df0.reindex(columns=["pref", "city", "count"])

    return df1


def csv_write(df, date, it, category):

    df.rename(columns={"count": date}, inplace=True)

    fromPath = pathlib.Path("csv", f"{category}", f"{it}", f"{date}.csv")
    fromPath.parent.mkdir(parents=True, exist_ok=True)
    
    if not fromPath.exists():

        df.to_csv(fromPath, encoding="utf_8_sig", index=False)

        toPath = pathlib.Path("csv", f"{category}", f"{it}_latest.csv")
        shutil.copy(fromPath, toPath)


for it in ["J", "I", "A", "B", "D", "C", "E", "F", "G", "H", "O"]:

    data = musen_api(rakuten, it)

    lastupdate = data["musenInformation"]["lastUpdateDate"]

    macro = (
        data["musen"][0]["detailInfo"]["note"]
        .split("\\n", 2)[2]
        .replace("\\n", " ")
        .strip()
    )

    df_macro = fetch_cities(macro)

    csv_write(df_macro, lastupdate, it, "macro")

    femto = (
        data["musen"][1]["detailInfo"]["note"]
        .split("\\n", 2)[2]
        .replace("\\n", " ")
        .strip()
    )

    df_femto = fetch_cities(femto)

    csv_write(df_femto, lastupdate, it, "femto")
