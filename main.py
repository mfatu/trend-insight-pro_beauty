import os
import json
import datetime
import requests
import io
from pytrends.request import TrendReq
from google.cloud import bigquery
from openai import OpenAI

def init_clients():
    client_ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    client_bq = bigquery.Client()
    return client_bq, client_ai

def get_rakuten_items(keyword):
    app_id = os.getenv("RAKUTEN_APP_ID")
    if not app_id: return []
    try:
        url = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
        params = {"applicationId": app_id, "keyword": keyword, "hits": 3, "sort": "-reviewCount", "format": "json"}
        res = requests.get(url, params=params, timeout=10)
        items = res.json().get("Items", [])
        return [{"name": i["Item"]["itemName"][:40], "price": i["Item"]["itemPrice"], "url": i["Item"]["itemUrl"]} for i in items]
    except Exception as e:
        print(f"❌ 楽天APIエラー ({keyword}): {e}")
        return []

def main():
    print("🚀 美容トレンド・アフィリエイト解析システム始動...")
    bq_client, ai_client = init_clients()
    pytrends = TrendReq(hl='ja-JP', tz=360)
    
    print("🔍 Googleトレンドから急上昇ワードを取得中...")
    try:
        pytrends.build_payload(['美容'], cat=44, timeframe='now 1-d', geo='JP')
        rising_queries = pytrends.related_queries()['美容']['rising']
        if rising_queries is None or rising_queries.empty:
            print("⚠️ 急上昇ワードが見つかりませんでした。")
            return
        target_keywords = rising_queries['query'].tolist()[:5]
    except Exception as e:
        print(f"❌ Googleトレンド取得失敗: {e}")
        return

    rows_for_bq = []
    today = datetime.date.today().isoformat()

    for kw in target_keywords:
        print(f"📊 分析中: {kw}")
        matched_items = get_rakuten_items(kw)
        
        prompt = f"美容トレンド「{kw}」を40代女性視点で分析しJSONで返せ。形式: {{'score': 0-100, 'insight': '60字以内', 'killer_phrase': '40字以内'}}"
        try:
            response = ai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={ "type": "json_object" }
            )
            analysis = json.loads(response.choices[0].message.content)
            
            report_data = {"ai_insight": analysis.get("insight", ""), "killer_phrase": analysis.get("killer_phrase", ""), "rakuten_items": matched_items}
            rows_for_bq.append({
                "date": today,
                "keyword": kw,
                "surge_score": 100.0,
                "ai_score": analysis.get("score", 0),
                "insight_report": json.dumps(report_data, ensure_ascii=False)
            })
        except Exception as e:
            print(f"❌ AI分析エラー ({kw}): {e}")

    if rows_for_bq:
        # 無料枠(Free Tier)対応：ストリーミング挿入ではなくLoad Jobを使用
        table_id = "trend-insight-pro.beauty_trends.daily_logs"
        
        # データを改行区切りJSONに変換
        json_data = "\n".join([json.dumps(row, ensure_ascii=False) for row in rows_for_bq])
        file_obj = io.BytesIO(json_data.encode("utf-8"))

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False, # スキーマは既存のものを使用
        )

        print("📦 BigQueryへロードジョブを送信中...")
        load_job = bq_client.load_table_from_file(
            file_obj, table_id, job_config=job_config
        )
        
        load_job.result() # 完了まで待機
        print(f"✅ {len(rows_for_bq)}件のデータをBigQueryに正常に保存しました。")

if __name__ == "__main__":
    main()
