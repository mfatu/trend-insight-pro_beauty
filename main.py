import os
import json
import datetime
import requests
from pytrends.request import TrendReq
from google.cloud import bigquery
from openai import OpenAI

# --- 1. 初期設定 ---
def init_clients():
    # OpenAIクライアント（ChatGPT）の初期化
    client_ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    # BigQueryクライアントの初期化
    client_bq = bigquery.Client()
    return client_bq, client_ai

# --- 2. 楽天API連携：キーワードに関連する商品を特定 ---
def get_rakuten_items(keyword):
    app_id = os.getenv("RAKUTEN_APP_ID")
    if not app_id:
        print("⚠️ RAKUTEN_APP_ID が設定されていません。")
        return []
    try:
        url = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
        params = {
            "applicationId": app_id,
            "keyword": keyword,
            "hits": 3,
            "sort": "-reviewCount",  # レビュー数順（信頼性重視）
            "format": "json"
        }
        res = requests.get(url, params=params, timeout=10)
        items = res.json().get("Items", [])
        return [
            {"name": i["Item"]["itemName"][:40], "price": i["Item"]["itemPrice"], "url": i["Item"]["itemUrl"]}
            for i in items
        ]
    except Exception as e:
        print(f"❌ 楽天APIエラー ({keyword}): {e}")
        return []

# --- 3. メイン実行ロジック ---
def main():
    print("🚀 美容トレンド・アフィリエイト解析システム始動...")
    bq_client, ai_client = init_clients()
    pytrends = TrendReq(hl='ja-JP', tz=360)
    
    # A. トレンドキーワード取得（美容カテゴリ：44）
    print("🔍 Googleトレンドから急上昇ワードを取得中...")
    try:
        pytrends.build_payload(['美容'], cat=44, timeframe='now 1-d', geo='JP')
        rising_queries = pytrends.related_queries()['美容']['rising']
        if rising_queries is None or rising_queries.empty:
            print("⚠️ 急上昇ワードが見つかりませんでした。")
            return
        # 上位5件をターゲットにする
        target_keywords = rising_queries['query'].tolist()[:5]
    except Exception as e:
        print(f"❌ Googleトレンド取得失敗: {e}")
        return

    rows_for_bq = []
    today = datetime.date.today().isoformat()

    for kw in target_keywords:
        print(f"📊 分析中: {kw}")
        
        # B. 楽天商品の紐付け
        matched_items = get_rakuten_items(kw)
        
        # C. ChatGPTによる40代女性向け分析
        prompt = f"""
        あなたは40代女性向け美容・スキンケア専門のマーケティングコンサルタントです。
        トレンドキーワード「{kw}」について、40代女性のインサイトを分析してください。
        
        以下のJSON形式のみで回答してください：
        {{
            "score": 0から100の数値（40代への刺さり度・市場価値）,
            "insight": "40代女性が抱く老化への悩みや期待と結びつけた深い洞察（60文字以内）",
            "killer_phrase": "このトレンドを使って商品を売るための強力なキャッチコピー（40文字以内）"
        }}
        """
        
        try:
            response = ai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={ "type": "json_object" }
            )
            analysis = json.loads(response.choices[0].message.content)
            
            # D. BigQuery用データの整形
            # insight_reportにはAIの分析結果と楽天商品のリストをまとめて格納
            report_data = {
                "ai_insight": analysis.get("insight", ""),
                "killer_phrase": analysis.get("killer_phrase", ""),
                "rakuten_items": matched_items
            }

            rows_for_bq.append({
                "date": today,
                "keyword": kw,
                "surge_score": 100.0, # 急上昇ワードであるため100を基準
                "ai_score": analysis.get("score", 0),
                "insight_report": json.dumps(report_data, ensure_ascii=False)
            })
        except Exception as e:
            print(f"❌ AI分析エラー ({kw}): {e}")

    # E. BigQueryへ保存
    if rows_for_bq:
        # プロジェクトID、データセット名、テーブル名を環境に合わせて書き換えてください
        table_id = "your-project-id.beauty_trends.daily_logs"
        errors = bq_client.insert_rows_json(table_id, rows_for_bq)
        if errors == []:
            print(f"✅ {len(rows_for_bq)}件のデータをBigQueryに保存しました。")
        else:
            print(f"❌ BigQuery保存エラー: {errors}")

if __name__ == "__main__":
    main()
