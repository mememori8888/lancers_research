import csv
import os
import sys
from datetime import datetime

from gemini_translator import GeminiTranslator
from sheets_client import SheetsClient


def load_env(key: str, required: bool = True) -> str:
    value = os.environ.get(key, "")
    if required and not value:
        print(f"[エラー] 環境変数 {key} が設定されていません")
        sys.exit(1)
    return value


def save_csv(results: list[dict], path: str) -> None:
    fieldnames = ["id", "original_name", "english_name"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"[出力] CSVファイル: {path}")


def main():
    sheets_creds = load_env("GOOGLE_SHEETS_CREDENTIALS")
    gemini_key = load_env("GEMINI_API_KEY")

    sheets = SheetsClient(sheets_creds)
    translator = GeminiTranslator(gemini_key)

    print("スプレッドシートから企業を読み込み中...")
    companies = sheets.get_companies()
    print(f"{len(companies)} 件の企業を取得しました")

    if not companies:
        print("[警告] 企業データが見つかりませんでした")
        return

    results = []
    for i, company in enumerate(companies, 1):
        print(f"[{i}/{len(companies)}] {company['name']}")

        english_name = translator.translate_to_english(company["name"])
        print(f"  英語名: {english_name}")

        results.append({
            "id": company["id"],
            "original_name": company["name"],
            "english_name": english_name,
        })

    # 常にCSVを出力する
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_csv(results, f"results_{timestamp}.csv")

    # スプレッドシートへの書き込み（失敗しても処理継続）
    try:
        sheets.write_results(results)
        print("スプレッドシートに結果を書き込みました")
    except Exception as e:
        print(f"[警告] スプレッドシートへの書き込みに失敗しました: {e}")

    print("完了！")


if __name__ == "__main__":
    main()
