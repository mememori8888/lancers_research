"""ローカルPDFをGemini File APIで解析するテスト。
使い方: python src/test_local_pdf.py [PDFパス] [会社名] [年]
例:     python src/test_local_pdf.py "/workspaces/lancers_research/2025 Proxy Statement.pdf" "EOG Resources, Inc." 2025
"""

import json
import os
import sys

import google.generativeai as genai

# extract_all.py の共通クラスを再利用
sys.path.insert(0, os.path.dirname(__file__))
from extract_all import AnnualReportAnalyzer, METRICS, load_env  # noqa: E402

DEFAULT_PDF = "/workspaces/lancers_research/2025 Proxy Statement.pdf"
DEFAULT_COMPANY = "EOG Resources, Inc."
DEFAULT_YEAR = 2025


def main():
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PDF
    company_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_COMPANY
    year = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_YEAR

    if not os.path.exists(pdf_path):
        print(f"[エラー] ファイルが見つかりません: {pdf_path}")
        sys.exit(1)

    gemini_key = load_env("GEMINI_API_KEY")
    genai.configure(api_key=gemini_key)
    analyzer = AnnualReportAnalyzer(gemini_key)

    print(f"PDF: {pdf_path}")
    print(f"会社: {company_name} / {year}年")
    print("Gemini File API にアップロード中...")

    uploaded = genai.upload_file(pdf_path, mime_type="application/pdf")
    print(f"アップロード完了: {uploaded.name}")

    prompt = analyzer._build_extract_prompt(company_name, year)
    print("Gemini で解析中...")
    response = analyzer.model.generate_content([uploaded, prompt])
    result = analyzer._parse_json(response.text)

    # メタ情報を付与
    result["company_name"] = company_name
    result["year"] = year
    result["source"] = "local_pdf"
    result["pdf_path"] = pdf_path

    output_path = "/workspaces/lancers_research/test_local_pdf_result.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n[出力] {output_path}")

    # null でない項目だけ表示
    metrics = result.get("metrics", result)
    filled = {k: v for k, v in metrics.items() if v is not None and k in METRICS}
    null_count = sum(1 for k in METRICS if metrics.get(k) is None)
    print(f"\n取得できた指標: {len(filled)} / {len(METRICS)} 件 (null: {null_count} 件)")
    for k, v in filled.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
