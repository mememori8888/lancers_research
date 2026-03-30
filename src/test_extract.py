"""テスト: Annual Report と Proxy Statement の両方からGeminiで指標を抽出しJSONで出力する。"""

import json
import os
import sys

# extract_all.py と同じ AnnualReportAnalyzer を再利用
sys.path.insert(0, os.path.dirname(__file__))
from extract_all import AnnualReportAnalyzer, METRICS, load_env, read_all_companies_from_csv  # noqa: E402

N_TEST = 3    # テスト対象企業数（変更可）
TEST_YEAR = 2025  # テスト対象年（変更可）


def main():
    gemini_key = load_env("GEMINI_API_KEY")
    analyzer = AnnualReportAnalyzer(gemini_key)

    all_companies = read_all_companies_from_csv()
    companies = all_companies[:N_TEST]
    print(f"\n対象: {len(companies)} 社 ({TEST_YEAR}年)\n")

    results = []
    for i, company in enumerate(companies, 1):
        print(f"[{i}/{len(companies)}] {company['name']} ({TEST_YEAR}年)")
        try:
            result = analyzer.process(company["name"], TEST_YEAR)
            results.append(result)
            print(f"  完了 (source: {result.get('source')})")
            print(f"  Annual Report URL : {result.get('annual_report_url')}")
            print(f"  Proxy Statement URL: {result.get('proxy_statement_url')}")
        except Exception as e:
            print(f"  [エラー] {e}")
            results.append(
                {
                    "company_name": company["name"],
                    "year": TEST_YEAR,
                    "source": "error",
                    "annual_report_url": None,
                    "proxy_statement_url": None,
                    "error": str(e),
                    "metrics": {m: None for m in METRICS},
                }
            )

    output_path = "/workspaces/lancers_research/test_results.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n[出力] {output_path}")
    print("完了！")


if __name__ == "__main__":
    main()
