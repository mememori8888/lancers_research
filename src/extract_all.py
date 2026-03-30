"""全社対象: Annual Report と Proxy Statement の両方からGeminiで指標を抽出しJSONで出力する。
ローカルPDFをGeminiで読み込み、年度と書類種別をAIが判定してから指標を抽出する。

PDFフォルダ構造（ファイル名は自由）:
  pdfs/
    {会社名フォルダ}/
      任意のファイル名.pdf   ← 年度・種別はGeminiが判定
      ...
"""

import csv
import glob
import json
import os
import re
import sys
import time

import google.generativeai as genai

YEARS = [2020, 2021, 2022, 2023, 2024, 2025]

METRICS = [
    "社外取締役数",
    "取締役数（男性）",
    "取締役数（女性）",
    "取締役比率（男性）",
    "取締役比率（女性）",
    "取締役数（外国籍）",
    "社外取締役比率",
    "取締役外国籍比率",
    "取締役（社外取締役を除く）の報酬等の総額",
    "社外取締役の報酬等の総額",
    "取締役報酬平均",
    "社長・CEO報酬",
    "総従業員数",
    "男性従業員数",
    "女性従業員数",
    "男性従業員比率",
    "女性従業員比率",
    "障がい者雇用人数",
    "平均年齢",
    "採用者数",
    "離職者数",
    "育児休業取得率（男性）",
    "育児休業取得率（女性）",
    "研修を受講した従業員数",
    "リーダーシップ開発・研修の対象者数",
    "リーダーシップ開発・研修の参加者数",
    "管理職研修を受けたリーダーの割合",
    "倫理・コンプライアンス研修を受けた従業員数",
    "人材開発・研修の総費用",
    "研修への参加率",
    "総研修実施時間",
    "従業員1人当たりの平均研修時間",
    "受講者当りの研修受講時間",
    "労災の件数",
    "労災による死亡者数",
    "従業員満足度調査結果",
    "男女間賃金格差（正社員）",
    "男女間賃金格差（非正規）",
    "売上高",
    "営業利益",
    "経常利益",
    "当期純利益",
    "従業員一人当たり経常利益",
    "従業員一人当たり純利益",
    "株価収益率",
    "総資産額",
    "純資産額",
    "資本金",
    "エンゲージメント",
    "平均勤続年数",
    "eNPS",
    "管理職1人当りの部下数",
    "管理職研修を受けた割合",
    "リーダーシップ開発・研修の参加率",
    "労働災害件数",
    "労災発生率",
    "離職率",
    "自発的離職率",
]

# 分類用モデル（年度・種別判定）: Flashで十分、安価・高速
CLASSIFY_MODEL_NAME = "gemini-2.5-flash"
CLASSIFY_CONFIG = genai.GenerationConfig(
    temperature=0.0,
    response_mime_type="application/json",
)

# 抽出用モデル: Flash（600PDF規模で実用的）。精度重視なら gemini-2.5-pro に変更可
EXTRACT_MODEL_NAME = "gemini-2.5-flash"
EXTRACT_CONFIG = genai.GenerationConfig(
    temperature=0.0,
    response_mime_type="application/json",
)

PDF_ROOT = "/workspaces/lancers_research/pdfs"
PDF_OUTPUTS_ROOT = "/workspaces/lancers_research/pdf_outputs"  # PDFごとの個別抽出結果
# 分類結果キャッシュ（再実行時に再分類をスキップ）

CACHE_FILE = "/workspaces/lancers_research/pdfs/.classification_cache.json"
# AI分類プロンプト（年度と書類種別を判定）
CLASSIFY_PROMPT = """このPDFの内容を確認して、以下のJSONを返してください。

{
  "fiscal_year": 2024,
  "document_type": "annual_report",
  "title_found": "Form 10-K for the fiscal year ended December 31, 2024"
}

判定基準:
- fiscal_year: このドキュメントが対象とする会計年度（西暦4桁の整数）
  例: "For the fiscal year ended December 31, 2024" → 2024
  ※ Annual Reportは表紙に "fiscal year ended [日付]" と明記されている
  ※ Proxy StatementはAnnual Meetingの年から1引いた年が会計年度（例: 2025年開催 → 2024会計年度）

- document_type: 以下のいずれか
  "annual_report"    ... 10-K, Annual Report（売上・利益・従業員数などの財務情報）
  "proxy_statement"  ... DEF 14A, Proxy Statement（役員報酬・取締役会情報・株主総会議案）
  "other"            ... 上記以外

- title_found: 表紙に記載されている文書タイトルをそのまま転記（確認用）
"""


def load_env(key: str) -> str:
    value = os.environ.get(key, "")
    if not value:
        print(f"[エラー] 環境変数 {key} が設定されていません")
        sys.exit(1)
    return value


def _pdf_output_path(company_folder_name: str, pdf_filename: str) -> str:
    """PDFごとの個別抽出結果JSONのパスを返す。"""
    out_dir = os.path.join(PDF_OUTPUTS_ROOT, company_folder_name)
    os.makedirs(out_dir, exist_ok=True)
    base = os.path.splitext(pdf_filename)[0]
    return os.path.join(out_dir, f"{base}.json")


def _load_pdf_output(company_folder_name: str, pdf_filename: str) -> dict | None:
    """既存の個別抽出結果を読み込む（キャッシュ）。"""
    path = _pdf_output_path(company_folder_name, pdf_filename)
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _save_pdf_output(company_folder_name: str, pdf_filename: str, data: dict) -> None:
    """個別抽出結果をJSONファイルに保存する。"""
    path = _pdf_output_path(company_folder_name, pdf_filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  [個別保存] {os.path.relpath(path, '/workspaces/lancers_research')}")


def read_all_companies_from_csv() -> list[dict]:
    csv_files = sorted(
        glob.glob("/workspaces/lancers_research/results_*.csv"), reverse=True
    )
    if not csv_files:
        print("[エラー] results_*.csv が見つかりません。先に main.py を実行してください")
        sys.exit(1)
    path = csv_files[0]
    print(f"CSV: {path}")
    companies = []
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            name = row["english_name"].strip()
            if name:
                companies.append({"id": row["id"], "name": name})
    return companies


def _company_to_folder_key(company_name: str) -> str:
    """会社名をフォルダ検索用キーに変換（小文字・アンダースコア統一）"""
    name = re.sub(r'[/\\:*?"<>|.,]', "", company_name)
    name = name.replace(" ", "_").lower()
    return re.sub(r"_+", "_", name).strip("_")


def find_company_folder(company_name: str) -> str | None:
    """pdfs/ 以下で会社名に最も近いフォルダパスを返す。見つからなければ None。"""
    if not os.path.isdir(PDF_ROOT):
        return None

    target = _company_to_folder_key(company_name)
    best = None

    for folder in os.listdir(PDF_ROOT):
        fpath = os.path.join(PDF_ROOT, folder)
        if not os.path.isdir(fpath):
            continue
        f_key = _company_to_folder_key(folder)
        if f_key == target:
            return fpath
        if len(target) >= 6 and f_key.startswith(target[:12]):
            best = fpath
        elif len(f_key) >= 6 and target.startswith(f_key[:12]):
            best = fpath

    if best:
        return best

    # フォールバック: 4文字以上の主要単語が2つ以上一致するフォルダ
    keywords = [w for w in target.split("_") if len(w) >= 4][:3]
    for folder in os.listdir(PDF_ROOT):
        fpath = os.path.join(PDF_ROOT, folder)
        if not os.path.isdir(fpath):
            continue
        f_key = _company_to_folder_key(folder)
        if len(keywords) >= 2 and sum(1 for kw in keywords if kw in f_key) >= 2:
            return fpath
        elif len(keywords) == 1 and keywords[0] in f_key:
            return fpath

    return None


# ---------------------------------------------------------------------------
# 分類キャッシュ
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cache(cache: dict) -> None:
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# PDF分類（Geminiでカバーページから年度・種別を判定）
# ---------------------------------------------------------------------------

def classify_pdf(classify_model, pdf_path: str, cache: dict) -> dict | None:
    """PDFの会計年度と文書種別をGeminiで判定する（キャッシュ付き）。

    Returns:
        {"fiscal_year": 2024, "document_type": "annual_report", "title_found": "..."} or None
    """
    mtime = str(os.path.getmtime(pdf_path))
    cache_key = f"{pdf_path}::{mtime}"

    if cache_key in cache:
        cached = cache[cache_key]
        print(f"  [キャッシュ] {os.path.basename(pdf_path)} → "
              f"{cached.get('fiscal_year')}年 {cached.get('document_type')}")
        return cached

    print(f"  [AI分類中] {os.path.basename(pdf_path)} ...")
    try:
        uploaded = genai.upload_file(pdf_path, mime_type="application/pdf")
        response = classify_model.generate_content([uploaded, CLASSIFY_PROMPT])
        text = response.text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
        result = json.loads(text)

        year = result.get("fiscal_year")
        doc_type = result.get("document_type")
        if not isinstance(year, int) or doc_type not in ("annual_report", "proxy_statement", "other"):
            print(f"  [分類不正] レスポンス: {result}")
            return None

        print(f"  [分類完了] {os.path.basename(pdf_path)} → "
              f"{year}年 {doc_type}  ({result.get('title_found', '')[:60]})")

        cache[cache_key] = result
        save_cache(cache)
        time.sleep(0.5)
        return result

    except Exception as e:
        print(f"  [分類失敗] {os.path.basename(pdf_path)}: {e}")
        return None


def scan_company_folder(classify_model, folder_path: str, cache: dict) -> dict:
    """会社フォルダ内の全PDFを分類し、{year: {"annual_report": path, "proxy_statement": path}} を返す。"""
    year_map: dict[int, dict] = {}
    pdf_files = sorted(
        f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")
    )

    if not pdf_files:
        print(f"  [警告] PDFなし: {folder_path}")
        return year_map

    print(f"  PDF {len(pdf_files)} 件を処理中...")
    for pdf_name in pdf_files:
        pdf_path = os.path.join(folder_path, pdf_name)
        info = classify_pdf(classify_model, pdf_path, cache)
        if not info:
            continue
        year = info.get("fiscal_year")
        doc_type = info.get("document_type")
        if not year or doc_type == "other":
            continue
        if year not in year_map:
            year_map[year] = {"annual_report": None, "proxy_statement": None}
        year_map[year][doc_type] = pdf_path

    return year_map


class AnnualReportAnalyzer:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.extract_model = genai.GenerativeModel(
            EXTRACT_MODEL_NAME, generation_config=EXTRACT_CONFIG
        )
        self.classify_model = genai.GenerativeModel(
            CLASSIFY_MODEL_NAME, generation_config=CLASSIFY_CONFIG
        )
        self.cache = load_cache()

    # ------------------------------------------------------------------
    # 会社フォルダのスキャン
    # ------------------------------------------------------------------
    def scan_company(self, company_name: str) -> dict:
        """会社名に対応するフォルダを探し、全PDFを分類して年マップを返す。"""
        folder = find_company_folder(company_name)
        if not folder:
            print(f"  [フォルダなし] {company_name}")
            return {}
        print(f"  フォルダ: {os.path.basename(folder)}")
        return scan_company_folder(self.classify_model, folder, self.cache)

    # ------------------------------------------------------------------
    # プロンプト構築
    # ------------------------------------------------------------------
    def _build_extract_prompt(self, company_name: str, year: int) -> str:
        metrics_list = "\n".join(f"- {m}" for m in METRICS)
        return (
            f"以下のPDFは {company_name} の {year} 会計年度の書類です。\n"
            "Annual Report（10-K）には財務情報・従業員情報、"
            "Proxy Statement（DEF 14A）には取締役会・役員報酬情報が含まれます。\n\n"
            "下記の指標をすべて抽出してください。\n"
            "- 数値が記載されていない場合のみ null にしてください\n"
            "- パーセンテージは数値のみ（例: 30.5）\n"
            "- 金額は百万ドル単位の数値のみ（例: 1234.5）\n"
            "- 人数は整数\n\n"
            "返却するJSONのキー一覧:\n"
            f"{metrics_list}\n\n"
            '例: {"社外取締役数": 8, "取締役数（男性）": 7, "売上高": 23456.7, ...}'
        )

    def _parse_json(self, text: str) -> dict:
        text = text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
        return json.loads(text)

    # ------------------------------------------------------------------
    # PDF解析（PDFごとに個別抽出・保存し、最後にマージ）
    # ------------------------------------------------------------------
    def _extract_single_pdf(
        self,
        pdf_path: str,
        company_name: str,
        year: int,
        doc_type: str,
        company_folder_name: str,
    ) -> dict | None:
        """1つのPDFから指標を抽出する。既存の結果があればキャッシュから返す。"""
        pdf_filename = os.path.basename(pdf_path)

        # キャッシュチェック（同じPDFを再抽出しない）
        cached = _load_pdf_output(company_folder_name, pdf_filename)
        if cached is not None:
            print(f"  [抽出キャッシュ] {pdf_filename}")
            return cached

        print(f"  [{doc_type}] アップロード中: {pdf_filename}")
        try:
            uploaded = genai.upload_file(pdf_path, mime_type="application/pdf")
        except Exception as e:
            print(f"  [アップロード失敗] {pdf_filename}: {e}")
            return None

        prompt = self._build_extract_prompt(company_name, year)
        try:
            response = self.extract_model.generate_content([uploaded, prompt])
            result = self._parse_json(response.text)
        except Exception as e:
            print(f"  [抽出失敗] {pdf_filename}: {e}")
            return None

        # メタ情報を付加してPDFごとに保存
        output = {
            "company_name": company_name,
            "year": year,
            "document_type": doc_type,
            "source_file": pdf_filename,
            **result,
        }
        _save_pdf_output(company_folder_name, pdf_filename, output)
        return result

    def extract_with_pdfs(
        self,
        company_name: str,
        year: int,
        ar_path: str | None,
        ps_path: str | None,
        company_folder_name: str,
    ) -> tuple[dict | None, str]:
        """PDFごとに個別抽出・保存し、マージした結果を返す。

        マージ優先順位:
        - Annual Report優先: 財務・従業員系の指標
        - Proxy Statement優先: 取締役会・役員報酬系の指標
        - どちらにもあればProxy Statementの値で上書き（より正確なため）
        """
        ar_result = None
        ps_result = None
        used = []

        if ar_path and os.path.exists(ar_path):
            ar_result = self._extract_single_pdf(
                ar_path, company_name, year, "annual_report", company_folder_name
            )
            if ar_result:
                used.append("annual_report")

        if ps_path and os.path.exists(ps_path):
            ps_result = self._extract_single_pdf(
                ps_path, company_name, year, "proxy_statement", company_folder_name
            )
            if ps_result:
                used.append("proxy_statement")

        if not used:
            return None, "no_pdf"

        # Annual Reportを基礎として、Proxy Statementの非nullで上書きマージ
        merged: dict = {}
        for m in METRICS:
            ar_val = ar_result.get(m) if ar_result else None
            ps_val = ps_result.get(m) if ps_result else None
            # Proxy Statementの値を優先（null以外）
            merged[m] = ps_val if ps_val is not None else ar_val

        return merged, "+".join(used)

    # ------------------------------------------------------------------
    # エントリーポイント
    # ------------------------------------------------------------------
    def process(self, company_name: str, year: int, year_map: dict, company_folder_name: str) -> dict:
        """年マップから該当年のPDFを取得して指標を抽出する。"""
        year_data = year_map.get(year, {})
        ar_path = year_data.get("annual_report")
        ps_path = year_data.get("proxy_statement")

        if not ar_path and not ps_path:
            print(f"  [{year}年] PDFなし → スキップ")
            return {
                "company_name": company_name,
                "year": year,
                "source": "no_pdf",
                "annual_report_file": None,
                "proxy_statement_file": None,
                **{m: None for m in METRICS},
            }

        result, source = self.extract_with_pdfs(
            company_name, year, ar_path, ps_path, company_folder_name
        )

        if result is not None:
            result["company_name"] = company_name
            result["year"] = year
            result["source"] = f"pdf:{source}"
            result["annual_report_file"] = os.path.basename(ar_path) if ar_path else None
            result["proxy_statement_file"] = os.path.basename(ps_path) if ps_path else None
            return result

        print(f"  [{year}年] 抽出失敗（source: {source}）")
        return {
            "company_name": company_name,
            "year": year,
            "source": f"error:{source}",
            "annual_report_file": os.path.basename(ar_path) if ar_path else None,
            "proxy_statement_file": os.path.basename(ps_path) if ps_path else None,
            **{m: None for m in METRICS},
        }


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main():
    gemini_key = load_env("GEMINI_API_KEY")
    analyzer = AnnualReportAnalyzer(gemini_key)

    companies = read_all_companies_from_csv()
    print(f"対象: {len(companies)} 社 × {len(YEARS)} 年 = {len(companies) * len(YEARS)} 件\n")

    # -----------------------------------------------------------------------
    # Step 1: 全会社のPDFをAIで分類（年度・種別を判定）
    # -----------------------------------------------------------------------
    print("=" * 60)
    print("  Step 1: PDF分類（AIが年度・種別を自動判定）")
    print("=" * 60)

    company_year_maps: dict[str, dict] = {}
    company_folder_names: dict[str, str] = {}  # company_name → フォルダ名
    for company in companies:
        print(f"\n[分類] {company['name']}")
        folder_path = find_company_folder(company["name"])
        folder_name = os.path.basename(folder_path) if folder_path else company["name"]
        company_folder_names[company["name"]] = folder_name
        year_map = analyzer.scan_company(company["name"])
        company_year_maps[company["name"]] = year_map
        if year_map:
            for y in sorted(year_map.keys()):
                docs = year_map[y]
                ar = "✓" if docs.get("annual_report") else "✗"
                ps = "✓" if docs.get("proxy_statement") else "✗"
                print(f"  {y}年: Annual Report {ar}  Proxy Statement {ps}")
        else:
            print("  PDFなし（フォルダ未作成または空）")

    # 分類結果サマリーを保存（確認用）
    summary_path = "/workspaces/lancers_research/pdf_classification_summary.json"
    summary = {
        name: {
            str(y): {
                "annual_report": os.path.basename(docs["annual_report"]) if docs.get("annual_report") else None,
                "proxy_statement": os.path.basename(docs["proxy_statement"]) if docs.get("proxy_statement") else None,
            }
            for y, docs in year_map.items()
        }
        for name, year_map in company_year_maps.items()
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"\n[分類サマリー出力] {summary_path}")

    # -----------------------------------------------------------------------
    # Step 2: 年ごとに指標抽出
    # -----------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  Step 2: 指標抽出")
    print("=" * 60)

    for year in YEARS:
        print(f"\n{'─' * 50}")
        print(f"  {year}年 処理開始")
        print(f"{'─' * 50}")

        year_results = []
        output_path = f"/workspaces/lancers_research/all_results_{year}.json"

        for i, company in enumerate(companies, 1):
            name = company["name"]
            year_map = company_year_maps[name]
            folder_name = company_folder_names[name]
            print(f"\n[{i}/{len(companies)}] {name} ({year}年)")

            try:
                result = analyzer.process(name, year, year_map, folder_name)
                year_results.append(result)
                print(f"  完了 (source: {result.get('source')})")
            except Exception as e:
                print(f"  [エラー] {e}")
                year_results.append({
                    "company_name": name,
                    "year": year,
                    "source": "error",
                    "error": str(e),
                    "annual_report_file": None,
                    "proxy_statement_file": None,
                    **{m: None for m in METRICS},
                })

            # 1社ごとに途中保存
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(year_results, f, ensure_ascii=False, indent=2)

        print(f"\n[出力] {output_path} ({len(year_results)} 社)")

    print("\n全年度 完了！")


if __name__ == "__main__":
    main()
