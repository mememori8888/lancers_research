"""PDFをGemini（Chain of Thought）で解析し、根拠付きMarkdownを出力するスクリプト（高速版）。

v2 の改善点（v1 との比較）:
  1. 1PDF 内の3カテゴリ（GOV/FIN/HR）を ThreadPoolExecutor で並列 API 呼び出し
     → 直列 + sleep 6秒 が並列化され、約3倍高速
  2. 複数 PDF を外側の ThreadPoolExecutor で並列処理（--workers オプション）
  3. 固定 time.sleep 廃止 → 429/quota エラー時のみ指数バックオフでリトライ
  4. グローバル Semaphore でトータルの同時 generate_content 呼び出し数を制御

使い方:
  python3 extract_to_markdown_v2.py                           # 全PDFを処理
  python3 extract_to_markdown_v2.py --company "General Mills" # 1社のみ
  python3 extract_to_markdown_v2.py --overwrite               # キャッシュ無視して再生成
  python3 extract_to_markdown_v2.py --workers 3               # PDF並列数（デフォルト: 2）
  python3 extract_to_markdown_v2.py --api-concurrency 6       # 同時API呼び出し上限（デフォルト: 5）

速度目安（v1 比較）:
  v1: 1PDF = 分類(1) + GOV(1) + sleep(3s) + FIN(1) + sleep(3s) + HR(1) = 直列4呼び出し + 6秒待機
  v2: 1PDF = 分類(1) → [GOV + FIN + HR 同時] = 直列2ステップ、sleepなし
  複数PDF: --workers 2 でさらに2PDF並列
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import google.generativeai as genai

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

PDF_ROOT = "/workspaces/lancers_research/pdfs"
MD_OUTPUT_ROOT = "/workspaces/lancers_research/md_outputs"

MODEL_NAME = "gemini-2.5-flash-lite"

METRICS_GOV = [
    "社外取締役数", "取締役数（男性）", "取締役数（女性）", "取締役比率（男性）", "取締役比率（女性）",
    "取締役数（外国籍）", "社外取締役比率", "取締役外国籍比率",
    "取締役（社外取締役を除く）の報酬等の総額", "社外取締役の報酬等の総額",
    "取締役報酬平均", "社長・CEO報酬"
]

METRICS_FIN = [
    "売上高", "営業利益", "経常利益", "当期純利益",
    "従業員一人当たり経常利益", "従業員一人当たり純利益",
    "株価収益率", "総資産額", "純資産額", "資本金"
]

METRICS_HR = [
    "総従業員数", "男性従業員数", "女性従業員数", "男性従業員比率", "女性従業員比率",
    "障がい者雇用人数", "平均年齢", "採用者数", "離職者数",
    "育児休業取得率（男性）", "育児休業取得率（女性）",
    "研修を受講した従業員数", "リーダーシップ開発・研修の対象者数", "リーダーシップ開発・研修の参加者数",
    "管理職研修を受けたリーダーの割合", "倫理・コンプライアンス研修を受けた従業員数",
    "人材開発・研修の総費用", "研修への参加率", "総研修実施時間",
    "従業員1人当たりの平均研修時間", "受講者当りの研修受講時間",
    "労災の件数", "労災による死亡者数", "従業員満足度調査結果",
    "男女間賃金格差（正社員）", "男女間賃金格差（非正規）",
    "エンゲージメント", "平均勤続年数", "eNPS",
    "管理職1人当りの部下数", "管理職研修を受けた割合", "リーダーシップ開発・研修の参加率",
    "労働災害件数", "労災発生率", "離職率", "自発的離職率"
]

COT_PROMPT = """あなたは企業の年次報告書（10-K）、委任状説明書（Proxy Statement）、サステナビリティレポート等を分析し、指定されたESG・財務データを正確に抽出する専門家です。
提供されたPDFファイルについて、以下のステップバイステップ（Chain of Thought）で各指標を抽出してください。

現在抽出対象となっているカテゴリは「{category_name}」に関する指標です。

## 重要な指示：視覚（マルチモーダル）能力の活用とページ番号の特定（2段階アプローチ）
あなたはPDFを「画像（ネイティブPDF）」として視覚的に認識できます。以下の手順で慎重に探してください。
1. **検索とページ特定**: まず目次（Table of Contents）を確認し、該当指標が載っていそうなセクションの「ページ番号」を特定してください。
2. **発見**: そのページに移動し、該当する箇所の原文テキストや、視覚的に読み取った表のレイアウト・チェックボックスなどの文脈を引用・説明してください。
3. **計算（必要な場合）**: 実数しかなく比率が問われている場合や、一人当たりの利益を出す場合は、取得した数値を用いて計算してください。
4. **結論**: 最終的な数値（単位を明記）と、それが記載されていたページ番号。

## グラフや図表の読み取りに関する特別な指示
- **グラフはあるが数値が不明な場合**: 該当する指標のグラフやチャートが存在するものの、目盛りや数値ラベルが省略されており正確な値が読み取れない場合は、無理に推測しないでください。
- その場合、**値**には「グラフあり・詳細数値不明」とし、**読み取り方**に「〇〇のグラフはあるが数値の記載なし」と理由を報告してください。

## 計算が必要な指標の定義と「計算式の明記」
直接的なパーセンテージや比率が記載されていない場合でも、基礎データ（実数）が記載されている場合は以下の定義で計算してください。
- **管理職研修を受けた割合** = (管理職研修を受講した人数) ÷ (全管理職数) × 100
- **男女間賃金格差** = (女性の平均給与) ÷ (男性の平均給与) × 100
- **離職率** = (離職者数) ÷ (総従業員数) × 100
- **従業員一人当たり純利益** = (当期純利益) ÷ (総従業員数)
- **従業員一人当たり経常利益** = (税引前利益/経常利益) ÷ (総従業員数)
- **社外取締役比率** = (独立/社外取締役の数) ÷ (取締役の総数) × 100

※後で人間が検算してハルシネーションをチェックできるよう、**計算を行った場合は必ず「実際に使用した数値」と「計算式」を出力**してください。

## 出力形式（Markdown）
各指標を以下の形式で必ず出力してください。ページ番号を明記することでハルシネーションを防ぎます。

### {指標名}
- **値**: {数値と単位、または「記載なし」、または「グラフあり・詳細数値不明」}
- **記載ページ**: {PDFの該当ページ番号、または「記載なし」}
- **根拠**:
  > {PDFからの原文引用や、表・グラフから視覚的に読み取った内容の説明}
- **計算式**: {計算を行った場合、実際に使用した数値と数式（例: 1,020 ÷ 1,200 × 100 = 85.0%）。計算していない場合は「なし」}
- **読み取り方**: {数値への変換・解釈の説明、またはグラフが読み取れなかった理由の説明}

---

## 抽出対象の指標リスト ({category_name})

{metrics_list}

## 注意事項
- 資料の性質上、記載がない項目も多々あります。テキストもグラフも見つからない場合は「記載なし」としてください。
- 記載ページは、PDFの物理的なページ数ではなく、資料に印字されているページ番号を優先してください。
- パーセンテージは「30.5%」のように単位付きで記載。
- 金額は「USD 1,234 million」のように通貨と単位を明記。
- 複数の場所に分散している場合は、最も具体的な数値を採用し、すべての引用を記載する。
"""

CLASSIFY_PROMPT = """このPDFの1ページ目（表紙）または目次を確認して、以下のJSON形式のみで返してください。
{"fiscal_year": 2024, "document_type": "annual_report", "company_name_in_doc": "General Mills, Inc."}

- fiscal_year: 会計年度（西暦4桁の整数。不明な場合は null）
  ※ Proxy StatementはAnnual Meeting開催年 - 1 を会計年度としてください。
- document_type: "annual_report" (10-K等) / "proxy_statement" (DEF 14A等) / "sustainability_report" (ESGレポート等) / "other"
- company_name_in_doc: 文書タイトルや表紙に記載されている会社名

JSON以外のテキストは不要です。"""


# ---------------------------------------------------------------------------
# グローバル制御
# ---------------------------------------------------------------------------

# generate_content の同時呼び出し数を制限する Semaphore（main で上書き）
_api_semaphore: threading.Semaphore = threading.Semaphore(5)

# レートリミット時の基本待機秒数（main で上書き）
_retry_base_wait: int = 60

# print のインターリーブを防ぐロック
_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


# ---------------------------------------------------------------------------
# API 呼び出し（リトライ付き）
# ---------------------------------------------------------------------------

def _call_with_retry(fn, label: str = "", max_retries: int = 6):
    """fn() を呼び出す。429 / quota エラー時は指数バックオフでリトライ。
    待機時間 = _retry_base_wait * 2^attempt（上限 _retry_base_wait * 16）。
    generate_content 専用（Semaphore 内で呼ぶ）。"""
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            err = str(e)
            if any(kw in err.lower() for kw in ("429", "quota", "resource exhausted", "rate limit")):
                wait_sec = min(_retry_base_wait * 16, _retry_base_wait * (2 ** attempt))
                log(f"  [レートリミット{f' ({label})' if label else ''}] {wait_sec}秒待機してリトライ ({attempt + 1}/{max_retries})...")
                time.sleep(wait_sec)
            else:
                raise
    raise RuntimeError(f"最大リトライ回数 ({max_retries}) を超えました: {label}")


# ---------------------------------------------------------------------------
# PDF 処理の各ステップ
# ---------------------------------------------------------------------------

def classify_pdf(model, uploaded_file, label: str = "") -> dict | None:
    """PDFの会計年度・種別・会社名を Gemini で判定する。"""
    def _call():
        with _api_semaphore:
            resp = model.generate_content([uploaded_file, CLASSIFY_PROMPT])
        text = resp.text.strip()
        if "```" in text:
            text = re.sub(r"```[a-z]*", "", text).replace("```", "").strip()
        m = re.search(r"\{.*?\}", text, re.DOTALL)
        if m:
            text = m.group(0)
        return json.loads(text)

    try:
        return _call_with_retry(_call, label=f"{label} 分類")
    except Exception as e:
        log(f"  [{label}] 分類失敗: {e}")
        return None


def extract_category(
    model,
    uploaded_file,
    cat_name: str,
    metrics: list[str],
    label: str = "",
) -> tuple[str, str]:
    """1カテゴリを抽出して (cat_name, result_text) を返す。"""
    metrics_list = "\n".join(f"- {m}" for m in metrics)
    prompt = (
        COT_PROMPT
        .replace("{metrics_list}", metrics_list)
        .replace("{category_name}", cat_name)
    )

    def _call():
        with _api_semaphore:
            resp = model.generate_content(
                [uploaded_file, prompt],
                generation_config=genai.GenerationConfig(temperature=0.0),
            )
        return resp.text.strip()

    result = _call_with_retry(_call, label=f"{label} {cat_name}")
    return cat_name, result


def build_markdown(
    company_name: str,
    fiscal_year,
    document_type: str,
    pdf_filename: str,
    cot_text: str,
) -> str:
    doc_label = {
        "annual_report": "Annual Report (10-K等)",
        "proxy_statement": "Proxy Statement (DEF 14A等)",
        "sustainability_report": "Sustainability / ESG Report",
    }.get(document_type, document_type)

    header = f"""# {company_name} — {fiscal_year}年度 解析レポート

| 項目 | 内容 |
|------|------|
| **会社名** | {company_name} |
| **会計年度** | {fiscal_year} |
| **書類種別** | {doc_label} |
| **ファイル名** | `{pdf_filename}` |

---

## 指標抽出（Chain of Thought）

"""
    return header + cot_text.strip() + "\n"


def md_output_path(company_folder: str, pdf_name: str) -> str:
    out_dir = os.path.join(MD_OUTPUT_ROOT, company_folder)
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(pdf_name)[0]
    return os.path.join(out_dir, f"{stem}.md")


def skip_marker_path(company_folder: str, pdf_name: str) -> str:
    out_dir = os.path.join(MD_OUTPUT_ROOT, company_folder)
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(pdf_name)[0]
    return os.path.join(out_dir, f"{stem}.skip")


def write_skip_marker(skip_path: str, reason: str) -> None:
    with open(skip_path, "w", encoding="utf-8") as f:
        f.write(reason + "\n")


# ---------------------------------------------------------------------------
# 1 PDF の処理（並列呼び出しエントリポイント）
# ---------------------------------------------------------------------------

def process_pdf(
    classify_model,
    extract_model,
    pdf_path: str,
    company_folder: str,
    overwrite: bool,
    pdf_index: int,
    pdf_total: int,
) -> None:
    pdf_name = os.path.basename(pdf_path)
    out_path = md_output_path(company_folder, pdf_name)
    skip_path = skip_marker_path(company_folder, pdf_name)
    prefix = f"[{pdf_index}/{pdf_total}] {company_folder}/{pdf_name}"

    if not overwrite:
        if os.path.exists(out_path):
            log(f"{prefix} → スキップ（既存）")
            return
        if os.path.exists(skip_path):
            with open(skip_path, encoding="utf-8") as f:
                reason = f.read().strip()
            log(f"{prefix} → スキップ（前回: {reason}）")
            return

    uploaded = None
    try:
        # --- アップロード（Semaphore 対象外：ファイル転送は API quota と別枠） ---
        log(f"{prefix} → アップロード中...")
        try:
            uploaded = genai.upload_file(pdf_path, mime_type="application/pdf")
        except Exception as upload_err:
            reason = f"アップロード失敗: {upload_err}"
            log(f"{prefix} → {reason}")
            write_skip_marker(skip_path, reason)
            return

        # --- Step 1: 分類（年度・種別・会社名） ---
        log(f"{prefix} → 分類中...")
        info = classify_pdf(classify_model, uploaded, label=prefix)
        if not info:
            reason = "分類失敗"
            log(f"{prefix} → スキップ（{reason}）")
            write_skip_marker(skip_path, reason)
            return

        fiscal_year = info.get("fiscal_year", "不明")
        document_type = info.get("document_type", "other")
        company_name_in_doc = info.get("company_name_in_doc", company_folder)
        log(f"{prefix} → {fiscal_year}年 / {document_type} / {company_name_in_doc}")

        if document_type == "other":
            reason = f"対象外書類 (other): {company_name_in_doc} {fiscal_year}年"
            log(f"{prefix} → スキップ（{reason}）")
            write_skip_marker(skip_path, reason)
            return

        # --- Step 2: 3カテゴリを並列抽出（v2 の核心） ---
        log(f"{prefix} → CoT抽出中（3カテゴリ並列）...")
        categories = [
            ("ガバナンス・役員・報酬", METRICS_GOV),
            ("財務・基本情報",         METRICS_FIN),
            ("人的資本・ESG",          METRICS_HR),
        ]
        results: dict[str, str] = {}

        with ThreadPoolExecutor(max_workers=3) as cat_pool:
            cat_futures = {
                cat_pool.submit(
                    extract_category,
                    extract_model, uploaded, cat_name, metrics, prefix,
                ): cat_name
                for cat_name, metrics in categories
            }
            for future in as_completed(cat_futures):
                try:
                    cat_name, text = future.result()
                    results[cat_name] = text
                    log(f"{prefix} → {cat_name} 完了")
                except Exception as e:
                    cat_name = cat_futures[future]
                    log(f"{prefix} → {cat_name} エラー: {e}")
                    results[cat_name] = f"（抽出エラー: {e}）"

        # カテゴリ定義順で結合（as_completed は順不同のため）
        cot_parts = [
            f"## {cat_name}\n\n{results.get(cat_name, '（結果なし）')}\n"
            for cat_name, _ in categories
        ]
        cot_text = "\n".join(cot_parts)

        # --- Step 3: Markdown 組み立て・保存 ---
        md = build_markdown(company_name_in_doc, fiscal_year, document_type, pdf_name, cot_text)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)

        rel = os.path.relpath(out_path, "/workspaces/lancers_research")
        log(f"{prefix} → 保存完了: {rel}")

    except Exception as e:
        log(f"{prefix} → エラー: {e}")
    finally:
        # 処理済みファイルをサーバー上から削除
        if uploaded:
            try:
                genai.delete_file(uploaded.name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# PDF 収集
# ---------------------------------------------------------------------------

def collect_pdfs(
    root: str,
    company_filter: str | None,
    start_from: str | None = None,
    pdf_filter: str | None = None,
) -> list[tuple[str, str]]:
    """(会社フォルダ名, PDFパス) のリストを再帰的に取得して返す。"""
    # --pdf で絶対パスのPDFファイルを直接指定された場合
    if pdf_filter and os.path.isabs(pdf_filter):
        if not os.path.isfile(pdf_filter):
            print(f"[エラー] 指定されたPDFが見つかりません: {pdf_filter}")
            sys.exit(1)
        folder_name = os.path.basename(os.path.dirname(pdf_filter))
        # pdfs/<company>/<file> 構造を想定してフォルダ名を親から取得
        parent = os.path.dirname(pdf_filter)
        if os.path.dirname(parent) == root:
            folder_name = os.path.basename(parent)
        else:
            folder_name = os.path.relpath(parent, root).split(os.sep)[0]
        return [(folder_name, pdf_filter)]

    result = []
    if not os.path.isdir(root):
        print(f"[エラー] PDFフォルダが存在しません: {root}")
        sys.exit(1)

    reached = start_from is None  # start_from 未指定なら最初から処理

    for folder_name in sorted(os.listdir(root)):
        folder_path = os.path.join(root, folder_name)
        if not os.path.isdir(folder_path) or folder_name.startswith("."):
            continue
        if company_filter and company_filter.lower() not in folder_name.lower():
            continue
        # --start-from: 指定フォルダ名（部分一致）に到達したら処理開始
        if not reached:
            if start_from.lower() in folder_name.lower():
                reached = True
            else:
                continue

        for current_dir, dirs, files in os.walk(folder_path):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fname in sorted(files):
                if fname.lower().endswith((".pdf", ".html", ".htm")):
                    if pdf_filter and pdf_filter.lower() not in fname.lower():
                        continue
                    result.append((folder_name, os.path.join(current_dir, fname)))

    return result


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------

def main():
    global MD_OUTPUT_ROOT, _api_semaphore, _retry_base_wait

    parser = argparse.ArgumentParser(
        description="PDFをChain of Thoughtで解析してMarkdownに出力する（高速版 v2）"
    )
    parser.add_argument("--company", metavar="NAME",
                        help="会社フォルダ名で絞り込み（部分一致）")
    parser.add_argument("--start-from", metavar="NAME",
                        help="指定した会社フォルダ名（部分一致）以降のみ処理する")
    parser.add_argument("--overwrite", action="store_true",
                        help="既存のMarkdownを上書きする")
    parser.add_argument("--output", default=MD_OUTPUT_ROOT, metavar="DIR",
                        help=f"出力先フォルダ（デフォルト: {MD_OUTPUT_ROOT}）")
    parser.add_argument("--workers", type=int, default=2, metavar="N",
                        help="PDF並列処理数（デフォルト: 2）。レートリミットに注意")
    parser.add_argument("--api-concurrency", type=int, default=5, metavar="N",
                        help="同時 generate_content 呼び出し上限（デフォルト: 5）")
    parser.add_argument("--retry-base-wait", type=int, default=60, metavar="SEC",
                        help="レートリミット時の基本待機秒数（デフォルト: 60）。指数バックオフの底になる")
    parser.add_argument("--pdf", metavar="PATH_OR_NAME",
                        help="処理するPDFを絶対パスまたはファイル名（部分一致）で指定")
    args = parser.parse_args()

    MD_OUTPUT_ROOT = os.path.abspath(args.output)
    _api_semaphore = threading.Semaphore(args.api_concurrency)
    _retry_base_wait = args.retry_base_wait

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("[エラー] 環境変数 GEMINI_API_KEY が設定されていません")
        sys.exit(1)
    genai.configure(api_key=api_key)

    classify_model = genai.GenerativeModel(
        MODEL_NAME,
        generation_config=genai.GenerationConfig(
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )
    extract_model = genai.GenerativeModel(MODEL_NAME)

    pdfs = collect_pdfs(PDF_ROOT, args.company, getattr(args, "start_from", None), getattr(args, "pdf", None))
    if not pdfs:
        print("処理対象のPDFが見つかりませんでした。")
        return

    total = len(pdfs)
    print(
        f"対象: {total} ファイル"
        f"  |  PDF並列数: {args.workers}"
        f"  |  同時API上限: {args.api_concurrency}\n"
    )

    # 複数 PDF を並列処理
    with ThreadPoolExecutor(max_workers=args.workers) as pdf_pool:
        futures = [
            pdf_pool.submit(
                process_pdf,
                classify_model, extract_model,
                pdf_path, company_folder,
                args.overwrite,
                i + 1, total,
            )
            for i, (company_folder, pdf_path) in enumerate(pdfs)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"[未処理エラー] {e}")

    print(f"\n完了。出力先: {MD_OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
