"""ISO30414(2018)指標をGemini Batch APIで抽出するスクリプト。

v2 との主な相違点:
  - 指標セット: ISO30414(2018)に特化した63指標（7カテゴリ）を抽出
  - API方式:    Batch API利用で料金約50%削減（非同期処理・完了まで最大24時間）
  - 依存SDK:    google-genai (新SDK) を使用
  - 出力先:     md_outputs_iso30414/ ディレクトリ

必要なパッケージのインストール:
  pip install google-genai

使い方:
  # ① バッチジョブを作成して送信のみ（途中で終了しても再開可能）
  python3 extract_iso30414_batch.py --submit

  # ② 送信済みジョブの完了待ちと結果処理
  python3 extract_iso30414_batch.py --poll --state-file batch_state_XXXXXXXX.json

  # ③ 全工程を1コマンドで実行（完了まで待機、最大24時間）
  python3 extract_iso30414_batch.py --run

  # 会社を絞り込んで実行
  python3 extract_iso30414_batch.py --run --company "General Mills"

  # 既存の出力を上書き
  python3 extract_iso30414_batch.py --run --overwrite

  # 使用モデルを変更（デフォルト: gemini-2.5-flash）
  python3 extract_iso30414_batch.py --run --model gemini-2.0-flash

Batch API の仕組み:
  1. 全PDFをFiles APIへアップロード
  2. 全リクエストをJSONLファイルにまとめてFiles APIへアップロード
  3. バッチジョブを1件送信
  4. 完了後、結果JSONLを取得してMarkdownを生成・保存
  5. アップロードしたファイルを削除
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from google import genai
    from google.genai import types as genai_types
except ImportError:
    print("[エラー] google-genai パッケージが必要です。")
    print("  pip install google-genai")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

PDF_ROOT = "/workspaces/lancers_research/pdfs"
MD_OUTPUT_ROOT = "/workspaces/lancers_research/md_outputs_iso30414"
STATE_DIR = "/workspaces/lancers_research"

DEFAULT_MODEL = "gemini-2.5-flash"

# バッチ結果ポーリング間隔（秒）
POLL_INTERVAL = 60

# バッチジョブのタイムアウト（秒）: 24時間
POLL_TIMEOUT = 86400

# リクエストキーの区切り文字（ファイル名に含まれにくい文字列）
KEY_SEP = "@@"

# ---------------------------------------------------------------------------
# ISO30414(2018) 指標カテゴリ定義
# フォーマット: (指標ID, 日本語名, 英語名)
# ---------------------------------------------------------------------------

CATEGORIES: list[tuple[str, list[tuple[str, str, str]]]] = [
    (
        "コンプライアンス・倫理",
        [
            ("types_and_number_of_complaints_that_were_raised",
             "提起された苦情の種類と件数",
             "Number and types of grievances filed"),
            ("types_and_number_of_disciplinary_action",
             "懲戒処分の種類と件数",
             "Number and types of disciplinary actions"),
            ("percentage_of_employees_who_received_ethics_and_compliance_training",
             "倫理・コンプライアンス研修を受けた従業員の割合",
             "Percentage of employees trained in ethics and compliance"),
            ("dispatched_to_be_resolved_by_third_parties",
             "第三者に解決を委ねられた係争",
             "Number of disputes referred to third parties"),
            ("the_number_of_matters_pointed_out_in_external_audits_the_type_the_source_of_the_occurrence_and_the_response_to_them",
             "外部監査で指摘された事項の数、種類および発生源と、それらへの対応",
             "Number, type, and source of issues identified in external audits, and actions taken"),
        ],
    ),
    (
        "コスト",
        [
            ("total_labor_cost",
             "総労働力コスト",
             "Total workforce cost"),
            ("external_labor_cost",
             "外部労働力コスト",
             "External workforce cost"),
            ("remuneration_ratio_for_specific_jobs_for_total_salary",
             "総給与に対する特定職の報酬割合",
             "Ratio of specific role compensation to total payroll"),
            ("total_employment_cost",
             "総雇用コスト",
             "Total employment cost"),
            ("one_person_recruitment_cost_new_graduate_mid_career",
             "1人当り採用コスト（新卒）",
             "Cost per hire (graduates)"),
            ("one_person_recruitment_cost_mid_career",
             "1人当り採用コスト（中途）",
             "Cost per hire (experienced hires)"),
            ("recruitment_cost",
             "採用コスト",
             "Total recruitment cost"),
            ("costs_associated_with_retirement",
             "離職に伴うコスト",
             "Cost of turnover"),
        ],
    ),
    (
        "ダイバーシティ（年齢・性別）",
        [
            ("employee_ratio_under_15_years_old",
             "従業員比率（15歳未満）",
             "Percentage of employees under age 15"),
            ("employee_ratio_15_19_years_old",
             "従業員比率（15歳～19歳）",
             "Percentage of employees aged 15-19"),
            ("employee_ratio_20_29_years_old",
             "従業員比率（20歳～29歳）",
             "Percentage of employees aged 20-29"),
            ("employee_ratio_30_39_years_old",
             "従業員比率（30歳～39歳）",
             "Percentage of employees aged 30-39"),
            ("employee_ratio_40_49_years_old",
             "従業員比率（40歳～49歳）",
             "Percentage of employees aged 40-49"),
            ("employee_ratio_50_59_years_old",
             "従業員比率（50歳～59歳）",
             "Percentage of employees aged 50-59"),
            ("employee_ratio_60_years_old",
             "従業員比率（60歳以上）",
             "Percentage of employees aged 60 and above"),
            ("male_employee_ratio",
             "男性従業員比率",
             "Percentage of male employees"),
            ("female_employee_ratio",
             "女性従業員比率",
             "Percentage of female employees"),
        ],
    ),
    (
        "リーダーシップ・エンゲージメント・安全衛生",
        [
            ("employee_survey_score_trust_in_leadership",
             "従業員サーベイスコア（リーダーシップに対する信頼）",
             "Employee survey score (trust in leadership)"),
            ("number_of_subordinates_per_manager",
             "管理職1人当りの部下数",
             "Span of control (employees per manager)"),
            ("participation_rate_of_leader_ship_development_and_training",
             "リーダーシップ開発・研修の参加率",
             "Leadership development participation rate"),
            ("engagement_score",
             "エンゲージメントスコア",
             "Employee engagement score"),
            ("established_rate_of_employees",
             "従業員の定着率",
             "Employee retention rate"),
            ("time_ratio_lost_by_work_injury",
             "労災により失われた時間",
             "Lost time due to work-related injuries"),
            ("mortality_rate_due_to_work_related_accidents",
             "労災による死亡者数",
             "Number of fatalities due to work-related injuries"),
            ("remarks_of_health_and_safety_training",
             "健康・安全研修の受講割合",
             "Percentage of employees trained in health and safety"),
        ],
    ),
    (
        "生産性・採用",
        [
            ("ebit_per_fte",
             "従業員1人あたりEBIT",
             "EBIT per employee"),
            ("sales_per_employee",
             "従業員一人当たり売上",
             "Revenue per employee"),
            ("net_income_per_employee",
             "従業員一人当たり純利益",
             "Net profit per employee"),
            ("human_capital_roi",
             "人的資本RoI",
             "Human capital ROI (HCROI)"),
            ("number_of_qualified_candidates_per_position",
             "募集ポスト当りの書類選考通過者数",
             "Number of candidates per vacancy"),
            ("quality_per_hire",
             "採用社員の質",
             "Quality of hire"),
            ("average_number_of_days_for_recruitment",
             "採用にかかる平均日数",
             "Average time to hire"),
            ("average_number_of_days_until_important_posts_are_filled",
             "重要ポストが埋まる迄の平均日数",
             "Average time to fill critical positions"),
            ("internal_appointment_rate",
             "内部登用率",
             "Internal hire rate"),
            ("internal_appointment_rate_of_important_posts",
             "重要ポストの内部登用率",
             "Internal hire rate for critical positions"),
            ("percentage_of_important_posts",
             "重要ポストの割合",
             "Percentage of critical positions"),
            ("empty_seat_rate_of_important_posts_in_all_vacant_seats",
             "全空席中の重要ポストの空席率",
             "Vacancy rate for critical positions"),
            ("internal_transfer_number",
             "内部異動数",
             "Number of internal moves"),
        ],
    ),
    (
        "後継者計画・離職",
        [
            ("preparation_for_executive_candidates",
             "幹部候補の準備度",
             "Succession readiness (leadership pipeline readiness)"),
            ("turnover",
             "離職率",
             "Turnover rate"),
            ("spontaneous_turnover_rate",
             "自発的離職率",
             "Voluntary turnover rate"),
            ("spontaneous_turnover_rate_that_is_painful",
             "痛手となる自発的離職率",
             "Regrettable voluntary turnover rate"),
            ("internal_succession_rate",
             "内部継承率",
             "Internal succession rate"),
            ("successor_candidate_preparation_rate",
             "後継者候補準備率",
             "Successor readiness rate"),
            ("successor_s_inheritance_preparation_immediate",
             "後継者の継承準備度（即時）",
             "Ready-now successors"),
            ("successor_s_inheritance_preparation_specific_period_a",
             "後継者の継承準備度（1-3年）",
             "Successors ready in 1-3 years"),
            ("successor_s_inheritance_preparation_specific_period_b",
             "後継者の継承準備度（4-5年）",
             "Successors ready in 4-5 years"),
        ],
    ),
    (
        "研修・従業員数",
        [
            ("total_expenses_for_human_resource_development_and_training",
             "人材開発・研修の総費用",
             "Total training and development cost"),
            ("participation_rate_in_training",
             "研修への参加率",
             "Training participation rate"),
            ("training_time_for_employees",
             "従業員当りの研修受講時間",
             "Training hours per employee"),
            ("employee_competition_sea_rate",
             "従業員のコンピテンシーレート",
             "Employee competency rate"),
            ("total_number_of_employees",
             "総従業員数",
             "Total number of employees"),
            ("total_number_of_employees_full_time",
             "総従業員数（フルタイム）",
             "Total number of employees (full-time)"),
            ("total_number_of_employees_part_time",
             "総従業員数（パートタイム）",
             "Total number of employees (part-time)"),
            ("full_time_equivalent_fte",
             "フルタイム当量 (FTE)",
             "Full-time equivalent (FTE)"),
            ("temporary_labor_independent_employer",
             "臨時の労働力（独立事業主）",
             "Contingent workforce (independent contractors)"),
            ("temporary_labor_dispatched_worker",
             "臨時の労働力（派遣労働者）",
             "Contingent workforce (agency workers)"),
            ("absence_rate",
             "欠勤率",
             "Absenteeism rate"),
        ],
    ),
]

# ---------------------------------------------------------------------------
# プロンプトテンプレート
# ---------------------------------------------------------------------------

CLASSIFY_PROMPT = """このPDFの1ページ目（表紙）または目次を確認して、以下のJSON形式のみで返してください。
{"fiscal_year": 2024, "document_type": "annual_report", "company_name_in_doc": "General Mills, Inc."}

- fiscal_year: 会計年度（西暦4桁の整数。不明な場合は null）
  ※ Proxy StatementはAnnual Meeting開催年 - 1 を会計年度としてください。
- document_type: "annual_report" (10-K等) / "proxy_statement" (DEF 14A等) / "sustainability_report" (ESGレポート等) / "other"
- company_name_in_doc: 文書タイトルや表紙に記載されている会社名

JSON以外のテキストは不要です。"""

COT_PROMPT_TEMPLATE = """あなたは企業の年次報告書（10-K）、委任状説明書（Proxy Statement）、サステナビリティレポート等を分析し、ISO30414:2018（人的資本報告）に定められた指標を正確に抽出する専門家です。
提供されたPDFファイルについて、以下のステップバイステップ（Chain of Thought）で各指標を抽出してください。

現在の抽出カテゴリ: 「{category_name}」

## 重要な指示：視覚（マルチモーダル）能力の活用とページ番号の特定（2段階アプローチ）
あなたはPDFを「画像（ネイティブPDF）」として視覚的に認識できます。以下の手順で慎重に探してください。
1. **検索とページ特定**: まず目次（Table of Contents）を確認し、該当指標が載っていそうなセクションの「ページ番号」を特定してください。
2. **発見**: そのページに移動し、該当する箇所の原文テキストや、視覚的に読み取った表のレイアウト・チェックボックスなどの文脈を引用・説明してください。
3. **計算（必要な場合）**: 実数しかなく比率が問われている場合は、取得した数値を用いて計算してください。
4. **結論**: 最終的な数値（単位を明記）と、それが記載されていたページ番号。

## グラフや図表の読み取りに関する特別な指示
- **グラフはあるが数値が不明な場合**: 該当する指標のグラフやチャートが存在するものの、目盛りや数値ラベルが省略されており正確な値が読み取れない場合は、無理に推測しないでください。
- その場合、**値**には「グラフあり・詳細数値不明」とし、**読み取り方**に「〇〇のグラフはあるが数値の記載なし」と理由を報告してください。

## 出力形式（Markdown）

### {指標名（日本語）} / {Metric Name (English)}
- **指標ID**: {metric_id}
- **値**: {数値と単位、または「記載なし」、または「グラフあり・詳細数値不明」}
- **記載ページ**: {PDFの該当ページ番号、または「記載なし」}
- **根拠**:
  > {PDFからの原文引用や、表・グラフから視覚的に読み取った内容の説明}
- **計算式**: {計算を行った場合、実際に使用した数値と数式（例: 1,020 ÷ 1,200 × 100 = 85.0%）。計算していない場合は「なし」}

---

## 抽出対象の指標リスト（ISO30414:2018 / {category_name}）

{metrics_list}

## 注意事項
- ISO30414:2018 は人的資本報告の国際標準規格です。企業によって開示項目・名称が異なる場合があります。
- 英語名と日本語名の両方を参照し、同義の指標を見落とさないようにしてください。
- 資料の性質上、記載がない項目も多々あります。テキストもグラフも見つからない場合は「記載なし」としてください。
- 記載ページは、PDFの物理的なページ数ではなく、資料に印字されているページ番号を優先してください。
- パーセンテージは「30.5%」のように単位付きで記載。
- 金額は「USD 1,234 million」のように通貨と単位を明記。
"""


def build_extract_prompt(cat_name: str, metrics: list[tuple[str, str, str]]) -> str:
    lines = []
    for metric_id, ja, en in metrics:
        lines.append(f"- **{ja}** / {en}  [ID: `{metric_id}`]")
    metrics_list = "\n".join(lines)
    return (
        COT_PROMPT_TEMPLATE
        .replace("{category_name}", cat_name)
        .replace("{metrics_list}", metrics_list)
    )


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def safe_key_part(s: str) -> str:
    """ファイル名・フォルダ名をバッチリクエストキー用の安全な文字列に変換する。"""
    return re.sub(r"[^a-zA-Z0-9\-]", "_", s)


def make_request_key(company_folder: str, pdf_stem: str, suffix: str) -> str:
    return KEY_SEP.join([safe_key_part(company_folder), safe_key_part(pdf_stem), suffix])


def parse_request_key(key: str) -> tuple[str, str, str]:
    """make_request_key の逆変換。(company, stem, suffix) を返す。"""
    parts = key.split(KEY_SEP)
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return "", "", key


# ---------------------------------------------------------------------------
# PDF 収集（v2 と同一ロジック）
# ---------------------------------------------------------------------------

def collect_pdfs(
    root: str,
    company_filter: Optional[str] = None,
    start_from: Optional[str] = None,
    pdf_path: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[tuple[str, str]]:
    """(会社フォルダ名, PDFパス) のリストを返す。"""
    # --pdf で絶対パスのPDFファイルを直接指定された場合
    if pdf_path and os.path.isabs(pdf_path):
        if not os.path.isfile(pdf_path):
            print(f"[エラー] 指定されたPDFが見つかりません: {pdf_path}")
            sys.exit(1)
        parent = os.path.dirname(pdf_path)
        if os.path.dirname(parent) == root:
            folder_name = os.path.basename(parent)
        else:
            folder_name = os.path.relpath(parent, root).split(os.sep)[0]
        return [(folder_name, pdf_path)]

    result = []
    if not os.path.isdir(root):
        print(f"[エラー] PDFフォルダが存在しません: {root}")
        sys.exit(1)

    reached = start_from is None

    for folder_name in sorted(os.listdir(root)):
        folder_path = os.path.join(root, folder_name)
        if not os.path.isdir(folder_path) or folder_name.startswith("."):
            continue
        if company_filter and company_filter.lower() not in folder_name.lower():
            continue
        if not reached:
            if start_from.lower() in folder_name.lower():
                reached = True
            else:
                continue

        for current_dir, dirs, files in os.walk(folder_path):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fname in sorted(files):
                if fname.lower().endswith((".pdf", ".html", ".htm")):
                    if pdf_path and pdf_path.lower() not in fname.lower():
                        continue
                    result.append((folder_name, os.path.join(current_dir, fname)))
                    if limit and len(result) >= limit:
                        return result

    return result


# ---------------------------------------------------------------------------
# Markdown 生成
# ---------------------------------------------------------------------------

def build_markdown(
    company_name: str,
    fiscal_year,
    document_type: str,
    pdf_filename: str,
    extract_texts: dict[str, str],
) -> str:
    doc_label = {
        "annual_report": "Annual Report (10-K等)",
        "proxy_statement": "Proxy Statement (DEF 14A等)",
        "sustainability_report": "Sustainability / ESG Report",
    }.get(document_type, document_type)

    header = f"""# {company_name} — {fiscal_year}年度 ISO30414 解析レポート

| 項目 | 内容 |
|------|------|
| **会社名** | {company_name} |
| **会計年度** | {fiscal_year} |
| **書類種別** | {doc_label} |
| **ファイル名** | `{pdf_filename}` |
| **フレームワーク** | ISO30414:2018（人的資本報告） |

---

## 指標抽出（Chain of Thought）

"""
    parts = []
    for cat_name, _ in CATEGORIES:
        cat_key_suffix = f"cat_{safe_key_part(cat_name)}"
        text = extract_texts.get(cat_key_suffix, "（結果なし）")
        parts.append(f"## {cat_name}\n\n{text}\n")

    return header + "\n".join(parts)


def md_output_path(company_folder: str, pdf_name: str, output_root: str) -> str:
    out_dir = os.path.join(output_root, company_folder)
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(pdf_name)[0]
    return os.path.join(out_dir, f"{stem}.md")


# ---------------------------------------------------------------------------
# 通常API 並列処理
# ---------------------------------------------------------------------------

def _generate_with_retry(
    client: "genai.Client",
    model: str,
    file_uri: str,
    prompt: str,
    temperature: float = 0.0,
    response_mime_type: Optional[str] = None,
    max_retries: int = 5,
) -> Optional[str]:
    """リトライ付きで generate_content を呼び出す。"""
    config_kwargs: dict = {"temperature": temperature}
    if response_mime_type:
        config_kwargs["response_mime_type"] = response_mime_type
    config = genai_types.GenerateContentConfig(**config_kwargs)

    contents = [
        genai_types.Content(
            role="user",
            parts=[
                genai_types.Part(
                    file_data=genai_types.FileData(
                        mime_type="application/pdf",
                        file_uri=file_uri,
                    )
                ),
                genai_types.Part(text=prompt),
            ],
        )
    ]

    for attempt in range(max_retries):
        try:
            resp = client.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
            return resp.text
        except Exception as e:
            if attempt >= max_retries - 1:
                raise
            err = str(e)
            if "429" in err or "quota" in err.lower() or "rate" in err.lower():
                wait = 60 * (2 ** attempt)
                print(f"    レート制限 ({attempt+1}/{max_retries}): {wait}秒待機...")
                time.sleep(wait)
            else:
                time.sleep(15)
    return None


def process_one_pdf(
    client: "genai.Client",
    model: str,
    company_folder: str,
    pdf_path: str,
    output_root: str,
    overwrite: bool,
) -> tuple[str, str]:
    """1つのPDFを分類・抽出してMarkdownを保存する。Returns: (status, message)"""
    pdf_name = os.path.basename(pdf_path)
    out_path = md_output_path(company_folder, pdf_name, output_root)

    if not overwrite and os.path.exists(out_path):
        return "skipped", ""

    file_ref = None
    try:
        # PDFをFiles APIへアップロード（display_nameはASCII安全な名前を使用）
        safe_display_name = re.sub(r"[^a-zA-Z0-9._\-]", "_", pdf_name)[:40] or "file.pdf"
        file_ref = client.files.upload(
            file=pdf_path,
            config={"mime_type": "application/pdf", "display_name": safe_display_name},
        )
        file_uri = file_ref.uri

        # 分類
        classify_text = _generate_with_retry(
            client, model, file_uri, CLASSIFY_PROMPT,
            response_mime_type="application/json",
        )
        fiscal_year = "不明"
        document_type = "other"
        company_name = company_folder
        if classify_text:
            try:
                text = classify_text.strip()
                if "```" in text:
                    text = re.sub(r"```[a-z]*", "", text).replace("```", "").strip()
                m = re.search(r"\{.*?\}", text, re.DOTALL)
                if m:
                    info = json.loads(m.group(0))
                    fiscal_year = info.get("fiscal_year", "不明")
                    document_type = info.get("document_type", "other")
                    company_name = info.get("company_name_in_doc", company_folder)
            except Exception:
                pass

        # 各カテゴリ抽出
        extract_texts: dict[str, str] = {}
        for cat_name, metrics in CATEGORIES:
            cat_suffix = f"cat_{safe_key_part(cat_name)}"
            prompt = build_extract_prompt(cat_name, metrics)
            result = _generate_with_retry(client, model, file_uri, prompt)
            extract_texts[cat_suffix] = result or "（結果なし）"

        # Markdown保存
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        md = build_markdown(company_name, fiscal_year, document_type, pdf_name, extract_texts)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)

        return "saved", ""

    except Exception as e:
        return "error", str(e)

    finally:
        if file_ref:
            try:
                client.files.delete(name=file_ref.name)
            except Exception:
                pass


def run_concurrent(
    client: "genai.Client",
    model: str,
    pdfs: list[tuple[str, str]],
    overwrite: bool,
    output_root: str,
    workers: int = 3,
) -> None:
    """通常のGenerative APIを並列処理で全PDFを処理する（バッチAPI不使用）。"""
    total = len(pdfs)
    print(f"\n=== 並列処理開始: {total} ファイル / 最大 {workers} 並列 ===")
    print(f"  モデル: {model}")
    print(f"  出力先: {output_root}")

    saved = skipped = errors = 0
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_pdf = {
            executor.submit(
                process_one_pdf,
                client, model, company_folder, pdf_path, output_root, overwrite,
            ): (company_folder, os.path.basename(pdf_path))
            for company_folder, pdf_path in pdfs
        }

        for future in as_completed(future_to_pdf):
            company_folder, pdf_name = future_to_pdf[future]
            done += 1
            try:
                status, msg = future.result()
                if status == "saved":
                    saved += 1
                    print(f"  [{done}/{total}] ✓ 保存: {company_folder}/{pdf_name}")
                elif status == "skipped":
                    skipped += 1
                    print(f"  [{done}/{total}] スキップ（既存）: {company_folder}/{pdf_name}")
                else:
                    errors += 1
                    print(f"  [{done}/{total}] ✗ エラー: {company_folder}/{pdf_name}: {msg}")
            except Exception as e:
                errors += 1
                print(f"  [{done}/{total}] ✗ 例外: {company_folder}/{pdf_name}: {e}")

    print(f"\n完了: 保存={saved}件 / スキップ={skipped}件 / エラー={errors}件")
    print(f"出力先: {output_root}")


# ---------------------------------------------------------------------------
# バッチサブミット
# ---------------------------------------------------------------------------

def submit(
    client: "genai.Client",
    model: str,
    pdfs: list[tuple[str, str]],
    overwrite: bool,
    output_root: str,
    batch_size: int = 20,
) -> str:
    """PDFをアップロードしてバッチジョブを送信し、ステートファイルのパスを返す。

    batch_size: バッチあたりの最大PDF数。小さいほど結果がinlined_responsesに
    格納されやすい（大きすぎるとファイル格納になりダウンロード不可になる）。
    """
    total = len(pdfs)
    print(f"\n=== Phase 1: PDFをFiles APIへアップロード ({total} ファイル) ===")

    # (company_folder, pdf_path, pdf_name, file_name, file_uri) のリスト
    uploaded: list[dict] = []
    skipped: list[dict] = []

    for i, (company_folder, pdf_path) in enumerate(pdfs, 1):
        pdf_name = os.path.basename(pdf_path)
        out_path = md_output_path(company_folder, pdf_name, output_root)

        if not overwrite and os.path.exists(out_path):
            print(f"  [{i}/{total}] スキップ（既存）: {company_folder}/{pdf_name}")
            skipped.append({
                "company_folder": company_folder,
                "pdf_path": pdf_path,
                "pdf_name": pdf_name,
                "out_path": out_path,
                "skipped": True,
            })
            continue

        print(f"  [{i}/{total}] アップロード中: {company_folder}/{pdf_name}", end="", flush=True)
        try:
            safe_display_name = re.sub(r"[^a-zA-Z0-9._\-]", "_", pdf_name)[:40] or "file.pdf"
            file_ref = client.files.upload(
                file=pdf_path,
                config={"mime_type": "application/pdf", "display_name": safe_display_name},
            )
            print(f" → {file_ref.name}")
            uploaded.append({
                "company_folder": company_folder,
                "pdf_path": pdf_path,
                "pdf_name": pdf_name,
                "file_name": file_ref.name,
                "file_uri": file_ref.uri,
                "out_path": out_path,
                "skipped": False,
            })
        except Exception as e:
            print(f" → アップロード失敗: {e}")
            skipped.append({
                "company_folder": company_folder,
                "pdf_path": pdf_path,
                "pdf_name": pdf_name,
                "out_path": out_path,
                "skipped": True,
                "upload_error": str(e),
            })

    if not uploaded:
        print("\n処理対象のPDFがありません（全てスキップ）。")
        sys.exit(0)

    # アップロード済みPDFをbatch_sizeごとのチャンクに分割
    chunks = [uploaded[i:i + batch_size] for i in range(0, len(uploaded), batch_size)]
    n_batches = len(chunks)
    reqs_per_pdf = 1 + len(CATEGORIES)
    print(f"\n=== Phase 2-4: バッチリクエスト作成・送信 ({len(uploaded)} PDF → {n_batches} バッチ) ===")
    print(f"  最大 {batch_size} PDF/バッチ × {reqs_per_pdf} リクエスト/PDF = 最大 {batch_size * reqs_per_pdf} リクエスト/バッチ")

    # アップロード済みPDFのファイルがFiles APIで利用可能になるまで少し待つ
    print("  Files APIの準備待機中（30秒）...")
    time.sleep(30)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_infos: list[dict] = []

    for batch_idx, chunk in enumerate(chunks, 1):
        print(f"\n--- バッチ {batch_idx}/{n_batches}: {len(chunk)} PDF ({len(chunk) * reqs_per_pdf} リクエスト) ---")

        requests_data: list[dict] = []
        for entry in chunk:
            company_folder = entry["company_folder"]
            pdf_name = entry["pdf_name"]
            file_uri = entry["file_uri"]
            stem = os.path.splitext(pdf_name)[0]

            # --- 分類リクエスト ---
            classify_key = make_request_key(company_folder, stem, "classify")
            requests_data.append({
                "key": classify_key,
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [
                                {
                                    "fileData": {
                                        "mimeType": "application/pdf",
                                        "fileUri": file_uri,
                                    }
                                },
                                {"text": CLASSIFY_PROMPT},
                            ],
                        }
                    ],
                    "generationConfig": {"temperature": 0.0, "responseMimeType": "application/json"},
                },
            })

            # --- 各カテゴリの抽出リクエスト ---
            for cat_name, metrics in CATEGORIES:
                cat_suffix = f"cat_{safe_key_part(cat_name)}"
                cat_key = make_request_key(company_folder, stem, cat_suffix)
                prompt = build_extract_prompt(cat_name, metrics)
                requests_data.append({
                    "key": cat_key,
                    "request": {
                        "contents": [
                            {
                                "role": "user",
                                "parts": [
                                    {
                                        "fileData": {
                                            "mimeType": "application/pdf",
                                            "fileUri": file_uri,
                                        }
                                    },
                                    {"text": prompt},
                                ],
                            }
                        ],
                        "generationConfig": {"temperature": 0.0},
                    },
                })

        # JSONL ファイルを保存
        jsonl_path = os.path.join(STATE_DIR, f"batch_requests_{ts}_{batch_idx:03d}.jsonl")
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for req in requests_data:
                f.write(json.dumps(req, ensure_ascii=False) + "\n")
        print(f"  JSONL保存: {os.path.basename(jsonl_path)} ({len(requests_data)} 件)")

        # JSONL を Files API へアップロード
        jsonl_ref = client.files.upload(
            file=jsonl_path,
            config={"mime_type": "text/plain"},
        )
        print(f"  JSONL アップロード完了: {jsonl_ref.name}")

        # バッチジョブを送信
        batch_job = client.batches.create(
            model=f"models/{model}",
            src=jsonl_ref.name,
        )
        print(f"  バッチジョブ名: {batch_job.name}")
        print(f"  状態: {batch_job.state}")

        batch_infos.append({
            "batch_index": batch_idx,
            "job_name": batch_job.name,
            "jsonl_file_name": jsonl_ref.name,
            "pdf_count": len(chunk),
        })

        # バッチ送信間に少し間隔を置く
        if batch_idx < n_batches:
            time.sleep(3)

    # ステートファイルを保存
    state = {
        "batch_infos": batch_infos,
        "job_name": batch_infos[0]["job_name"],  # 旧フォーマット互換
        "model": model,
        "submitted_at": ts,
        "output_root": output_root,
        "pdfs": uploaded + skipped,
    }
    state_path = os.path.join(STATE_DIR, f"batch_state_{ts}.json")
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    print(f"\nステートファイル保存: {state_path}")
    if n_batches == 1:
        print(f"  ジョブ: {batch_infos[0]['job_name']}")
    else:
        for info in batch_infos:
            print(f"  バッチ {info['batch_index']}: {info['job_name']}")
    print(f"\n✅ バッチジョブ送信完了。結果取得は以下のコマンドで実行してください:")
    print(f"   python3 extract_iso30414_batch.py --poll --state-file {state_path}")

    return state_path


# ---------------------------------------------------------------------------
# バッチ結果取得・Markdown 生成
# ---------------------------------------------------------------------------

def poll_and_process(client: "genai.Client", state_path: str) -> None:
    """バッチジョブの完了を待ち、結果からMarkdownを生成する。"""
    with open(state_path, encoding="utf-8") as f:
        state = json.load(f)

    output_root = state["output_root"]
    pdfs = state["pdfs"]

    # 旧フォーマット（job_name のみ）と新フォーマット（batch_infos リスト）の両方をサポート
    batch_infos = state.get("batch_infos")
    if batch_infos is None:
        batch_infos = [{
            "batch_index": 1,
            "job_name": state["job_name"],
            "jsonl_file_name": state.get("jsonl_file_name"),
        }]

    n_batches = len(batch_infos)
    print(f"\n=== バッチジョブ完了待機: {n_batches} バッチ ===")
    print(f"  ポーリング間隔: {POLL_INTERVAL}秒 / タイムアウト: {POLL_TIMEOUT // 3600}時間")
    for info in batch_infos:
        print(f"  バッチ {info['batch_index']}: {info['job_name']}")

    terminal_states = {"JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"}

    def is_terminal(s) -> bool:
        return any(t in str(s) for t in terminal_states)

    def is_succeeded(s) -> bool:
        return "JOB_STATE_SUCCEEDED" in str(s)

    # 全バッチが完了するまでポーリング
    start_time = time.time()
    pending = {info["job_name"]: info for info in batch_infos}
    finished: dict = {}  # job_name -> batch_job オブジェクト

    while pending:
        for job_name in list(pending.keys()):
            batch_job = client.batches.get(name=job_name)
            if is_terminal(batch_job.state):
                idx = pending[job_name]["batch_index"]
                print(f"  ✓ バッチ {idx} 完了: {batch_job.state}")
                finished[job_name] = batch_job
                del pending[job_name]
        if pending:
            elapsed = int(time.time() - start_time)
            if elapsed > POLL_TIMEOUT:
                print("[エラー] タイムアウト: バッチジョブが完了しませんでした。")
                sys.exit(1)
            jobs_str = ", ".join(str(pending[j]["batch_index"]) for j in pending)
            print(f"  [{elapsed // 60}分経過] 待機中（バッチ {jobs_str}）... {POLL_INTERVAL}秒後に再確認")
            time.sleep(POLL_INTERVAL)

    # 失敗チェック
    failed_jobs = [jn for jn, bj in finished.items() if not is_succeeded(bj.state)]
    if failed_jobs:
        for jn in failed_jobs:
            bj = finished[jn]
            print(f"[エラー] バッチ失敗: {jn} / 状態: {bj.state}")
            if hasattr(bj, "error") and bj.error:
                print(f"  エラー詳細: {bj.error}")
        if len(failed_jobs) == n_batches:
            sys.exit(1)
        print(f"  {len(failed_jobs)} バッチが失敗しましたが、成功分の結果を処理します。")

    print("\n=== Phase 5: 結果を取得してMarkdownを生成 ===")

    # 全バッチの結果を収集
    results: dict[str, str] = {}
    errors: dict[str, str] = {}
    file_based_batches: list = []

    for job_name, batch_job in finished.items():
        if not is_succeeded(batch_job.state):
            continue

        batch_info = next((bi for bi in batch_infos if bi["job_name"] == job_name), {})
        batch_idx = batch_info.get("batch_index", "?")

        dest = getattr(batch_job, "dest", None)
        if not dest:
            print(f"  [バッチ {batch_idx}] dest が存在しません。スキップ。")
            continue

        inlined = getattr(dest, "inlined_responses", None) or []
        file_name = getattr(dest, "file_name", None)

        if inlined:
            print(f"  [バッチ {batch_idx}] インラインレスポンス: {len(inlined)} 件")
            for item in inlined:
                key = getattr(item, "key", "") or ""
                err = getattr(item, "error", None)
                if err:
                    errors[key] = str(err)
                    continue
                resp = getattr(item, "response", None)
                if resp is None:
                    errors[key] = "レスポンスなし"
                    continue
                try:
                    results[key] = resp.text
                except Exception:
                    try:
                        results[key] = resp.candidates[0].content.parts[0].text
                    except Exception as e:
                        errors[key] = f"レスポンス解析エラー: {e}"
        elif file_name:
            print(f"  [バッチ {batch_idx}] ⚠️  結果がファイルに格納されています: {file_name}")
            print(f"  [バッチ {batch_idx}]    Gemini APIの制限でこのファイルはダウンロード不可です。")
            file_based_batches.append(batch_idx)
        else:
            print(f"  [バッチ {batch_idx}] inlined_responses も file_name も存在しません。")

    if file_based_batches:
        print(f"\n⚠️  バッチ {file_based_batches} の結果はダウンロードできませんでした。")
        print(f"   原因: リクエスト数が多すぎて結果がGemini Filesに格納されましたが、")
        print(f"         バッチ出力ファイルはAPIの制限でダウンロード不可です。")
        print(f"   対策: --batch-size をより小さい値（例: --batch-size 10）で再送信してください。")
        if not results:
            print("\n取得できた結果がありません。再送信が必要です:")
            print("   python3 src/extract_iso30414_batch.py --submit --batch-size 10")
            sys.exit(1)

    print(f"  合計取得レスポンス: {len(results) + len(errors)}件 (成功: {len(results)}, エラー: {len(errors)})")

    if errors:
        print(f"  エラーのあったリクエスト ({len(errors)}件):")
        for k, v in list(errors.items())[:10]:
            print(f"    {k}: {v}")

    # PDFごとにMarkdownを生成
    saved = 0
    skipped = 0
    for entry in pdfs:
        if entry.get("skipped"):
            skipped += 1
            continue

        company_folder = entry["company_folder"]
        pdf_name = entry["pdf_name"]
        out_path = entry["out_path"]
        stem = os.path.splitext(pdf_name)[0]

        # 分類結果を取得
        classify_key = make_request_key(company_folder, stem, "classify")
        classify_text = results.get(classify_key, "")

        fiscal_year = "不明"
        document_type = "other"
        company_name = company_folder

        if classify_text:
            try:
                text = classify_text.strip()
                if "```" in text:
                    text = re.sub(r"```[a-z]*", "", text).replace("```", "").strip()
                m = re.search(r"\{.*?\}", text, re.DOTALL)
                if m:
                    info = json.loads(m.group(0))
                    fiscal_year = info.get("fiscal_year", "不明")
                    document_type = info.get("document_type", "other")
                    company_name = info.get("company_name_in_doc", company_folder)
            except Exception as e:
                print(f"  [{company_folder}/{pdf_name}] 分類解析エラー: {e}")

        # 各カテゴリの抽出結果を取得
        extract_texts: dict[str, str] = {}
        for cat_name, _ in CATEGORIES:
            cat_suffix = f"cat_{safe_key_part(cat_name)}"
            cat_key = make_request_key(company_folder, stem, cat_suffix)
            if cat_key in results:
                extract_texts[cat_suffix] = results[cat_key]
            elif cat_key in errors:
                extract_texts[cat_suffix] = f"（抽出エラー: {errors[cat_key]}）"
            else:
                extract_texts[cat_suffix] = "（結果なし）"

        # データがない場合はスキップ
        has_data = classify_text or any(v != "（結果なし）" for v in extract_texts.values())
        if not has_data:
            continue

        # Markdown を生成・保存
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        md = build_markdown(company_name, fiscal_year, document_type, pdf_name, extract_texts)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)

        rel = os.path.relpath(out_path, "/workspaces/lancers_research")
        print(f"  保存: {rel}")
        saved += 1

    print(f"\n完了: 保存={saved}件 / スキップ={skipped}件")
    print(f"出力先: {output_root}")

    # アップロードしたPDFファイルをFiles APIから削除
    print("\n=== クリーンアップ: Files APIからアップロード済みファイルを削除 ===")
    deleted = 0
    for entry in pdfs:
        if entry.get("skipped") or "file_name" not in entry:
            continue
        try:
            client.files.delete(name=entry["file_name"])
            deleted += 1
        except Exception as e:
            print(f"  削除失敗 {entry['file_name']}: {e}")

    # 各バッチのJSONLファイルも削除
    for info in batch_infos:
        jsonl_file_name = info.get("jsonl_file_name")
        if jsonl_file_name:
            try:
                client.files.delete(name=jsonl_file_name)
                deleted += 1
            except Exception:
                pass

    print(f"  削除完了: {deleted} ファイル")


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ISO30414(2018)指標をGemini APIで抽出するスクリプト"
    )
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--run", action="store_true",
                            help="通常APIの並列処理で全工程を実行（推奨）")
    mode_group.add_argument("--submit", action="store_true",
                            help="[バッチAPI] ジョブを送信してステートファイルを保存し終了する")
    mode_group.add_argument("--poll", action="store_true",
                            help="[バッチAPI] 既存ジョブの完了を待ち結果を処理する（--state-file 必須）")

    parser.add_argument("--state-file", metavar="PATH",
                        help="--poll 時に使用するステートファイルのパス")
    parser.add_argument("--company", metavar="NAME",
                        help="会社フォルダ名で絞り込み（部分一致）")
    parser.add_argument("--start-from", metavar="NAME",
                        help="指定した会社フォルダ名（部分一致）以降のみ処理する")
    parser.add_argument("--pdf", metavar="PATH_OR_NAME",
                        help="処理するPDFを絶対パスまたはファイル名（部分一致）で指定")
    parser.add_argument("--limit", type=int, metavar="N",
                        help="処理するPDF数の上限（テスト用: 例 --limit 1）")
    parser.add_argument("--workers", type=int, default=3, metavar="N",
                        help="--run 時の並列ワーカー数（デフォルト: 3）")
    parser.add_argument("--batch-size", type=int, default=20, metavar="N",
                        help="バッチあたりの最大PDF数（デフォルト: 20）。"
                             "大きすぎると結果がファイル格納になりダウンロード不可になります。")
    parser.add_argument("--overwrite", action="store_true",
                        help="既存のMarkdownを上書きする")
    parser.add_argument("--output", default=MD_OUTPUT_ROOT, metavar="DIR",
                        help=f"出力先フォルダ（デフォルト: {MD_OUTPUT_ROOT}）")
    parser.add_argument("--model", default=DEFAULT_MODEL, metavar="NAME",
                        help=f"使用するGeminiモデル（デフォルト: {DEFAULT_MODEL}）")
    args = parser.parse_args()

    if args.poll and not args.state_file:
        parser.error("--poll には --state-file が必要です")

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("[エラー] 環境変数 GEMINI_API_KEY が設定されていません")
        sys.exit(1)

    client = genai.Client(api_key=api_key)
    output_root = os.path.abspath(args.output)

    if args.poll:
        poll_and_process(client, args.state_file)
        return

    # --run / --submit: PDFを収集
    pdfs = collect_pdfs(
        PDF_ROOT,
        company_filter=args.company,
        start_from=getattr(args, "start_from", None),
        pdf_path=getattr(args, "pdf", None),
        limit=getattr(args, "limit", None),
    )
    if not pdfs:
        print("処理対象のPDFが見つかりませんでした。")
        return

    print(f"対象: {len(pdfs)} ファイル | モデル: {args.model}")

    if args.run:
        run_concurrent(
            client, args.model, pdfs, args.overwrite, output_root,
            workers=args.workers,
        )
        return

    # --submit: バッチAPI送信
    submit(client, args.model, pdfs, args.overwrite, output_root, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
