"""Markdownファイルから指標を抽出し、HCPro入力フォーマット（年度列ピボット）で出力するスクリプト。

出力列:
  企業ID, 企業名, 指標ID, 指標名, 単位, データソース, 備考,
  2020年度, 2021年度, 2022年度, 2023年度, 2024年度, 2025年度

ルール:
  - 年度セルには半角数値のみ（%, 名, USD 等の単位はセルに入れない）
  - 単位は「単位」列に記載
  - 「離職率」「育児休業取得率」は開示値のみ採用（計算値は除外）
  - 複数書類で値が一致しない場合、備考列に「要確認」を記載
  - 計算式から導出した値は備考列に「計算値」を記載

使い方:
  python3 aggregate_metrics.py                          # 全データを処理
  python3 aggregate_metrics.py --company "General Mills" # 1社のみ処理
  python3 aggregate_metrics.py --output my_output.csv   # 出力ファイル指定
  python3 aggregate_metrics.py --skip-empty             # 年度データが全て空の行を除外
"""

import argparse
import csv
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

MD_OUTPUT_ROOT = "/workspaces/lancers_research/md_outputs"
DEFAULT_OUTPUT = "/workspaces/lancers_research/aggregated_metrics.csv"

# 記載なしとみなす値
EMPTY_VALUES = {"記載なし", "不明", "", "null", "None"}

# 出力対象年度
FISCAL_YEARS = ["2020", "2021", "2022", "2023", "2024", "2025"]

# 指標定義順（extract_to_markdown.py の METRICS_GOV / FIN / HR と同一）
METRIC_ORDER = [
    # ガバナンス
    "社外取締役数", "取締役数（男性）", "取締役数（女性）",
    "取締役比率（男性）", "取締役比率（女性）",
    "取締役数（外国籍）", "社外取締役比率", "取締役外国籍比率",
    "取締役（社外取締役を除く）の報酬等の総額", "社外取締役の報酬等の総額",
    "取締役報酬平均", "社長・CEO報酬",
    # 財務
    "売上高", "営業利益", "経常利益", "当期純利益",
    "従業員一人当たり経常利益", "従業員一人当たり純利益",
    "株価収益率", "総資産額", "純資産額", "資本金",
    # 人的資本・ESG
    "総従業員数", "男性従業員数", "女性従業員数",
    "男性従業員比率", "女性従業員比率",
    "障がい者雇用人数", "平均年齢", "採用者数", "離職者数",
    "育児休業取得率（男性）", "育児休業取得率（女性）",
    "研修を受講した従業員数", "リーダーシップ開発・研修の対象者数",
    "リーダーシップ開発・研修の参加者数",
    "管理職研修を受けたリーダーの割合", "倫理・コンプライアンス研修を受けた従業員数",
    "人材開発・研修の総費用", "研修への参加率", "総研修実施時間",
    "従業員1人当たりの平均研修時間", "受講者当りの研修受講時間",
    "労災の件数", "労災による死亡者数", "従業員満足度調査結果",
    "男女間賃金格差（正社員）", "男女間賃金格差（非正規）",
    "エンゲージメント", "平均勤続年数", "eNPS",
    "管理職1人当りの部下数", "管理職研修を受けた割合",
    "リーダーシップ開発・研修の参加率",
    "労働災害件数", "労災発生率", "離職率", "自発的離職率",
]

# 指標ID マッピング（M001 …）
METRIC_ID = {name: f"M{i + 1:03d}" for i, name in enumerate(METRIC_ORDER)}

# 指標フィールドID（英語フィールド名）マッピング: 日本語指標名 → English field name
METRIC_FIELD_ID: dict[str, str] = {
    "社外取締役数":                                     "number_of_external_directors_independent_employer",
    "取締役数（男性）":                                 "number_of_directors_male",
    "取締役数（女性）":                                 "number_of_directors_female",
    "取締役比率（男性）":                               "director_ratio_male",
    "取締役比率（女性）":                               "director_ratio_female",
    "取締役数（外国籍）":                               "number_of_directors_foreign_national",
    "社外取締役比率":                                   "external_director_ratio",
    "取締役外国籍比率":                                 "directors_foreign_national_ratio",
    "取締役（社外取締役を除く）の報酬等の総額":          "total_director_s_remuneration",
    "社外取締役の報酬等の総額":                         "total_amount_of_remuneration_etc_for_outside_directors",
    "取締役報酬平均":                                   "directors_average",
    "社長・CEO報酬":                                    "president_ceo_reward",
    "総従業員数":                                       "total_number_of_employees",
    "男性従業員数":                                     "number_of_male_employees",
    "女性従業員数":                                     "number_of_female_employees",
    "男性従業員比率":                                   "male_employee_ratio",
    "女性従業員比率":                                   "female_employee_ratio",
    "障がい者雇用人数":                                 "number_of_people_with_disabilities_employed",
    "平均年齢":                                         "average_age",
    "採用者数":                                         "number_of_hires_new_graduates_mid_career",
    "離職者数":                                         "number_of_people_left",
    "育児休業取得率（男性）":                           "childcare_leave_acquisition_employee_rate_male",
    "育児休業取得率（女性）":                           "childcare_leave_acquisition_employee_rate_female",
    "研修を受講した従業員数":                           "number_of_employees_trained",
    "リーダーシップ開発・研修の対象者数":               "number_of_subjects_subject_to_leader_ship_development_and_training",
    "リーダーシップ開発・研修の参加者数":               "number_of_participants_in_leader_ship_development_and_training",
    "管理職研修を受けたリーダーの割合":                 "percent_of_organizations_leaders_who_have_participated_in_formal_training_in_a_given_time_period",
    "倫理・コンプライアンス研修を受けた従業員数":       "ethics_compliance_training_number_of_employees_received",
    "人材開発・研修の総費用":                           "total_expenses_for_human_resource_development_and_training",
    "研修への参加率":                                   "training_participation_rate",
    "総研修実施時間":                                   "total_training_time",
    "従業員1人当たりの平均研修時間":                    "training_time_for_employees",
    "受講者当りの研修受講時間":                         "training_time_for_students",
    "労災の件数":                                       "number_of_work_related_accidents",
    "労災による死亡者数":                               "number_of_deaths_due_to_work_related_accidents",
    "従業員満足度調査結果":                             "employee_satisfaction_survey_score",
    "男女間賃金格差（正社員）":                         "gender_wage_gap_regular_employee",
    "男女間賃金格差（正社員)":                          "gender_wage_gap_regular_employee",
    "男女間賃金格差（非正規）":                         "gender_wage_gap_non_regular",
    "男女間賃金格差（非正規)":                          "gender_wage_gap_non_regular",
    "売上高":                                           "earnings",
    "営業利益":                                         "operating_income",
    "経常利益":                                         "ordinary_income",
    "当期純利益":                                       "net_income",
    "従業員一人当たり経常利益":                         "ordinary_income_per_employee",
    "従業員一人当たり純利益":                           "net_income_per_employee",
    "株価収益率":                                       "price_earnings_ratio",
    "総資産額":                                         "total_assets",
    "純資産額":                                         "net_assets",
    "資本金":                                           "capital",
    "エンゲージメント":                                 "engagement_score",
    "平均勤続年数":                                     "average_length_of_service",
    "eNPS":                                             "e_nps",
    "管理職1人当りの部下数":                            "number_of_subordinates_per_manager",
    "管理職研修を受けた割合":                           "percentage_of_employees_who_have_received_management_training",
    "リーダーシップ開発・研修の参加率":                 "participation_rate_of_leader_ship_development_and_training",
    "労働災害件数":                                     "number_of_occupational_accidents_occurred",
    "労災発生率":                                       "rate_of_work_related_accidents",
    "離職率":                                           "turnover",
    "自発的離職率":                                     "spontaneous_turnover_rate",
}

# 企業ID マッピング: 英語社名（正規化済み） → 企業ID
# ※ 正規化: 小文字化、ピリオド/カンマ/アポストロフィ除去、ハイフン→スペース、& →スペース
_COMPANY_ID_TABLE: list[tuple[int, str]] = [
    (151, "EOG Resources Inc."),
    (151, "EOG"),                                  # フォルダ名のエイリアス
    (152, "Southwest Airlines Co."),
    (153, "Altria Group Inc."),
    (154, "Union Pacific Corporation"),
    (155, "McDonald's Company (Japan) Ltd."),
    (156, "U.S. Bancorp"),
    (157, "Macy's Inc."),
    (158, "Light Aid Co. Ltd."),
    (159, "Sunoco LP"),
    (160, "Mastercard Incorporated"),
    (161, "The Hartford Financial Services Group Inc."),
    (162, "Baker Hughes Company"),
    (163, "The Sherwin-Williams Company"),
    (164, "Marriott International"),
    (165, "Genuine Parts Company"),
    (166, "Rea Corporation"),
    (167, "Halliburton Company"),
    (168, "C.U.D. Ocation Co. Ltd."),
    (169, "Truist Financial Corporation"),
    (170, "WESKO INTERNATIONAL CO. LTD."),
    (171, "PG&E Corporation"),
    (172, "Murphy USA Inc."),
    (173, "PNC Financial Services Group Inc."),
    (174, "Cleveland-Cliffs Inc."),
    (175, "Freeport-McMoRan Inc."),
    (176, "Advanced Micro Devices Inc."),
    (177, "Career Global Inc."),
    (178, "Marsh & McLennan Companies Inc."),
    (179, "The Charles Schwab Corporation"),
    (180, "WestRock Company"),
    (181, "Jones Lang LaSalle Incorporated"),
    (182, "Goodyear Tire & Rubber Co."),
    (183, "Steel Dynamics Inc."),
    (184, "Kimberly-Clark Corporation"),
    (185, "Pioneer Natural Resources Company"),
    (186, "International Paper Company"),
    (187, "C.H. Robinson Worldwide Inc."),
    (188, "General Mills Inc."),
    (189, "Waste Management Co. Ltd."),
    (190, "Tenet Healthcare Corporation"),
    (191, "Exelon Corporation"),
    (192, "Block Inc."),
    (193, "American Electric Power Company Inc."),
    (194, "Stryker Corporation"),
    (195, "BJ's Wholesale Club Holdings Inc."),
    (196, "Cognizant Technology Solutions Corporation"),
    (197, "Booking Holdings Inc."),
    (198, "ManpowerGroup Japan Co. Ltd."),
    (199, "Ross Stores Inc."),
    (200, "Live Nation Entertainment Inc."),
]


def _norm_company_name(name: str) -> str:
    """企業名をマッチング用に正規化する。"""
    n = name.strip().lower()
    n = re.sub("[.,'\u2019]", "", n)  # ピリオド・カンマ・アポストロフィ除去
    n = re.sub(r"\s*&\s*", " ", n)      # & → スペース
    n = re.sub(r"-", " ", n)            # ハイフン → スペース
    n = re.sub(r"\s+", " ", n).strip()
    return n


_COMPANY_ID_LOOKUP: dict[str, int] = {
    _norm_company_name(english): cid
    for cid, english in _COMPANY_ID_TABLE
}


def _lookup_company_id(company: str) -> str:
    """企業名から企業IDを取得する。見つからない場合は空文字を返す。"""
    key = _norm_company_name(company)
    cid = _COMPANY_ID_LOOKUP.get(key)
    if cid is not None:
        return str(cid)
    # 部分一致フォールバック: 登録名がcompanyを含む or companyが登録名を含む
    for norm_key, cid2 in _COMPANY_ID_LOOKUP.items():
        if norm_key and key and (norm_key.startswith(key) or key.startswith(norm_key)):
            return str(cid2)
    return ""

# 開示値のみ採用（手計算・計算式からの導出値を除外）する指標
NO_CALCULATE_METRICS = {
    "離職率",
    "自発的離職率",
    "育児休業取得率（男性）",
    "育児休業取得率（女性）",
}

# 生USD（スケールなし, USD単体）の場合に百万USD換算する指標
# ※ 千USD/百万USD/十億USD は指標名によらず常に換算するため不要
# ※ 「従業員一人当たり」系は1人あたりの小さな値なので除外
METRICS_IN_MILLION_USD = {
    "売上高", "営業利益", "経常利益", "当期純利益",
    "総資産額", "純資産額", "資本金",
    "取締役（社外取締役を除く）の報酬等の総額",
    "社外取締役の報酬等の総額",
    "取締役報酬平均", "社長・CEO報酬",
    "人材開発・研修の総費用",
}


# ---------------------------------------------------------------------------
# データ構造
# ---------------------------------------------------------------------------

@dataclass
class MetricEntry:
    company: str
    fiscal_year: str
    document_type: str
    source_file: str
    metric_name: str
    value: str
    page: str
    formula: str
    evidence_summary: str   # 根拠の先頭120文字
    reading_note: str = ""  # 読み取り方フィールドの全文


# ---------------------------------------------------------------------------
# 数値・単位の分離
# ---------------------------------------------------------------------------

def extract_numeric_and_unit(value_str: str) -> tuple[str, str]:
    """値文字列から (半角数値文字列, 単位文字列) を返す。

    HCProルール: 年度セルには数値のみ入力。単位は別列に記載。
    抽出不能な場合は ("", "") を返す。
    """
    if not value_str or value_str.strip() in EMPTY_VALUES:
        return "", ""
    v = value_str.strip()

    # グラフあり・詳細不明
    if re.search(r'グラフ|詳細数値不明', v):
        return "", ""

    # USD X thousand / million / billion
    m = re.match(r'USD\s*([\d,]+(?:\.\d+)?)\s*(thousand|million|billion)?', v, re.IGNORECASE)
    if m:
        num = m.group(1).replace(",", "")
        scale_map = {"thousand": "千USD", "million": "百万USD", "billion": "十億USD"}
        unit = scale_map.get((m.group(2) or "").lower(), "USD")
        return num, unit

    # X%
    m = re.match(r'^([\d,]+(?:\.\d+)?)\s*%', v)
    if m:
        return m.group(1).replace(",", ""), "%"

    # X倍
    m = re.match(r'^([\d,]+(?:\.\d+)?)\s*倍', v)
    if m:
        return m.group(1).replace(",", ""), "倍"

    # X名 / 人 / 件 / 時間 / 円 / 歳 等（日本語単位）
    m = re.match(r'^([\d,]+(?:\.\d+)?)\s*([名人件時間円歳ヶ月年]+)', v)
    if m:
        return m.group(1).replace(",", ""), m.group(2)

    # 純数値（カンマ区切りも許容）
    m = re.match(r'^[\d,]+(?:\.\d+)?$', v)
    if m:
        return v.replace(",", ""), ""

    # 「少なくともN名」/ "at least N" 形式（修飾語付き数値）
    m = re.match(r'^(?:少なくとも|最低|at\s+least)\s*([\d,]+(?:\.\d+)?)\s*([名人件時間円歳ヶ月年]*)', v, re.IGNORECASE)
    if m:
        return m.group(1).replace(",", ""), m.group(2) or ""

    return "", ""


def normalize_to_million_usd(num_str: str, current_unit: str, metric_name: str) -> tuple[str, str]:
    """
    USD系単位を百万USD に統一換算する。

    変換ルール:
      千USD / 百万USD / 十億USD → 指標名によらず常に百万USD へ換算
        千USD   → ÷ 1,000
        百万USD → そのまま
        十億USD → × 1,000
      USD（スケールなし）→ 指標名が METRICS_IN_MILLION_USD に含まれる場合のみ ÷ 1,000,000
        ※ 「従業員一人当たり」など per-person 値は除外されているため誤換算しない
      それ以外の単位（%, 名, 人 など）はそのまま返す。

    Returns: (換算後の数値文字列, 単位文字列)
    """
    if not num_str:
        return num_str, current_unit

    norm_unit = current_unit.lower().replace(" ", "")

    # スケール付き USD は単位列だけで一意に判断できる → 常に換算
    scaled_map = {
        "千usd":  1 / 1_000,
        "百万usd": 1,
        "十億usd": 1_000,
    }
    factor = scaled_map.get(norm_unit)
    if factor is not None:
        try:
            val = float(num_str)
        except ValueError:
            return num_str, current_unit
        converted = val * factor
        if converted == int(converted):
            result = str(int(converted))
        else:
            result = f"{converted:.4f}".rstrip("0").rstrip(".")
        return result, "百万USD"

    # 生 USD（スケールなし）: 指標名が金額総額系の場合のみ換算
    # （1人あたり値は METRICS_IN_MILLION_USD から除外済みなので誤換算しない）
    if norm_unit == "usd" and metric_name in METRICS_IN_MILLION_USD:
        try:
            val = float(num_str)
        except ValueError:
            return num_str, current_unit
        converted = val / 1_000_000
        if converted == int(converted):
            result = str(int(converted))
        else:
            result = f"{converted:.4f}".rstrip("0").rstrip(".")
        return result, "百万USD"

    # USD系でない、またはスケールなしUSDで換算対象外 → そのまま返す
    return num_str, current_unit


# ---------------------------------------------------------------------------
# パーサー
# ---------------------------------------------------------------------------

def _extract_table_value(content: str, key: str) -> Optional[str]:
    """Markdownのメタ表 | **key** | value | から値を取得する。"""
    pattern = rf'\|\s*\*\*{re.escape(key)}\*\*\s*\|\s*(.+?)\s*\|'
    m = re.search(pattern, content)
    return m.group(1).strip() if m else None


def _extract_field(block: str, field_name: str) -> Optional[str]:
    """指標ブロック内の - **field_name**: value を抽出する。"""
    pattern = rf'-\s*\*\*{re.escape(field_name)}\*\*:\s*(.+?)(?:\n|$)'
    m = re.search(pattern, block)
    return m.group(1).strip() if m else None


def _extract_evidence(block: str) -> str:
    """根拠ブロック（> 引用）の先頭を取得する。"""
    m = re.search(r'-\s*\*\*根拠\*\*:\s*\n((?:\s*>.+\n?)+)', block)
    if m:
        text = re.sub(r'\s*>\s*', ' ', m.group(1)).strip()
        return text[:120]
    return ""


def _normalize_doctype(raw: str) -> str:
    if "annual report" in raw.lower() or "10-k" in raw.lower():
        return "annual_report"
    if "proxy" in raw.lower():
        return "proxy_statement"
    if "sustainability" in raw.lower() or "esg" in raw.lower() or "responsibility" in raw.lower():
        return "sustainability_report"
    return raw.lower().replace(" ", "_")


def parse_markdown_file(filepath: str) -> list[MetricEntry]:
    """
    Markdownファイルを行ストリームで読み込み MetricEntry のリストを返す。

    全体を一括 read() するのではなく、行単位で ### セクションの開始を検出して
    ブロックを逐次組み立てる。ピークメモリを削減し大きなファイルでも効率的。
    """
    header_lines: list[str] = []
    blocks: list[str] = []
    current_block: list[str] = []
    in_header = True  # 最初の ### より前はヘッダー部

    with open(filepath, encoding="utf-8") as f:
        for line in f:
            if line.startswith("### "):
                if in_header:
                    in_header = False
                elif current_block:
                    blocks.append("".join(current_block))
                current_block = [line]
            elif in_header:
                header_lines.append(line)
            else:
                current_block.append(line)
    if current_block:
        blocks.append("".join(current_block))

    header_text = "".join(header_lines)
    # フォルダ名を正規の企業名として使う（Gemini が返す company_name_in_doc は表記ゆれが多いため）
    company = os.path.basename(os.path.dirname(filepath)).replace("_", " ")
    fiscal_year = _extract_table_value(header_text, "会計年度") or "不明"
    doc_type_raw = _extract_table_value(header_text, "書類種別") or ""
    document_type = _normalize_doctype(doc_type_raw)
    source_file = os.path.basename(filepath)

    seen_metrics: dict[str, MetricEntry] = {}

    for block in blocks:
        header_line = block.split("\n", 1)[0]
        metric_name = header_line[4:].strip()  # "### " の4文字を除去
        if not metric_name:
            continue

        value = _extract_field(block, "値") or "記載なし"
        page = _extract_field(block, "記載ページ") or "記載なし"
        formula = _extract_field(block, "計算式") or "なし"
        evidence = _extract_evidence(block)
        reading_note = _extract_field(block, "読み取り方") or ""

        entry = MetricEntry(
            company=company,
            fiscal_year=fiscal_year,
            document_type=document_type,
            source_file=source_file,
            metric_name=metric_name,
            value=value,
            page=page,
            formula=formula,
            evidence_summary=evidence,
            reading_note=reading_note,
        )

        # ファイル内重複：値が記載なし → 有意な値に上書き
        if metric_name in seen_metrics:
            if seen_metrics[metric_name].value in EMPTY_VALUES and value not in EMPTY_VALUES:
                seen_metrics[metric_name] = entry
        else:
            seen_metrics[metric_name] = entry

    return list(seen_metrics.values())


# ---------------------------------------------------------------------------
# 信頼性チェック
# ---------------------------------------------------------------------------

def _normalize_value_for_compare(value: str) -> str:
    """比較用に値を正規化する（通貨記号・スペース・大小文字・日本語単位を統一）。"""
    v = value.strip().lower()
    # 通貨・単位の表記揺れを吸収
    v = re.sub(r'usd\s*', '', v)
    v = re.sub(r'\$\s*', '', v)
    v = re.sub(r',', '', v)
    v = re.sub(r'\s+', ' ', v)
    v = re.sub(r'\.0+([^0-9]|$)', r'\1', v)  # 不要な小数点以下ゼロを削除
    # 括弧内の補足説明を除去（例: "0.03 (200,000労働時間あたり)" → "0.03"）
    # ただし値自体が括弧表記の場合（負数 "(20)" など）は除去しない
    v = re.sub(r'(?<=\S)\s+[\(\（][^\)）]*[\)\）]\s*$', '', v)
    # 日本語単位サフィックスを除去（例: "6名"→"6", "10件"→"10", "5.9時間"→"5.9"）
    v = re.sub(r'\s*(名|人|件|社|時間|%|％)\s*$', '', v)
    # 「以上」サフィックスを除去（例: "2以上"→"2"）
    v = re.sub(r'\s*以上\s*$', '', v)
    # 数量修飾語プレフィックスを除去（例: "少なくとも2"→"2", "at least 3"→"3"）
    v = re.sub(r'^(?:少なくとも|最低|at\s+least)\s*', '', v)
    # USD スケール正規化: thousand/million/billion を百万USD換算で統一比較
    # 例: "18918435 thousand" と "18918.435 million" → どちらも "18918.435" に
    scale_m = re.match(r'^(-?[\d.]+)\s+(thousand|million|billion)$', v)
    if scale_m:
        try:
            num = float(scale_m.group(1))
            scale = scale_m.group(2)
            if scale == "thousand":
                num /= 1000.0
            elif scale == "billion":
                num *= 1000.0
            v = f"{num:.6f}".rstrip("0").rstrip(".")
        except ValueError:
            pass
    return v.strip()


def check_reliability(entries: list[MetricEntry]) -> tuple[str, str]:
    """
    同一会社・年度・指標の複数エントリを比較して信頼性を判定する。

    返り値: (reliability_flag, note)
      - "OK"            : 単一ソースの直接引用
      - "OK（複数確認済）" : 複数ソースで値が一致
      - "計算値"         : 計算式から導出（直接引用なし）
      - "要確認"         : 複数ソースで値が異なる
      - "記載なし"       : 有意な値なし
    """
    meaningful = [e for e in entries if e.value not in EMPTY_VALUES]

    if not meaningful:
        return "記載なし", ""

    if len(meaningful) == 1:
        e = meaningful[0]
        has_formula = e.formula not in ("なし", "", None)
        if has_formula:
            return "計算値", f"計算式: {e.formula}"
        return "OK", ""

    # 複数ソース間の値比較
    norm_values = [_normalize_value_for_compare(e.value) for e in meaningful]
    if len(set(norm_values)) == 1:
        return "OK（複数確認済）", f"{len(meaningful)}件一致"

    # 値が不一致: 優先ドキュメントタイプ内で一致しているか確認
    # annual_report > sustainability_report > proxy_statement の順で優先
    _DOC_PRIO = {"annual_report": 0, "sustainability_report": 1, "proxy_statement": 2}
    by_type: dict = defaultdict(list)
    for e in meaningful:
        by_type[e.document_type].append(e)
    best_type = min(by_type.keys(), key=lambda t: _DOC_PRIO.get(t, 9))
    best_norms = [_normalize_value_for_compare(e.value) for e in by_type[best_type]]

    if len(set(best_norms)) == 1:
        # 優先ソース内では値が一致 → 優先採用し他ソース差異を補足に記録
        other_entries = [e for e in meaningful if e.document_type != best_type]
        other_diffs = " / ".join(
            f"[{e.document_type}|{e.source_file}] {e.value}"
            for e in other_entries
        )
        adopted_value = by_type[best_type][0].value
        return (
            "OK（優先ソース採用）",
            f"{best_type}の値を採用（他ソース差異: {other_diffs}）",
        )
    else:
        # 優先ソース内でも不一致 → 要確認
        diffs = " / ".join(
            f"[{e.document_type}|{e.source_file}] {e.value}"
            for e in meaningful
        )
        return "要確認", f"値が一致しない → {diffs}"


# ---------------------------------------------------------------------------
# ファイル収集
# ---------------------------------------------------------------------------

def collect_md_files(md_root: str, company_filter: Optional[str]) -> list[str]:
    """md_outputs 以下の .md ファイルを列挙する（再帰あり）。"""
    result = []
    for folder_name in sorted(os.listdir(md_root)):
        folder_path = os.path.join(md_root, folder_name)
        if not os.path.isdir(folder_path) or folder_name.startswith("."):
            continue
        if company_filter:
            # スペース・アンダースコアを同一視して部分一致
            norm_filter = company_filter.lower().replace(" ", "_").replace("-", "_")
            norm_folder = folder_name.lower().replace(" ", "_").replace("-", "_")
            if norm_filter not in norm_folder:
                continue
        for root, dirs, files in os.walk(folder_path):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fname in sorted(files):
                if fname.endswith(".md"):
                    result.append(os.path.join(root, fname))
    return result


# ---------------------------------------------------------------------------
# 集約・HCPro形式出力
# ---------------------------------------------------------------------------

def aggregate_to_hcpro(
    md_root: str,
    company_filter: Optional[str],
    skip_empty: bool = False,
    ai_model=None,
) -> tuple[list[dict], list[str]]:
    """
    全Markdownを処理し、1行＝企業×指標（年度は列）の行リストを返す。

    出力列:
      企業名, 指標名, 単位, データソース, 備考, 2020年度, 2021年度, …, 2025年度
    """
    md_files = collect_md_files(md_root, company_filter)
    if not md_files:
        return [], []

    # (company, fiscal_year, metric_name) → [MetricEntry]
    all_entries: dict[tuple, list[MetricEntry]] = defaultdict(list)

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os as _os
    worker_count = min(8, len(md_files))  # CPUバウンドでなくI/Oバウンドなのでスレッドで十分

    def _safe_parse(fp: str):
        try:
            return parse_markdown_file(fp)
        except Exception as e:
            print(f"  [解析エラー] {fp}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_path = {executor.submit(_safe_parse, fp): fp for fp in md_files}
        done = 0
        for future in as_completed(future_to_path):
            done += 1
            entries = future.result()
            for e in entries:
                all_entries[(e.company, e.fiscal_year, e.metric_name)].append(e)
            if done % 20 == 0 or done == len(md_files):
                print(f"  [{done}/{len(md_files)}] ファイル読み込み完了")

    # --- データ収集 ---
    # (company, fiscal_year, metric_name) → 各種キャッシュ（年度単位で保持）
    value_cache: dict[tuple, str] = {}          # cym_key → 採用値（★付きも含む）
    unit_cache: dict[tuple, str] = {}           # cym_key → 単位
    reliability_cache: dict[tuple, str] = {}    # cym_key → 信頼性
    note_cache: dict[tuple, str] = {}           # cym_key → 信頼性ノート
    source_cache: dict[tuple, set] = defaultdict(set)  # cym_key → {(fname, page), ...}
    ai_context_cache: dict[tuple, tuple] = {}   # cym_key → (metric_name, context_text)
    static_note_cache: dict[tuple, str] = {}    # cym_key → 静的補足ノート（AIなし）
    proxy_warn_cache: dict[tuple, str] = {}     # cym_key → Proxy値の疑い警告
    all_cym_keys: set[tuple] = set()            # 存在する (company, year, metric) の全集合

    for (company, fiscal_year, metric_name), entries in all_entries.items():
        cym_key = (company, fiscal_year, metric_name)
        all_cym_keys.add(cym_key)

        # NO_CALCULATE_METRICS は開示値のみ採用
        if metric_name in NO_CALCULATE_METRICS:
            candidates = [
                e for e in entries
                if e.value not in EMPTY_VALUES and e.formula in ("なし", "", None)
            ]
        else:
            candidates = [e for e in entries if e.value not in EMPTY_VALUES]

        reliability, note = check_reliability(entries)

        # 代表値の選択優先順位（指標カテゴリ別）:
        #   財務指標   : annual_report > sustainability_report > proxy_statement
        #   ガバナンス : proxy_statement > annual_report > sustainability_report
        #   HR・ESG   : sustainability_report > annual_report > proxy_statement
        _GOV_METRICS = {
            "社外取締役数", "取締役数（男性）", "取締役数（女性）",
            "取締役比率（男性）", "取締役比率（女性）", "取締役数（外国籍）",
            "社外取締役比率", "取締役外国籍比率",
            "取締役（社外取締役を除く）の報酬等の総額", "社外取締役の報酬等の総額",
            "取締役報酬平均", "社長・CEO報酬",
        }
        _FIN_METRICS = {
            "売上高", "営業利益", "経常利益", "当期純利益",
            "従業員一人当たり経常利益", "従業員一人当たり純利益",
            "株価収益率", "総資産額", "純資産額", "資本金",
        }

        # 全指標でアニュアルレポートを最優先
        DOC_PRIORITY = {"annual_report": 0, "sustainability_report": 1, "proxy_statement": 2}

        def _entry_sort_key(e):
            doc_prio = DOC_PRIORITY.get(e.document_type, 9)
            formula_prio = 0 if e.formula in ("なし", "", None) else 1
            return (doc_prio, formula_prio)

        sorted_candidates = sorted(
            [e for e in candidates],
            key=_entry_sort_key,
        )
        direct = [e for e in sorted_candidates if e.formula in ("なし", "", None)]
        primary = direct[0] if direct else (sorted_candidates[0] if sorted_candidates else None)

        if primary:
            num, unit = extract_numeric_and_unit(primary.value)
            # 金額系指標は百万USDに統一換算（指標名で判断）
            num, unit = normalize_to_million_usd(num, unit, metric_name)
        else:
            num, unit = "", ""

        # 単位キャッシュ
        if unit:
            unit_cache[cym_key] = unit

        # 信頼性
        reliability_cache[cym_key] = reliability
        # 要確認の場合、どのソースの値を採用したかをnoteに付記する
        if reliability == "要確認" and primary and note:
            note = f"{note}  ★採用: [{primary.document_type}|{primary.source_file}] {primary.value}"
        if note:
            note_cache[cym_key] = note

        if primary:
            source_cache[cym_key].add((primary.source_file, primary.page))
            # 要確認（複数ソースで値が食い違う）場合は全ソースを記録
            if reliability == "要確認":
                for e in entries:
                    if e.value not in EMPTY_VALUES:
                        source_cache[cym_key].add((e.source_file, e.page))

        # 内訳データあり・静的ノート（AIなしで常に動作）
        static_note = _detect_partial_data_note(entries)
        if static_note:
            static_note_cache[cym_key] = static_note

        # Proxy Statement 値の信頼性チェック
        proxy_warn = _check_proxy_value_suspicion(primary, entries, metric_name)
        if proxy_warn:
            proxy_warn_cache[cym_key] = proxy_warn

        # AI注釈が必要なケースを収集
        needs_ai, context = _needs_ai_note(entries)
        if needs_ai:
            ai_context_cache[cym_key] = (metric_name, context)

        if num and fiscal_year in FISCAL_YEARS:
            # 要確認 かつ 採用ソースが annual_report 以外の場合のみ ★
            if reliability == "要確認" and primary and primary.document_type != "annual_report":
                value_cache[cym_key] = f"{num}★"
            else:
                value_cache[cym_key] = num

    # --- AI補足コメント生成 ---
    ai_note_cache: dict[tuple, str] = {}
    if ai_model and ai_context_cache:
        print(f"  [AI注釈] {len(ai_context_cache)} 件を分析中...")
        items = [(k, v[0], v[1]) for k, v in ai_context_cache.items()]
        ai_note_cache = generate_ai_notes(items, ai_model)
        print(f"  [AI注釈] {len(ai_note_cache)} 件のコメントを生成しました")

    # --- 企業・指標の出力順 ---
    all_companies = sorted({k[0] for k in all_cym_keys})

    ordered_metrics: list[str] = []
    seen_m: set[str] = set()
    for m in METRIC_ORDER:
        if m not in seen_m:
            ordered_metrics.append(m)
            seen_m.add(m)
    extra = sorted({k[2] for k in all_cym_keys} - seen_m)
    ordered_metrics.extend(extra)

    # --- 行生成（1行 = 企業 × 指標、年度は列に展開、備考も年度ごと） ---
    rows = []
    for company in all_companies:
        for metric_name in ordered_metrics:
            # この企業×指標の組み合わせが存在するか確認
            if not any((company, yr, metric_name) in all_cym_keys for yr in FISCAL_YEARS):
                continue

            # 全年度のデータを収集（年度ごとに独立して保持）
            year_values: dict[str, str] = {}
            year_notes: dict[str, str] = {}   # 年度ごとの備考
            all_src: set = set()
            unit = ""

            for fiscal_year in FISCAL_YEARS:
                cym_key = (company, fiscal_year, metric_name)
                if cym_key not in all_cym_keys:
                    continue
                year_values[fiscal_year] = value_cache.get(cym_key, "")
                if not unit:
                    unit = unit_cache.get(cym_key, "")
                all_src |= source_cache.get(cym_key, set())

                # 年度固有の備考を組み立てる
                rel = reliability_cache.get(cym_key, "記載なし")
                note = note_cache.get(cym_key, "")
                ai_note = ai_note_cache.get(cym_key, "")
                sn = static_note_cache.get(cym_key, "")
                pw = proxy_warn_cache.get(cym_key, "")
                combined_static = "  ".join(filter(None, [sn, pw]))
                year_notes[fiscal_year] = _build_note(rel, note, ai_note, combined_static)

            if skip_empty and not any(year_values.values()):
                continue

            sources = ", ".join(
                sorted(
                    (
                        f"{fname} p.{page}"
                        if page and page not in ("記載なし", "", "null", "None")
                        else fname
                    )
                    for fname, page in all_src
                )
            )

            company_id = _lookup_company_id(company)
            metric_field_id = METRIC_FIELD_ID.get(metric_name, "")
            row = {
                "企業ID": company_id,
                "企業名": company,
                "指標ID": metric_field_id,
                "指標名": metric_name,
                "単位": unit,
                "データソース": sources,
                "備考": "",
            }
            for yr in FISCAL_YEARS:
                row[f"{yr}年度"] = year_values.get(yr, "") or "記載なし"
            for yr in FISCAL_YEARS:
                row[f"{yr}年備考"] = year_notes.get(yr, "")
            rows.append(row)

    value_columns = [f"{yr}年度" for yr in FISCAL_YEARS]
    note_columns = [f"{yr}年備考" for yr in FISCAL_YEARS]
    fieldnames = ["企業ID", "企業名", "指標ID", "指標名", "単位", "データソース", "備考"] + value_columns + note_columns
    return rows, fieldnames


def _build_note(reliability: str, note: str, ai_note: str = "", static_note: str = "") -> str:
    """備考列の文字列を組み立てる。
    ai_note はGeminiが生成した補足コメント、static_note は静的検出による補足（AIなし）。
    """
    parts = []
    if reliability == "要確認":
        parts.append(f"要確認｜{note}" if note else "要確認")
    elif reliability == "OK（優先ソース採用）":
        parts.append(f"優先採用｜{note}" if note else "優先採用")
    elif reliability == "計算値":
        short = note[:80] + "…" if len(note) > 80 else note
        parts.append(f"計算値｜{short}" if short else "計算値")
    elif note:
        parts.append(note)
    if static_note:
        parts.append(static_note)
    if ai_note:
        parts.append(ai_note)
    return "  ".join(parts)


# ---------------------------------------------------------------------------
# 静的補足ノート検出（AIなし・常時動作）
# ---------------------------------------------------------------------------

def _detect_partial_data_note(entries: list[MetricEntry]) -> str:
    """
    値は記載なしだが、部分的な情報・理由が読み取り方や根拠に記載されているケースを
    静的に検出し、備考文字列を返す（AIなし・常時動作）。

    対応パターン:
      ① 合計は非記載だが内訳データあり   → "内訳データあり（年齢別・性別）"
      ② 計算に必要な別データが不足       → "算出に必要なデータ不足（株価等）"
      ③ 比率はあるが実数が不明           → "比率・割合のみ記載（実数不明）"
      ④ 特定スコープ・サブグループのみ   → "特定スコープのみ記載（役員レベル等）"
      ⑤ 代替指標・近似値のみ存在         → "代替指標のみ記載"

    値が存在する場合や該当しない場合は "" を返す。
    """
    for e in entries:
        # 値がある場合はスキップ
        if e.value not in EMPTY_VALUES:
            continue

        text = (e.reading_note or "") + " " + (e.evidence_summary or "")
        if not text.strip():
            continue

        # ① 合計/総数は記載なし、内訳データは記載あり
        has_breakdown_positive = re.search(
            r'は記載されている|内訳.*あり|breakdown.*available',
            text, re.IGNORECASE
        )
        has_breakdown_denial = re.search(
            r'合計は.*記載されていない|総.*は.*記載.*ない|全体.*記載.*ない'
            r'|total.*not.*record|total.*not.*available',
            text, re.IGNORECASE
        )
        if has_breakdown_positive and has_breakdown_denial:
            parts = []
            if re.search(r'年齢|age', text, re.IGNORECASE):
                parts.append("年齢別")
            if re.search(r'性別|gender|男性|女性|male|female', text, re.IGNORECASE):
                parts.append("性別")
            if re.search(r'部門|division|department|region|地域', text, re.IGNORECASE):
                parts.append("部門別")
            if re.search(r'雇用区分|雇用形態|employment type', text, re.IGNORECASE):
                parts.append("雇用区分別")
            return f"内訳データあり（{'・'.join(parts)}）" if parts else "内訳データあり"

        # ② 計算に必要な別データが不足（計算不可系）
        if re.search(
            r'不足しているため.*(計算|算出)|記載.*(ない|なく).*(ため|ので).*(計算|算出)|'
            r'計算不可|算出できない|計算できない',
            text, re.IGNORECASE
        ):
            # 具体的な欠落データを特定
            if re.search(r'株価|stock price|share price', text, re.IGNORECASE):
                return "算出に必要なデータ不足（株価情報なし）"
            if re.search(r'従業員.*総数|総従業員.*数|total.*employee', text, re.IGNORECASE):
                return "算出に必要なデータ不足（従業員総数なし）"
            if re.search(r'経常利益|営業利益|純利益|operating income|net income', text, re.IGNORECASE):
                return "算出に必要なデータ不足（利益情報なし）"
            return "算出に必要なデータ不足"

        # ③ 比率はあるが実数が不明（「比率から実数を算出できない」「総数が不明確」）
        if re.search(
            r'比率から実数|総数.*不明|実数.*算出できない|総数.*記載.*ない.*比率|'
            r'total.*unclear|total.*not.*clear',
            text, re.IGNORECASE
        ):
            return "比率・割合のみ記載（実数不明）"

        # ④ 特定スコープ・サブグループのみ記載（全体値なし）
        if re.search(
            r'役員レベル.*記載|Executive.*のみ|管理職.*のみ|特定.*(プログラム|研修).*のみ|'
            r'非契約.*のみ|全従業員ではなく|全体.*ではなく',
            text, re.IGNORECASE
        ):
            # スコープの種類を特定
            if re.search(r'役員|Executive|Leadership Team', text, re.IGNORECASE):
                return "特定スコープのみ記載（役員レベル等）"
            if re.search(r'非契約|Noncontract', text, re.IGNORECASE):
                return "特定スコープのみ記載（非契約社員のみ）"
            return "特定スコープのみ記載"

        # ⑤ 代替指標・近似値のみ存在
        if re.search(
            r'近い指標|代替.*として|代替指標|参考値として|最も関連性.*数値|'
            r'EBITDA.*記載|調整後.*として|proxy.*measure',
            text, re.IGNORECASE
        ):
            return "代替指標のみ記載"

    return ""


# ---------------------------------------------------------------------------
# AI 補足コメント生成（Gemini バッチ）
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Proxy Statement 値の疑わしさ検出
# ---------------------------------------------------------------------------

# Proxy Statement の根拠・読み取り方に含まれると「非GAAP/部分期間」を示すキーワード
_PROXY_SUSPICIOUS_KEYWORDS = [
    # 非GAAP・調整後
    "adjusted", "non-gaap", "non gaap", "adjusted revenue", "adjusted operating",
    "non-gaap revenue", "adjusted ebitda", "operating earnings",
    "adjusted net income", "adjusted eps",
    # 部分期間
    "quarter", "q1", "q2", "q3", "q4", "six months", "nine months",
    "3 months", "6 months", "9 months", "half year", "interim",
    "3ヶ月", "6ヶ月", "9ヶ月", "四半期", "半期",
    # 目標・見積もり
    "target", "goal", "forecast", "guidance", "budget", "estimate",
    "目標", "見込み", "予算",
    # 報酬評価文脈（Proxy特有の文脈）
    "incentive", "psu", "performance unit", "compensation metric",
    "award", "peer group",
]

# 財務指標 + HR指標（Proxy が低優先のカテゴリ）
_PROXY_CHECK_METRICS = {
    "売上高", "営業利益", "経常利益", "当期純利益",
    "従業員一人当たり経常利益", "従業員一人当たり純利益",
    "株価収益率", "総資産額", "純資産額", "資本金",
    "総従業員数", "男性従業員数", "女性従業員数",
    "総研修実施時間", "人材開発・研修の総費用",
    "労災の件数", "労働災害件数",
}


def _check_proxy_value_suspicion(
    primary: "MetricEntry | None",
    all_entries: "list[MetricEntry]",
    metric_name: str,
) -> str:
    """
    採用された代表値が Proxy Statement 由来の場合、またはすべてのソースが Proxy のみの場合に
    値の疑わしさを検出し、警告文字列を返す。

    検出パターン:
      A) 採用値が proxy_statement 由来で、annual_report の値と大幅に乖離（>5%）
      B) 採用値が proxy_statement 由来で、evidence/reading_note に非GAAP・部分期間キーワード
      C) 財務指標で annual_report が存在せず proxy_statement のみ（信頼性低）
    """
    if metric_name not in _PROXY_CHECK_METRICS:
        return ""
    if primary is None:
        return ""

    warnings = []

    # パターン B: キーワード検出（採用値が proxy 由来のとき）
    if primary.document_type == "proxy_statement":
        combined_text = (
            (primary.evidence_summary or "") + " " + (primary.reading_note or "")
        ).lower()
        matched = [kw for kw in _PROXY_SUSPICIOUS_KEYWORDS if kw in combined_text]
        if matched:
            warnings.append(
                f"Proxy値要注意（非GAAP/部分期間の可能性: {', '.join(matched[:3])}）"
            )

    # パターン A: annual_report 値と proxy_statement 値の乖離チェック
    ar_entries = [
        e for e in all_entries
        if e.document_type == "annual_report" and e.value not in EMPTY_VALUES
    ]
    px_entries = [
        e for e in all_entries
        if e.document_type == "proxy_statement" and e.value not in EMPTY_VALUES
    ]

    if ar_entries and px_entries:
        ar_num, _ = extract_numeric_and_unit(ar_entries[0].value)
        px_num, _ = extract_numeric_and_unit(px_entries[0].value)
        try:
            ar_f, px_f = float(ar_num), float(px_num)
            if ar_f != 0:
                diff_ratio = abs(ar_f - px_f) / abs(ar_f)
                if diff_ratio > 0.05:  # 5%超の乖離
                    warnings.append(
                        f"Proxy値要注意（Annual Report比 {diff_ratio*100:.1f}% 乖離）"
                    )
        except (ValueError, ZeroDivisionError):
            pass

    # パターン C: 財務指標で annual_report が存在しない
    if metric_name in {
        "売上高", "営業利益", "経常利益", "当期純利益", "総資産額", "純資産額"
    }:
        if not ar_entries and px_entries:
            warnings.append("Annual Report未参照（Proxy値のみ: 確認推奨）")

    return "  ".join(warnings)


# AI分析が必要なケースの判定
def _needs_ai_note(entries: list[MetricEntry]) -> tuple[bool, str]:
    """
    AI補足コメントが必要かどうかを判定し、Geminiに渡す文脈テキストを返す。

    対象ケース:
      1. グラフはあるが数値が読み取れない
      2. 別書類に情報があると示唆されている
      3. 数値なしだが「読み取り方」に具体的な理由がある
      4. 定義や集計対象が通常と異なる可能性がある
    """
    for e in entries:
        v = e.value
        rn = e.reading_note
        ev = e.evidence_summary

        # グラフあり・数値不明
        if re.search(r'グラフ|詳細数値不明|chart|graph', v + rn, re.IGNORECASE):
            return True, rn or ev

        # 別書類への参照
        if re.search(
            r'proxy statement|別書類|別報告書|incorporated by reference|参照として組み込|委任状',
            rn + ev, re.IGNORECASE
        ):
            return True, rn or ev

        # 読み取り方に具体的な理由が書かれていて、かつ値が空の場合
        if v in EMPTY_VALUES and rn and len(rn) > 10:
            return True, rn

        # 定義・スコープの限定
        if re.search(
            r'定義|スコープ|対象外|含まない|除く|のみ|限定|全従業員ではなく|米国のみ|日本のみ',
            rn + ev, re.IGNORECASE
        ):
            return True, rn or ev

    return False, ""


AI_NOTE_PROMPT = """以下は企業の年次報告書等から指標を抽出した際のAIの「読み取り方」または「根拠」メモです。
この内容を人間のデータ入力担当者向けに、備考欄に記載する簡潔な日本語コメント（30文字以内）に要約してください。

【要約ルール】
- 「グラフはあるが数値の記載なし」「別書類（Proxy Statement）に記載あり」「日本のみの集計」「定義が通常と異なる可能性あり」のような端的な表現にする
- 不要な説明・修飾語は省く
- 必ずコメントのみを返す（JSON不要、説明文不要）

【メモ】
{context}
"""


def generate_ai_notes(
    items: list[tuple],  # [(cm_key, metric_name, context_text), ...]
    model,
) -> dict[tuple, str]:
    """
    Gemini を使って備考コメントを一括生成する。

    items: [(cm_key, metric_name, context_text), ...]
    返り値: {cm_key: ai_note_string}
    """
    import time
    results: dict[tuple, str] = {}
    BATCH_SIZE = 15  # 1リクエストに詰め込む最大件数

    for batch_start in range(0, len(items), BATCH_SIZE):
        batch = items[batch_start: batch_start + BATCH_SIZE]

        # バッチを1プロンプトにまとめる
        lines = []
        for i, (cm_key, metric_name, context) in enumerate(batch, 1):
            lines.append(f"[{i}] 指標名: {metric_name}\nメモ: {context[:300]}")
        combined_context = "\n\n".join(lines)

        prompt = (
            "以下は複数の指標に関するメモです。各指標について備考欄コメント（30文字以内）を、"
            "番号付きリスト形式（例: 1. コメント）で返してください。コメント以外のテキストは不要です。\n\n"
            + combined_context
        )

        for attempt in range(6):
            try:
                resp = model.generate_content(
                    prompt,
                    generation_config=model._generation_config
                    if hasattr(model, '_generation_config')
                    else None,
                )
                text = resp.text.strip()
                # 番号付きリストをパース
                for i, (cm_key, _, _) in enumerate(batch, 1):
                    m = re.search(rf'^{i}[.．、]\s*(.+)', text, re.MULTILINE)
                    if m:
                        results[cm_key] = m.group(1).strip()[:60]
                break  # 成功したらループを抜ける
            except Exception as e:
                err = str(e)
                if any(kw in err.lower() for kw in ("429", "quota", "resource exhausted", "rate limit")):
                    wait_sec = min(60, 2 ** attempt)
                    print(f"  [レートリミット] {wait_sec}秒待機してリトライ ({attempt + 1}/6)...")
                    time.sleep(wait_sec)
                else:
                    print(f"  [AI注釈エラー] batch {batch_start}: {e}")
                    break

        if batch_start + BATCH_SIZE < len(items):
            time.sleep(2.0)

    return results


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="md_outputs のMarkdownをHCPro入力フォーマットのCSVに出力する"
    )
    parser.add_argument("--company", metavar="NAME", help="会社名で絞り込み（部分一致）")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, metavar="FILE",
                        help=f"出力CSVファイル（デフォルト: {DEFAULT_OUTPUT}）")
    parser.add_argument("--skip-empty", action="store_true",
                        help="値が空の行を除外する")
    parser.add_argument("--ai-notes", action="store_true",
                        help="GeminiAIで備考コメントを自動生成する（GEMINI_API_KEY 必須）")
    args = parser.parse_args()

    ai_model = None
    if args.ai_notes:
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            print("[エラー] --ai-notes を使うには環境変数 GEMINI_API_KEY を設定してください")
            return
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        ai_model = genai.GenerativeModel(
            "gemini-2.5-flash-lite",
            generation_config=genai.GenerationConfig(temperature=0.0),
        )
        print("[AI注釈モード] Gemini を使用して備考コメントを生成します")

    print("Markdownファイルを解析・集約中...")
    rows, fieldnames = aggregate_to_hcpro(
        MD_OUTPUT_ROOT, args.company, skip_empty=args.skip_empty, ai_model=ai_model
    )

    if not rows:
        print("データが見つかりませんでした。")
        return

    output_path = os.path.abspath(args.output)
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # --- サマリー ---
    total = len(rows)
    with_data = sum(1 for r in rows if any(r.get(f"{yr}年度") not in ("", "記載なし") for yr in FISCAL_YEARS))
    needs_review = [r for r in rows if any("要確認" in r.get(f"{yr}年備考", "") for yr in FISCAL_YEARS)]
    calc_only = [r for r in rows if any(r.get(f"{yr}年備考", "").startswith("計算値") for yr in FISCAL_YEARS)]

    print(f"\n=== 集計結果 ===")
    print(f"総行数（企業×指標）: {total:,}")
    print(f"  数値あり          : {with_data:,}")
    print(f"  要確認            : {len(needs_review):,}")
    print(f"  計算値            : {len(calc_only):,}")

    if needs_review:
        print(f"\n=== 要確認リスト ===")
        for r in needs_review:
            years_with_val = [yr for yr in FISCAL_YEARS if r.get(f"{yr}年度") not in ("", "記載なし")]
            print(f"  [{r['企業名']}] {r['指標名']}: {', '.join(years_with_val)}")
            notes_preview = "  ".join(filter(None, [r.get(f"{yr}年備考", "") for yr in FISCAL_YEARS if "要確認" in r.get(f"{yr}年備考", "")]))
            print(f"    → {notes_preview[:120]}")

    print(f"\n[出力] {output_path}")


if __name__ == "__main__":
    main()
