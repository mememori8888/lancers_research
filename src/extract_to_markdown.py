"""PDFをGemini（Chain of Thought）で解析し、根拠付きMarkdownを出力するスクリプト。

各PDFについて:
  1. Geminiが Chain of Thought で指標を読み取り（カテゴリごとに3回に分けてAPIを実行）
  2. 指標ごとに「根拠テキスト（原文引用）」と「値」「記載ページ」「計算式」を返す
  3. Markdown形式で md_outputs/{会社}/{ファイル名}.md に保存

使い方:
  python3 extract_to_markdown.py                       # 全PDFを処理
  python3 extract_to_markdown.py --company "General Mills"  # 1社のみ
  python3 extract_to_markdown.py --overwrite           # キャッシュ無視して再生成
"""

import argparse
import os
import re
import sys
import time

import google.generativeai as genai

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

PDF_ROOT = "/workspaces/lancers_research/pdfs"
MD_OUTPUT_ROOT = "/workspaces/lancers_research/md_outputs"

MODEL_NAME = "gemini-2.5-flash"

# ▼ 指標を3つのグループに分割（AIの注意力散漫を防ぐ） ▼
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

# ▼ プロンプト：視覚能力の活用、ページ番号の特定、計算式の明記、グラフ対応 ▼
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
# ユーティリティ
# ---------------------------------------------------------------------------

def load_env(key: str) -> str:
    value = os.environ.get(key, "")
    if not value:
        print(f"[エラー] 環境変数 {key} が設定されていません")
        sys.exit(1)
    return value


def md_output_path(company_folder: str, pdf_name: str) -> str:
    out_dir = os.path.join(MD_OUTPUT_ROOT, company_folder)
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(pdf_name)[0]
    return os.path.join(out_dir, f"{stem}.md")


def classify_pdf(model, uploaded_file) -> dict | None:
    """PDFの会計年度・種別・会社名をGeminiで判定する。"""
    import json
    try:
        resp = model.generate_content([uploaded_file, CLASSIFY_PROMPT])
        text = resp.text.strip()
        
        if "```" in text:
            text = re.sub(r"```[a-z]*", "", text).replace("```", "").strip()
        m = re.search(r"\{.*?\}", text, re.DOTALL)
        if m:
            text = m.group(0)
        return json.loads(text)
    except Exception as e:
        print(f"  [分類失敗]: {e}")
        return None


def build_markdown(
    company_name: str,
    fiscal_year: int | str,
    document_type: str,
    pdf_filename: str,
    cot_text: str,
) -> str:
    """Chain of Thought の出力にメタ情報ヘッダーを付けてMarkdownを組み立てる。"""
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


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def process_pdf(
    classify_model,
    extract_model,
    pdf_path: str,
    company_folder: str,
    overwrite: bool,
) -> None:
    pdf_name = os.path.basename(pdf_path)
    out_path = md_output_path(company_folder, pdf_name)

    if os.path.exists(out_path) and not overwrite:
        print(f"  [スキップ] 既存: {os.path.relpath(out_path, '/workspaces/lancers_research')}")
        return

    uploaded = None
    try:
        print(f"  [アップロード中] {pdf_name} ...")
        # PDFは1回だけアップロードし、分類と抽出のキャッシュとして使い回す
        uploaded = genai.upload_file(pdf_path, mime_type="application/pdf")

        # Step 1: 分類（年度・種別・会社名）
        print(f"  [分類中] {pdf_name}")
        info = classify_pdf(classify_model, uploaded)
        if not info:
            print(f"  [スキップ] 分類失敗: {pdf_name}")
            return

        fiscal_year = info.get("fiscal_year", "不明")
        document_type = info.get("document_type", "other")
        company_name_in_doc = info.get("company_name_in_doc", company_folder)

        print(f"  → {fiscal_year}年 / {document_type} / {company_name_in_doc}")

        if document_type == "other":
            print(f"  [スキップ] document_type=other: {pdf_name}")
            return

        # Step 2: グループごとに分けて Chain of Thought 抽出
        print(f"  [CoT抽出中] {pdf_name} ...")
        
        categories = [
            ("ガバナンス・役員・報酬", METRICS_GOV),
            ("財務・基本情報", METRICS_FIN),
            ("人的資本・ESG", METRICS_HR)
        ]
        
        cot_results = []
        for cat_name, metrics in categories:
            print(f"    - {cat_name} を抽出しています...")
            metrics_list = "\n".join(f"- {m}" for m in metrics)
            prompt = COT_PROMPT.replace("{metrics_list}", metrics_list).replace("{category_name}", cat_name)
            
            resp = extract_model.generate_content(
                [uploaded, prompt],
                generation_config=genai.GenerationConfig(temperature=0.0),
            )
            cot_results.append(f"## {cat_name}\n\n{resp.text.strip()}\n")
            
            # APIのレートリミット対策（連続リクエストになるため）
            time.sleep(3.0)

        # すべてのカテゴリの結果を結合
        cot_text = "\n".join(cot_results)

        # Step 3: Markdown 組み立て・保存
        md = build_markdown(company_name_in_doc, fiscal_year, document_type, pdf_name, cot_text)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)

        rel = os.path.relpath(out_path, "/workspaces/lancers_research")
        print(f"  [保存] {rel}")

    except Exception as e:
        print(f"  [エラー発生] {pdf_name}: {e}")
    finally:
        # ストレージ圧迫を防ぐため、処理が終わったファイルはサーバー上から削除
        if uploaded:
            try:
                genai.delete_file(uploaded.name)
            except Exception:
                pass


def collect_pdfs(root: str, company_filter: str | None) -> list[tuple[str, str]]:
    """(会社フォルダ名, PDFパス) のリストを再帰的に取得して返す。"""
    result = []
    if not os.path.isdir(root):
        print(f"[エラー] PDFフォルダが存在しません: {root}")
        sys.exit(1)

    for folder_name in sorted(os.listdir(root)):
        folder_path = os.path.join(root, folder_name)
        if not os.path.isdir(folder_path):
            continue
        if folder_name.startswith("."):
            continue
        if company_filter:
            if company_filter.lower() not in folder_name.lower():
                continue
        
        # os.walk を使って会社フォルダの内部を再帰的に探索
        for current_dir, dirs, files in os.walk(folder_path):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            
            for fname in sorted(files):
                if fname.lower().endswith((".pdf", ".html", ".htm")):
                    result.append((folder_name, os.path.join(current_dir, fname)))

    return result


def main():
    global MD_OUTPUT_ROOT

    parser = argparse.ArgumentParser(
        description="PDFをChain of Thoughtで解析してMarkdownに出力する"
    )
    parser.add_argument("--company", metavar="NAME",
                        help="会社フォルダ名で絞り込み（部分一致）")
    parser.add_argument("--overwrite", action="store_true",
                        help="既存のMarkdownを上書きする")
    parser.add_argument("--output", default=MD_OUTPUT_ROOT, metavar="DIR",
                        help=f"出力先フォルダ（デフォルト: {MD_OUTPUT_ROOT}）")
    args = parser.parse_args()

    MD_OUTPUT_ROOT = os.path.abspath(args.output)

    api_key = load_env("GEMINI_API_KEY")
    genai.configure(api_key=api_key)

    classify_model = genai.GenerativeModel(
        MODEL_NAME,
        generation_config=genai.GenerationConfig(
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )
    extract_model = genai.GenerativeModel(MODEL_NAME)

    pdfs = collect_pdfs(PDF_ROOT, args.company)
    if not pdfs:
        print("処理対象のPDFが見つかりませんでした。")
        return

    print(f"対象: {len(pdfs)} ファイル\n")

    for i, (company_folder, pdf_path) in enumerate(pdfs, 1):
        print(f"[{i}/{len(pdfs)}] {company_folder} / {os.path.basename(pdf_path)}")
        process_pdf(classify_model, extract_model, pdf_path, company_folder, args.overwrite)

    print(f"\n完了。出力先: {MD_OUTPUT_ROOT}")


if __name__ == "__main__":
    main()