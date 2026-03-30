"""PDFを自動ダウンロードして会社ごとのフォルダに保存する独立ツール。

入力CSV形式:
  company_name,url
  EOG Resources Inc,https://...
  EOG Resources Inc,https://...
  Apple Inc,https://...

使い方:
  python3 download_pdfs.py urls.csv
  python3 download_pdfs.py urls.csv --output /path/to/pdfs
  python3 download_pdfs.py urls.csv --overwrite       # 既存ファイルを上書き
  python3 download_pdfs.py --check                    # ダウンロード済み一覧を表示
  python3 download_pdfs.py --template                 # サンプルCSVを出力
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import unquote, urlparse

import requests

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pdfs")
DEFAULT_REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "download_reports")

DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/pdf,application/xhtml+xml,*/*",
}

CONTENT_TYPE_EXT = {
    "application/pdf": ".pdf",
    "text/html": ".html",
    "application/xhtml+xml": ".html",
    "application/octet-stream": ".pdf",
}

TEMPLATE_CSV = """\
company_name,url
EOG Resources Inc,https://example.com/eog-2024-10k.pdf
EOG Resources Inc,https://example.com/eog-2024-proxy.pdf
EOG Resources Inc,https://example.com/eog-2023-10k.pdf
Apple Inc,https://example.com/apple-2024-10k.pdf
Apple Inc,https://example.com/apple-2024-proxy.pdf
"""

# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def company_to_folder_name(company_name: str) -> str:
    """会社名をフォルダ名に変換する。
    例: "EOG Resources, Inc." → "EOG_Resources_Inc"
    """
    name = re.sub(r'[/\\:*?"<>|.,]', "", company_name)
    name = name.replace(" ", "_")
    return re.sub(r"_+", "_", name).strip("_")


def detect_extension(content_type: str, url: str) -> str:
    """Content-TypeヘッダーとURLからファイル拡張子を決定する。"""
    mime = content_type.split(";")[0].strip().lower()
    if mime in CONTENT_TYPE_EXT:
        return CONTENT_TYPE_EXT[mime]

    path = urlparse(url).path.lower()
    if path.endswith(".pdf"):
        return ".pdf"
    if path.endswith((".html", ".htm")):
        return ".html"

    return ".pdf"


def url_to_filename(url: str, company_name: str, existing: set, ext: str = ".pdf") -> str:
    """URLからファイル名を決める。URLのパス末尾を使い、重複時は連番を付ける。

    Args:
        url: ダウンロード元URL
        company_name: 会社名（重複回避の将来の拡張用）
        existing: 同フォルダに既に存在するファイル名のセット
        ext: detect_extension() で事前に決めた拡張子
    """
    parsed = urlparse(url)
    basename = os.path.basename(unquote(parsed.path))

    if not basename or basename == "/":
        basename = "document"

    # 拡張子を正しいものに統一
    stem, current_ext = os.path.splitext(basename)
    if current_ext.lower() in (".pdf", ".htm", ".html"):
        basename = stem + ext
    elif not current_ext:
        basename = stem + ext
    else:
        basename = basename + ext

    # 重複回避: 同フォルダに同名があれば連番
    candidate = basename
    counter = 1
    while candidate in existing:
        stem2, ext2 = os.path.splitext(candidate)
        stem2 = re.sub(r"_\d+$", "", stem2)
        candidate = f"{stem2}_{counter}{ext2}"
        counter += 1

    existing.add(candidate)
    return candidate


# ---------------------------------------------------------------------------
# CSV読み込み
# ---------------------------------------------------------------------------

def load_csv(csv_path: str) -> list[dict]:
    """CSVを読み込んでバリデーション済みレコードのリストを返す。

    ヘッダー形式: company_name,url,,,,...
    1行 = 1社 + 複数URL（2列目以降に並列配置）
    URLごとに1レコードを生成して返す。
    """
    if not os.path.exists(csv_path):
        print(f"[エラー] ファイルが見つかりません: {csv_path}", file=sys.stderr)
        sys.exit(1)

    records = []
    with open(csv_path, encoding="utf-8-sig") as f:
        # DictReaderは重複キー（空文字）を扱えないため、rawリーダーで読む
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None or not header:
            print("[エラー] CSVが空です", file=sys.stderr)
            sys.exit(1)

        if header[0].strip() != "company_name":
            print(f"[エラー] 1列目が 'company_name' ではありません: '{header[0]}'", file=sys.stderr)
            sys.exit(1)

        for line_no, row in enumerate(reader, start=2):
            if not row:
                continue
            company = row[0].strip()
            if not company:
                continue

            # 2列目以降の全セルをURLとして収集
            urls = [cell.strip() for cell in row[1:] if cell.strip().startswith("http")]
            if not urls:
                print(f"  [行{line_no}] {company}: URLなし（スキップ）")
                continue

            for url in urls:
                records.append({"company_name": company, "url": url})

    return records


# ---------------------------------------------------------------------------
# ダウンロード
# ---------------------------------------------------------------------------

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
RETRY_WAIT_SECONDS = [30, 60, 120]  # 1回目30秒、2回目60秒、3回目120秒待機


def download_file(url: str, save_path: str) -> tuple[bool, str, str]:
    """URLからPDF/HTMLをダウンロードしてsave_pathに保存する。
    503等のサーバーエラーは最大3回リトライする。
    Returns: (成功フラグ, 実際に保存したパス, メッセージ)
    """
    last_error = ""
    for attempt, wait in enumerate([0] + RETRY_WAIT_SECONDS):
        if wait > 0:
            print(f"  [{attempt}回目リトライ] {wait}秒待機中...")
            time.sleep(wait)
        try:
            resp = requests.get(url, headers=DOWNLOAD_HEADERS, timeout=60, stream=True)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            ext = detect_extension(content_type, url)

            # 拡張子をContent-Typeに合わせて修正
            stem, current_ext = os.path.splitext(save_path)
            if current_ext.lower() != ext:
                save_path = stem + ext

            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            size_kb = os.path.getsize(save_path) // 1024
            file_type = "HTML" if ext == ".html" else "PDF"
            return True, save_path, f"{file_type}  {size_kb:,} KB"

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            last_error = f"HTTP {status}"
            if status in RETRY_STATUS_CODES and attempt < len(RETRY_WAIT_SECONDS):
                print(f"  [{last_error}] リトライします...")
                continue
            return False, save_path, last_error
        except requests.exceptions.ConnectionError:
            last_error = "接続エラー"
            if attempt < len(RETRY_WAIT_SECONDS):
                print(f"  [{last_error}] リトライします...")
                continue
            return False, save_path, last_error
        except requests.exceptions.Timeout:
            last_error = "タイムアウト"
            if attempt < len(RETRY_WAIT_SECONDS):
                print(f"  [{last_error}] リトライします...")
                continue
            return False, save_path, last_error
        except Exception as e:
            return False, save_path, str(e)

    return False, save_path, last_error


def run_download(csv_path: str, output_dir: str, report_dir: str, overwrite: bool) -> None:
    """CSVを読み込んでPDFを一括ダウンロードし、結果レポートを出力する。"""
    records = load_csv(csv_path)
    if not records:
        print("ダウンロード対象がありません。")
        return

    output_dir = os.path.abspath(output_dir)
    report_dir = os.path.abspath(report_dir)
    print(f"CSV:    {csv_path}")
    print(f"出力先: {output_dir}")
    print(f"対象:   {len(records)} 件\n")

    # 会社ごとに既存ファイル名を追跡（重複回避用）
    company_existing: dict[str, set] = {}
    results = []

    for i, rec in enumerate(records, 1):
        company = rec["company_name"]
        url = rec["url"]
        folder_name = company_to_folder_name(company)
        folder_path = os.path.join(output_dir, folder_name)

        if folder_name not in company_existing:
            # 既存ファイルをセットに登録
            existing = set()
            if os.path.isdir(folder_path):
                existing = set(os.listdir(folder_path))
            company_existing[folder_name] = existing

        filename = url_to_filename(url, company, company_existing[folder_name])
        save_path = os.path.join(folder_path, filename)

        print(f"[{i}/{len(records)}] {company}")
        print(f"  URL: {url[:80]}{'...' if len(url) > 80 else ''}")

        if os.path.exists(save_path) and not overwrite:
            size_kb = os.path.getsize(save_path) // 1024
            print(f"  [スキップ] 既存: {filename} ({size_kb:,} KB)")
            results.append({
                "company_name": company,
                "url": url,
                "filename": filename,
                "status": "skipped",
                "detail": f"既存ファイル ({size_kb:,} KB)",
            })
            continue

        ok, actual_path, detail = download_file(url, save_path)
        actual_filename = os.path.basename(actual_path) if ok else None
        status = "success" if ok else "failed"
        icon = "完了" if ok else "失敗"
        print(f"  [{icon}] {os.path.basename(actual_path)}  {detail}")

        results.append({
            "company_name": company,
            "url": url,
            "filename": actual_filename,
            "status": status,
            "detail": detail,
        })

        time.sleep(0.5)

    # ---------------------------------------------------------------------------
    # 結果集計・レポート出力
    # ---------------------------------------------------------------------------
    success = sum(1 for r in results if r["status"] == "success")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = sum(1 for r in results if r["status"] == "failed")

    print(f"\n{'═' * 50}")
    print(f"  完了: {success} 件  スキップ: {skipped} 件  失敗: {failed} 件")
    print(f"{'═' * 50}")

    if failed:
        print("\n[失敗一覧]")
        for r in results:
            if r["status"] == "failed":
                print(f"  {r['company_name']} | {r['detail']}")
                print(f"    {r['url']}")

    # レポートファイル出力
    os.makedirs(report_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON（全詳細）
    json_path = os.path.join(report_dir, f"download_report_{timestamp}.json")
    report_data = {
        "timestamp": timestamp,
        "csv_file": csv_path,
        "output_dir": output_dir,
        "summary": {"success": success, "skipped": skipped, "failed": failed, "total": len(results)},
        "results": results,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)

    # CSV（失敗一覧）
    if failed:
        fail_csv_path = os.path.join(report_dir, f"failed_{timestamp}.csv")
        with open(fail_csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["company_name", "url", "detail"])
            writer.writeheader()
            for r in results:
                if r["status"] == "failed":
                    writer.writerow({
                        "company_name": r["company_name"],
                        "url": r["url"],
                        "detail": r["detail"],
                    })
        print(f"\n[失敗CSVを出力] {fail_csv_path}")

    print(f"[レポートを出力] {json_path}")


# ---------------------------------------------------------------------------
# 一覧表示
# ---------------------------------------------------------------------------

def show_status(output_dir: str) -> None:
    """ダウンロード済みPDFの一覧を表示する。"""
    output_dir = os.path.abspath(output_dir)
    if not os.path.isdir(output_dir):
        print(f"フォルダが存在しません: {output_dir}")
        return

    total_files = 0
    total_size = 0

    for company_folder in sorted(os.listdir(output_dir)):
        folder_path = os.path.join(output_dir, company_folder)
        if not os.path.isdir(folder_path):
            continue
        pdfs = sorted(f for f in os.listdir(folder_path) if f.lower().endswith(".pdf"))
        if not pdfs:
            continue
        print(f"\n{company_folder}/")
        for pdf in pdfs:
            size_kb = os.path.getsize(os.path.join(folder_path, pdf)) // 1024
            print(f"  {pdf:<50} {size_kb:>6,} KB")
            total_files += 1
            total_size += size_kb

    print(f"\n{'─' * 50}")
    print(f"合計: {total_files} ファイル  ({total_size:,} KB)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="download_pdfs",
        description="PDFを自動ダウンロードして会社ごとのフォルダに保存します",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python3 download_pdfs.py urls.csv
  python3 download_pdfs.py urls.csv --output /data/pdfs
  python3 download_pdfs.py urls.csv --overwrite
  python3 download_pdfs.py --check
  python3 download_pdfs.py --template > urls.csv
        """,
    )
    parser.add_argument("csv_file", nargs="?",
                        help="入力CSVファイルのパス（--check / --template 時は不要）")
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT_DIR, metavar="DIR",
                        help="PDF保存先フォルダ")
    parser.add_argument("--report-dir", default=DEFAULT_REPORT_DIR, metavar="DIR",
                        help="レポート出力フォルダ")
    parser.add_argument("--overwrite", action="store_true",
                        help="既存ファイルを上書きする（デフォルト: スキップ）")
    parser.add_argument("--check", action="store_true",
                        help="ダウンロード済みPDFの一覧を表示して終了")
    parser.add_argument("--template", action="store_true",
                        help="サンプルCSVを標準出力して終了")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.template:
        print(TEMPLATE_CSV, end="")
        return

    if args.check:
        show_status(args.output)
        return

    if not args.csv_file:
        parser.error("CSVファイルを指定してください。例: python3 download_pdfs.py urls.csv")

    run_download(args.csv_file, args.output, args.report_dir, args.overwrite)


if __name__ == "__main__":
    main()
