"""SEC EDGAR APIを使ってAnnual Report(10-K)とProxy Statement(DEF 14A)のURLを取得する。

方式:
  1. efts.sec.gov (EDGAR全文検索) で会社名 → CIK + accession_no を取得
  2. data.sec.gov/submissions で提出書類一覧を確認（補完用）
  3. www.sec.gov/Archives でドキュメントURLを構築
"""

import time

import requests

EDGAR_HEADERS = {"User-Agent": "lancers-research mememori8888@github.com"}
EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
INDEX_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{acc_dashed}-index.json"
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{filename}"


class EdgarClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(EDGAR_HEADERS)

    # ------------------------------------------------------------------
    # 内部ユーティリティ
    # ------------------------------------------------------------------
    def _get_json(self, url: str, params: dict | None = None) -> dict | None:
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  [EDGAR] GET失敗: {e}")
            print(f"          URL: {url}")
            return None

    # ------------------------------------------------------------------
    # efts.sec.gov で会社名+フォーム種別を検索
    # ------------------------------------------------------------------
    def _efts_search(self, company_name: str, form_type: str, year: int) -> dict | None:
        """EDGAR全文検索APIでフォームを検索。ヒットした最初の _source を返す。"""
        # カンマ以前の短縮社名のほうがヒットしやすい
        short_name = company_name.split(",")[0].strip()
        params = {
            "q": f'"{short_name}"',
            "forms": form_type,
            "dateRange": "custom",
            "startdt": f"{year}-01-01",
            "enddt": f"{year + 1}-04-30",
        }
        data = self._get_json(EFTS_URL, params=params)
        if not data:
            return None
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            # クォートなし再試行（完全一致ではなくキーワード検索）
            params["q"] = short_name
            data = self._get_json(EFTS_URL, params=params)
            if not data:
                return None
            hits = data.get("hits", {}).get("hits", [])
        return hits[0]["_source"] if hits else None

    # ------------------------------------------------------------------
    # ファイリングインデックスから最適なドキュメントURLを取得
    # ------------------------------------------------------------------
    def _get_doc_url(self, cik: int, accession: str) -> str | None:
        """filing index JSONからPDF優先・次いでHTMのURLを返す"""
        acc_nodash = accession.replace("-", "")
        idx_url = INDEX_URL.format(
            cik=cik, acc_nodash=acc_nodash, acc_dashed=accession
        )
        idx = self._get_json(idx_url)
        if not idx:
            return None

        items = idx.get("directory", {}).get("item", [])
        pdf_url = htm_url = None
        for item in items:
            name = item.get("name", "")
            doc_type = item.get("type", "")
            if doc_type not in ("10-K", "DEF 14A", "10-K/A", "DEFA14A"):
                continue
            url = ARCHIVES_BASE.format(
                cik=cik, acc_nodash=acc_nodash, filename=name
            )
            if name.lower().endswith(".pdf"):
                pdf_url = url
            elif name.lower().endswith((".htm", ".html")):
                htm_url = url
        return pdf_url or htm_url

    # ------------------------------------------------------------------
    # 公開エントリーポイント
    # ------------------------------------------------------------------
    def get_filing_urls(self, company_name: str, year: int) -> dict:
        """指定会社・年の10-K と DEF 14A のドキュメントURLを返す"""
        result = {
            "cik": None,
            "annual_report_url": None,
            "proxy_statement_url": None,
        }

        for form_type, key in [("10-K", "annual_report_url"), ("DEF 14A", "proxy_statement_url")]:
            src = self._efts_search(company_name, form_type, year)
            if not src:
                print(f"  [EDGAR] {form_type} ({year}年) ヒットなし: {company_name}")
                continue

            # CIK（先頭ゼロ除去した整数）
            cik_str = src.get("entity_id", "")
            cik = int(cik_str) if cik_str else None
            if cik and result["cik"] is None:
                result["cik"] = cik

            accession = src.get("accession_no", "")  # 例: "0000821189-24-000011"
            if not cik or not accession:
                continue

            url = self._get_doc_url(cik, accession)
            if url:
                result[key] = url
                print(f"  [EDGAR] {form_type}: {url}")
            else:
                # フォールバック: primary_document をそのまま組み立てる
                primary = src.get("file_name", "")
                if primary:
                    acc_nodash = accession.replace("-", "")
                    result[key] = ARCHIVES_BASE.format(
                        cik=cik, acc_nodash=acc_nodash, filename=primary
                    )
                    print(f"  [EDGAR] {form_type} (fallback): {result[key]}")

            time.sleep(0.2)

        return result
