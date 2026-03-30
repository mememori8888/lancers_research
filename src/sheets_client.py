import json

import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1crQML52by8U3rGbEBrO8n4SX1iSbSifn7hKHWHpVeOM"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetsClient:
    def __init__(self, credentials_json: str):
        creds_dict = json.loads(credentials_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        self.spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    def get_companies(self, sheet_index: int = 0, id_col: int = 1, name_col: int = 2) -> list[dict]:
        """スプレッドシートから企業IDと企業名の一覧を取得する"""
        sheet = self.spreadsheet.get_worksheet(sheet_index)
        all_values = sheet.get_all_values()

        companies = []
        for row in all_values[1:]:  # ヘッダー行をスキップ
            if len(row) < max(id_col, name_col):
                continue
            company_id = row[id_col - 1].strip()
            company_name = row[name_col - 1].strip()
            if not company_name:
                continue
            companies.append({"id": company_id, "name": company_name})

        return companies

    def write_results(self, results: list[dict], sheet_name: str = "調査結果") -> None:
        """結果をスプレッドシートの指定シートに書き込む"""
        try:
            ws = self.spreadsheet.worksheet(sheet_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=10)

        headers = ["企業ID", "元の会社名", "英語名"]
        rows = [headers] + [
            [
                r["id"],
                r["original_name"],
                r["english_name"],
            ]
            for r in results
        ]
        ws.update("A1", rows)
