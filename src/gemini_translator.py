import google.generativeai as genai

MODEL = "gemini-2.5-flash"


class GeminiTranslator:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(MODEL)

    def translate_to_english(self, company_name: str) -> str:
        """会社名を英語の正式名称に変換する。失敗時は元の名前を返す"""
        prompt = (
            "以下の会社名を英語の正式名称に変換してください。"
            "会社名だけを回答し、余分な説明は不要です。\n"
            f"会社名: {company_name}"
        )
        try:
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            print(f"  [翻訳エラー] '{company_name}': {e}")
            return company_name  # フォールバック: 元の名前を返す
