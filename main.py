"""
[1] Trino 병렬 호출
    └─ table_info.json 생성
        ↓
[2] table_info.json 로드
        ↓
[3] langchain 테이블 단위 호출
        ↓
[4] classification_result.json 생성

"""
import os
from lib.trino_for_table_info import build_table_info_json
from lib.classify_columns import classify_tables
from dotenv import load_dotenv

load_dotenv()

def main():
    env={
        "llm_host": os.getenv("LLM_HOST"),
        "llm_model": os.getenv("LLM_MODEL"),
        "host": os.getenv("HOST"),
        "port": int(os.getenv("PORT")),
        "user": os.getenv("USER"),
        "password": os.getenv("PASSWORD"),
        "http_scheme": os.getenv("HTTP_SCHEME"),
        "catalog": os.getenv("CATALOG"),
        "verify": os.getenv("VERIFY")# SSL 인증서 검사 여부
    }

    # ✅ 테이블 목록은 main에서만 정의
    table_list = os.getenv("TABLE_LIST").split(',')

    print("🚀 파이프라인 시작")

    # 1. Trino → table_info.json
    build_table_info_json(
        env,
        table_list=table_list,
        output_path="table_info.json"
    )

    print("\n============================\n")

    # 2. table_info.json → LLM 분류
    classify_tables(
        env,
        input_json="table_info.json",
        model=env['llm_model']
    )

    print("\n🎉 파이프라인 완료")


if __name__ == "__main__":
    main()
