import json
import time
from pathlib import Path

from langchain_ollama import OllamaLLM
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser


PROMPT_TEMPLATE = """
You are a data warehouse modeling expert.

Given the table information below, classify each column into one of the following categories:
- "dimension"
- "metric"
- "excluded"

System-generated columns such as timestamps, technical audit fields, or metadata
(e.g. ts, timestamp, created_at, updated_at, ingestion_time) must be excluded
from both dimension and metric classifications.

[Classification Rules]
- dimension:
  - identifiers (id)
  - string or categorical values
  - dates (business dates, not system timestamps)
  - order_date_local & event_date_local is business dates
  - status or state fields
- metric:
  - numeric values
  - fields intended for aggregation (sum, count, average, etc.)
- excluded:
  - system or technical columns
  - timestamps or audit fields not used for business analysis

[Input]
{table_info}

[Output Rules]
- Output MUST be valid JSON only
- put a reason why classification was made for each colum 
- Do NOT include explanations or additional text
- Follow this exact JSON structure:

{{
  "table_name": "...",
  "dimension": [],
  "metric": [],
  "reasoning": {{}},
  "excluded": []
}}
"""

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def build_chain(env,model: str = "glm-64k:latest"):
    llm = OllamaLLM(
        model=model,
        base_url=env['llm_host'],
        tamperature=0.0
    )

    prompt = PromptTemplate(
        template=PROMPT_TEMPLATE,
        input_variables=["table_info"]
    )

    parser = JsonOutputParser()

    return prompt | llm | parser


def classify_tables(
    env,
    input_json: str = "table_info.json",
    model: str = "glm-64k:latest",
    concurrency: int = 3
):
    print("▶ Building LLM chain")

    chain = (
        build_chain(env,model)
        .with_config({"timeout": 300})
        .with_retry(stop_after_attempt=2)
    )

    tables = json.loads(Path(input_json).read_text(encoding="utf-8"))

    # output 디렉토리 보장
    Path("output").mkdir(exist_ok=True)

    total_start = time.time()
    total_tables = len(tables)

    print(f"▶ LLM Model: {model}")
    print(f"▶ Total tables: {total_tables}")
    print(f"▶ Chunk size (concurrency): {concurrency}")
    print("-" * 60)

    for chunk_idx, table_chunk in enumerate(chunked(tables, concurrency), start=1):
        # 이번 chunk의 table_name 목록
        table_names = [
            table.get("table_name", "UNKNOWN")
            for table in table_chunk
        ]

        print(f"🚀 Chunk {chunk_idx} started")
        print(f"   Tables: {', '.join(table_names)}")

        inputs = [
            {"table_info": json.dumps(table, ensure_ascii=False)}
            for table in table_chunk
        ]

        chunk_start = time.time()

        responses = chain.batch(
            inputs,
            max_concurrency=concurrency
        )

        elapsed = round(time.time() - chunk_start, 2)
        print(f"✅ Chunk {chunk_idx} completed in {elapsed}s")

        for response in responses:
            table_name = response.get("table_name")
            if not table_name:
                print("⚠️  Skipped response without table_name")
                continue

            output_path = Path(f"output/{table_name}.json")

            output_path.write_text(
                json.dumps(
                    {
                        "table_name": table_name,
                        "dimension": response.get("dimension", []),
                        "metric": response.get("metric", []),
                        "excluded": response.get("excluded", []),
                        "reasoning": response.get("reasoning", {}),
                    },
                    ensure_ascii=False,
                    indent=2
                ),
                encoding="utf-8"
            )

            print(f"📂 Saved: {output_path}")

        print("-" * 60)

    total_elapsed = round(time.time() - total_start, 2)
    print(f"🏁 All chunks completed in {total_elapsed}s")
