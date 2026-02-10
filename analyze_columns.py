import json
from pathlib import Path
from collections import defaultdict
import time

from langchain_ollama import OllamaLLM
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
import os

from dotenv import load_dotenv

load_dotenv()

# -------------------------
# 1. 컬럼 메타 집계
# -------------------------
def collect_column_stats(output_dir="output"):
    metric_stats = defaultdict(lambda: {"frequency": 0, "tables": set()})
    dimension_stats = defaultdict(lambda: {"frequency": 0, "tables": set()})

    for path in Path(output_dir).glob("*.json"):
        table = json.loads(path.read_text(encoding="utf-8"))
        table_name = table.get("table_name")

        for m in table.get("metric", []):
            metric_stats[m]["frequency"] += 1
            metric_stats[m]["tables"].add(table_name)

        for d in table.get("dimension", []):
            dimension_stats[d]["frequency"] += 1
            dimension_stats[d]["tables"].add(table_name)

    metrics = [
        {
            "name": k,
            "frequency": v["frequency"],
            "table_coverage": len(v["tables"]),
            "tables": sorted(v["tables"]),
        }
        for k, v in metric_stats.items()
    ]

    dimensions = [
        {
            "name": k,
            "frequency": v["frequency"],
            "table_coverage": len(v["tables"]),
            "tables": sorted(v["tables"]),
        }
        for k, v in dimension_stats.items()
    ]

    return metrics, dimensions


# -------------------------
# 2. LLM 체인 구성
# -------------------------
def build_chain(env):
    prompt = PromptTemplate.from_template(
        """
            You are a senior data warehouse QA expert.

            Given the list of metrics and dimensions extracted from multiple tables,
            select the most important ones for QA coverage.

            Rules:
            - Do NOT merge or normalize names
            - Dimension is most likely to have country-specific columns and date related to order or event
            - revenue, visit, count ,amount have high possiblility to be metric columns
            - Keep column names exactly as-is
            - Deduplicate identical names only
            - Prioritize:
            1. Frequency of appearance
            2. Table coverage
            3. QA risk / importance
            - Target:
            - Up to 20 metrics
            - Up to 20 dimensions
            - You MAY exceed 20 if necessary to ensure full table coverage

            Return JSON ONLY in the following format:

            {{
            "selected_metrics": [
                {{
                "name": "...",
                "reason": "..."
                }}
            ],
            "selected_dimensions": [
                {{
                "name": "...",
                "reason": "..."
                }}
            ]
            }}

            Input:
            {input_data}
        """
    )

    llm = OllamaLLM(
        model=env['llm_model'],
        base_url=env['llm_host'],
        temperature=0.0,
    )

    parser = JsonOutputParser()

    return prompt | llm | parser


# -------------------------
# 3. 전체 분석 실행
# -------------------------
def main(
    output_dir="output",
    result_path="qa_key_columns.json",
):
    env={
        "llm_host": os.getenv("LLM_HOST"),
        "llm_model": os.getenv("LLM_MODEL"),
    }

    print("▶ Collecting column statistics")
    metrics, dimensions = collect_column_stats(output_dir)

    payload = {
        "metrics": metrics,
        "dimensions": dimensions,
        "constraints": {
            "max_metrics": 10,
            "max_dimensions": 10,
            "allow_exceed_if_needed": True,
            "priority": "frequency > table_coverage > qa_importance",
        },
    }

    print(
        f"▶ Sending {len(metrics)} metrics / {len(dimensions)} dimensions to LLM"
    )

    chain = build_chain(env)

    start_time=time.time()

    response = chain.invoke(
        {
            "input_data": json.dumps(payload, ensure_ascii=False, indent=2)
        }
    )

    elapsed = round(time.time() - start_time, 2)
    print(f"✅ LLM completed in {elapsed}s")

    Path(result_path).write_text(
        json.dumps(response, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"✅ QA key columns saved to {result_path}")


# -------------------------
# CLI 실행
# -------------------------
if __name__ == "__main__":
    main()
