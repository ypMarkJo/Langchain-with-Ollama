# Langchain-with-Ollama
Langchain PoC


Local LLM을 이용해 QA 자동화에 활용.
현재 디멘전, 메트릭 자동 분류까지는 구현.
주요 디멘전, 메트릭 뽑는 프롬트는 구현중.



    # .env 예시

    # LLM INFO

    LLM_MODEL=qwen3:235b
    # LLM_MODEL=glm-64k:latest
    LLM_HOST=

    # TRINO INFO

    HOST=
    PORT=
    USER=
    PASSWORD=
    HTTP_SCHEME=https
    CATALOG=
    VERIFY={estore-CA.crt}경로

    # TAGET TABLE
    TABLE_LIST= dwh.t_global_agg_marketplace_promotion_sales_raw,dwh.t_glb_sales_sku_agg
