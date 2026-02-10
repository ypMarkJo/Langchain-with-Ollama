import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from trino.auth import BasicAuthentication
import trino



def get_trino_connection(env):
    return trino.dbapi.connect(
        host=env['host'],
        port=env['port'],
        user=env['user'],
        http_scheme=env['http_scheme'],
        auth=BasicAuthentication(env['user'], env['password']),
        catalog=env['catalog'],
        verify=env['verify']
    )


def fetch_table_info(env,table_name: str) -> dict:
    conn = get_trino_connection(env)
    cur = conn.cursor()

    query = f"SELECT * FROM {table_name} LIMIT 1"
    cur.execute(query)

    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    sample_data = [str(v) if v is not None else None for v in row]

    return {
        "table_name": table_name,
        "columns": columns,
        "sample_data": sample_data
    }


def build_table_info_json(
    env,
    table_list: list[str],
    output_path: str = "table_info.json",
    max_workers: int = 10
):
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_table_info,env,table): table
            for table in table_list
        }

        for future in as_completed(futures):
            table = futures[future]
            try:
                results.append(future.result())
                print(f"✅ Trino 수집 완료: {table}")
            except Exception as e:
                print(f"❌ Trino 실패: {table} | {e}")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n📂 {output_path} 생성 완료 ({len(results)} tables)")
