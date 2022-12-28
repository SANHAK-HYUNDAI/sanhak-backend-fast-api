from typing import List


async def save_similar(similar_list, cursor):
    insert_similarity_sql = "insert into similarity (ca_id, ro_id) values (%s, %s)"
    values = [[str(ca_id), str(ro_id)] for ca_id, val in similar_list.items() for ro_id, similar in val]
    await cursor.executemany(insert_similarity_sql, values)


async def find_total_phenom_frequency(cursor) -> List:
    # 여러 맵핑 중에서 가장 유사도가 높은 ro에 대해서만 조회를 진행해서 구현할 예정
    select_sql = """
    select a.big_phenom, a.sub_phenom 
    from repair_order as a, 
    (
    select min(ro_id) as ro_id
    from similarity 
    group by ca_id
    ) as b
    where a.ro_id=b.ro_id
    """
    await cursor.execute(select_sql)
    return await cursor.fetchall()
