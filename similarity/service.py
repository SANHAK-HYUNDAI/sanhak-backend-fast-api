async def save_similar(similar_list, cursor):
    insert_similarity_sql = "insert into similarity (ca_id, ro_id) values (%s, %s)"
    values = [[str(ca_id), str(ro_id)] for ca_id, val in similar_list.items() for ro_id, similar in val]
    await cursor.executemany(insert_similarity_sql, values)
