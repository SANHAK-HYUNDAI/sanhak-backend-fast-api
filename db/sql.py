bulk_insert_ro_sql = """
insert into repair_order (
    big_phenom          ,
    cause               ,
    cause_part          ,
    cause_part_cluster  ,
    cause_part_name_eng ,
    cause_part_name_kor ,
    location            ,
    part_number         ,
    problematic         ,
    special_note        ,
    sub_phenom          ,
    vehicle_type        
) values """

bulk_insert_ca_sql = """
insert into cafe_article(
board_name              ,
cafe_name               ,
content                 ,
created_at              ,
title                   ,
url                     ,
writer 
) values """

insert_ca_keyword_frequency_sql = """
insert into ca_keyword(
word,
count
)values(%s,%d) ON DUPLICATE KEY UPDATE VALUES word=%s, count=%d """

insert_ca_keywords = """
insert into keyword(
word,
ca_id
)values(%s, %d)
"""
