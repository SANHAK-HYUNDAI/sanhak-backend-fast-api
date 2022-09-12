bulk_insert_ro_sql = """
insert into repair_order (
vehicle_type,
cause,
part_number,
cause_part,
cause_part_name_kor,
cause_part_name_eng,
cause_part_cluster,
symptom,
pecial_note,
location,
problematic_situation,
created_at,
modified_at 
) values """