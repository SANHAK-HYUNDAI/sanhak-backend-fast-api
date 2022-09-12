import pandas as pd

insert_size_limit = 10000
rename_dict = {
    "차종": "vehicle_type",
    "★품번10": "part_number",
    "원인부품": "cause_part",
    "원인품명(국문)": "cause_part_name_kor",
    "원인품명(영문)": "cause_part_name_eng",
    "현상": "symptom",
    "특이사항": "special_note",
    "★위치": "location",
    "★원인부품군집": "cause_part_cluster",
    "★문제현상": "problematic_situation",
    "★문제점": "cause"
}

order_list = ["vehicle_type",
              "cause",
              "part_number",
              "cause_part",
              "cause_part_name_kor",
              "cause_part_name_eng",
              "cause_part_cluster",
              "symptom",
              "special_note",
              "location",
              "problematic_situation",
              "created_at",
              "modified_at"]

excel_data = pd.read_excel("./data/ro.xlsx")
print(excel_data.columns)
excel_data.rename(columns=rename_dict, inplace=True)
print(excel_data.columns)

data_str = ""

for j in range(10):
    data_str += "("
    sort_list = []
    for i in order_list[:-2]:
        sort_list.append(str(excel_data.iloc[j][i]))
    data_str += ",".join(sort_list)
    data_str += "),"

print(data_str)
