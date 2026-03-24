import json
with open('data/raw_data_movies_top_100.json','r') as raw_data:
   data =  json.load(raw_data)
#print(json.dumps(data,indent=4)[:500])
column_list = list(data[1].keys())
print(column_list)
for row in data:
    if all(key in column_list for key in list(row.keys())):
      continue
    else:
       print(f'Row number:{data.index(row)} has unmatching columns')
for row in data:
   for key,value in row.items():
        if value is None or len(str(value)) == 0:
            print(f"Value is missing for: '{key}' at row number: {data.index(row)}")
        elif isinstance(value,(list,tuple,set)):
           value = list(value)

                      

         