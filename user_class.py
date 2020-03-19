import json,os,csv

filename=os.environ.get("CLASS_FILE_NAME","User_classification_data.csv")

with open(filename,'r')as f:
    reader = csv.reader(f, delimiter=",")
    class_matrix = []
    for i, class_list in enumerate(reader):
        class_matrix.append(class_list)
