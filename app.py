import zlib
import gzip
import sqlite3
import re
import json

def remove_html_tags(string):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', string)

def load_soc_heirarchy(path):
    soc_heirarchy_map = {}

    with open(path) as f:

        if f is None:
            print("error opening " + path)
            exit()

        for line in f:
            tokens = line.split('\n')[0].split(',')

            #easiest way to skip first entry
            if tokens[0] == "child":
                continue
            elif tokens[0] in soc_heirarchy_map:
                print("Duplicate Key")
            else:
                soc_heirarchy_map[tokens[0]] = [tokens[1], tokens[2], tokens[3]]
        return soc_heirarchy_map

            

def load_map_onet_soc(path):

    onet_soc_map = {}

    with open(path) as f:

        if f is None:
            print("error opening " + path)
            exit()

        for line in f:
            tokens = line.split('\n')[0].split(',')
            
            #easiest way to skip first entry
            if tokens[0] == "onet":
                continue
            elif tokens[0] not in onet_soc_map:
                onet_soc_map[tokens[0]] = tokens[1]
            else:
                print("Duplicate Key")
    return onet_soc_map

def main():

    soc_heirarchy_map = load_soc_heirarchy("soc_hierarchy.csv")
    onet_map = load_map_onet_soc("map_onet_soc.csv")
    
    conn = sqlite3.connect("test.db")

    create_table_sqlite = '''CREATE TABLE JOB_POSTINGS
    (ID INT PRIMARY KEY NOT NULL,
    BODY TEXT,
    TITLE TEXT,
    EXPIRED DATE,
    POSTED DATE,
    STATE TEXT,
    CITY TEXT,
    ONET TEXT,
    SOC5 TEXT,
    SOC2 TEXT)'''
    conn.execute(create_table_sqlite)

    id = 0
    with gzip.open("sample.gz") as f:
        for line in f:
            data_object = json.loads(line.decode())
            soc5 = onet_map[data_object["onet"]]
            

            #not the most elegant string doctoring.......
            try:
                soc2 = soc_heirarchy_map[soc5]
                
                sqlite_values_string = "(" + \
                    str(id) + ", '" + \
                    remove_html_tags(data_object['body'].replace("'","''")) + "', '"  + \
                    data_object['title'].replace("'","''") + "', '" +  \
                    data_object['state'].replace("'","''") + "', '" + \
                    data_object['city'].replace("'","''")  + "', '" +  \
                    data_object['posted'].replace("'","''")  + "', '" +  \
                    data_object['expired'].replace("'","''")  + "', '" +  \
                    data_object['onet'].replace("'","''")  + "', '" +  \
                    soc5.replace("'","''")  + "', '" + \
                    soc2[0] + "')"

            except KeyError:
                sqlite_values_string = "(" + \
                    str(id) + ", '" + \
                    remove_html_tags(data_object['body'].replace("'","''")) + "', '"  + \
                    data_object['title'].replace("'","''") + "', '" +  \
                    data_object['state'].replace("'","''") + "', '" + \
                    data_object['city'].replace("'","''")  + "', '" +  \
                    data_object['posted'].replace("'","''")  + "', '" +  \
                    data_object['expired'].replace("'","''")  + "', '" +  \
                    data_object['onet'].replace("'","''")  + "', '" +  \
                    soc5.replace("'","''")  + "', '" + \
                   "N/A')"


            sqlite_statement = "INSERT INTO JOB_POSTINGS (ID,BODY,TITLE,STATE,CITY,POSTED,EXPIRED,ONET,SOC5,SOC2) \
                VALUES " + sqlite_values_string

            conn.execute(sqlite_statement)
            conn.commit()
            id += 1

    c = conn.execute('SELECT posted, expired from JOB_POSTINGS')
    for row in c:
        print(row)


if __name__ == "__main__":
    main()
