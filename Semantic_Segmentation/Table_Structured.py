import jsonlines
import psycopg2


# 1.建表
def create_table():
    # text_id：给每行计算一个唯一编号，把“名称+字段名+子片段”放到下面的get_sha256函数计算
    create_sql = 'CREATE TABLE public.{} (text_id char(64) PRIMARY KEY,'.format('knowledge_split')
    # 子片段text、名称+字段名name_field、名称+字段名+子片段name_field_text，以及它们的embedding。var后面的数字是最大长度，不够可以加
    create_sql += 'text varchar(3600) DEFAULT NULL,text_emb vector(1024) NOT NULL,name_field varchar(3600) DEFAULT NULL,name_field_emb vector(1024) NOT NULL,name_field_text varchar(3600) DEFAULT NULL,name_field_text_emb vector(1024) NOT NULL,'
    # 药物名称name、字段名field、id、表名table_name
    create_sql += 'origin_name varchar(250) DEFAULT NULL,origin_field varchar(250) DEFAULT NULL,origin_row_id char(64) DEFAULT NULL,origin_table_name varchar(50) DEFAULT NULL)'
    cur.execute(create_sql)
    conn.commit()
create_table()


# 2.处理数据类型
# SHA-256算法生成的散列值是一个256位的二进制数字，通常以64个十六进制字符的形式表示
import hashlib
def get_sha256(text):
    obj = hashlib.sha256()
    obj.update(text.encode("utf-8"))
    return obj.hexdigest()
# BGE模型嵌入
from sentence_transformers import SentenceTransformer
model = SentenceTransformer(model_name_or_path='C:/Users/nibh/Desktop/科大讯飞/BAAI_bge-large-zh-v1.5')
def embedding(text):
    return model.encode([text], normalize_embeddings=True)[0].tolist()


# 3.切分jsonl
def split_jsonl(input_file,num_chunks):
    print("切分jsonl")
    with open(input_file, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    lines_per_part = len(lines)//num_chunks # if20行，得到7部分0-6，第0-5每部分3行，第6部分2行
    for i in range(num_chunks+1):
        start_idx = i * lines_per_part
        if i < num_chunks:
            end_idx = (i+1)*lines_per_part-1
        else:
            end_idx = len(lines)
        output_file = f"drug_knowledge片段_{i+1}.jsonl"
        with open(output_file, 'w', encoding='utf-8') as output:
            output.writelines(lines[start_idx:end_idx+1])
if __name__ == "__main__":
    input_file_path = r'C:\Users\nibh\Desktop\科大讯飞\drug_knowledge片段——再切分.jsonl'
    num_chunks = 6
    split_jsonl(input_file_path, num_chunks)

# 4.多进程
# 单个chunk处理数据
def process_chunk(file_path, msg):
    print("in:",msg)
    # with open(file_path, 'r', encoding='utf-8') as file:
    #     total_lines = sum(1 for line in file)
    with (jsonlines.open(file_path) as reader):
        processed_data = []
        for entry in reader:
            #tqdm(reader, desc="Processing entries", unit=" entries", total=total_lines):
            processed_data.append([get_sha256(entry["名称+字段名+子片段"]),
                                   entry["子片段"], embedding(entry["子片段"]),
                                   entry["名称+字段名"], embedding(entry["名称+字段名"]),
                                   entry["名称+字段名+子片段"], embedding(entry["名称+字段名+子片段"]),
                                   entry["药物名称"], entry["字段名"], entry["id"], entry["表名"]])
    print("out:",msg)
    return processed_data

# 多进程
from multiprocessing import Pool
processed_data = []
if __name__ == "__main__":
    pool = Pool(processes=2)
    print("多进程")
    item_list = [f'process{i+1}' for i in range(num_chunks+1)]
    main_path = r'C:\Users\nibh\Desktop\科大讯飞'
    ls_chunk_path = [f"{main_path}\drug_knowledge片段_{i+1}.jsonl" for i in range(num_chunks+1)]
    for i,path in enumerate(ls_chunk_path):
        processed_data += pool.apply_async(process_chunk, (path,item_list[i])).get()



'''
# 单进程处理
from tqdm import tqdm

with open(file_path, 'r', encoding='utf-8') as file:
    total_lines = sum(1 for line in file)

with (jsonlines.open(file_path) as reader):
    processed_data = []
    for entry in tqdm(reader, desc="Processing entries", unit=" entries", total=total_lines):
        processed_data.append([get_sha256(entry["名称+字段名+子片段"]),
                               entry["子片段"], embedding(entry["子片段"]),
                               entry["名称+字段名"], embedding(entry["名称+字段名"]),
                               entry["名称+字段名+子片段"], embedding(entry["名称+字段名+子片段"]),
                               entry["药物名称"], entry["字段名"], entry["id"], entry["表名"]])
'''

# 5.插入数据
conn = psycopg2.connect(host='', port=1111, database='', user='', password='')
cur = conn.cursor()   # 创建一个数据库游标

def insert_data(processed_data):
    print("插入")
    for d in processed_data:
        # 准备插入数据的 SQL 语句
        insert_sql = "INSERT INTO knowledge_split (text_id, text, text_emb, name_field, name_field_emb, name_field_text, name_field_text_emb,"
        insert_sql += "origin_name, origin_field, origin_row_id, origin_table_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cur.execute(insert_sql,d)
        conn.commit()

insert_data(processed_data)


# 关闭游标和数据库连接
cur.close()
conn.close()
