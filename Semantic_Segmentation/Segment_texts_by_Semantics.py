#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
import numpy as np
from collections import defaultdict
from codes.helptool.help_data import *
import pandas as pd
from tqdm import tqdm

from sentence_transformers import SentenceTransformer
model = SentenceTransformer(model_name_or_path='C:/Users/nibh/Desktop/科大讯飞/BAAI_bge-large-zh-v1.5')

from multiprocessing import Pool

df_drug = pd.read_csv(r'C:\Users\nibh\Desktop\科大讯飞\drug_knowledge.csv')


# In[ ]:


# 切分片段
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
split_max_num = 300
split_min_num = 30
thread_sim_score = 0.7
all_statis = defaultdict(list)


def get_score_bge(sent_list):
    return model.encode(sent_list, normalize_embeddings=True)

def split(context):  
    sent_list = context.split('。')
    sent_list = [_ for _ in sent_list if _]
    all_sens = []
    try:  # 计算相邻句子的相似度
        Z = get_score_bge(sent_list)    # 将单个句子转化为1024维向量。 如果sent_list有3个句子，Z里就是3个元素，每个元素是一个含1024个数的列表
        Z /= (Z ** 2).sum(axis=1, keepdims=True) ** 0.5  # 对句子向量进行L2范数归一化
        score = np.dot(Z[:-1], Z[1:].T)  # 计算两两句子间的语义相似度，score[i,j]表示第i个句子和第j+1个句子的语义相似度
        score = np.diagonal(score).tolist() # 只取相邻句子的相似度，作为切分依据
        
        cur_passage = ''
        for index, one_score in enumerate(score):
        
            # 如果已有段落超过阈值，直接切分
            if len(cur_passage) > split_max_num:
                all_sens.append(cur_passage)
                cur_passage = ''
            
            cur_sent = sent_list[index]
        
            # 如果相似度大于0.7或字数小于30，合并当前句子到当前段落，不切分
            if (one_score > thread_sim_score) or (len(cur_sent) < split_min_num):
                cur_passage += cur_sent + '。'
            
            else:  # 切分句子
                cur_passage += cur_sent + '。'
                all_sens.append(cur_passage)
                cur_passage = ''
            
        # 处理最后一句
        if cur_passage:
            all_sens.append(cur_passage)

        '''
        score是两两之间，比如4句话就有3个score，所以应该是
        cur_passage = sent_list[0]+"。"
        for index, one_score in enumerate(score):
            # 如果已有段落超过阈值，直接切分
            if len(cur_passage) > split_max_num:
                all_sens.append(cur_passage)
                cur_passage = ''
            
            cur_sent = sent_list[index+1]
            # 如果相似度大于0.7或字数小于30，合并当前句子到当前段落，不切分
            if (one_score > thread_sim_score) or (len(cur_sent) < split_min_num):
                cur_passage += cur_sent + '。'
            elif one_score <= thread_sim_score:  # 若相似度小于等于0.7，先切分并储存当前段落，再将当前句子赋值给当前段落
                all_sens.append(cur_passage)
                cur_passage = cur_sent + '。'
            else: # 若当前句子大于等于30字，先合并再切分句子
                cur_passage += cur_sent + '。'
                all_sens.append(cur_passage)
                cur_passage = ''
            
        # 处理最后一句
        if cur_passage:
            all_sens.append(cur_passage)
        '''

    except:
        print('无法计算相似度：{}'.format(context))
        all_sens.append(context)
    
    return all_sens


# In[ ]:


# 每个进程处理
def process_chunk(chunk):
    ls_info_chunk = []
    for i in range(len(chunk)):
        info = chunk.iloc[i, :].dropna()
        name_dict = {"id":"id","name":"药物名称","english_name":"英文名称","drug_sort":"药理分类","standard_name":"标准名称",
                     "commodity_name":"商品名","common_name":"通用名","specifications":"规格","component":"成份","appearance":"性状",
                     "indication":"适应症","contraindications":"禁忌证","usage":"用法用量","over_dosage":"药物过量",
                     "adverse_reactions":"不良反应","use_in_elderly":"老年用药","use_in_children":"儿童用药",
                     "use_in_pregnant":"孕妇及哺乳期妇女用药","pharmacokinetics":"药代动力学","relative_effection":"药物相互作用",
                     "pharmacological_effects":"药理作用","attention":"注意事项"}
        ls_columns_eng = info.index
        ls_columns = [name_dict[eng] for eng in ls_columns_eng]
        ncol = len(ls_columns)

        for j in range(2, ncol):
            if (j <= 8) or (len(info.iloc[j]) < 300):
                item = {"表名": "drug_knowledge",
                        "id": f"{info.iloc[0]}",
                        "药物名称": f"{info.iloc[1]}",
                        "字段名": f"{ls_columns[j]}",
                        "子片段": f"{info.iloc[j]}",
                        "名称+字段名": f"{info.iloc[1]}的{ls_columns[j]}",
                        "名称+字段名+子片段": f"{info.iloc[1]}的{ls_columns[j]}是{info.iloc[j]}"}
                ls_info_chunk.append(item)
            else:
                all_sens = split(info.iloc[j])
                for cut in all_sens:
                    item = {"表名": "drug_knowledge",
                            "id": f"{info.iloc[0]}",
                            "药物名称": f"{info.iloc[1]}",
                            "字段名": f"{ls_columns[j]}",
                            "子片段": f"{cut}",
                            "名称+字段名": f"{info.iloc[1]}的{ls_columns[j]}",
                            "名称+字段名+子片段": f"{info.iloc[1]}的{ls_columns[j]}是{cut}"}
                    ls_info_chunk.append(item)
    return ls_info_chunk


# In[ ]:


num_chunks = 4
chunks = [df_drug.iloc[i:i + len(df_drug) // num_chunks] for i in range(0, len(df_drug), len(df_drug) // num_chunks)]

if __name__ == "__main__":
    pool = Pool(processes=2)
    results = []
    for i in tqdm(chunks):
        results += pool.apply_async(process_chunk, (i,)).get()

    # 去重
    set_unique = set(tuple(d.items()) for d in results)
    ls_unique = [dict(item) for item in set_unique]
    print("生成片段{}个，去重后剩余{}个。".format(len(results),len(ls_unique)))


    # 写成json文件
    with open("drug_knowledge片段.jsonl", "w", encoding="utf-8") as f:
        for item in tqdm(ls_unique):
            f.write(json.dumps(item, ensure_ascii=False)+'\n')

    pool.close() # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join() # 等待进程池中的所有进程执行完毕