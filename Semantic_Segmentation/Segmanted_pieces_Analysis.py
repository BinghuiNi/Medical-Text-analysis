import pandas as pd
df_drug = pd.read_csv(r'C:\Users\nibh\Desktop\科大讯飞\drug_knowledge.csv')

### 一、2.（1）统计数据量
amount = len(df_drug)
unique_amount = len(df_drug.groupby(["id","name"]))
ls_col = df_drug.columns
name_dict = {"id": "id", "name": "药物名称", "english_name": "英文名称", "drug_sort": "药理分类",
             "standard_name": "标准名称",
             "commodity_name": "商品名", "common_name": "通用名", "specifications": "规格", "component": "成份",
             "appearance": "性状",
             "indication": "适应症", "contraindications": "禁忌证", "usage": "用法用量", "over_dosage": "药物过量",
             "adverse_reactions": "不良反应", "use_in_elderly": "老年用药", "use_in_children": "儿童用药",
             "use_in_pregnant": "孕妇及哺乳期妇女用药", "pharmacokinetics": "药代动力学",
             "relative_effection": "药物相互作用",
             "pharmacological_effects": "药理作用", "attention": "注意事项"}
print(amount, unique_amount)

### 一、2.（2）分片段统计数据量
df = pd.DataFrame(columns=["name", "non-missing count", "unique count"])
for i in ls_col:
    name = name_dict[i]
    non_missing_count = len(df_drug[i].dropna())
    unique_count = len(df_drug[i].dropna().unique())
    df = pd.concat([df, pd.DataFrame({"name": [name], "non-missing count": [non_missing_count], "unique count": [unique_count]})], ignore_index=True)

import openpyxl
df.to_excel ('分析.xlsx')

### 一、2.（3）分片段统计文本长度分布
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib
matplotlib.rc("font",family='FangSong')  # 设置中文字体

for i in range(9, len(ls_col)):
    text_column = df_drug[ls_col[i]].dropna()
    text_lengths = text_column.str.len()

    fig, axs = plt.subplots(1, 2, figsize=(15, 6))

    # 箱型图
    sns.boxplot(text_lengths, ax=axs[0])
    axs[0].set_title(f"片段名：{name_dict[ls_col[i]]}\n文本长度箱型图")
    axs[0].set_ylabel("文本长度")
    axs[0].text(x=0.82, y=0.8,
                s=f'Mean: {text_lengths.mean():.2f}\nMedian: {text_lengths.median()}\nStd Dev: {text_lengths.std():.2f}\nMax: {text_lengths.max():.2f}\nMin: {text_lengths.min()}\nSkewness: {text_lengths.skew():.2f}\nKurtosis: {text_lengths.kurt():.2f}',
                bbox=dict(facecolor='white', alpha=0.5),
                transform=axs[0].transAxes)

    # 去离群值后的频数直方图
    Q1 = text_lengths.quantile(0.25)
    Q3 = text_lengths.quantile(0.75)
    IQR = Q3 - Q1
    upper_limit = Q3 + 1.5 * IQR
    filtered_data = text_lengths[text_lengths <= upper_limit]

    sns.histplot(filtered_data, bins=20, kde=True, ax=axs[1])
    axs[1].set_title(f"片段名：{name_dict[ls_col[i]]}\n去离群值后的频数直方图")
    axs[1].set_xlabel("文本长度")
    axs[1].set_ylabel("频数")

    plt.tight_layout()
    plt.savefig(f"{i}.片段名：{name_dict[ls_col[i]]}_分布图.png")
    # plt.show()


### 四、2.切分结果分析
import jsonlines
# file_path = r'C:\Users\nibh\Desktop\科大讯飞\drug_knowledge片段.jsonl'
file_path = r'C:\Users\nibh\Desktop\科大讯飞\drug_knowledge片段——再切分.jsonl'
if __name__ == "__main__":
    len_english_name, len_drug_sort, len_standard_name, len_commodity_name, len_common_name,\
    len_specifications, len_component, len_appearance, len_indication, len_contraindications, len_usage,\
    len_over_dosage, len_adverse_reactions, len_use_in_elderly, len_use_in_children, len_use_in_pregnant,\
    len_pharmacokinetics, len_relative_effection, len_pharmacological_effects, len_attention\
    = [],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]

    with jsonlines.open(file_path) as reader:
        for entry in reader: # entry是文件里的一行，即一个字典
            name = entry["字段名"]
            segment = entry["子片段"]
            if name == "英文名称":
                len_english_name.append(len(segment))
            elif name == "药理分类":
                len_drug_sort.append(len(segment))
            elif name == "标准名称":
                len_standard_name.append(len(segment))
            elif name == "商品名":
                len_commodity_name.append(len(segment))
            elif name == "通用名":
                len_common_name.append(len(segment))
            elif name == "规格":
                len_specifications.append(len(segment))
            elif name == "成份":
                len_component.append(len(segment))
            elif name == "性状":
                len_appearance.append(len(segment))
            elif name == "适应症":
                len_indication.append(len(segment))
            elif name == "禁忌证":
                len_contraindications.append(len(segment))
            elif name == "用法用量":
                len_usage.append(len(segment))
            elif name == "药物过量":
                len_over_dosage.append(len(segment))
            elif name == "不良反应":
                len_adverse_reactions.append(len(segment))
            elif name == "老年用药":
                len_use_in_elderly.append(len(segment))
            elif name == "儿童用药":
                len_use_in_children.append(len(segment))
            elif name == "孕妇及哺乳期妇女用药":
                len_use_in_pregnant.append(len(segment))
            elif name == "药代动力学":
                len_pharmacokinetics.append(len(segment))
            elif name == "药物相互作用":
                len_relative_effection.append(len(segment))
            elif name == "药理作用":
                len_pharmacological_effects.append(len(segment))
            elif name == "注意事项":
                len_attention.append(len(segment))

len_all = len_english_name + len_drug_sort + len_standard_name + len_commodity_name\
          + len_common_name + len_specifications + len_component + len_appearance + len_indication\
          + len_contraindications + len_usage + len_over_dosage + len_adverse_reactions + len_use_in_elderly\
          + len_use_in_children + len_use_in_pregnant + len_pharmacokinetics + len_relative_effection\
          + len_pharmacological_effects + len_attention

ls_all = [len_all, len_english_name, len_drug_sort, len_standard_name, len_commodity_name,
          len_common_name, len_specifications, len_component, len_appearance, len_indication,
          len_contraindications, len_usage, len_over_dosage, len_adverse_reactions, len_use_in_elderly,
          len_use_in_children, len_use_in_pregnant, len_pharmacokinetics, len_relative_effection,
          len_pharmacological_effects, len_attention]
name_dict = ["所有子片段", "英文名称", "药理分类", "标准名称", "商品名", "通用名", "规格", "成份",
             "性状", "适应症", "禁忌证", "用法用量", "药物过量", "不良反应", "老年用药", "儿童用药",
             "孕妇及哺乳期妇女用药", "药代动力学", "药物相互作用", "药理作用", "注意事项"]

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rc("font",family='FangSong')

for i in range(len(ls_all)):
    text_lengths = pd.Series(ls_all[i])
    fig, axs = plt.subplots(1, 2, figsize=(15, 6))
    # 箱型图
    sns.boxplot(text_lengths, ax=axs[0])
    axs[0].set_title(f"片段名：{name_dict[i]}\n片段长度箱型图")
    axs[0].set_ylabel("片段长度")
    axs[0].text(x=0.82, y=0.8,
                s=f'Mean: {text_lengths.mean():.2f}\nMedian: {text_lengths.median()}\nStd Dev: {text_lengths.std():.2f}\nMax: {text_lengths.max():.2f}\nMin: {text_lengths.min()}\nSkewness: {text_lengths.skew():.2f}\nKurtosis: {text_lengths.kurt():.2f}',
                bbox=dict(facecolor='white', alpha=0.5),
                transform=axs[0].transAxes)

    # 去离群值后的频数直方图
    Q1 = text_lengths.quantile(0.25)
    Q3 = text_lengths.quantile(0.75)
    IQR = Q3 - Q1
    upper_limit = Q3 + 1.5 * IQR
    filtered_data = text_lengths[text_lengths <= upper_limit]

    sns.histplot(filtered_data, bins=20, kde=True, ax=axs[1])
    axs[1].set_title(f"片段名：{name_dict[i]}\n去离群值后的频数直方图")
    axs[1].set_xlabel("片段长度")
    axs[1].set_ylabel("频数")

    plt.tight_layout()
    plt.savefig(f"{i}.{name_dict[i]}_分布图.png")
    # plt.show()

# 提取异常值
ls_outlier = []
with jsonlines.open(file_path) as reader:
    for entry in reader:
        if entry["字段名"] in ["性状", "适应症", "禁忌证", "用法用量", "药物过量", "不良反应", "老年用药", "儿童用药","孕妇及哺乳期妇女用药",
                               "药代动力学", "药物相互作用", "药理作用", "注意事项"] and len(entry["子片段"]) > 350:
            ls_outlier.append({
                "id": entry["id"],
                "药物名称": entry["药物名称"],
                "字段名": entry["字段名"],
                "子片段长度": len(entry['子片段']),
                "子片段": entry["子片段"]
            })
pd.DataFrame(ls_outlier).to_csv("切分后的异常值.csv", index=False)

### 再切分
with jsonlines.open(file_path) as reader:
    ls_add = []
    for entry in reader:
        if (entry["字段名"] in ["性状", "适应症", "禁忌证", "用法用量", "药物过量", "不良反应", "老年用药", "儿童用药","孕妇及哺乳期妇女用药", "药代动力学", "药物相互作用", "药理作用", "注意事项"]) and (len(entry["子片段"]) > 350):
            all_sens = split(entry["子片段"])
            for cut in all_sens:
                item = {"表名": "drug_knowledge",
                        "id": f"{entry['id']}",
                        "药物名称": f"{entry['药物名称']}",
                        "字段名": f"{entry['字段名']}",
                        "子片段": f"{cut}",
                        "名称+字段名": f"{entry['药物名称']}的{entry['字段名']}",
                        "名称+字段名+子片段": f"{entry['药物名称']}的{entry['字段名']}是{cut}"}
                ls_add.append(item)
        else:
            ls_add.append(entry)

set_add = set(tuple(d.items()) for d in ls_add)
unique_add = [dict(item) for item in set_add]
print("生成片段{}个，去重后剩余{}个。".format(len(ls_add),len(unique_add)))

with open("drug_knowledge片段——再切分.jsonl", "w", encoding="utf-8") as f:
    for item in tqdm(unique_add):
        f.write(json.dumps(item, ensure_ascii=False)+'\n')