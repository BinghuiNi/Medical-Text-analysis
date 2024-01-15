import json
import re
import pandas as pd
from datetime import datetime
file = open(r"C:\Users\nibh\Desktop\科大讯飞\2023.11.24_吕梁市医保局-爬虫文件.txt", "r", encoding="utf-8")
lines = file.readlines()

keywords = ['医疗保险','医疗','医保','医药','三医','卫生健康','DRG','医疗保障','医院','医改']
titles = []
contents = []
links = []
for line in lines:
    if len(str(line.strip()))<400:
        continue
    else:
        title = str(json.loads(line).get('title'))
        contains_keywords = any(keyword in title for keyword in keywords) if title else False
        if contains_keywords:
            titles.append(title)
            content = str(json.loads(line).get('content'))
            contents.append(content)
            link = json.loads(line).get('id')
            links.append(link)

def format_date(time):
    try:
        return datetime.strptime(time,'%Y年%m月%d日').strftime('%Y-%m-%d')
    except ValueError:
        return time
    
df = pd.DataFrame(columns=['标题','时间','内容','网站链接'])

title_pattern = re.compile(r"\['(.*?)'\]",re.DOTALL)
content_pattern = re.compile(r"网站地图\n(.*?)\n首页",re.DOTALL)
time_pattern = re.compile(r"(更新时间|发布日期)：(.*?)\n",re.DOTALL)

for i in range(len(titles)): 
    try:
        cleaned_title = title_pattern.findall(titles[i])[0]
    
        content1 = re.sub(r"\s+",'',contents[i]) #去除空格
        content2 = re.sub(r"\\u\d+",'',content1) #去除\\u3000等Unicode 编码空格
        content3 = re.sub(r"(\\n)+",'\n',content2) #去除连续换行
        content4 = content3.replace('\\n','\n').replace('\\xa0','')
        cleaned_content = content_pattern.findall(content4)[0]
    
        time = format_date(time_pattern.findall(cleaned_content)[0][1])
    
        df.loc[i] = [cleaned_title, time, cleaned_content, links[i]]
        
    except IndexError:
        df.loc[i] = [cleaned_title, '', '', links[i]]
