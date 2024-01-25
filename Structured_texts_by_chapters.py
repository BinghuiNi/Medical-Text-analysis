import re
import pandas as pd
import json

def cut_c(c):
    bg_newc = []
    try:
        po = re.search(
            '第.条|第..条|第...条',
            c)
        beijing = c[:po.span()[0]] # return backgroud of articles
        newc = c[po.span()[0]:] # return content of articles
        for i in re.split('第.条|第..条|第...条', newc):
            if len(i.strip()) > 0:
                bg_newc.append(beijing + '\n' + i)
    except:
        try:
            po = re.search('\(.\)|\(..\)|（.）|（..）', c)
            beijing = c[:pos.span()[0]]
            newc = c[pos.span()[0]:]
            for i in re.split('\(.\)|\(..\)|（.）|（..）', newc):
                if len(i.strip())>0:
                    bg_newc.append(beijing + '\n' + i)
        except:
            try:
                po = re.search(
                    '\d、|\d\d、',
                    c)
                beijing = c[:pos.span()[0]]
                newc = c[pos.span()[0]:]
                for i in re.split('\d、|\d\d、',newc):
                    bg_newc.append(beijing + '\n' + i)
            except:
                bg_newc.append(c)
    return bg_newc


# Segment long texts by chapters
def cut_main(content):
    return_content = []
    pos = re.search('一、',content)
    if pos:
        background = content[:pos.span()[0]]
        main_text = content[pos.span()[0]:]
        ls_text = re.split('一、|二、|三、|四、|五、|六、|七、|八、|九、|十、|十一、|十二、|十三、|十四、|十五、', re.sub(' ', '', main_text.strip()))
    elif re.search('第一章',content):
        pos = re.search('第一章',content)
        background = content[:pos.span()[0]]
        main_text = content[pos.span()[0]:]
        ls_text = re.split('第一章|第二章|第三章|第四章|第五章|第六章|第七章|第八章|第九章|第十章|第十一章', re.sub(' ', '', main_text.strip()))
    elif re.search('^\(一\)|\(二\)|\(三\)|\(四\)|\(五\)|\(六\)|\(七\)|\(八\)|\(九\)|\(十\)|\(十一\)|\(十二\)|\(十三\)|（一）|（二）|（三）|（四）|（五）|（六）|（七）|（八）|（九）|（十）|（十二）|（十三）',content):
        pos = re.search('^\(一\)|\(二\)|\(三\)|\(四\)|\(五\)|\(六\)|\(七\)|\(八\)|\(九\)|\(十\)|\(十一\)|\(十二\)|\(十三\)|（一）|（二）|（三）|（四）|（五）|（六）|（七）|（八）|（九）|（十）|（十二）|（十三）',content)
        background = content[:pos.span()[0]]
        main_text = content[pos.span()[0]:]
        ls_text = re.split('\(一\)|\(二\)|\(三\)|\(四\)|\(五\)|\(六\)|\(七\)|\(八\)|\(九\)|\(十\)|\(十一\)|\(十二\)|\(十三\)|（一）|（二）|（三）|（四）|（五）|（六）|（七）|（八）|（九）|（十）|（十二）|（十三）', re.sub(' ', '', main_text.strip()))
    elif re.search('^第.条|第..条|第...条',main_text):
        ls_text = re.split('第.条|第..条|第...条', re.sub(' ', '', main_text.strip()))
    else:
        ls_text = main_text.strip()
    
    for c in ls_text:
        c = re.sub('\s+', '', c)
        if len(c) > 700: 
            for i in cut_c(c):
                return_content.append(i)
        elif len(c)>0:
            return_content.append(c)
    
    return background, return_content


df = pd.read_excel(r'path\xxx.xls',sheet_name='Sheet2',header=None)
ls_info = []
for i in range(len(df)):
    background, return_content = cut_main(df.iloc[i,3])
    for text in return_content:
        item = {"Title": f"{df.iloc[i,0]}",
                "Time": f"{df.iloc[i,1]}",
                "No.": f"{df.iloc[i,2]}",
                "Background": background,
                "Content":f"{df.iloc[i,3]}" ,
                "Content_piece":text}
        ls_info.append(item)

with open("xxx.jsonl", "w", encoding="utf-8") as f:
    for item in ls_info:
        f.write(json.dumps(item, ensure_ascii=False)+'\n')
