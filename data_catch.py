def pulldata_from_solr():
    import requests
    import json
    import re
    a,b=0,0
    # 查询网址
    query_string = "*gov.cn*"
    # 构建请求URL
    request_url = f"http://xxx/solr/nutch/select?indent=true&q.op=OR&q=id%3A{query_string}&rows=100000"
    # 发送GET请求
    response = requests.get(request_url, verify=False)
    print(response.text)
    fout = open('policies-meta.jsonl','w',encoding='utf-8')
    # 检查响应状态码
    if response.status_code == 200:
        # 解析响应内容（JSON格式）
        response_json = json.loads(response.text)
        # 处理响应数据
        docs = response_json["response"]["docs"]
        for doc in docs:
            a+=1
            #print(doc)
            if doc.get('content'):
                content = str(doc.get('content')[0])
                id = str(doc.get('id'))
                digest = str(doc.get('digest')[0])
                item = {"meta": {"filename": id,
                                 "id": digest}, "content": content}
                fout.write(json.dumps(item, ensure_ascii=False)+'\n')
                b+=1
    else:
        print("请求失败:", response.status_code)
    print(a,b)
pulldata_from_solr()