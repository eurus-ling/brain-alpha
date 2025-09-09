import pandas as pd
import csv
import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth
import os
os.environ['NO_PROXY'] = 'api.worldquantbrain.com'


'''
登录
'''
def sign_in():
    # Load credentials # 加载凭证
    with open(expanduser('config/brain_credentials.txt')) as f:
        credentials = json.load(f)

    # Extract username and password from the list # 从列表中提取用户名和密码
    username, password = credentials

    # Create a session object # 创建会话对象
    sess = requests.Session()

    # Set up basic authentication # 设置基本身份验证
    sess.auth = HTTPBasicAuth(username, password)

    # Send a POST request to the API for authentication # 向API发送POST请求进行身份验证
    response = sess.post('https://api.worldquantbrain.com/authentication')

    # Print response status and content for debugging # 打印响应状态和内容以调试
    print(response.status_code)
    print(response.json())
    return sess




'''
获得想要的数据字段
'''
# 获取数据集ID为fundamental6（Company Fundamental Data for Equity）下的所有数据字段
### Get Data_fields like Data Explorer 获取所有满足条件的数据字段及其ID
def get_datafields(
        s,
        searchScope,
        dataset_id: str = '',
        search: str = ''
):
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']

    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                       "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count']
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" + \
                       f"&search={search}" + \
                       "&offset={x}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])

    datafields_list_flat = [item for sublist in datafields_list for item in sublist]

    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df




'''
将alpha模板的字段组合进去表达式和setting封装
'''
def alpha_setting(akpha_expressions, universes = ['TOP3000'], decays = [0], neutralizations = ['SUBINDUSTRY'], truncations = [0.08]):
    alpha_list = []
    for index, alpha_expression in enumerate(alpha_expressions, start=1):
        for universe in universes:
            for decay in decays:
                for neutralization in neutralizations:
                    for truncation in truncations:
                        simulation_data = {
                            'type': 'REGULAR',
                            'settings': {
                                'instrumentType': 'EQUITY',
                                'region': 'USA',
                                'universe': universe,
                                'delay': 1,
                                'decay': decay,
                                'neutralization': neutralization,
                                'truncation': truncation,
                                'pasteurization': 'ON',
                                'unitHandling': 'VERIFY',
                                'nanHandling': 'OFF',
                                'language': 'FASTEXPR',
                                'visualization': False,
                            },
                            'regular': alpha_expression
                        }
                        alpha_list.append(simulation_data)
    return alpha_list



'''
将alpha_list存到csv文件中
'''
def alpha_to_csv(alpha_list, filename):
    with open(filename, 'w', newline = '') as csvfile:
        fieldnames = ['type', 'settings', 'regular']
        writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
        writer.writeheader()
        for item in alpha_list:
            writer.writerow({
                'type' : item['type'],
                'settings' : item['settings'],
                'regular' : item['regular']
            })




sess = sign_in()
searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'} # 定义搜索范围
fnd6 = get_datafields(s=sess, searchScope=searchScope, dataset_id='fundamental6') # 从数据集中获取数据字段
fnd6 = fnd6[fnd6['type'] == "MATRIX"] # 过滤类型为 "MATRIX" 的数据字段
datafields_list_fnd6 = fnd6['id'].values # 提取数据字段的ID并转换为列表


# #将alpha模板的函数和字段组合
# # 模板<group_compare_op>(<ts_compare_op>(<company_fundamentals>,<days>),<group>)
# group_compare_op = ['group_rank', 'group_zscore', 'group_neutralize']  # 分组比较操作符列表
# ts_compare_op = ['ts_rank', 'ts_zscore', 'ts_av_diff']  # 时间序列比较操作符列表
# company_fundamentals = datafields_list_fnd6
# days = [60, 200] # 定义时间周期列表
# group = ['market', 'industry', 'subindustry', 'sector', 'densify(pv13_h_f1_sector)'] # 定义分组依据列表
# alpha_expressions = []
# for gco in group_compare_op:
#     for tco in ts_compare_op:
#         for cf in company_fundamentals:
#             for d in days:
#                 for grp in group:
#                     alpha_expressions.append(f"{gco}({tco}({cf}, {d}), {grp})")
# print(f"there are total {len(alpha_expressions)} alpha expressions") # 输出生成的alpha表达式总数 # 打印或返回结果字符串列表
alpha_expressions = []
for datafield1 in datafields_list_fnd6:
    alpha_expression = f"group_rank({datafield1}/{datafield1}, subindustry)"
    alpha_expressions.append(alpha_expression)

# universes = ['TOP3000','TOP1000','TOP500','TOP200']
# decays = [0, 1, 5, 20, 60, 120, 250]
# neutralizations = ['NONE', 'MARKET', 'SECTOR', 'INDUSTRY', 'SUBINDUSTRY']
# truncations = [0.01, 0.05, 0.08]
# alpha_list = alpha_setting(alpha_expressions, universes = universes, decays = decays, neutralizations = neutralizations, truncations = truncations)
alpha_list = alpha_setting(alpha_expressions)
print(f"there are {len(alpha_list)} Alphas to simulate")
alpha_to_csv(alpha_list, "pending_alphas/pending_simulated_fnd6_selfRatioRank.csv") #pending_alphas/pending_simulated_dataset_opeartor/idea.csv