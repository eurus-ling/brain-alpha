import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth
from time import sleep, time
from datetime import timedelta
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
os.environ['NO_PROXY'] = 'api.worldquantbrain.com'


# ---------------- 登录函数 ---------------- #
def sign_in():
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)
    username, password = credentials
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    response = sess.post('https://api.worldquantbrain.com/authentication')
    if response.status_code == 201:
        logging.info("登录成功")
    else:
        logging.error(f"登录失败: {response.status_code} {response.text}")
    return sess


# ---------------- 日志配置 ---------------- #
logging.basicConfig(
    filename='simulation.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)


# ---------------- 单个 Alpha 运行函数 ---------------- #
def run_single_alpha(idx, total, alpha, sess, check_interval=40, fail_tolerance=15, min_check_interval=40):
    start_alpha = time()
    failure_count = 0
    sim_progress_url = None

    # ---- 获取 location ----
    while True:
        try:
            sim_resp = sess.post('https://api.worldquantbrain.com/simulations', json=alpha)
            sim_progress_url = sim_resp.headers['Location']
            break
        except Exception as e:
            failure_count += 1
            logging.warning(f"Alpha {idx}/{total} 获取 location 失败 ({failure_count}/{fail_tolerance})，15s后重试... 错误: {str(e)}")
            sleep(15)
            if failure_count >= fail_tolerance:
                sess = sign_in()
                logging.error(f"Alpha {idx}/{total} 超过失败次数上限，跳过。公式: {alpha['regular']}")
                return None

    logging.info(f"开始处理 Alpha {idx}/{total} (进度 {idx/total*100:.2f}%)")

    # ---- 检查进度 ----
    while True:
        sim_progress_resp = sess.get(sim_progress_url)
        retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", check_interval))
        wait_sec = max(retry_after_sec, min_check_interval)
        if wait_sec == 0:  # 任务完成
            break
        logging.info(f"[INFO] Alpha {idx}/{total} 正在运行中... 下次检查 {int(wait_sec)} 秒后")
        sleep(wait_sec)

    # ---- 获取结果 ----
    try:
        alpha_id = sim_progress_resp.json().get("alpha", "UNKNOWN")
        end_alpha = time()
        elapsed = timedelta(seconds=int(end_alpha - start_alpha))
        logging.info(f"完成 Alpha {idx}/{total} ✅ ID={alpha_id} (耗时 {elapsed})")
        return {
            "index": idx,
            "alpha_expr": alpha['regular'],
            "alpha_id": alpha_id,
            "elapsed": str(elapsed)
        }
    except Exception as e:
        logging.error(f"Alpha {idx}/{total} 获取结果失败: {str(e)}")
        return None


# ---------------- 批量 Alpha 并发运行 ---------------- #
def run_simulations(alpha_list, sess, max_workers=3, results_file="simulation_results.csv"):
    total = len(alpha_list)
    logging.info(f"开始批量运行 {total} 个 Alphas (并发={max_workers})")

    # ---- 断点续跑：加载已完成的结果 ----
    finished_exprs = set()
    if os.path.exists(results_file):
        old_df = pd.read_csv(results_file)
        finished_exprs = set(old_df['alpha_expr'].tolist())
        logging.info(f"检测到已有 {len(finished_exprs)} 个 Alpha 已完成，自动跳过")
    else:
        old_df = pd.DataFrame(columns=["index", "alpha_expr", "alpha_id", "elapsed"])

    # ---- 过滤未完成的 ----
    pending_alphas = [
        (idx, alpha) for idx, alpha in enumerate(alpha_list, start=1)
        if alpha['regular'] not in finished_exprs
    ]
    logging.info(f"需要运行剩余 {len(pending_alphas)} 个 Alphas")

    if not pending_alphas:
        logging.info("所有 Alpha 已完成 ✅ 无需继续运行")
        return

    start_all = time()
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(run_single_alpha, idx, total, alpha, sess, min_check_interval=40): idx
            for idx, alpha in pending_alphas
        }

        for future in as_completed(future_to_idx):
            res = future.result()
            if res:
                results.append(res)
                # 追加写入 CSV
                pd.DataFrame([res]).to_csv(results_file, index=False, mode="a", header=not os.path.exists(results_file))

            # 计算ETA
            done = len(finished_exprs) + len(results)
            elapsed_total = time() - start_all
            avg_time_per_alpha = elapsed_total / len(results) if len(results) > 0 else 0
            eta = timedelta(seconds=int(avg_time_per_alpha * (total - done)))
            logging.info(f"已完成 {done}/{total} (进度 {done/total*100:.2f}%)，预计剩余 {eta}")

    logging.info("全部完成 ✅")


# 获取数据集ID为fundamental6（Company Fundamental Data for Equity）下的所有数据字段
### Get Data_fields like Data Explorer 获取所有满足条件的数据字段及其ID
def get_datafields(
        s,
        searchScope,
        dataset_id: str = '',
        search: str = ''
):
    import pandas as pd
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


sess = sign_in()

# 定义搜索范围
searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
# 从数据集中获取数据字段
fnd6 = get_datafields(s=sess, searchScope=searchScope, dataset_id='fundamental6')
# 过滤类型为 "MATRIX" 的数据字段
fnd6 = fnd6[fnd6['type'] == "MATRIX"]
# 提取数据字段的ID并转换为列表
datafields_list_fnd6 = fnd6['id'].values
# 输出数据字段的ID列表
print(datafields_list_fnd6)
print(len(datafields_list_fnd6))


# 将datafield和operator替换到Alpha模板(框架)中group_rank(ts_rank({fundamental model data},252),industry),批量生成Alpha
# 模板<group_compare_op>(<ts_compare_op>(<company_fundamentals>,<days>),<group>)
# 定义分组比较操作符
group_compare_op = ['group_rank', 'group_zscore', 'group_neutralize']  # 分组比较操作符列表
# 定义时间序列比较操作符
ts_compare_op = ['ts_rank', 'ts_zscore', 'ts_av_diff']  # 时间序列比较操作符列表
# 定义公司基本面数据的字段列表
company_fundamentals = datafields_list_fnd6
# 定义时间周期列表
days = [60, 200]
# 定义分组依据列表
group = ['market', 'industry', 'subindustry', 'sector', 'densify(pv13_h_f1_sector)']
# 初始化alpha表达式列表
alpha_expressions = []
# 遍历分组比较操作符
for gco in group_compare_op:
    # 遍历时间序列比较操作符
    for tco in ts_compare_op:
        # 遍历公司基本面数据的字段
        for cf in company_fundamentals:
            # 遍历时间周期
            for d in days:
                # 遍历分组依据
                for grp in group:
                    # 生成alpha表达式并添加到列表中
                    alpha_expressions.append(f"{gco}({tco}({cf}, {d}), {grp})")

# 输出生成的alpha表达式总数 # 打印或返回结果字符串列表
print(f"there are total {len(alpha_expressions)} alpha expressions")

# 打印结果
print(alpha_expressions[:5])
print(len(alpha_expressions))

# 将datafield替换到Alpha模板(框架)中group_rank({fundamental model data}/cap,subindustry)批量生成Alpha
alpha_list = []

for index, alpha_expression in enumerate(alpha_expressions, start=1):
    # print(f"正在循环第 {index} 个元素")
    # print("正在将如下alpha表达式与setting封装")
    simulation_data = {
        "type": "REGULAR",
        "settings": {
            "instrumentType": "EQUITY",
            "region": "USA",
            "universe": "TOP3000",
            "delay": 1,
            "decay": 0,
            "neutralization": "SUBINDUSTRY",
            "truncation": 0.01,
            "pasteurization": "ON",
            "unitHandling": "VERIFY",
            "nanHandling": "OFF",
            "language": "FASTEXPR",
            "visualization": False,
        },
        "regular": alpha_expression
    }
    alpha_list.append(simulation_data)
    print(f"there are {len(alpha_list)} Alphas to simulate")

# 输出
print(alpha_list[0])


run_simulations(alpha_list, sess, max_workers=3, results_file="simulation_results.csv")
