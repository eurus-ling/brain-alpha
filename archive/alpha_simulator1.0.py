'''
Alpha Simulator 路线图 with「随时停掉, 随时重启, 接着跑」

pending: 还没开始跑的 alphs
simulated_alphas_xxx.csv: 已经完成的alpha
active_simulations.csv: 正在运行的任务
alpha_list_pending_simulated.csv: 待运行的alphas, 会从pending中删除该alpha
一旦完成就移除。
fail_alphas.csv: 失败的alphas

程序重启时：

先读 simulated_alphas_xxx.csv → 生成已完成列表。

先读 fail_alphas.csv → 生成失败列表。

先读 active_simulations.csv → 继续追踪没完成的任务。

再读 alpha_list_pending_simulated.csv → 只加载未完成、未失败的 alphas。

运行时：

启动新的 alpha 时，把它的配置和 URL 写进 active_simulations.csv。

完成后，移动到 simulated_alphas_xxx.csv 并从 active_simulations.csv 里删除。

失败时，写进 fail_alphas.csv。

1. 初始化alphasimulator
    - 使用以下参数初始化类
        - max_concurrent: 允许的最大并发回测数
        - username passwor: API认证所需的登录凭证
        - alpha_list_file_path: 包含待回测alphas的csv文件路径
    - 使用sign in进行会话初始化
2. 登录过程
    - 尝试API登录认证
    - 如果成功
        - 继续管理回测任务
    - 如果失败
        记录错误日志并停止流程
3. 主要回测管理循环
    - 持续管理回测任务，包含以下步骤：
        1. 检查回测状态
            - 检查每个活动回测的当前状态
            - 如果有任何回测已完成，记录结果并将其从活动列表中移除
            - 如果没有任何活动回测, 则记录日志消息并继续加载新的alpha
        2. 加载新alpha并进行回测
            - 如果sim_queue_ls为空, 则从csv文件中重新填充
            - 如果当前回测数未达到max_concurrent上限, 从sim_queue_ls中弹出一个alpha并开始回测
        3. 回测alpha
            - 尝试继续alpha回测
            - 错误处理: 如果失败，重试直到达到限制次数
            - 成功时: 记录该回测的location URL
            - 超过最大重试次数: 将alpha记录为失败, 并写入fail_alphas.csv
    4. 结束条件
        - 无活动回测: 记录日志消息, 显示空闲状态
        - 达到错误阈值时: 记录错误详情并尝试重新登陆
        - 会话认证失败: 若无法重新认证则正常退出流程
'''





import pandas as pd
import csv
import requests
import json
import logging
import time
import ast
import os
from datetime import datetime
from pytz import timezone
from os.path import expanduser
from requests.auth import HTTPBasicAuth
from logging.handlers import TimedRotatingFileHandler
import os
os.environ['NO_PROXY'] = 'api.worldquantbrain.com'



with open(expanduser('config/brain_credentials.txt')) as f:
    credentials = json.load(f)
username, password = credentials

eastern = timezone('US/Eastern') # 获取美东时间
fmt = '%Y-%m-%d'
loc_dt = datetime.now(eastern)
print("Current time in Eastern is", loc_dt.strftime(fmt))

from pytz import timezone
from logging.handlers import BaseRotatingHandler

# 自定义美东时间轮换处理器
class EasternTimeRotatingFileHandler(BaseRotatingHandler):
    def __init__(self, filename_template, backupCount=30):
        self.eastern = timezone('US/Eastern')
        self.filename_template = filename_template  # 传入带占位符的模板，如"logs/simulation_{}.log"
        self.backupCount = backupCount
        self.current_date = self._get_current_eastern_date()
        self.baseFilename = self._get_current_filename()
        # 确保日志目录存在
        os.makedirs(os.path.dirname(self.baseFilename), exist_ok=True)
        super().__init__(self.baseFilename, 'a', encoding='utf-8')

    def _get_current_eastern_date(self):
        return datetime.now(self.eastern).strftime('%Y-%m-%d')

    def _get_current_filename(self):
        return self.filename_template.format(self.current_date)

    def shouldRollover(self, record):
        # 检查美东时间是否已跨天
        new_date = self._get_current_eastern_date()
        if new_date != self.current_date:
            self.current_date = new_date
            return True
        return False

    def doRollover(self):
        # 关闭当前日志文件
        self.stream.close()
        
        # 处理旧日志（确保在logs目录内）
        old_filename = self.baseFilename
        if self.backupCount > 0:
            # 无需重命名，直接使用日期命名
            pass
        
        # 更新文件名并打开新文件
        self.baseFilename = self._get_current_filename()
        self.stream = self._open()

        # 删除超出保留数量的旧日志
        self._cleanup_old_logs()

    def _cleanup_old_logs(self):
        # 获取所有日志文件并按日期排序
        log_dir = os.path.dirname(self.baseFilename)
        log_files = [f for f in os.listdir(log_dir) if f.startswith('simulation_') and f.endswith('.log')]
        log_files.sort(reverse=True)  # 最新的在前
        
        # 删除多余日志
        if len(log_files) > self.backupCount:
            for file in log_files[self.backupCount:]:
                os.remove(os.path.join(log_dir, file))

# 配置日志按美东时间按天分割
def setup_logging():
    # 日志格式（包含美东时间）
    log_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 创建自定义处理器（指定日志路径模板）
    handler = EasternTimeRotatingFileHandler(
        filename_template="logs/simulation_{}.log",
        backupCount=30  # 保留30天的日志
    )
    handler.setFormatter(log_format)
    
    # 获取根日志器并配置
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # 移除现有处理器避免重复输出
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(handler)
    
    # 添加控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

# 初始化日志
setup_logging()


with open(expanduser('config/brain_credentials.txt')) as f:
    credentials = json.load(f)
username, password = credentials

eastern = timezone('US/Eastern') # 获取美东时间
fmt = '%Y-%m-%d'
loc_dt = datetime.now(eastern)
print("Current time in Eastern is", loc_dt.strftime(fmt))

class AlphaSimulator:
    def __init__(self, max_concurrent, username, password, alpha_list_file_path, batch_numer_for_every_queue):
        self.fail_alphas = 'progress_alphas/fail_alphas_csv'
        self.simulated_alphas = f'simulated_alphas/simulated_alphas_{loc_dt.strftime(fmt)}.csv'
        self.max_concurrent = max_concurrent
        self.active_simulations = []
        self.username = username
        self.password = password
        self.session = self.sign_in(username, password)
        self.alpha_list_file_path = alpha_list_file_path
        self.sim_queue_ls = []
        self.batch_numer_for_every_queue = batch_numer_for_every_queue
    
    def sign_in(self, username, password):
        s = requests.Session()
        s.auth = (username, password)
        count = 0
        count_limit = 30
        while True:
            try:
                response = s.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                break
            except:
                count += 1
                logging.error("Connection down, try to login again...")
                time.sleep(1)
                if count > count_limit:
                    logging.error(f"{username} failed too many times, returning None.")
                    return None
        logging.info("Login to BRAIN successfully.")
        return s
    
    def read_alphas_frm_csv_in_batches(self, batch_size = 50):
        '''
        1. d打开alpha_list_pending_simulated
        2. 去除batch_size个alpha放入列表变量alphas
        3. 取出后覆写(overwrite) alpha_list_pending_simulated
        4. 把取出的alphas写到sim_queue.csv文件中,方便随时监控在排队的alpha有多少
        5. 返回列表变量alphas
        '''
        alphas = []
        temp_file_name = self.alpha_list_file_path + '.tmp'
        with open(self.alpha_list_file_path, 'r') as file, open(temp_file_name, 'w', newline = '') as temp_file:
            reader = csv.DictReader(file)
            fieldnemas = reader.fieldnames
            writer = csv.DictWriter(temp_file, fieldnames = fieldnemas)
            writer.writeheader()
            for _ in range(batch_size):
                try:
                    row = next(reader)
                    if 'settings' in row:
                        if isinstance(row['settings'], str):
                            try:
                                row['settings'] = ast.literal_eval(row['settings'])
                            except (ValueError, SyntaxError):
                                print(f"Error wvaluating settings: {row['settings']}")
                        elif isinstance(row['settings'], dict):
                            pass
                        else:
                            print(f"Unexcepted type for settings:{type(row['settings'])}")
                    alphas.append(row)
                except StopIteration:
                    break
            for remaining_row in reader:
                writer.writerow(remaining_row)
        os.replace(temp_file_name, self.alpha_list_file_path)
        if alphas:
            with open('sim_queue.csv', 'w') as file:
                writer = csv.DictWriter(file, fieldnames = alphas[0].keys())
                if file.tell() == 0:
                    writer.writeheader()
                writer.writerows(alphas)
        return alphas
    
    def simulate_alpha(self, alpha):
        count = 0
        while True:
            try:
                response = self.session.post('https://api.worldquantbrain.com/simulations', json = alpha)
                response.raise_for_status
                if "location" in response.headers:
                    logging.info("Alpha location retrieved successfully.")
                    logging.info(f"Location:{response.headers['Location']}")
                    return response.headers['Location']
            except requests.exceptions.RequestException as e:
                logging.error(f"Error in sending simualtion request:{e}")
                if count > 35:
                    self.session = self.sign_in(self.username, self.password)
                    logging.error("Error occured too many times, skipping this alpha and re logging in.")
                    break
                logging.error("Error in sending simulation request. Retry after 5s...")
                time.sleep(5)
                count += 1
        logging.error("Simulation request failed after {count} attempts")
        with open(self.fail_alphas, 'a', newline = '') as file:
            writer = csv.DictWriter(file, fieldnames = alpha.keys())
            writer.writerow(alpha)
        return None
    
    def load_new_alpha_and_simulate(self):
        if len(self.sim_queue_ls) < 1:
            self.sim_queue_ls = self.read_alphas_frm_csv_in_batches(self.batch_numer_for_every_queue)

        if len(self.active_simulations) >= self.max_concurrent:
            logging.info(f"Max concurrent simulations reached({self.max_concurrent}). Waiting 2 seconds")
            time.sleep(2)
            return
        
        logging.info("Loadin new alpha...")
        try:
            alpha = self.sim_queue_ls.pop(0)
            logging.info(f"Strating simulation for alpha: {alpha['regular']} with settings: {alpha['settings']}")
            location_url = self.simulate_alpha(alpha)
            if location_url:
                self.active_simulations.append(location_url)
        except IndexError:
            logging.info("No alphas available in the queue.")

    def check_simulation_porgress(self, simulation_progress_url):
        try:
            simulation_progress = self.session.get(simulation_progress_url)
            simulation_progress.raise_for_status()
            if simulation_progress.headers.get("Rety-After", 0) == 0:
                alpha_id = simulation_progress.json().get("alpha")
                if alpha_id:
                    alpha_response = self.session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                    alpha_response.raise_for_status
                    return alpha_response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching simulation progress: {e}")
            self.session = self.sign_in(self.username, self.password)
            return None
        
    def check_simulation_status(self):
        count = 0
        if len(self.active_simulations) == 0:
            logging.info("No one is in active simulation row.")
            return None
        for sim_url in self.active_simulations:
            sim_progress = self.check_simulation_porgress(sim_url)
            if sim_progress is None:
                count += 1
                continue
            alpha_id = sim_progress.get("id")
            status = sim_progress.get("status")
            logging.info(f"Alpha id: {alpha_id} ended with status: {status}. Removing from active list.")
            self.active_simulations.remove(sim_url)
            with open(self.simulated_alphas, 'a', newline = '') as file:
                writer = csv.DictWriter(file, fieldnames = sim_progress.keys())
                writer.writerow(sim_progress)
        logging.info(f"Total {count} simulations are in progress for account {self.username}")

    def manage_simulations(self):
        if not self.session:
            logging.error("Failed to sign in. Exiting...")
            return
        while True:
            self.check_simulation_status()
            self.load_new_alpha_and_simulate()
            time.sleep(3)

#example usage
alpha_list_file_path = 'pending_alphas/pending_simulated_fnd6_ratioRank_shuffled.csv'
simulator = AlphaSimulator(max_concurrent = 3, username = username, password = password, alpha_list_file_path = alpha_list_file_path, batch_numer_for_every_queue = 20)
simulator.manage_simulations()