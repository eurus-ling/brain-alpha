import pandas as pd
import json

def process_alpha_csv_vectorized(input_file, output_file):
    """
    使用向量化方法处理alpha CSV文件
    """
    
    # 读取CSV文件
    df = pd.read_csv(input_file, header=None)
    
    def parse_performance(perf_str):
        """解析performance metrics"""
        try:
            # 添加更健壮的JSON解析
            perf_str_clean = perf_str.replace("'", "\"")
            # 处理可能的None或NaN值
            if pd.isna(perf_str) or perf_str is None:
                return {}
            return json.loads(perf_str_clean)
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {e}, 字符串: {perf_str[:100]}...")
            return {}
        except Exception as e:
            print(f"解析performance时出错: {e}")
            return {}
    
    def parse_settings(settings_str):
        """解析settings并构建新的settings"""
        try:
            settings_str_clean = settings_str.replace("'", "\"")
            settings = json.loads(settings_str_clean)
            # 移除不需要的字段
            fields_to_keep = [
                'instrumentType', 'region', 'universe', 'delay', 'decay',
                'neutralization', 'truncation', 'pasteurization', 'unitHandling',
                'nanHandling', 'language', 'visualization'
            ]
            new_settings = {k: settings.get(k) for k in fields_to_keep if k in settings}
            return str(new_settings).replace("'", "\"")
        except:
            return "{}"
    
    def parse_code(code_str):
        """解析code获取alpha表达式"""
        try:
            code_str_clean = code_str.replace("'", "\"")
            code_data = json.loads(code_str_clean)
            return code_data.get('code', '')
        except:
            return ''

    # def parse_performance(perf_str):
    #     """解析performance metrics"""
    #     try:
    #         return json.loads(perf_str.replace("'", "\""))
    #     except:
    #         return {}
    
    # def parse_settings(settings_str):
    #     """解析settings并构建新的settings"""
    #     try:
    #         settings = json.loads(settings_str.replace("'", "\""))
    #         # 移除不需要的字段
    #         fields_to_keep = [
    #             'instrumentType', 'region', 'universe', 'delay', 'decay',
    #             'neutralization', 'truncation', 'pasteurization', 'unitHandling',
    #             'nanHandling', 'language', 'visualization'
    #         ]
    #         new_settings = {k: settings.get(k) for k in fields_to_keep if k in settings}
    #         return str(new_settings).replace("'", "\"")
    #     except:
    #         return "{}"
    
    # def parse_code(code_str):
    #     """解析code获取alpha表达式"""
    #     try:
    #         code_data = json.loads(code_str.replace("'", "\""))
    #         return code_data.get('code', '')
    #     except:
    #         return ''
    
    def check_conditions(perf_data):
        """检查所有条件"""
        if not perf_data:
            return False
        
        sharpe = perf_data.get('sharpe', 0)
        fitness = perf_data.get('fitness', 0)
        checks = perf_data.get('checks', [])
        
        # 初始化检查结果
        low_turnover_pass = False
        high_turnover_pass = False
        concentrated_weight_pass = False
        low_sub_value = 0
        low_sub_limit = 0
        
        for check in checks:
            name = check.get('name', '')
            result = check.get('result', '')
            value = check.get('value', 0)
            limit = check.get('limit', 0)
            
            if name == 'LOW_TURNOVER' and result == 'PASS':
                low_turnover_pass = True
            elif name == 'HIGH_TURNOVER' and result == 'PASS':
                high_turnover_pass = True
            elif name == 'CONCENTRATED_WEIGHT' and result == 'PASS':
                concentrated_weight_pass = True
            elif name == 'LOW_SUB_UNIVERSE_SHARPE':
                low_sub_value = value
                low_sub_limit = limit
        
        # print(f"Debug - sharpe: {sharpe}, fitness: {fitness}, low_sub_value: {low_sub_value}, low_sub_limit: {low_sub_limit}, low_sub_condition: {low_sub_value * -1 >= -1 * low_sub_limit}")

        return (sharpe <= -1.25 and 
                fitness <= -0.9 and 
                low_turnover_pass and 
                high_turnover_pass and 
                concentrated_weight_pass and 
                (low_sub_value * -1 >= -1 * low_sub_limit))
    
    # 应用条件筛选
    mask = df.iloc[:, 18].apply(
        lambda x: check_conditions(parse_performance(x))
    )
    
    filtered_df = df[mask]
    
    # 构建结果
    result_data = []
    for _, row in filtered_df.iterrows():
        result_data.append({
            'type': 'REGULAR',
            'settings': parse_settings(row.iloc[3]),
            'regular': parse_code(row.iloc[4])
        })
    
    # 保存结果
    if result_data:
        result_df = pd.DataFrame(result_data)
        result_df.to_csv(output_file, index=False)
        print(f"找到 {len(result_data)} 条符合条件的记录，已保存到 {output_file}")
    else:
        print("未找到符合条件的记录")

# 使用示例
if __name__ == "__main__":
    input_csv = "test_alpha_data.csv"  # 替换为您的输入文件路径
    output_csv = "pending_alphas/negative_alphas.csv"  # 输出文件路径
    
    process_alpha_csv_vectorized(input_csv, output_csv)