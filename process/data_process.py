import math
import matplotlib.pyplot as plt
import pandas as pd
plt.style.use('fivethirtyeight')
# TODO
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)

def statistics(data, count):
    """
    去除一些特殊的列
    :param data 原始数据
    :param count: 阈值，低于这个数量的Job会被删除
    """
    number = {}
    for i in data:
        if i.queue_name in number.keys():
            number[i.queue_name] = number[i.queue_name] + 1
        else:
            number[i.queue_name] = 1

    for i in number.keys():
        print(i, number.get(i))
    index = []
    target = []
    for i in number.keys():
        if number[i] > count:
            target.append(i)

    for i in range(0, len(data)):
        if data[i].queue_name in target:
            index.append(i)
    print(target)
    new_data = []
    for i in index:
        new_data.append(data[i])
    return new_data




def visualization(data):
    """
           可视化数据等待时间的分布，会针对每一条队列以及总的数据各输出分布图
           :param data:所有数据（list）
           :param count: 阈值，低于这个数量的Job会被删除
    """
    print('----------------------------------------------')
    print(data['queue_name'].value_counts().head(20))
    queue_name_list = data['queue_name'].values.tolist()
    data['actual_sec'] = data['actual_sec'].apply(lambda x: x/3600)
    queue_name_list = list(dict.fromkeys(queue_name_list))

    data['actual_sec'].hist()
    plt.title('all')
    plt.show()
    for i in queue_name_list:
        print(i)
        print(data[data['queue_name'] == i]['actual_sec'].describe())
        data[data['queue_name'] == i]['actual_sec'].hist()
        plt.title(i)
        plt.show()
    print('----------------------------------------------')

# TODO
def outlier(data, queue_name, percentage):
    """
        去除数据中的离群值
        :param data:所有数据
        :param queue_name: 处理的是哪条队列
        :param percentage: 截尾的百分百比
    """
    pass

# TODO
def normalization(data):
    """
        对所有数据的某些列进行归一化
        :param data:所有数据
    """
    pass

def create_feature(data):
    """
        创建特征，比如在原来基础上创建特征，比如虚拟变量的特征创建
        :param data : dataframe类型的数据
    """
    pass

def save(data, path):
    """
        以csv格式保存数据
        :param data : 需要保存的数据
        :param path : 保存的路径
    """
    data_list = list()
    for i in data:
        tmp_list = [i.node, i.request_time, i.cpu_sec, i.queue_node_sum, i.queue_request_time_sum, i.queue_job_sum, i.queue_cpu_sec_sum,
                    i.queue_time_sum, i.queue_node_class_1, i.queue_node_class_2, i.queue_node_class_3, i.queue_request_time_class_1,
                    i.queue_request_time_class_2, i.queue_request_time_class_3, i.queue_cpu_sec_class_1, i.queue_cpu_sec_class_2,
                    i.queue_cpu_sec_class_3, i.queue_time_1, i.queue_time_2, i.queue_time_3, i.run_node_sum, i.run_request_time_sum,
                    i.run_job_sum, i.run_cpu_sec_sum, i.run_time_remain, i.run_node_class_1, i.run_node_class_2, i.run_node_class_3, i.run_request_time_class_1,
                    i.run_request_time_class_2, i.run_request_time_class_3, i.run_cpu_sec_class_1, i.run_cpu_sec_class_2, i.run_cpu_sec_class_3, i.run_time_remain_class_1,
                    i.run_time_remain_class_2, i.run_time_remain_class_3, i.actual_sec, i.actual_run_time, i.queue_name]
        data_list.append(tmp_list)

    name = ['node', 'request_time', 'cpu_sec', 'queue_node_sum', 'queue_request_time_sum', 'queue_job_sum', 'queue_cpu_sec_sum', 'queue_time_sum',
            'queue_node_class_1', 'queue_node_class_2', 'queue_node_class_3', 'queue_request_time_class_1', 'queue_request_time_class_2', 'queue_request_time_class_3',
            'queue_cpu_sec_class_1', 'queue_cpu_sec_class_2', 'queue_cpu_sec_class_3', 'queue_time_1', 'queue_time_2', 'queue_time_3', 'run_node_sum', 'run_request_time_sum',
            'run_job_sum', 'run_cpu_sec_sum', 'run_time_remain', 'run_node_class_1', 'run_node_class_2', 'run_node_class_3', 'run_request_time_class_1', 'run_request_time_class_2',
            'run_request_time_class_3', 'run_cpu_sec_class_1', 'run_cpu_sec_class_2', 'run_cpu_sec_class_3', 'run_time_remain_class_1', 'run_time_remain_class_2', 'run_time_remain_class_3',
            'actual_sec', 'actual_run_time', 'queue_name']
    data_csv = pd.DataFrame(columns=name, data=data_list)
    data_csv.to_csv(path)

