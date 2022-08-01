import math
import gc
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.preprocessing import StandardScaler
plt.style.use('fivethirtyeight')
# TODO
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)


dataset = 'taiyi'
map = {}
map['mira'] = {'prod-short': 72 * 3600, 'prod-long': 300 * 3600, 'prod-capability': 300 * 3600, 'R.pm': 3 * 3600, 'backfill': 96 * 3600, 'prod-1024-torus': 108 * 3600
               ,'backfill-1024-torus': 250 * 3600}
map['mira'] = {}
map['taiyi'] = {'large': 48 * 3600, 'gpu': 48 * 3600, 'ser': 48 * 3600, 'debug': 48 * 3600, 'short': 48 * 3600, 'spec': 48 * 3600, 'medium': 48 * 3600}
# map['taiyi'] = {}
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

    index = []
    for i in range(0, len(new_data)):
       if new_data[i].queue_name in map[dataset].keys():
           if new_data[i].actual_sec <= map[dataset][new_data[i].queue_name]:
               index.append(i)
       else:
           index.append(i)

    output_data = []
    for i in index:
        output_data.append(new_data[i])
    return output_data




def visualization(data):
    """
           可视化数据等待时间的分布，会针对每一条队列以及总的数据各输出分布图
           :param data:所有数据（list）
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
def normalization(data):
    """
        对所有数据的某些列进行归一化
        :param data:所有数据
    """
    scaler = StandardScaler()
    data_z_nomalization = data.copy()
    for index, row in data_z_nomalization.iteritems():
        if index == 'queue_name':
            continue
        if index == 'actual_sec':
            continue
        data_z_nomalization[index] = scaler.fit_transform(data_z_nomalization[[index]])
    return data_z_nomalization

def create_feature(data):
    """
        创建特征，比如在原来基础上创建特征，比如虚拟变量的特征创建
        :param data : dataframe类型的数据
    """
    data_dummies = pd.get_dummies(data, prefix_sep='_', columns=['queue_name'])
    return data_dummies

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
                    i.run_time_remain_class_2, i.run_time_remain_class_3, i.actual_sec, i.actual_run_time, i.queue_name, i.queue_node_sum_itself, i.queue_request_time_sum_itself,
                    i.queue_job_sum_itself, i.queue_cpu_sec_sum_itself, i.queue_time_sum_itself]
        data_list.append(tmp_list)

    name = ['node', 'request_time', 'cpu_sec', 'queue_node_sum', 'queue_request_time_sum', 'queue_job_sum', 'queue_cpu_sec_sum', 'queue_time_sum',
            'queue_node_class_1', 'queue_node_class_2', 'queue_node_class_3', 'queue_request_time_class_1', 'queue_request_time_class_2', 'queue_request_time_class_3',
            'queue_cpu_sec_class_1', 'queue_cpu_sec_class_2', 'queue_cpu_sec_class_3', 'queue_time_1', 'queue_time_2', 'queue_time_3', 'run_node_sum', 'run_request_time_sum',
            'run_job_sum', 'run_cpu_sec_sum', 'run_time_remain', 'run_node_class_1', 'run_node_class_2', 'run_node_class_3', 'run_request_time_class_1', 'run_request_time_class_2',
            'run_request_time_class_3', 'run_cpu_sec_class_1', 'run_cpu_sec_class_2', 'run_cpu_sec_class_3', 'run_time_remain_class_1', 'run_time_remain_class_2', 'run_time_remain_class_3',
            'actual_sec', 'actual_run_time', 'queue_name', 'queue_node_sum_itself', 'queue_request_time_sum_itself', 'queue_job_sum_itself', 'queue_cpu_sec_sum_itself', 'queue_time_sum_itself']
    data_csv = pd.DataFrame(columns=name, data=data_list)
    data_csv.to_csv(path)

def seq_save(data,path):
    data_list = list()
    for i in data:
        tmp_list = [i.node, i.request_time, i.cpu_sec, i.queue_name, i.actual_sec, i.itself_queue_list, i.queue_list, i.run_list]
        data_list.append(tmp_list)
    del data
    gc.collect()
    name = ['node', 'request_time', 'cpu_sec', 'queue_name', 'actual_sec','itself_queue_list', 'queue_list', 'run_list']
    data_csv = pd.DataFrame(columns=name, data=data_list)
    data_csv.to_csv(path)

