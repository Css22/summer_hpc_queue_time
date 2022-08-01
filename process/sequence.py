import pickle
import joblib

class seq_sample:
    def __init__(self,
                 node=-1,
                 request_time=-1,
                 cpu_sec=-1,
                 queue_name=-1,
                 actual_sec=-1,
                 itself_queue_list=[],
                 queue_list=[],
                 run_list=[]):
        self.node = node
        self.request_time = request_time
        self.cpu_sec = cpu_sec
        self.queue_name = queue_name
        self.queue_list = queue_list
        self.itself_queue_list = itself_queue_list
        self.run_list = run_list
        self.actual_sec = actual_sec

    def __str__(self):
        return self.__dict__.__str__()


# TODO
def sample_save(sample_list, file_path):
    """
    保存sample数组。
    :param raw_sample_list: 待保存的Sample数组
    :param file_path: 保存的位置
    """
    print('saving')
    save_list = []
    index = 0
    for i in sample_list:
        index = index + 1
        tmp_list = [i.node, i.request_time, i.cpu_sec, i.queue_name, i.queue_list, i.itself_queue_list, i.run_list, i.actual_sec]
        save_list.append(tmp_list)
        print(index, len(sample_list))
    with open(file_path, 'wb') as text:
        pickle.dump(save_list, text)
    print('successful save')


# TODO
def sample_load(file_path):
    """
    导入sample数组。
    :param file_path: 保存的位置
    :return: Sample数组
    """
    print('loading')
    with open(file_path, 'rb') as text:
        tmp_list = pickle.load(text)
    sample_list = [seq_sample(node=x[0], request_time=x[1], cpu_sec=x[2], queue_name=x[3],
                              queue_list=x[4], itself_queue_list=x[5], run_list=x[6], actual_sec=x[7]
                              ) for index, x in enumerate(tmp_list)]
    return sample_list


# TODO
def to_sample_list(preprocessed_list):
    """
    将RawSample数组转Sample数组。不需要对class_label处理。
    :param preprocessed_list: RawSample数组
    :return: Sample数组
    """

    sample_list = []
    index = 0
    for i in preprocessed_list:
        index = index + 1
        tmp_sample = seq_sample()
        tmp_sample.queue_name = i.queue_name
        tmp_sample.node = i.node_num
        tmp_sample.request_time = i.requested_sec
        tmp_sample.cpu_sec = tmp_sample.node * tmp_sample.request_time

        for j in i.queue_job_list_itself:
            tmp_sample.itself_queue_list.append([preprocessed_list[j].requested_sec, preprocessed_list[j].node_num,
                                                 preprocessed_list[j].node_num * preprocessed_list[j].requested_sec,
                                                 i.request_ts - preprocessed_list[j].request_ts])

        for j in i.queue_job_list:
            tmp_sample.queue_list.append([preprocessed_list[j].requested_sec, preprocessed_list[j].node_num,
                                          preprocessed_list[j].node_num * preprocessed_list[j].requested_sec,
                                          i.request_ts - preprocessed_list[j].request_ts])

        for j in i.run_job_list:
            tmp_sample.run_list.append([preprocessed_list[j].requested_sec, preprocessed_list[j].node_num,
                                        preprocessed_list[j].node_num * preprocessed_list[j].requested_sec,
                                        preprocessed_list[j].end_ts - i.request_ts])

        actual_sec = i.actual_sec
        tmp_sample.actual_sec = actual_sec
        sample_list.append(tmp_sample)
        if index % 1000 == 0:
            print(index, len(preprocessed_list))
    return sample_list
