import pickle


class RawSample:
    def __init__(self, request_ts=-1, start_ts=-1, end_ts=-1, node_num=-1, requested_sec=-1, queue_name=None):
        self.request_ts = request_ts
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.node_num = node_num
        self.requested_sec = requested_sec
        self.queue_name = queue_name
        self.actual_sec = self.start_ts - self.request_ts

    def __lt__(self, other):
        if self.end_ts < other.end_ts:
            return True
        else:
            return False

    def __str__(self):
        return self.__dict__.__str__()


class Preprocessor:
    # TODO
    def save(self, raw_sample_list, file_path):
        """
        保存预处理的结果。
        :param raw_sample_list: 待保存的RawSample数组
        :param file_path: 保存的位置
        """
        save_list = []
        for i in raw_sample_list:
            tmp_list = [i.request_ts, i.start_ts, i.end_ts, i.node_num, i.requested_sec, i.queue_name, i.actual_sec]
            save_list.append(tmp_list)
        with open(file_path, 'wb') as text:
            pickle.dump(save_list, text)

    # TODO
    def load(self, file_path):
        """
        从保存的位置读取预处理过的数据。
        :param file_path: 保存的位置
        :return: 生成的RawSample数组"""
        with open(file_path, 'rb') as text:
            tmp_list = pickle.load(text)
        raw_list = []
        for i in tmp_list:
            tmp_rawsample = RawSample()
            tmp_rawsample.request_ts = i[0]
            tmp_rawsample.start_ts = i[1]
            tmp_rawsample.end_ts = i[2]
            tmp_rawsample.node_num = i[3]
            tmp_rawsample.requested_sec = i[4]
            tmp_rawsample.queue_name = i[5]
            tmp_rawsample.actual_sec = i[6]
            raw_list.append(tmp_rawsample)
        return raw_list

    def preprocess(self, file_path):
        """
        原始文件转RawSample数组
        :param file_path: 原始文件位置
        :return: RawSample数组
        """