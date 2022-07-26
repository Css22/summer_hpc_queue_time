import pickle
import heapq

class RawSample:
    def __init__(self, request_ts=-1, start_ts=-1, end_ts=-1, node_num=-1, requested_sec=-1, queue_name=None, id=0):
        self.request_ts = request_ts
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.node_num = node_num
        self.requested_sec = requested_sec
        self.queue_name = queue_name
        self.actual_sec = self.start_ts - self.request_ts
        self.actual_runtime = self.end_ts - self.start_ts
        self.queue_job_list = []
        self.run_job_list = []
        self.queue_job_list_itself = []
        self.id = id

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
            tmp_list = [i.request_ts, i.start_ts, i.end_ts, i.node_num, i.requested_sec, i.queue_name, i.actual_sec, i.actual_runtime, i.queue_job_list, i.run_job_list, i.queue_job_list_itself, i.id]
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
        for index, i in enumerate(tmp_list):
            tmp_rawsample = RawSample()
            tmp_rawsample.request_ts = i[0]
            tmp_rawsample.start_ts = i[1]
            tmp_rawsample.end_ts = i[2]
            tmp_rawsample.node_num = i[3]
            tmp_rawsample.requested_sec = i[4]
            tmp_rawsample.queue_name = i[5]
            tmp_rawsample.actual_sec = i[6]
            tmp_rawsample.actual_runtime = i[7]
            tmp_rawsample.queue_job_list = i[8]
            tmp_rawsample.run_job_list = i[9]
            tmp_rawsample.queue_job_list_itself = i[10]
            tmp_rawsample.id = i[11]
            raw_list.append(tmp_rawsample)

        return raw_list

    def preprocess(self, file_path):
        """
        原始文件转RawSample数组
        :param file_path: 原始文件位置
        :return: RawSample数组
        """
    def index(self, raw_sample_list):
        """
        这里是给原始数据附上run_job_index与queue_job_index
        """

        for i in range(0, len(raw_sample_list)):
            raw_sample_list[i].id = i

        raw_sample_list.sort(key=lambda x: x.request_ts)
        request_ts_list = []

        for i in raw_sample_list:
            if len(request_ts_list) == 0:
                i.run_job_list = []
                i.queue_job_list = []
            else:
                while(len(request_ts_list) != 0):
                    if request_ts_list[0].end_ts <= i.request_ts:
                        heapq.heappop(request_ts_list)
                    else:
                        for job in request_ts_list:
                            if job.request_ts == i.request_ts:
                                continue
                            if job.start_ts < i.request_ts:
                                i.run_job_list.append(job.id)
                            if job.start_ts >= i.request_ts:
                                if i.queue_name == job.queue_name:
                                    i.queue_job_list_itself.append(job.id)
                                i.queue_job_list.append(job.id)
                        break
            if raw_sample_list.index(i) % 1000 == 0:
                print(raw_sample_list.index(i), len(raw_sample_list))
            heapq.heappush(request_ts_list, i)
        return raw_sample_list
