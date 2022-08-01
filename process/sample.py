import pickle


class Sample:
    def __init__(self,
                 node=-1,
                 request_time=-1,
                 cpu_sec=-1,
                 queue_node_sum=-1,
                 queue_request_time_sum=-1,
                 queue_job_sum=-1,
                 queue_cpu_sec_sum=-1,
                 queue_time_sum=-1,
                 queue_node_class_1=-1, queue_node_class_2=-1, queue_node_class_3=-1,
                 queue_request_time_class_1=-1, queue_request_time_class_2=-1, queue_request_time_class_3=-1,
                 queue_cpu_sec_class_1=-1,
                 queue_cpu_sec_class_2=-1, queue_cpu_sec_class_3=-1, queue_time_1=-1, queue_time_2=-1, queue_time_3=-1,
                 run_node_sum=-1,
                 run_request_time_sum=-1, run_job_sum=-1, run_cpu_sec_sum=-1, run_time_remain=-1, run_node_class_1=-1,
                 run_node_class_2=-1,
                 run_node_class_3=-1, run_request_time_class_1=-1, run_request_time_class_2=-1,
                 run_request_time_class_3=-1, run_cpu_sec_class_1=-1,
                 run_cpu_sec_class_2=-1, run_cpu_sec_class_3=-1, run_time_remain_class_1=-1, run_time_remain_class_2=-1,
                 run_time_remain_class_3=-1,
                 actual_sec=-1, actual_run_time=-1,
                 queue_name=-1,
                 queue_node_sum_itself=-1,
                 queue_request_time_sum_itself=-1,
                 queue_job_sum_itself=-1,
                 queue_cpu_sec_sum_itself=-1,
                 queue_time_sum_itself=-1,):
        self.node = node
        self.request_time = request_time
        self.cpu_sec = cpu_sec

        self.queue_node_sum_itself = queue_node_sum_itself
        self.queue_request_time_sum_itself = queue_request_time_sum_itself
        self.queue_job_sum_itself = queue_job_sum_itself
        self.queue_cpu_sec_sum_itself = queue_cpu_sec_sum_itself
        self.queue_time_sum_itself = queue_time_sum_itself


        self.queue_node_sum = queue_node_sum
        self.queue_request_time_sum = queue_request_time_sum
        self.queue_job_sum = queue_job_sum
        self.queue_cpu_sec_sum = queue_cpu_sec_sum
        self.queue_time_sum = queue_time_sum
        self.queue_node_class_1 = queue_node_class_1
        self.queue_node_class_2 = queue_node_class_2
        self.queue_node_class_3 = queue_node_class_3
        self.queue_request_time_class_1 = queue_request_time_class_1
        self.queue_request_time_class_2 = queue_request_time_class_2
        self.queue_request_time_class_3 = queue_request_time_class_3
        self.queue_cpu_sec_class_1 = queue_cpu_sec_class_1
        self.queue_cpu_sec_class_2 = queue_cpu_sec_class_2
        self.queue_cpu_sec_class_3 = queue_cpu_sec_class_3
        self.queue_time_1 = queue_time_1
        self.queue_time_2 = queue_time_2
        self.queue_time_3 = queue_time_3

        self.run_node_sum = run_node_sum
        self.run_request_time_sum = run_request_time_sum
        self.run_job_sum = run_job_sum
        self.run_cpu_sec_sum = run_cpu_sec_sum
        self.run_time_remain = run_time_remain

        self.run_node_class_1 = run_node_class_1
        self.run_node_class_2 = run_node_class_2
        self.run_node_class_3 = run_node_class_3
        self.run_request_time_class_1 = run_request_time_class_1
        self.run_request_time_class_2 = run_request_time_class_2
        self.run_request_time_class_3 = run_request_time_class_3
        self.run_cpu_sec_class_1 = run_cpu_sec_class_1
        self.run_cpu_sec_class_2 = run_cpu_sec_class_2
        self.run_cpu_sec_class_3 = run_cpu_sec_class_3
        self.run_time_remain_class_1 = run_time_remain_class_1
        self.run_time_remain_class_2 = run_time_remain_class_2
        self.run_time_remain_class_3 = run_time_remain_class_3

        self.actual_sec = actual_sec
        self.actual_run_time = actual_run_time
        self.queue_name = queue_name
    def __str__(self):
        return self.__dict__.__str__()


# TODO
def sample_save(sample_list, file_path):
    """
    保存sample数组。
    :param raw_sample_list: 待保存的Sample数组
    :param file_path: 保存的位置
    """
    save_list = []
    for i in sample_list:
        tmp_list = [i.node, i.request_time, i.cpu_sec, i.queue_node_sum, i.queue_request_time_sum, i.queue_job_sum,
                    i.queue_cpu_sec_sum, i.queue_time_sum, i.queue_node_class_1, i.queue_node_class_2,
                    i.queue_node_class_3,
                    i.queue_request_time_class_1, i.queue_request_time_class_2, i.queue_request_time_class_3,
                    i.queue_cpu_sec_class_1,
                    i.queue_cpu_sec_class_2, i.queue_cpu_sec_class_3, i.queue_time_1, i.queue_time_2, i.queue_time_3,
                    i.run_node_sum,
                    i.run_request_time_sum, i.run_job_sum, i.run_cpu_sec_sum, i.run_time_remain, i.run_node_class_1,
                    i.run_node_class_2,
                    i.run_node_class_3, i.run_request_time_class_1, i.run_request_time_class_2,
                    i.run_request_time_class_3, i.run_cpu_sec_class_1,
                    i.run_cpu_sec_class_2, i.run_cpu_sec_class_3, i.run_time_remain_class_1, i.run_time_remain_class_2,
                    i.run_time_remain_class_3,
                    i.actual_sec, i.actual_run_time, i.queue_name, i.queue_node_sum_itself, i.queue_request_time_sum_itself,
                    i.queue_job_sum_itself, i.queue_cpu_sec_sum_itself, i.queue_time_sum_itself]
        save_list.append(tmp_list)
    with open(file_path, 'wb') as text:
        pickle.dump(save_list, text)


# TODO
def sample_load(file_path):
    """
    导入sample数组。
    :param file_path: 保存的位置
    :return: Sample数组
    """
    with open(file_path, 'rb') as text:
        tmp_list = pickle.load(text)
    sample_list = [Sample(node=x[0], request_time=x[1], cpu_sec=x[2], queue_node_sum=x[3], queue_request_time_sum=x[4],
                          queue_job_sum=x[5], queue_cpu_sec_sum=x[6], queue_time_sum=x[7], queue_node_class_1=x[8],
                          queue_node_class_2=x[9], queue_node_class_3=x[10], queue_request_time_class_1=x[11],
                          queue_request_time_class_2=x[12], queue_request_time_class_3=x[13],
                          queue_cpu_sec_class_1=x[14],
                          queue_cpu_sec_class_2=x[15], queue_cpu_sec_class_3=x[16], queue_time_1=x[17],
                          queue_time_2=x[18], queue_time_3=x[19], run_node_sum=x[20], run_request_time_sum=x[21],
                          run_job_sum=x[22],
                          run_cpu_sec_sum=x[23], run_time_remain=x[24], run_node_class_1=x[25], run_node_class_2=x[26],
                          run_node_class_3=x[27], run_request_time_class_1=x[28], run_request_time_class_2=x[29],
                          run_request_time_class_3=x[30], run_cpu_sec_class_1=x[31], run_cpu_sec_class_2=x[32],
                          run_cpu_sec_class_3=x[33], run_time_remain_class_1=x[34], run_time_remain_class_2=x[35],
                          run_time_remain_class_3=x[36], actual_sec=x[37], actual_run_time=x[38], queue_name=x[39],
                          queue_node_sum_itself=x[40], queue_request_time_sum_itself=x[41], queue_job_sum_itself=x[42],
                          queue_cpu_sec_sum_itself=x[43], queue_time_sum_itself=x[44]
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
        tmp_sample = Sample()
        tmp_sample.queue_name = i.queue_name
        tmp_sample.node = i.node_num
        tmp_sample.request_time = i.requested_sec
        tmp_sample.cpu_sec = tmp_sample.node * tmp_sample.request_time

        queue_node_sum_itself = 0
        queue_request_time_sum_itself = 0
        queue_job_sum_itself = 0
        queue_cpu_sec_sum_itself = 0
        queue_time_sum_itself = 0

        for j in i.queue_job_list_itself:
            queue_node_sum_itself = queue_node_sum_itself + preprocessed_list[j].node_num
            queue_request_time_sum_itself = queue_request_time_sum_itself + preprocessed_list[j].requested_sec
            queue_job_sum_itself = queue_job_sum_itself + 1
            queue_cpu_sec_sum_itself = queue_cpu_sec_sum_itself + preprocessed_list[j].node_num * preprocessed_list[j].requested_sec
            queue_time_sum_itself = queue_time_sum_itself + i.request_ts - preprocessed_list[j].request_ts

        tmp_sample.queue_node_sum_itself = queue_node_sum_itself
        tmp_sample.queue_request_time_sum_itself = queue_request_time_sum_itself
        tmp_sample.queue_job_sum_itself = queue_job_sum_itself
        tmp_sample.queue_cpu_sec_sum_itself = queue_cpu_sec_sum_itself
        tmp_sample.queue_time_sum_itself = queue_time_sum_itself

        queue_node_sum = 0
        queue_request_time_sum = 0
        queue_job_sum = 0
        queue_cpu_sec_sum = 0
        queue_time_sum = 0

        queue_node_class_1 = 0
        queue_node_class_2 = 0
        queue_node_class_3 = 0
        queue_request_time_class_1 = 0
        queue_request_time_class_2 = 0
        queue_request_time_class_3 = 0
        queue_cpu_sec_class_1 = 0
        queue_cpu_sec_class_2 = 0
        queue_cpu_sec_class_3 = 0
        queue_time_1 = 0
        queue_time_2 = 0
        queue_time_3 = 0

        for j in i.queue_job_list:
            queue_node_sum = queue_node_sum + preprocessed_list[j].node_num
            queue_request_time_sum = queue_request_time_sum + preprocessed_list[j].requested_sec
            queue_job_sum = queue_job_sum + 1
            queue_cpu_sec_sum = queue_cpu_sec_sum + preprocessed_list[j].node_num * preprocessed_list[j].requested_sec
            queue_time_sum = queue_time_sum + i.request_ts - preprocessed_list[j].request_ts

        tmp_sample.queue_node_sum = queue_node_sum
        tmp_sample.queue_request_time_sum = queue_request_time_sum
        tmp_sample.queue_job_sum = queue_job_sum
        tmp_sample.queue_cpu_sec_sum = queue_cpu_sec_sum
        tmp_sample.queue_time_sum = queue_time_sum

        run_node_sum = 0
        run_request_time_sum = 0
        run_job_sum = 0
        run_cpu_sec_sum = 0
        run_time_remain = 0

        run_node_class_1 = 0
        run_node_class_2 = 0
        run_node_class_3 = 0
        run_request_time_class_1 = 0
        run_request_time_class_2 = 0
        run_request_time_class_3 = 0
        run_cpu_sec_class_1 = 0
        run_cpu_sec_class_2 = 0
        run_cpu_sec_class_3 = 0
        run_time_remain_class_1 = 0
        run_time_remain_class_2 = 0
        run_time_remain_class_3 = 0
        for j in i.run_job_list:
            run_node_sum = run_node_sum + preprocessed_list[j].node_num
            run_request_time_sum = run_request_time_sum + preprocessed_list[j].requested_sec
            run_job_sum = run_job_sum + 1
            run_cpu_sec_sum = run_cpu_sec_sum + preprocessed_list[j].node_num * preprocessed_list[j].requested_sec
            run_time_remain = run_time_remain + preprocessed_list[j].end_ts - i.request_ts

        tmp_sample.run_node_sum = run_node_sum
        tmp_sample.run_request_time_sum = run_request_time_sum
        tmp_sample.run_job_sum = run_job_sum
        tmp_sample.run_cpu_sec_sum = run_cpu_sec_sum
        tmp_sample.run_time_remain = run_time_remain

        actual_sec = i.actual_sec
        actual_run_time = i.actual_runtime
        tmp_sample.actual_sec = actual_sec
        tmp_sample.actual_run_time = actual_run_time
        sample_list.append(tmp_sample)
        if index % 1000 == 0 :
            print(index, len(preprocessed_list))
    return sample_list
