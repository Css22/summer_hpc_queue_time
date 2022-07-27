from preprocess.preprocess import Preprocessor, RawSample


def operate_line(op_line):
    if len(op_line) < 20: return None
    event_time = int(op_line[2])
    submit_time = int(op_line[7])
    start_time = int(op_line[10])
    CPU_num = int(op_line[6])
    queue_name = op_line[12].replace('"', '')

    sec = 0
    try:
        if '-W' in op_line:
            ind = op_line.index('-W')
            time_str = op_line[ind+1]
            if time_str[2] == ':':
                time_str = time_str[:6]
            elif time_str[1] == ':':
                time_str = '0' + time_str[:5]
            sec = int(time_str[0:2]) * 3600 + int(time_str[3:5]) * 60
    except Exception:
        pass
    if sec == 0: return None

    raw_sample = RawSample(submit_time, start_time, event_time, CPU_num, sec, queue_name)
    return raw_sample


class PreprocessorTaiyi(Preprocessor):
    # TODO
    def preprocess(self, file_path):
        raw_samples = []
        with open(file_path, encoding='utf-8', errors='ignore') as f:
            op_line = []
            i = 0
            for line in f:
                print(i)
                i+=1
                tmp_arr = line.split()
                if len(tmp_arr) == 0:
                    continue
                elif tmp_arr[0] in ['"JOB_FINISH"', '"EVENT_ADRSV_FINISH"', '"JOB_RESIZE"']:
                    raw_sample = operate_line(op_line)
                    if raw_sample is not None: raw_samples.append(raw_sample)
                    if tmp_arr[0] == '"JOB_FINISH"':
                        op_line = tmp_arr
                    else:
                        op_line = []
                else:
                    if len(op_line) >=1 and op_line[0] == '"JOB_FINISH"':
                        op_line.extend(tmp_arr)
        return raw_samples

    def fix(self, raw_data):
        index = []
        for i in range(0, len(raw_data)):
            if raw_data[i].start_ts != 0:
                index.append(i)

        new_data = []
        for i in index:
            new_data.append(raw_data[i])
        return  new_data