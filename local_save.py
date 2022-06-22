import pickle

import torch


def mid(arr):
    if len(arr) == 0: return 0
    arr.sort()
    return arr[len(arr) // 2]


def avg(arr):
    if len(arr) == 0: return 0
    return sum(arr) / len(arr)


class Result:
    def __init__(self, pred, actual):
        self.pred = pred
        self.actual = actual

    @classmethod
    def get_eval(cls, results, log2=False):
        if log2:
            for r in results:
                # r.actual = 2 ** r.actual - 1
                r.pred = 2 ** r.pred - 1

        # 0-1 1-3 3-6 6-12 12-24 24+
        total = [0, 0, 0, 0, 0, 0]
        err = [0, 0, 0, 0, 0, 0]
        for result in results:
            if result.actual < 3600:
                op = 0
            elif result.actual < 3600 * 3:
                op = 1
            elif result.actual < 3600 * 6:
                op = 2
            elif result.actual < 3600 * 12:
                op = 3
            elif result.actual < 3600 * 24:
                op = 4
            else:
                op = 5
            total[op] += 1
            err[op] += abs(result.actual - result.pred)

        txt_arr = '0-1 1-3 3-6 6-12 12-24 24+'.split()
        for i in range(6):
            print(f'{txt_arr[i]:>6}\t{total[i]:>10}\t{0 if total[i] == 0 else err[i] / total[i] / 3600:>6.2f}')
        print(f'AAE: {sum(err) / sum(total) / 3600 :>21.2f}')


class TimePieces:
    def __str__(self) -> str:
        return f'id{self.id} qtask{self.queuing_task} rtask{self.running_task}'

    def __init__(self, id):
        self.id = id
        # only if one task over 5 min, it would considered as load
        self.queuing_task = 0  # yes done
        self.running_task = 0  # yes done
        self.used_nodes = 0  # yes done

        # considered as avg job status
        self.queue_which_start = []  # no
        self.queue_which_end = []  # yes
        self.run_which_start = []  # no
        self.run_which_end = []  # yes
        self.request_ts_here = 0  # yes
        self.start_ts_here = 0  # yes
        self.end_ts_here = 0  # yes


from preprocess import RawSample, Preprocessor

mnum = 0


def operate_line(op_line):
    if len(op_line) < 20: return None
    event_time = int(op_line[2])
    submit_time = int(op_line[7])
    start_time = int(op_line[10])
    CPU_num = int(op_line[6])
    queue_name = op_line[12].replace('"', '')
    # if not queue_name == 'medium':
    #     return None

    global mnum
    mnum += 1
    if (mnum % 10000 == 0): print('mm', mnum)

    sec = 0
    time_str = ''
    try:
        if '-W' in op_line:
            ind = op_line.index('-W')
            time_str = op_line[ind + 1]
            if time_str[2] == ':':
                time_str = time_str[:6]
            elif time_str[1] == ':':
                time_str = '0' + time_str[:5]
            sec = int(time_str[0:2]) * 3600
            if not time_str[3] == '0':
                sec += int(time_str[3:5]) * 60
    except Exception as e:
        try:
            if time_str[3] == ':':
                time_str = time_str[:7]
                sec = int(time_str[0:3]) * 3600
                if not time_str[4] == '0':
                    sec += int(time_str[4:5]) * 60
        except Exception as e2:
            print(e2)
            print(op_line)

    raw_sample = None
    if True:
        if sec == 0: sec = 168 * 3600
        raw_sample = RawSample(submit_time, start_time, event_time, CPU_num, sec, queue_name)
    else:
        pass
    return raw_sample


class PreprocessorTaiyi(Preprocessor):
    # TODO
    def preprocess(self, file_path):
        raw_samples = []
        with open(file_path, errors='ignore', encoding='utf-8') as f:
            op_line = []
            i = 0
            total = 0
            for line in f:
                if total > 100_0000:
                    break
                i += 1
                tmp_arr = line.split()
                if len(tmp_arr) == 0:
                    continue
                elif tmp_arr[0] in ['"JOB_FINISH"', '"EVENT_ADRSV_FINISH"', '"JOB_RESIZE"']:
                    raw_sample = operate_line(op_line)
                    if raw_sample is not None:
                        raw_samples.append(raw_sample)
                        total += 1
                        if total % 10000 == 0: print(total)
                    if tmp_arr[0] == '"JOB_FINISH"':
                        op_line = tmp_arr
                    else:
                        op_line = []
                else:
                    if len(op_line) >= 1 and op_line[0] == '"JOB_FINISH"':
                        op_line.extend(tmp_arr)
        return raw_samples


raw_samples = PreprocessorTaiyi().load('taiyi_raw_samples.txt')
raw_samples.sort(key=lambda x: x.request_ts)
for i in raw_samples:
    i.start_ts = (i.start_ts - 1538038215) // 300
    i.end_ts = (i.end_ts - 1538038215) // 300
    i.request_ts = (i.request_ts - 1538038215) // 300
raw_samples = [x for x in raw_samples if x.request_ts > 10 and x.actual_sec >= 0]

with open('timepieces.pkt', mode='rb') as f:
    time_pieces = pickle.load(f)

import numpy as np
import math
from torch.utils.data import Dataset


def log(n):
    return math.log2(n + 1)


def exp(n):
    return 2 ** - 1


origin_data = []
for i in time_pieces:
    origin_data.append(
        [log(mid(i.queue_which_start)), log(mid(i.run_which_start)), log(i.queuing_task), log(i.running_task),
         log(i.used_nodes)])
origin_data = np.array(origin_data)


class TimeDataset(Dataset):
    def __init__(self, origin_data):
        self.origin_data = origin_data

    def __len__(self):
        return len(self.origin_data) - 32

    def __getitem__(self, idx):
        x = self.origin_data[idx:idx + 30, :]
        y = self.origin_data[idx + 1:idx + 31, 0]
        return torch.from_numpy(x).to(torch.float32), torch.from_numpy(y).to(torch.float32)


ds = TimeDataset(origin_data)

import torch.nn as nn
from torch.utils.data import DataLoader


class RNN(nn.Module):
    def __init__(self, input_size):
        super(RNN, self).__init__()
        self.rnn = nn.LSTM(
            input_size=input_size,
            hidden_size=64,
            num_layers=1,
            batch_first=True
        )
        self.out = nn.Sequential(
            nn.Linear(64, 1)
        )

    def forward(self, x):
        r_out, (h_n, h_c) = self.rnn(x, None)  # None 表示 hidden state 会用全0的 state
        out = self.out(r_out)
        return out


train_dataloader = DataLoader(ds, batch_size=20)


def train(dataloader, model, loss_fn, optimizer):
    size = len(dataloader.dataset)
    model.train()
    for step in range(10):
        k = 0
        for tx, ty in dataloader:
            k += 1
            output = model(tx)
            loss = loss_fn(torch.squeeze(output), ty)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            if k % 1000 == 0:
                print(k, loss)
        print('-' * 20)


# 你回车nmn
model = RNN(5)
loss_fn = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
train(train_dataloader, model, loss_fn, optimizer)


def get(num):
    if num % 2000 == 0: print(num)
    with torch.no_grad():
        dsn = TimeDataset(origin_data[num:num + 33, :])
        dl = DataLoader(dsn, batch_size=1, )
        for tx, ty in dl:
            output = model(tx)
            return float(list(output)[-1][-1][-1]), \
                   float(list(ty)[-1][-1])


n = 60000
s = 1
xs = [i for i in range(n)]
y1s = []
y2s = []
for i in range(s, s + n):
    x, y = get(i)
    y1s.append(x)
    y2s.append(y)


def exp(n):
    return 2 ** n - 1


y1ss = [exp(y if y > 0 else 0) * 300 for y in y1s]
y2ss = [exp(y if y > 0 else 0) * 300 for y in y2s]  # std
results = []

for i in raw_samples:
    if 100 < i.request_ts < 60000:
        results.append(Result((y2ss[i.request_ts - 32] * 0.5 + y1ss[i.request_ts - 31] * 1.5), i.actual_sec))
Result.get_eval(results)
