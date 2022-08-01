import torch
import gc
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets, transforms
from process.sequence import sample_save, sample_load, to_sample_list
from preprocess.preprocess_taiyi import PreprocessorTaiyi
from process.data_process import seq_save


sample_save(seq_sample_data, '../data/local/Taiyi/seq_sample_data.txt')
sample_data = sample_load('../data/local/Taiyi/seq_sample_data.txt')

#         run_state = self.run_list(run_list)
