
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from preprocess.Preprocessor_mira import PreprocessorMira
from process import data_process
from process import feature_process
from process.sample import sample_save, sample_load, to_sample_list
import seaborn as sns
plt.style.use('fivethirtyeight')

process = PreprocessorMira()
raw_data = process.load('data/local/theta/RawSample_saved.txt')


sample_data = to_sample_list(raw_data)
sample_save(sample_data, 'data/local/theta/Sample_data.txt')
sample_data = sample_load('data/local/theta/Sample_data.txt')
#
# count = 400
new_data = data_process.statistics(sample_data, 400)
data_process.save(new_data, 'data/local/theta/RawSample.csv')
# csv_data = pd.read_csv('data/local/theta/RawSample.csv')
# data_process.visualization(csv_data)

