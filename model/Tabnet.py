import numpy as np
from pytorch_tabnet.tab_model import TabNetClassifier, TabNetRegressor
from sklearn.model_selection import train_test_split

class TabNet():
    def __init__(self, data):
        self.data = data
        data_array = np.array(self.data['actual_sec'])
        self.Y = data_array.tolist()

        self.drop()
        data_array = np.array(self.data)
        self.X = data_array.tolist()


        self.model = None


    def train(self):
        X, X_test, Y, Y_test = train_test_split(
            self.X, self.Y,
            train_size=0.7, test_size=0.3,
            shuffle=False
        )

        X_train, X_valid, Y_train, Y_valid = train_test_split(
            X, Y,
            train_size=0.6, test_size=0.4,
            shuffle=False
        )
        X_train = np.array(X_train)
        y_train = np.array(Y_train)
        X_valid = np.array(X_valid)
        y_valid = np.array(Y_valid)

        clf = TabNetRegressor()
        y_train = y_train.reshape(-1, 1)
        y_valid = y_valid.reshape(-1, 1)
        clf.fit(X_train=X_train, y_train=y_train, eval_set=[(X_valid, y_valid)])

        self.model = clf
        self.test(self.model.predict(np.array(X_test)), Y_test)

    def test(self, Y_result, Y_test):
        sums = [0 for _ in range(6)]
        nums = [0 for _ in range(6)]
        sample_predict = []
        sample_actual = []
        for i in range(0, 6):
            sample_predict.append(list())
            sample_actual.append(list())

        for i in range(0, len(Y_test)):
            predict_time = Y_result[i]
            actual_time = Y_test[i]
            # print(predict_time, actual_time)
            if actual_time <= 1:  # 0-1
                sums[0] += abs(predict_time - actual_time)
                nums[0] += 1
                sample_predict[0].append(int(predict_time))
                sample_actual[0].append(actual_time)
            elif actual_time <= 3:  # 1-3
                print(predict_time, actual_time)
                sums[1] += abs(predict_time - actual_time)
                nums[1] += 1
                sample_predict[1].append(int(predict_time))
                sample_actual[1].append(actual_time)
            elif actual_time <= 6:  # 3-6
                print(predict_time, actual_time)
                sums[2] += abs(predict_time - actual_time)
                nums[2] += 1
                sample_predict[2].append(int(predict_time))
                sample_actual[2].append(actual_time)
            elif actual_time <= 12:  # 6-12
                print(predict_time, actual_time)
                sums[3] += abs(predict_time - actual_time)
                nums[3] += 1
                sample_predict[3].append(int(predict_time))
                sample_actual[3].append(actual_time)
            elif actual_time <= 24:  # 12-24
                print(predict_time, actual_time)
                sums[4] += abs(predict_time - actual_time)
                nums[4] += 1
                sample_predict[4].append(int(predict_time))
                sample_actual[4].append(actual_time)
            else:
                print(predict_time, actual_time)
                sums[5] += abs(predict_time - actual_time)
                nums[5] += 1
                sample_predict[5].append(int(predict_time))
                sample_actual[5].append(actual_time)

        print(nums)
        avgs = [np.round(sums[i] / nums[i], 2) for i in range(6)]
        print(avgs)
        AAE = np.round(sum(sums) / sum(nums), 2)
        print('AAE :', end=' ')
        print(AAE)
        return AAE


    def drop(self):
        X = self.data.drop(labels=['actual_sec', 'queue_node_class_1', 'queue_node_class_2', 'queue_node_class_3',
                                   'queue_request_time_class_1', 'queue_request_time_class_2'
            , 'queue_request_time_class_3', 'queue_cpu_sec_class_1', 'queue_cpu_sec_class_2', 'queue_cpu_sec_class_3',
                                   'queue_time_1', 'queue_time_2', 'queue_time_3',
                                   'run_node_class_1', 'run_node_class_2', 'run_node_class_3',
                                   'run_request_time_class_1', 'run_request_time_class_2', 'run_request_time_class_3',
                                   'run_cpu_sec_class_1',
                                   'run_cpu_sec_class_2', 'run_cpu_sec_class_3', 'run_time_remain_class_1',
                                   'run_time_remain_class_2', 'run_time_remain_class_3'], axis=1)
        return X



