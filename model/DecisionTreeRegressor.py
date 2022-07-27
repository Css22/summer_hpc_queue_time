import numpy as np
from pytorch_tabnet.tab_model import TabNetClassifier, TabNetRegressor
from sklearn.model_selection import train_test_split
from sklearn import tree
class DecisionTreeRegressor():
    def __init__(self, data):
        # data_array = np.array(data.drop('actual_sec', 1))
        # self.X = data_array.tolist()
        #
        # data_array = np.array(data['actual_sec'])
        # print(data['actual_sec'].describe())
        # self.Y = data_array.tolist()
        self.data = data
        self.model = None


    def train(self):
        X=self.data.drop('actual_sec', axis =1)
        Y=self.data['actual_sec']
        X_train, X_test, Y_train, Y_test = train_test_split(
            X,Y,
            train_size=0.8, test_size=0.2,
            shuffle= False
        )

        # index = len(self.X)/10 * 8
        # X_train = list()
        # X_test = list()
        # Y_train = list()
        # Y_test = list()
        # for i in range(0, len(self.X)):
        #     if i < index:
        #         X_train.append(self.X[i])
        #         Y_train.append(self.Y[i])
        #     else:
        #         X_test.append(self.X[i])
        #         Y_test.append(self.Y[i])
        tree_model = tree.DecisionTreeRegressor()
        X_train = np.array(X_train)
        Y_train = np.array(Y_train)

        tree_model.max_depth = 100
        Y_train = Y_train.reshape(-1, 1)
        tree_model.fit(X_train, Y_train)

        self.model = tree_model
        # self.test(self.model.predict(X_test), Y_test)

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
            print(predict_time,actual_time)
            if actual_time <= 1:  # 0-1
                sums[0] += abs(predict_time - actual_time)
                nums[0] += 1
                sample_predict[0].append(int(predict_time))
                sample_actual[0].append(actual_time)
            elif actual_time <= 3:  # 1-3
                sums[1] += abs(predict_time - actual_time)
                nums[1] += 1
                sample_predict[1].append(int(predict_time))
                sample_actual[1].append(actual_time)
            elif actual_time <= 6:  # 3-6
                sums[2] += abs(predict_time - actual_time)
                nums[2] += 1
                sample_predict[2].append(int(predict_time))
                sample_actual[2].append(actual_time)
            elif actual_time <= 12:  # 6-12
                sums[3] += abs(predict_time - actual_time)
                nums[3] += 1
                sample_predict[3].append(int(predict_time))
                sample_actual[3].append(actual_time)
            elif actual_time <= 24:  # 12-24
                sums[4] += abs(predict_time - actual_time)
                nums[4] += 1
                sample_predict[4].append(int(predict_time))
                sample_actual[4].append(actual_time)
            else:
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





