import numpy as np
from pytorch_tabnet.tab_model import TabNetClassifier, TabNetRegressor
from sklearn.model_selection import train_test_split

class TabNet():
    def __init__(self, data):
        data_array = np.array(data.drop('actual_sec', 1))
        self.X = data_array.tolist()

        data_array = np.array(data['actual_sec'])
        self.Y = data_array.tolist()

        self.model = None


    def train(self):
        X, X_test, Y, Y_test = train_test_split(
            self.X, self.Y,
            train_size=0.8, test_size=0.2
        )

        X_train, X_valid, Y_train, Y_valid = train_test_split(
            X, Y,
            train_size=0.75, test_size=0.25
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
        self.test(X_test, Y_test)

    def test(self, X_test, Y_test):
        sums = [0 for _ in range(6)]
        nums = [0 for _ in range(6)]
        count = [0 for _ in range(6)]
        sample_predict = []
        sample_actual = []
        for i in range(0, 6):
            sample_predict.append(list())
            sample_actual.append(list())

        for i in X_test:
            predict_time = max(0, self.predict(i))
            predict_time = 2 ** predict_time - 1
            actual_time = i.actual_sec
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
        # a = Draw()
        # a.draw_line(sample_predict[0], sample_actual[0], str(0))
        # a.draw_line(sample_predict[1], sample_actual[1], str(1))
        for i in range(0, 6):
            print(count[i], nums[i])
        avgs = [np.round(sums[i] / nums[i], 2) for i in range(6)]
        print(avgs)
        AAE = np.round(sum(sums) / sum(nums), 2)
        print('AAE :', end=' ')
        print(AAE)
        return AAE


    def predict(self,X):
        result = self.model.predict(X)
        return result[0]




