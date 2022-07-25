import time

def to_timestamp(time_str):
    return int(time.mktime(time.strptime(time_str, "%Y-%m-%d %H:%M:%S")))

def to_time_str(timestamp):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))