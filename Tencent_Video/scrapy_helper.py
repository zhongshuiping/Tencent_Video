import datetime
import os


def delete_old_logs(name, days):
    today_str = str(datetime.date.today())
    today = datetime.datetime.strptime(today_str, '%Y-%m-%d')  # 转化为datetime类型,时间为当天0点
    target_day = today - datetime.timedelta(days=days)
    root, dirs, files = [x for x in os.walk('logs')][0]
    for file_name in files:
        if name not in file_name:
            continue
        try:
            log_create_day_str = file_name.split('_')[1].split(' ')[0]
            log_create_day = datetime.datetime.strptime(log_create_day_str, '%Y-%m-%d')
        except:
            continue
        if log_create_day < target_day:
            file_path = root + '/' + file_name
            os.remove(file_path)