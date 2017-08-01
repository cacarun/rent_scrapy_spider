# -*- coding: utf-8 -*-
import datetime


class Util(object):
    @staticmethod
    def get_time_now():
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_time_from_str(time_str):
        if '-' in time_str and ':' in time_str:  # 06-18 21:07
            date_today = datetime.date.today()
            date_with_year = str(date_today.year) + '-' + time_str
            date = datetime.datetime.strptime(date_with_year, "%Y-%m-%d %H:%M")
            return date
        elif '-' in time_str:  # 2017-06-18
            return datetime.datetime.strptime(time_str, "%Y-%m-%d")
