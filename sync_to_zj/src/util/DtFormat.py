import datetime as dt
from enum import Enum


class DtFormat(Enum):
    yyyy_MM_dd_yesterday = (
        dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d')

    yyyy_MM_dd_hh_mm_ss = (
        dt.datetime.now().strftime('%Y-%m-%d %X')
    )


class DtUtil:
    @classmethod
    def getUtcTimestamp(cls, dtTuple: dt.datetime):
        return str(int(dtTuple.replace(tzinfo=dt.timezone.utc).timestamp()))

    @classmethod
    def getCurrentUtcTimestamp(cls):
        return str(int(dt.datetime.now().replace(tzinfo=dt.timezone.utc).timestamp()))

    @classmethod
    def timestampTostr(cls, timestamp:int, out_fomat:str) -> str:
        return dt.datetime.strftime(dt.datetime.fromtimestamp(timestamp), out_fomat)


    @classmethod
    def year(cls, string, in_format) -> int:
        return dt.datetime.strptime(string, in_format).year

    @classmethod
    def month(cls, string, in_format:str) -> int:
        return dt.datetime.strptime(string, in_format).month

    @classmethod
    def day(cls, string, in_format:str) -> int:
        return dt.datetime.strptime(string, in_format).day

    @classmethod
    def hour(cls, string, in_format:str)-> int:
        return dt.datetime.strptime(string, in_format).hour

    @classmethod
    def hour_ago(cls, string, in_format:str, out_fomat:str):
        return (dt.datetime.strptime(string, in_format) - dt.timedelta(hours=1)).strftime(out_fomat)
    
    @classmethod
    def n_hour_ago(cls, string, n, in_format:str, out_fomat:str) -> str:
        return (dt.datetime.strptime(string, in_format) - dt.timedelta(hours=n)).strftime(out_fomat)

    @classmethod
    def day_ago(cls, string, in_format:str, out_fomat:str) -> str:
        return (dt.datetime.strptime(string, in_format) - dt.timedelta(days=1)).strftime(out_fomat)

    @classmethod
    def day_later(cls, string, in_format:str, out_fomat:str) -> str:
        return (dt.datetime.strptime(string, in_format) + dt.timedelta(days=1)).strftime(out_fomat)

    @classmethod
    def n_minutes_ago(cls, string, n, in_format:str, out_fomat:str) -> str:
        return (dt.datetime.strptime(string, in_format) - dt.timedelta(minutes=n)).strftime(out_fomat)
    
    @classmethod
    def n_minutes_later(cls, string, n, in_format:str, out_fomat:str) -> str:
        return (dt.datetime.strptime(string, in_format) + dt.timedelta(minutes=n)).strftime(out_fomat)