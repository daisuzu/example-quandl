#!/usr/bin/env python

import datetime
import math

import quandl
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from models import (
    Stock,
    N225,
    get_codes,
    latest_date,
)


def new_stock(code, data):
    return Stock(
        code,
        data.Date.date(),
        float(data.Open) if not math.isnan(data.Open) else 0.0,
        float(data.High) if not math.isnan(data.High) else 0.0,
        float(data.Low) if not math.isnan(data.Low) else 0.0,
        float(data.Close) if not math.isnan(data.Close) else 0.0,
        float(data.Volume) if not math.isnan(data.Volume) else 0.0,
    )


def download_from_tse(code, start_date):
    data = quandl.get(
        'TSE/{0}'.format(code),
        start_date=start_date,
        returns='numpy',
    )

    return [new_stock(code, d) for d in data]


def new_n225(data):
    return N225(
        data.Date.date(),
        float(data.Open) if not math.isnan(data.Open) else 0.0,
        float(data.High) if not math.isnan(data.High) else 0.0,
        float(data.Low) if not math.isnan(data.Low) else 0.0,
        float(data.Close) if not math.isnan(data.Close) else 0.0,
        float(data.Volume) if not math.isnan(data.Volume) else 0.0,
        float(data['Adjusted Close']) if not math.isnan(data['Adjusted Close']) else 0.0,
    )


def download_n225(start_date):
    data = quandl.get(
        'YAHOO/INDEX_N225',
        start_date=start_date,
        returns='numpy',
    )

    return [new_n225(d) for d in data]


if __name__ == '__main__':
    try:
        from config import QUANDL_API_KEY
        quandl.ApiConfig.api_key = QUANDL_API_KEY
    except ImportError:
        pass

    engine = create_engine('sqlite:///stock.db')
    db_session = scoped_session(sessionmaker(bind=engine, autoflush=False))

    start_date = latest_date(db_session) + datetime.timedelta(days=1)

    print('download: Nikkei 225 Index')
    n225_data = download_n225(start_date)
    if n225_data:
        print(n225_data)
        db_session.add_all(n225_data)
        db_session.commit()

    for code in get_codes(db_session):
        print('download:', code)
        stock_list = download_from_tse(code, start_date)
        if stock_list:
            print(stock_list)
            db_session.add_all(stock_list)
            db_session.commit()
