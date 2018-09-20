#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

import json
import datetime
import argparse
import logging
import logging.config

import utils


def initlog(level=None, log="-"):
    if level is None:
        level = logging.DEBUG if __debug__ else logging.INFO
    if isinstance(level, basestring):
        level = getattr(logging, level.upper())

    class MyFormatter(logging.Formatter):

        def format(self, record):
            dformatter = '[%(asctime)s] %(name)s %(levelname)s %(pathname)s %(lineno)d [%(funcName)s] %(message)s'
            formatter = '[%(asctime)s] %(levelname)s %(name)s %(message)s'
            if record.levelno <= logging.DEBUG:
                self._fmt = dformatter
            else:
                self._fmt = formatter
            return super(MyFormatter, self).format(record)

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "custom": {
                '()': MyFormatter
            },
            "simple": {
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s"
            },
            "verbose": {
                "format": "[%(asctime)s] %(name)s %(levelname)s %(pathname)s %(lineno)d [%(funcName)s] %(message)s"
            }
        },
        "handlers": {
        },
        'root': {
            'level': level,
            'handlers': ['console']
        }
    }
    console = {
        "class": "logging.StreamHandler",
        "level": "DEBUG",
        "formatter": "verbose",
        "stream": "ext://sys.stdout"
    }
    file_handler = {
        "class": "logging.handlers.RotatingFileHandler",
        "level": "DEBUG",
        "formatter": "verbose",
        "filename": log,
        "maxBytes": 10*1000**2,  # 10M
        "backupCount": 5,
        "encoding": "utf8"
    }
    if log == "-":
        config["handlers"]["console"] = console
        config["root"]["handlers"] = ["console"]
    else:
        config["handlers"]["file_handler"] = file_handler
        config["root"]["handlers"] = ["file_handler"]
    logging.config.dictConfig(config)
# end initlog


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", default="-", help="log file")
    parser.add_argument("--level", default="info")
    parser.add_argument("--eshost")
    args = parser.parse_args()

    initlog(level=args.level, log=args.l)

    url = u'{}/_cat/indices?h=status,index,pri,rep,docs.count,pri.store.size,sc'.format(args.eshost)
    logging.debug(url)

    r = requests.get(url)
    logging.debug(r)

    all_indices = {}

    for l in [e.strip() for e in r.text.split('\n') if e.strip()]:
        if 'close' in l:
            status, index = l.split()
            all_indices[index] = {'status': status}
        else:
            status, index, pri, rep, docs_count,  pri_store_size, sc = l.split()
            all_indices[index] = {'status': status, 'pri': pri, 'rep': rep,
                                  'docs_count': docs_count, 'pri_store_size': pri_store_size, 'sc': sc}
    logging.info(json.dumps(all_indices, indent=2))

    now = datetime.datetime.now()

    # test1-YYYY-mm-dd
    # delete
    for i in range(10, 15):
        date = now - datetime.timedelta(i)
        indexname = u'test1-{}'.format(date.strftime("%Y-%m-%d"))
        assert indexname not in all_indices
    # close
    for i in range(3, 10):
        date = now - datetime.timedelta(i)
        indexname = u'test1-{}'.format(date.strftime("%Y-%m-%d"))
        assert all_indices[indexname]['status'] == 'close'
    date = now - datetime.timedelta(1)
    indexname = u'test1-{}'.format(date.strftime("%Y-%m-%d"))
    url = u'{}/{}/_settings'.format(args.eshost, indexname)
    logging.debug(url)
    settings = requests.get(url).json()[indexname]['settings']
    logging.debug(settings)
    assert str(settings['index']['number_of_replicas']) == '1'
    assert settings['index']['refresh_interval'] == '30s'
    assert settings['index']['routing']['allocation']['require']['boxtype'] == 'weak'
    # merge
    date = now - datetime.timedelta(2)
    indexname = u'test1-{}'.format(date.strftime("%Y-%m-%d"))
    assert int(all_indices[indexname]['sc']) <= 1

    logging.info('test1-YYYY-mm-dd passed')

    # test1-YYYY.mm.dd
    # delete
    for i in range(8, 15):
        date = now - datetime.timedelta(i)
        indexname = u'test1-{}'.format(date.strftime("%Y.%m.%d"))
        assert indexname not in all_indices
    # close
    for i in range(3, 8):
        date = now - datetime.timedelta(i)
        indexname = u'test1-{}'.format(date.strftime("%Y.%m.%d"))
        assert all_indices[indexname]['status'] == 'close'
    date = now - datetime.timedelta(1)
    indexname = u'test1-{}'.format(date.strftime("%Y.%m.%d"))
    url = u'{}/{}/_settings'.format(args.eshost, indexname)
    logging.debug(url)
    settings = requests.get(url).json()[indexname]['settings']
    logging.debug(settings)
    assert str(settings['index']['number_of_replicas']) == '1'
    assert settings['index']['refresh_interval'] == '30s'
    assert settings['index']['routing']['allocation']['require']['boxtype'] == 'weak'
    # merge
    date = now - datetime.timedelta(2)
    indexname = u'test1-{}'.format(date.strftime("%Y.%m.%d"))
    assert int(all_indices[indexname]['sc']) <= 1
    logging.info('test1-YYYY.mm.dd passed')

    # test2-YYYY.mm.dd
    # delete
    for i in range(8, 15):
        date = now - datetime.timedelta(i)
        indexname = u'test2-{}'.format(date.strftime("%Y.%m.%d"))
        assert indexname not in all_indices
    # close
    for i in range(3, 8):
        date = now - datetime.timedelta(i)
        indexname = u'test2-{}'.format(date.strftime("%Y.%m.%d"))
        assert all_indices[indexname]['status'] == 'close'
    date = now - datetime.timedelta(1)
    indexname = u'test2-{}'.format(date.strftime("%Y.%m.%d"))
    url = u'{}/{}/_settings'.format(args.eshost, indexname)
    logging.debug(url)
    settings = requests.get(url).json()[indexname]['settings']
    logging.debug(settings)
    assert str(settings['index']['number_of_replicas']) == '1'
    assert settings['index']['refresh_interval'] == '30s'
    assert settings['index']['routing']['allocation']['require']['boxtype'] == 'weak'
    # merge
    date = now - datetime.timedelta(2)
    indexname = u'test2-{}'.format(date.strftime("%Y.%m.%d"))
    assert int(all_indices[indexname]['sc']) <= 2
    logging.info('test2-YYYY.mm.dd passed')

    # test3
    # delete
    for i in range(8, 15):
        date = now - datetime.timedelta(i)
        indexname = u'test2-{}'.format(date.strftime("%Y.%m.%d"))
        assert indexname not in all_indices
    for i in range(8):
        date = now - datetime.timedelta(i)
        indexname = u'test2-{}'.format(date.strftime("%Y.%m.%d"))
        assert indexname in all_indices
    logging.info('test3-YYYY.mm.dd passed')

    # month-YYYY.mm
    # delete
    for i in range(34, 50):
        date = now - datetime.timedelta(days=i)
        indexname = u'month-{}'.format(date.strftime("%Y.%m"))
        assert indexname not in all_indices
    for i in range(1):
        date = now - datetime.timedelta(days=i)
        indexname = u'month-{}'.format(date.strftime("%Y.%m"))
        assert indexname in all_indices
    logging.info('month-YYYY.mm passed')

    # test-YYYYMMDDHHmm-1
    # delete
    for i in range(6, 15):
        date = now - datetime.timedelta(hours=i)
        indexname = u'test-{}00-1'.format(date.strftime("%Y%m%d%H"))
        assert indexname not in all_indices
    for i in range(6):
        date = now - datetime.timedelta(hours=i)
        indexname = u'test-{}00-1'.format(date.strftime("%Y%m%d%H"))
        assert indexname in all_indices
    logging.info('test-YYYYMMDDHHmm-1 passed')


if __name__ == '__main__':
    main()
