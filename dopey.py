#!/usr/bin/env python
# -*- coding: utf-8 -*-
import yaml
import re
import datetime
import argparse
import logging
import logging.handlers
import logging.config
import elasticsearch
import curator


def initlog(level=None):

    if level is None:
        level = logging.DEBUG if __debug__ else logging.INFO

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "simple": {
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s"
            },
            "verbose": {
                "format": "%(asctime)s %(levelname)s %(module)s:%(lineno)d %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "verbose",
                "stream": "ext://sys.stdout"
            },
            "file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "verbose",
                "filename": "dopey.log",
                "maxBytes": 10*1000**3,  # 10M
                "backupCount": 5,
                "encoding": "utf8"
            }
        },
        'root': {
            'level': level,
            'handlers': ['file_handler', 'console']
        },
        "loggers": {
            "myloger": {
                "level": level,
                "handlers": [
                    "console"
                ],
            }
        },
    }
    logging.config.dictConfig(config)


def filter_indices(all_indices, indices_config):
    """return action indices, and not_involved indices """

    indices = {
        "close": [],
        "delete": [],
        "optimize": [],
        "reallocate": []
    }

    not_involved = []

    today = datetime.date.today()

    #indices_timedelta = {}
    for indexname in all_indices:
        logging.debug(indexname)
        r = re.findall(r'-(\d{4}\.\d{2}\.\d{2})$', indexname)
        if r:
            date = datetime.datetime.strptime(r[0], '%Y.%m.%d')
        else:
            r = re.findall(r'-(\d{4}\.\d{2})$', indexname)
            if r:
                date = datetime.datetime.strptime(r[0], '%Y.%m')
            else:
                logging.warn('%s dont endswith date' % indexname)
                continue
        date = date.date()

        logging.debug(date)
        logging.debug(today-date)
        #indices_timedelta[indexname] = today - date

        for index_prefix, config in indices_config.items():
            # if not indexname.startswith(index_prefix):
            if not (re.match(r'%s\d{4}\.\d{2}\.\d{2}' % index_prefix, indexname)
                    or re.match(r'%s\d{4}\.\d{2}' % index_prefix, indexname)):
                continue
            for action, d in config.items():
                if action == "optimize":
                    if datetime.timedelta(d) == today-date:
                        indices["optimize"].append(indexname)
                else:
                    if datetime.timedelta(d) <= today-date:
                        indices[action].append(indexname)
            break
        else:
            not_involved.append(indexname)

    return indices, not_involved


def main():
    initlog()

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", default="dopey.yaml", help="yaml config")
    args = parser.parse_args()

    config = yaml.load(open(args.c))

    eshosts = config.get("esclient")
    if eshosts is not None:
        esclient = elasticsearch.Elasticsearch(eshosts)
    else:
        esclient = elasticsearch.Elasticsearch()

    all_indices = curator.get_indices(esclient)
    logging.debug(all_indices)
    return

    action_indices, not_involved = filter_indices(
        all_indices, config['indices'])
    logging.info(action_indices)
    logging.info(not_involved)

    if action_indices['close']:
        curator.close_indices(esclient, action_indices['close'])

    if action_indices['delete']:
        curator.delete(esclient, action_indices['delete'])

    for index in  action_indices['optimize']:
        curator.optimize_index(esclient,index)

if __name__ == '__main__':
    main()
