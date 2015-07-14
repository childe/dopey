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


def get_all_indices(esclient):
    """return index list
    {
    "delete":[],
    "optimize":[],
    "reroute":[]
    }
    """

    indices = {
        "close": [],
        "delete": [],
        "optimize": [],
        "reroute": []
    }

    catClient = elasticsearch.client.CatClient(esclient)
    all_indices = catClient.indices(h="i")
    return all_indices

    #for line in r.text.split('\n'):
        #if line.strip() == '':
            #continue

        #logger.debug(line)

        #parts = line.split()

        #indexname = parts[1]
        #doc_cnt = int(parts[4])

        #if doc_cnt == 0:
            #logger.info('%s count is 0' % indexname)
            #continue
            #requests.delete('http://10.8.84.90:443/%s' % indexname)

        #r = re.findall(r'-(\d{4}\.\d{2}\.\d{2})$', indexname)
        #if not r:
            #logger.info('%s dont endswith date' % indexname)
            #continue

        #date = datetime.datetime.strptime(r[0], '%Y.%m.%d')
        #if date > datetime.datetime.now():
            #logger.info(indexname)

    #for index_prefix, v in config.iteritems():
        #for action, d in v.iteritems():
            #pass


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

    all_indices = get_all_indices(esclient)
    logging.debug(all_indices)

if __name__ == '__main__':
    main()
