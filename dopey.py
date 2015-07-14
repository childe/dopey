#!/usr/bin/env python
# -*- coding: utf-8 -*-
import yaml
import re
import datetime
import json
import time
import argparse
from threading import Thread
import logging
import logging.handlers
import logging.config
import elasticsearch
import curator

records = []


def initlog(level=None):

    if level is None:
        level = logging.DEBUG if __debug__ else logging.INFO

    formatter = logging.Formatter("%(asctime)s - %(levelname)s %(name)s - %(message)s")
    handler = logging.handlers.RotatingFileHandler(
              "dopey.log", maxBytes=50000000, backupCount=2)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)


def filter_indices(all_indices, indices_config):
    """return action indices, and not_involved indices """

    indices = {
        "close": set(),
        "delete": set(),
        "optimize": set(),
        "reallocate": set()
    }

    not_involved = set()

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
                not_involved.add(indexname)
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
                        indices["optimize"].add(indexname)
                else:
                    if datetime.timedelta(d) <= today-date:
                        indices[action].add(indexname)
            break
        else:
            not_involved.add(indexname)

    indices["reallocate"].difference_update(indices["delete"])
    for k, v in indices.items():
        indices[k] = list(v)
    not_involved = list(not_involved)
    return indices, not_involved


def get_relo_index_cnt(esclient):
    cnt = elasticsearch.client.CatClient(esclient).health(h="relo")
    return int(cnt)


def close_indices(esclient, indices):
    if not indices:
        return
    logging.debug("try to close %s" % ','.join(indices))
    if curator.close_indices(esclient, indices):
        logging.info('indices closed: %s' % ','.join(indices))


def delete_indices(esclient, indices):
    if not indices:
        return
    logging.debug("try to delete %s" % ','.join(indices))
    if curator.delete_indices(esclient, indices):
        logging.info('indices deleted: %s' % ','.join(indices))


def optimize_index(esclient, index):
    if curator.optimize_index(
            esclient,
            index,
            max_num_segments=1,
            request_timeout=10 *
            3600):
        logging.info('%s optimized' % index)


def optimize_indices(esclient, indices):
    if not indices:
        return
    logging.debug("try to optimize %s" % ','.join(indices))
    for index in indices:
        Thread(target=optimize_index, args=(esclient, index,)).start()


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

    action_indices, not_involved = filter_indices(
        all_indices, config['indices'])
    logging.info(action_indices)
    records.append('all actions: \n%s' % json.dumps(action_indices, indent=2))
    logging.info(not_involved)
    records.append(
        'indices not configured: \n%s' %
        json.dumps(
            not_involved,
            indent=2))

    close_indices(esclient, action_indices['close'])
    delete_indices(esclient, action_indices['delete'])

    curator.api.allocation(
        esclient,
        list(action_indices['reallocate']),
        rule="tag=cores8")

    while True:
        relo_cnt = get_relo_index_cnt(esclient)
        logging.info("relocation indices count: %s" % relo_cnt)
        if relo_cnt == 0:
            break
        time.sleep(10*60)

    optimize_indices(esclient, action_indices['optimize'])

if __name__ == '__main__':
    main()
    #print >> open('a', 'a'), '\n'.join(records)
