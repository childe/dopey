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
import smtplib
from email.mime.text import MIMEText
import elasticsearch
import curator


class Sumary(object):

    def __init__(self):
        super(Sumary, self).__init__()
        self.records = []

    def add(self, record):
        self.records.append(
            '[%s] %s' %
            (datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S'), record))

    @property
    def sumary(self):
        return '\n'.join(self.records)

    def prints(self):
        print self.sumary

    def log(self):
        logging.getLogger("DopeySumary").info(self.sumary)

    def mail(self, mail_host, from_who, to_list, sub="dopey summary"):
        content = self.sumary
        content = content.encode('utf-8')

        msg = MIMEText(content)
        msg['Subject'] = sub
        msg['From'] = from_who
        msg['To'] = ";".join(to_list)
        try:
            s = smtplib.SMTP()
            s.connect(mail_host)
            s.sendmail(from_who, to_list, msg.as_string())
            s.close()
        except Exception as e:
            logging.error(str(e))


dopey_summary = Sumary()


def initlog(level=None, logfile="/var/log/dopey/dopey.log"):

    if level is None:
        level = logging.DEBUG if __debug__ else logging.INFO
    else:
        level = getattr(logging, level.upper())

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s %(name)s - %(message)s")
    handler = logging.handlers.RotatingFileHandler(
        logfile, maxBytes=50000000, backupCount=2)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger('')
    root_logger.setLevel(level)
    root_logger.addHandler(handler)

logger = logging.getLogger("dopey")


def filter_indices(esclient, all_indices, indices_config):
    """return action indices, and not_involved indices """

    index_client = elasticsearch.client.IndicesClient(esclient)

    indices = {
        "close": dict(),
        "delete": dict(),
        "optimize": dict(),
        "optimize_nowait": dict(),
        "reallocate": dict()
    }

    close_replic_indices = dict()

    not_involved = set()

    today = datetime.date.today()

    #indices_timedelta = {}
    for indexname in all_indices:
        logger.debug(indexname)
        r = re.findall(r'(\d{4}\.\d{2}\.\d{2})$', indexname)
        if r:
            date = datetime.datetime.strptime(r[0], '%Y.%m.%d')
        else:
            r = re.findall(r'-(\d{4}\.\d{2})$', indexname)
            if r:
                date = datetime.datetime.strptime(r[0], '%Y.%m')
            else:
                logger.warn('%s dont endswith date' % indexname)
                not_involved.add(indexname)
                continue
        date = date.date()
        if (date > today):
            not_involved.add(indexname)

        logger.debug(date)
        logger.debug(today-date)
        #indices_timedelta[indexname] = today - date

        for index_prefix, config in indices_config.items():
            # if not indexname.startswith(index_prefix):
            if not (re.match(r'%s\d{4}\.\d{2}\.\d{2}' % index_prefix, indexname)
                    or re.match(r'%s\d{4}\.\d{2}' % index_prefix, indexname)):
                continue

            settings = config.get("settings", {})

            optimize_nowait = settings.get("optimize_nowait", True)

            for action, v in config.items():
                if action == "settings":
                    continue
                if action == "optimize":
                    if datetime.timedelta(v) == today-date:
                        if settings.get("close_replic", False):
                            index_settings = index_client.get_settings(
                                index=indexname)
                            close_replic_indices[indexname] = index_settings[
                                indexname]['settings']['index'][
                                'number_of_replicas']

                        if optimize_nowait is False:
                            indices["optimize"][indexname] = settings
                        else:
                            indices["optimize_nowait"][indexname] = settings
                else:
                    if datetime.timedelta(v) <= today-date:
                        indices[action][indexname] = settings
            break
        else:
            not_involved.add(indexname)

    indices["reallocate"] = dict([(k,v) for k,v in indices["reallocate"].items() if k not in indices["delete"]])
    indices["close"] = dict([(k,v) for k,v in indices["close"].items() if k not in indices["delete"]])

    not_involved = list(not_involved)

    return indices, close_replic_indices, not_involved


def get_relo_index_cnt(esclient):
    cnt = elasticsearch.client.CatClient(esclient).health(h="relo")
    return int(cnt)


def close_indices(esclient, indices):
    if not indices:
        return
    indices = indices.keys()
    logger.debug("try to close %s" % ','.join(indices))
    if curator.close_indices(esclient, indices):
        logger.info('indices closed: %s' % ','.join(indices))


def delete_indices(esclient, indices):
    if not indices:
        return
    indices = indices.keys()
    logger.debug("try to delete %s" % ','.join(indices))
    for index in indices:
        if curator.delete_indices(esclient, [index]):
            logger.info('%s deleted' % index)


def optimize_index(esclient, index, settings):
    try:
        if curator.optimize_index(
                esclient,
                index,
                max_num_segments=settings.get("max_num_segments", 1),
                request_timeout=18 *
                3600):
            logger.info('%s optimized' % index)
            dopey_summary.add(u"%s optimize 完成" % index)
        else:
            raise
    except:
        logger.info(u"%s optimize 未完成退出" % index)
        dopey_summary.add(u"%s optimize 未完成退出" % index)


def optimize_indices(esclient, indices):
    if not indices:
        return []
    logger.debug("try to optimize %s" % ','.join(indices.keys()))

    threads = []
    for index, settings in indices.items():
        t = Thread(target=optimize_index, args=(esclient, index, settings))
        t.start()
        threads.append(t)

    return threads


def close_replic(esclient, indices):
    if not indices:
        return
    logger.debug("try to close replic, %s" % ','.join(indices))
    index_client = elasticsearch.client.IndicesClient(esclient)
    index_client.put_settings(
        index=",".join(indices),
        body={"index.number_of_replicas": 0}
    )


def recovery_replic(esclient, indices):
    if not indices:
        return
    logger.debug("try to recover replic, %s" % ','.join(indices.keys()))
    index_client = elasticsearch.client.IndicesClient(esclient)
    for index, replic in indices.items():
        index_client.put_settings(
            index=index,
            body={"index.number_of_replicas": replic}
        )



def optimizea_and_then_reallocate(esclient,action_indices):
    dopey_summary.add(u"开始optimize需要等待的索引")
    optimize_threads = optimize_indices(esclient, action_indices['optimize'])
    for t in optimize_threads:
        t.join()

    dopey_summary.add(u"开始reallocate索引")
    curator.api.allocation(
        esclient,
        list(action_indices['reallocate']),
        rule="tag=cores8")

    while True:
        relo_cnt = get_relo_index_cnt(esclient)
        logger.info("relocation indices count: %s" % relo_cnt)
        if relo_cnt == 0:
            break
        time.sleep(10*60)
    dopey_summary.add(u"reallocate索引完成")

def reallocate_and_then_optimize(esclient,action_indices):
    dopey_summary.add(u"开始reallocate索引")
    curator.api.allocation(
        esclient,
        list(action_indices['reallocate']),
        rule="tag=cores8")

    while True:
        relo_cnt = get_relo_index_cnt(esclient)
        logger.info("relocation indices count: %s" % relo_cnt)
        if relo_cnt == 0:
            break
        time.sleep(10*60)
    dopey_summary.add(u"reallocate索引完成")

    dopey_summary.add(u"开始optimize需要等待的索引")
    optimize_threads = optimize_indices(esclient, action_indices['optimize'])

    for t in optimize_threads:
        t.join()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", default="dopey.yaml", help="yaml config")
    parser.add_argument(
        "-l",
        default="/var/log/dopey/dopey.log",
        help="log file")
    parser.add_argument("--level", default="info")
    args = parser.parse_args()

    config = yaml.load(open(args.c))

    initlog(
        level=args.level, logfile=config["log"]
        if "log" in config else args.l)

    eshosts = config.get("esclient")
    if eshosts is not None:
        esclient = elasticsearch.Elasticsearch(eshosts)
    else:
        esclient = elasticsearch.Elasticsearch()

    all_indices = curator.get_indices(esclient)
    logger.debug(all_indices)

    action_indices, close_replic_indices, not_involved = filter_indices(
        esclient, all_indices, config['indices'])

    logger.info(action_indices)
    dopey_summary.add(
        u"今日维护工作: \n%s" %
        json.dumps(
            dict([(k,v.keys()) for k,v in action_indices.items()]),
            indent=2))

    logger.info(close_replic_indices)
    dopey_summary.add(
        u"需要先关闭replic的索引: \n%s" %
        json.dumps(
            close_replic_indices,
            indent=2))

    logger.info(not_involved)
    dopey_summary.add(
        u"未配置的索引: \n%s" %
        json.dumps(
            not_involved,
            indent=2))

    dopey_summary.add(u"开始关闭replic")
    close_replic(esclient, close_replic_indices.keys())
    dopey_summary.add(u"replic已经关闭")

    dopey_summary.add(u"开始关闭索引")
    close_indices(esclient, action_indices['close'])
    dopey_summary.add(u"索引已经关闭")

    dopey_summary.add(u"开始删除索引")
    delete_indices(esclient, action_indices['delete'])
    dopey_summary.add(u"索引已经删除")


    reallocate =  config.get("reallocate","first")
    if reallocate.lower()!="last":
        reallocate = "first"

    dopey_summary.add(u"开始optimize不需要等待的索引")
    optimize_nowait_threads = optimize_indices(
        esclient,
        action_indices['optimize_nowait'])

    if reallocate == "first":
        reallocate_and_then_optimize(esclient,action_indices)
    else:
        optimizea_and_then_reallocate(esclient,list(action_indices['reallocate']))

    for t in optimize_nowait_threads:
        t.join()

    logger.info(u"开始恢复replic")
    dopey_summary.add(u"开始恢复replic")
    recovery_replic(esclient, close_replic_indices)
    logger.info(u"已经恢复replic配置")
    dopey_summary.add(u"开始恢复replic配置")

    sumary_config = config.get("sumary")
    for action, kargs in sumary_config.items():
        if kargs:
            getattr(dopey_summary, action)(**kargs)
        else:
            getattr(dopey_summary, action)()


if __name__ == '__main__':
    main()
