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
        print self.sumary.encode('utf-8')

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


def get_relo_index_cnt(esclient):
    cnt = elasticsearch.client.CatClient(esclient).health(h="relo")
    return int(cnt)


def delete_indices(esclient, indices, settings):
    """
    :type esclient: elasticsearch.Elasticsearch
    :type indices: list of (indexname,index_settings)
    :type settings: dict, not used
    :rtype: None
    """
    if not indices:
        return
    indices = [e[0] for e in indices]
    logger.debug("try to delete %s" % ','.join(indices))
    for index in indices:
        if curator.delete_indices(esclient, [index]):
            logger.info('%s deleted' % index)
            dopey_summary.add(u'%s 己删除' % index)


def close_indices(esclient, indices, settings):
    """
    :type esclient: elasticsearch.Elasticsearch
    :type indices: list of (indexname,index_settings)
    :type settings: dict, not used
    :rtype: None
    """
    if not indices:
        return
    indices = [e[0] for e in indices]
    logger.debug("try to close %s" % ','.join(indices))
    if curator.close_indices(esclient, indices):
        logger.info('indices closed: %s' % ','.join(indices))
        dopey_summary.add(u'indices 已关闭: %s' % ','.join(indices))


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


def optimize_indices(esclient, indices, settings):
    """
    :type esclient: elasticsearch.Elasticsearch
    :type indices: list of (indexname,index_settings)
    :type settings: dict, max_num_segments setting and so on
    :rtype: None
    """
    if not indices:
        return []

    indices = [e[0] for e in indices]
    logger.debug("try to optimize %s" % ','.join(indices))

    for index in indices:
        optimize_index(esclient, index, settings)


def reallocate_indices(esclient, indices, settings):
    """
    :type esclient: elasticsearch.Elasticsearch
    :type indices: list of (indexname,index_settings)
    :type settings: dict, max_num_segments setting and so on
    :rtype: None
    """
    if not indices:
        return []

    indices = [e[0] for e in indices]
    dopey_summary.add(u"%s 开始reallocate" % ",".join(indices))
    curator.api.allocation(
        esclient,
        indices,
        rule="tag=cores8")

    while True:
        relo_cnt = get_relo_index_cnt(esclient)
        logger.info("relocation indices count: %s" % relo_cnt)
        if relo_cnt == 0:
            break
        time.sleep(10*60)
    dopey_summary.add(u"%s reallocate完成" % ",".join(indices))


def close_replic(esclient, indices, settings):
    """
    :type esclient: elasticsearch.Elasticsearch
    :type indices: list of (indexname,index_settings)
    :type settings: dict, not used
    :rtype: None
    """
    if not indices:
        return

    indices = [e[0] for e in indices]
    logger.debug("try to close replic, %s" % ','.join(indices))
    dopey_summary.add(u"%s 关闭replic" % ",".join(indices))
    index_client = elasticsearch.client.IndicesClient(esclient)
    index_client.put_settings(
        index=",".join(indices),
        body={"index.number_of_replicas": 0}
    )


def open_replic(esclient, indices, settings):
    """
    :type esclient: elasticsearch.Elasticsearch
    :type indices: list of (indexname,index_settings)
    :type settings: dict, not used
    :rtype: None
    """
    if not indices:
        return
    logger.debug("try to open replic, %s" % ','.join([e[0] for e in indices]))
    dopey_summary.add(u"%s 打开replic" % ",".join([e[0] for e in indices]))
    index_client = elasticsearch.client.IndicesClient(esclient)
    for index, index_settings in indices:
        replic = index_settings[index]['settings'][
            'index']['number_of_replicas']
        index_client.put_settings(
            index=index,
            body={"index.number_of_replicas": replic}
        )


def process(esclient, all_indices, index_prefix, index_config):
    """
    :type esclient: elasticsearch.Elasticsearch
    :type all_indices: list of str
    :type index_prefix: str
    :type index_config: list of actions
    :rtype: list of indexname
    """
    index_client = elasticsearch.client.IndicesClient(esclient)
    today = datetime.date.today()
    actions = {}
    rst = []

    for indexname in all_indices:
        logging.debug("indexname: "+indexname)
        r = re.findall(
            r'%s(\d{4}\.\d{2}\.\d{2})$' % index_prefix,
            indexname)
        if r:
            date = datetime.datetime.strptime(r[0], '%Y.%m.%d')
            rst.append(indexname)
        else:
            r = re.findall(
                r'%s(\d{4}\.\d{2})$' % index_prefix,
                indexname)
            if r:
                date = datetime.datetime.strptime(r[0], '%Y.%m')
                rst.append(indexname)
            else:
                continue

        date = date.date()
        logging.debug("date: %s" % date)
        for e in index_config:
            action, settings = e.keys()[0], e.values()[0]
            offset = today-date
            if indexname in [e[0] for e in actions.get("delete_indices", [])]:
                continue
            if "day" in settings and offset == datetime.timedelta(settings["day"]) or \
                    "days" in settings and offset >= datetime.timedelta(settings["days"]):
                actions.setdefault(action, [])
                index_settings = index_client.get_settings(
                    index=indexname)
                actions[action].append((indexname, index_settings))

    # 如果一个索引需要删除, 别的action里面可以直接去掉

    for e in index_config:
        action, settings = e.keys()[0], e.values()[0]
        logger.debug(action)
        logger.debug([e[0] for e in actions.get(action, [])])
        eval(action)(esclient, actions.get(action), settings)

    return rst


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
    logger.debug("all_indices: {}".format(all_indices))

    process_threads = []
    for index_prefix, index_config in config.get("indices").items():
        t = Thread(
            target=process,
            args=(
                esclient,
                all_indices,
                index_prefix,
                index_config))
        t.start()
        process_threads.append(t)

    for t in process_threads:
        t.join()

    sumary_config = config.get("sumary")
    for action, kargs in sumary_config.items():
        if kargs:
            getattr(dopey_summary, action)(**kargs)
        else:
            getattr(dopey_summary, action)()


if __name__ == '__main__':
    main()
