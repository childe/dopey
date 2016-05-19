#!/usr/bin/env python
# -*- coding: utf-8 -*-
import yaml
import re
import datetime
import time
import json
import argparse
from threading import Thread,Lock
import logging
import logging.handlers
import logging.config
import smtplib
from email.mime.text import MIMEText
import elasticsearch
import curator


import logging
import logging.config


def initlog(level=None, log="-"):
    if level is None:
        level = logging.DEBUG if __debug__ else logging.INFO
    if isinstance(level, basestring):
        level = getattr(logging, level.upper())

    class MyFormatter(logging.Formatter):

        def format(self, record):
            dformatter = '[%(asctime)s] %(levelname)s %(thread)d %(name)s %(pathname)s %(lineno)d - %(message)s'
            formatter = '[%(asctime)s] %(levelname)s %(thread)d %(name)s %(message)s'
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
                "format": "[%(asctime)s] %(levelname)s %(thread)d %(name)s %(message)s"
            },
            "verbose": {
                "format": "[%(asctime)s] %(levelname)s %(thread)d %(name)s %(pathname)s %(lineno)d - %(message)s"
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
        "formatter": "custom",
        "stream": "ext://sys.stdout"
    }
    file_handler = {
        "class": "logging.handlers.RotatingFileHandler",
        "level": "DEBUG",
        "formatter": "custom",
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


logger = None
lock = Lock()

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

_delete = []
_close = []
_optimize = []
_dealt = []


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
    _delete.extend(indices)
    logger.debug("try to delete %s" % ','.join(indices))
    global lock
    with lock:
        for index in indices:
            if curator.delete_indices(esclient, [index], master_timeout='300s'):
                logger.info('%s deleted' % index)
                dopey_summary.add(u'%s 己删除' % index)
            else:
                logger.warn('%s deleted failed' % index)
                dopey_summary.add(u'%s 删除失败' % index)


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
    _close.extend(indices)
    logger.debug("try to close %s" % ','.join(indices))
    for index in indices:
        if curator.close_indices(esclient, [index]):
            logger.info('%s closed' % index)
            dopey_summary.add(u'%s 已关闭' % index)
        else:
            logger.warn('%s closed failed' % index)
            dopey_summary.add(u'%s 关闭失败' % index)


def optimize_index(esclient, index, settings):
    dopey_summary.add(u"%s optimize 开始" % index)
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
    _optimize.extend(indices)
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
        rule=settings.get("rule"))

    # while True:
        # relo_cnt = get_relo_index_cnt(esclient)
        # logger.info("relocation indices count: %s" % relo_cnt)
        # if relo_cnt == 0:
            # break
        # time.sleep(10*60)
    dopey_summary.add(u"%s reallocate 已经开始" % ",".join(indices))


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
        body={"index.number_of_replicas": 0},
        params = {'master_timeout':'300s'}
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
            body={"index.number_of_replicas": replic},
            params = {'master_timeout':'300s'}
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
        r = re.findall(
            r'^%s(\d{4}\.\d{2}\.\d{2})$' % index_prefix,
            indexname)
        if r:
            date = datetime.datetime.strptime(r[0], '%Y.%m.%d')
            rst.append(indexname)
        else:
            r = re.findall(
                r'^%s(\d{4}\.\d{2})$' % index_prefix,
                indexname)
            if r:
                date = datetime.datetime.strptime(r[0], '%Y.%m')
                rst.append(indexname)
            else:
                continue

        date = date.date()
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

    # TODO 如果一个索引需要删除, 别的action里面可以直接去掉

    for e in index_config:
        action, settings = e.keys()[0], e.values()[0]
        logger.debug(action)
        logger.debug([e[0] for e in actions.get(action, [])])
        try:
            eval(action)(esclient, actions.get(action), settings)
        except Exception as e:
            logging.warn('%s action failed: %s' % (action, e))

    _dealt.extend(rst)
    return rst


def main():
    global logger
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", default="dopey.yaml", help="yaml config")
    parser.add_argument(
        "-l",
        default="-",
        help="log file")
    parser.add_argument("--level", default="info")
    args = parser.parse_args()

    config = yaml.load(open(args.c))

    initlog(
        level=args.level, log=config["l"]
        if "log" in config else args.l)
    logger = logging.getLogger("dopey")

    eshosts = config.get("esclient")
    logger.debug(eshosts)
    if eshosts is not None:
        esclient = elasticsearch.Elasticsearch(eshosts, timeout=300)
    else:
        esclient = elasticsearch.Elasticsearch(timeout=300)

    all_indices = curator.get_indices(esclient)
    logger.debug("all_indices: {}".format(all_indices))
    if all_indices is False:
        raise Exception("could not get indices")

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

    not_dealt = list(set(all_indices).difference(_dealt))
    dopey_summary.add(
        json.dumps(
            {"not_dealt": not_dealt, "delete": _delete, "close": _close,
             "optimize": _optimize}, indent=2))
    sumary_config = config.get("sumary")
    for action, kargs in sumary_config.items():
        if kargs:
            getattr(dopey_summary, action)(**kargs)
        else:
            getattr(dopey_summary, action)()


if __name__ == '__main__':
    main()
