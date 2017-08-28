#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
import requests

import json
import re
import datetime
import argparse
from threading import Thread, Lock
import smtplib
from email.mime.text import MIMEText
import logging.handlers
import logging
import logging.config

import utils

config = {}


def initlog(level=None, log="-"):
    if level is None:
        level = logging.DEBUG if __debug__ else logging.INFO
    if isinstance(level, basestring):
        level = getattr(logging, level.upper())

    class MyFormatter(logging.Formatter):

        def format(self, record):
            dformatter = "[%(asctime)s] %(levelname)s %(thread)d %(name)s %(pathname)s %(lineno)d - %(message)s"
            formatter = "[%(asctime)s] %(levelname)s %(thread)d %(name)s %(message)s"
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
                "()": MyFormatter
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
        "root": {
            "level": level,
            "handlers": ["console"]
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
            "[%s] %s" %
            (datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"), record))

    @property
    def sumary(self):
        return "\n".join(self.records)

    def prints(self):
        print self.sumary.encode("utf-8")

    def log(self):
        logging.getLogger("DopeySumary").info(self.sumary)

    def mail(
            self,
            mail_host=None,
            from_who=None,
            to_list=None,
            login_user=None,
            login_password=None,
            sub="dopey summary"):
        content = self.sumary
        content = content.encode("utf-8")

        msg = MIMEText(content)
        msg["Subject"] = sub
        msg["From"] = from_who
        msg["To"] = ";".join(to_list)
        try:
            s = smtplib.SMTP()
            s.connect(mail_host)
            if login_user is not None:
                s.login(login_user, login_password)
            s.sendmail(from_who, to_list, msg.as_string())
            s.close()
        except Exception as e:
            logging.error(str(e))


dopey_summary = Sumary()

_delete = []
_close = []
_optimize = []
_dealt = []
_update_settings = []


def update_cluster_settings(settings):
    """
    :type settings: cluster settings
    :rtype: response
    """
    global config
    logging.info("update cluster settings: %s" % settings)
    try:
        url = u"{}/_cluster/settings".format(config["eshost"])
        logging.debug(u"update cluster by {}: {}".format(url, settings))
        r = requests.put(
            url, data=json.dumps(settings), params={
                "master_timeout": "300s"})
        return r.ok
    except Exception as e:
        logging.error("failed to update cluster settings. %s" % e)
        return False


def process(
        all_indices,
        index_prefix,
        index_config,
        base_day,
        action_filters):
    """
    :type all_indices: list of str
    :type index_prefix: str
    :type index_config: list of actions
    :rtype: list of indexname
    """
    actions = {}
    rst = []

    for indexname in all_indices:
        r = re.findall(
            r"^%s(\d{4}\.\d{2}\.\d{2})$" % index_prefix,
            indexname)
        if r:
            date = datetime.datetime.strptime(r[0], "%Y.%m.%d")
            rst.append(indexname)
        else:
            r = re.findall(
                r"^%s(\d{4}\.\d{2})$" % index_prefix,
                indexname)
            if r:
                date = datetime.datetime.strptime(r[0], "%Y.%m")
                rst.append(indexname)
            else:
                continue

        date = date.date()
        for e in index_config:
            action, settings = e.keys()[0], e.values()[0]
            offset = base_day-date
            if indexname in [e[0] for e in actions.get("delete_indices", [])]:
                continue
            if "day" in settings and offset == datetime.timedelta(
                    settings["day"]) or "days" in settings and offset >= datetime.timedelta(
                    settings["days"]):
                actions.setdefault(action, [])
                index_settings = get_index_settings(config, indexname)
                actions[action].append((indexname, index_settings))

    # TODO 如果一个索引需要删除, 别的action里面可以直接去掉

    for e in index_config:
        action, settings = e.keys()[0], e.values()[0]
        logger.debug(action)
        if action not in action_filters:
            logger.info("skip %s" % action)
            continue
        logger.debug([e[0] for e in actions.get(action, [])])
        try:
            eval(action)(actions.get(action), settings)
        except Exception as e:
            logging.warn("%s action failed: %s" % (action, e))

    _dealt.extend(rst)
    return rst


def _get_base_day(base_day):
    try:
        int(base_day)
    except BaseException:
        return datetime.datetime.strptime(base_day, r"%Y-%m-%d").date()
    else:
        return (
            datetime.datetime.now() +
            datetime.timedelta(
                int(base_day))).date()


def _get_action_filters(action_filters_arg):
    action_filters_mapping = {
        "c": "close_indices",
        "d": "delete_indices",
        "u": "update_settings",
        "f": "optimize_indices",
    }
    if action_filters_arg == "":
        return action_filters_mapping.values()
    try:
        return [action_filters_mapping[k]
                for k in action_filters_arg.split(",")]
    except BaseException:
        raise Exception("unrecognizable action filters")


def pre_process_index_config(index_config):
    """
    type index_config: list[{}]
    """
    action_weight = {
        "update_settings": 0,
        "delete_indices": 1,
        "close_indices": 2,
        "close_indices": 3,
        "optimize_indices": 4,
    }
    index_config.sort(key=lambda x: action_weight[x.keys()[0]])
    return index_config


def main():
    global logger

    today = datetime.date.today()

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", default="dopey.yaml", help="yaml config file")
    parser.add_argument(
        "--base-day", default="0",
        help="number 0(today), 1(tommorow), -1(yestoday), or string line 2011-11-11")
    parser.add_argument(
        "--action-filters",
        default="",
        help="comma splited. d:delete, c:close, u:update settings, f:forcemerge. leaving blank means do all the actions configuared in config file")
    parser.add_argument(
        "-l",
        default="-",
        help="log file")
    parser.add_argument("--level", default="info")
    args = parser.parse_args()

    global config
    config = yaml.load(open(args.c))

    initlog(
        level=args.level, log=config["l"]
        if "log" in config else args.l)
    logger = logging.getLogger("dopey")

    all_indices = get_indices(config)
    if all_indices is False:
        raise Exception("could not get indices")
    logger.debug(u"all_indices: {}".format(all_indices))

    for action in config.get("setup", []):
        settings = action.values()[0]
        eval(action.keys()[0])(settings)

    base_day = _get_base_day(args.base_day)
    logging.info("base day is %s" % base_day)
    action_filters = _get_action_filters(args.action_filters)

    if 'update_settings' in action_filters:
        to_update_indices = get_to_update_indices(
            config, all_indices, base_day)
        logging.info(
            u"to_update_indices: %s".format(
                ' '.join(to_update_indices)))
        update_settings(config, indices, batch=50)

    if 'delete_indices' in action_filters:
        to_delete_indices = get_to_delete_indices(
            config, all_indices, base_day)
        logging.info(
            u"to_delete_indices: %s".format(
                ' '.join(to_delete_indices)))
        delete_indices(config, indices, batch=50)

    if 'close_indices' in action_filters:
        to_close_indices = get_to_close_indices(
            config, all_indices, base_day)
        logging.info(
            u"to_close_indices: %s".format(
                ' '.join(to_close_indices)))
        close_indices(config, indices, batch=50)

    if 'optimize_indices' in action_filters:
        to_optimize_indices = get_to_optimize_indices(
            config, all_indices, base_day)
        logging.info(
            u"to_optimize_indices: %s".format(
                ' '.join(to_optimize_indices)))
        optimize_indices(config, indices, batch=50)

    dopey_summary.add(
        u"未处理:\n{}\n删除:\n{}\n关闭:\n{}\n优化:{}\n更新索配置:{}".format(
            "\n".join(sorted(not_dealt)),
            "\n".join(sorted(_delete)),
            "\n".join(sorted(_close)),
            "\n".join(sorted(_optimize)),
            "\n".join(sorted(_update_settings))))

    for action in config.get("teardown", []):
        settings = action.values()[0]
        eval(action.keys()[0])(settings)

    sumary_config = config.get("sumary")
    for action, kargs in sumary_config.items():
        if kargs:
            getattr(dopey_summary, action)(**kargs)
        else:
            getattr(dopey_summary, action)()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(e)
