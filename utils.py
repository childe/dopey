#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

import datetime
import logging
import re
import json


def _compare_index_settings(part, whole):
    """
    return True if part is part of whole
    type part: dict or else
    type whole: dict or else
    rtype: boolean
    >>> whole={"index":{"routing":{"allocation":{"include":{"group":"4,5"},"total_shards_per_node":"2"}},"refresh_interval":"60s","number_of_shards":"20","store":{"type":"niofs"},"number_of_replicas":"1"}}
    >>> part={"index":{"routing":{"allocation":{"include":{"group":"4,5"}}}}}
    >>> _compare_index_settings(part, whole)
    >>> part={"index":{"routing":{"allocation":{"include":{"group":"5"}}}}}
    >>> _compare_index_settings(part, whole)
    False
    """
    if part == whole:
        return True
    if part is None and whole is None:
        return True
    if part is None or whole is None:
        return (part, whole)
    if not isinstance(part, type(whole)):
        return (part, whole)
    if not isinstance(part, dict):
        return part == whole
    for k, v in part.items():
        r = _compare_index_settings(v, whole.get(k))
        if r is not True:
            return r
    return True


def get_indices(config):
    all_indices = []
    eshost = config["eshost"]
    url = "{}/_cat/indices?h=i".format(eshost)
    logging.debug(u"get all indices from {}".format(url))
    try:
        r = requests.get(url)
        for i in r.text.split():
            i = i.strip()
            if i == "":
                continue
            all_indices.append(i)
        return all_indices
    except BaseException:
        return False


def get_index_settings(config, indexname):
    url = u"{}/{}/_settings".format(config['eshost'], indexname)
    try:
        return requests.get(url).json()[indexname]['settings']
    except Exception as e:
        logging.error(
            u"could not get {} settings: {}".format(
                indexname, str(e)))
        return {}


def get_to_process_indices(to_select_action, config, all_indices, base_day):
    """
    rtype: [(indexname, index_settings, dopey_index_settings)]
    """
    rst = []

    for index_prefix, index_config in config['indices'].items():
        for indexname in all_indices:
            r = re.findall(
                r"^%s(\d{4}\.\d{2}\.\d{2})$" % index_prefix,
                indexname)
            if r:
                date = datetime.datetime.strptime(r[0], "%Y.%m.%d")
            else:
                r = re.findall(
                    r"^%s(\d{4}\.\d{2})$" % index_prefix,
                    indexname)
                if r:
                    date = datetime.datetime.strptime(r[0], "%Y.%m")
                else:
                    continue

            for e in index_config:
                action, configs = e.keys()[0], e.values()[0]
                if action != to_select_action:
                    continue
                offset = base_day-date.date()
                if "day" in configs and offset == datetime.timedelta(
                        configs["day"]) or "days" in configs and offset >= datetime.timedelta(
                        configs["days"]):
                    index_settings = get_index_settings(config, indexname)
                    rst.append((indexname, index_settings, configs['settings']))

    return rst


def get_to_delete_indices(config, all_indices, base_day):
    return get_to_process_indices(
        'delete_indices', config, all_indices, base_day)


def get_to_close_indices(config, all_indices, base_day):
    return get_to_process_indices(
        'close_indices', config, all_indices, base_day)


def get_to_update_indices(config, all_indices, base_day):
    return get_to_process_indices(
        'update_settings', config, all_indices, base_day)


def get_to_optimize_indices(config, all_indices, base_day):
    return get_to_process_indices(
        'optimize_indices', config, all_indices, base_day)


def delete_indices(config, indices, batch=50):
    """
    :type indices: list of (indexname,index_settings, dopey_index_settings)
    :rtype: None
    """
    if not indices:
        return

    indices = [e[0] for e in indices]

    logging.debug(u"try to delete %s" % ",".join(indices))
    while indices:
        to_delete_indices = indices[:batch]
        to_delete_indices_joined = ','.join(to_delete_indices)
        url = u"{}/{}".format(config['eshost'], to_delete_indices_joined)
        logging.info(u"delete: {}".format(url))

        r = requests.delete(
            url, timeout=300, params={
                "master_timeout": "10m"})
        if r.ok:
            logging.info(u"%s deleted" % to_delete_indices_joined)
            # dopey_summary.add(u"%s 己删除" % to_delete_indices_joined)
        else:
            logging.warn(u"%s deleted failed" % to_delete_indices_joined)
            # dopey_summary.add(u"%s 删除失败" % to_delete_indices_joined)
        indices = indices[batch:]


def close_indices(config, indices, batch=50):
    """
    :type indices: list of (indexname,index_settings, dopey_index_settings)
    :rtype: None
    """
    if not indices:
        return

    indices = [e[0] for e in indices]

    while indices:
        to_close_indices = indices[:batch]
        to_close_indices_joined = ','.join(to_close_indices)
        logging.debug(u"try to close %s" % ",".join(indices))
        for index in indices:
            url = u"{}/{}/_close".format(config['eshost'],
                                         to_close_indices_joined)
            logging.info(u"close: {}".format(url))

            r = requests.post(url)

            if r.ok:
                logging.info(u"%s closed" % to_close_indices_joined)
                # dopey_summary.add(u"%s 已关闭" % to_close_indices_joined)
            else:
                logging.warn(u"%s closed failed" % to_close_indices_joined)
                # dopey_summary.add(u"%s 关闭失败" % to_close_indices_joined)
        indices = indices[batch:]


def find_need_to_update_indices(indices):
    """
    :type indices: [(indexname,index_settings, dopey_index_settings)]
    :rtype : [(indexname,index_settings, dopey_index_settings)]
    """
    rst = []
    for index, index_settings, dopey_index_settings in indices:
        if_same = _compare_index_settings(index_settings, dopey_index_settings)
        if if_same is True:
            logging.info(u"unchanged settings, skip")
            continue
        else:
            logging.info(
                u"settings need to be changed. %s" %
                json.dumps(if_same))
            rst.append((index, index_settings, dopey_index_settings))

    return rst


def arrange_indices_by_settings(indices):
    """
    :type indices: [(indexname,index_settings, dopey_index_settings)]
    :rtype: [(dopey_index_settings,[indexname])]
    """
    rst = []
    for index, index_settings, dopey_index_settings in indices:
        for e in rst:
            if dopey_index_settings == e[0]:
                rst[1].append(index)
                break
        else:
            rst.append((dopey_index_settings, [index]))

    return rst


def update_settings_same_settings(
        config,
        indices,
        dopey_index_settings,
        batch=50):
    """
    :type indices: [indexname]
    :rtype: None
    """
    while indices:
        to_update_indices = indices[:batch]
        to_update_indices_joined = ','.join(to_update_indices)

        url = u"{}/{}/_settings".format(config["eshost"],
                                        to_update_indices_joined)
        logging.debug(u"update settings: %s", url)

        r = requests.put(url)

        if r.ok:
            logging.info(u"%s updated" % to_update_indices_joined)
            # dopey_summary.add(u"%s 已更新" % to_update_indices_joined)
        else:
            logging.warn(u"%s updated failed" % to_update_indices_joined)
            # dopey_summary.add(u"%s 更新失败" % to_update_indices_joined)

        indices = indices[batch:]


def update_settings(config, indices, batch=50):
    """
    :type indices: [(indexname,index_settings, dopey_index_settings)]
    :rtype: None
    """
    if not indices:
        return

    logging.debug(u"try to update index settings %s" %
                  ','.join([e[0] for e in indices]))

    need_to_update_indices = find_need_to_update_indices(indices)
    logging.debug(u"need_to_update_indices: %s", need_to_update_indices)

    to_update_indices = arrange_indices_by_settings(need_to_update_indices)
    logging.debug(u"to_update_indices: %s", to_update_indices)

    for dopey_index_settings, indices in to_update_indices:
        update_settings_same_settings(
            config, indices, dopey_index_settings, batch)


def optimize_indices(config, indices, batch=50):
    """
    :type indices: [(indexname,index_settings, dopey_index_settings)]
    :rtype: None
    """
    while indices:
        to_optimize_indices = indices[:batch]
        to_optimize_indices_joined = ','.join(to_optimize_indices)
        url = u"{}/{}/_forcemerge?max_num_segments=1".format(
            config["eshost"], to_optimize_indices_joined)
        logging.debug(u"forcemerge: %s" % url)

        r = requests.post(url)
        if r.ok:
            logging.info(u"%s forcemerged" % to_optimize_indices_joined)
            # dopey_summary.add(u"%s merge请求已经发送" % to_optimize_indices_joined)
        else:
            logging.warn(u"%s forcemerge failed" % to_optimize_indices_joined)
            # dopey_summary.add(
            # u"%s merge请求发送失败[%s]" %
            # (to_optimize_indices_joined, r.status_code))

        indices = indices[batch:]
