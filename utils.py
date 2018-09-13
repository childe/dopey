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
    >>> whole={"index":{"routing":{"allocation":{"include":{"group":"4,5"},"total_shards_per_node":"2"}},"refresh_interval":"60s","number_of_shards":"20",\
        "store":{"type":"niofs"},"number_of_replicas":"1"}}
    >>> part={"index":{"routing":{"allocation":{"include":{"group":"4,5"}}}}}
    >>> _compare_index_settings(part, whole)
    True
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


def get_indices(eshost):
    all_indices = []
    url = "{}/_cat/indices?h=i".format(eshost)
    logging.debug(u"get all indices from {}".format(url))

    r = requests.get(url, headers={"content-type": "application/json"})
    if not r.ok:
        raise BaseException(u"could get indices from {}:{}".format(url, r.status_code))
    for i in r.text.split():
        i = i.strip()
        if i == "":
            continue
        all_indices.append(i)
    return all_indices


def get_index_settings(config, indexname):
    url = u"{}/{}/_settings".format(config['eshost'], indexname)
    try:
        return requests.get(url, headers={"content-type": "application/json"}).json()[indexname]['settings']
    except Exception as e:
        logging.error(
            u"could not get {} settings: {}".format(
                indexname, str(e)))
        return {}


def pick_date_from_indexname(indexname, index_prefix):
    patterns = (
        (r"^%s(\d{4}\.\d{2}\.\d{2})$", "%Y.%m.%d"),
        (r"^%s(\d{4}\-\d{2}\-\d{2})$", "%Y-%m-%d"),
        (r"^%s(\d{4}\.\d{2})$", "%Y.%m"),
        (r"^%s(\d{4}\-\d{2})$", "%Y-%m"),
    )
    for pattern_format, date_format in patterns:
        r = re.findall(
            pattern_format % index_prefix,
            indexname)
        if r:
            date = datetime.datetime.strptime(r[0], date_format)
            return date

    index_format = index_prefix
    r = re.findall(u'\(\?P<date>([^)]+)\)', index_format)
    if len(r) != 1:
        return
    date_format = r[0]

    index_format = index_format.replace('%Y', r'\d{4}')
    index_format = index_format.replace('%y', r'\d{2}')
    index_format = index_format.replace('%m', r'\d{2}')
    index_format = index_format.replace('%d', r'\d{2}')
    index_format = index_format.replace('%H', r'\d{2}')
    index_format = index_format.replace('%M', r'\d{2}')
    index_format = index_format.replace('.', r'\.')

    r = re.findall(index_format, indexname)
    if r:
        date = datetime.datetime.strptime(r[0], date_format)
        return date


def get_to_process_indices(to_select_action, config, all_indices, base_day):
    """
    rtype: [(indexname, index_settings, dopey_index_settings)]
    """
    rst = []

    for index_prefix, index_config in config['indices'].items():
        for indexname in all_indices:
            date = pick_date_from_indexname(indexname, index_prefix)
            if date is None:
                continue

            for e in index_config:
                action, configs = e.keys()[0], e.values()[0]
                if action != to_select_action:
                    continue

                offset = base_day-date
                if "day" in configs and offset.days == configs["day"]:
                    index_settings = get_index_settings(config, indexname)
                    rst.append((indexname, index_settings, configs.get('settings')))
                    continue
                if "days" in configs and offset.days >= configs["days"]:
                    index_settings = get_index_settings(config, indexname)
                    rst.append((indexname, index_settings, configs.get('settings')))
                    continue

                if "hour" in configs and offset.days*24+offset.seconds // 3600 == configs["hour"]:
                    index_settings = get_index_settings(config, indexname)
                    rst.append((indexname, index_settings, configs.get('settings')))
                    continue
                if "hours" in configs and offset.days*24+offset.seconds // 3600 >= configs["hours"]:
                    index_settings = get_index_settings(config, indexname)
                    rst.append((indexname, index_settings, configs.get('settings')))
                    continue
                if "minute" in configs and offset.days*24*60+offset.seconds // 60 == configs["minute"]:
                    index_settings = get_index_settings(config, indexname)
                    rst.append((indexname, index_settings, configs.get('settings')))
                    continue
                if "minutes" in configs and offset.days*24*60+offset.seconds // 60 >= configs["minutes"]:
                    index_settings = get_index_settings(config, indexname)
                    rst.append((indexname, index_settings, configs.get('settings')))
                    continue

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


def delete_indices(config, indices):
    """
    :type indices: list of (indexname,index_settings, dopey_index_settings)
    :rtype: None
    """
    if not indices:
        return

    retry = config.get('retry', 3)
    batch = config.get('batch', 50)
    indices = [e[0] for e in indices]

    logging.debug(u"try to delete %s" % ",".join(indices))
    while indices:
        to_delete_indices = indices[:batch]
        to_delete_indices_joined = ','.join(to_delete_indices)
        url = u"{}/{}".format(
            config['eshost'], to_delete_indices_joined)
        logging.info(u"delete: {}".format(url))

        for _ in range(retry):
            try:
                r = requests.delete(
                    url, timeout=300, params={
                        "master_timeout": "10m", "ignore_unavailable": 'true'}, headers={
                        "content-type": "application/json"})
                if r.ok:
                    logging.info(u"%s deleted" % to_delete_indices_joined)
                    break
                else:
                    logging.warn(
                        u"%s deleted failed. %s" %
                        (to_delete_indices_joined, r.text))
            except BaseException as e:
                logging.info(e)
        indices = indices[batch:]


def close_indices(config, indices):
    """
    :type indices: list of (indexname,index_settings, dopey_index_settings)
    :rtype: None
    """
    if not indices:
        return

    retry = config.get('retry', 3)
    batch = config.get('batch', 50)
    indices = [e[0] for e in indices]

    while indices:
        to_close_indices = indices[:batch]
        to_close_indices_joined = ','.join(to_close_indices)
        logging.debug(u"try to close %s" % to_close_indices_joined)

        url = u"{}/{}/_close".format(
            config['eshost'], to_close_indices_joined)
        logging.info(u"close: {}".format(url))

        for _ in range(retry):
            try:
                r = requests.post(
                    url,
                    timeout=300,
                    params={
                        "master_timeout": "10m",
                        "ignore_unavailable": 'true'}, headers={"content-type": "application/json"})

                if r.ok:
                    logging.info(u"%s closed" % to_close_indices_joined)
                    break
                else:
                    logging.warn(
                        u"%s closed failed. %s" %
                        (to_close_indices_joined, r.text))
            except BaseException as e:
                logging.info(e)
        indices = indices[batch:]


def find_need_to_update_indices(indices):
    """
    :type indices: [(indexname,index_settings, dopey_index_settings)]
    :rtype : [(indexname,index_settings, dopey_index_settings)]
    """
    rst = []
    for index, index_settings, dopey_index_settings in indices:
        if_same = _compare_index_settings(dopey_index_settings, index_settings)
        if if_same is True:
            logging.info(u"%s settings is unchanged , skip" % index)
            continue
        else:
            logging.info(
                u"%s settings need to be updated. %s" % (index,
                                                         json.dumps(if_same)))
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
                e[1].append(index)
                break
        else:
            rst.append((dopey_index_settings, [index]))

    return rst


def update_settings_same_settings(config, indices, dopey_index_settings):
    """
    :type indices: [indexname]
    :rtype: None
    """
    retry = config.get('retry', 3)
    batch = config.get('batch', 50)
    while indices:
        to_update_indices = indices[:batch]
        to_update_indices_joined = ','.join(to_update_indices)

        url = u"{}/{}/_settings".format(
            config["eshost"], to_update_indices_joined)
        logging.debug(u"update settings: %s", url)

        for _ in range(retry):
            try:
                r = requests.put(
                    url,
                    timeout=300,
                    params={
                        "master_timeout": "10m",
                        "ignore_unavailable": 'true'},
                    data=json.dumps(dopey_index_settings), headers={"content-type": "application/json"})

                if r.ok:
                    logging.info(u"%s updated" % to_update_indices_joined)
                    break
                else:
                    logging.warn(
                        u"%s updated failed. %s" %
                        (to_update_indices_joined, r.text))
            except BaseException as e:
                logging.info(e)

        indices = indices[batch:]


def update_settings(config, indices):
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
            config, indices, dopey_index_settings)


def optimize_indices(config, indices):
    """
    :type indices: [(indexname,index_settings, dopey_index_settings)]
    :rtype: None
    """
    arranged_indices = arrange_indices_by_settings(indices)

    retry = config.get('retry', 1)
    batch = config.get('batch', 50)

    for dopey_index_settings, indices in arranged_indices:
        if not dopey_index_settings:
            dopey_index_settings = {}
        dopey_index_settings.setdefault("max_num_segments", 1)
        while indices:
            to_optimize_indices = indices[:batch]
            to_optimize_indices_joined = ','.join(to_optimize_indices)
            url = u"{}/{}/_forcemerge".format(
                config["eshost"], to_optimize_indices_joined)
            logging.debug(u"forcemerge: %s" % url)

            for _ in range(retry):
                try:
                    r = requests.post(url, headers={"content-type": "application/json"}, params=dopey_index_settings)
                    if r.ok:
                        logging.info(u"%s forcemerged" % to_optimize_indices_joined)
                        break
                    else:
                        logging.warn(
                            u"%s forcemerge failed. %s" %
                            (to_optimize_indices_joined, r.text))
                except BaseException as e:
                    logging.info(e)

            indices = indices[batch:]
