#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging


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
        return None


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
            dopey_summary.add(u"%s 己删除" % to_delete_indices_joined)
        else:
            logging.warn(u"%s deleted failed" % to_delete_indices_joined)
            dopey_summary.add(u"%s 删除失败" % to_delete_indices_joined)
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
        logger.debug(u"try to close %s" % ",".join(indices))
        for index in indices:
            url = u"{}/{}/_close".format(config['eshost'],
                                         to_close_indices_joined)
            logging.info(u"close: {}".format(url))

            r = requests.post(url)

            if r.ok:
                logger.info(u"%s closed" % to_close_indices_joined)
                dopey_summary.add(u"%s 已关闭" % to_close_indices_joined)
            else:
                logger.warn(u"%s closed failed" % to_close_indices_joined)
                dopey_summary.add(u"%s 关闭失败" % to_close_indices_joined)
        indices = indices[batch:]


def get_to_process_indices(to_select_action, config, all_indices, base_day):
    """
    rtype: [(indexname, index_settings, dopey_index_settings)]
    """
    rst = []
    index_config = config['indices']

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
            action, settings = e.keys()[0], e.values()[0]
            if action != to_select_action:
                continue
            offset = base_day-date.date()
            if "day" in settings and offset == datetime.timedelta(
                    settings["day"]) or "days" in settings and offset >= datetime.timedelta(
                    settings["days"]):
                index_settings = get_index_settings(config, indexname)
                rst.append((indexname, index_settings, settings))

    return rst


def get_to_delete_indices(config, all_indices, base_day):
    return get_to_process_indices('delete_indices', all_indices, base_day)


def get_to_update_indices(config, all_indices, base_day):
    return get_to_process_indices('update_settings', all_indices, base_day)
