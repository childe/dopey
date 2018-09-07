#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
test1-YYYY-MM-dd 15 days
test1-YYYY.MM.dd 15 days
test2-YYYY.MM.dd 15 days
test3-YYYY.MM.dd 15 days
test-YYYYMMDDHHmm-1 15 hours
'''

import requests

import datetime
import json


def main():
    yn = raw_input('this will delete all indices. [y/n]')
    if yn != 'y':
        return

    url = u'http://127.0.0.1:9200/*'
    print url
    print requests.delete(url, headers={'content-type': 'application/json'})

    now = datetime.datetime.now()
    settings = {
        "settings": {
            "index": {
                "number_of_shards": "1",
                "number_of_replicas": "0"
            }
        }
    }
    for i in range(15):
        date = now - datetime.timedelta(i)
        indexname = u'test1-{}'.format(date.strftime("%Y.%m.%d"))
        print indexname

        # create index
        url = u'http://127.0.0.1:9200/{}'.format(indexname)
        print url
        r = requests.put(url, data=json.dumps(settings), headers={'content-type': 'application/json'})
        print r.text

        # write 10 docs to index and refresh
        for j in range(10):
            url = u'http://127.0.0.1:9200/{}/logs?refresh=true'.format(indexname)
            print url
            r = requests.post(url, data=json.dumps({"age": j}), headers={'content-type': 'application/json'})
            print r.text

        indexname = u'test1-{}'.format(date.strftime("%Y-%m-%d"))
        print indexname
        url = u'http://127.0.0.1:9200/{}'.format(indexname)
        print url
        r = requests.put(url, data=json.dumps(settings), headers={'content-type': 'application/json'})
        print r.text

        date = now - datetime.timedelta(i)
        indexname = u'test2-{}'.format(date.strftime("%Y.%m.%d"))
        print indexname
        url = u'http://127.0.0.1:9200/{}'.format(indexname)
        print url
        r = requests.put(url, data=json.dumps(settings), headers={'content-type': 'application/json'})
        print r.text

        date = now - datetime.timedelta(i)
        indexname = u'test3-{}'.format(date.strftime("%Y.%m.%d"))
        print indexname
        url = u'http://127.0.0.1:9200/{}'.format(indexname)
        print url
        r = requests.put(url, data=json.dumps(settings), headers={'content-type': 'application/json'})
        print r.text

        date = now - datetime.timedelta(hours=i)
        indexname = u'test-{}00-1'.format(date.strftime("%Y%m%d%H"))
        print indexname

        url = u'http://127.0.0.1:9200/{}'.format(indexname)
        print url
        r = requests.put(url, data=json.dumps(settings), headers={'content-type': 'application/json'})
        print r.text


if __name__ == '__main__':
    main()
