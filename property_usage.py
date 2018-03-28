#!/usr/bin/python
# -*- coding: utf-8 -*-
# (C) eranroz
#
# Distributed under the terms of the MIT license.
import sys
import datetime
import MySQLdb

usedPages = []


def fillStatsForCluster(host, dbList):
    conn = MySQLdb.connect(host=host, read_default_file='~/replica.my.cnf')
    cursor = conn.cursor()
    for db, lang, family in dbList:
        print('Querying {}'.format(db))
        cursor.execute('USE `%s_p`' % db)
        try:
            cursor.execute('''
    /* property-usage.py */
    SELECT
    page_title, page_namespace
    from wbc_entity_usage
    inner join page on page_id=eu_page_id
    where eu_aspect='C.%s'  limit 1
    ''' % sys.argv[1])
        except:
            continue
        for page_title, page_namespace in cursor.fetchall():
            if page_namespace == 0:
                page_title = '%s' % (page_title)
            else:
                page_title = '{{subst:ns:%s}}:%s' % (page_namespace, page_title)
            usedPages.append('[[%s:%s]]', lang, page_title)
    cursor.close()
    conn.close()

conn = MySQLdb.connect(host='enwiki.labsdb',
                       db='meta_p',
                       read_default_file='~/replica.my.cnf')
cursor = conn.cursor()
cursor.execute('''
        select slice,dbname,lang,family from meta_p.wiki
        where is_closed=0
        and family in ('wikipedia')
        /*('wikibooks','wikipedia','wiktionary','wikiquote','wikisource','wikinews','wikiversity','wikivoyage')*/
        and dbname not like 'test%'
''')
servers, dbnames, wikiLangs, wikiFamilies = zip(*cursor.fetchall())
nameToCluster = dict()
for clus, db, lang, family in zip(servers, dbnames, wikiLangs, wikiFamilies):
    if clus not in nameToCluster:
        nameToCluster[clus] = []
    nameToCluster[clus].append((db, lang, family))

for cluster, wikisMetaData in nameToCluster.iteritems():
    print 'Filling data from cluster ', cluster
    fillStatsForCluster(cluster, wikisMetaData)

print('\n'.join(usedPages))
