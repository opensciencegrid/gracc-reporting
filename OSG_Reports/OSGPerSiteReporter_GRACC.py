from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

def establish_client():
    """Initialize and return the elasticsearch client"""
    client = Elasticsearch(['https://gracc.opensciencegrid.org/q'],
                           use_ssl=True,
                           # verify_certs = True,
                           # ca_certs = 'gracc_cert/lets-encrypt-x3-cross-signed.pem',
                           # client_cert = 'gracc_cert/gracc-reports-dev.crt',
                           # client_key = 'gracc_cert/gracc-reports-dev.key',
                           timeout=60)
    return client

def query(client):

    startdate = "2016-10-12"
    enddate = "2016-10-24"

    s = Search(using=client, index='gracc.osg.summary*') \
        .filter(Q({"range": {"@received": {"gte": "{0}".format(startdate), "lt":"{0}".format(enddate)}}}))\
        .filter('term', ResourceType="Batch")

    s.aggs.bucket('site_bucket', 'terms', field='Site', size=1000000000)\
        .bucket('vo_bucket', 'terms', field='VOName', size=1000000000)\
        .metric('sum_wall_dur', 'sum', field='WallDuration')

    return s

def generate():
    client = establish_client()
    qresults = query(client).execute()
    results = qresults.aggregations
    for site_bucket in results.site_bucket.buckets:
        site = site_bucket['key']
        for vo_bucket in site_bucket.vo_bucket.buckets:
            vo = vo_bucket['key']
            wallsec = vo_bucket['sum_wall_dur']['value']
            yield [site, vo, wallsec]

def generate_report_file():
    results_dict = {}
    for item in generate():
        if item[0] not in results_dict:
            results_dict[item[0]] = {}
        results_dict[item[0]][item[1]] = item[2]

    voset = set([vo for vos in results_dict.itervalues() for vo in vos])

    for vo in voset:
        for site, vos in results_dict.iteritems():
            if vo not in vos:
                results_dict[site][vo] = 0

    for site, vos in results_dict.iteritems():
        print "Site: {0}".format(site)
        for vo,walldur in vos.iteritems():
            print "\tVO: {0}, Wall Seconds: {1}".format(vo, walldur)

if __name__ == '__main__':
    generate_report_file()
