# Copyright (c) 2003-2010 Ralph Meijer
# See LICENSE for details.

from twisted.application import service
from twisted.python import usage
from twisted.words.protocols.jabber.jid import JID

from wokkel.component import Component
from wokkel.disco import DiscoHandler
from wokkel.generic import FallbackHandler, VersionHandler
from wokkel.iwokkel import IPubSubResource
from wokkel.pubsub import PubSubService


from idavoll import __version__
from idavoll.backend import BackendService

class Options(usage.Options):
    optParameters = [
        ('jid', None, 'pubsub', 'JID this component will be available at'),
        ('secret', None, 'secret', 'Jabber server component secret'),
        ('rhost', None, '127.0.0.1', 'Jabber server host'),
        ('rport', None, '5347', 'Jabber server port'),
        ('backend', None, 'memory', 'Choice of storage backend'),
        ('dbuser', None, None, 'Database user (pgsql backend)'),
        ('dbname', None, 'pubsub', 'Database name (pgsql backend)'),
        ('dbpass', None, None, 'Database password (pgsql backend)'),
        ('dbhost', None, None, 'Database host (pgsql backend)'),
        ('dbport', None, None, 'Database port (pgsql backend)'),
        ('cdbhost', None, '127.0.0.1', 'CouchDB host (couchdb backend)'),
        ('cdbport', None, '5984', 'CouchDB port (couchdb backend)'),
        ('cdbname', None, 'pubsub', 'CouchDB databse name (couchdb backend'),
    ]

    optFlags = [
        ('verbose', 'v', 'Show traffic'),
        ('hide-nodes', None, 'Hide all nodes for disco')
    ]

    def postOptions(self):
        if self['backend'] not in ['pgsql', 'memory']:
            raise usage.UsageError, "Unknown backend!"

        self['jid'] = JID(self['jid'])



def makeService(config):
    s = service.MultiService()

    # Create backend service with storage

    if config['backend'] == 'pgsql':
        from twisted.enterprise import adbapi
        from idavoll.pgsql_storage import Storage
        dbpool = adbapi.ConnectionPool('pyPgSQL.PgSQL',
                                       user=config['dbuser'],
                                       password=config['dbpass'],
                                       database=config['dbname'],
                                       host=config['dbhost'],
                                       port=config['dbport'],
                                       cp_reconnect=True,
                                       client_encoding='utf-8',
                                       )
        st = Storage(dbpool)
    elif config['backend'] == 'memory':
        from idavoll.memory_storage import Storage
        st = Storage()

    elif config['backend'] == 'pgsql_couchdb':
        from twisted.enterprise import adbapi
        from idavoll.pgsql_couchdb_storage import Storage
        from couchdbkit import Server
        import restkit
        from restkit import Resource, Manager

        # postgresql config
        dbpool = adbapi.ConnectionPool('pyPgSQL.PgSQL',
                                       user=config['dbuser'],
                                       password=config['dbpass'],
                                       database=config['dbname'],
                                       host=config['dbhost'],
                                       port=config['dbport'],
                                       cp_reconnect=True,
                                       client_encoding='utf-8',
                                       )

        # couchdb config
        try:
            manager = Manager(max_conn=5)
            server = Server('http://%s:%s/' % (config['cdbhost'],
                                               config['cdbport']),
                                               manager=manager)
            if not config['cdbname'] in server:
                print 'CouchDB database "%s" not found.' % config['cdbname']
                exit(-1)

            couchdb = server[config['cdbname']]
        except restkit.errors.RequestFailed as e:
            print 'Error connecting to couchdb: %s' % e
            exit(-1)

        st = Storage(dbpool, couchdb)


    bs = BackendService(st)
    bs.setName('backend')
    bs.setServiceParent(s)

    # Set up XMPP server-side component with publish-subscribe capabilities

    cs = Component(config["rhost"], int(config["rport"]),
                   config["jid"].full(), config["secret"])
    cs.setName('component')
    cs.setServiceParent(s)

    cs.factory.maxDelay = 900

    if config["verbose"]:
        cs.logTraffic = True

    FallbackHandler().setHandlerParent(cs)
    VersionHandler('Idavoll', __version__).setHandlerParent(cs)
    DiscoHandler().setHandlerParent(cs)

    resource = IPubSubResource(bs)
    resource.hideNodes = config["hide-nodes"]
    resource.serviceJID = config["jid"]

    ps = PubSubService(resource)
    ps.setHandlerParent(cs)
    resource.pubsubService = ps

    return s
