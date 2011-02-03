# Copyright (c) 2003-2009 Ralph Meijer
# See LICENSE for details.

from twisted.application import service
from twisted.python import usage
from twisted.words.protocols.jabber.jid import JID

from wokkel.component import Component
from wokkel.disco import DiscoHandler
from wokkel.generic import FallbackHandler, VersionHandler
from wokkel.iwokkel import IPubSubService

from idavoll import __version__
from idavoll.backend import BackendService

import restkit

DEFAULT_OPTIONS = {
	'couchdb-host': 'localhost',
	'couchdb-port': 5984
}

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
	]

	optFlags = [
		('verbose', 'v', 'Show traffic'),
		('hide-nodes', None, 'Hide all nodes for disco')
	]

	def postOptions(self):
		if self['backend'] not in ['pgsql', 'memory', 'couchdb']:
			raise usage.UsageError, "Unknown backend!"

		self['jid'] = JID(self['jid'])

def makeService(config):
	
	# http://bugs.python.org/issue7980 workaround:
	# call strptime before creating threads
	import datetime
	datetime.datetime.strptime('10:00', '%H:%M')
	
	s = service.MultiService()

	# Create backend service with storage

	if config['backend'] == 'pgsql':
		from twisted.enterprise import adbapi
		from idavoll.pgsql_storage import Storage
		
		args = {}
		if 'dbuser' in config:
			args['user'] = config['dbuser']
		if 'dbpass' in config:
			args['password'] = config['dbuser']
		if 'dbname' in config:
			args['database'] = config['dbname']
		if 'dbhost' in config:
			args['host'] = config['dbhost']
		if 'dbport' in config:
			args['port'] = config['dbport']
		args['cp_reconnect'] = True
			
		
		dbpool = adbapi.ConnectionPool('psycopg2',
									   **args
									   #client_encoding='utf-8',
									   )
		st = Storage(dbpool)
	elif config['backend'] == 'memory':
		from idavoll.memory_storage import Storage
		st = Storage()

	elif config['backend'] == 'couchdb':
		from idavoll.couchdb_storage import Storage
		#from restkit import SimplePool
		from couchdbkit import Server

		# default arguments
		if config['dbhost'] is None:
			config['dbhost'] = DEFAULT_OPTIONS['couchdb-host']
		if config['dbport'] is None:
			config['dbport'] = DEFAULT_OPTIONS['couchdb-port']

		try:
			#pool = SimplePool(keepalive=5) # pool of 5 connections to couchdb
			server = Server('http://%s:%s/' % (config['dbhost'], config['dbport']))
			db = server.get_or_create_db('pubsub')
		except restkit.errors.RequestFailed as e:
			print 'Error connecting to couchdb: %s' % e
			exit(-1)

		st = Storage(db)
		
		# upload design docs
		from couchdbkit.loaders import FileSystemDocsLoader
		loader = FileSystemDocsLoader('db/couchdb')
		loader.sync(db)

	bs = BackendService(st)
	bs.setName('backend')
	bs.setServiceParent(s)

	# Set up XMPP server-side component with publish-subscribe capabilities

	cs = Component(config["rhost"], int(config["rport"]),
				   config["jid"].full(), config["secret"])
	cs.setName('component')
	cs.setServiceParent(s)

	cs.factory.maxDelay = 900

	if 'verbose' in config and config['verbose']:
		cs.logTraffic = True

	FallbackHandler().setHandlerParent(cs)
	VersionHandler('Idavoll', __version__).setHandlerParent(cs)
	DiscoHandler().setHandlerParent(cs)

	ps = IPubSubService(bs)
	ps.setHandlerParent(cs)
	if 'hide-nodes' in config:
		ps.hideNodes = config["hide-nodes"]
	ps.serviceJID = config["jid"]

	return s
