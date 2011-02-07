from zope.interface import implements

from idavoll import error, iidavoll
from idavoll import pgsql_storage, couchdb_storage

"""
PostgreSQL / CouchDB Engine
Uses CouchDB for items, PostgreSQL for everything else
"""

class Storage(pgsql_storage.Storage):
	implements(iidavoll.IStorage)
	
	def __init__(self, dbpool):
		pgsql_storage.Storage.__init__(self, dbpool)
		#couchdb_storage.Storage.__init__(self, couchdb)

class Node(pgsql_storage.Node):
	implements(iidavoll.INode)


class LeafNode(pgsql_storage.LeafNode):
	implements(iidavoll.ILeafNode)

class CollectionNode(pgsql_storage.CollectionNode):
	pass
