import datetime

from zope.interface import implements

from twisted.internet import threads

from idavoll import error, iidavoll
from idavoll import pgsql_storage, couchdb_storage

from couchdbkit import *

from twisted_utils import *

"""
PostgreSQL / CouchDB Engine
Uses CouchDB for items, PostgreSQL for everything else
"""

KEY_SEPARATOR = ':'

# CouchDB data structures
class CouchStorage:
	class Item(Document):
		doc_type = 'item'
		item_id = StringProperty()
		node = StringProperty()
		publisher = StringProperty()
		content = DictProperty()
		date = DateTimeProperty()
		
		def save(self):
			# always update date
			self.date = datetime.datetime.utcnow()
			if self['_id'] is None:
				self['_id'] = self.key(node=self.node, item_id=self.item_id)
			return Document.save(self)
		
		def key(self):
			return self.key(node=self.node, item_id=self.item_id)
			
		@staticmethod
		def key(node='', item_id=''):
			#return 'item' + KEY_SEPARATOR + node + KEY_SEPARATOR + item_id
			#return node + KEY_SEPARATOR + item_id
			return item_id


class Storage(pgsql_storage.Storage):
	print 'PGSQL CDB STORAGE'
	implements(iidavoll.IStorage)
	
	def __init__(self, dbpool, cdb):
		pgsql_storage.Storage.__init__(self, dbpool)

		# couchdb
		self.cdb = cdb
		CouchStorage.Item.set_db(self.cdb)

	def getNode(self, nodeIdentifier):
		return self.dbpool.runInteraction(self._getNode, nodeIdentifier)

	def _getNode(self, cursor, nodeIdentifier):
		node = pgsql_storage.Storage._getNode(self, cursor, nodeIdentifier)
		# change class
		if node.__class__ == pgsql_storage.LeafNode:
			node.__class__ = LeafNode
		elif node.__class__ == pgsql_storage.CollectionNode:
			node.__class__ = CollectionNode
		return node

class Node(pgsql_storage.Node):
	implements(iidavoll.INode)

	#def __init__(self, nodeIdentifier, config):
	#	pgsql_storage.Node.__init__(self, nodeIdentifier, config)
		


class LeafNode(pgsql_storage.LeafNode):
	implements(iidavoll.ILeafNode)

	nodeType = 'leaf'

	def storeItems(self, items, publisher):
		d = self.dbpool.runInteraction(self._checkNodeExists)
		d.addCallback(self._storeItems, items, publisher)
		return d
		#return threads.deferToThread(self._storeItems, items, publisher)


	def _storeItems(self, cursor, items, publisher):
		# ignore cursor!

		#self._checkNodeExists()
		
		for item in items:
			self._storeItem(item, publisher)


	def _storeItem(self, item, publisher):
		s = DictSerializer()
		data = s.dict_from_elem(item)
		
		# try updating existing item;
		# if it doesnt exist, create a new one
		try:
			item = CouchStorage.Item.get(
				CouchStorage.Item.key(
					item_id=item['id'],
					node=self.nodeIdentifier)
				)
			item.publisher = publisher.full()
			item.data = data
			item.save()
		except ResourceNotFound:
			# create new item
			item = CouchStorage.Item(
				item_id = item['id'],
				node = self.nodeIdentifier,
				publisher = publisher.full(),
				data = data
				)
			item.save()
		

	def getItems(self, maxItems=None):
		d = self.dbpool.runInteraction(self._checkNodeExists)
		d.addCallback(self._getItems, maxItems)
		return d
		#threads.deferToThread(self._getItems, maxItems)


	def _getItems(self, cursor, maxItems):
		# ignore cursor!

		if maxItems:
			items = CouchStorage.Item.view(
				'pubsub/items_by_node_date',
				startkey=[self.nodeIdentifier, {}],
				endkey=[self.nodeIdentifier],
				descending=True,
				limit=maxItems
				)
		else:
			items = CouchStorage.Item.view(
				'pubsub/items_by_node_date',
				startkey=[self.nodeIdentifier, {}],
				endkey=[self.nodeIdentifier],
				descending=True,
				)
		
		#print 'items: %s' % items
		#elements = [parseXml(i.data.encode('utf-8')) for i in items]
		s = DictSerializer()
		elements = [s.serialize_to_xml(i.data) for i in items]
		return elements


	def getItemsById(self, itemIdentifiers):
		return threads.deferToThread(self._getItemsById, itemIdentifiers)


	def _getItemsById(self, itemIdentifiers):
		self._checkNodeExists()
		
		keys = [[self.nodeIdentifier, i] for i in itemIdentifiers]

		items = CouchStorage.Item.view(
			'pubsub/items_by_node_item',
			keys=keys
		)
		
		#values = [parseXml(i.data.encode('utf-8')) for i in items.all()]
		s = DictSerializer()
		values = [s.serialize_to_xml(i.data) for i in items.all()]
		return values
	
	

class CollectionNode(pgsql_storage.CollectionNode):
	pass
