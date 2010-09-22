# Copyright (c) 2010 Nuno Santos
# See LICENSE for details.

import copy
import datetime

from zope.interface import implements

from restkit import SimplePool
import restkit, logging

from couchdbkit import *

from twisted.words.protocols.jabber import jid
from twisted.internet import threads

from wokkel.generic import parseXml, stripNamespace
from wokkel.pubsub import Subscription

from idavoll import error, iidavoll

from twisted_utils import *
#from xml_utils import *

import os

KEY_SEPARATOR = ':'
COLLECTION_NODE_DOCID = 'nodecollection'

# Data structures
class CouchStorage:
	class CollectionNodeTree(Document):
		doc_type = 'collection_node_tree'
		collection = DictProperty()
		
		def save(self):
			if not self['_id']:
				self['_id'] = COLLECTION_NODE_DOCID
			return Document.save(self)
		
		# adds a collection node to the collection node tree
		# raises a error.NodeNotFound if the parent node doesn't exist
		def addCollectionNode(self, parent, node):
			path = []
			found = self.findTreePath(self.collection, parent, node, path)     

			if not found:
				raise error.NodeNotFound()

			path.reverse()
			print 'path: %s' % path
			collection = self.collection
			for p in path:
				collection = collection[p]
			collection[node] = {}
			print 'collection: %s' % self.collection

		# returns the tree path to the node
		def findTreePath(self, root, parent, node, result):
			# root node
			if not parent or parent is '':
				return True
		
			# node in this level
			if parent in root:
				result.append(parent)
				return True
			else:
				for k in root.iterkeys():
					if self.findTreePath(root[k], parent, node, result):
						result.append(k)
						return True
				return False
				
			
	class Node(Document):
		doc_type = 'node'
		node = StringProperty()
		node_type = StringProperty()
#		persist_items = DynamicProperty()
#		deliver_payloads = BooleanProperty()
#		send_last_published_item = StringProperty()
		date = DateTimeProperty()
		
		def save(self):
			# set defaults
			if self.node_type is None:
				self.node_type = 'leaf'

			# set leaf node defauls
			if self.node_type == 'leaf':
				if not hasattr(self, 'persist_items'):
					self.persist_items = True
				if not hasattr(self, 'deliver_payloads'):
					self.deliver_payloads = True
				if not hasattr(self, 'send_last_published_item'):
					self.send_last_published_item = 'on_sub'

			if not hasattr(self, 'collection'):
				self.collection = ''

			if self['_id'] is None:
				self['_id'] = self.key(node=self.node)
				
			return Document.save(self)
		
		def key(self):
			return self.key(node=self.node)

		@staticmethod
		def key(node=''):
			return 'node' + KEY_SEPARATOR + node

	class Entity(Document):
		doc_type = 'entity'
		jid = StringProperty()
		#nodes = ListProperty() # node affiliation list [{node, affiliation}]

		def save(self):
			self['_id'] = self.key(jid=self.jid)
			Document.save(self)
		
		def key(self):
			return self.key(jid=self.jid)

		@staticmethod
		def key(jid=''):
			return 'entity' + KEY_SEPARATOR + jid

	class Affiliation(Document):
		doc_type = 'affiliation'
		entity = StringProperty()
		node = StringProperty()
		affiliation = StringProperty()
		
		def save(self):
			self['_id'] = self.key(entity=self.entity, node=self.node,
								   affiliation=self.affiliation)
			Document.save(self)
		
		def key(self):
			return self.key(entity=self.entity, node=self.node,
							affiliation=self.affiliation)

		@staticmethod
		def key(entity='', node='', affiliation=''):
			return 'affiliation' + KEY_SEPARATOR + entity + KEY_SEPARATOR + \
					node + KEY_SEPARATOR + affiliation
	
	class Subscription(Document):
		doc_type = 'subscription'
		entity = StringProperty()
		node = StringProperty()
		resource = StringProperty()
		subscription_type = StringProperty()
		subscription_depth = StringProperty()
		
		def save(self):
			if self.state is None:
				self.state = 'subscribed'
			if self['_id'] is None:
				self['_id'] = self.key(node=self.node, entity=self.entity,
									   resource=self.resource)
			return Document.save(self)
		
		def key(self):
			return self.key(node=self.node, entity=self.entity,
							resource=self.resource)

		@staticmethod
		def key(node='', entity='', resource=''):
			return 'subscription' + KEY_SEPARATOR + node + KEY_SEPARATOR + \
					entity + KEY_SEPARATOR + resource
	
	class Item(Document):
		doc_type = 'item'
		item_id = StringProperty()
		node = StringProperty()
		publisher = StringProperty()
		data = DictProperty()
		date = DateTimeProperty()
		
		def save(self):
			if self.date is None:
				self.date = datetime.datetime.utcnow()
			if self['_id'] is None:
				self['_id'] = self.key(node=self.node, item_id=self.item_id)
			return Document.save(self)
		
		def key(self):
			return self.key(node=self.node, item_id=self.item_id)
			
		@staticmethod
		def key(node='', item_id=''):
			return 'item' + KEY_SEPARATOR + node + KEY_SEPARATOR + item_id


# Main CouchDB Storage class
class Storage:
	
	implements(iidavoll.IStorage)

	defaultConfig = {
			'leaf': {
				"pubsub#persist_items": True,
				"pubsub#deliver_payloads": True,
				"pubsub#send_last_published_item": 'on_sub',
			},
			'collection': {
				#"pubsub#deliver_payloads": True,
				#"pubsub#send_last_published_item": 'on_sub',
			}
	}

	def __init__(self, dbpool):
		#restkit.set_logging(logging.ERROR);
		self.dbpool = dbpool
		
		# associate datastructures to the db
		CouchStorage.Node.set_db(self.dbpool)
		CouchStorage.CollectionNodeTree.set_db(self.dbpool)
		CouchStorage.Entity.set_db(self.dbpool)
		CouchStorage.Affiliation.set_db(self.dbpool)
		CouchStorage.Subscription.set_db(self.dbpool)
		CouchStorage.Item.set_db(self.dbpool)


	def getNode(self, nodeIdentifier, callback=None):
		d = threads.deferToThread(self._getNode, nodeIdentifier)
		return d

	
	def _getNode(self, nodeIdentifier):
		configuration = {}
		
		try:
			node = CouchStorage.Node.get(
					CouchStorage.Node.key(node=nodeIdentifier))
		except ResourceNotFound:
			raise error.NodeNotFound()

		if node.node_type == 'leaf':
			configuration = {
					'pubsub#persist_items': node.persist_items,
					'pubsub#deliver_payloads': node.deliver_payloads,
					'pubsub#send_last_published_item':
							node.send_last_published_item}
			
			return_node = LeafNode(nodeIdentifier, configuration)
			return_node.dbpool = self.dbpool
		elif node.node_type == 'collection':
			configuration = {}
			return_node = CollectionNode(nodeIdentifier, configuration)
			return_node.dbpool = self.dbpool
		return return_node
	
	# returns ids of all nodes for the given parent;
	# if parent is empty, returns all root collection nodes
	# and leaf nodes not associated with any collection
	def getChildNodeIds(self, parentNodeIdentifier=''):
		d = threads.deferToThread(self._getChildNodeIds, parentNodeIdentifier)
		return d

	def _getChildNodeIds(self, parentNodeIdentifier):
		nodes = CouchStorage.Node.view('pubsub/nodes_by_collection',
				startkey=[parentNodeIdentifier],
				endkey=[parentNodeIdentifier, {}]
				)
		# nodes: {'key': [parent, nodename]}
		result = []
		for node in nodes.iterator():
			result.append(node['key'][1])
		#print 'result: %s' % result
		return result


	def getNodeIds(self):
		d = threads.deferToThread(self._getNodeIds)
		return d
		
	
	def _getNodeIds(self):
		nodes = CouchStorage.Node.view('pubsub/nodes_by_node')
		result = []
		for node in nodes.iterator():
			result.append(node.node)
		return result

	def createNode(self, nodeIdentifier, owner, config=None):
		d = threads.deferToThread(self._createNode, nodeIdentifier, owner,
								  config)
		return d

	def _createNode(self, nodeIdentifier, owner, config):
		owner = owner.userhost()

		if 'pubsub#node_type' in config:
			nodeType = config['pubsub#node_type']
		else:
			nodeType = 'leaf'

		try:
			# leaf node
			if nodeType == 'leaf':
				node = CouchStorage.Node(
					node = nodeIdentifier,
					node_type = 'leaf',
					persist_items = config['pubsub#persist_items'],
					deliver_payloads = config['pubsub#deliver_payloads'],
					send_last_published_item =
							config['pubsub#send_last_published_item'],
					date = datetime.datetime.utcnow()
					)
				if 'pubsub#collection' in config:
					node.collection = config['pubsub#collection']
				node.save()
					
			# collection node
			elif nodeType == 'collection':

				# first create node
				node = CouchStorage.Node(
					node = nodeIdentifier,
					node_type = 'collection',
					date = datetime.datetime.utcnow()
					)
				if 'pubsub#collection' in config:
					node.collection = config['pubsub#collection']
				node.save()

				# update collection node tree
				try:
					collectionNode = CouchStorage.CollectionNodeTree.get(
						COLLECTION_NODE_DOCID)
				except ResourceNotFound:
					collectionNode = CouchStorage.CollectionNodeTree()
				
				# find the collection node in the tree,
				# append the current collection node
				try:
					if 'pubsub#collection' in config:
						added = collectionNode.addCollectionNode(
								config['pubsub#collection'], nodeIdentifier)
					# root collection node
					else:
						added = collectionNode.addCollectionNode(None,
								nodeIdentifier)
				#
				except error.NodeNotFound:
					node.delete()
					raise error.NodeNotFound()

				collectionNode.save()
			else:
				raise error.Error(msg='Unknown node type')
				
		except ResourceConflict:
			raise error.NodeExists()
		except error.NodeNotFound:
			raise error.NodeNotFound()
		except Exception as e:
			print 'Error: ' + str(e)
			raise error.Error()

		# save entity
		try:
			entity = CouchStorage.Entity.get('entity' + KEY_SEPARATOR + owner)
		except ResourceNotFound:
			entity = CouchStorage.Entity(jid=owner)
			entity.save()			
			pass
			

		# save affiliation
		try:
			affiliation = CouchStorage.Affiliation(
				node=nodeIdentifier,
				entity=owner,
				affiliation='owner',
			)
			# 'affiliation' : entity : node : affiliation
			#affiliation['_id'] = 'affiliation:' + owner + ':' + 
			#nodeIdentifier + ':owner'

			#print affiliation['_id']
			affiliation.save()
		except ResourceConflict:
			pass

	def deleteNode(self, nodeIdentifier):
		return threads.deferToThread(self._deleteNode, nodeIdentifier)


	# because the lack of CASCADE DELETE as used in SQL storage, delete:
	# 1. node
	# 2. affiliations of this node
	# 3. subscriptions of this node
	# 4. items of this node
	def _deleteNode(self, nodeIdentifier):
		
		# 1. delete node
		try:
			node = CouchStorage.Node.get('node' + KEY_SEPARATOR + \
					nodeIdentifier)
			print 'nodeType: %s' % node.node_type
			if node.node_type == 'leaf':
				node.delete()
			elif node.node_type == 'collection':
				# TODO: find child nodes of this node, and set their parent
				# as '' (no parent)
				nodes = CouchStorage.Node.view('pubsub/nodes_by_collection',
					startkey=[parentNodeIdentifier],
					endkey=[parentNodeIdentifier, {}]
				)
				# nodes: {'key': [parent, nodename]}
				result = []
				for node in nodes.iterator():
					result.append(node['key'][1])

				# get all documents by id (result)

		except ResourceNotFound:
			raise error.NodeNotFound()
		
		# 2. delete affiliations
		try:
			# delete affiliations
			affiliations = CouchStorage.Affiliation.view(
				'pubsub/affiliations_by_node',
				key=nodeIdentifier,
				)
			
			affiliations = [a.to_json() for a in affiliations]
			 
			self.dbpool.bulk_delete(affiliations)
		except Exception as e:
			print e
			pass

		# 3. delete subscriptions
		try:
			subscriptions = CouchStorage.Subscription.view(
				'pubsub/subscriptions_by_node_state',
				startkey=[nodeIdentifier],
				endkey=[nodeIdentifier, {}],
				)
			
			subscriptions = [s.to_json() for s in subscriptions]
			 
			self.dbpool.bulk_delete(subscriptions)
		except Exception as e:
			print e
			pass
			
		# 4. delete items
		try:
			items = CouchStorage.Item.view(
				'pubsub/items_by_node',
				startkey=[nodeIdentifier],
				endkey=[nodeIdentifier, {}],
				)
			
			items = [i.to_json() for i in items]

			self.dbpool.bulk_delete(items)
		except Exception as e:
			print e
			pass

	def getAffiliations(self, entity):
		return threads.deferToThread(self._getAffiliations, entity)
	
	def _getAffiliations(self, entity):
		affiliations = CouchStorage.Affiliation.view(
			'pubsub/affiliations_by_entity',
			key=entity.userhost(),
			)
		return [ tuple(a['value']) for a in affiliations]


	def getSubscriptions(self, entity):
		return threads.deferToThread(self._getSubscriptions, entity)
		
	def _getSubscriptions(self, entity):
		def toSubscriptions(db_subscriptions):
			subscriptions = []
			for db_subscription in db_subscriptions:
				subscriber = jid.internJID('%s/%s' % (db_subscription.entity,
						 							 db_subscription.resource))
				subscription = Subscription(db_subscription.node, subscriber,
											db_subscription.state)
				subscriptions.append(subscription)
			return subscriptions

		subscriptions = CouchStorage.Subscription.view(
			'pubsub/subscriptions_by_entity',
			startkey=[entity.userhost()],
			endkey=[entity.userhost(), {}, {}],
			)
		#print subscriptions.all()
		return toSubscriptions(subscriptions.all())
	


	def getDefaultConfiguration(self, nodeType):
		return self.defaultConfig[nodeType]



class Node:

	implements(iidavoll.INode)

	def __init__(self, nodeIdentifier, config):
		self.nodeIdentifier = nodeIdentifier
		self._config = config


	def _checkNodeExists(self):
		try:
			node = CouchStorage.Node.get('node' + KEY_SEPARATOR + \
					self.nodeIdentifier)
		except ResourceNotFound:
			raise error.NodeNotFound()			


	def getType(self):
		return self.nodeType


	def getConfiguration(self):
		return self._config


	def setConfiguration(self, options):
		config = copy.copy(self._config)

		for option in options:
			if option in config:
				config[option] = options[option]

		d = threads.deferToThread(self._setConfiguration, config)
		d.addCallback(self._setCachedConfiguration, config)
		return d

	def _setConfiguration(self, config):
		self._checkNodeExists()
		
		try:
			node = CouchStorage.Node.get('node' + KEY_SEPARATOR + \
					self.nodeIdentifier)
			node.persist_items = config["pubsub#persist_items"]
			node.deliver_payloads = config["pubsub#deliver_payloads"]
			node.send_last_published_item = \
					config["pubsub#send_last_published_item"]
			node.save() # update
		except Exception as e:
			raise error.NodeNotFound()

	def _setCachedConfiguration(self, void, config):
		self._config = config


	def getMetaData(self):
		config = copy.copy(self._config)
		config["pubsub#node_type"] = self.nodeType
		return config


	def getAffiliation(self, entity):
		return threads.deferToThread(self._getAffiliation, entity)


	def _getAffiliation(self, entity):
		self._checkNodeExists()
		
		affiliations = CouchStorage.Affiliation.view(
			'pubsub/affiliations_flat',
			startkey=[entity.userhost(), self.nodeIdentifier],
			endkey=[entity.userhost(), self.nodeIdentifier, {}]
			)
		
		if affiliations.count() == 0:
			return None
		else:
			affiliation = affiliations.first()['key'][2]
			return affiliation

	def getSubscription(self, subscriber):
		return threads.deferToThread(self._getSubscription, subscriber)


	def _getSubscription(self, subscriber):
		self._checkNodeExists()

		userhost = subscriber.userhost()
		resource = subscriber.resource or ''

		try:
			subscription = CouchStorage.Subscription.get(
				'subscription' + KEY_SEPARATOR +
				self.nodeIdentifier + KEY_SEPARATOR +
				userhost + KEY_SEPARATOR +
				resource
				)
			return Subscription(self.nodeIdentifier, subscriber,
								subscription.state)
		except ResourceNotFound as e:
			return None


	def getSubscriptions(self, state=None):
		return threads.deferToThread(self._getSubscriptions, state)


	def _getSubscriptions(self, state):
		self._checkNodeExists()
		
		if state:
			db_subscriptions = CouchStorage.Subscription.view(
				'pubsub/subscriptions_by_node_state',
				key=[self.nodeIdentifier, state]
				)
		else:
			db_subscriptions = CouchStorage.Subscription.view(
				'pubsub/subscriptions_by_node_state',
				startkey=[self.nodeIdentifier],
				endkey=[self.nodeIdentifier, {}]
				)

		subscriptions = []
		for subscription in db_subscriptions:
			subscriber = jid.JID('%s/%s' % (subscription.entity, \
					subscription.resource))

			options = {}
			if subscription.subscription_type:
				options['pubsub#subscription_type'] = subscription_type;
			if subscription.subscription_depth:
				options['pubsub#subscription_depth'] = subscription_depth;

			subscriptions.append(Subscription(self.nodeIdentifier, subscriber,
											  subscription.state, options))

		return subscriptions


	def addSubscription(self, subscriber, state, config):
		return threads.deferToThread(self._addSubscription, subscriber,
									 state, config)


	def _addSubscription(self, subscriber, state, config):
		self._checkNodeExists()

		userhost = subscriber.userhost()
		resource = subscriber.resource or ''

		subscription_type = config.get('pubsub#subscription_type')
		subscription_depth = config.get('pubsub#subscription_depth')

		try:
			# jid must be unique
			entity = CouchStorage.Entity(
				jid=userhost
				)
			entity['_id'] = 'entity' + KEY_SEPARATOR + userhost
		except:
			pass

		try:
			subscription = CouchStorage.Subscription(
				node=self.nodeIdentifier,
				entity=userhost,
				resource=resource,
				state=state,
				subscription_type=subscription_type,
				subscription_depth=subscription_depth,
				)
			subscription.save()
		except Exception as e:
			raise error.SubscriptionExists()


	def removeSubscription(self, subscriber):
		return threads.deferToThread(self._removeSubscription, subscriber)

	def _removeSubscription(self, subscriber):
		self._checkNodeExists()

		userhost = subscriber.userhost()
		resource = subscriber.resource or ''
		key = CouchStorage.Subscription.key(node=self.nodeIdentifier, 
				entity=userhost, resource=resource)
		try:
			subscription = CouchStorage.Subscription.get(key)
		except:
			raise error.NotSubscribed()
		
		# delete subscription
		subscription.delete()
		
		return None


	def isSubscribed(self, entity):
		return threads.deferToThread(self._isSubscribed, entity)


	def _isSubscribed(self, entity):
		self._checkNodeExists()
		
		# TODO: dont return docs, only keys
		subscriptions = CouchStorage.Subscription.view(
			'pubsub/subscriptions_by_entity',
			startkey=[entity.userhost(), self.nodeIdentifier, 'subscribed'],
			endkey=[entity.userhost(), self.nodeIdentifier, 'subscribed', {}],
			#limit=0, # dont emit documents, emit only number of documents
			)
		
		#print subscriptions.count()
		if subscriptions.count() > 0:
			return True
		else:
			return False


	def getAffiliations(self):
		return threads.deferToThread(self._getAffiliations)


	def _getAffiliations(self):
		self._checkNodeExists()
		
		affiliations = CouchStorage.Affiliation.view(
			'pubsub/affiliations_by_node',
			key=self.nodeIdentifier,
			)
			
		affiliations = affiliations.all()
		#return [(jid.internJID(r['value'][0]), r['value'][1]) for r in affiliations]
		return [(jid.internJID(r.entity), r.affiliation) for r in affiliations]


class LeafNode(Node):

	implements(iidavoll.ILeafNode)

	nodeType = 'leaf'

	def storeItems(self, items, publisher):
		return threads.deferToThread(self._storeItems, items, publisher)


	def _storeItems(self, items, publisher):
		self._checkNodeExists()
		
		for item in items:
			self._storeItem(item, publisher)


	def _storeItem(self, item, publisher):
		#xml = item.toXml()
		#print 'ORIG: ' + str(item.toXml().encode('utf-8'))
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
		

	def removeItems(self, itemIdentifiers):
		return threads.deferToThread(self._removeItems, itemIdentifiers)


	# FIXME: bulk_delete returns None
	# currently function returns itemIdentifiers, should return only the deleted docs
	def _removeItems(self, itemIdentifiers):
		self._checkNodeExists()

		deleted = []
		
		keys =  [[self.nodeIdentifier, i] for i in itemIdentifiers]

		items = CouchStorage.Item.view(
			'pubsub/items_by_node',
			keys=keys
			)
		
		items = [i.to_json() for i in items]
		
		if not items:
			return []

		try:
			response = self.dbpool.bulk_delete(items) # FIXME: returns None (why?)
			#response = CouchStorage.Item.bulk_save(items)
			return itemIdentifiers
		except Exception as e:
			print e
			return []
			
		return itemIdentifiers


	def getItems(self, maxItems=None):
		return threads.deferToThread(self._getItems, maxItems)


	def _getItems(self, maxItems):
		self._checkNodeExists()
		
		if maxItems:
			items = CouchStorage.Item.view(
				'pubsub/items_by_node_date',
				startkey=[self.nodeIdentifier],
				endkey=[self.nodeIdentifier, {}, {}],
				limit=maxItems
				)
		else:
			items = CouchStorage.Item.view(
				'pubsub/items_by_node_date',
				startkey=[self.nodeIdentifier],
				endkey=[self.nodeIdentifier, {}, {}],
				)
		
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
			'pubsub/items_by_node',
			keys=keys
		)
		
		#values = [parseXml(i.data.encode('utf-8')) for i in items.all()]
		s = DictSerializer()
		values = [s.serialize_to_xml(i.data) for i in items.all()]
		return values
	

	def purge(self):
		return threads.deferToThread(self._purge)


	def _purge(self):
		self._checkNodeExists()

		items = CouchStorage.Item.view(
			'pubsub/items_by_node',
			startkey=[self.nodeIdentifier],
			endkey=[self.nodeIdentifier, {}]
			)
		items = [i.to_json() for i in items]

		try:
			response = self.dbpool.bulk_delete(items) # FIXME: returns None (why?)
		except Exception as e:
			print e
		

class CollectionNode(Node):

	nodeType = 'collection'



class GatewayStorage(object):
	"""
	Memory based storage facility for the XMPP-HTTP gateway.
	"""

	def __init__(self, dbpool):
		self.dbpool = dbpool


	def _countCallbacks(self, cursor, service, nodeIdentifier):
		"""
		Count number of callbacks registered for a node.
		"""
		cursor.execute("""SELECT count(*) FROM callbacks
						  WHERE service=%s and node=%s""",
					   service.full(),
					   nodeIdentifier)
		results = cursor.fetchall()
		return results[0][0]


	def addCallback(self, service, nodeIdentifier, callback):
		def interaction(cursor):
			cursor.execute("""SELECT 1 FROM callbacks
							  WHERE service=%s and node=%s and uri=%s""",
						   service.full(),
						   nodeIdentifier,
						   callback)
			if cursor.fetchall():
				return

			cursor.execute("""INSERT INTO callbacks
							  (service, node, uri) VALUES
							  (%s, %s, %s)""",
						   service.full(),
						   nodeIdentifier,
						   callback)

		return self.dbpool.runInteraction(interaction)


	def removeCallback(self, service, nodeIdentifier, callback):
		def interaction(cursor):
			cursor.execute("""DELETE FROM callbacks
							  WHERE service=%s and node=%s and uri=%s""",
						   service.full(),
						   nodeIdentifier,
						   callback)

			if cursor.rowcount != 1:
				raise error.NotSubscribed()

			last = not self._countCallbacks(cursor, service, nodeIdentifier)
			return last

		return self.dbpool.runInteraction(interaction)

	def getCallbacks(self, service, nodeIdentifier):
		def interaction(cursor):
			cursor.execute("""SELECT uri FROM callbacks
							  WHERE service=%s and node=%s""",
						   service.full(),
						   nodeIdentifier)
			results = cursor.fetchall()

			if not results:
				raise error.NoCallbacks()

			return [result[0] for result in results]

		return self.dbpool.runInteraction(interaction)


	def hasCallbacks(self, service, nodeIdentifier):
		def interaction(cursor):
			return bool(self._countCallbacks(cursor, service, nodeIdentifier))

		return self.dbpool.runInteraction(interaction)
