# Copyright (c) 2010 Nuno Santos
# See LICENSE for details.

import copy
import datetime

from zope.interface import implements

from restkit import SimplePool
#import restkit, logging

from couchdbkit import *

from twisted.words.protocols.jabber import jid

from wokkel.generic import parseXml, stripNamespace
from wokkel.pubsub import Subscription

from idavoll import error, iidavoll

KEY_SEPARATOR = ':'


# Main CouchDB Storage class
class Storage:
	
	# Data structures
	class Node(Document):
		node = StringProperty()
		node_type = StringProperty()
		persist_items = BooleanProperty()
		deliver_payloads = BooleanProperty()
		send_last_published_item = StringProperty()
		entities = ListProperty()
		date = DateTimeProperty()

	class Entity(Document):
		jid = StringProperty()
		nodes = ListProperty() # node affiliation list [{node, affiliation}]

	class Affiliation(Document):
		entity_id = StringProperty()
		node_id = StringProperty()
		affiliation = StringProperty()
	
	
	implements(iidavoll.IStorage)

	defaultConfig = {
			'leaf': {
				"pubsub#persist_items": True,
				"pubsub#deliver_payloads": True,
				"pubsub#send_last_published_item": 'on_sub',
			},
			'collection': {
				"pubsub#deliver_payloads": True,
				"pubsub#send_last_published_item": 'on_sub',
			}
	}

	def __init__(self, dbpool):
		#restkit.set_logging(logging.CRITICAL);
		self.dbpool = dbpool
		
		# associate datastructures to the db
		self.Node.set_db(self.dbpool)
		self.Entity.set_db(self.dbpool)
		self.Affiliation.set_db(self.dbpool)


	def getNode(self, nodeIdentifier):
		return self._getNode(nodeIdentifier)

	
	def _getNode(self, nodeIdentifier):
		configuration = {}
		
		try:
			node = self.Node.get('node%s%s' % (KEY_SEPARATOR, nodeIdentifier))
		except ResourceNotFound:
			raise error.NodeNotFound()

		if node.node_type == 'leaf':
			configuration = {
					'pubsub#persist_items': node.persist_items,
					'pubsub#deliver_payloads': node.deliver_payloads,
					'pubsub#send_last_published_item': node.send_last_published_item}
			
			return_node = LeafNode(nodeIdentifier, configuration)
			return_node.dbpool = self.dbpool
			return return_node
		elif node.node_type == 'collection':
			configuration = {
					'pubsub#deliver_payloads': node.deliver_payloads,
					'pubsub#send_last_published_item': node.send_last_published_item}
			return_node = CollectionNode(nodeIdentifier, configuration)
			return_node.dbpool = self.dbpool
			return return_node



	def getNodeIds(self):
		d = self.dbpool.runQuery("""SELECT node from nodes""")
		d.addCallback(lambda results: [r[0] for r in results])
		return d


	def createNode(self, nodeIdentifier, owner, config):
		return self._createNode(nodeIdentifier, owner, config)

	def _createNode(self, nodeIdentifier, owner, config):
		if config['pubsub#node_type'] != 'leaf':
			raise error.NoCollections()

		owner = owner.userhost()
		
		# check if node exists
		try:
			node = self.Node.get('node%s%s' % (KEY_SEPARATOR, nodeIdentifier))
			raise error.NodeExists()
		except ResourceNotFound:
			pass

		try:
			node = self.Node(
				#node = nodeIdentifier,
				node_type = 'leaf',
				persist_items = config['pubsub#persist_items'],
				deliver_payloads = config['pubsub#deliver_payloads'],
				send_last_published_item = config['pubsub#send_last_published_item'],
				entities = [{'jid': owner, 'affiliation': 'owner'}],
				date = datetime.datetime.utcnow()
				)
			node['_id'] = 'node:' + nodeIdentifier
			node.save()
		except Exception as e:
			raise error.Error()

		# save entity / affiliation
		try:
			owner_entity = self.Entity.get('entity%s%s' % (KEY_SEPARATOR, owner))
		except ResourceNotFound:
			entity = self.Entity(jid=owner, nodes=[{'node': nodeIdentifier, 'affiliation': 'owner'}])
			entity['_id'] = 'entity%s%s' % (KEY_SEPARATOR, owner)
			entity.save()

	def deleteNode(self, nodeIdentifier):
		return self._deleteNode(nodeIdentifier)


	def _deleteNode(self, nodeIdentifier):
		try:
			node = self.Node.get('node%s%s' % (KEY_SEPARATOR, nodeIdentifier))
			node.delete()
		except:
			raise error.NodeNotFound()
			


	def getAffiliations(self, entity):
		d = self.dbpool.runQuery("""SELECT node, affiliation FROM entities
										NATURAL JOIN affiliations
										NATURAL JOIN nodes
										WHERE jid=%s""",
									 (entity.userhost(),))
		d.addCallback(lambda results: [tuple(r) for r in results])
		return d


	def getSubscriptions(self, entity):
		def toSubscriptions(rows):
			subscriptions = []
			for row in rows:
				subscriber = jid.internJID('%s/%s' % (row[1],
													  row[2]))
				subscription = Subscription(row[0], subscriber, row[3])
				subscriptions.append(subscription)
			return subscriptions

		d = self.dbpool.runQuery("""SELECT node, jid, resource, state
									 FROM entities
									 NATURAL JOIN subscriptions
									 NATURAL JOIN nodes
									 WHERE jid=%s""",
								  (entity.userhost(),))
		d.addCallback(toSubscriptions)
		return d


	def getDefaultConfiguration(self, nodeType):
		return self.defaultConfig[nodeType]



class Node:

	implements(iidavoll.INode)

	def __init__(self, nodeIdentifier, config):
		self.nodeIdentifier = nodeIdentifier
		self._config = config


	def _checkNodeExists(self, cursor):
		cursor.execute("""SELECT node_id FROM nodes WHERE node=%s""",
					   (self.nodeIdentifier,))
		if not cursor.fetchone():
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

		d = self.dbpool.runInteraction(self._setConfiguration, config)
		d.addCallback(self._setCachedConfiguration, config)
		return d


	def _setConfiguration(self, cursor, config):
		self._checkNodeExists(cursor)
		cursor.execute("""UPDATE nodes SET persist_items=%s,
										   deliver_payloads=%s,
										   send_last_published_item=%s
						  WHERE node=%s""",
					   (config["pubsub#persist_items"],
						config["pubsub#deliver_payloads"],
						config["pubsub#send_last_published_item"],
						self.nodeIdentifier))


	def _setCachedConfiguration(self, void, config):
		self._config = config


	def getMetaData(self):
		config = copy.copy(self._config)
		config["pubsub#node_type"] = self.nodeType
		return config


	def getAffiliation(self, entity):
		return self.dbpool.runInteraction(self._getAffiliation, entity)


	def _getAffiliation(self, cursor, entity):
		self._checkNodeExists(cursor)
		cursor.execute("""SELECT affiliation FROM affiliations
						  NATURAL JOIN nodes
						  NATURAL JOIN entities
						  WHERE node=%s AND jid=%s""",
					   (self.nodeIdentifier,
						entity.userhost()))

		try:
			return cursor.fetchone()[0]
		except TypeError:
			return None


	def getSubscription(self, subscriber):
		return self.dbpool.runInteraction(self._getSubscription, subscriber)


	def _getSubscription(self, cursor, subscriber):
		self._checkNodeExists(cursor)

		userhost = subscriber.userhost()
		resource = subscriber.resource or ''

		cursor.execute("""SELECT state FROM subscriptions
						  NATURAL JOIN nodes
						  NATURAL JOIN entities
						  WHERE node=%s AND jid=%s AND resource=%s""",
					   (self.nodeIdentifier,
						userhost,
						resource))
		row = cursor.fetchone()
		if not row:
			return None
		else:
			return Subscription(self.nodeIdentifier, subscriber, row[0])


	def getSubscriptions(self, state=None):
		return self.dbpool.runInteraction(self._getSubscriptions, state)


	def _getSubscriptions(self, cursor, state):
		self._checkNodeExists(cursor)

		query = """SELECT jid, resource, state,
						  subscription_type, subscription_depth
				   FROM subscriptions
				   NATURAL JOIN nodes
				   NATURAL JOIN entities
				   WHERE node=%s""";
		values = [self.nodeIdentifier]

		if state:
			query += " AND state=%s"
			values.append(state)

		cursor.execute(query, values);
		rows = cursor.fetchall()

		subscriptions = []
		for row in rows:
			subscriber = jid.JID('%s/%s' % (row[0], row[1]))

			options = {}
			if row[3]:
				options['pubsub#subscription_type'] = row[3];
			if row[4]:
				options['pubsub#subscription_depth'] = row[4];

			subscriptions.append(Subscription(self.nodeIdentifier, subscriber,
											  row[2], options))

		return subscriptions


	def addSubscription(self, subscriber, state, config):
		return self.dbpool.runInteraction(self._addSubscription, subscriber,
										  state, config)


	def _addSubscription(self, cursor, subscriber, state, config):
		self._checkNodeExists(cursor)

		userhost = subscriber.userhost()
		resource = subscriber.resource or ''

		subscription_type = config.get('pubsub#subscription_type')
		subscription_depth = config.get('pubsub#subscription_depth')

		try:
			# jid must be unique
			cursor.execute("""INSERT INTO entities (jid) VALUES (%s)""",
						   (userhost,))
		
		except cursor._pool.dbapi.IntegrityError:
			cursor.connection.rollback()

		try:
			cursor.execute("""INSERT INTO subscriptions
							  (node_id, entity_id, resource, state,
							   subscription_type, subscription_depth)
							  SELECT node_id, entity_id, %s, %s, %s, %s FROM
							  (SELECT node_id FROM nodes
											  WHERE node=%s) as n
							  CROSS JOIN
							  (SELECT entity_id FROM entities
												WHERE jid=%s) as e""",
						   (resource,
							state,
							subscription_type,
							subscription_depth,
							self.nodeIdentifier,
							userhost))
							
		except cursor._pool.dbapi.IntegrityError:
			raise error.SubscriptionExists()


	def removeSubscription(self, subscriber):
		return self.dbpool.runInteraction(self._removeSubscription,
										   subscriber)


	def _removeSubscription(self, cursor, subscriber):
		self._checkNodeExists(cursor)

		userhost = subscriber.userhost()
		resource = subscriber.resource or ''

		cursor.execute("""DELETE FROM subscriptions WHERE
						  node_id=(SELECT node_id FROM nodes
												  WHERE node=%s) AND
						  entity_id=(SELECT entity_id FROM entities
													  WHERE jid=%s) AND
						  resource=%s""",
					   (self.nodeIdentifier,
						userhost,
						resource))
		if cursor.rowcount != 1:
			raise error.NotSubscribed()

		return None


	def isSubscribed(self, entity):
		return self.dbpool.runInteraction(self._isSubscribed, entity)


	def _isSubscribed(self, cursor, entity):
		self._checkNodeExists(cursor)

		cursor.execute("""SELECT 1 FROM entities
						  NATURAL JOIN subscriptions
						  NATURAL JOIN nodes
						  WHERE entities.jid=%s
						  AND node=%s AND state='subscribed'""",
					   (entity.userhost(),
					   self.nodeIdentifier))

		return cursor.fetchone() is not None


	def getAffiliations(self):
		return self.dbpool.runInteraction(self._getAffiliations)


	def _getAffiliations(self, cursor):
		self._checkNodeExists(cursor)

		cursor.execute("""SELECT jid, affiliation FROM nodes
						  NATURAL JOIN affiliations
						  NATURAL JOIN entities
						  WHERE node=%s""",
					   (self.nodeIdentifier,))
		result = cursor.fetchall()

		return [(jid.internJID(r[0]), r[1]) for r in result]



class LeafNode(Node):

	implements(iidavoll.ILeafNode)

	nodeType = 'leaf'

	def storeItems(self, items, publisher):
		return self.dbpool.runInteraction(self._storeItems, items, publisher)


	def _storeItems(self, cursor, items, publisher):
		self._checkNodeExists(cursor)
		for item in items:
			self._storeItem(cursor, item, publisher)


	def _storeItem(self, cursor, item, publisher):
		data = item.toXml()
		cursor.execute("""UPDATE items SET date=now(), publisher=%s, data=%s
						  FROM nodes
						  WHERE nodes.node_id = items.node_id AND
								nodes.node = %s and items.item=%s""",
					   (publisher.full(),
						data,
						self.nodeIdentifier,
						item["id"]))
		if cursor.rowcount == 1:
			return

		cursor.execute("""INSERT INTO items (node_id, item, publisher, data)
						  SELECT node_id, %s, %s, %s FROM nodes
													 WHERE node=%s""",
					   (item["id"],
						publisher.full(),
						data,
						self.nodeIdentifier))


	def removeItems(self, itemIdentifiers):
		return self.dbpool.runInteraction(self._removeItems, itemIdentifiers)


	def _removeItems(self, cursor, itemIdentifiers):
		self._checkNodeExists(cursor)

		deleted = []

		for itemIdentifier in itemIdentifiers:
			cursor.execute("""DELETE FROM items WHERE
							  node_id=(SELECT node_id FROM nodes
													  WHERE node=%s) AND
							  item=%s""",
						   (self.nodeIdentifier,
							itemIdentifier))

			if cursor.rowcount:
				deleted.append(itemIdentifier)

		return deleted


	def getItems(self, maxItems=None):
		return self.dbpool.runInteraction(self._getItems, maxItems)


	def _getItems(self, cursor, maxItems):
		self._checkNodeExists(cursor)
		query = """SELECT data FROM nodes
				   NATURAL JOIN items
				   WHERE node=%s ORDER BY date DESC"""
		if maxItems:
			cursor.execute(query + " LIMIT %s",
						   (self.nodeIdentifier,
							maxItems))
		else:
			cursor.execute(query, (self.nodeIdentifier,))

		result = cursor.fetchall()
		items = [stripNamespace(parseXml(r[0])) for r in result]
		return items


	def getItemsById(self, itemIdentifiers):
		return self.dbpool.runInteraction(self._getItemsById, itemIdentifiers)


	def _getItemsById(self, cursor, itemIdentifiers):
		self._checkNodeExists(cursor)
		items = []
		for itemIdentifier in itemIdentifiers:
			cursor.execute("""SELECT data FROM nodes
							  NATURAL JOIN items
							  WHERE node=%s AND item=%s""",
						   (self.nodeIdentifier,
							itemIdentifier))
			result = cursor.fetchone()
			if result:
				items.append(parseXml(result[0]))
		return items


	def purge(self):
		return self.dbpool.runInteraction(self._purge)


	def _purge(self, cursor):
		self._checkNodeExists(cursor)

		cursor.execute("""DELETE FROM items WHERE
						  node_id=(SELECT node_id FROM nodes WHERE node=%s)""",
					   (self.nodeIdentifier,))


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
