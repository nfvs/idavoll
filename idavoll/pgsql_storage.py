# Copyright (c) 2003-2008 Ralph Meijer
# See LICENSE for details.

import copy

from zope.interface import implements

from twisted.enterprise import adbapi
from twisted.words.protocols.jabber import jid

from wokkel.generic import parseXml, stripNamespace
from wokkel.pubsub import Subscription

from idavoll import error, iidavoll

from string import Template


"""
Get Direct Child Nodes
parameters:
	$fields: fields to return (nodes.node, nodes.node_id, ...)
	$node: parent (collection) node name
"""
sql_get_direct_child_nodes = Template(
"""SELECT $fields from nodes
	INNER JOIN nodes AS n ON (nodes.collection = n.node_id)
	WHERE n.node='$node'
	ORDER BY nodes.node""")

class Storage:

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
		self.dbpool = dbpool


	def getNode(self, nodeIdentifier):
		return self.dbpool.runInteraction(self._getNode, nodeIdentifier)


	def _getNode(self, cursor, nodeIdentifier):
		configuration = {}
		cursor.execute("""SELECT node_type,
								 persist_items,
								 deliver_payloads,
								 send_last_published_item,
								 collection
						  FROM nodes
						  WHERE node=%s""",
					   (nodeIdentifier,))
		row = cursor.fetchone()

		if not row:
			raise error.NodeNotFound()
			
		if row[0] == 'leaf':
			configuration = {
					'pubsub#node_type': 'leaf',
					'pubsub#persist_items': row[1],
					'pubsub#deliver_payloads': row[2],
					'pubsub#send_last_published_item': row[3],
					'pubsub#collection': row[4]}
			node = LeafNode(nodeIdentifier, configuration)
			node.dbpool = self.dbpool
			return node
		elif row[0] == 'collection':
			configuration = {
					'pubsub#node_type': 'collection',
					'pubsub#deliver_payloads': row[2],
					'pubsub#send_last_published_item': row[3],
					'pubsub#collection': row[4]}
			node = CollectionNode(nodeIdentifier, configuration)
			node.dbpool = self.dbpool
			return node
		else:
			raise error.NodeNotFound()



	def getNodeIds(self):
		d = self.dbpool.runQuery("""SELECT node from nodes""")
		d.addCallback(lambda results: [r[0] for r in results])
		return d

	def getChildNodeIds(self, parentNodeIdentifier=''):
		return self.dbpool.runInteraction(self._getChildNodeIds, parentNodeIdentifier)

	def _getChildNodeIds(self, cursor, parentNodeIdentifier):
		sql = sql_get_direct_child_nodes.substitute(fields='nodes.node',
			node=parentNodeIdentifier)
		cursor.execute(sql)
		rows = cursor.fetchall()
		
		if len(rows) > 0:
			#result = [n['value'] for n in nodes.all()]
			result = [r[0] for r in rows]
		else:
			result = []
		return result

	def createNode(self, nodeIdentifier, owner, config=None):
		return self.dbpool.runInteraction(self._createNode, nodeIdentifier,
										   owner, config)


	def _createNode(self, cursor, nodeIdentifier, owner, config):
		#if config['pubsub#node_type'] != 'leaf':
		#	raise error.NoCollections()

		owner = owner.userhost()
		
		if 'pubsub#node_type' in config:
			nodeType = config['pubsub#node_type']
		else:
			nodeType = 'leaf'
		
		try:
			collection = ''
			if 'pubsub#collection' in config:
				collection = config['pubsub#collection']
				
			# get collection (parent node_id)
			if collection != '':
				cursor.execute("""SELECT node_id from nodes where node=%s""", (collection,))
				row = cursor.fetchone()
				if row is None:
					print 'Collection node not found'
					raise error.NodeNotFound()
				else:
					collection_id = row[0]
			else:
				collection_id = 0
			
			if nodeType == 'leaf':
				cursor.execute("""INSERT INTO nodes
								  (node, node_type, persist_items,
								   deliver_payloads, send_last_published_item, collection)
								  VALUES
								  (%s, 'leaf', %s, %s, %s, %s)""",
							   (nodeIdentifier,
								config['pubsub#persist_items'],
								config['pubsub#deliver_payloads'],
								config['pubsub#send_last_published_item'],
								collection_id)
							   )
			# collection node
			elif nodeType == 'collection':

				cursor.execute("""INSERT INTO nodes
								  (node, node_type, collection)
								  VALUES
								  (%s, 'collection', %s)""",
							   (nodeIdentifier, collection_id)
							   )
				
			else:
				raise error.Error(msg='Unknown node type')
				
		#except cursor._pool.dbapi.OperationalError:
		except error.NodeNotFound:
			raise error.NodeNotFound()
		except cursor._pool.dbapi.IntegrityError:
			raise error.NodeExists()
		except Exception as e:
			print 'Error: ' + unicode(e)
			raise error.Error()

		cursor.execute("""SELECT 1 from entities where jid=%s""",
					   (owner,))

		if not cursor.fetchone():
			cursor.execute("""INSERT INTO entities (jid) VALUES (%s)""",
						   (owner,))

		cursor.execute("""INSERT INTO affiliations
						  (node_id, entity_id, affiliation)
						  SELECT node_id, entity_id, 'owner' FROM
						  (SELECT node_id FROM nodes WHERE node=%s) as n
						  CROSS JOIN
						  (SELECT entity_id FROM entities
											WHERE jid=%s) as e""",
					   (nodeIdentifier, owner))

	def deleteNode(self, nodeIdentifier):
		return self.dbpool.runInteraction(self._deleteNode, nodeIdentifier)


	def _deleteNode(self, cursor, nodeIdentifier):
		cursor.execute("""DELETE FROM nodes WHERE node=%s""",
					   (nodeIdentifier,))

		if cursor.rowcount != 1:
			raise error.NodeNotFound()
	
	
	def setSubscriptionOptions(self, nodeIdentifier, subscriber, options,
							   subscriptionIdentifier=None, sender=None):
		return self.dbpool.runInteraction(self._setSubscriptionOptions,
				nodeIdentifier, subscriber, options, subscriptionIdentifier,
				sender)

	# TODO: JID resource??
	def _setSubscriptionOptions(self, cursor, nodeIdentifier, subscriber, options,
								subscriptionIdentifier=None, sender=None):
		userhost = subscriber.userhost()
		resource = subscriber.resource or ''

		try:
			sqlStr = "UPDATE subscriptions SET "

			if 'pubsub#subscription_type' in options:
				sqlStr += " subscription_type = %s," % options['pubsub#subscription_type']
			if 'pubsub#subscription_depth' in options:
				sqlStr += " subscription_type = %s," % options['pubsub#subscription_type']
			
			# remove ','
			if sqlStr[-1:] == ",":
				sqlStr = sqlStr[:-1]

			cursor.execute(sqlStr)
		except Exception as e:
			#raise error.
			pass



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
		return self.defaultConfig[nodeType].copy()



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
	
	# TODO: test me!!!
	def setAffiliations(self, affiliations):
		return self.dbpool.runInteraction(self._setAffiliations, affiliations)

	def _setAffiliations(self, cursor, affiliations):
		
		# jids list to sql (jid1, jid2, jid3)
		def jidsToSql(jids):
			jidsStr = ''.join(["'" + j + "'," for j in jids]) # jid str; e.g. 'jid1', 'jid2'
			if jidsStr[-1:] == ',': jidsStr = jidsStr[:-1]
			return jidsStr
			
		# affiliations = [ { 'jid': .., 'affiliation': .. }, ..]
		
		affs = affiliations
		if not affs:
			return
			
		jidsStr = ''
		
		# use only bare JID, not full JID
		jids = [a['jid'].userhost() for a in affiliations] # jid list
		
		# insert new entities
		cursor.execute("""SELECT jid FROM entities WHERE jid IN (%s)""" % jidsToSql(jids) )
		existingJids = [existing[0] for existing in cursor.fetchall()]
		
		#print 'existing: %s' % existingJids
		# insert new entities (dont insert existing ones)
		insertJids = [j for j in jids if j not in existingJids]
		if insertJids:
			print 'insert: %s' % insertJids
			cursor.executemany("""INSERT INTO entities (jid) VALUES (%s)""", [[j] for j in jids] )
		
		# delete existing affiliations first
		cursor.execute("""DELETE FROM affiliations
			WHERE node_id=(SELECT node_id FROM nodes WHERE node='%s') AND
			entity_id IN (SELECT entity_id FROM entities WHERE jid IN (%s))""" % (self.nodeIdentifier, jidsToSql(jids)) )
		
		# insert new affiliations
		insertSql = """"""
			
		for aff in affs:
			insertSql += """INSERT INTO affiliations (node_id, entity_id, affiliation)
				SELECT node_id, entity_id, '%s' FROM 
				(SELECT node_id FROM nodes WHERE node='%s') as n
				CROSS JOIN (SELECT entity_id FROM entities WHERE jid='%s') as e; """ % (aff['affiliation'], self.nodeIdentifier, aff['jid'].userhost())
		
		#print 'INSERT SQL: %s' % insertSql
		cursor.execute(insertSql)
		return
		
		


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
		
		print 'SELF CONF: %s' % self.getConfiguration()

		#subscription_type = config.get('pubsub#subscription_type')
		#subscription_depth = config.get('pubsub#subscription_depth')
		
		userhost = subscriber.userhost()
		resource = subscriber.resource or ''
		node_type = self._config['pubsub#node_type']

		#print 'add subscription options: %s' % config
		if config:
			subscription_type = config.get('pubsub#subscription_type') or 'nodes'
			subscription_depth = config.get('pubsub#subscription_depth') or '1'
		# default subscription for collection nodes
		elif node_type == 'collection':
			subscription_type = 'nodes'
			subscription_depth = '1'
		# subscription options for leaf nodes (empty)
		else:
			subscription_type = ''
			subscription_depth = ''

		try:
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
