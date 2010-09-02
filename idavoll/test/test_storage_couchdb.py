# Copyright (c) 2003-2008 Ralph Meijer
# See LICENSE for details.

"""
Tests for L{idavoll.memory_storage} and L{idavoll.pgsql_storage}.
"""

from zope.interface.verify import verifyObject
from twisted.trial import unittest
from twisted.words.protocols.jabber import jid
from twisted.internet import defer
from twisted.words.xish import domish

from wokkel import pubsub

from idavoll import error, iidavoll

OWNER = jid.JID('owner@example.com')
SUBSCRIBER = jid.JID('subscriber@example.com/Home')
SUBSCRIBER_NEW = jid.JID('new@example.com/Home')
SUBSCRIBER_TO_BE_DELETED = jid.JID('to_be_deleted@example.com/Home')
SUBSCRIBER_PENDING = jid.JID('pending@example.com/Home')
PUBLISHER = jid.JID('publisher@example.com')
ITEM = domish.Element((None, 'item'))
ITEM['id'] = 'current'
ITEM.addElement(('testns', 'test'), content=u'Test \u2083 item')
ITEM_NEW = domish.Element((None, 'item'))
ITEM_NEW['id'] = 'new'
ITEM_NEW.addElement(('testns', 'test'), content=u'Test \u2083 item')
ITEM_UPDATED = domish.Element((None, 'item'))
ITEM_UPDATED['id'] = 'current'
ITEM_UPDATED.addElement(('testns', 'test'), content=u'Test \u2084 item')
ITEM_TO_BE_DELETED = domish.Element((None, 'item'))
ITEM_TO_BE_DELETED['id'] = 'to-be-deleted'
ITEM_TO_BE_DELETED.addElement(('testns', 'test'), content=u'Test \u2083 item')

def decode(object):
	if isinstance(object, str):
		object = object.decode('utf-8')
	return object



class StorageTests:

	def _assignTestNode(self, node):
		self.node = node


	def setUp(self):
		d = self.s.getNode('pre-existing')
		self._assignTestNode(d)

	# ok
	def test_interfaceIStorage(self):
		self.assertTrue(verifyObject(iidavoll.IStorage, self.s))

	# ok
	def test_interfaceINode(self):
		self.assertTrue(verifyObject(iidavoll.INode, self.node))

	# ok
	def test_interfaceILeafNode(self):
		self.assertTrue(verifyObject(iidavoll.ILeafNode, self.node))

	# ok
	def test_getNode(self):
		return self.s.getNode('pre-existing')

	# ok
	def test_getNonExistingNode(self):
		self.assertRaises(error.NodeNotFound, lambda: self.s.getNode('non-existing'))

	# ok
	def test_getNodeIDs(self):
		nodeIdentifiers = self.s.getNodeIds()
		self.assertIn('pre-existing', nodeIdentifiers)
		self.assertNotIn('non-existing', nodeIdentifiers)

	# ok
	def test_createExistingNode(self):
		config = self.s.getDefaultConfiguration('leaf')
		config['pubsub#node_type'] = 'leaf'
		self.assertRaises(error.NodeExists, lambda: self.s.createNode('pre-existing', OWNER, config))

	# ok
	def test_createNode(self):		
		config = self.s.getDefaultConfiguration('leaf')
		config['pubsub#node_type'] = 'leaf'
		self.s.createNode('new 1', OWNER, config)
		node_created = self.s.getNode('new 1')

	# ok
	def test_deleteNonExistingNode(self):
		self.assertRaises(error.NodeNotFound, lambda: self.s.deleteNode('non-existing'))

	# ok
	def test_deleteNode(self):
		d = self.s.deleteNode('to-be-deleted')
		self.assertRaises(error.NodeNotFound, lambda: self.s.getNode('to-be-deleted'))
		return d

	# ok
	def test_getAffiliations(self):
		affiliations = self.s.getAffiliations(OWNER)
		self.assertIn(('pre-existing', 'owner'), affiliations)

	# ok
	def test_getSubscriptions(self):
		subscriptions = self.s.getSubscriptions(SUBSCRIBER)
		found = False
		for subscription in subscriptions:
			if (subscription.nodeIdentifier == 'pre-existing' and
				subscription.subscriber == SUBSCRIBER and
				subscription.state == 'subscribed'):
				found = True
		self.assertTrue(found)


	# Node tests

	# ok
	def test_getType(self):
		self.assertEqual(self.node.getType(), 'leaf')

	# ok
	def test_getConfiguration(self):
		config = self.node.getConfiguration()
		self.assertIn('pubsub#persist_items', config.iterkeys())
		self.assertIn('pubsub#deliver_payloads', config.iterkeys())
		self.assertEqual(config['pubsub#persist_items'], True)
		self.assertEqual(config['pubsub#deliver_payloads'], True)

	# ok
	def test_setConfiguration(self):
		node = self.s.getNode('to-be-reconfigured')
		node.setConfiguration({'pubsub#persist_items': False})
		config = node.getConfiguration()
		self.assertEqual(config['pubsub#persist_items'], False)
		node = self.s.getNode('to-be-reconfigured')
		config = node.getConfiguration()
		self.assertEqual(config['pubsub#persist_items'], False)

	# ok
	def test_getMetaData(self):
		metaData = self.node.getMetaData()
		for key, value in self.node.getConfiguration().iteritems():
			self.assertIn(key, metaData.iterkeys())
			self.assertEqual(value, metaData[key])
		self.assertIn('pubsub#node_type', metaData.iterkeys())
		self.assertEqual(metaData['pubsub#node_type'], 'leaf')

	# ok
	def test_getAffiliation(self):
		affiliation = self.node.getAffiliation(OWNER)
		self.assertEqual(affiliation, 'owner')

	# ok
	def test_getNonExistingAffiliation(self):
		affiliation = self.node.getAffiliation(SUBSCRIBER)
		self.assertEqual(affiliation, None)

	# ok
	def test_addSubscription(self):
		self.node.addSubscription(SUBSCRIBER_NEW, 'pending', {})
		subscription = self.node.getSubscription(SUBSCRIBER_NEW)
		self.assertEqual(subscription.state, 'pending')

	# ok
	def test_addExistingSubscription(self):
		self.assertRaises(error.SubscriptionExists, lambda: self.node.addSubscription(SUBSCRIBER, 'pending', {}))

	# ok
	def test_getSubscription(self):

		subscriptions = [self.node.getSubscription(SUBSCRIBER),
						 self.node.getSubscription(SUBSCRIBER_PENDING),
						 self.node.getSubscription(OWNER)]
		self.assertEquals(subscriptions[0].state, 'subscribed')
		self.assertEquals(subscriptions[1].state, 'pending')
		self.assertEquals(subscriptions[2], None)

	# ok
	def test_removeSubscription(self):
		return self.node.removeSubscription(SUBSCRIBER_TO_BE_DELETED)

	# ok
	def test_removeNonExistingSubscription(self):
		self.assertRaises(error.NotSubscribed, lambda: self.node.removeSubscription(OWNER))

	# ok
	def test_getNodeSubscriptions(self):
		subscriptions = self.node.getSubscriptions('subscribed')
		subscribers = [subscription.subscriber for subscription in subscriptions]
		self.assertIn(SUBSCRIBER, subscribers)
		self.assertNotIn(SUBSCRIBER_PENDING, subscribers)
		self.assertNotIn(OWNER, subscribers)

	# ok
	def test_isSubscriber(self):

		subscribed = [self.node.isSubscribed(SUBSCRIBER),
					  self.node.isSubscribed(SUBSCRIBER.userhostJID()),
					  self.node.isSubscribed(SUBSCRIBER_PENDING),
					  self.node.isSubscribed(OWNER)]
		self.assertEquals(subscribed[0], True)
		self.assertEquals(subscribed[1], True)
		self.assertEquals(subscribed[2], False)
		self.assertEquals(subscribed[3], False)

	# ok
	def test_storeItems(self):
		self.node.storeItems([ITEM_NEW], PUBLISHER)
		result = self.node.getItemsById(['new'])
		self.assertEqual(ITEM_NEW.toXml(), result[0].toXml())

	# ok
	def test_storeUpdatedItems(self):
		d = self.node.storeItems([ITEM_UPDATED], PUBLISHER)
		result = self.node.getItemsById(['current'])
		self.assertEqual(ITEM_UPDATED.toXml(), result[0].toXml())

	# ok (FIXME)
	def test_removeItems(self):
		result = self.node.removeItems(['to-be-deleted', 'to-be-deleted2'])
		self.assertEqual(['to-be-deleted', 'to-be-deleted2'], result)
		result = self.node.getItemsById(['to-be-deleted'])
		self.assertEqual(0, len(result))
		result = self.node.getItemsById(['to-be-deleted2'])
		self.assertEqual(0, len(result))

	# ok
	def test_removeNonExistingItems(self):
		result = self.node.removeItems(['non-existing'])
		self.assertEqual([], result)

	# ok
	def test_getItems(self):
		result = self.node.getItems()
		items = [item.toXml() for item in result]
		self.assertIn(ITEM.toXml(), items)


	# ok
	def test_lastItem(self):
		result = self.node.getItems(1)
		self.assertEqual(1, len(result))
		self.assertEqual(ITEM.toXml(), result[0].toXml())

	# ok
	def test_getItemsById(self):
		result = self.node.getItemsById(['current'])
		self.assertEqual(1, len(result))

	# ok
	def test_getNonExistingItemsById(self):
		result = self.node.getItemsById(['non-existing'])
		self.assertEqual(0, len(result))

	# ok (FIXME: bulk_delete)
	def test_purge(self):
		node = self.s.getNode('to-be-purged')
		node.purge()
		result = node.getItems()
		self.assertEqual([], result)

	# ok
	def test_getNodeAffilatiations(self):
		node = self.s.getNode('pre-existing')
		affiliations = node.getAffiliations()
		affiliations = dict(((a[0].full(), a[1]) for a in affiliations))
		self.assertEquals(affiliations[OWNER.full()], 'owner')



class CouchdbStorageStorageTestCase(unittest.TestCase, StorageTests):
	
	def setUp(self):
		from idavoll.couchdb_storage import Storage
		from restkit import SimplePool
		from couchdbkit import Server
		
		# set a threadsafe pool to keep 2 connections alives
		self.pool = SimplePool(keepalive=2)
		self.server = Server('http://localhost:5984/',
						pool_instance=self.pool)
		# DEBUG: remove
		try:
			self.server.delete_db('pubsub_test')
		except:
			pass

		self.db = self.server.get_or_create_db('pubsub_test')
		self.s = Storage(self.db)
		
		# upload design docs
		from couchdbkit.loaders import FileSystemDocsLoader
		loader = FileSystemDocsLoader('../../db/couchdb')
		loader.sync(self.db, verbose=True)
		
		# upload initial docs
		self.init()
		
		StorageTests.setUp(self)
		
	
	def tearDown(self):
		pass
		#self.server.delete_db('pubsub_test')
		

	def init(self):
		from idavoll.couchdb_storage import CouchStorage
		
		# nodes
		node = CouchStorage.Node(node='pre-existing', node_type='leaf', persist_items=True)
		node.save()
		node = CouchStorage.Node(node='to-be-deleted')
		node.save()
		node = CouchStorage.Node(node='to-be-reconfigured')
		node.save()
		node = CouchStorage.Node(node='to-be-purged')
		node.save()
		
		# entities
		entity = CouchStorage.Entity(jid=OWNER.userhost())
		entity['_id'] = 'entity:' + OWNER.userhost()
		entity.save()
		entity = CouchStorage.Entity(jid=SUBSCRIBER.userhost())
		entity['_id'] = 'entity:' + SUBSCRIBER.userhost()
		entity.save()
		entity = CouchStorage.Entity(jid=SUBSCRIBER_TO_BE_DELETED.userhost())
		entity['_id'] = 'entity:' + SUBSCRIBER_TO_BE_DELETED.userhost()
		entity.save()
		entity = CouchStorage.Entity(jid=SUBSCRIBER_PENDING.userhost())
		entity['_id'] = 'entity:' + SUBSCRIBER_PENDING.userhost()
		entity.save()
		entity = CouchStorage.Entity(jid=PUBLISHER.userhost())
		entity['_id'] = 'entity:' + PUBLISHER.userhost()
		entity.save()

		# affiliations
		aff = CouchStorage.Affiliation(node='pre-existing', entity=OWNER.userhost(), affiliation='owner')
		aff.save()
		
		# subscriptions
		subs = CouchStorage.Subscription(
			node='pre-existing',
			entity=SUBSCRIBER.userhost(),
			resource=SUBSCRIBER.resource,
			state='subscribed',
			)
		subs.save()

		subs = CouchStorage.Subscription(
			node='pre-existing',
			entity=SUBSCRIBER_TO_BE_DELETED.userhost(),
			resource=SUBSCRIBER_TO_BE_DELETED.resource,
			state='subscribed',
			)
		subs.save()

		subs = CouchStorage.Subscription(
			node='pre-existing',
			entity=SUBSCRIBER_PENDING.userhost(),
			resource=SUBSCRIBER_PENDING.resource,
			state='pending',
			)
		subs.save()
		
		#items
		
		# pre-existing:to-be-deleted & pre-existing:to-be-deleted2
		item = CouchStorage.Item(
			node='pre-existing',
			publisher=PUBLISHER.userhost(),
			item_id='to-be-deleted',
			data=ITEM_TO_BE_DELETED.toXml(),
			)
		item.save()
		item = CouchStorage.Item(
			node='pre-existing',
			publisher=PUBLISHER.userhost(),
			item_id='to-be-deleted2',
			data=ITEM_TO_BE_DELETED.toXml(),
			)
		item.save()
		
		item = CouchStorage.Item(
			node='to-be-purged',
			publisher=PUBLISHER.userhost(),
			item_id='to-be-deleted',
			data=ITEM_TO_BE_DELETED.toXml(),
			)
		item.save()
		
		item = CouchStorage.Item(
			node='pre-existing',
			publisher=PUBLISHER.userhost(),
			item_id='current',
			data=ITEM.toXml(),
			)
		item.save()
		

try:
	import couchdbkit
	couchdbkit
except ImportError:
	PgsqlStorageStorageTestCase.skip = "couchdbkit not available"
