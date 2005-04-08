import copy
from zope.interface import implements
from twisted.internet import defer
from twisted.words.protocols.jabber import jid
import storage

default_config = {"pubsub#persist_items": False,
                  "pubsub#deliver_payloads": False}

class Storage:

    implements(storage.IStorage)

    def __init__(self):
        self._nodes = {}

    def get_node(self, node_id):
        try:
            node = self._nodes[node_id]
        except KeyError:
            return defer.fail(storage.NodeNotFound())

        return defer.succeed(node)

    def get_node_ids(self):
        return defer.succeed(self._nodes.keys())

    def create_node(self, node_id, owner, config = None, type='leaf'):
        if node_id in self._nodes:
            return defer.fail(storage.NodeExists())

        if not config:
            config = copy.copy(default_config)

        if type != 'leaf':
            raise NotImplementedError

        node = LeafNode(node_id, owner, config)
        self._nodes[node_id] = node

        return defer.succeed(None)

    def delete_node(self, node_id):
        try:
            del self._nodes[node_id]
        except KeyError:
            return defer.fail(storage.NodeNotFound())

        return defer.succeed(None)

    def get_affiliations(self, entity):
        entity_full = entity.full()
        return defer.succeed([(node.id, node._affiliations[entity_full])
                              for name, node in self._nodes.iteritems()
                              if entity_full in node._affiliations])

    def get_subscriptions(self, entity):
        subscriptions = []
        for node in self._nodes.itervalues():
            for subscriber, subscription in node._subscriptions.iteritems():
                subscriber = jid.JID(subscriber)
                if subscriber.userhostJID() == entity:
                    subscriptions.append((node.id, subscriber,
                                          subscription.state))

        return defer.succeed(subscriptions)
        
class Node:

    implements(storage.INode)

    def __init__(self, node_id, owner, config):
        self.id = node_id
        self._affiliations = {owner.full(): 'owner'}
        self._subscriptions = {}
        self._config = config

    def get_type(self):
        return self.type

    def get_configuration(self):
        return self._config
    
    def get_meta_data(self):
        config = copy.copy(self._config)
        config["pubsub#node_type"] = self.type
        return config

    def set_configuration(self, options):
        for option in options:
            if option in self._config:
                self._config[option] = options[option]

        return defer.succeed(None)
                
    def get_affiliation(self, entity):
        return defer.succeed(self._affiliations.get(entity.full()))

    def add_subscription(self, subscriber, state):
        try:
            subscription = self._subscriptions[subscriber.full()]
        except:
            subscription = Subscription(state)
            self._subscriptions[subscriber.full()] = subscription

        return defer.succeed({'node': self.id,
                              'jid': subscriber,
                              'subscription': subscription.state})

    def remove_subscription(self, subscriber):
        del self._subscriptions[subscriber.full()]

        return defer.succeed(None)

    def get_subscribers(self):
        subscribers = [jid.JID(subscriber) for subscriber, subscription
                       in self._subscriptions.iteritems()
                       if subscription.state == 'subscribed']

        return defer.succeed(subscribers)

    def is_subscribed(self, subscriber):
        try:
            subscription = self._subscriptions[subscriber.full()]
        except KeyError:
            return defer.succeed(False)

        return defer.succeed(subscription.state == 'subscribed')

class LeafNode(Node):

    implements(storage.ILeafNode)
    type = 'leaf'

    def __init__(self, node_id, owner, config):
        Node.__init__(self, node_id, owner, config)
        self._items = {}
        self._itemlist = []

    def store_items(self, items, publisher):
        for data in items:
            id = data["id"]
            item = (data.toXml(), publisher)
            if id in self._items:
                self._itemlist.remove(self._items[id])
            self._items[id] = item
            self._itemlist.append(item)

        return defer.succeed(None)

    def remove_items(self, item_ids):
        deleted = []

        for item_id in item_ids:
            try:
                item = self._items[item_id]
                self._itemlist.remove(item)
                del self._items[item_id]
                deleted.append(item_id)
            except KeyError:
                pass
        
        return defer.succeed(deleted)

    def get_items(self, max_items=None):
        if max_items:
            list = self._itemlist[-max_items:]
        else:
            list = self._itemlist
        return defer.succeed([item[0] for item in list])
    
    def get_items_by_id(self, item_ids):
        items = []
        for item_id in item_ids:
            try:
                item = self._items[item_id]
            except KeyError:
                pass
            else:
                items.append(item[0])
        return defer.succeed(items)

    def purge(self):
        self._items = {}
        self._itemlist = []

        return defer.succeed(None)

class Subscription:

    implements(storage.ISubscription)

    def __init__(self, state):
        self.state = state
