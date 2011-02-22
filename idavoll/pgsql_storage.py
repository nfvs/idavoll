# Copyright (c) 2003-2008 Ralph Meijer
# See LICENSE for details.

import copy

from string import Template

from zope.interface import implements

from twisted.enterprise import adbapi
from twisted.words.protocols.jabber import jid

from wokkel.generic import parseXml, stripNamespace
from wokkel.pubsub import Subscription

from idavoll import error, iidavoll


"""
Get All Ancestors
http://explainextended.com/2009/09/24/adjacency-list-vs-nested-sets-postgresql/
"""
sql_get_all_parent_nodes = Template(
"""WITH RECURSIVE
 q AS
 (
 SELECT  n.node, n.collection, 1 AS level
 FROM    nodes n
 WHERE   node = '$node'
 UNION ALL
 SELECT  np.node, np.collection, level + 1
 FROM    q
 JOIN    nodes np
 ON      np.node_id = q.collection
 )
SELECT  node
FROM    q
ORDER BY
        level DESC
""")


class Storage:

    implements(iidavoll.IStorage)

    defaultConfig = {
            'leaf': {
                "pubsub#node_type": 'leaf',
                "pubsub#collection": None,
                "pubsub#persist_items": True,
                "pubsub#deliver_payloads": True,
                "pubsub#send_last_published_item": 'on_sub',
            },
            'collection': {
                "pubsub#node_type": 'collection',
                "pubsub#collection": None,
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
        cursor.execute("""SELECT n.node_type,
                                 n.persist_items,
                                 n.deliver_payloads,
                                 n.send_last_published_item,
                                 nc.node
                          FROM nodes n
                          INNER JOIN nodes nc ON (n.collection=nc.node_id)
                          WHERE n.node=%s""",
                       (nodeIdentifier,))
        row = cursor.fetchone()

        if not row:
            raise error.NodeNotFound()

        if row.node_type == 'leaf':
            configuration = {
                    'pubsub#node_type': 'leaf',
                    'pubsub#collection': row.node,
                    'pubsub#persist_items': row.persist_items,
                    'pubsub#deliver_payloads': row.deliver_payloads,
                    'pubsub#send_last_published_item':
                        row.send_last_published_item}
            node = LeafNode(nodeIdentifier, configuration)
            node.dbpool = self.dbpool
            return node
        elif row.node_type == 'collection':
            configuration = {
                    'pubsub#node_type': 'collection',
                    'pubsub#collection': row.node,
                    'pubsub#deliver_payloads': row.deliver_payloads,
                    'pubsub#send_last_published_item':
                        row.send_last_published_item}
            node = CollectionNode(nodeIdentifier, configuration)
            node.dbpool = self.dbpool
            return node


    def getNodeIds(self, collection=''):
        d = self.dbpool.runQuery("""SELECT nodes.node FROM nodes
                INNER JOIN nodes AS n ON (nodes.collection = n.node_id)
                WHERE n.node=%s
                """, (collection,))
        d.addCallback(lambda results: [r[0] for r in results])
        return d


    def createNode(self, nodeIdentifier, owner, config):
        return self.dbpool.runInteraction(self._createNode, nodeIdentifier,
                                           owner, config)


    def _createNode(self, cursor, nodeIdentifier, owner, config):
        #if config['pubsub#node_type'] != 'leaf':
        #    raise error.NoCollections()

        owner = owner.userhost()

        if not 'pubsub#node_type' in config:
            config['pubsub#node_type'] = 'leaf'

        if not 'pubsub#persist_items' in config:
            config['pubsub#persist_items'] = None

        try:
            # check if the node has a collection, and the collection node
            # exists; if it has no collection, set collection=0 (root node)
            if 'pubsub#collection' in config and config['pubsub#collection']:
                cursor.execute("""SELECT node_id FROM nodes WHERE node=%s""",
                               (config['pubsub#collection'],))
                row = cursor.fetchone()
                if not row:
                    raise error.NodeNotFound()

                collection = row[0]

            else:
                collection = 0

            cursor.execute("""INSERT INTO nodes
                             (node, node_type, collection, persist_items,
                               deliver_payloads, send_last_published_item)
                              VALUES
                              (%s, %s, %s, %s, %s, %s)""",
                           (nodeIdentifier,
                            config['pubsub#node_type'],
                            collection,
                            config['pubsub#persist_items'],
                            config['pubsub#deliver_payloads'],
                            config['pubsub#send_last_published_item'])
                           )

        except cursor._pool.dbapi.OperationalError:
            raise error.NodeExists()

        cursor.execute("""SELECT 1 from entities where jid=%s""",
                       (owner))

        if not cursor.fetchone():
            cursor.execute("""INSERT INTO entities (jid) VALUES (%s)""",
                           (owner))

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
                subscriber = jid.internJID('%s/%s' % (row.jid,
                                                      row.resource))
                subscription = Subscription(row.node, subscriber, row.state)
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
                       (self.nodeIdentifier))
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

        if config['pubsub#node_type'] == 'leaf':
            cursor.execute("""UPDATE nodes SET
                                    persist_items=%s,
                                    deliver_payloads=%s,
                                    send_last_published_item=%s
                                    WHERE node=%s""",
                           (config["pubsub#persist_items"],
                            config["pubsub#deliver_payloads"],
                            config["pubsub#send_last_published_item"],
                            self.nodeIdentifier))

        elif config['pubsub#node_type'] == 'collection':
            cursor.execute("""UPDATE nodes SET
                                    deliver_payloads=%s,
                                    send_last_published_item=%s
                                    WHERE node=%s""",
                           (config["pubsub#deliver_payloads"],
                            config["pubsub#send_last_published_item"],
                            self.nodeIdentifier))

        # check if there's a collection, and if it exists
        if 'pubsub#collection' in config and config['pubsub#collection']:
            try:
                cursor.execute("""UPDATE nodes SET collection=(
                                    SELECT node_id FROM nodes WHERE node=%s)
                                    WHERE node=%s""",
                               (collection, self.nodeIdentifier))
            except cursor._pool.dbapi.OperationalError:
                raise error.NodeNotFound()




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
        resource = subscriber.resource

        sqlStr = """SELECT state FROM subscriptions
                    NATURAL JOIN nodes
                    NATURAL JOIN entities
                    WHERE node='%s' AND jid='%s'""" % (
                        self.nodeIdentifier,
                        userhost)

        if resource:
            sqlStr += """ AND resource=%s""" % resource

        cursor.execute(sqlStr)
        row = cursor.fetchone()

        if not row:
            return None
        else:
            return Subscription(self.nodeIdentifier, subscriber, row.state)


    def getSubscriptions(self, state=None):
        return self.dbpool.runInteraction(self._getSubscriptions, state)

# FIXME
    def _getSubscriptions(self, cursor, state):
        self._checkNodeExists(cursor)

        config = self.getConfiguration()
        collection = config['pubsub#collection']

        # belongs to a collection node,
        # get subscriptions for current node and all parent nodes
        if collection and collection > 0:
            # fetch all nodes, current + ancestors
            # sql is in the beggining of the file
            query = sql_get_all_parent_nodes.substitute(
                    node=self.nodeIdentifier)
            cursor.execute(query)
            rows = cursor.fetchall()
            nodes = [n[0] for n in rows if n[0] != '']

            # fetch all subscriptions for the nodes
            nodesStr = ''.join(["'" + n + "'," for n in nodes])
            nodesStr = nodesStr[:-1]
            # uses MIN / GROUP_BY to return only unique JIDs
            query = """SELECT e.jid, MIN(s.resource), MIN(s.state),
                              MIN(s.subscription_type),
                              MIN(s.subscription_depth)
                       FROM subscriptions s
                       NATURAL JOIN nodes n
                       NATURAL JOIN entities e
                       WHERE n.node IN (%s)
                       GROUP BY e.jid""" % nodesStr
            cursor.execute(query)
            rows = cursor.fetchall()

        # no collection, get only subscriptions for the node
        else:
            query = """SELECT jid, resource, state,
                              subscription_type, subscription_depth
                       FROM subscriptions
                       NATURAL JOIN nodes
                       NATURAL JOIN entities
                       WHERE node=%s"""

            values = [self.nodeIdentifier]

            if state:
                query += " AND state=%s"
                values.append(state)

            cursor.execute(query, values)
            rows = cursor.fetchall()

        # fill subscriptions
        subscriptions = []
        for row in rows:
            subscriber = jid.JID('%s/%s' % (row[0], row[1]))

            options = {}
            if row[3]:
                options['pubsub#subscription_type'] = row[3]
            if row[4]:
                options['pubsub#subscription_depth'] = row[4]

            subscriptions.append(Subscription(self.nodeIdentifier, subscriber,
                                              row[2], options))
        return subscriptions


    def addSubscription(self, subscriber, state, config):
        return self.dbpool.runInteraction(self._addSubscription, subscriber,
                                          state, config)


    def _addSubscription(self, cursor, subscriber, state, config):
        self._checkNodeExists(cursor)

        userhost = subscriber.userhost()
        resource = subscriber.resource

        subscription_type = config.get('pubsub#subscription_type') or ''
        subscription_depth = config.get('pubsub#subscription_depth') or ''

        try:
            cursor.execute("""INSERT INTO entities (jid) VALUES (%s)""",
                           (userhost))
        except cursor._pool.dbapi.OperationalError:
            pass

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
        except cursor._pool.dbapi.OperationalError:
            raise error.SubscriptionExists()


    def removeSubscription(self, subscriber):
        return self.dbpool.runInteraction(self._removeSubscription,
                                           subscriber)


    def _removeSubscription(self, cursor, subscriber):
        self._checkNodeExists(cursor)

        userhost = subscriber.userhost()
        resource = subscriber.resource

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


    def setSubscriptionOptions(self, subscriber, options):
        return self.dbpool.runInteraction(self._setSubscriptionOptions,
                                          subscriber, options)


    def _setSubscriptionOptions(self, cursor, subscriber, options):
        self._checkNodeExists(cursor)

        userhost = subscriber.userhost()
        resource = subscriber.resource

        try:
            sqlStr = """UPDATE subscriptions SET """

            if ('pubsub#subscription_type' in options and
                options['pubsub#subscription_type']):
                sqlStr += (" subscription_type = '%s'," %
                          options['pubsub#subscription_type'])
            if ('pubsub#subscription_depth' in options and
                options['pubsub#subscription_depth']):
                sqlStr += (" subscription_depth = '%s'," %
                          options['pubsub#subscription_depth'])

            # remove trailing ','
            if sqlStr[-1:] == ",":
                sqlStr = sqlStr[:-1]

            # WHERE
            sqlStr += \
                " WHERE node_id=" \
                    "(SELECT node_id FROM nodes WHERE node='%s')" \
                " AND entity_id=" \
                    "(SELECT entity_id FROM entities WHERE jid='%s');" \
                    % (self.nodeIdentifier, userhost)

            cursor.execute(sqlStr)

        except cursor._pool.dbapi.OperationalError:
            raise error.Error()


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
                       self.nodeIdentifier)
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
            cursor.execute(query, (self.nodeIdentifier))

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
