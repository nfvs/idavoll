from idavoll import pgsql_storage, couchdb_storage



class Storage(pgsql_storage.Storage, couchdb_storage.Storage):
	implements(iidavoll.IStorage)
	
	def __init__(self, dbpool):
		super(pgsql_storage.Storage, self).__init__(dbpool)
		super(couchdb_storage.Storage, self).__init__(dbpool)

	def getNode(self, nodeIdentifier):
		super(pgsql_storage.Storage, self).getNode(nodeIdentifier)

class Node(pgsql_storage.Node, couchdb_storage.Node):
	implements(iidavoll.INode)


class LeafNode(pgsql_storage.LeafNode, couchdb_storage.LeafNode):
	implements(iidavoll.ILeafNode)

class CollectionNode(pgsql_storage.CollectionNode,
					 couchdb_storage.CollectionNode):
	pass
