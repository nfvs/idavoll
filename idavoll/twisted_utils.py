import types
from twisted.words.xish import domish

def _splitPrefix(name):
	""" Internal method for splitting a prefixed Element name into its
		respective parts """
	ntok = name.split(":", 1)
	if len(ntok) == 2:
		return ntok
	else:
		return (None, ntok[0])

# Global map of prefixes that always get injected
# into the serializers prefix map (note, that doesn't
# mean they're always _USED_)
G_PREFIXES = { "http://www.w3.org/XML/1998/namespace":"xml" }

class DictSerializer:
	
	@staticmethod
	def sanitize(string):
		return ' '.join(string.split())
	
	""" Internal class which serializes an Element tree into a buffer """
	def __init__(self, prefixes=None, prefixesInScope=None):
		self.writelist = []
		self.prefixes = {}
		if prefixes:
			self.prefixes.update(prefixes)
		self.prefixes.update(G_PREFIXES)
		self.prefixStack = [G_PREFIXES.values()] + (prefixesInScope or [])
		self.prefixCounter = 0
	
	def getPrefix(self, uri):
		if not self.prefixes.has_key(uri):
			self.prefixes[uri] = "xn%d" % (self.prefixCounter)
			self.prefixCounter = self.prefixCounter + 1
		return self.prefixes[uri]

	def prefixInScope(self, prefix):
		stack = self.prefixStack
		for i in range(-1, (len(self.prefixStack)+1) * -1, -1):
			if prefix in stack[i]:
				return True
		return False
	
	def dict_from_elem(self, elem):
		# TODO: no root
		if isinstance(elem, list):
			res = [self._serialize_to_dict(e) for e in elem]
		else:
			res = dict(self._serialize_to_dict(elem))
		return res

	"""
	Serialize twisted domish.Element to python dict
	"""
	def _serialize_to_dict(self, elem, closeElement=1, defaultUri=''):
		ret = {}
	
		# Shortcut, check to see if elem is actually a string (aka Cdata)
		if isinstance(elem, types.StringTypes):
			#ret[key] = elem
			#return
			return self.sanitize(elem)
	
		# Further optimizations
		parent = elem.parent
		name = elem.name
		uri = elem.uri
		defaultUri, currentDefaultUri = elem.defaultUri, defaultUri
		
		ret[name] = {}
		ret[name]['value'] = []
		ret[name]['attribs'] = {}
		
		for p, u in elem.localPrefixes.iteritems():
			self.prefixes[u] = p
		self.prefixStack.append(elem.localPrefixes.keys())

		# Inherit the default namespace
		if defaultUri is None:
			defaultUri = currentDefaultUri

		if uri is None:
			uri = defaultUri

		prefix = None
		if uri != defaultUri or uri in self.prefixes:
			prefix = self.getPrefix(uri)
			inScope = self.prefixInScope(prefix)
			
		# Create the starttag

		if not prefix:
			pass
			#write("<%s" % (name))
		else:
			#write("<%s:%s" % (prefix, name))

			if not inScope:
				#write(" xmlns:%s='%s'" % (prefix, uri))
				ret[name]['attribs']['xmlns:' + prefix] = uri
				self.prefixStack[-1].append(prefix)
				inScope = True

		if defaultUri != currentDefaultUri and \
		   (uri != defaultUri or not prefix or not inScope):
			#write(" xmlns='%s'" % (defaultUri))
			ret[name]['attribs']['xmlns'] = defaultUri

		for p, u in elem.localPrefixes.iteritems():
			#write(" xmlns:%s='%s'" % (p, u))
			ret[name]['attribs']['xmlns:' + p] = u


		# Serialize attributes
		#if len(elem.attributes.items()) > 0:
		for k,v in elem.attributes.items():
			# If the attribute name is a tuple, it's a qualified attribute
			if isinstance(k, types.TupleType):
				attr_uri, attr_name = k
				attr_prefix = self.getPrefix(attr_uri)
				if not self.prefixInScope(attr_prefix):
					ret[name]['attribs']["xmlns:" + attr_prefix] = attr_uri
					self.prefixStack[-1].append(attr_prefix)

				ret[name]['attribs']["xmlns:" + attr_prefix]=attr_uri
			else:
				ret[name]['attribs'][k] = v

		# Serialize children
		# if len(elem.children) > 0:
		# 	ret[name] = []
		# 	for c in elem.children:
		# 		ret[name].append(_serialize_to_dict(c, defaultUri=defaultUri))
	
		# save a single element as a key:value, and multiple as [key:value]
		# leaf xml node (single element)
		if len(elem.children) == 1 and isinstance(elem.children[0], types.StringTypes) and self.sanitize(elem.children[0]):
			ret[name]['value'] = elem.children[0]
		# tree xml node (element list)
		elif len(elem.children) > 0:
			for c in elem.children:
				# check if the returned element is an empty string, skip if it is
				ret_child = self._serialize_to_dict(c, defaultUri=defaultUri)
				if ret_child:
					ret[name]['value'].append(ret_child)

		if not ret[name]['attribs']:
			del ret[name]['attribs']
		return ret
	
	"""
	Serialize couchdbkit LazyList to twisted domish.Elements (xml)
	Note: must have a single root element
	"""
	def serialize_to_xml(self, itemlist):

		# root element
		root_elem = itemlist.keys()[0]
		root = domish.Element((None, root_elem))

		# root attributes
		if 'attribs' in itemlist[root_elem]:
			self._add_xml_attributes(root, itemlist[root_elem]['attribs'])
		
		# child elements
		self._serialize_to_xml(root, itemlist[root_elem]['value'])
	
		#return root.toXml()
		return root
		
	def _serialize_to_xml(self, root, itemlist):
		
		#print 'ORIG: ' + str(itemlist)
		
		# if value is a string, add content
		if isinstance(itemlist, types.StringTypes):
			root.addContent(itemlist)
			return
		
		# iterate child elements
		for item in itemlist:
			root_elem = item.keys()[0]
				
			child_elem = root.addElement(root_elem)
			
			# add attributes
			if 'attribs' in item[root_elem]:
				self._add_xml_attributes(child_elem, item[root_elem]['attribs'])\
			
			# add child nodes
			self._serialize_to_xml(child_elem, item[root_elem]['value'])
			
	
	def _add_xml_attributes(self, element, attr_list):
		attr_list = dict(attr_list)
		for k,v in attr_list.iteritems():
			element[k] = v
		