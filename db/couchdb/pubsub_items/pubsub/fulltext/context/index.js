function(doc)
{
	var ret=new Document();
	ret.add(doc.node, {'field': 'node'});
	ret.add(doc.publisher, {'field': 'publisher'});
	ret.add(new Date(doc.date.substring(0, 19)), {'type': 'date', 'field': 'date'});
	ret.add(doc.data, {'field': 'context'});
	return ret;
}
