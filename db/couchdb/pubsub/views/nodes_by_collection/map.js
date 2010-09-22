function(doc)
{
	if(doc.doc_type == 'node')
		emit([doc.collection, doc.node], null);
}