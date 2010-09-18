function(doc)
{
	if(doc.doc_type == 'node' && doc.collection)
		emit([doc.collection, doc.node], doc); 
}