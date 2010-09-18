function(doc)
{
	if(doc.doc_type == 'node' && doc.node_type == 'collection')
		emit([doc.collection, doc.node]); 
}