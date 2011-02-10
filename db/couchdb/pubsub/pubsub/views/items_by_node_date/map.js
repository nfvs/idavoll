function(doc)
{
	if(doc.doc_type == 'item')
		emit([doc.node, doc.date], doc);
}