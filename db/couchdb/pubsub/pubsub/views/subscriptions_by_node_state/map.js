function(doc)
{
	if(doc.doc_type == 'subscription')
		emit([doc.node, doc.state], doc);
}