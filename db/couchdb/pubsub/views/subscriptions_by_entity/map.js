function(doc)
{
	if(doc.doc_type == 'subscription')
		emit([doc.entity, doc.node, doc.state, doc.resource], null);
}