function(doc)
{
	if(doc.doc_type == 'affiliation')
		emit([doc.entity, doc.node, doc.affiliation], null);
}