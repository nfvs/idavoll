function(doc)
{
	if(doc.doc_type == 'affiliation')
		emit([doc.node, doc.entity], doc.affiliation);
}