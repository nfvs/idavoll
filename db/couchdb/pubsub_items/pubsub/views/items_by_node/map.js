function(doc)
{
	if(doc.doc_type == 'item')
		emit([doc.node, doc.item_id], doc);
}