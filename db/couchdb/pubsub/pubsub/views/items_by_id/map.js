function(doc)
{
	if(doc.doc_type == 'item')
		emit(doc.item_id, doc);
}