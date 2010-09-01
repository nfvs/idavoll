function(doc)
{
	if(doc.doc_type == 'EntityDoc')
		emit(doc.jid, doc); 
}