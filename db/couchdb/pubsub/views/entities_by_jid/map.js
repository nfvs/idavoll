function(doc)
{
	if(doc.doc_type == 'entity')
		emit(doc.jid, doc); 
}