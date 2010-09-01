function(doc)
{
	if(doc.doc_type == 'NodeDoc')
		emit(doc.node, doc); 
}