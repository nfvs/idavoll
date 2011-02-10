function(doc)
{
	if(doc.doc_type == 'node')
		emit(doc.node, doc); 
}