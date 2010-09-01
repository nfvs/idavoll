function(doc)
{
	for (var i in doc.nodes)
		emit(doc.nodes[i].node, doc); 
}