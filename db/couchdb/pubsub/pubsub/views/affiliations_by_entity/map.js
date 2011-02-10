function(doc) {
	if (doc.doc_type == 'affiliation') {
		emit(doc.entity,[doc.node, doc.affiliation]);
	}
	// else if (doc.doc_type == 'entity') {
	// 		emit(doc.jid,doc);
	// 	}
}