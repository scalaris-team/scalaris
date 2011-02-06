
-record(req_head,
    {magic=request,
    opcode,
	opcodeB,
    keylength,
	extralength,
	datatype,
	datatypeB,
	reserved,
	totalbodylength,
	opaque,
	cAS}).
-record(req_body,
        {extra,key,value}).