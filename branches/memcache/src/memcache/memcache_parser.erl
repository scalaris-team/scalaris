%% Author: Christian Hennig (caraboides@googlemail.com)
%% Created: 23.10.2010
%% Description: Parser for the binary memcache protocol
-module(memcache_parser).


%%
%% Include files
%%
-include("memcache.hrl").

%%
%% Exported Functions
%%
-export([parse_req/1,build_response/3]).

%%
%% API Functions
%%


parse_req(Data) ->
    {Header,Rest} = split_binary(Data,24),
    Head = parse_head(Header), 
    Body = parse_body(Rest, Head), 
    {Head,Body}.

build_response(Header,_Body,{fail,unknown_command}) ->
	<<16#81,
	  (Header#req_head.opcodeB),
	  (Header#req_head.keylength),
	  0:8,(Header#req_head.datatypeB),0,16#81,
	  0,0,0,0,
	  (Header#req_head.opaque):32,
	  (Header#req_head.cAS):64>>;

build_response(Header,_Body,{fail,_Reson}) ->
	<<16#81,(Header#req_head.opcodeB),(Header#req_head.keylength),
	  0:8,(Header#req_head.datatypeB),0,16#4,
	  0,0,0,0,
	  (Header#req_head.opaque):32,
	  (Header#req_head.cAS):64>>; 
build_response(Header,_Body,ok) ->
	io:format("Header: ~p~n",[Header]),
	<<16#81,(Header#req_head.opcodeB),0,0,
	  0,(Header#req_head.datatypeB),0,16#0,
	  0,0,0,0,
	  (Header#req_head.opaque):32,
	  (Header#req_head.cAS):64>>;
build_response(Header,_Body,Value) ->
	io:format("Build Response for: ~p ~n~p ~n~p~n",[Header,_Body,Value]),
	Length = pad_to(4,binary:encode_unsigned(byte_size(Value))),
	<<16#81,(Header#req_head.opcodeB),(Header#req_head.keylength),
	  0:8,(Header#req_head.datatypeB),0,16#0,
	  Length,
	 (Header#req_head.opaque):32,
	  (Header#req_head.cAS):64,
	  Value:(binary:encode_unsigned(byte_size(Value)))/binary>>.
	

%%
%% Local Functions
%%

parse_head(<<MagicB:8,OpcodeB:8,KeylengthB:16,ExtralengtB:8,DatatypeB:8,ReservedB:16,TotalbodylengthB:32,OpaqueB:32,CASB:64>>) ->
case MagicB of
        16#80 ->
                #req_head{  
						opcode=parse_opcode(OpcodeB),
						opcodeB=OpcodeB,
                        keylength=KeylengthB,
						extralength=ExtralengtB,
                        datatype=parse_datatype(DatatypeB),
						datatypeB=DatatypeB,
                        reserved=ReservedB,
                        totalbodylength=TotalbodylengthB,
                        opaque= OpaqueB,
                        cAS= CASB};
        16#81 ->
                throw("Request expeded but response was get");
		A -> throw("Request expeded but "+A+" was get")
end.

parse_body(Rest, Head) ->
    %% Todo Check that sizes are valid
    SizeExtra = Head#req_head.extralength*8, 
    SizeKey = Head#req_head.keylength*8, 
    SizeValue = (Head#req_head.totalbodylength-(Head#req_head.extralength+Head#req_head.keylength))*8 ,
	io:format("RestSize in Byte ~p~n",[byte_size(Rest)]),
    <<ExtraB:SizeExtra,KeyB:SizeKey,ValueB:SizeValue>> = Rest, 
    #req_body{extra = ExtraB, key = KeyB, value = ValueB}.

parse_datatype(16#00) ->raw_type;
parse_datatype(_) ->throw("unknown datyType").
             

parse_opcode( 16#00) -> get;
parse_opcode( 16#01) -> set;
parse_opcode( 16#02) -> add;
parse_opcode( 16#03) -> replace;
parse_opcode( 16#04) -> delete;
parse_opcode( 16#05) -> increment;
parse_opcode( 16#06) -> decrement;
parse_opcode( 16#07) -> quit;
parse_opcode( 16#08) -> flush;
parse_opcode( 16#09) -> getq;
parse_opcode( 16#0A) -> noop;
parse_opcode( 16#0B) -> version;
parse_opcode( 16#0C) -> getk;
parse_opcode( 16#0D) -> getkq;
parse_opcode( 16#0E) -> append;
parse_opcode( 16#0F) -> prepend;
parse_opcode( 16#10) -> stat;
parse_opcode( 16#11) -> setq;
parse_opcode( 16#12) -> addq;
parse_opcode( 16#13) -> repleaceq;
parse_opcode( 16#14) -> deleteq;
parse_opcode( 16#15) -> incrementq;
parse_opcode( 16#16) -> decrementq;
parse_opcode( 16#17) -> quitq;
parse_opcode( 16#18) -> flushq;
parse_opcode( 16#19) -> appendq;
parse_opcode( 16#1A) -> prependq;
parse_opcode(_) -> throw("Unknown Opcode").



pad_to(Width, Binary) ->
     case (Width - size(Binary) rem Width) rem Width
       of 0 -> Binary
        ; N -> <<Binary/binary, 0:(N*8)>>
     end.