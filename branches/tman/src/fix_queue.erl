%% Author: christian
%% Created: Feb 16, 2009
%% Description: TODO: Add description to ch_queue
-module(fix_queue).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([new/1,add/2 ,filter/2]).

%%
%% API Functions
%%

filter(Fun,{_,_,Q}) ->
    queue:filter(Fun,Q).
    
new(MaxLength) ->
    {MaxLength,0,queue:new()}.

add(Elem,{Max,Len,Q}) ->
    {_,NewQ} = 
        case Len = Max of
        	true ->  	NewLen=Len,
                		queue:out(queue:in(Elem, Q));
			false -> 	NewLen=Len+1,
                		{foo,queue:in(Elem, Q)}
        end,
    {Max,NewLen,NewQ}
	;
add(_,_) ->
    badarg.
	
%%
%% Local Functions
%%

