%% Author: christian
%% Created: Feb 16, 2009
%% Description: TODO: Add description to fix_queue
-module(fix_queue).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([new/1,add/2 ,map/2]).

%%
%% API Functions
%%

map(Fun,{_,_,Q}) ->
   lists:map(Fun,queue:to_list(Q)).
    
new(MaxLength) ->
    {MaxLength,0,queue:new()}.
add(Elem,{Max,Len,Q}) ->
    {_,NewQ} = 
        case Len == Max of
        	true ->  	NewLen=Len,
                		queue:out(queue:in(Elem, Q));
			false -> 	NewLen=Len+1,
           		{foo,queue:in(Elem, Q)}
        end,
    {Max,NewLen,NewQ}.
