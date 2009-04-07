%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable law or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.
%%%-------------------------------------------------------------------
%% Author: christian hennig <hennig@zib.de>
%% Created: Feb 11, 2009
%% Description: TODO: Add description to metric
-module(metric).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([ring_health/0,rpc_get_node_list/0]).

%%
%% API Functions
%%

ring_health() ->
    RealRing = get_ring_details(),
    Ring = lists:sort(fun compare_node_details/2, RealRing),
    RingSize = length(Ring),
    case RingSize>1 of 
        true ->
			ListsErrors = lists:map(fun (Node_Details) -> node_health(Node_Details,Ring) end, Ring),
    		{lists:foldr(fun (X,Acc) -> X+Acc end, 0, ListsErrors)/length(ListsErrors),RingSize};
        false ->
            {1,RingSize}
    end.





node_health(Details,Ring) ->
	Node = node_details:me(Details),
	MyIndex = get_indexed_id(Node, Ring),
    NIndex = length(Ring),
	PredList = node_details:predlist(Details),
    Node = node_details:me(Details),
    SuccList = node_details:succlist(Details),
	
    PredIndices = lists:map(fun(Pred) -> get_indexed_pred_id(Pred, Ring, MyIndex, NIndex) end, PredList),
    SuccIndices = lists:map(fun(Succ) -> get_indexed_succ_id(Succ, Ring, MyIndex, NIndex) end, SuccList),
    
    NP= util:min(NIndex-1,config:read(pred_list_length)),
    NS= util:min(NIndex-1,config:read(pred_list_length)),
    
    Ps = lists:sublist(PredIndices++lists:duplicate(NP, -1*(NIndex-1)),NP),
    Ss = lists:sublist(SuccIndices++lists:duplicate(NS, (NIndex-1)),NS),
    
    CorrectFakP= 1/(NIndex*lists:foldr(fun (A,Acc) -> 1/A+Acc end, 0, lists:seq(1, NP))), 
    CorrectFakS= 1/(NIndex*lists:foldr(fun (A,Acc) -> 1/A+Acc end, 0, lists:seq(1, NS))),
    NormPs = norm(Ps,1), 
    NormSs = norm(Ss,1),
	
    
    P = fun(A, AccIn) -> A + AccIn end,
    Error = (lists:foldr( P , 0, NormPs)*CorrectFakP+lists:foldr( P , 0, NormSs)*CorrectFakS)/2,
    %io:format("~p~n",[{NormPs,NormSs}]),
    Error;

node_health(failed,_Ring) ->
    0.

norm([],_) ->
    [];
norm([H|T],X) ->
    [(save_abs(H,X)-X)*(1/X)|norm(T,X+1)].

save_abs(H,X) ->
    try abs(H)
    catch 
        _ -> X
	end.

is_valid({ok, _}) ->
    true;
is_valid({failed}) ->
    false.


get_indexed_pred_id(Node, Ring, MyIndex, NIndex) ->
    case get_indexed_id(Node, Ring) of
        "null" -> -1*(NIndex-1);
        "none" -> -1*(NIndex-1);
        Index -> ((Index-MyIndex+NIndex) rem NIndex)-NIndex
    end.
get_indexed_succ_id(Node, Ring, MyIndex, NIndex) ->
    case get_indexed_id(Node, Ring) of
        "null" -> (NIndex-1);
        "none" -> (NIndex-1);
        Index -> (Index-MyIndex+NIndex) rem NIndex
    end.
get_indexed_id(Node, Ring) ->
    case node:is_null(Node) of
        true -> "null";
        false -> get_indexed_id(Node, Ring, 0)
    end.
get_indexed_id(Node, [Details|Ring], Index) ->
    case node:id(Node) =:= node:id(node_details:me(Details)) of
        true -> Index;
        false -> get_indexed_id(Node, Ring, Index+1)
    end;

get_indexed_id(_Node, [], _Index) ->
   "none".
    
    

%%
%% Local Functions
%%

get_ring_details() ->
    Nodes = node_list(),
    lists:map(fun (Pid) -> send_get_node_details(Pid) end, Nodes),
    receive_get_node_details([]).
    

%% @doc returns all nodes known to the boot server
%% @spec node_list() -> list(pid())
-spec(node_list/0 :: () -> list(cs_send:mypid())).
node_list() ->
    %Local = rpc_get_node_list(),
    Remote =
        case rpc:multicall([boot@ubuntu1.zib.de,node@ubuntu2.zib.de], metric, rpc_get_node_list, [], 1000) of
            {badrpc, _Reason} ->
                [];
            {ResL, _BadNodes} ->
                lists:flatten(ResL)
        end,
    Remote.
    %cs_send:send(config:bootPid(), {get_list, cs_send:this()}),
    %receive
    %{get_list_response, Nodes} ->
    %    Nodes
    %end.

rpc_get_node_list() ->
    lists:map(fun (Pid) -> cs_send:get(Pid, cs_send:this()) end, process_dictionary:find_all_cs_nodes()).

send_get_node_details(Pid) ->
    cs_send:send(Pid, {get_node_details, cs_send:this(), Pid}).

receive_get_node_details(List) ->
    receive
    {get_node_details_response, _Pid, Details} -> 
        receive_get_node_details([Details|List])
    after
    1000 ->
        List
    end.

compare_node_details( X,  Y) ->
    node:id(node_details:me(X)) < node:id(node_details:me(Y)).
