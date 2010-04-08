%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : simu_slave.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : 
%%%
%%% Created :  12 Jan 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

-module(simu_slave).

-author('hennig@zib.de').



-behavior(gen_component).
-export([init/1,on/2,start/0]).





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


start() ->
   gen_component:start_link(?MODULE, [], [{register_native, simu_slave}]).

init(_ARG) ->
    cs_send:send_local_after(2000, self(), {addnodes,1000}),
    cs_send:send_local_after(10000, self(), {check_ring}),
    cs_send:send_local_after(1000*60*60, self(), {simu_stop}),
    {initState}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


on({addnodes,1},_) ->
    admin:add_nodes(1),
    {state_1};
on({addnodes,N},_) ->
    admin:add_nodes(1),
    cs_send:send_local_after(1, self(), {addnodes,N-1}),
    {state_1}; 
on({simu_stop},_) ->
    scheduler:stop();

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Check Ring
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    
on({check_ring},_) ->
    %io:format("Check"),
    %scheduler ! {halt_simulation},
    %Implement a Noneblocking CheckRing 
    erlang:put(instance_id, process_dictionary:find_group(dht_node)),
    cs_send:send_local(bootPid(), {get_list, cs_send:this()}),
    %io:format("A~n"),
    {check_ring_p1};
on({get_list_response, N}, _) ->
    [cs_send:send_local(Pid, {get_node_details, cs_send:this(), [node, succ]}) ||  Pid <-N],
    %io:format("C~n"),
    {length(N),[]};
on({get_node_details_response, NodeDetails}, {1, Responses}) ->
    %io:format("E~n"),
    Details = {node:id(node_details:get(NodeDetails, node)), node:id(node_details:get(NodeDetails, succ))},
    Sort = lists:sort(fun compare_node_details/2,[Details|Responses]),   
   %io:format("F~n"),
   Res = case lists:foldl(fun check_ring_foldl/2, first, Sort) of

    {error, Reason} ->
        {error, Reason};
    _X ->
        ok
    end,
    %io:format("G ~p~n",[Res]),
    case Res of
        ok -> scheduler:stop();
        _Y ->   
                %io:format("~p~n",[_Y]),
                cs_send:send_local_after(1000, self(), {check_ring})
    end,
     %io:format("K~n"),
    %scheduler ! {continue},
    %io:format("Ring~n"),
    {state_2};

on({get_node_details_response, NodeDetails}, {Counter, Responses}) ->
    %io:format("D~n"),
    Details = {node:id(node_details:get(NodeDetails, node)), node:id(node_details:get(NodeDetails, succ))},
    {Counter - 1, [Details | Responses]};
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Catch unknown Events
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


on(_, _State) ->
    unknown_event.
% @private

compare_node_details(X, Y) ->
    id(X) < id(Y).

check_ring_foldl(Node, first) ->
    succ(Node);
check_ring_foldl({failed}, Last) ->
    Last;
check_ring_foldl(_, {error, Message}) ->
    {error, Message};
check_ring_foldl(Node, PredsSucc) ->
    MyId = id(Node),
    if
    MyId == PredsSucc ->
        succ(Node);
    true ->
        {error, lists:flatten(io_lib:format("~p didn't match ~p", [MyId, PredsSucc]))}
    end.


id({X,_}) -> X.
succ({_,X}) -> X.

get_pid() ->
    process_dictionary:get_group_member(simu_slave).

%% @doc pid of the boot daemon
%% @spec bootPid() -> pid()
bootPid() ->
    config:read(boot_host).
