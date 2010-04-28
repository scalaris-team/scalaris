%  Copyright 2008, 2009 Konrad-Zuse-Zentrum fr Informationstechnik Berlin
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
%%% File    : admin.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Supervisor for boot nodes
%%%
%%% Created : 03 Mar 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(admin).
-author('schuett@zib.de').
-vsn('$Id$ ').

-export([add_nodes/1, del_nodes/1, check_ring/0, nodes/0,
         start_link/0, start/0, get_dump/0, get_dump_bw/0,
         diff_dump/3, print_ages/0, check_routing_tables/1,
         dd_check_ring/1,dd_check_ring/0, number_of_nodes/0 ]).

%%====================================================================
%% API functions
%%====================================================================

%% userdevguide-begin admin:add_nodes
%%--------------------------------------------------------------------
%% Function: add_nodes(int()) -> ok
%% Description: add new Scalaris nodes
%%--------------------------------------------------------------------
% @doc add new Scalaris nodes on the local node
-spec(add_nodes/1 :: (non_neg_integer()) -> ok).
add_nodes(0) ->
    ok;
add_nodes(Count) ->
    [ begin
          Desc = util:sup_supervisor_desc(randoms:getRandomId(),
                                   sup_dht_node, start_link),
          supervisor:start_child(main_sup, Desc)
      end || _ <- lists:seq(1, Count) ],
    ok.
%% userdevguide-end admin:add_nodes

%% deletes nodes started with add_nodes()
%% detects them by their random names (which is a list)
%% other processes in main_sup must not be started with
%% a list as their name!
-spec(del_nodes/1 :: (integer()) -> ok).
del_nodes(Count) ->
    [ del_single_node(supervisor:which_children(main_sup))
      || _ <- lists:seq(1, Count) ],
    ok.

del_single_node([]) ->
    ok;
del_single_node([H|T]) ->
    {Key, _, _, _} = H,
    case is_list(Key) of
        true ->
            supervisor:terminate_child(main_sup,Key),
            supervisor:delete_child(main_sup,Key);
        false -> del_single_node(T)
    end.

%%--------------------------------------------------------------------
%% Function: check_ring() -> term()
%% Description: contact boot server and check ring
%%--------------------------------------------------------------------
% @doc contact boot server and check ring
-spec(check_ring/0 :: () -> {error, string()} | ok).
check_ring() ->
    erlang:put(instance_id, process_dictionary:find_group(dht_node)),
    Nodes = statistics:get_ring_details(),
    case lists:foldl(fun check_ring_foldl/2, first, Nodes) of
        {error, Reason} ->
            {error, Reason};
        _X ->
            ok
    end.

check_ring_foldl({ok, Node}, first) ->
    get_id(hd(node_details:get(Node, succlist)));
check_ring_foldl({failed}, Last) ->
    Last;
check_ring_foldl(_, {error, Message}) ->
    {error, Message};
check_ring_foldl({ok, Node}, PredsSucc) ->
    MyId = get_id(node_details:get(Node, node)),
    if
        MyId == PredsSucc ->
            get_id(hd(node_details:get(Node, succlist)));
        true ->
            {error, lists:flatten(io_lib:format("~p didn't match ~p", [MyId, PredsSucc]))}
    end.

get_id(Node) ->
    case node:is_valid(Node) of
        true  -> node:id(Node);
        false -> "null"
    end.

number_of_nodes() ->
    boot_server:number_of_nodes(),
    receive
        {get_list_length_response, X} ->
            X
    after
        5000 ->
            timeout
    end.

%%===============================================================================
%% comm_logger functions
%%===============================================================================
get_dump() ->
    [cs_send:send(cs_send:get(admin_server, Server), {get_comm_layer_dump, cs_send:this()})
     || Server <- util:get_nodes()],
    %% list({Map, StartTime})
    Dumps = [receive
                 {get_comm_layer_dump_response, Dump} ->
                     Dump
             end || _ <- util:get_nodes()],
    StartTime = lists:min([Start || {_, Start} <- Dumps]),
    Keys = lists:usort(lists:flatten([gb_trees:keys(Map) || {Map, _} <- Dumps])),
    {lists:foldl(fun (Tag, Map) ->
                         gb_trees:enter(Tag, get_aggregate(Tag, Dumps), Map)
                 end, gb_trees:empty(), Keys), StartTime}.

get_dump_bw() ->
    {Map, StartTime} = get_dump(),
    RunTime = timer:now_diff(erlang:now(), StartTime),
    [{Tag, Size / RunTime, Count / RunTime} || {Tag, {Size, Count}} <- gb_trees:to_list(Map)].

get_aggregate(_Tag, []) ->
    {0, 0};
get_aggregate(Tag, [{Dump, _} | Rest]) ->
    case gb_trees:lookup(Tag, Dump) of
        none ->
            get_aggregate(Tag, Rest);
        {value, {Size, Count}} ->
            {AggSize, AggCount} = get_aggregate(Tag, Rest),
            {AggSize + Size, AggCount + Count}
    end.

diff_dump(BeforeDump, AfterDump, _RunTime) ->
    Tags = lists:usort(lists:flatten([gb_trees:keys(BeforeDump),
                                      gb_trees:keys(AfterDump)])),
    diff(Tags, BeforeDump, AfterDump).

diff([], _Before, _After) ->
    [];
diff([Tag | Rest], Before, After) ->
    {NewSize, NewCount} = gb_trees:get(Tag, After),
    case gb_trees:lookup(Tag, Before) of
        none ->
            [{Tag, NewSize, NewCount} | diff(Rest, Before, After)];
        {value, {Size, Count}} ->
            [{Tag, NewSize - Size, NewCount - Count} | diff(Rest, Before, After)]
    end.

%%=============================================================================
%% admin server functions
%%=============================================================================
start_link() ->
    {ok, spawn_link(?MODULE, start, [])}.

start() ->
    register(admin_server, self()),
    loop().

loop() ->
    receive
        {halt,N} ->
            halt(N);
        {get_comm_layer_dump, Sender} ->
            cs_send:send(Sender, {get_comm_layer_dump_response,
                                  comm_logger:dump()}),
            loop()
    end.

%%--------------------------------------------------------------------
%% Function: nodes() -> list()
%% Description: contact boot server and list the known ip addresses
%%--------------------------------------------------------------------
% @doc contact boot server and list the known ip addresses
% @spec nodes() -> list()
-spec(nodes/0 :: () -> list()).
nodes() ->
    boot_server:node_list(),
    Nodes = receive
                {get_list_response, List} ->
                    util:uniq([IP || {IP, _, _} <- lists:sort(List)])
            end,
    Nodes.

%%=============================================================================
%% Debug functions
%%=============================================================================

-spec(print_ages/0 :: () -> ok).
print_ages() ->
    boot_server:node_list(),
    receive
        {get_list_response, List} ->
            [ cs_send:send_to_group_member(Node,cyclon,{get_ages,self()}) || Node <- List ]
    end,
    worker_loop(),
    ok.

worker_loop() ->
    receive
        {cy_ages, Ages} ->
            io:format("~p~n",[Ages]),
            worker_loop()
    after 400 ->
            ok
    end.

-spec(check_routing_tables/1 :: (any()) -> ok).
check_routing_tables(Port) ->
    boot_server:node_list(),
    receive
        {get_list_response, List} ->
            [ cs_send:send_to_group_member(Node,routing_table,{check,Port}) || Node <- List ]
    end,
    ok.

dd_check_ring() ->
    dd_check_ring(0).

dd_check_ring(Token) ->
    {ok,One} = process_dictionary:find_dht_node(),
    cs_send:send_local(One , {send_to_group_member,ring_maintenance,{init_check_ring,Token}}),
    {token_on_the_way}.
