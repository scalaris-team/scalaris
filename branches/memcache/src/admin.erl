% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc Aministrative helper functions (mostly for debugging)
%% @version $Id$
-module(admin).
-author('schuett@zib.de').
-vsn('$Id$').

-export([add_node/1, add_node_at_id/1, add_nodes/1, del_nodes/1, del_nodes/2,
         check_ring/0, check_ring_deep/0, nodes/0, start_link/0, start/0, get_dump/0,
         get_dump_bw/0, diff_dump/3, print_ages/0,
         check_routing_tables/1, dd_check_ring/1,dd_check_ring/0,
         number_of_nodes/0]).

-include("scalaris.hrl").

-type check_ring_deep_error() ::
        {error, Node::node:node_type(), Preds::nodelist:non_empty_snodelist(), Succs::nodelist:non_empty_snodelist()}.

%%====================================================================
%% API functions
%%====================================================================

%% userdevguide-begin admin:add_nodes
% @doc add new Scalaris nodes on the local node
-spec add_node_at_id(?RT:key()) ->
        ok | {error, already_present | {already_started, pid() | undefined} | term()}.
add_node_at_id(Id) ->
    add_node([{{dht_node, id}, Id}, {skip_psv_lb}]).

-spec add_node([tuple()]) ->
        ok | {error, already_present | {already_started, pid() | undefined} | term()}.
add_node(Options) ->
    DhtNodeId = randoms:getRandomId(),
    Desc = util:sup_supervisor_desc(
             DhtNodeId, config:read(dht_node_sup), start_link,
             [[{my_sup_dht_node_id, DhtNodeId} | Options]]),
    case supervisor:start_child(main_sup, Desc) of
        {ok, _Child}        -> ok;
        {ok, _Child, _Info} -> ok;
        {error, _Error} = X -> X
    end.

-spec add_nodes(non_neg_integer()) ->
        nothing_to_do | [ok | {error, already_present |
                         {already_started, pid() | undefined} | term()},...].
add_nodes(0) -> nothing_to_do;
add_nodes(Count) ->
    [add_node([]) || _X <- lists:seq(1, Count)].
%% userdevguide-end admin:add_nodes

%% @doc Deletes nodes started with add_nodes();
%%      detects them by their random names (which is a list).
%%      Beware: Other processes in main_sup must not be started with
%%      a list as their name!
%%      Provided for convenience and backwards-compatibility - kills the node,
%%      i.e. _no_ graceful leave !
-spec del_nodes(Count::integer())
        -> nothing_to_do | [ok | {error, running | not_found | simple_one_for_one},...].
del_nodes(Count) -> del_nodes(Count, false).

%% @doc Deletes nodes started with add_nodes();
%%      detects them by their random names (which is a list).
%%      Beware: Other processes in main_sup must not be started with
%%      a list as their name!
-spec del_nodes(Count::integer(), Graceful::boolean())
        -> nothing_to_do | [ok | {error, running | not_found | simple_one_for_one},...].
del_nodes(0, _Graceful) -> nothing_to_do;
del_nodes(Count, Graceful) ->
    [del_single_node(supervisor:which_children(main_sup), Graceful)
    || _X <- lists:seq(1, Count)].

%% @doc Delete a single node if its Id, i.e. name, is a list.
-spec del_single_node([{Id::term() | undefined, Child::pid() | undefined,
                        Type::worker | supervisor, Modules::[module()] | dynamic}],
                      Graceful::boolean())
        -> ok | {error, running | not_found | simple_one_for_one}.
del_single_node([], _Graceful) ->
    ok;
del_single_node([{Key, Pid, _, _} | T], Graceful) ->
    case is_list(Key) of
        true ->
            case Graceful of
                true ->
                    Group = pid_groups:group_of(Pid),
                    DhtNode = pid_groups:pid_of(Group, dht_node),
                    comm:send_local(DhtNode, {leave});
                false ->
                    _ = supervisor:terminate_child(main_sup, Key),
                    supervisor:delete_child(main_sup, Key)
            end;
        _ -> del_single_node(T, Graceful)
    end.

%% @doc Contact boot server and check that each node's successor is correct.
-spec check_ring() -> {error, string()} | ok.
check_ring() ->
    Nodes = statistics:get_ring_details(),
    case lists:foldl(fun check_ring_foldl/2, first, Nodes) of
        {error, Reason} -> {error, Reason};
        _ -> ok
    end.

-spec check_ring_foldl(NodeState::statistics:ring_element(),
                       Acc::first | ?RT:key())
        -> first | {error, Reason::string()} | ?RT:key().
check_ring_foldl({ok, NodeDetails}, first) ->
    node:id(node_details:get(NodeDetails, succ));
check_ring_foldl({ok, NodeDetails}, PredsSucc) ->
    MyId = node:id(node_details:get(NodeDetails, node)),
    if
        MyId =:= PredsSucc ->
            node:id(node_details:get(NodeDetails, succ));
        true ->
            {error, lists:flatten(io_lib:format("~p didn't match ~p", [MyId, PredsSucc]))}
    end;
check_ring_foldl({failed, _}, Previous) ->
    Previous;
check_ring_foldl(_, Previous = {error, _Message}) ->
    Previous.

%% @doc Contact boot server and check that each node's successor and
%%      predecessor lists are correct.
-spec check_ring_deep() -> ok | check_ring_deep_error().
check_ring_deep() ->
    case check_ring() of
        ok ->
            Ring = statistics:get_ring_details(),
            Nodes = [node_details:get(Details, node) || {ok, Details} <- Ring],
            case lists:foldl(fun check_ring_deep_foldl/2, {ok, Nodes}, Ring) of
                {ok, _} -> ok;
                X       -> X
            end;
        X -> X
    end.

-spec check_ring_deep_foldl(Element::statistics:ring_element(),
        {ok, Nodes::[node:node_type()]} | check_ring_deep_error())
            -> {ok, Nodes::[node:node_type()]} | check_ring_deep_error().
check_ring_deep_foldl({ok, NodeDetails}, {ok, Nodes}) ->
    PredList = node_details:get(NodeDetails, predlist),
    SuccList = node_details:get(NodeDetails, succlist),
    CheckIsKnownNode = fun (Node) -> lists:member(Node, Nodes) end,
    case lists:all(CheckIsKnownNode, PredList) andalso
          lists:all(CheckIsKnownNode, SuccList) of
        true -> {ok, Nodes};
        _    -> {error, node_details:get(NodeDetails, node), PredList, SuccList}
    end;
check_ring_deep_foldl({failed, _}, Previous) ->
    Previous;
check_ring_deep_foldl(_, Previous = {error, _Node, _Preds, _Succs}) ->
    Previous.

-spec number_of_nodes() -> non_neg_integer() | timeout.
number_of_nodes() ->
    boot_server:number_of_nodes(),
    receive
        {get_list_length_response, X} -> X
    after
        5000 -> timeout
    end.

%%===============================================================================
%% comm_logger functions
%%===============================================================================
get_dump() ->
    _ = [comm:send(comm:get(admin_server, Server), {get_comm_layer_dump, comm:this()})
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
-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, spawn_link(?MODULE, start, [])}.

-spec start() -> ok.
start() ->
    register(admin_server, self()),
    loop().

loop() ->
    receive
        {halt, N} ->
            init:stop(N),
            receive nothing -> ok end;
        {get_comm_layer_dump, Sender} ->
            comm:send(Sender, {get_comm_layer_dump_response,
                                  comm_logger:dump()}),
            loop()
    end.

% @doc contact boot server and list the known ip addresses
-spec(nodes/0 :: () -> list()).
nodes() ->
    boot_server:node_list(),
    Nodes = receive
                {get_list_response, List} ->
                    lists:usort([IP || {IP, _, _} <- List])
            end,
    Nodes.

%%=============================================================================
%% Debug functions
%%=============================================================================

-spec print_ages() -> ok.
print_ages() ->
    boot_server:node_list(),
    _ = receive
            {get_list_response, List} ->
                [ comm:send_to_group_member(Node, cyclon, {get_ages, self()}) || Node <- List ]
        end,
    worker_loop().

-spec worker_loop() -> ok.
worker_loop() ->
    receive
        {cy_ages, Ages} ->
            io:format("~p~n", [Ages]),
            worker_loop()
        after 400 ->
            ok
    end.

%% TODO: the message this method sends is not received anywhere - either delete or update it! 
-spec check_routing_tables(any()) -> ok.
check_routing_tables(Port) ->
    boot_server:node_list(),
    _ = receive
            {get_list_response, List} ->
                [ comm:send_to_group_member(Node, routing_table, {check, Port}) || Node <- List ]
        end,
    ok.

-spec dd_check_ring() -> {token_on_the_way}.
dd_check_ring() ->
    dd_check_ring(0).

-spec dd_check_ring(non_neg_integer()) -> {token_on_the_way}.
dd_check_ring(Token) ->
    One = pid_groups:find_a(dht_node),
    _ = comm:send_local(One, {send_to_group_member, ring_maintenance, {init_check_ring, Token}}),
    {token_on_the_way}.
