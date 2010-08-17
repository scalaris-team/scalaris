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

-export([add_node/1, add_node_at_id/1, add_nodes/1, del_nodes/1,
         check_ring/0, check_ring_deep/0, nodes/0, start_link/0, start/0, get_dump/0,
         get_dump_bw/0, diff_dump/3, print_ages/0,
         check_routing_tables/1, dd_check_ring/1,dd_check_ring/0,
         number_of_nodes/0]).

-include("scalaris.hrl").

%%====================================================================
%% API functions
%%====================================================================

%% userdevguide-begin admin:add_nodes
% @doc add new Scalaris nodes on the local node
-spec add_node_at_id(?RT:key()) -> ok.
add_node_at_id(Id) ->
    add_node([{{idholder, id}, Id}]).

-spec add_node(list(tuple())) -> ok.
add_node(Options) ->
    Desc = util:sup_supervisor_desc(randoms:getRandomId(),
                                    sup_dht_node, start_link, [Options]),
    supervisor:start_child(main_sup, Desc),
    ok.

-spec add_nodes(non_neg_integer()) -> ok.
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

%% @doc Deletes nodes started with add_nodes();
%%      detects them by their random names (which is a list).
%%      Beware: Other processes in main_sup must not be started with
%%      a list as their name!
-spec del_nodes(integer()) -> ok.
del_nodes(Count) ->
    [ del_single_node(supervisor:which_children(main_sup))
      || _ <- lists:seq(1, Count) ],
    ok.

%% @doc Delete a single node if its Id, i.e. name, is a list.
-spec del_single_node([{Id::term() | undefined, Child::pid() | undefined,
                        Type::worker | supervisor, Modules::[module()] | dynamic}]) -> ok.
del_single_node([]) ->
    ok;
del_single_node([{Key, _, _, _} | T]) ->
    case is_list(Key) of
        true ->
            supervisor:terminate_child(main_sup, Key),
            supervisor:delete_child(main_sup, Key);
        _ -> del_single_node(T)
    end.

%% @doc Contact boot server and check ring.
-spec check_ring() -> {error, string()} | ok.
check_ring() ->
    Nodes = statistics:get_ring_details(),
    case lists:foldl(fun check_ring_foldl/2, first, Nodes) of
        {error, Reason} -> {error, Reason};
        _ -> ok
    end.

-spec check_ring_deep() -> {error, list(), list()} | ok.
check_ring_deep() ->
    case check_ring() of
        ok ->
            Nodes = statistics:get_ring_details(),
            NodePids = strip_node_details(Nodes),
            case lists:foldl(fun check_ring_deep_foldl/2,
                             {ok, NodePids}, Nodes) of
                {ok, NodePids} ->
                    ok;
                X ->
                    X
            end;
        X ->
            X
    end.

strip_node_details([]) -> [];
strip_node_details([{ok, NodeDetails} | Rest]) ->
    [node_details:get(NodeDetails, node) | strip_node_details(Rest)];
strip_node_details([_ | Rest]) ->
    strip_node_details(Rest).

check_ring_deep_foldl({ok, NodeDetails}, {ok, NodePids}) ->
    PredList = node_details:get(NodeDetails, predlist),
    SuccList = node_details:get(NodeDetails, succlist),
    CheckIsKnownNode = fun (Node) ->
                               lists:member(Node, NodePids)
                       end,
    case lists:all(CheckIsKnownNode, PredList) andalso
          lists:all(CheckIsKnownNode, SuccList) of
        true ->
            {ok, NodePids};
        _ ->
            {error, PredList, SuccList}
    end;
check_ring_deep_foldl(X, {ok, _}) ->
    X;
check_ring_deep_foldl(_, X) ->
    X.

-spec check_ring_foldl(NodeState::{ok, node_details:node_details()} | {failed},
                       Acc::first | ?RT:key())
        -> first | {error, Reason::string()} | ?RT:key().
check_ring_foldl({ok, NodeDetails}, first) ->
    node:id(node_details:get(NodeDetails, succ));
check_ring_foldl({failed}, Previous) ->
    Previous;
check_ring_foldl(_, {error, Message}) ->
    {error, Message};
check_ring_foldl({ok, NodeDetails}, PredsSucc) ->
    MyId = node:id(node_details:get(NodeDetails, node)),
    if
        MyId =:= PredsSucc ->
            node:id(node_details:get(NodeDetails, succ));
        true ->
            {error, lists:flatten(io_lib:format("~p didn't match ~p", [MyId, PredsSucc]))}
    end.

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
    [comm:send(comm:get(admin_server, Server), {get_comm_layer_dump, comm:this()})
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

-spec start() -> none().
start() ->
    register(admin_server, self()),
    loop().

loop() ->
    receive
        {halt,N} ->
            halt(N);
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

-spec(print_ages/0 :: () -> ok).
print_ages() ->
    boot_server:node_list(),
    receive
        {get_list_response, List} ->
            [ comm:send_to_group_member(Node,cyclon,{get_ages,self()}) || Node <- List ]
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
            [ comm:send_to_group_member(Node,routing_table,{check,Port}) || Node <- List ]
    end,
    ok.

-spec dd_check_ring() -> {token_on_the_way}.
dd_check_ring() ->
    dd_check_ring(0).

-spec dd_check_ring(non_neg_integer()) -> {token_on_the_way}.
dd_check_ring(Token) ->
    One = pid_groups:find_a(dht_node),
    comm:send_local(One, {send_to_group_member, ring_maintenance, {init_check_ring, Token}}),
    {token_on_the_way}.
