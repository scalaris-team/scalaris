% @copyright 2008-2011 Zuse Institute Berlin

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

-export([add_node/1, add_node_at_id/1, add_nodes/1,
         del_node/2, del_nodes/1, del_nodes/2, del_nodes_by_name/2,
         get_dht_node_specs/0,
         check_ring/0, check_ring_deep/0, nodes/0, start_link/0, start/0, get_dump/0,
         get_dump_bw/0, diff_dump/2, print_ages/0,
         check_routing_tables/1, dd_check_ring/1,dd_check_ring/0,
         number_of_nodes/0]).

-include("scalaris.hrl").

-type check_ring_deep_error() ::
        {error,
         'in_node:', Node::node:node_type(),
         'predList:', Preds::nodelist:non_empty_snodelist(),
         'succList:', Succs::nodelist:non_empty_snodelist(),
         'nodes:', Nodes::nodelist:non_empty_snodelist(),
         'UnknownPreds:', UnknownPreds::nodelist:snodelist(),
         'UnknownSuccs:', UnknownSuccs::nodelist:snodelist()}.

%%====================================================================
%% API functions
%%====================================================================

%% userdevguide-begin admin:add_nodes
% @doc add new Scalaris nodes on the local node
-spec add_node_at_id(?RT:key()) -> pid_groups:groupname() | {error, term()}.
add_node_at_id(Id) ->
    add_node([{{dht_node, id}, Id}, {skip_psv_lb}]).

-spec add_node([tuple()]) -> pid_groups:groupname() | {error, term()}.
add_node(Options) ->
    DhtNodeId = randoms:getRandomString(),
    Desc = util:sup_supervisor_desc(
             DhtNodeId, config:read(dht_node_sup), start_link,
             [[{my_sup_dht_node_id, DhtNodeId} | Options]]),
    case supervisor:start_child(main_sup, Desc) of
        {ok, _Child, Group}           -> Group;
        {error, already_present}      -> add_node(Options); % try again, different Id
        {error, {already_started, _}} -> add_node(Options); % try again, different Id
        {error, _Error} = X           -> X
    end.

-spec add_nodes(non_neg_integer()) -> {[pid_groups:groupname()], [{error, term()}]}.
add_nodes(0) -> {[], []};
add_nodes(Count) ->
    Results = [add_node([]) || _X <- lists:seq(1, Count)],
    lists:partition(fun(E) -> not is_tuple(E) end, Results).
%% userdevguide-end admin:add_nodes

-spec get_dht_node_specs()
        -> [{Id::term() | undefined, Child::pid() | undefined,
             Type::worker | supervisor, Modules::[module()] | dynamic}].
get_dht_node_specs() ->
    % note: only sup_dht_node children have strings as identifiers!
    [Spec || {Id, Pid, _Type, _} = Spec <- supervisor:which_children(main_sup),
             is_pid(Pid), is_list(Id)].

%% @doc Deletes Scalaris nodes from the current VM.
%%      Provided for convenience and backwards-compatibility - kills the node,
%%      i.e. _no_ graceful leave!
-spec del_nodes(Count::integer()) -> [ok | {error, not_found}].
del_nodes(Count) -> del_nodes(Count, false).

%% @doc Deletes Scalaris nodes from the current VM.
-spec del_nodes(Count::non_neg_integer(), Graceful::boolean())
        -> Successful::[pid_groups:groupname()].
del_nodes(Count, Graceful) ->
    del_nodes(Count, Graceful, []).

-spec del_nodes(Count::non_neg_integer(), Graceful::boolean(), Prev::Acc)
        -> Acc when is_subtype(Acc, Successful::[pid_groups:groupname()]).
del_nodes(X, _Graceful, Prev) when X =< 0 ->
    Prev;
del_nodes(Count, Graceful, Prev) ->
    % note: specs selected now may not be available anymore when trying to
    % delete them during concurrent executions
    case util:random_subset(Count, get_dht_node_specs()) of
        [] -> Prev;
        [_|_] = Specs ->
            Successful =
                lists:foldr(fun(Spec = {_Id, Pid, _Type, _}, Successful) ->
                                    Name = pid_groups:group_of(Pid),
                                    case del_node(Spec, Graceful) of
                                        ok -> [Name | Successful];
                                        {error, not_found} -> Successful
                                    end
                            end, [], Specs),
            Missing = Count - length(Successful),
            del_nodes(Missing, Graceful, Successful)
    end.

-spec del_nodes_by_name(Names::[pid_groups:groupname()], Graceful::boolean())
        -> {Successful::[pid_groups:groupname()], NotFound::[pid_groups:groupname()]}.
del_nodes_by_name(Names, Graceful) ->
    % note: specs selected now may not be available anymore when trying to
    % delete them during concurrent executions
    Specs = [Spec || {_Id, Pid, _Type, _} = Spec <- get_dht_node_specs(),
                     lists:member(pid_groups:group_of(Pid), Names)],
    case Specs of
        [] -> {[], []};
        [_|_] ->
            lists:foldr(
              fun(Spec = {_Id, Pid, _Type, _}, {Ok, NotFound}) ->
                      Name = pid_groups:group_of(Pid),
                      case del_node(Spec, Graceful) of
                          ok -> {[Name | Ok], NotFound};
                          {error, not_found} -> {Ok, [Name | NotFound]}
                      end
              end, {[], []}, Specs)
    end.

%% @doc Delete a single node.
-spec del_node({Id::term() | undefined, Child::pid() | undefined,
                Type::worker | supervisor, Modules::[module()] | dynamic},
               Graceful::boolean()) -> ok | {error, not_found}.
del_node({Id, Pid, _Type, _}, Graceful) ->
    case Graceful of
        true ->
            Group = pid_groups:group_of(Pid),
            case pid_groups:pid_of(Group, dht_node) of
                failed  -> {error, not_found};
                DhtNode -> comm:send_local(DhtNode, {leave})
            end;
        false ->
            util:supervisor_terminate_childs(Pid),
            _ = supervisor:terminate_child(main_sup, Id),
            supervisor:delete_child(main_sup, Id)
    end.

%% @doc Contact mgmt server and check that each node's successor is correct.
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
check_ring_foldl(_, {error, Message}) ->
    {error, Message};
check_ring_foldl({ok, NodeDetails}, PredsSucc) ->
    MyId = node:id(node_details:get(NodeDetails, node)),
    if
        MyId =:= PredsSucc ->
            node:id(node_details:get(NodeDetails, succ));
        true ->
            {error, lists:flatten(io_lib:format("MyID ~p didn't match preds succc ~p", [MyId, PredsSucc]))}
    end;
check_ring_foldl({failed, _}, Previous) ->
    Previous.

%% @doc Contact mgmt server and check that each node's successor and
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
    CheckIsKnownNode = fun (Node) -> not lists:member(Node, Nodes) end,
    case {lists:filter(CheckIsKnownNode, PredList),
          lists:filter(CheckIsKnownNode, SuccList)} of
        {[],[]} -> {ok, Nodes};
        {UnknownPreds, UnknownSuccs} ->
            {error,
             'in_node:', node_details:get(NodeDetails, node),
             'predList:', PredList,
             'succList:', SuccList,
             'nodes:', Nodes,
             'UnknownPreds:', UnknownPreds,
             'UnknownSuccs:', UnknownSuccs}
    end;
check_ring_deep_foldl({failed, _}, Previous) ->
    Previous;
check_ring_deep_foldl(_, Previous) ->
    Previous.

-spec number_of_nodes() -> non_neg_integer() | timeout.
number_of_nodes() ->
    mgmt_server:number_of_nodes(),
    receive
        {get_list_length_response, X} -> X
    after
        5000 -> timeout
    end.

%%===============================================================================
% % comm_logger functions
% %===============================================================================
% @doc returns communications information. the comm-layer logs for
% each message-tag how many message were sent and how large were these
% messages in total. get_dump/0 returns a map from message-tag to
% message-count and message-size and a timestamp when the measurement
% was started.
-spec get_dump() -> {gb_tree(), {integer(), integer(), integer()}}.
get_dump() ->
    Servers = util:get_proc_in_vms(admin_server),
    _ = [comm:send(Server, {get_comm_layer_dump, comm:this()})
         || Server <- Servers],
    %% list({Map, StartTime})
    Dumps = [receive
                 {get_comm_layer_dump_response, Dump} ->
                     Dump
             end || _ <- Servers],
    StartTime = lists:min([Start || {_, Start} <- Dumps]),
    Keys = lists:usort(lists:flatten([gb_trees:keys(Map) || {Map, _} <- Dumps])),
    {lists:foldl(fun (Tag, Map) ->
                         gb_trees:enter(Tag, get_aggregate(Tag, Dumps), Map)
                 end, gb_trees:empty(), Keys), StartTime}.

% @doc returns communications information. similar to get_dump_bw/0 it
% returns message statistics per message-tag, but it scales all values
% by the elapsed time. so it returns messages per second and bytes per
% second for each message-tag
-spec get_dump_bw() -> list({atom(), float(), float()}).
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

-spec diff_dump(gb_tree(), gb_tree()) -> list({atom(), integer(), integer()}).
diff_dump(BeforeDump, AfterDump) ->
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
    erlang:register(admin_server, self()),
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

% @doc contact mgmt server and list the known ip addresses
-spec(nodes/0 :: () -> list()).
nodes() ->
    mgmt_server:node_list(),
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
    mgmt_server:node_list(),
    _ = receive
            {get_list_response, List} ->
                [ comm:send(Node, {get_ages, self()}, [{group_member, cyclon}]) || Node <- List ]
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
    mgmt_server:node_list(),
    _ = receive
            {get_list_response, List} ->
                [ comm:send(Node, {check, Port}, [{group_member, routing_table}]) || Node <- List ]
        end,
    ok.

-spec dd_check_ring() -> {token_on_the_way}.
dd_check_ring() ->
    dd_check_ring(0).

-spec dd_check_ring(non_neg_integer()) -> {token_on_the_way}.
dd_check_ring(Token) ->
    One = pid_groups:find_a(dht_node),
    _ = comm:send_local(One, {rm, init_check_ring, Token}),
    {token_on_the_way}.
