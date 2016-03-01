% @copyright 2008-2015 Zuse Institute Berlin

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

-export([add_node/1, add_node_at_id/1, add_nodes/1, add_nodes_at_ids/1,
         add_remaining_symmetric_nodes/0,
         del_node/2, del_nodes/2, del_nodes_by_name/2,
         get_dht_node_specs/0,
         wait_for_stable_ring/1,
         check_ring/0, check_ring_deep/0, nodes/0, start_link/0, start/0, get_dump/0,
         get_dump_bw/0, diff_dump/2, print_ages/0,
         number_of_nodes/0,
         check_leases/0, check_leases/1]).

-include("scalaris.hrl").

-type check_ring_deep_error() ::
        {error,
         'in_node:', Node::node:node_type(),
         'predList:', Preds::nodelist:non_empty_snodelist(),
         'succList:', Succs::nodelist:non_empty_snodelist(),
         'nodes:', Nodes::nodelist:non_empty_snodelist(),
         'UnknownPreds:', UnknownPreds::nodelist:snodelist(),
         'UnknownSuccs:', UnknownSuccs::nodelist:snodelist()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin admin:add_nodes
% @doc add new Scalaris nodes on the local node
-spec add_node_at_id(?RT:key()) -> pid_groups:groupname() | {error, term()}.
add_node_at_id(Id) ->
    add_node([{{dht_node, id}, Id}, {skip_psv_lb}, {add_node}]).

-spec add_node([tuple()]) -> pid_groups:groupname() | {error, term()}.
add_node(Options) ->
    DhtNodeId = randoms:getRandomString(),
    Group = pid_groups:new(dht_node),
    Desc = sup:supervisor_desc(
             DhtNodeId, sup_dht_node, start_link,
             [{Group,
               [{my_sup_dht_node_id, DhtNodeId}, {add_node} | Options]}]),
    Sup = erlang:whereis(main_sup),
    case sup:start_sup_as_child([" +"], Sup, Desc) of
        {ok, _Child, Group}           ->
            DhtNodePid = pid_groups:pid_of(Group, dht_node),
            comm:send_local(DhtNodePid, {join, start}),
            Group;
        {error, already_present}      -> add_node(Options); % try again, different Id
        {error, {already_started, _}} -> add_node(Options); % try again, different Id
        {error, _Error} = X           -> X
    end.

-spec add_nodes(non_neg_integer()) -> {[pid_groups:groupname()], [{error, term()}]}.
add_nodes(0) -> {[], []};
add_nodes(Count) ->
    Results = [add_node([]) || _X <- lists:seq(1, Count)],
    lists:partition(fun(E) -> case E of
                                  {error, _} -> false;
                                  _ -> true
                              end end, Results).
%% userdevguide-end admin:add_nodes

-spec add_nodes_at_ids(list(?RT:key())) -> {[pid_groups:groupname()], [{error, term()}]}.
add_nodes_at_ids([]) -> {[], []};
add_nodes_at_ids(Keys) ->
    Results = [add_node_at_id(Key) || Key <- Keys],
    lists:partition(fun(E) -> case E of
                                  {error, _} -> false;
                                  _ -> true
                              end end, Results).

% @doc if you started a single firstnode.sh you can add the remaining symmetric r-1 nodes in the local VM,
%       for example to interactively run a unittest scenario step by step.
-spec add_remaining_symmetric_nodes() -> {[pid_groups:groupname()], [{error, term()}]}.
add_remaining_symmetric_nodes() ->
    ?ASSERT2(1 =:= admin:number_of_nodes(), more_than_one_dht_node),
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {get_state, comm:this(), node_id}),
    receive
        ?SCALARIS_RECV({get_state_response, Id}, Id)
    end,
    Keys = ?RT:get_replica_keys(Id),
    add_nodes_at_ids([X || X <- Keys, X =/= Id]).

-spec get_dht_node_specs()
        -> [{Id::term() | undefined, Child::pid() | undefined,
             Type::worker | supervisor, Modules::[module()] | dynamic}].
get_dht_node_specs() ->
    % note: only sup_dht_node children have strings as identifiers!
    [Spec || {Id, Pid, _Type, _} = Spec <- supervisor:which_children(main_sup),
             is_pid(Pid), is_list(Id)].

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
            {Successful, NoPartner} =
                lists:foldr(fun(Spec = {_Id, Pid, _Type, _}, {Successful, NoPartner}) ->
                                    Name = pid_groups:group_of(Pid),
                                    case del_node(Spec, Graceful) of
                                        ok -> {[Name | Successful], NoPartner};
                                        {error, not_found} -> {Successful, NoPartner};
                                        {error, no_partner_found} -> {Successful, [Name | NoPartner]}
                                    end
                            end, {[], []}, Specs),
            Missing = Count - length(Successful) - length(NoPartner),
            del_nodes(Missing, Graceful, Successful)
    end.

-spec del_nodes_by_name(Names::[pid_groups:groupname()], Graceful::boolean())
        -> {Successful::[pid_groups:groupname()], NotFound::[pid_groups:groupname()]}.
del_nodes_by_name(Names, Graceful) ->
    % note: specs selected now may not be available anymore when trying to
    % delete them during concurrent executions
    Specs = [{Spec, pid_groups:group_of(Pid)}
             || {_Id, Pid, _Type, _} = Spec <- get_dht_node_specs(),
                lists:member(pid_groups:group_of(Pid), Names)],
    case Specs of
        [] -> {[], []};
        [_|_] when Graceful ->
            lists:foldr(
              fun({Spec, Name}, {Ok, NotFound}) ->
                      case del_node(Spec, Graceful) of
                          ok -> {[Name | Ok], NotFound};
                          {error, not_found} -> {Ok, [Name | NotFound]};
                          {error, no_partner_found} -> {Ok, NotFound}
                      end
              end, {[], []}, Specs);
        [_|_] when not Graceful ->
            Pids = [Pid || {{_Id, Pid, _Type, _}, _Name} <- Specs],
            AllChildren = lists:flatmap(fun sup:sup_get_all_children/1, Pids),
            comm:send_local(erlang:whereis(fd), {del_all_subscriptions, AllChildren}),
            sup:sup_terminate_childs(Pids),
            lists:foldr(
              fun({{Id, _Pid, _Type, _}, Name}, {Ok, NotFound}) ->
                      _ = supervisor:terminate_child(main_sup, Id),
                      case supervisor:delete_child(main_sup, Id) of
                          ok -> {[Name | Ok], NotFound};
                          {error, not_found} -> {Ok, [Name | NotFound]}
                      end
              end, {[], []}, Specs)
    end.

%% @doc Delete a single node.
-spec del_node({Id::term() | undefined, Child::pid() | undefined,
                Type::worker | supervisor, Modules::[module()] | dynamic},
               Graceful::boolean()) -> ok | {error, not_found | no_partner_found}.
del_node({Id, Pid, _Type, _}, Graceful) ->
    case Graceful of
        true ->
            Group = pid_groups:group_of(Pid),
            case pid_groups:pid_of(Group, dht_node) of
                failed  -> {error, not_found};
                DhtNode ->
                    UId = uid:get_pids_uid(),
                    Self = comm:reply_as(comm:this(), 3, {leave_result, UId, '_'}),
                    comm:send_local(DhtNode, {leave, Self}),
                    trace_mpath:thread_yield(),
                    receive
                        ?SCALARIS_RECV(
                            {leave_result, UId, {move, result, leave, ok}}, %% ->
                            ok
                          );
                        ?SCALARIS_RECV(
                            {leave_result, UId, {move, result, leave, leave_no_partner_found}}, %% ->
                            {error, no_partner_found}
                          )
                    end
            end;
        false ->
            sup:sup_terminate_childs(Pid),
            _ = supervisor:terminate_child(main_sup, Id),
            supervisor:delete_child(main_sup, Id)
    end.

-spec wait_for_stable_ring(non_neg_integer()) -> ok.
wait_for_stable_ring(NrOfNodes) ->
    Fun = fun() ->
                  case config:read(leases) of
                      true ->
                          (number_of_nodes() =:= NrOfNodes) andalso
                              (check_leases() =:= true);
                      _ ->
                          (number_of_nodes() =:= NrOfNodes) andalso
                              (check_ring() =:= ok) andalso
                              (check_ring_deep() =:= ok)
                  end
          end,
    util:wait_for(Fun).

%% @doc Contact mgmt server and check that each node's successor and predecessor
%%      are correct.
-spec check_ring() -> {error, string()} | ok.
check_ring() ->
    Nodes = statistics:get_ring_details(),
    case lists:foldl(fun check_ring_foldl/2, first, Nodes) of
        {error, Reason} -> {error, Reason};
        _ -> ok
    end.

-spec check_ring_foldl(NodeState::statistics:ring_element(),
                       Acc::first | ?RT:key())
        -> first | {error, Reason::string()} | {Pred::?RT:key(), PredsSucc::?RT:key()}.
check_ring_foldl({ok, NodeDetails}, first) ->
    {node:id(node_details:get(NodeDetails, node)),
     node:id(node_details:get(NodeDetails, succ))};
check_ring_foldl(_, {error, Message}) ->
    {error, Message};
check_ring_foldl({ok, NodeDetails}, {Pred, PredsSucc}) ->
    MyPredId = node:id(node_details:get(NodeDetails, pred)),
    MyId = node:id(node_details:get(NodeDetails, node)),
    if
        MyId =/= PredsSucc ->
            {error, lists:flatten(io_lib:format("MyID ~p didn't match preds succ ~p", [MyId, PredsSucc]))};
        MyPredId =/= Pred ->
            {error, lists:flatten(io_lib:format("MyPredID ~p didn't match preds id ~p", [MyPredId, Pred]))};
        true ->
            {node:id(node_details:get(NodeDetails, node)),
             node:id(node_details:get(NodeDetails, succ))}
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

-spec check_leases() -> boolean().
check_leases() ->
    lease_checker:check_leases_for_all_nodes() andalso
        lease_checker:check_leases_for_the_ring().

-spec check_leases(pos_integer()) -> boolean().
check_leases(TargetSize) ->
    lease_checker:check_leases_for_all_nodes() andalso
        lease_checker:check_leases_for_the_ring(TargetSize).

-spec number_of_nodes() -> non_neg_integer() | timeout.
number_of_nodes() ->
    mgmt_server:number_of_nodes(),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {get_list_length_response, X}, %% ->
            X
          )
    after
        5000 -> timeout
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%=
% % comm_logger functions
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%=
% @doc returns communications information. the comm-layer logs for
% each message-tag how many message were sent and how large were these
% messages in total. get_dump/0 returns a map from message-tag to
% message-count and message-size and a timestamp when the measurement
% was started.
-spec get_dump() -> {Received::comm_logger:stat_tree(), Sent::comm_logger:stat_tree(), erlang_timestamp()}.
get_dump() ->
    Servers = util:get_proc_in_vms(admin_server),
    _ = [comm:send(Server, {get_comm_layer_dump, comm:this()})
         || Server <- Servers],
    %% list({Map, StartTime})
    Dumps = [  begin
                   trace_mpath:thread_yield(),
                   receive
                       ?SCALARIS_RECV(
                          {get_comm_layer_dump_response, Dump}, %% ->
                          Dump
                         )
                       end
               end || _ <- Servers],
    StartTime = lists:min([Start || {_, _, Start} <- Dumps]),
    GetKeys =
        fun(DumpsX, ElementX) ->
                lists:usort(
                  lists:flatmap(fun(X) -> gb_trees:keys(element(ElementX, X)) end,
                                DumpsX))
        end,
    ReceivedKeys = GetKeys(Dumps, 1),
    SentKeys = GetKeys(Dumps, 2),
    {lists:foldl(fun (Tag, Map) ->
                         gb_trees:enter(Tag, get_aggregate(Tag, Dumps, 1), Map)
                 end, gb_trees:empty(), ReceivedKeys),
     lists:foldl(fun (Tag, Map) ->
                         gb_trees:enter(Tag, get_aggregate(Tag, Dumps, 2), Map)
                 end, gb_trees:empty(), SentKeys),
     StartTime}.

% @doc returns communications information. similar to get_dump/0 it
% returns message statistics per message-tag, but it scales all values
% by the elapsed time. so it returns messages per second and bytes per
% second for each message-tag
-spec get_dump_bw() -> {Received::[{atom(), float(), float()}], Sent::[{atom(), float(), float()}]}.
get_dump_bw() ->
    {Received, Sent, StartTime} = get_dump(),
    RunTime = timer:now_diff(os:timestamp(), StartTime),
    AggFun =
        fun(Map) ->
                [{Tag, Size / RunTime, Count / RunTime} || {Tag, {Size, Count}} <- gb_trees:to_list(Map)]
        end,
    {AggFun(Received), AggFun(Sent)}.

get_aggregate(_Tag, [], _Element) ->
    {0, 0};
get_aggregate(Tag, [Dump | Rest], Element) ->
    Map = element(Element, Dump),
    case gb_trees:lookup(Tag, Map) of
        none ->
            get_aggregate(Tag, Rest, Element);
        {value, {Size, Count}} ->
            {AggSize, AggCount} = get_aggregate(Tag, Rest, Element),
            {AggSize + Size, AggCount + Count}
    end.

-spec diff_dump(Before::comm_logger:stat_tree(), After::comm_logger:stat_tree())
        -> [{Tag::atom(), Size::integer(), Count::integer()}].
diff_dump(BeforeDump, AfterDump) ->
    Tags = lists:usort(gb_trees:keys(BeforeDump) ++ gb_trees:keys(AfterDump)),
    diff(Tags, BeforeDump, AfterDump).

-spec diff(Tags::[atom()], Before::comm_logger:stat_tree(), After::comm_logger:stat_tree())
        -> [{Tag::atom(), Size::integer(), Count::integer()}].
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% admin server functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, spawn_link(?MODULE, start, [])}.

-spec start() -> ok.
start() ->
    erlang:register(admin_server, self()),
    loop().

loop() ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {halt, N}, %% ->
            begin
                init:stop(N),
                receive nothing -> ok end % wait forever
            end
          );
        ?SCALARIS_RECV(
            {get_comm_layer_dump, Sender}, %% ->
            begin
                comm:send(Sender, {get_comm_layer_dump_response,
                                  comm_logger:dump()}),
                loop()
            end
          )
    end.

% @doc contact mgmt server and list the known ip addresses
-spec(nodes() -> list()).
nodes() ->
    mgmt_server:node_list(),
    Nodes = begin
                trace_mpath:thread_yield(),
                receive
                    ?SCALARIS_RECV(
                       {get_list_response, List}, %% ->
                       lists:usort([IP || {IP, _, _} <- List])
                      )
                    end
            end,
    Nodes.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Debug functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec print_ages() -> ok.
print_ages() ->
    mgmt_server:node_list(),
    _ = begin
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV(
                   {get_list_response, List}, %% ->
                   [ comm:send(Node, {cb_msg, {gossip_cyclon, default}, {get_ages, self()}},
                               [{group_member, gossip}]) || Node <- List ]
                  )
                end
        end,
    worker_loop().

-spec worker_loop() -> ok.
worker_loop() ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {cy_ages, Ages}, %% ->
            begin
                io:format("~p~n", [Ages]),
                worker_loop()
            end
          )
        after 400 ->
            ok
    end.
