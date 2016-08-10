% @copyright 2013-2015 Zuse Institute Berlin

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

%% @author Magnus Mueller <mamuelle@informatik.hu-berlin.de>
%% @author Jens Fischer <jensvfischer@gmail.com>

%% @doc Flexible routing table. Two flexible routing tables (FRTs) are implemented,
%% FRT-chord and grouped FRT-chord (GFRT). The functions specific to these two
%% implementations can be found at the end of this file (seperated by ifdefs).
%%
%%
%% @end
%% @version $Id$
-module(rt_frt).
-author('mamuelle@informatik.hu-berlin.de').
-behaviour(rt_beh).

-export([dump_to_csv/1, get_source_id/1, get_source_node/1]).

% exports for unit tests
-export([check_rt_integrity/1, check_well_connectedness/1, get_random_key_from_generator/3]).

% Functions which are specific to the frt/gfrt implementation
-export([allowed_nodes/1, frt_check_config/0, rt_entry_info/4]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% RT Implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type key() :: rt_chord:key().
-type external_rt_t_tree() :: gb_trees:tree(NodeId::key(), Node::mynode()).
-opaque external_rt() :: {Size::unknown | float(), external_rt_t_tree()}. %% @todo: make opaque

% define the possible types of nodes in the routing table:
%  - normal nodes are nodes which have been added by entry learning
%  - source is the node at which this RT is
%  - sticky is a node which is not to be deleted with entry filtering
-type entry_type() :: normal | source | sticky.
-type custom_info() :: undefined | term().

-type(mynode() :: {Id::key(), IdVersion::non_neg_integer(),
                   DHTNodePid::comm:mypid(), RTLoopPid::comm:mypid() | none}).
-record(rt_entry, {
        node :: mynode(),
        type :: entry_type(),
        adjacent_fingers = {undefined, undefined} :: {key() |
                                                      'undefined', key() |
                                                      'undefined'},
        custom = undefined :: custom_info()
    }).

-type(rt_entry() :: #rt_entry{}).

-export_type([rt_entry/0]).

-type rt_t_tree() :: gb_trees:tree(NodeId::key(), rt_entry()).
-record(rt_t, {
        source = undefined :: key() | undefined
        , num_active_learning_lookups = 0 :: non_neg_integer()
        , nodes = gb_trees:empty() :: rt_t_tree()
        , nodes_in_ring = unknown :: unknown | float()
        %% , nodes_in_ring = unknown :: Size :: unknown | float()
    }).

-opaque rt() :: #rt_t{}.

-type custom_message() :: {get_rt, SourcePID::comm:mypid()}
                        | {get_rt_reply, RT::rt()}
                        | {trigger_random_lookup}
                        | {rt_get_node, From::comm:mypid()}
                        | {rt_learn_node, NewNode::mynode()}
                        | {rt_get_neighbor, From::comm:mypid()}
                        | {rt_learn_neighbor, NewNode::mynode()}.

-include("scalaris.hrl").
-include("rt_beh.hrl").

% @doc Maximum number of entries in a routing table
-spec maximum_entries() -> non_neg_integer().
maximum_entries() -> config:read(rt_frt_max_entries).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc This function is called during the startup of the rt_loop process and
%%      is allowed to send trigger messages.
-spec init() -> ok.
init() ->
    % initializse the trigger for random lookups
    case config:read(rt_frt_al) of
        true -> msg_delay:send_trigger(0, {trigger_random_lookup});
        false -> ok
    end.

%% @doc Activate the routing table.
%%      This function is called during the activation of the routing table process.
-spec activate(nodelist:neighborhood()) -> rt().
activate(Neighbors) ->
    % ask the successor node for its routing table
    Msg = {?send_to_group_member, routing_table, {get_rt, comm:this()}},
    comm:send(node:pidX(nodelist:succ(Neighbors)), Msg),

    % request approximated ring size
    gossip_load:get_values_best([]),

    MyNode = node2mynode(nodelist:node(Neighbors), comm:this()),
    update_entries(Neighbors, add_source_entry(MyNode, #rt_t{})).

%% @doc Hashes the key to the identifier space.
-spec hash_key(client_key() | binary()) -> key().
hash_key(Key) -> rt_chord:hash_key(Key).
%% userdevguide-end rt_frtchord:hash_key

%% userdevguide-begin rt_frtchord:get_random_node_id
%% @doc Generates a random node id, i.e. a random 128-bit number.
-spec get_random_node_id() -> key().
get_random_node_id() -> rt_chord:get_random_node_id().
%% userdevguide-end rt_frtchord:get_random_node_id

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_frtchord:init_stabilize
%% @doc Triggered by a new stabilization round, renews the routing table.
%% Check:
%% - if node id didn't change, i.e. only preds/succs changed, update sticky entries
%% - if node id changed, just renew the complete table and maybe tell known nodes
%%  that something changed (async and optimistic -> if they don't care, we don't care)
-spec init_stabilize(nodelist:neighborhood(), rt()) -> rt().
init_stabilize(Neighbors, RT) ->
    %% log:pal("[~w] init_stabilize", [self()]),
    case node:id(nodelist:node(Neighbors)) =:= entry_nodeid(get_source_node(RT)) of
        true ->
            update_entries(Neighbors, RT) ;
        false -> % source node changed, replace the complete table
            activate(Neighbors)
    end
    .
%% userdevguide-end rt_frtchord:init_stabilize

%% @doc Update sticky entries
%% This function converts sticky nodes from the RT which aren't in the neighborhood
%% anymore to normal nodes. Afterwards it adds new nodes from the neighborhood.
-spec update_entries(NewNeighborhood::nodelist:neighborhood(), RT::rt()) -> rt().
update_entries(NewNeighborhood, RT) ->
    %% neighborhood nodes (sticky nodes) from RT
    OldNeighbors1 = lists:map(fun rt_entry_node/1, get_sticky_entries(RT)),
    OldNeighbors2 = lists:map(fun(Node) -> {id(Node), pid_dht(Node)} end, OldNeighbors1),
    OldNeighbors3 = sets:from_list(OldNeighbors2),
    %% OldNeighbors = sets:from_list(tl(nodelist:to_list(OldNeighborhood))),

    %% neighborhood nodes (succs + preds w/o self) from new neighborhood from rm
    NewNeighbors1 = tl(nodelist:to_list(NewNeighborhood)),
    NewNeighbors2 = lists:map(fun(Node) -> {node:id(Node), node:pidX(Node)} end, NewNeighbors1),
    NewNeighbors3 = sets:from_list(NewNeighbors2),

    %% former neighborhood nodes which aren't neighboorhod nodes anymore
    ConvertNodes = sets:subtract(OldNeighbors3, NewNeighbors3),
    ConvertNodesIds = util:sets_map(fun({Id, _Pid}) -> Id end, ConvertNodes),

    %% new neighboorhod nodes
    ToBeAddedNodes1 = sets:subtract(NewNeighbors3, OldNeighbors3),
    %% Add PidRTs to the nodes to be added. There might be PidRTs in the old RT
    %% even for "new" nodes, e.g. for nodes added through get_rt_reply (i.e. for
    %% normal nodes that are converted to sticky nodes). Get the VersionIds from
    %% the list of new nodes.
    OldNodes = lists:map(fun rt_entry_node/1, gb_trees:values(get_rt_tree(RT))),
    NewNodes = lists:map(fun(Node) -> {node:id(Node), node:id_version(Node),
                                       node:pidX(Node)}
                         end, NewNeighbors1),
    ToBeAddedNodes2 =
        util:sets_map(fun({Id, PidDHT}) ->
                              PidRT = case lists:keyfind(Id, 1, OldNodes) of
                                  {_Id, _IdVersion, _PidDHT, PidRT0} -> PidRT0;
                                  _else -> none
                              end,
                              {_, IdVersion, _} = lists:keyfind(Id, 1, NewNodes),
                              {Id, IdVersion, PidDHT, PidRT}
                      end, ToBeAddedNodes1),

    %% uncomment for debugging
    %% case sets:size(ConvertNodes) > 0 orelse sets:size(ToBeAddedNodes1) > 0 of
    %%     true ->
    %%         log:pal("~w~nOldNeighbors:~n~190.2p~nNewNeighors:~n~190.2p~n" ++
    %%                 "ConvertNodes:~n~190.2p~nToBeAddedNodes~n~190.2p~n",
    %%                 [self(), OldNeighbors1, NewNeighbors1, sets:to_list(ConvertNodes),
    %%                  ToBeAddedNodes2]);
    %%     false -> ok
    %% end,

    %% convert former neighboring nodes to normal nodes and add sticky nodes
    FilteredRT = lists:foldl(fun sticky_entry_to_normal_node/2, RT, ConvertNodesIds),
    NewRT = lists:foldl(fun add_sticky_entry/2, FilteredRT, ToBeAddedNodes2),

    %% request rt_loop pids of new neighbours with unknown PidRTs
    ToBeAddedNodes3 = lists:filter(fun({_Id, _IdVersion, _PidDHT, PidRT}) ->
                                           PidRT =:= none
                                   end, ToBeAddedNodes2),
    lists:foreach(fun(Node) -> comm:send(pid_dht(Node),
                                         {?send_to_group_member, routing_table,
                                          {rt_get_neighbor, comm:this()}})
                  end, ToBeAddedNodes3),

    check_helper(RT, NewRT, true),
    NewRT.

%% userdevguide-begin rt_frtchord:update
%% @doc Updates the routing table due to a changed node ID, pred and/or succ.
%% - We must rebuild the complete routing table when the source node id changed
%% - If only the preds/succs changed, adapt the old routing table
-spec update(OldRT::rt(), OldNeighbors::nodelist:neighborhood(),
    NewNeighbors::nodelist:neighborhood()) -> {trigger_rebuild, rt()} | {ok, rt()}.
update(OldRT, Neighbors, Neighbors) -> {ok, OldRT};
update(OldRT, OldNeighbors, NewNeighbors) ->
    case nodelist:node(OldNeighbors) =:= nodelist:node(NewNeighbors) of
        true -> % source node didn't change
            % update the sticky nodes: delete old nodes and add new nodes
            {ok, update_entries(NewNeighbors, OldRT)};
        _Else -> % source node changed, rebuild the complete table
            {trigger_rebuild, OldRT}
    end
    .
%% userdevguide-end rt_frtchord:update

%% userdevguide-begin rt_frtchord:filter_dead_node
%% @doc Removes dead nodes from the routing table (rely on periodic
%%      stabilization here).
-spec filter_dead_node(rt(), DeadPid::comm:mypid(), Reason::fd:reason()) -> rt().
filter_dead_node(RT, DeadPid, _Reason) ->
    % find the node id of DeadPid and delete it from the RT
    case [N || N <- internal_to_list(RT), pid_dht(N) =:= DeadPid] of
        [Node] -> entry_delete(id(Node), RT);
        [] -> RT
    end
    .
%% userdevguide-end rt_frtchord:filter_dead_node

%% userdevguide-begin rt_frtchord:to_pid_list
%% @doc Returns the pids of the routing table entries.
-spec to_pid_list(rt()) -> [comm:mypid()].
to_pid_list(RT) -> [pid_dht(N) || N <- internal_to_list(RT)].
%% userdevguide-end rt_frtchord:to_pid_list

%% @doc Get the size of the RT excluding entries which are not tagged as normal entries.
-spec get_size_without_special_nodes(rt()) -> non_neg_integer().
get_size_without_special_nodes(#rt_t{} = RT) ->
    util:gb_trees_foldl(
        fun(_Key, Val, Acc) ->
                Acc + case entry_type(Val) of
                    normal -> 1;
                    _else -> 0
                end
        end, 0, get_rt_tree(RT)).

%% userdevguide-begin rt_frtchord:get_size
%% @doc Returns the size of the routing table.
-spec get_size(rt()) -> non_neg_integer().
get_size(#rt_t{} = RT) -> gb_trees:size(get_rt_tree(RT)).

%% @doc Returns the size of the external routing table.
-spec get_size_ext(external_rt()) -> non_neg_integer().
get_size_ext(RT) ->
    gb_trees:size(external_rt_get_tree(RT)).
%% userdevguide-end rt_frtchord:get_size

%% userdevguide-begin rt_frtchord:n
%% @doc Returns the size of the address space.
-spec n() -> 16#100000000000000000000000000000000.
n() -> 16#100000000000000000000000000000000.
%% userdevguide-end rt_frtchord:n

%% @doc Gets the number of keys in the interval (Begin, End]. In the special
%%      case of Begin==End, the whole key range as specified by n/0 is returned.
-spec get_range(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE) -> non_neg_integer().
get_range(Begin, End) -> rt_chord:get_range(Begin, End).

%% @doc Gets the key that splits the interval (Begin, End] so that the first
%%      interval will be (Num/Denom) * range(Begin, End). In the special case of
%%      Begin==End, the whole key range is split.
%%      Beware: SplitFactor must be in [0, 1]; the final key will be rounded
%%      down and may thus be Begin.
-spec get_split_key(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE,
                    SplitFraction::{Num::non_neg_integer(), Denom::pos_integer()}) -> key() | ?PLUS_INFINITY_TYPE.
get_split_key(Begin, End, SplitFraction) ->
    rt_chord:get_split_key(Begin, End, SplitFraction).

%% @doc Splits the range between Begin and End into up to Parts equal parts and
%%      returning the according split keys.
-spec get_split_keys(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE,
                     Parts::pos_integer()) -> [key()].
get_split_keys(Begin, End, Parts) ->
    rt_chord:get_split_keys(Begin, End, Parts).

%% @doc Gets input similar to what intervals:get_bounds/1 returns and
%%      calculates a random key in this range. Fails with an exception if there
%%      is no key.
-spec get_random_in_interval(intervals:simple_interval2()) -> key().
get_random_in_interval(SimpleI) ->
    rt_chord:get_random_in_interval(SimpleI).

%% @doc Gets input similar to what intervals:get_bounds/1 returns and
%%      calculates Count number of random keys in this range (duplicates may
%%      exist!). Fails with an exception if there is no key.
-spec get_random_in_interval(intervals:simple_interval2(), Count::pos_integer()) -> [key(),...].
get_random_in_interval(SimpleI, Count) ->
    rt_chord:get_random_in_interval(SimpleI, Count).

%% userdevguide-begin rt_frtchord:get_replica_keys
%% @doc Returns the replicas of the given key.
-spec get_replica_keys(key()) -> [key()].
get_replica_keys(Key) ->
    rt_simple:get_replica_keys(Key).

-spec get_replica_keys(key(), pos_integer()) -> [key()].
get_replica_keys(Key, ReplicationFactor) ->
    rt_simple:get_replica_keys(Key, ReplicationFactor).
%% userdevguide-end rt_frtchord:get_replica_keys

-spec get_key_segment(key()) -> pos_integer().
get_key_segment(Key) ->
    rt_simple:get_key_segment(Key).

-spec get_key_segment(key(), pos_integer()) -> pos_integer().
get_key_segment(Key, ReplicationFactor) ->
    rt_simple:get_key_segment(Key, ReplicationFactor).

%% userdevguide-begin rt_frtchord:dump
%% @doc Dumps the RT state for output in the web interface.
-spec dump(RT::rt()) -> KeyValueList::[{Index::string(), Node::string()}].
dump(RT) -> [{"0", webhelpers:safe_html_string("~p", [RT])}].
%% userdevguide-end rt_frtchord:dump

% @doc Dump the routing table into a CSV string
-spec dump_to_csv(RT::rt()) -> [char()].
dump_to_csv(RT) ->
    Fingers = internal_to_list(RT),
    IndexedFingers = lists:zip(lists:seq(1,length(Fingers)), Fingers),
    MyId = get_source_id(RT),
    lists:flatten(
        [
            "Finger,Id\n"
            , io_lib:format("0,~p~n", [MyId])
        ] ++
        [
            io_lib:format("~p,~p~n",[Index,id(Finger)])
            || {Index, Finger} <- IndexedFingers
        ]
    )
    .


%% @doc Checks whether config parameters of the rt_frtchord process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_in(key_creator, [random, random_with_bit_mask]) and
        case config:read(key_creator) of
            random -> true;
            random_with_bit_mask ->
                config:cfg_is_tuple(key_creator_bitmask, 2,
                                fun({Mask1, Mask2}) ->
                                        erlang:is_integer(Mask1) andalso
                                            erlang:is_integer(Mask2) end,
                                "{int(), int()}");
            _ -> false
        end and
    config:cfg_is_bool(rt_frt_al) and
    config:cfg_is_greater_than_equal(rt_frt_al_interval, 0) and
    config:cfg_is_integer(rt_frt_max_entries) and
    config:cfg_is_greater_than(rt_frt_max_entries, 0) and
    config:cfg_is_integer(rt_frt_max_entries) and
    config:cfg_is_greater_than(rt_frt_max_entries, 0) and
    config:cfg_is_integer(rt_frt_gossip_interval) and
    config:cfg_is_greater_than(rt_frt_gossip_interval, 0) and
    config:cfg_is_in(rt_frt_reduction_ratio_strategy,
        [best_rt_reduction_ratio, convergent_rt_reduction_ratio]) and
    frt_check_config()
    .

%% @doc Generate a random key from the pdf as defined in (Nagao, Shudo, 2011)
%% TODO I floor the key for now; the key generator should return ints, but returns
%float. It is currently unclear if this is a bug in the original paper by Nagao and
%Shudo. Using erlang:trunc/1 should be enough for flooring, as X >= 0
-spec get_random_key_from_generator(SourceNodeId::key(),
                                    PredId::key(),
                                    SuccId::key()
                                   ) -> key().
get_random_key_from_generator(SourceNodeId, PredId, SuccId) ->
    Rand = randoms:uniform(),
    X = erlang:trunc(get_range(SourceNodeId, SuccId) *
                         math:pow(get_range(SourceNodeId, PredId) /
                                      get_range(SourceNodeId, SuccId),
                                  Rand
                                 )),
    rt_chord:add_range(SourceNodeId, X).

%% @doc Trigger need to be resend here w/o queuing to avoid trace infestation.
-spec handle_custom_message_inactive(custom_message(), msg_queue:msg_queue()) ->
    msg_queue:msg_queue().
% resend trigger
handle_custom_message_inactive({trigger_random_lookup}, MsgQueue) ->
    Interval = config:read(rt_frt_al_interval),
    msg_delay:send_trigger(Interval, {trigger_random_lookup}),
    MsgQueue;
% queue all other messages
handle_custom_message_inactive(Msg, MsgQueue) ->
    msg_queue:add(MsgQueue, Msg).


%% userdevguide-begin rt_frtchord:handle_custom_message
%% @doc Handle custom messages. The following messages can occur:
%%      - TODO explain messages

% send the RT to a node asking for it
-spec handle_custom_message(custom_message(), rt_loop:state_active()) ->
                                   rt_loop:state_active() | unknown_event.
handle_custom_message({get_rt, Pid}, State) ->
    comm:send(Pid, {get_rt_reply, rt_loop:get_rt(State)}),
    State
    ;

handle_custom_message({get_rt_reply, RT}, State) ->
    %% merge the routing tables. Note: We don't care if this message is not from our
    %% current successor. We just have to make sure that the merged entries are valid.
    OldRT = rt_loop:get_rt(State),
    NewRT = case OldRT =/= RT of
        true ->
            % - add each entry from the other RT if it doesn't already exist
            % - entries are added as normal entries. RM (-> update()) invokes
            %   adding sticky nodes or converting normal nodes to sticky nodes
            %   respectively
            util:gb_trees_foldl(
                fun(Key, Entry, Acc) ->
                        case entry_exists(Key, Acc) of
                            true -> Acc;
                            false -> add_normal_entry(rt_entry_node(Entry), Acc)
                        end
                end,
                OldRT, get_rt_tree(RT));
        false -> OldRT
    end,
    NewERT = check(OldRT, NewRT, rt_loop:get_ert(State),
                    rt_loop:get_neighb(State), true),
    rt_loop:set_ert(rt_loop:set_rt(State, NewRT), NewERT);

% lookup a random key chosen with a pdf:
% x = sourcenode + d(s,succ)*(d(s,pred)/d(s,succ))^rnd
% where rnd is chosen uniformly from [0,1)
handle_custom_message({trigger_random_lookup}, State) ->
    RT = rt_loop:get_rt(State),
    SourceNode = get_source_node(RT),
    SourceNodeId = entry_nodeid(SourceNode),
    {PredId, SuccId} = adjacent_fingers(SourceNode),
    Key = get_random_key_from_generator(SourceNodeId, PredId, SuccId),

    % schedule the next random lookup
    Interval = config:read(rt_frt_al_interval),
    msg_delay:send_trigger(Interval, {trigger_random_lookup}),

    api_dht_raw:unreliable_lookup(Key, {?send_to_group_member, routing_table,
                                        {rt_get_node, comm:this()}}),
    State
    ;

handle_custom_message({rt_get_node, From}, State) ->
    MyNode = nodelist:node(rt_loop:get_neighb(State)),
    comm:send(From, {rt_learn_node, node2mynode(MyNode, comm:this())}),
    State;

handle_custom_message({rt_learn_node, NewNode}, State) ->
    OldRT = rt_loop:get_rt(State),
    {NewRT, NewERT} =
        case rt_lookup_node(id(NewNode), OldRT) of
            none ->
                RT = add_normal_entry(NewNode, OldRT),
                ERT = check(OldRT, RT, rt_loop:get_ert(State),
                                rt_loop:get_neighb(State), true),
                {RT, ERT};
            {value, _RTEntry} -> {OldRT, rt_loop:get_ert(State)}
        end,
    rt_loop:set_ert(rt_loop:set_rt(State, NewRT), NewERT);

handle_custom_message({rt_get_neighbor, From}, State) ->
    MyNode = nodelist:node(rt_loop:get_neighb(State)),
    comm:send(From, {rt_learn_neighbor, node2mynode(MyNode, comm:this())}),
    State;

handle_custom_message({rt_learn_neighbor, {Id, IdVersion, PidDHT, PidRT}}, State) ->
    OldRT = rt_loop:get_rt(State),
    {NewRT, NewERT} =
        case rt_lookup_node(Id, OldRT) of
            %% if no entry exists: don't add
            %% this could be due to
            %% 1) nodes which were neighbors when sending the rt_get_neighbor
            %%    request, but aren't neighbors anymore and have been filtered
            %%    out of the rt since
            %% 2) Interleavings of requesting the rt pids and crashing nodes
            none -> {OldRT, rt_loop:get_ert(State)};

            %% only update the RT if the PidRT is unknown
            {value, OldEntry} ->
                OldNode = rt_entry_node(OldEntry),
                case OldNode =:= {Id, IdVersion, PidDHT, none} of
                    true ->
                        %% update the rt pid
                        OldEntry= rt_get_node(Id, OldRT),
                        NewNode = {Id, IdVersion, PidDHT, PidRT},
                        NewEntry = rt_entry_set_node(OldEntry, NewNode),
                        OldNodeTree = get_rt_tree(OldRT),
                        NewNodeTree = gb_trees:update(Id, NewEntry, OldNodeTree),
                        RT = rt_set_nodes(OldRT, NewNodeTree),
                        ERT = check(OldRT, RT, rt_loop:get_ert(State),
                                    rt_loop:get_neighb(State), true),
                        %% print_debug(true, OldNode, {Id, PidDHT, PidRT}, ERT),
                        {RT, ERT};
                    false ->
                        %% print_debug(false, OldNode, {Id, PidDHT, PidRT}, rt_loop:get_ert(State)),
                        {OldRT, rt_loop:get_ert(State)}
                end
        end,
    rt_loop:set_ert(rt_loop:set_rt(State, NewRT), NewERT);

handle_custom_message({gossip_get_values_best_response, Vals}, State) ->
    RT = rt_loop:get_rt(State),
    NewRT = rt_set_ring_size(RT, gossip_load:load_info_get(size_kr, Vals)),
    gossip_load:get_values_best([{msg_delay, config:read(rt_frt_gossip_interval)}]),
    ERT = export_rt_to_dht_node(NewRT, rt_loop:get_neighb(State)),
    comm:send_local(rt_loop:get_pid_dht(State), {rt_update, ERT}),
    rt_loop:set_ert(rt_loop:set_rt(State, NewRT), ERT);

handle_custom_message(_Message, _State) -> unknown_event.
%% userdevguide-end rt_frtchord:handle_custom_message

%% userdevguide-begin rt_frtchord:check
%% @doc Notifies the dht_node and failure detector if the routing table changed.
%%      Provided for convenience (see check/5).
-spec check(OldRT::rt(), NewRT::rt(), OldERT::external_rt(),
                Neighbors::nodelist:neighborhood(),
                ReportToFD::boolean()) -> NewERT::external_rt().
check(OldRT, NewRT, OldERT, Neighbors, ReportToFD) ->
    check(OldRT, NewRT, OldERT, Neighbors, Neighbors, ReportToFD).

%% @doc Notifies the dht_node if the (external) routing table changed.
%%      Also updates the failure detector if ReportToFD is set.
%%      Note: the external routing table only changes if the internal RT has
%%      changed.
-spec check(OldRT::rt(), NewRT::rt(), OldERT::external_rt(),
                OldNeighbors::nodelist:neighborhood(),
                NewNeighbors::nodelist:neighborhood(),
                ReportToFD::boolean()) -> NewERT::external_rt().
check(OldRT, NewRT, OldERT, OldNeighbors, NewNeighbors, ReportToFD) ->
    % if the routing tables haven't changed and the successor/predecessor haven't changed
    % as well, do nothing
    case OldRT =:= NewRT andalso
         nodelist:succ(OldNeighbors) =:= nodelist:succ(NewNeighbors) andalso
         nodelist:pred(OldNeighbors) =:= nodelist:pred(NewNeighbors) of
        true -> OldERT;
        _Else -> export_to_dht(NewRT, ReportToFD)
    end.

% @doc Helper to send the new routing table to the dht node
-spec export_to_dht(rt(), ReportToFD::boolean()) ->
    NewERT::external_rt().
export_to_dht(NewRT, ReportToFD) ->
    Pid = pid_groups:get_my(dht_node),
    ERT = export_rt_to_dht_node_helper(NewRT),
    case Pid of
        failed -> ok;
        _ -> comm:send_local(Pid, {rt_update, ERT})
    end,
    % update failure detector:
    case ReportToFD of
        true -> add_fd(NewRT);
        _ -> ok
    end,
    ERT.

% @doc Helper to check for routing table changes, excluding changes to the neighborhood.
-spec check_helper(OldRT::rt(), NewRT::rt(), ReportToFD::boolean()) -> ok.
check_helper(OldRT, NewRT, ReportToFD) ->
    case OldRT =:= NewRT
    of
        true -> ok;
        false -> export_to_dht(NewRT, ReportToFD)
    end.

%% @doc Filter the source node's pid from a list.
-spec filter_source_pid(rt(), [comm:mypid()]) -> [comm:mypid()].
filter_source_pid(RT, ListOfPids) ->
    SourcePid = pid_dht(rt_entry_node(get_source_node(RT))),
    [P || P <- ListOfPids, P =/= SourcePid].

%% @doc Set up a set of subscriptions from a routing table
-spec add_fd(RT::rt())  -> ok.
add_fd(#rt_t{} = RT) ->
    NewPids = to_pid_list(RT),
    % Filter the source node from the Pids, as we don't need an FD for that node. If the
    % source node crashes (which is the process calling this function), we are done
    % for.
    fd:subscribe(self(), filter_source_pid(RT, NewPids)).

%% @doc Update subscriptions
-spec update_fd(OldRT::rt(), NewRT::rt()) -> ok.
update_fd(OldRT, OldRT) -> ok;
update_fd(#rt_t{} = OldRT, #rt_t{} = NewRT) ->
    OldPids = filter_source_pid(OldRT, to_pid_list(OldRT)),
    NewPids = filter_source_pid(NewRT, to_pid_list(NewRT)),
    fd:update_subscriptions(self(), OldPids, NewPids).

%% userdevguide-end rt_frtchord:check

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Communication with routing_table and dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_frtchord:empty_ext
-spec empty_ext(nodelist:neighborhood()) -> external_rt().
empty_ext(_Neighbors) -> {unknown, gb_trees:empty()}.
%% userdevguide-end rt_frtchord:empty_ext

%% userdevguide-begin rt_frtchord:next_hop
%% @doc Returns the next hop to contact for a lookup.
%% Beware: Sometimes called from dht_node
-spec next_hop_node(nodelist:neighborhood(), external_rt(), key()) -> succ | mynode().
next_hop_node(Neighbors, ERT, Id) ->
    case intervals:in(Id, nodelist:succ_range(Neighbors)) of
        true -> succ;
        _else -> RT = external_rt_get_tree(ERT),
                 RTSize = get_size_ext(ERT),
                 case util:gb_trees_largest_smaller_than(Id, RT) of
                     {value, _Id1, Node} -> Node;
                     nil when RTSize =:= 0 ->
                         node2mynode(nodelist:succ(Neighbors));
                     nil -> % forward to largest finger
                         {_Id, Node} = gb_trees:largest(RT),
                         Node
                 end
    end.

-spec next_hop(nodelist:neighborhood(), external_rt(), key()) -> succ | comm:mypid().
next_hop(Neighbors, ERT, Id) ->
    case next_hop_node(Neighbors, ERT, Id) of
        succ -> succ;
        %% prefer rt pid
        Node -> case pid_rt(Node) of
                    none -> pid_dht(Node);
                    Pid -> Pid
                end
    end.
%% userdevguide-end rt_frtchord:next_hop

%% @doc Return the succ
-spec succ(ERT::external_rt(), Neighbors::nodelist:neighborhood()) -> comm:mypid().
succ(ERT, Neighbors) ->
    Succ = nodelist:succ(Neighbors),
    ERTtree = external_rt_get_tree(ERT),
    %% prefer pid of rt_loop
    case gb_trees:lookup(node:id(Succ), ERTtree) of
        {value, {_Key, _IdVersion, PidDHT, none}} -> PidDHT;
        {value, {_Key, _IdVersion, _PidDHT, PidRT}} -> PidRT;
        none -> node:pidX(Succ)
    end.



%% userdevguide-begin rt_frtchord:export_rt_to_dht_node
%% @doc Converts the internal RT to the external RT used by the dht_node.
%% The external routing table is optimized to speed up next_hop/2. For this, it is
%%  only a gb_tree with keys being node ids and values being of type mynode().
-spec export_rt_to_dht_node_helper(rt()) -> external_rt().
export_rt_to_dht_node_helper(RT) ->
    % From each rt_entry, we extract only the field "node" and add it to the tree
    % under the node id. The source node is filtered.
    {RT#rt_t.nodes_in_ring, util:gb_trees_foldl(
            fun(_K, V, Acc) ->
                    case entry_type(V) of
                        source -> Acc;
                        _Else -> Node = rt_entry_node(V),
                            gb_trees:enter(id(Node), Node, Acc)
                    end
            end, gb_trees:empty(),get_rt_tree(RT))}.

-spec export_rt_to_dht_node(rt(), Neighbors::nodelist:neighborhood()) -> external_rt().
export_rt_to_dht_node(RT, _Neighbors) ->
    export_rt_to_dht_node_helper(RT).
%% userdevguide-end rt_frtchord:export_rt_to_dht_node

%% userdevguide-begin rt_frtchord:to_list
%% @doc Converts the external representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
-spec to_list(dht_node_state:state()) -> [{Id::key(), DHTPid::comm:mypid()}].
to_list(State) ->
    ERT = external_rt_get_tree(dht_node_state:get(State, rt)),
    MyNodeId = dht_node_state:get(State, node_id),
    L1 = util:gb_trees_foldl(fun(Id, {Id, _IdV, PidDHT, _PidRT}, Acc) ->
                                     [{Id, PidDHT}|Acc]
                             end, [], ERT),
    lists:usort(fun({AId, _APid}, {BId, _PPid}) ->
                        nodelist:succ_ord_id(AId, BId, MyNodeId)
                end, L1).

%% @doc Converts the internal representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
-spec internal_to_list(rt()) -> [mynode()].
internal_to_list(#rt_t{} = RT) ->
    SourceNode = get_source_node(RT),
    ListOfNodes = [rt_entry_node(N) || N <- gb_trees:values(get_rt_tree(RT))],
    sorted_nodelist(ListOfNodes, id(rt_entry_node(SourceNode)))
    .

% @doc Helper to do the actual work of converting a list of mynode() records
%      to list beginning with the source node and wrapping around at 0
-spec sorted_nodelist(ListOfNodes::[mynode()], SourceNode::key()) -> [mynode()].
sorted_nodelist(ListOfNodes, SourceNode) ->
    % sort
    Sorted = lists:sort(fun(A, B) -> id(A) =< id(B) end,
        ListOfNodes),
    % rearrange elements: all until the source node must be attached at the end
    {Front, Tail} = lists:splitwith(fun(N) -> id(N) =< SourceNode end, Sorted),
    Tail ++ Front
    .
%% userdevguide-end rt_frtchord:to_list

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% FRT specific algorithms and functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Filter one element from a set of nodes. Do it in a way that filters such a node
% that the resulting routing table is the best one under all _possible_ routing tables.

-spec entry_filtering(rt()) -> rt().
entry_filtering(#rt_t{} = RT) ->
    entry_filtering(RT, allowed_nodes(RT)).

-spec entry_filtering(rt(),[#rt_entry{type :: 'normal'}]) -> rt().
entry_filtering(RT, []) -> RT; % only sticky entries and the source node given; nothing to do
entry_filtering(RT, [_|_] = AllowedNodes) ->
    Spacings = [
        begin PredNode = predecessor_node(RT,Node),
              {Node, spacing(Node, RT) + spacing(PredNode, RT)}
        end || Node <- AllowedNodes],
    % remove the element with the smallest canonical spacing range between its predecessor
    % and its successor. TODO beware of numerical errors!
    {FilterEntry, _Spacing} = hd(lists:sort(
            fun ({_,SpacingA}, {_, SpacingB})
                -> SpacingA =< SpacingB
            end, Spacings)
    ),
    FilteredNode = rt_entry_node(FilterEntry),
    entry_delete(id(FilteredNode), RT)
    .

% @doc Delete an entry from the routing table
-spec entry_delete(EntryKey::key(), RT::rt()) -> RefinedRT::rt().
entry_delete(EntryKey, RT) ->
    Tree = gb_trees:delete(EntryKey, get_rt_tree(RT)),
    Node = rt_get_node(EntryKey, RT),

    % update affected routing table entries (adjacent fingers)
    {PredId, SuccId} = adjacent_fingers(Node),
    Pred = rt_get_node(PredId, RT),
    Succ = rt_get_node(SuccId, RT),

    UpdatedTree = case PredId =:= SuccId of
        true ->
            Entry = set_adjacent_fingers(Pred, PredId, PredId),
            gb_trees:enter(PredId, Entry, Tree);
        false ->
            NewPred = set_adjacent_succ(Pred, SuccId),
            NewSucc = set_adjacent_pred(Succ, PredId),
            gb_trees:enter(PredId, NewPred,
                gb_trees:enter(SuccId, NewSucc, Tree))
    end,
    % Note: We don't update the subscription here, as it is unclear at this point wether
    % the node died and the FD informed us on that or if the node is alive and was
    % filtered due to a full routing table. If the node died, the FD
    % already removed the subscription.

    RT#rt_t{nodes=UpdatedTree}
    .

% @doc Convert a sticky entry to a normal node
-spec sticky_entry_to_normal_node(EntryKey::key(), RT::rt()) -> rt().
sticky_entry_to_normal_node(EntryKey, RT) ->
    Node = #rt_entry{type=sticky} = rt_get_node(EntryKey, RT),
    NewNode = Node#rt_entry{type=normal},
    UpdatedTree = gb_trees:enter(EntryKey, NewNode, get_rt_tree(RT)),
    RT#rt_t{nodes=UpdatedTree}.


-spec rt_entry_from(Node::mynode(), Type::entry_type(),
                    PredId::key(), SuccId::key()) -> rt_entry().
rt_entry_from(Node, Type, PredId, SuccId) ->
    #rt_entry{node=Node , type=Type , adjacent_fingers={PredId, SuccId},
             custom=rt_entry_info(Node, Type, PredId, SuccId)}.

% @doc Create an rt_entry and return the entry together with the Pred und Succ node, where
% the adjacent fingers are changed for each node.
-spec create_entry(Node::mynode(), Type::entry_type(), RT::rt()) ->
    {rt_entry(), rt_entry(), rt_entry()}.
create_entry(Node, Type, RT) ->
    NodeId = id(Node),
    Tree = get_rt_tree(RT),
    FirstNode = id(Node) =:= get_source_id(RT),
    case util:gb_trees_largest_smaller_than(NodeId, Tree)of
        nil when FirstNode -> % this is the first entry of the RT
            NewEntry = rt_entry_from(Node, Type, NodeId, NodeId),
            {NewEntry, NewEntry, NewEntry};
        nil -> % largest finger
            {_PredId, Pred} = gb_trees:largest(Tree),
            get_adjacent_fingers_from(Pred, Node, Type, RT);
        {value, _PredId, Pred} ->
            get_adjacent_fingers_from(Pred, Node, Type, RT)
    end.

% Get the tuple of adjacent finger ids with Node being in the middle:
% {Predecessor, Node, Successor}
-spec get_adjacent_fingers_from(Pred::rt_entry(), Node::mynode(),
    Type::entry_type(), RT::rt()) -> {rt_entry(), rt_entry(), rt_entry()}.
get_adjacent_fingers_from(Pred, Node, Type, RT) ->
    PredId = entry_nodeid(Pred),
    Succ = successor_node(RT, Pred),
    SuccId = entry_nodeid(Succ),
    NodeId = id(Node),
    NewEntry = rt_entry_from(Node, Type, PredId, SuccId),
    case PredId =:= SuccId of
        false ->
            {set_adjacent_succ(Pred, NodeId),
             NewEntry,
             set_adjacent_pred(Succ, NodeId)
            };
        true ->
            AdjacentNode = set_adjacent_fingers(Pred, NodeId, NodeId),
            {AdjacentNode, NewEntry, AdjacentNode}
    end
    .

% @doc Add a new entry to the routing table. A source node is only allowed to be added
% once.
-spec entry_learning(Entry::mynode(), Type::entry_type(), RT::rt()) -> RefinedRT::rt().
entry_learning(Entry, Type, RT) ->
    % only add the entry if it doesn't exist yet or if it is a sticky node. If its a
    % stickynode, RM told us about a new neighbour -> if the neighbour was already added
    % as a normal node, convert it to a sticky node now.
    case gb_trees:lookup(id(Entry), get_rt_tree(RT)) of
        none ->
            % change the type to 'sticky' if the node is between the neighbors of the source
            % node
            AdaptedType = case Type of
                sticky -> Type;
                source -> Type;
                normal ->
                    {Pred, Succ} = adjacent_fingers(get_source_node(RT)),
                    ShouldBeAStickyNode = case Pred =/= Succ of
                        true ->
                            case Pred =< Succ of
                                true ->
                                    Interval = intervals:new('[', Pred, Succ, ']'),
                                    intervals:in(id(Entry), Interval);
                                false ->
                                    Interval = intervals:new('[', Pred, 0, ']'),
                                    Interval2 = intervals:new('[', 0, Succ, ']'),
                                    intervals:in(id(Entry), Interval) orelse
                                        intervals:in(id(Entry), Interval2)
                            end;
                        false ->
                            % Only two nodes are existing in the ring (otherwise, Pred == Succ
                            % means there is a bug somewhere!). When two nodes are in the
                            % system, another third node will be either the successor or
                            % predecessor of the source node when added.
                            true
                    end,
                    case ShouldBeAStickyNode of
                        true -> sticky;
                        false -> Type
                    end
            end,

            Ns = {NewPred, NewNode, NewSucc} = create_entry(Entry, AdaptedType, RT),
            % - if the nodes are all the same, we entered the first node and thus only enter
            % a single node to the tree
            % - if pred and succ are the same, we enter the second node: add that node and
            % an updated pred
            % - else, add the new node and update succ and pred
            Nodes = case Ns of
                {NewNode, NewNode, NewNode} ->
                    gb_trees:enter(entry_nodeid(NewNode), NewNode, get_rt_tree(RT));
                {NewPred, NewNode, NewPred} ->
                    gb_trees:enter(entry_nodeid(NewNode), NewNode,
                            gb_trees:enter(entry_nodeid(NewPred), NewPred,
                                get_rt_tree(RT)));
                _Else ->
                    gb_trees:enter(entry_nodeid(NewSucc), NewSucc,
                        gb_trees:enter(entry_nodeid(NewNode), NewNode,
                            gb_trees:enter(entry_nodeid(NewPred), NewPred, get_rt_tree(RT))
                        )
                    )
            end,
            rt_set_nodes(RT, Nodes);
        {value, ExistingEntry} ->
            % Always update a sticky entry, as that information was send from ring
            % maintenance.
            case Type of
                sticky -> % update entry
                    StickyEntry = rt_get_node(id(Entry), RT),
                    Nodes = gb_trees:enter(id(Entry),
                        StickyEntry#rt_entry{type=sticky},
                        get_rt_tree(RT)),
                    rt_set_nodes(RT, Nodes);
                _ ->
                    Nodes = gb_trees:enter(rt_entry_id(ExistingEntry),
                                           rt_entry_set_node(ExistingEntry, Entry),
                                           get_rt_tree(RT)),
                    rt_set_nodes(RT, Nodes)
            end
    end.

% @doc Combines entry learning and entry filtering.
-spec entry_learning_and_filtering(mynode(), entry_type(), rt()) -> rt().
entry_learning_and_filtering(Entry, Type, RT) ->
    IntermediateRT = entry_learning(Entry, Type, RT),

    SizeOfRT = get_size_without_special_nodes(IntermediateRT),
    MaxEntries = maximum_entries(),
    case SizeOfRT > MaxEntries of
        true ->
            NewRT = entry_filtering(IntermediateRT),
            %% only delete the subscription if the newly added node was not filtered;
            %otherwise, there isn't a subscription yet
            case rt_lookup_node(id(Entry), NewRT) of
                none -> ok;
                {value, _RTEntry} -> update_fd(RT, NewRT)
            end,
            NewRT;
        false ->
            add_fd(IntermediateRT),
            IntermediateRT
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% RT and RT entry record accessors
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Get the source node of a routing table
-spec get_source_node(RT::rt()) -> rt_entry().
get_source_node(#rt_t{source=undefined}) -> erlang:error("routing table source unknown");
get_source_node(#rt_t{source=NodeId, nodes=Nodes}) ->
    case gb_trees:is_empty(Nodes) of
            false -> gb_trees:get(NodeId, Nodes);
            true  -> exit(rt_broken_tree_empty)
    end.

% @doc Get the id of the source node.
-spec get_source_id(RT::rt()) -> key().
get_source_id(#rt_t{source=NodeId}) -> NodeId.

% @doc Set the source node of a routing table
-spec set_source_node(SourceId::key(), RT::rt()) -> rt().
set_source_node(SourceId, #rt_t{source=undefined}=RT) ->
    RT#rt_t{source=SourceId}.

% @doc Get the gb_tree of the routing table containing its nodes
-spec get_rt_tree(Nodes::rt()) -> rt_t_tree().
get_rt_tree(#rt_t{nodes=Nodes}) -> Nodes.

% @doc Get all sticky entries of a routing table
-spec get_sticky_entries(rt()) -> [rt_entry()].
get_sticky_entries(#rt_t{nodes=Nodes}) ->
    util:gb_trees_foldl(fun(_K, #rt_entry{type=sticky} = E, Acc) -> [E|Acc];
                      (_,_,Acc) -> Acc
                  end, [], Nodes).

% @doc Check if a node exists in the routing table
-spec entry_exists(EntryKey::key(), rt()) -> boolean().
entry_exists(EntryKey, #rt_t{nodes=Nodes}) ->
    case gb_trees:lookup(EntryKey, Nodes) of
        none -> false;
        _Else -> true
    end.

% @doc Add an entry of a specific type to the routing table
-spec add_entry(Node::mynode(), 'normal' | 'source' | 'sticky', rt()) -> rt().
add_entry(Node, Type, RT) ->
    %% TODO Optimize? Atm linear traversal of the RT
    RTList = lists:map(fun rt_entry_node/1, rt_get_nodes(RT)),
    %% check if a node with an identical PidDHT (3 element in mynode tuple)
    %% already exists.
    case lists:keyfind(pid_dht(Node), 3, RTList) of
        false -> %% no node with same PidDHT exists -> add entry
            entry_learning_and_filtering(Node, Type, RT);
        OldNode -> %% node already exists in RT -> check id version
            %% Only nodes with different id but same PidDHT are found here, nodes
            %% with identical ids are already filtered out higher up. Nodes with
            %% different id but same DHTPid can occur during slides/jumps.
            case is_newer(Node, OldNode) of
                false -> %% ignore node w/ outdated id version
                    RT;
                true -> %% remove the old (outdated) entry before adding the new one
                    SourceNode = rt_entry_node(get_source_node(RT)),
                    {Id1, IdV1, PidDHT1, _PidRT1} = SourceNode,
                    {Id2, IdV2, PidDHT2, _PidRT2} = OldNode,
                    case {Id1, IdV1, PidDHT1} =:= {Id2, IdV2, PidDHT2} of
                        %% If the new entry is the source node of the current node,
                        %% we need to change the source node manually.
                        %% This can happen, when messages from the learning on
                        %% forwarding (of lookup messages) reach the node faster,
                        %% than messages from the RM, i.e. in this case, the node
                        %% learns its new Id from a rt_learn_node message, not the RM.
                        true ->
                            RT1 = entry_delete(id(OldNode), RT),
                            add_source_entry(Node, RT1#rt_t{source=undefined});
                        false ->
                            RT1 = entry_delete(id(OldNode), RT),
                            entry_learning_and_filtering(Node, Type, RT1)
                    end
            end
    end.


% @doc Add a sticky entry to the routing table
-spec add_sticky_entry(Entry::mynode(), rt()) -> rt().
add_sticky_entry(Entry, RT) -> add_entry(Entry, sticky, RT).

% @doc Add the source entry to the routing table
-spec add_source_entry(Entry::mynode(), rt()) -> rt().
add_source_entry(Entry, #rt_t{source=undefined} = RT) ->
    IntermediateRT = set_source_node(id(Entry), RT),
    % only learn; the RT must be empty, so there is no filtering needed afterwards
    NewRT = entry_learning(Entry, source, IntermediateRT),
    add_fd(NewRT),
    NewRT.

% @doc Add a normal entry to the routing table
-spec add_normal_entry(Entry::mynode(), rt()) -> rt().
add_normal_entry(Entry, RT) ->
    add_entry(Entry, normal, RT).

% @doc Get the inner mynode() of a rt_entry
-spec rt_entry_node(N::rt_entry()) -> mynode().
rt_entry_node(#rt_entry{node=N}) -> N.

% @doc Set the inner mynode() of a rt_entry
-spec rt_entry_set_node(Entry::rt_entry(), Node::mynode()) -> rt_entry().
rt_entry_set_node(#rt_entry{} = Entry, Node) -> Entry#rt_entry{node=Node}.

% @doc Get all nodes within the routing table
-spec rt_get_nodes(RT::rt()) -> [rt_entry()].
rt_get_nodes(RT) -> gb_trees:values(get_rt_tree(RT)).

% @doc Set the tree of nodes of the routing table.
-spec rt_set_nodes(RT :: rt(), Nodes::rt_t_tree()) -> rt().
rt_set_nodes(#rt_t{source=undefined}, _) -> erlang:error(source_node_undefined);
rt_set_nodes(#rt_t{} = RT, Nodes) -> RT#rt_t{nodes=Nodes}.

% @doc Set the size estimate of the ring
-spec rt_set_ring_size(RT::rt(), Size::unknown | float()) -> rt().
rt_set_ring_size(#rt_t{}=RT, Size) -> RT#rt_t{nodes_in_ring=Size}.

% @doc Get the ring size estimate from the external routing table
-spec external_rt_get_ring_size(RT::external_rt()) -> Size::float() | unknown.
external_rt_get_ring_size(RT)
  when element(1, RT) >= 0 orelse element(1, RT) == unknown ->
    element(1, RT).

% @doc Get the tree of an external rt
-spec external_rt_get_tree(RT::external_rt()) -> external_rt_t_tree().
external_rt_get_tree(RT) when is_tuple(RT) ->
    element(2, RT).

%% Get the node with the given Id. This function will crash if the node doesn't exist.
-spec rt_get_node(NodeId::key(), RT::rt()) -> rt_entry().
rt_get_node(NodeId, RT)  -> gb_trees:get(NodeId, get_rt_tree(RT)).

% @doc Similar to rt_get_node/2, but doesn't crash when the id doesn't exist
-spec rt_lookup_node(NodeId::key(), RT::rt()) -> {value, rt_entry()} | none.
rt_lookup_node(NodeId, RT) -> gb_trees:lookup(NodeId, get_rt_tree(RT)).

% @doc Get the id of a given node
-spec rt_entry_id(Entry::rt_entry()) -> key().
rt_entry_id(Entry) -> id(rt_entry_node(Entry)).

%% @doc Check if the given routing table entry is of the given entry type.
-spec entry_is_of_type(rt_entry(), Type::entry_type()) -> boolean().
entry_is_of_type(#rt_entry{type=Type}, Type) -> true;
entry_is_of_type(_,_) -> false.

%% @doc Check if the given routing table entry is a source entry.
-spec is_source(Entry::rt_entry()) -> boolean().
is_source(Entry) -> entry_is_of_type(Entry, source).

%% @doc Check if the given routing table entry is a sticky entry.
-spec is_sticky(Entry::rt_entry()) -> boolean().
is_sticky(Entry) -> entry_is_of_type(Entry, sticky).

-spec entry_type(Entry::rt_entry()) -> entry_type().
entry_type(Entry) -> Entry#rt_entry.type.

%% @doc Get the node id of a routing table entry
-spec entry_nodeid(Node::rt_entry()) -> key().
entry_nodeid(#rt_entry{node=Node}) -> id(Node).

% @doc Get the adjacent fingers from a routing table entry
-spec adjacent_fingers(rt_entry()) -> {key(), key()}.
adjacent_fingers(#rt_entry{adjacent_fingers=Fingers}) -> Fingers.

%% @doc Get the adjacent predecessor key() of the current node.
-spec adjacent_pred(rt_entry()) -> key().
adjacent_pred(#rt_entry{adjacent_fingers={Pred,_Succ}}) -> Pred.

%% @doc Get the adjacent successor key of the current node
-spec adjacent_succ(rt_entry()) -> key().
adjacent_succ(#rt_entry{adjacent_fingers={_Pred,Succ}}) -> Succ.

%% @doc Set the adjacent fingers of a node
-spec set_adjacent_fingers(rt_entry(), key(), key()) -> rt_entry().
set_adjacent_fingers(#rt_entry{} = Entry, PredId, SuccId) ->
    Entry#rt_entry{adjacent_fingers={PredId, SuccId}}.

%% @doc Set the adjacent successor of the finger
-spec set_adjacent_succ(rt_entry(), key()) -> rt_entry().
set_adjacent_succ(#rt_entry{adjacent_fingers={PredId, _Succ}} = Entry, SuccId) ->
    set_adjacent_fingers(Entry, PredId, SuccId).

%% @doc Set the adjacent predecessor of the finger
-spec set_adjacent_pred(rt_entry(), key()) -> rt_entry().
set_adjacent_pred(#rt_entry{adjacent_fingers={_Pred, SuccId}} = Entry, PredId) ->
    set_adjacent_fingers(Entry, PredId, SuccId).

%% @doc Get the adjacent predecessor rt_entry() of the given node.
-spec predecessor_node(RT::rt(), Node::rt_entry()) -> rt_entry().
predecessor_node(RT, Node) ->
    gb_trees:get(adjacent_pred(Node), get_rt_tree(RT)).

-spec successor_node(RT::rt(), Node::rt_entry()) -> rt_entry().
successor_node(RT, Node) ->
    try gb_trees:get(adjacent_succ(Node), get_rt_tree(RT)) catch
         error:function_clause -> exit('stale adjacent fingers')
    end.

-spec spacing(Node::rt_entry(), RT::rt()) -> float().
spacing(Node, RT) ->
    SourceNodeId = entry_nodeid(get_source_node(RT)),
    canonical_spacing(SourceNodeId, entry_nodeid(Node),
        adjacent_succ(Node)).

%% @doc Calculate the canonical spacing, which is defined as
%%  S_i = log_2(distance(SourceNode, SuccId) / distance(SourceNode, Node))
canonical_spacing(SourceId, NodeId, SuccId) ->
    util:log2(get_range(SourceId, SuccId) / get_range(SourceId, NodeId)).

% @doc Check that all entries in an rt are well connected by their adjacent fingers
-spec check_rt_integrity(RT::rt()) -> boolean().
check_rt_integrity(#rt_t{} = RT) ->
    Nodes = [id(N) || N <- internal_to_list(RT)],

    %  make sure that the entries are well-connected
    Currents = Nodes,
    Last = lists:last(Nodes),
    Preds = [Last| lists:filter(fun(E) -> E =/= Last end, Nodes)],
    Succs = tl(Nodes) ++ [hd(Nodes)],

    % for each 3-tuple of pred, current, succ, check if the RT obeys the fingers
    Checks = [begin Node = rt_get_node(C, RT),
                case adjacent_fingers(Node) of
                    {P, S} ->
                        true;
                    _Else ->
                        false
                end end || {P, C, S} <- lists:zip3(Preds, Currents, Succs)],
    lists:all(fun(X) -> X end, Checks).

%% userdevguide-begin rt_frtchord:wrap_message
%% @doc Wrap lookup messages.
%% For node learning in lookups, a lookup message is wrapped with the global Pid of the
-spec wrap_message(Key::key(), Msg::comm:message(), MyERT::external_rt(),
                   Neighbors::nodelist:neighborhood(),
                   Hops::non_neg_integer()) -> {'$wrapped', mynode(),
                                                comm:message()} | comm:message().
wrap_message(_Key, Msg, _ERT, Neighbors, 0) ->
    MyNode = nodelist:node(Neighbors),
    RTLoopPid = comm:make_global(pid_groups:get_my(routing_table)),
    {'$wrapped', {node:id(MyNode), node:id_version(MyNode), node:pidX(MyNode),
                  RTLoopPid}, Msg};

wrap_message(Key, {'$wrapped', Issuer, _} = Msg, ERT, Neighbors, 1) ->
    %% The reduction ratio is only useful if this is not the last hop
    case intervals:in(Key, nodelist:succ_range(Neighbors)) of
        true -> ok;
        false ->
            MyNode = nodelist:node(Neighbors),
            NextHop = next_hop_node(Neighbors, ERT, Key),
            SendMsg = case external_rt_get_ring_size(ERT) of
                unknown -> true;
                RingSize ->
                    FirstDist = get_range(id(Issuer), node:id(MyNode)),
                    TotalDist = get_range(id(Issuer), id(NextHop)),
                    % reduction ratio > optimal/convergent ratio?
                    1 - FirstDist / TotalDist >
                    case config:read(rt_frt_reduction_ratio_strategy) of
                        best_rt_reduction_ratio -> best_rt_reduction_ratio(RingSize);
                        convergent_rt_reduction_ratio -> convergent_rt_reduction_ratio(RingSize)
                    end
            end,

            case SendMsg of
                true -> send2rt(Issuer, {rt_learn_node, NextHop});
                false -> ok
            end
    end,

    learn_on_forward(Issuer, nodelist:node(Neighbors)),
    Msg;

wrap_message(_Key, {'$wrapped', Issuer, _} = Msg, _ERT, Neighbors, _Hops) ->
    learn_on_forward(Issuer, nodelist:node(Neighbors)),
    Msg.

best_rt_reduction_ratio(RingSize) ->
    1 - math:pow(1 / RingSize, 2 / (maximum_entries() - 1)).
convergent_rt_reduction_ratio(RingSize) ->
    1 - math:pow(1 / RingSize, 4 / (maximum_entries() - 2)).

-spec learn_on_forward(Issuer::mynode(), MyNode::node:node_type()) -> ok.
learn_on_forward(Issuer, MyNode) ->
    PidDHT = comm:make_local(node:pidX(MyNode)),
    case self() of
        PidDHT ->
            comm:send_local(pid_groups:get_my(routing_table), {rt_learn_node, Issuer});
        _else ->
            comm:send_local(self(), {rt_learn_node, Issuer})
    end.

%% userdevguide-end rt_frtchord:wrap_message

%% userdevguide-begin rt_frtchord:unwrap_message
%% @doc Unwrap lookup messages.
%% The Pid is retrieved and the Pid of the current node is sent to the retrieved Pid
-spec unwrap_message(Msg::comm:message(), State::dht_node_state:state()) -> comm:message().
unwrap_message({'$wrapped', _Issuer, UnwrappedMessage}, _State) -> UnwrappedMessage.
%% userdevguide-end rt_frtchord:unwrap_message

% @doc Check that the adjacent fingers of a RT are building a ring
-spec check_well_connectedness(RT::rt()) -> boolean().
check_well_connectedness(RT) ->
    Nodes = [N || {_, N} <- gb_trees:to_list(get_rt_tree(RT))],
    NodeIds = lists:sort([Id || {Id, _} <- gb_trees:to_list(get_rt_tree(RT))]),
    % traverse the adjacent fingers of the nodes and add each visited node to a list
    % NOTE: each node should only be visited once
    InitVisitedNodes = ordsets:from_list([{N, false} || N <- Nodes]),
    %% check forward adjacent fingers
    Visit = fun(Visit, Current, Visited, Direction) ->
            case ordsets:is_element({Current, false}, Visited) of
                true -> % node wasn't visited
                    AccVisited = ordsets:add_element({Current, true},
                        ordsets:del_element({Current, false}, Visited)),
                    Next = case Direction of
                        succ -> rt_get_node(adjacent_succ(Current), RT);
                        pred -> rt_get_node(adjacent_pred(Current), RT)
                    end,
                    Visit(Visit, Next, AccVisited, Direction);
                false ->
                    Filtered = ordsets:filter(fun({_, true}) -> true; (_) -> false end,
                        Visited),
                    lists:sort([entry_nodeid(N) || {N, true} <- ordsets:to_list(Filtered)])
            end
    end,
    Succs = Visit(Visit, get_source_node(RT), InitVisitedNodes, succ),
    Preds = Visit(Visit, get_source_node(RT), InitVisitedNodes, pred),
    try
        NodeIds = Succs,
        NodeIds = Preds,
        true
    catch
        _:_ -> false
    end
    .

%% @doc Transform a node:node_type() to mynode().
-spec node2mynode(Node::node:node_type()) -> mynode().
node2mynode(Node) -> node2mynode(Node, none).

%% @doc Transform a node:node_type() to mynode().
-spec node2mynode(Node::node:node_type(), PidRT::comm:mypid() | none) -> mynode().
node2mynode(Node, PidRT) ->
    {node:id(Node), node:id_version(Node), node:pidX(Node), PidRT}.

%% @doc Get the id from a mynode().
-spec id(Node::mynode()) -> key().
id({Id, _IdVersion, _PidDHT, _PidRT}) -> Id.

%% @doc Get the Pid from an mynode().
-spec pid_dht(Node::mynode()) -> comm:mypid().
pid_dht({_Id, _IdVersion, PidDHT, _PidRT}) -> PidDHT.

%% @doc Get the Pid from an mynode().
-spec pid_rt(Node::mynode()) -> comm:mypid() | none.
pid_rt({_Id, _IdVersion, _PidDHT, PidRT}) -> PidRT.

%% @doc Determines whether Node1 is a newer instance of Node2.
%%      Note: Both nodes need to share the same PidDHT, otherwise an exception of
%%      type 'throw' is thrown! PidRT is ignored for the comparison.
-spec is_newer(Node1::mynode(), Node2::mynode()) -> boolean().
is_newer(_Node1 = {Id1, IdVersion1, PidDHT, _PidRT1},
         _Node2 = {Id2, IdVersion2, PidDHT, _PidRT2}) ->
    if
        (IdVersion1 > IdVersion2) -> true;
        IdVersion1 < IdVersion2 -> false;
        Id1 =:= Id2 -> false;
        true -> throw('got two nodes with same IDversion but different ID')
    end.


%% @doc Send Msg to the routing table.
%%      Send directly if rt_loop pid is known, otherwise as group_member msg.
-spec send2rt(Node::mynode(), Msg::comm:message()) -> ok.
send2rt(Node, Msg) ->
    {Pid, NewMsg} = case pid_rt(Node) of
                        none ->
                            MsgNew = {?send_to_group_member, routing_table, Msg},
                            {pid_dht(Node), MsgNew};
                        PidDHT ->
                            {PidDHT, Msg}
                    end,
    comm:send(Pid, NewMsg).

%% print_debug(Case, OldNode, NewNode, ERT) ->
%%     OldPidDHT = comm:make_local(pid_dht(OldNode)),
%%     OldPidRT = comm:make_local(pid_rt(OldNode)),
%%     NewPidDHT = comm:make_local(pid_dht(NewNode)),
%%     NewPidRT = comm:make_local(pid_rt(NewNode)),
%%     log:pal("{~w, ~w} Update Neighbor: ~w. KnownNode: ~w. NewNode: ~w~n",
%%             [pid_groups:get_my(dht_node), self(), Case, {OldPidDHT, OldPidRT},
%%              {NewPidDHT, NewPidRT}]).

-ifdef(GFRT).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% GFRT-chord
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Additional information appended to an rt_entry()
-record(rt_entry_info, {group = other_dc :: other_dc | same_dc}).
-type rt_entry_info_t()::#rt_entry_info{}.


-spec allowed_nodes(RT::rt()) -> [rt_entry()].
allowed_nodes(RT) ->
    Source = get_source_node(RT),
    SourceId = rt_entry_id(Source),
    Nodes = rt_get_nodes(RT),

    % $E_{\bar{G}}$ and $E_{G}$
    {E_NG, E_G} = lists:partition(fun is_from_other_group/1, Nodes),

    % If $E_G = \emptyset$, we know that we can allow all nodes to be filtered.
    % Otherwise, check if $E_\text{leap} \neq \emptyset$.
    {OnlyNonGroupMembers, {E_a, E_b}} = case E_G of
        [] -> {true, {ignore, ignore}};
        [First|_] ->
            Predecessor = predecessor_node(RT, Source),
            FirstDist = get_range(SourceId, rt_entry_id(First)),

            % Compute $e_\alpha$, $e_\beta$ and the respective distances
            {{E_alpha, E_alphaDist}, {E_beta, _E_betaDist}} = lists:foldl(
                fun (Node, {{MinNode, MinDist}, {MaxNode, MaxDist}}) ->
                        NodeDist = get_range(SourceId, rt_entry_id(Node)),
                        NewE_alpha = case erlang:min(MinDist, NodeDist) of
                            MinDist -> {MinNode, MinDist};
                            _ -> {Node, NodeDist}
                        end,
                        NewE_beta = case erlang:max(MaxDist, NodeDist) of
                            MinDist -> {MaxNode, MaxDist};
                            _ -> {Node, NodeDist}
                        end,
                        {NewE_alpha, NewE_beta}
                end, {{First, FirstDist}, {First, FirstDist}}, E_G),

            % Is there any non-group entry $n$ such that $d(s, e_\alpha) \leq d(s, n)$ and
            % $n \neq s.pred$? The following line basically computes $E_leap$ and checks
            % if that set is empty.
            {lists:any(fun(P) when P =:= Predecessor -> false;
                         (N) -> get_range(SourceId, rt_entry_id(N)) >= E_alphaDist andalso
                            not is_sticky(N) andalso not is_source(N)
                      end, E_NG),
                  {E_alpha, E_beta}}
    end,

    if OnlyNonGroupMembers -> [N || N <- E_NG, not is_sticky(N),
                                               not is_source(N)];
       not OnlyNonGroupMembers andalso E_a =/= ignore andalso E_b =/= ignore ->
           [N || N <- Nodes, not is_sticky(N), not is_source(N),
                                               N =/= E_a,
                                               N =/= E_b]
    end.

-spec rt_entry_info(Node::mynode(), Type::entry_type(),
                    PredId::key(), SuccId::key()) -> rt_entry_info_t().
rt_entry_info(Node, _Type, _PredId, _SuccId) ->
    #rt_entry_info{group=case comm:get_ip(pid_dht(Node)) =:= comm:get_ip(comm:this()) of
            true -> same_dc;
            false -> other_dc
        end}.

%% Check if the given node belongs to another group of nodes
-spec is_from_other_group(rt_entry()) -> boolean().
is_from_other_group(Node) ->
    CustomInfo = Node#rt_entry.custom,
    CustomInfo#rt_entry_info.group =:= same_dc.

-else.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% FRT-chord
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Additional information appended to an rt_frt_helpers:rt_entry()
-type rt_entry_info_t() :: undefined.

-spec allowed_nodes(RT::rt()) -> [rt_entry()].
allowed_nodes(RT) ->
    [N || N <- rt_get_nodes(RT), not is_sticky(N) and not is_source(N)].

-spec rt_entry_info(Node::mynode(), Type::entry_type(),
                    PredId::key(), SuccId::key()) -> rt_entry_info_t().
rt_entry_info(_Node, _Type, _PredId, _SuccId) ->
    undefined.

-endif.

%% @doc Checks whether config parameters of the rt_frtchord/rt_gfrtchord
%%      process exist and are valid.
-spec frt_check_config() -> boolean().
frt_check_config() -> true.
