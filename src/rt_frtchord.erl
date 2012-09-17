% @copyright 2012 Zuse Institute Berlin

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
%% @doc A flexible routing table algorithm as presented in (Nagao, Shudo, 2011)
%% @end
%% @version $Id$

-module(rt_frtchord).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

-behaviour(rt_beh).
-include("scalaris.hrl").

% exports for unit tests
-export([check_rt_integrity/1]).

%% userdevguide-begin rt_frtchord:types
-type key_t() :: 0..16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF. % 128 bit numbers
-type external_rt_t() :: gb_tree().

% define the possible types of nodes in the routing table:
%  - normal nodes are nodes which have been added by entry learning
%  - source is the node at which this RT is
%  - sticky is a node which is not to be deleted with entry filtering
-type entry_type() :: normal | source | sticky.
-record(rt_entry, {
        node :: node:node_type()
        , type :: entry_type()
        , adjacent_fingers = {undefined, undefined} ::
        {key_t() | 'undefined', key_t() | 'undefined'}

    }).

-type(rt_entry() :: #rt_entry{}).

-record(rt_t, {
        source = undefined :: key_t() | undefined
        , nodes = gb_trees:empty() :: gb_tree()
    }).

-type(rt_t() :: #rt_t{}).

-type custom_message() :: {get_rt, SourcePID :: comm:mypid()}
                        | {get_rt_reply, RT::rt_t()}
                        | {trigger_random_lookup}
                        | {rt_get_node, From :: comm:mypid()}
                        | {rt_get_node_response, NewNode :: node:node_type()}
                        .

%% userdevguide-end rt_frtchord:types

% Note: must include rt_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("rt_beh.hrl").

% @doc Maximum number of entries in a routing table
-spec maximum_entries() -> non_neg_integer().
maximum_entries() -> 10.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Initialize the routing table. This function is allowed to send messages.
-spec init(nodelist:neighborhood()) -> rt().
init(Neighbors) -> 
    % trigger a random lookup after initializing the table
    comm:send_local(self(), {trigger_random_lookup}),

    % ask the successor node for its routing table
    Msg = {send_to_group_member, routing_table, {get_rt, comm:this()}},
    comm:send(node:pidX(nodelist:succ(Neighbors)), Msg),
    % create an initial RT consisting of the neighbors
    EmptyRT = add_source_entry(nodelist:node(Neighbors), #rt_t{}),
    remove_and_ping_entries(Neighbors, EmptyRT)
    .

%% @doc Hashes the key to the identifier space.
-spec hash_key(client_key()) -> key().
hash_key(Key) -> hash_key_(Key).

%% @doc Hashes the key to the identifier space (internal function to allow
%%      use in e.g. get_random_node_id without dialyzer complaining about the
%%      opaque key type).
-spec hash_key_(client_key()) -> key_t().
hash_key_(Key) ->
    <<N:128>> = erlang:md5(client_key_to_binary(Key)),
    N.
%% userdevguide-end rt_frtchord:hash_key

%% userdevguide-begin rt_frtchord:get_random_node_id
%% @doc Generates a random node id, i.e. a random 128-bit number.
-spec get_random_node_id() -> key().
get_random_node_id() ->
    case config:read(key_creator) of
        random -> hash_key_(randoms:getRandomString());
        random_with_bit_mask ->
            {Mask1, Mask2} = config:read(key_creator_bitmask),
            (hash_key_(randoms:getRandomString()) band Mask2) bor Mask1
    end.
%% userdevguide-end rt_frtchord:get_random_node_id

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_frtchord:init_stabilize
%% @doc Triggered by a new stabilization round, renews the routing table.
%% Check:
%% - if node id changed, just renew the complete table and maybe tell known nodes
%%  that something changed (async and optimistic -> if they don't care, we don't care)
%% - if pred/succ changed, update sticky entries
%% TODO: what is the difference to update?
-spec init_stabilize(nodelist:neighborhood(), rt()) -> rt().
init_stabilize(Neighbors, RT) -> 
    case node:id(nodelist:node(Neighbors)) =:= entry_nodeid(get_source_node(RT)) of
        true -> remove_and_ping_entries(Neighbors, RT) ;
        _Else -> % source node changed, replace the complete table
            init(Neighbors)
    end
    .


%% userdevguide-end rt_frtchord:init_stabilize

% Get the adjacent nodes. The source node is filtered.
-spec get_node_neighbors(nodelist:neighborhood()) -> set().
get_node_neighbors(Neighborhood) ->
    Source = nodelist:node(Neighborhood),
    % filter the source node and add other nodes to a set
    Filter = fun(Entry, Acc) -> case Entry of
                Source -> Acc;
                _Else -> sets:add_element(Entry, Acc) end
    end,

    lists:foldl(Filter,
        lists:foldl(Filter, sets:new(),
            nodelist:preds(Neighborhood)),
        nodelist:succs(Neighborhood))
    .

%% @doc Remove and ping entries
%% This function removes sticky nodes from the RT which aren't in the neighborhood
%% anymore. Additionally, it pings all entries to make sure that the IDs of the entries
%% haven't changed.
-spec remove_and_ping_entries(NewNeighbors :: nodelist:neighborhood(),
                              RT :: rt()) -> rt().
remove_and_ping_entries(NewNeighbors, RT) ->
    OldStickyNodes = sets:from_list(lists:map(fun rt_entry_node/1,
            get_sticky_entries(RT))),
    NewStickyNodes = get_node_neighbors(NewNeighbors),
    DeadStickyNodes = sets:subtract(OldStickyNodes, NewStickyNodes),
    DeadStickyNodesIds = util:sets_map(fun node:id/1, DeadStickyNodes),
    ToBeAddedNodes = sets:subtract(NewStickyNodes, OldStickyNodes),
    % filter dead nodes and add sticky nodes
    FilteredRT = lists:foldl(fun entry_delete/2, RT, DeadStickyNodesIds),
    NewRT = sets:fold(fun add_sticky_entry/2, FilteredRT, ToBeAddedNodes),
    %% TODO ping entries
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
            {ok, remove_and_ping_entries(NewNeighbors, OldRT)}
            ;
        _Else -> % source node changed, rebuild the complete table
            {trigger_rebuild, OldRT} % XXX this triggers init_stabilize
    end
    .
%% userdevguide-end rt_frtchord:update

%% userdevguide-begin rt_frtchord:filter_dead_node
%% @doc Removes dead nodes from the routing table (rely on periodic
%%      stabilization here).
-spec filter_dead_node(rt(), comm:mypid()) -> rt().
filter_dead_node(RT, DeadPid) -> 
    % find the node id of DeadPid and delete it from the RT
    [Node] = [N || N <- internal_to_list(RT), node:pidX(N) =:= DeadPid],
    delete_fd(node:pidX(Node)),
    entry_delete(node:id(Node), RT)
    .
%% userdevguide-end rt_frtchord:filter_dead_node

%% userdevguide-begin rt_frtchord:to_pid_list
%% @doc Returns the pids of the routing table entries.
-spec to_pid_list(rt()) -> [comm:mypid()].
to_pid_list(RT) -> [node:pidX(N) || N <- internal_to_list(RT)].
%% userdevguide-end rt_frtchord:to_pid_list

%% userdevguide-begin rt_frtchord:get_size
%% @doc Returns the size of the routing table.
-spec get_size(rt() | external_rt()) -> non_neg_integer().
get_size(#rt_t{} = RT) -> gb_trees:size(get_rt_tree(RT));
get_size(RT) -> gb_trees:size(RT). % size of external rt
%% userdevguide-end rt_frtchord:get_size

%% userdevguide-begin rt_frtchord:n
%% @doc Returns the size of the address space.
-spec n() -> integer().
n() -> n_().
%% @doc Helper for n/0 to make dialyzer happy with internal use of n/0.
-spec n_() -> 16#100000000000000000000000000000000.
n_() -> 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF + 1.
%% userdevguide-end rt_frtchord:n

%% @doc Keep a key in the address space. See n/0.
-spec normalize(Key::key_t()) -> key_t().
normalize(Key) -> Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.

%% @doc Gets the number of keys in the interval (Begin, End]. In the special
%%      case of Begin==End, the whole key range as specified by n/0 is returned.
-spec get_range(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE) -> number().
get_range(Begin, End) -> get_range_(Begin, End).

%% @doc Helper for get_range/2 to make dialyzer happy with internal use of
%%      get_range/2 in the other methods, e.g. get_split_key/3.
-spec get_range_(Begin::key_t(), End::key_t() | ?PLUS_INFINITY_TYPE) -> number().
get_range_(Begin, Begin) -> n_(); % I am the only node
get_range_(?MINUS_INFINITY, ?PLUS_INFINITY) -> n_(); % special case, only node
get_range_(Begin, End) when End > Begin -> End - Begin;
get_range_(Begin, End) when End < Begin -> (n_() - Begin) + End.

%% @doc Gets the key that splits the interval (Begin, End] so that the first
%%      interval will be (Num/Denom) * range(Begin, End). In the special case of
%%      Begin==End, the whole key range is split.
%%      Beware: SplitFactor must be in [0, 1]; the final key will be rounded
%%      down and may thus be Begin.
-spec get_split_key(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE,
                    SplitFraction::{Num::non_neg_integer(), Denom::pos_integer()}) -> key().
get_split_key(Begin, _End, {Num, _Denom}) when Num == 0 -> Begin;
get_split_key(_Begin, End, {Num, Denom}) when Num == Denom -> End;
get_split_key(Begin, End, {Num, Denom}) ->
    normalize(Begin + (get_range_(Begin, End) * Num) div Denom).

%% userdevguide-begin rt_frtchord:get_replica_keys
%% @doc Returns the replicas of the given key.
-spec get_replica_keys(key()) -> [key()].
get_replica_keys(Key) ->
    [Key,
     Key bxor 16#40000000000000000000000000000000,
     Key bxor 16#80000000000000000000000000000000,
     Key bxor 16#C0000000000000000000000000000000
    ].
%% userdevguide-end rt_frtchord:get_replica_keys

%% userdevguide-begin rt_frtchord:dump
%% @doc Dumps the RT state for output in the web interface.
-spec dump(RT::rt()) -> KeyValueList::[{Index::string(), Node::string()}].
dump(RT) -> [{"0", webhelpers:safe_html_string("~p", [RT])}].
%% userdevguide-end rt_frtchord:dump

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
        end.

%% @doc Generate a random key from the pdf as defined in (Nagao, Shudo, 2011)
% TODO unit test that the generator doesn't generate keys from outside ?MINUS_INFINITY
% to ?PLUS_INFINITY
%% TODO
%I floor the key for now; the key generator should return ints, but returns
%float. It is currently unclear if this is a bug in the original paper by Nagao and
%Shudo. Using erlang:trunc/1 should be enough for flooring, as X >= 0
% TODO using the remainder might destroy the CDF. why can X > 2^128?
-spec get_random_key_from_generator(SourceNodeId :: key(),
                                    PredId :: key(),
                                    SuccId :: key()
                                   ) -> key().
get_random_key_from_generator(SourceNodeId, PredId, SuccId) ->
    Rand = random:uniform(),
    X = SourceNodeId + get_range(SourceNodeId, SuccId) *
        math:pow(get_range(SourceNodeId, PredId) /
                    get_range(SourceNodeId, SuccId),
                    Rand
                ),
    erlang:trunc(X) rem 16#100000000000000000000000000000000
    .

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
    %current successor. We just have to make sure that the merged entries are valid.
    LocalRT = rt_loop:get_rt(State),
    NewRT = case LocalRT =/= RT of
        true ->
            % - add each entry from the other RT if it doesn't already exist
            % - entries to be added have to be normal entries as RM invokes adding sticky
            % nodes
            util:gb_trees_foldl(
                fun(Key, Entry, Acc) ->
                        %% TODO send an async message to check if entry is valid; if it
                        %isn't, delete it later
                        case entry_exists(Key, Acc) of
                            true -> Acc;
                            false -> add_normal_entry(rt_entry_node(Entry), Acc)
                        end
                end,
                LocalRT, get_rt_tree(RT)
            )
            ;

        false -> LocalRT
    end,
    update_fd(LocalRT, NewRT),
    rt_loop:set_rt(State, NewRT)
    ;

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
    Interval = config:read(active_learning_lookup_interval),
    msg_delay:send_local(Interval, self(), {trigger_random_lookup}),

    api_dht_raw:unreliable_lookup(Key, {send_to_group_member, routing_table,
                                        {rt_get_node, comm:this()}}),
    State
    ;

handle_custom_message({rt_get_node, From}, State) ->
    MyNode = nodelist:node(rt_loop:get_neighb(State)),
    comm:send(From, {rt_learn_node, MyNode}),
    State
    ;

handle_custom_message({rt_learn_node, NewNode}, State) ->
    OldRT = rt_loop:get_rt(State),
    NewRT = case rt_lookup_node(node:id(NewNode), OldRT) of
        none -> RT = add_normal_entry(NewNode, OldRT),
                check(OldRT, RT, rt_loop:get_neighb(State), true),
                RT;
        _Else ->  OldRT
    end,

    rt_loop:set_rt(State, NewRT)
    ;

handle_custom_message(_Message, _State) -> unknown_event.
%% userdevguide-end rt_frtchord:handle_custom_message

%% userdevguide-begin rt_frtchord:check
%% @doc Notifies the dht_node and failure detector if the routing table changed.
%%      Provided for convenience (see check/5).
-spec check(OldRT::rt(), NewRT::rt(), Neighbors::nodelist:neighborhood(),
            ReportToFD::boolean()) -> ok.
check(OldRT, NewRT, Neighbors, ReportToFD) ->
    check(OldRT, NewRT, Neighbors, Neighbors, ReportToFD).

%% @doc Notifies the dht_node if the (external) routing table changed.
%%      Also updates the failure detector if ReportToFD is set.
%%      Note: the external routing table only changes if the internal RT has
%%      changed.
-spec check(OldRT::rt(), NewRT::rt(), OldNeighbors::nodelist:neighborhood(),
            NewNeighbors::nodelist:neighborhood(), ReportToFD::boolean()) -> ok.
check(OldRT, NewRT, OldNeighbors, NewNeighbors, ReportToFD) ->
    % if the routing tables haven't changed and the successor/predecessor haven't changed
    % as well, do nothing
    case OldRT =:= NewRT andalso 
         nodelist:succ(OldNeighbors) =:= nodelist:succ(NewNeighbors) andalso
         nodelist:pred(OldNeighbors) =:= nodelist:pred(NewNeighbors) of
        true -> ok;
        _Else -> % update the exported routing table
            Pid = pid_groups:get_my(dht_node),
            case Pid of
                failed -> ok;
                _E     -> RTExt = export_rt_to_dht_node(NewRT, NewNeighbors),
                          comm:send_local(Pid, {rt_update, RTExt})
            end,
            % update failure detector:
            case ReportToFD of
                true -> update_fd(OldRT, NewRT);
                _Else -> ok
            end
    end
    .


%% @doc Set up a set of subscriptions from a routing table
-spec add_fd(RT :: rt())  -> ok.
add_fd(#rt_t{} = RT) ->
    NewPids = to_pid_list(RT),
    fd:subscribe(NewPids).

%% @doc Update subscriptions
-spec update_fd(OldRT :: rt(), NewRT :: rt()) -> ok.
update_fd(#rt_t{} = OldRT, #rt_t{} = NewRT) ->
    OldPids = to_pid_list(OldRT),
    NewPids = to_pid_list(NewRT),
    fd:update_subscriptions(OldPids, NewPids).

%% @doc Delete a subscription
%-spec delete_fd(EntryPid :: pid()) -> ok.
delete_fd(EntryPid) -> fd:unsubscribe(EntryPid).

%% userdevguide-end rt_frtchord:check

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_frtchord:empty_ext
-spec empty_ext(nodelist:neighborhood()) -> external_rt().
empty_ext(_Neighbors) -> gb_trees:empty().
%% userdevguide-end rt_frtchord:empty_ext

%% userdevguide-begin rt_frtchord:next_hop
%% @doc Returns the next hop to contact for a lookup.
-spec next_hop(dht_node_state:state(), key()) -> comm:mypid().
next_hop(State, Id) ->
    Neighbors = dht_node_state:get(State, neighbors),
    case intervals:in(Id, nodelist:succ_range(Neighbors)) of
        true -> node:pidX(nodelist:succ(Neighbors));
        _ ->
            % check routing table:
            RT = dht_node_state:get(State, rt),
            RTSize = get_size(RT),
            NodeRT = case util:gb_trees_largest_smaller_than(Id, RT) of
                {value, _Key, N} -> N;
                nil when RTSize =:= 0 -> nodelist:succ(Neighbors);
                nil -> % forward to largest finger
                    {_Key, N} = gb_trees:largest(RT),
                    N
            end,
            FinalNode =
                case RTSize < config:read(rt_size_use_neighbors) of
                    false -> NodeRT;
                    _     -> % check neighborhood:
                             nodelist:largest_smaller_than(Neighbors, Id, NodeRT)
                end,
            node:pidX(FinalNode)
    end.
%% userdevguide-end rt_frtchord:next_hop

%% userdevguide-begin rt_frtchord:export_rt_to_dht_node
%% @doc Converts the internal RT to the external RT used by the dht_node.
%% The external routing table is optimized to speed up ?RT:next_hop/2. For this, it is
%%  only a gb_tree with keys being node ids and values being of type node:node_type().
%% TODO is it ok to ignore the neighbors?
-spec export_rt_to_dht_node(rt(), Neighbors::nodelist:neighborhood()) -> external_rt().
export_rt_to_dht_node(InternalRT, _Neighbors) ->
    % From each rt_entry, we extract only the field "node" and add it to the tree
    % under the node id. The source node is filtered.
    OldTree = get_rt_tree(InternalRT),
    RTExt = util:gb_trees_foldl(fun(_K, V, Acc) ->
                case entry_type(V) of
                    source -> Acc;
                    _Else -> Node = rt_entry_node(V),
                             gb_trees:enter(node:id(Node), Node, Acc)
                end
        end,
        gb_trees:empty(), OldTree),
    RTExt
    .
%% userdevguide-end rt_frtchord:export_rt_to_dht_node

%% userdevguide-begin rt_frtchord:to_list
%% @doc Converts the external representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
-spec to_list(dht_node_state:state()) -> nodelist:snodelist().
to_list(State) -> % match external RT
    RT = dht_node_state:get(State, rt),
    SourceNode = dht_node_state:get(State, node_id),
    sorted_nodelist(gb_trees:values(RT), SourceNode)
    .

%% @doc Converts the internal representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
-spec internal_to_list(rt()) -> nodelist:snodelist().
internal_to_list(#rt_t{} = RT) ->
    SourceNode = get_source_node(RT),
    ListOfNodes = [rt_entry_node(N) || N <- gb_trees:values(get_rt_tree(RT))],
    sorted_nodelist(ListOfNodes, node:id(rt_entry_node(SourceNode)))
    .

% @doc Helper to do the actual work of converting a list of node:node_type() records
% to a sorted list
-spec sorted_nodelist(nodelist:snodelist(), SourceNode::key()) -> nodelist:snodelist().
sorted_nodelist(ListOfNodes, SourceNode) ->
    % sort
    Sorted = lists:sort(fun(A, B) -> node:id(A) =< node:id(B) end,
        ListOfNodes),
    % rearrange elements: all until the source node must be attached at the end
    {Front, Tail} = lists:splitwith(fun(N) -> node:id(N) =< SourceNode end, Sorted),
    % TODO inefficient -> reimplement with foldl (combine with splitwith)
    Tail ++ Front
    .
%% userdevguide-end rt_frtchord:to_list

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% FRT specific algorithms and functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Filter one element from a set of nodes. Do it in a way that filters such a node
% that the resulting routing table is the best one under all _possible_ routing tables.
-spec entry_filtering(rt(),[#rt_entry{type :: 'normal'}]) -> rt().
entry_filtering(RT, []) -> RT; % only sticky entries and the source node given; nothing to do
entry_filtering(RT, [_|_] = AllowedNodes) ->
    % XXX ensure that the allowed nodes are actually contained in the routing table
    false = lists:any(fun(none) -> true; (_) -> false end,
            [rt_lookup_node(entry_nodeid(N), RT) || N
                <- AllowedNodes]),

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
    NewRT = entry_delete(entry_nodeid(FilterEntry), RT),

    %% XXX Assertion
    OldSourceNode = get_source_node(RT),
    OldSourceNode = get_source_node(NewRT),

    NewRT
    .

% @doc Delete an entry from the routing table
-spec entry_delete(EntryKey :: key(), RT :: rt()) -> RefinedRT :: rt().
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

    RT#rt_t{nodes=UpdatedTree}
    .

% @doc Create an rt_entry and return the entry together with the Pred und Succ node, where
% the adjacent fingers are changed for each node.
-spec create_entry(Node :: node:node_type(), Type :: entry_type(), RT :: rt()) ->
    {rt_entry(), rt_entry(), rt_entry()}.
create_entry(Node, Type, RT) ->
    NodeId = node:id(Node),
    Tree = get_rt_tree(RT),
    % search the adjacent fingers using the routing table
    PredLookup = util:gb_trees_largest_smaller_than(NodeId, Tree),
    RTSize = get_size(RT),
    case PredLookup of
        nil when RTSize =:= 0 -> % this is the first entry of the RT
            NewEntry = #rt_entry{
                node=Node,
                type=Type,
                adjacent_fingers={NodeId, NodeId}
            },
            {NewEntry, NewEntry, NewEntry}
            ;
        nil -> % largest finger
            {PredId, Pred} = gb_trees:largest(Tree),
            get_adjacent_fingers_from(PredId, Pred, Node, Type, RT)
            ;
        {value, PredId, Pred} ->
            get_adjacent_fingers_from(PredId, Pred, Node, Type, RT)
    end
    .

% Get the tuple of adjacent finger ids with Node being in the middle:
% {Predecessor, Node, Successor}
get_adjacent_fingers_from(PredId, Pred, Node, Type, RT) ->
    Succ = successor_node(RT, Pred),
    SuccId = entry_nodeid(Succ),
    NodeId = node:id(Node),
    % construct the node
    NewEntry = #rt_entry{
        node=Node,
        type=Type,
        adjacent_fingers={PredId, entry_nodeid(Succ)}
    },
    case PredId =:= SuccId of
        false -> {set_adjacent_succ(Pred, NodeId),
                NewEntry,
                set_adjacent_pred(Succ, NodeId)
            };
        true -> {set_adjacent_fingers(Pred, NodeId, NodeId),
                NewEntry,
                set_adjacent_fingers(Succ, NodeId, NodeId)
            }
    end
    .

% @doc Add a new entry to the routing table. A source node is only allowed to be added
% once.
-spec entry_learning(Entry :: node:node_type(), Type :: entry_type(), RT :: rt()) -> RefinedRT :: rt().
entry_learning(Entry, Type, RT) -> 
    % only add the entry if it doesn't exist yet
    case gb_trees:lookup(node:id(Entry), get_rt_tree(RT)) of
        none ->
            Ns = {NewPred, NewNode, NewSucc} = create_entry(Entry, Type, RT),
            % if the nodes are all the same, we entered the first node and thus only enter
            % a single node to the tree
            Nodes = case Ns of
                {NewNode, NewNode, NewSucc} ->
                    gb_trees:enter(entry_nodeid(NewSucc), NewSucc, get_rt_tree(RT));
                    _Else ->
                        gb_trees:enter(entry_nodeid(NewSucc), NewSucc,
                            gb_trees:enter(entry_nodeid(NewNode), NewNode,
                                gb_trees:enter(entry_nodeid(NewPred), NewPred, get_rt_tree(RT))
                            )
                        )
                end,
            rt_set_nodes(RT, Nodes);
        _else -> RT
    end
    .

% @doc Combines entry learning and entry filtering.
-spec entry_learning_and_filtering(node:node_type(), entry_type(), rt()) -> rt().
entry_learning_and_filtering(Entry, Type, RT) ->
    IntermediateRT = entry_learning(Entry, Type, RT),

    SizeOfRT = get_size(IntermediateRT),
    MaxEntries = maximum_entries(),
    case SizeOfRT >= MaxEntries of
        true ->
            AllowedNodes = [N || N <- gb_trees:values(get_rt_tree(IntermediateRT)),
                not is_sticky(N) and not is_source(N)],
            NewRT = entry_filtering(IntermediateRT, AllowedNodes),
            %% only delete the subscription if not the newly added node was filtered;
            %otherwise, there isn't a subscription yet
            case rt_lookup_node(node:id(Entry), NewRT) of
                none -> % the newly added node was filtered; do nothing
                    ok;
                _Else ->
                    update_fd(RT, NewRT)
            end,
            NewRT
            ;
        false -> IntermediateRT
    end
    .

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% RT and RT entry record accessors
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Get the source node of a routing table
-spec get_source_node(RT :: rt()) -> rt_entry().
get_source_node(#rt_t{source=undefined}) -> erlang:error("routing table source unknown");
get_source_node(#rt_t{source=NodeId, nodes=Nodes}) -> gb_trees:get(NodeId, Nodes).

% @doc Set the source node of a routing table
-spec set_source_node(SourceId :: key(), RT :: rt()) -> rt().
set_source_node(SourceId, #rt_t{source=undefined}=RT) ->
    RT#rt_t{source=SourceId}.

% @doc Get the gb_tree of the routing table containing its nodes
-spec get_rt_tree(Nodes::rt()) -> gb_tree().
get_rt_tree(#rt_t{nodes=Nodes}) -> Nodes.

% @doc Get all sticky entries of a routing table
-spec get_sticky_entries(rt()) -> [rt_entry()].
get_sticky_entries(#rt_t{nodes=Nodes}) ->
    util:gb_trees_foldl(fun(_K, #rt_entry{type=sticky} = E, Acc) -> [E|Acc];
                      (_,_,Acc) -> Acc
                  end, [], Nodes)
    .

% @doc Check if a node exists in the routing table
-spec entry_exists(EntryKey :: key(), rt()) -> boolean().
entry_exists(EntryKey, #rt_t{nodes=Nodes}) ->
    case gb_trees:lookup(EntryKey, Nodes) of
        none -> false;
        _Else -> true
    end.

% @doc Add an entry of a specific type to the routing table
%-spec add_entry(Node :: node:node_type(), Type :: entry_type(), RT :: rt()) -> rt().
-spec add_entry(node:node_type(),'normal' | 'source' | 'sticky',rt()) -> rt().
add_entry(Node, Type, RT) ->
    NewRT = entry_learning_and_filtering(Node, Type, RT),
    update_fd(RT, NewRT),
    NewRT
    .

% @doc Add a sticky entry to the routing table
-spec add_sticky_entry(Entry :: node:node_type(), rt()) -> rt().
add_sticky_entry(Entry, RT) ->
   add_entry(Entry, sticky, RT)
   .

% @doc Add the source entry to the routing table
-spec add_source_entry(Entry :: node:node_type(), rt()) -> rt().
add_source_entry(Entry, #rt_t{source=undefined} = RT) ->
    IntermediateRT = set_source_node(node:id(Entry), RT),
    % only learn; the RT must be empty, so there is no filtering needed afterwards
    NewRT = entry_learning(Entry, source, IntermediateRT),
    %% TODO is this needed? Maybe we don't need a subscription.
    add_fd(NewRT),
    NewRT
    .

% @doc Add a normal entry to the routing table
-spec add_normal_entry(Entry :: node:node_type(), rt()) -> rt().
add_normal_entry(Entry, RT) ->
    add_entry(Entry, normal, RT)
    .

% @doc Get the inner node:node_type() of a rt_entry
-spec rt_entry_node(N :: rt_entry()) -> node:node_type().
rt_entry_node(#rt_entry{node=N}) -> N.

% @doc Set the treeof nodes of the routing table.
-spec rt_set_nodes(RT :: rt(), Nodes :: gb_tree()) -> rt().
rt_set_nodes(#rt_t{source=undefined}, _) -> erlang:error(source_node_undefined);
rt_set_nodes(#rt_t{} = RT, Nodes) -> RT#rt_t{nodes=Nodes}.

%% Get the node with the given Id. This function will crash if the node doesn't exist.
-spec rt_get_node(NodeId :: key(), RT :: rt()) -> rt_entry().
rt_get_node(NodeId, RT)  -> gb_trees:get(NodeId, get_rt_tree(RT)).

-spec rt_lookup_node(NodeId :: key(), RT :: rt()) -> {value, rt_entry()} | nil.
rt_lookup_node(NodeId, RT) -> gb_trees:lookup(NodeId, get_rt_tree(RT)).

%% @doc Check if the given routing table entry is of the given entry type.
-spec entry_is_of_type(rt_entry(), Type::entry_type()) -> boolean().
entry_is_of_type(#rt_entry{type=Type}, Type) -> true;
entry_is_of_type(_,_) -> false.

%% @doc Check if the given routing table entry is a source entry.
-spec is_source(Entry :: rt_entry()) -> boolean().
is_source(Entry) -> entry_is_of_type(Entry, source).

%% @doc Check if the given routing table entry is a sticky entry.
-spec is_sticky(Entry :: rt_entry()) -> boolean().
is_sticky(Entry) -> entry_is_of_type(Entry, sticky).

-spec entry_type(Entry :: rt_entry()) -> entry_type().
entry_type(Entry) -> Entry#rt_entry.type.

%% @doc Get the node id of a routing table entry
-spec entry_nodeid(Node :: rt_entry()) -> key().
entry_nodeid(#rt_entry{node=Node}) -> node:id(Node).

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
-spec predecessor_node(RT :: rt(), Node :: rt_entry()) -> rt_entry().
predecessor_node(RT, Node) ->
    gb_trees:get(adjacent_pred(Node), get_rt_tree(RT)).

-spec successor_node(RT :: rt(), Node :: rt_entry()) -> rt_entry().
successor_node(RT, Node) ->
    try gb_trees:get(adjacent_succ(Node), get_rt_tree(RT)) catch
         error:function_clause -> exit('stale adjacent fingers')
    end.

-spec spacing(Node :: rt_entry(), RT :: rt()) -> float().
spacing(Node, RT) ->
    SourceNodeId = entry_nodeid(get_source_node(RT)),
    canonical_spacing(SourceNodeId, entry_nodeid(Node),
        adjacent_succ(Node)).

%% @doc Calculate the canonical spacing, which is defined as
%%  S_i = log_2(distance(SourceNode, SuccId) / distance(SourceNode, Node))
canonical_spacing(SourceId, NodeId, SuccId) ->
    util:log2(get_range(SourceId, SuccId) / get_range(SourceId, NodeId)).

% @doc Check that all entries in an rt are well connected by their adjacent fingers
-spec check_rt_integrity(RT :: rt()) -> boolean().
check_rt_integrity(#rt_t{} = RT) ->
    Nodes = [node:id(N) || N <- internal_to_list(RT)],

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
    lists:all(fun(X) -> X end, Checks)
    .

%% userdevguide-begin rt_frtchord:wrap_message
%% @doc Wrap lookup messages.
%% For node learning in lookups, a lookup message is wrapped with the global Pid of the
%% sending routing table.
-spec wrap_message(Msg::comm:message()) -> comm:message().
wrap_message(Msg) when element(1, Msg) =:= lookup -> {'$wrapped', comm:this(), Msg};
wrap_message(Msg) -> Msg.
%% userdevguide-end rt_frtchord:wrap_message

%% userdevguide-begin rt_frtchord:unwrap_message
%% @doc Unwrap lookup messages.
%% The Pid is retrieved and the Pid of the current node is sent to the retrieved Pid
-spec unwrap_message(Msg::comm:message(), State::dht_node_state:state()) -> comm:message().
unwrap_message({'$wrapped', Pid, UnwrappedMessage}, State) ->
    comm:send(Pid,
        {send_to_group_member, routing_table,
            {rt_learn_node, dht_node_state:get(State, node)}
        }),
    UnwrappedMessage
    ;
unwrap_message(Msg, _State) -> Msg
    .
%% userdevguide-end rt_frtchord:unwrap_message
