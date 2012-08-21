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

%% TODO check if calls to empty/1 should be replaced with calles to init/1
-module(rt_frtchord).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

-behaviour(rt_beh).
-include("scalaris.hrl").

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
        {key() | 'undefined', key() | 'undefined'}

    }).

-type(rt_entry() :: #rt_entry{}).

-record(rt_t, {
        source = undefined :: key_t() | undefined
        , nodes = gb_trees:empty() :: gb_tree()
    }).

-type(rt_t() :: #rt_t{}).

-type custom_message() :: {get_rt, SourcePID :: comm:mypid()}
                        | {get_rt_reply, RT::rt_t()}.

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

%% userdevguide-begin rt_frtchord:empty
%% @doc Creates an "empty" routing table containing the successor.
-spec empty(nodelist:neighborhood()) -> rt().
empty(Neighbors) ->
    RT = add_source_entry(nodelist:node(Neighbors), #rt_t{}),
    Preds = nodelist:preds(Neighbors),
    Succs = nodelist:succs(Neighbors),

    % coyping the preds and succs silences dialyzer
    StickyPreds = [N || N <- Preds],
    StickySuccs = [N || N <- Succs],

    % insert preds and succs as sticky entries to the routing table
    % TODO We should always be able to add sticky nodes -> don't check the RT size
    TmpRT = lists:foldl(fun add_sticky_entry/2, RT, StickyPreds),
    lists:foldl(fun add_sticky_entry/2, TmpRT, StickySuccs)
    .
%% userdevguide-end rt_frtchord:empty

% @doc Initialize the routing table. This function is allowed to send messages.
-spec init(nodelist:neighborhood()) -> rt().
init(Neighbors) -> 
    % ask the successor node for its routing table
    % XXX i have to wrap the message as the Pid given as the succ is actually the dht
    % node, but the message should be send to the routing table
    Msg = {send_to_group_member, routing_table, {get_rt, comm:this()}},
    comm:send(node:pidX(nodelist:succ(Neighbors)), Msg),
    empty(Neighbors).

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
        true -> % replace sticky entries
            StickyEntries = sets:from_list(get_sticky_entries(RT)),
            remove_and_ping_entries(StickyEntries, Neighbors, RT)
            ;
        _Else -> % source node changed, replace the complete table
            init(Neighbors)
    end.


%% userdevguide-end rt_frtchord:init_stabilize

-spec get_node_neighbors(nodelist:neighborhood()) -> set().
get_node_neighbors(Neighborhood) ->
    lists:foldl(fun sets:add_element/2,
        lists:foldl(fun sets:add_element/2, sets:new(),
            nodelist:preds(Neighborhood)),
        nodelist:succs(Neighborhood))
    .

%% @doc Remove and ping entries
%% This function removes sticky nodes from the RT which aren't in the neighborhood
%% anymore. Additionally, it pings all entries to make sure that the IDs of the entries
%% haven't changed.
%% TODO do we delete living sticky entries here?
-spec remove_and_ping_entries(OldStickyNodes :: set(),
                              NewNeighbors :: nodelist:neighborhood(),
                              RT :: rt()) -> rt().
remove_and_ping_entries(OldStickyNodes, NewNeighbors, RT) ->
    NewStickyNodes = get_node_neighbors(NewNeighbors),
    DeadStickyNodes = sets:subtract(OldStickyNodes, NewStickyNodes),
    DeadStickyNodesIds = util:sets_map(fun entry_nodeid/1, DeadStickyNodes),
    % filter dead nodes
    FilteredRT = lists:foldl(fun entry_delete/2, RT, DeadStickyNodesIds),
    % add new sticky nodes
    NewRT = sets:fold(fun add_sticky_entry/2, FilteredRT, NewStickyNodes),
    %% TODO ping entries
    NewRT.

%% userdevguide-begin rt_frtchord:update
%% @doc Updates the routing table due to a changed node ID, pred and/or succ.
%% - We must rebuild the complete routing table when the source node id changed
%% - If only the preds/succs changed, adapt the old routing table
-spec update(OldRT::rt(), OldNeighbors::nodelist:neighborhood(),
    NewNeighbors::nodelist:neighborhood()) -> {trigger_rebuild, rt()} | {ok, rt()}.
update(OldRT, OldNeighbors, NewNeighbors) ->
    case nodelist:node(OldNeighbors) =:= nodelist:node(NewNeighbors) of
        true -> % source node didn't change
            % update the sticky nodes: delete old nodes and add new nodes
            OldStickyNodes = get_node_neighbors(OldNeighbors),
            NewRT = remove_and_ping_entries(OldStickyNodes, NewNeighbors, OldRT),
            
            case NewRT of % only trigger a rebuild if something changed
                OldRT -> 
                    {ok, NewRT};
                _Else ->
                    {trigger_rebuild, NewRT}
            end
            ;
        _Else -> % source node changed, rebuild the complete table
            {trigger_rebuild, OldRT}
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
%%      Begin==End, the whole key range is split in halves.
%%      Beware: SplitFactor must be in [0, 1]; the final key will be rounded
%%      down and may thus be Begin.
-spec get_split_key(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE, SplitFraction::{Num::0..100, Denom::0..100}) -> key().
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
                                "{int(), int()}")
        end.

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
    NewRT = case LocalRT =:= RT of
        true ->
            % - add each entry from the other RT if it doesn't already exist
            % - entries to be added have to be normal entries
            util:gb_trees_foldl(
                fun(Key, Entry, Acc) ->
                        %% TODO send an async message to check if entry is valid; if it
                        %isn't, delete it later
                        case entry_exists(Key, Acc) of
                            true -> Acc;
                            false -> 
                                io:format("Adding normal entry ~p~n", [Entry]),
                                add_normal_entry(Entry, Acc)
                        end
                end,
                LocalRT, get_rt_tree(RT)
            );
        false -> LocalRT
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
            io:format("And the Pid is....~p~n", [Pid]),
            case Pid of
                failed -> ok;
                _E     -> RTExt = export_rt_to_dht_node(NewRT, NewNeighbors),
                          comm:send_local(Pid, {rt_update, RTExt})
            end,
            % update failure detector:
            case ReportToFD of
                true -> NewPids = to_pid_list(NewRT),
                        OldPids = to_pid_list(OldRT),
                        fd:update_subscriptions(OldPids, NewPids);
                _ -> ok
            end
    end
    .
%% userdevguide-end rt_frtchord:check

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_frtchord:empty_ext
-spec empty_ext(nodelist:neighborhood()) -> external_rt().
empty_ext(Neighbors) -> export_rt_to_dht_node(empty(Neighbors), Neighbors).
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
-spec entry_filtering(rt(),[rt_entry()]) -> rt().
entry_filtering(RT, []) -> RT;
entry_filtering(RT, AllowedNodes) ->
    Spacings = [
        begin
            PredNode = predecessor_node(RT,Node),
            {Node, spacing(Node, RT) + spacing(PredNode, RT)}
        end || Node <- AllowedNodes],
    % remove the element with the smallest canonical spacing range between its predecessor
    % and its successor. TODO beware of numerical errors!
    {FilterEntry, _Spacing} = hd(lists:sort(
            fun ({_,SpacingA}, {_, SpacingB})
                        -> SpacingA =< SpacingB
            end, Spacings)
      ),
    entry_delete(entry_nodeid(FilterEntry), RT)
    .

% @doc Delete an entry from the routing table
-spec entry_delete(EntryKey :: key(), RT :: rt()) -> RefinedRT :: rt().
entry_delete(EntryKey, RT) ->
    Tree = gb_trees:delete(EntryKey, get_rt_tree(RT)),
    RT#rt_t{nodes=Tree}.

% @doc Create an rt_entry and return the entry together with the Pred und Succ node, where
% the adjacent fingers are changed for each node.
-spec create_entry(Node :: node:node_type(), Type :: entry_type(), RT :: rt()) ->
    {rt_entry(), rt_entry(), rt_entry()}.
create_entry(Node, Type, RT) ->
    NodeId = node:id(Node),
    Tree = get_rt_tree(RT),
    % search the adjacent fingers using the routing table
    PredLookup = util:gb_trees_largest_smaller_than(NodeId, Tree),
    case PredLookup of
        nil -> % this is the first entry of the RT
            NewEntry = #rt_entry{
                node=Node,
                type=Type,
                adjacent_fingers={NodeId, NodeId}
            },
            {NewEntry, NewEntry, NewEntry}
            ;
        {value, PredId, Pred} ->
            Succ = successor_node(RT, Pred),
            % construct the node
            NewEntry = #rt_entry{
                node=Node,
                type=Type,
                adjacent_fingers={PredId, entry_nodeid(Succ)}
            },
            {set_adjacent_succ(Pred, NodeId), NewEntry, set_adjacent_pred(Succ, NodeId)}
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
            NewRT = case Type of
                source -> set_source_node(entry_nodeid(NewNode), RT);
                _else  -> RT
            end,
            rt_set_nodes(NewRT, Nodes);
        _else -> RT
    end
    .

% @doc Combines entry learning and entry filtering.
-spec entry_learning_and_filtering(node:node_type(), entry_type(), rt()) -> rt().
entry_learning_and_filtering(Entry, Type, RT) ->
    IntermediateRT = entry_learning(Entry, Type, RT),
    SizeOfRT = get_size(IntermediateRT),
    MaxEntries = maximum_entries(),
    if SizeOfRT >= MaxEntries ->
            io:format("Have to filter...~nRT:~p~n", [IntermediateRT]),
            AllowedNodes = [N || N <- gb_trees:values(get_rt_tree(RT)),
                not is_sticky(N) and not is_source(N)],
            NewRT = entry_filtering(IntermediateRT, AllowedNodes),
            io:format("NewRT:~p~n", [NewRT]),
            NewRT;
       true -> IntermediateRT
    end.

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
    entry_learning_and_filtering(Node, Type, RT).

% @doc Add a sticky entry to the routing table
-spec add_sticky_entry(Entry :: node:node_type(), rt()) -> rt().
add_sticky_entry(Entry, RT) -> add_entry(Entry, sticky, RT).

% @doc Add the source entry to the routing table
% TODO unit test that only one source entry is allowed
-spec add_source_entry(Entry :: node:node_type(), rt()) -> rt().
add_source_entry(Entry, RT) -> add_entry(Entry, source, RT).

% @doc Add a normal entry to the routing table
-spec add_normal_entry(Entry :: node:node_type(), rt()) -> rt().
add_normal_entry(Entry, RT) -> add_entry(Entry, normal, RT).

% @doc Get the inner node:node_type() of a rt_entry
-spec rt_entry_node(N :: rt_entry()) -> node:node_type().
rt_entry_node(#rt_entry{node=N}) -> N.

% @doc Set the treeof nodes of the routing table.
-spec rt_set_nodes(RT :: rt(), Nodes :: gb_tree()) -> rt().
rt_set_nodes(#rt_t{source=undefined}, _) -> erlang:error(source_node_undefined);
rt_set_nodes(#rt_t{} = RT, Nodes) -> RT#rt_t{nodes=Nodes}.

%% Get the node with the given Id. This function will crash if the node doesn't exist.
%-spec rt_get_node(NodeId :: key(), RT :: rt()) -> rt_entry().
%rt_get_node(NodeId, RT)  -> gb_trees:get(NodeId, get_rt_tree(RT)).

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

%% @doc Get the adjacent predecessor key() of the current node.
-spec adjacent_pred(rt_entry()) -> key().
adjacent_pred(#rt_entry{adjacent_fingers={Pred,_Succ}}) -> Pred.

%% @doc Get the adjacent successor key of the current node
-spec adjacent_succ(rt_entry()) -> key().
adjacent_succ(#rt_entry{adjacent_fingers={_Pred,Succ}}) -> Succ.

%% @doc Set the adjacent successor of the finger
-spec set_adjacent_succ(rt_entry(), key()) -> rt_entry().
set_adjacent_succ(#rt_entry{adjacent_fingers={PredId, _Succ}} = Entry, SuccId) ->
    Entry#rt_entry{adjacent_fingers={PredId, SuccId}}.

%% @doc Set the adjacent predecessor of the finger
-spec set_adjacent_pred(rt_entry(), key()) -> rt_entry().
set_adjacent_pred(#rt_entry{adjacent_fingers={_Pred, SuccId}} = Entry, PredId) ->
    Entry#rt_entry{adjacent_fingers={PredId, SuccId}}.

%% @doc Get the adjacent predecessor rt_entry() of the given node.
-spec predecessor_node(RT :: rt(), Node :: rt_entry()) -> rt_entry().
predecessor_node(RT, Node) ->
    gb_trees:get(adjacent_pred(Node), get_rt_tree(RT)).

-spec successor_node(RT :: rt(), Node :: rt_entry()) -> rt_entry().
successor_node(RT, Node) ->
    gb_trees:get(adjacent_succ(Node), get_rt_tree(RT)).

-spec spacing(Node :: rt_entry(), RT :: rt()) -> float().
spacing(Node, RT) ->
    SourceNodeId = entry_nodeid(get_source_node(RT)),
    canonical_spacing(SourceNodeId, entry_nodeid(Node),
        adjacent_succ(Node)).

%% @doc Calculate the canonical spacing, which is defined as
%%  S_i = log_2(distance(SourceNode, SuccId) / distance(SourceNode, Node))
canonical_spacing(SourceId, NodeId, SuccId) ->
    util:log2(get_range(SourceId, SuccId) / get_range(SourceId, NodeId)).
