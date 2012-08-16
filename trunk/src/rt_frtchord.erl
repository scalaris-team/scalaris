% @copyright 2008-2012 Zuse Institute Berlin

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
        , id :: key_t()
        , type :: entry_type()
        , spacing = 0.0 :: float() % canonical space around a node
        , adjacent_fingers = {undefined, undefined} ::
            {#rt_entry{} | 'undefined', #rt_entry{} | 'undefined'}

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
maximum_entries() -> 1000.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_frtchord:empty
%% @doc Creates an "empty" routing table containing the successor.
add_sticky_nodes_to_rt([H|_] = List, RT) when is_tuple(H) ->
    lists:foldl(fun(Entry, Acc) ->
                add_sticky_entry(Entry, Acc)
        end, RT, List).

-spec empty(nodelist:neighborhood()) -> rt().
empty(Neighbors) -> 
    % enter all known nodes as sticky entries into the gb tree
    EmptyRT = #rt_t{},
    Preds = nodelist:preds(Neighbors),
    Succs = nodelist:succs(Neighbors),
    PredsAdded = add_sticky_nodes_to_rt(Preds, EmptyRT),
    SuccsAndPredsAdded = add_sticky_nodes_to_rt(Succs, PredsAdded),
    add_source_entry(nodelist:node(Neighbors), SuccsAndPredsAdded)
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
%% TODO
-spec init_stabilize(nodelist:neighborhood(), rt()) -> rt().
init_stabilize(Neighbors, _RT) -> empty(Neighbors).
%% userdevguide-end rt_frtchord:init_stabilize

%% userdevguide-begin rt_frtchord:update
%% @doc Updates the routing table due to a changed node ID, pred and/or succ.
%% TODO what should be done here? should we really throw away everything? or trigger
%rebuild?
-spec update(OldRT::rt(), OldNeighbors::nodelist:neighborhood(),
             NewNeighbors::nodelist:neighborhood()) -> {ok, rt()}.
update(_OldRT, _OldNeighbors, NewNeighbors) ->
    {ok, empty(NewNeighbors)}.
%% userdevguide-end rt_frtchord:update

%% userdevguide-begin rt_frtchord:filter_dead_node
%% @doc Removes dead nodes from the routing table (rely on periodic
%%      stabilization here).
-spec filter_dead_node(rt(), comm:mypid()) -> rt().
filter_dead_node(RT, DeadPid) -> 
    % find the node id of DeadPid and delete it from the RT
    [Node] = [N || {N, _,_,_} <- to_list(RT), node:pidX(N) =:= DeadPid],
    entry_delete(node:id(Node), RT)
    .
%% userdevguide-end rt_frtchord:filter_dead_node

%% userdevguide-begin rt_frtchord:to_pid_list
%% @doc Returns the pids of the routing table entries.
%% TODO RT should probably be a State tuple, not the RT itself
-spec to_pid_list(rt()) -> [comm:mypid()].
to_pid_list(RT) -> [node:pidX(rt_entry_node(N)) || N <- to_list(RT)].
%% userdevguide-end rt_frtchord:to_pid_list

%% userdevguide-begin rt_frtchord:get_size
%% @doc Returns the size of the routing table.
-spec get_size(rt() | external_rt()) -> non_neg_integer().
get_size(RT) -> gb_trees:size(get_rt_tree(RT)).
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
    %% merge the routing tables
    LocalRT = rt_loop:get_rt(State),
    NewRT = case LocalRT =:= RT of
        true ->
            % - add each entry from the other RT if it doesn't already exist
            % - entries to be added have to be normal entries
            lists:foldl(
                fun(Entry, Acc) ->
                        case entry_exists(Entry, Acc) of
                            true -> Acc;
                            false -> add_normal_entry(Entry, Acc)
                        end
                end,
                LocalRT, to_list(RT)
            );
        false -> LocalRT
    end,
    io:format("Merged RTs"),
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
check(OldRT, OldRT, _, _, _) -> ok;
check(OldRT, NewRT, _OldNeighbors, NewNeighbors, ReportToFD) ->
    Pid = pid_groups:get_my(dht_node),
    RT_ext = export_rt_to_dht_node(NewRT, NewNeighbors),
    comm:send_local(Pid, {rt_update, RT_ext}),
    % update failure detector:
    case ReportToFD of
        true ->
            NewPids = to_pid_list(NewRT),
            OldPids = to_pid_list(OldRT),
            fd:update_subscriptions(OldPids, NewPids);
        _ -> ok
    end
    .
%% userdevguide-end rt_frtchord:check

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_frtchord:empty_ext
-spec empty_ext(nodelist:neighborhood()) -> external_rt().
empty_ext(Neighbors) -> empty(Neighbors).
%% userdevguide-end rt_frtchord:empty_ext

%% userdevguide-begin rt_frtchord:next_hop
%% @doc Returns the next hop to contact for a lookup.
%% TODO Next hop contains type errors
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
                         {value, _Key, N} ->
                             N;
                         nil when RTSize =:= 0 ->
                             nodelist:succ(Neighbors);
                         nil -> % forward to largest finger
                             {_Key, N} = gb_trees:largest(get_rt_tree(RT)),
                             N
                     end,
            FinalNode =
                case RTSize < config:read(rt_size_use_neighbors) of
                    false -> NodeRT;
                    _     ->
                        % check neighborhood:
                        nodelist:largest_smaller_than(Neighbors, Id, NodeRT)
                end,
            node:pidX(FinalNode)
    end.
%% userdevguide-end rt_frtchord:next_hop

%% userdevguide-begin rt_frtchord:export_rt_to_dht_node
%% @doc Converts the internal RT to the external RT used by the dht_node.
%% TODO
-spec export_rt_to_dht_node(rt(), Neighbors::nodelist:neighborhood()) -> external_rt().
export_rt_to_dht_node(RT, _Neighbors) -> RT.
%% userdevguide-end rt_frtchord:export_rt_to_dht_node

%% userdevguide-begin rt_frtchord:to_list
%% @doc Converts the (external) representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
-spec to_list(dht_node_state:state()) -> nodelist:snodelist().
to_list(#rt_t{} = RT) ->
    SourceNode = get_source_node(RT),
    % sort
    Sorted = lists:sort(fun(A, B) -> entry_nodeid(A) =< entry_nodeid(B) end,
        [X || X <- gb_trees:values(get_rt_tree(RT))]),
    % rearrange elements: all until the source node must be attached at the end
    {Front, Tail} = lists:splitwith(fun(N) ->
                entry_nodeid(N) =:= SourceNode end, Sorted),
    TailNoSourceNode = case Tail of
        [SourceNode] -> [];
        [SourceNode|T] -> T
    end,
    % TODO inefficient -> reimplement with foldl
    TailNoSourceNode ++ Front
    ;
to_list(State) -> % match external RT
    RT = dht_node_state:get(State, rt),
    to_list(RT)
    .
%% userdevguide-end rt_frtchord:to_list

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% FRT specific algorithms and functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Filter one element from a set of nodes. Do it in a way that filters such a node
% that the resulting routing table is the best one under all _possible_ routing tables.
entry_filtering(RT, AllowedNodes) ->
    Spacings = [
        begin
            PredNode = predecessor_node(RT,Node),
            {Node, spacing(Node) + spacing(PredNode)}
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
entry_delete(EntryKey, RT) ->
    Tree = gb_trees:delete(EntryKey, get_rt_tree(RT)),
    RT#rt_t{nodes=Tree}.

% @doc Add a new entry to the routing table. A source node is only allowed to be added
% once.
entry_learn(#rt_entry{type=source} = Entry, #rt_t{source=undefined} = RT) -> 
    entry_learn(Entry, RT#rt_t{source=entry_nodeid(Entry)})
    ;
entry_learn(Entry, RT) -> 
    Nodes = gb_trees:enter(entry_nodeid(Entry), Entry, get_rt_tree(RT)),
    rt_set_nodes(RT, Nodes)
    .

% @doc Combines entry learning and entry filtering.
entry_learning_and_filtering(Entry, RT) ->
    IntermediateRT = entry_learn(Entry, RT),
    SizeOfRT = get_size(IntermediateRT),
    MaxEntries = maximum_entries(),
    if SizeOfRT >= MaxEntries ->
            AllowedNodes = [N || N <- to_list(RT),
                not is_sticky(N) and not is_source(N)],
            entry_filtering(IntermediateRT, AllowedNodes);
        true -> IntermediateRT
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% RT and RT entry record accessors
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @private

% @doc Get the source node of a routing table
% XXX using any() as return as dialyzer complains when using rt_entry()
get_source_node(#rt_t{source=undefined}) ->
    exit("routing table source unknown");
get_source_node(#rt_t{source=Node} = RT) -> gb_trees:get(Node, RT#rt_t.nodes).

% @doc Get the gb_tree of the routing table containing its nodes
get_rt_tree(#rt_t{nodes=Nodes}) -> Nodes.

% @doc Check if a node exists in the routing table
entry_exists(EntryKey, #rt_t{nodes=Nodes}) ->
    case gb_trees:lookup(EntryKey, Nodes) of
        none -> false;
        _Else -> true
    end.

-spec add_entry(Entry :: rt_entry() | node:node_type(),
    entry_type(),
    RT :: rt()) -> rt().
add_entry(#rt_entry{node=Entry}, Type, RT) ->
    add_entry(Entry, Type, RT);
add_entry(Entry, Type, RT) ->
    NewNode = #rt_entry{
        node = Entry,
        id   = node:id(Entry),
        type = Type
    },
    entry_learning_and_filtering(NewNode, RT)
    .

% @doc Add a sticky entry to the routing table
% TODO add adjacent fingers if possible
add_sticky_entry(Entry, RT) -> add_entry(Entry, sticky, RT).

% @doc Add the source entry to the routing table
% TODO unit test that only one source entry is allowed
% TODO add adjacent fingers if possible
add_source_entry(Entry, RT) -> add_entry(Entry, source, RT).

% @doc Add a normal entry to the routing table
% TODO add adjacent fingers if possible
add_normal_entry(Entry, RT) -> add_entry(Entry, normal, RT).

rt_entry_node(#rt_entry{node=N}) -> N.

rt_set_nodes(RT, Nodes) ->
    RT#rt_t{nodes=Nodes}.

%% is the given node the source entry?
entry_is_of_type(#rt_entry{type=Type}, Type) -> true;
entry_is_of_type(_,_) -> false.

is_source(Entry) -> entry_is_of_type(Entry, source).
is_sticky(Entry) -> entry_is_of_type(Entry, sticky).

entry_nodeid(#rt_entry{node=Node}) -> node:id(Node).

adjacent_pred(#rt_entry{adjacent_fingers={Pred,_Succ}}) -> Pred.

predecessor_node(RT, Node) ->
    gb_trees:get(adjacent_pred(Node), get_rt_tree(RT)).

spacing(Node) -> Node#rt_entry.spacing.
