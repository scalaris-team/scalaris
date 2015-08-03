%  @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Provides lists of nodes that are sorted by the nodes' ids.
%% @end
%% @version $Id$
-module(nodelist).
-author('kruber@zib.de').
-vsn('$Id$').

-compile({inline, [node/1, nodeid/1, pred/1, preds/1, succ/1, succs/1,
                   node_range/1, succ_range/1,
                   create_pid_to_node_dict/2]}).

-export([% constructors:
         new_neighborhood/1, new_neighborhood/2, new_neighborhood/3,
         mk_neighborhood/2, mk_neighborhood/4,
         mk_nodelist/2,

         % getters:
         node/1, nodeid/1, pred/1, preds/1, succ/1, succs/1,
         node_range/1, succ_range/1,
         has_real_pred/1, has_real_succ/1,

         % modifiers:
         trunc/3, trunc_preds/2, trunc_succs/2,
         remove/2, remove/3, filter/2, filter/3,
         lremove/2, lremove/3,lfilter/2, lfilter/3,
         lfilter_min_length/3, filter_min_length/4,
         merge/4, add_node/4, add_nodes/4,

         update_node/2, % update base node
         update_ids/2, % update node ids

         % converters:
         to_list/1,

         % miscellaneous:
         succ_ord_node/2, succ_ord_id/2,
         succ_ord_node/3, succ_ord_id/3,
         lupdate_ids/2, lremove_outdated/1, lremove_outdated/2,
         largest_smaller_than/2, largest_smaller_than/3,
         create_pid_to_node_dict/2]).

-include("scalaris.hrl").

-export_type([neighborhood/0, snodelist/0, non_empty_snodelist/0]).

-type(snodelist() :: [node:node_type()]). % (sorted) node list
-type(non_empty_snodelist() :: [node:node_type(),...]). % non-empty (sorted) node list
-opaque(neighborhood() :: {Preds::non_empty_snodelist(), Node::node:node_type(), Succs::non_empty_snodelist(),
                           NodeInterval::intervals:interval(), SuccInterval::intervals:interval()}).

%% @doc Helper function that throws an exception if the given neighbor is newer
%%      than the BaseNode (requires that both are equal!).
-spec throw_if_newer(Neighbor::node:node_type(), BaseNode::node:node_type()) -> ok.
throw_if_newer(Neighbor, BaseNode) ->
    case node:is_newer(Neighbor, BaseNode) of
        true ->
            throw('cannot create a neighborhood() with a neighbor newer than the node itself');
        _ -> ok
    end.

-spec add_intervals(Preds::non_empty_snodelist(), Node::node:node_type(), Succs::non_empty_snodelist()) -> neighborhood().
add_intervals([Pred | _] = Preds, Node, [Succ | _] = Succs) ->
    {Preds, Node, Succs,
     node:mk_interval_between_nodes(Pred, Node),
     node:mk_interval_between_nodes(Node, Succ)}.

%% @doc Creates a new neighborhood structure for the given node.
-spec new_neighborhood(Node::node:node_type()) -> neighborhood().
new_neighborhood(Node) ->
    add_intervals([Node], Node, [Node]).

%% @doc Creates a new neighborhood structure for the given node and a neighbor
%%      (this will be its predecessor and successor).
-spec new_neighborhood(Node::node:node_type(), Neighbor::node:node_type()) -> neighborhood().
new_neighborhood(Node, Neighbor) ->
    case node:same_process(Node, Neighbor) of
        true ->
            % the node should always be the first to get a new ID!
            % if not, something went wrong
            throw_if_newer(Neighbor, Node),
            new_neighborhood(Node);
        false ->
            add_intervals([Neighbor], Node, [Neighbor])
    end.

%% @doc Creates a new neighborhood structure for the given node, its predecessor
%%      and successor. If the order is wrong, Pred and Succ will be exchanged.
%%      (provided for convenience - special case of mk_neighborhood)
-spec new_neighborhood(Pred::node:node_type(), Node::node:node_type(), Succ::node:node_type()) -> neighborhood().
new_neighborhood(Pred, Node, Succ) ->
    case node:same_process(Pred, Node) of
        false ->
            case node:same_process(Succ, Node) of
                false ->
                    case node:same_process(Pred, Succ) of
                        false ->
                            % distinct nodes -> determine order:
                            case succ_ord_node(Pred, Succ, Node) of
                                true  -> add_intervals([Succ], Node, [Pred]);
                                false -> add_intervals([Pred], Node, [Succ])
                            end;
                        true ->
                            NewerNode = node:newer(Pred, Succ),
                            new_neighborhood(Node, NewerNode)
                    end;
                true ->
                    throw_if_newer(Succ, Node),
                    new_neighborhood(Node, Pred)
            end;
        true ->
            % the node should always be the first to get a new ID!
            % if not, something went wrong
            throw_if_newer(Pred, Node),
            new_neighborhood(Node, Succ)
    end.

%% @doc Helper function to make sure a (temporary) neighborhood object has
%%      non-empty predecessor and successor lists (fills them with itself if
%%      necessary).
-spec ensure_lists_not_empty(Preds::snodelist(), Node::node:node_type(), Succs::snodelist()) -> neighborhood().
ensure_lists_not_empty([_|_] = Preds, Node, [_|_] = Succs) ->
    add_intervals(Preds, Node, Succs);
ensure_lists_not_empty([], Node, []) ->
    add_intervals([Node], Node, [Node]);
ensure_lists_not_empty([], Node, [_|_] = Succs) ->
    add_intervals([lists:last(Succs)], Node, Succs);
ensure_lists_not_empty([_|_] = Preds, Node, []) ->
    add_intervals(Preds, Node, [lists:last(Preds)]).

%% @doc Truncates the given neighborhood's predecessor and successor lists to
%%      the given sizes.
-spec trunc(Neighborhood::neighborhood(), PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
trunc({Preds, Node, Succs, NodeIntv, SuccIntv}, PredsLength, SuccsLength) when (PredsLength > 0) andalso (SuccsLength > 0) ->
    {lists:sublist(Preds, PredsLength), Node, lists:sublist(Succs, SuccsLength), NodeIntv, SuccIntv}.

%% @doc Truncates the given neighborhood's predecessor list to the given size.
-spec trunc_preds(neighborhood(), PredsLength::pos_integer()) -> neighborhood().
trunc_preds({Preds, Node, Succs, NodeIntv, SuccIntv}, PredsLength) when (PredsLength > 0) ->
    {lists:sublist(Preds, PredsLength), Node, Succs, NodeIntv, SuccIntv}.

%% @doc Truncates the given neighborhood's successor list to the given size.
-spec trunc_succs(neighborhood(), SuccsLength::pos_integer()) -> neighborhood().
trunc_succs({Preds, Node, Succs, NodeIntv, SuccIntv}, SuccsLength) when (SuccsLength > 0) ->
    {Preds, Node, lists:sublist(Succs, SuccsLength), NodeIntv, SuccIntv}.

%% @doc Returns the neighborhood's node.
-spec node(neighborhood()) -> node:node_type().
node(Neighborhood) -> element(2, Neighborhood).

%% @doc Returns the ID of the neighborhood's node (provided for convenience).
-spec nodeid(neighborhood()) -> ?RT:key().
nodeid(Neighborhood) -> node:id(?MODULE:node(Neighborhood)).

%% @doc Returns the node's range.
-spec node_range(neighborhood()) -> intervals:interval().
node_range(Neighborhood) -> element(4, Neighborhood).

%% @doc Updates the base node of the neighborhood if its ID is still between
%%      the ID of the predecessor and successor. Otherwise throws an exception.
%%      Note: pred and/or succ could change due to (outdated) other preds/succs.
-spec update_node(neighborhood(), NewBaseNode::node:node_type()) -> neighborhood().
update_node({[Node], Node, [Node], _NodeIntv, _SuccIntv}, NewBaseNode) ->
    add_intervals([NewBaseNode], NewBaseNode, [NewBaseNode]);
update_node({[Pred] = Neighbors, _Node, Neighbors, _NodeIntv, _SuccIntv}, NewBaseNode) ->
    case node:id(NewBaseNode) =:= node:id(Pred) of
        false -> add_intervals(Neighbors, NewBaseNode, Neighbors);
        _     -> throw(new_node_not_in_pred_succ_interval)
    end;
update_node({[Pred | _] = Preds, _Node, [Succ | _] = Succs, _NodeIntv, _SuccIntv}, NewBaseNode) ->
    PredSuccInt = intervals:new('(', node:id(Pred), node:id(Succ), ')'),
    case intervals:in(node:id(NewBaseNode), PredSuccInt) of
        true -> % sort preds, succs
                SuccOrd = fun(N1, N2) -> succ_ord_node(N1, N2, NewBaseNode) end,
                PredOrd = fun(N1, N2) -> succ_ord_node(N2, N1, NewBaseNode) end,
                SuccsSorted = lists:usort(SuccOrd, Succs),
                PredsSorted = lists:usort(PredOrd, Preds),
                add_intervals(PredsSorted, NewBaseNode, SuccsSorted);
        _    -> throw(new_node_not_in_pred_succ_interval)
    end.

%% @doc Returns the neighborhood's predecessor list.
-spec preds(neighborhood()) -> non_empty_snodelist().
preds(Neighborhood) -> element(1, Neighborhood).

%% @doc Returns a neighborhood's or a node lists's predecessor (may be the node
%%      itself).
-spec pred(neighborhood()) -> node:node_type().
pred(Neighborhood) -> hd(preds(Neighborhood)).

%% @doc Returns whether the neighborhood contains a real predecessor (one not
%%      equal to the own node) or not (provided for convenience).
-spec has_real_pred(neighborhood()) -> boolean().
has_real_pred(Neighborhood) -> pred(Neighborhood) =/= ?MODULE:node(Neighborhood).

%% @doc Returns the neighborhood's successor list.
-spec succs(neighborhood()) -> non_empty_snodelist().
succs(Neighborhood) -> element(3, Neighborhood).

%% @doc Returns the neighborhood's or a node lists's successor (may be the node
%%      itself).
-spec succ(neighborhood()) -> node:node_type().
succ(Neighborhood) -> hd(succs(Neighborhood)).

%% @doc Returns the successor's range.
-spec succ_range(neighborhood()) -> intervals:interval().
succ_range(Neighborhood) -> element(5, Neighborhood).

%% @doc Returns whether the neighborhood contains a real predecessor (one not
%%      equal to the own node) or not (provided for convenience).
-spec has_real_succ(neighborhood()) -> boolean().
has_real_succ(Neighborhood) -> succ(Neighborhood) =/= ?MODULE:node(Neighborhood).

%% @doc Splits the given (unsorted) node list into sorted lists with nodes that
%%      have smaller, equal and larger IDs than the given node. The NodeList may
%%      contain duplicates, i.e. nodes with the same pid but different IDs (and
%%      IDVersions).
-spec lsplit_nodelist(NodeList::[node:node_type()], Node::node:node_type()) -> {Smaller::snodelist(), Equal::snodelist(), Larger::snodelist()}.
lsplit_nodelist(NodeList, Node) ->
    lusplit_nodelist(lremove_outdated(NodeList, Node), Node).

%% @doc Splits the given (unsorted) "unique" node list, i.e. a list containing
%%      no two nodes with the same pid but different IDs (and IDVersions), into
%%      three sorted lists with nodes that have smaller, equal and larger IDs
%%      than the given node.
-spec lusplit_nodelist(NodeList::[node:node_type()], Node::node:node_type()) -> {Smaller::snodelist(), Equal::snodelist(), Larger::snodelist()}.
lusplit_nodelist(NodeList, Node) ->
    NodeId = node:id(Node),
    {Smaller, Equal, Larger} =
        util:lists_partition3(
          fun(N) ->
                  case node:same_process(N, Node) of
                      true -> throw_if_newer(N, Node), 2;
                      _    -> NId = node:id(N),
                              if NId < NodeId -> 1;
                                 NId > NodeId -> 3;
                                 true -> 2
                              end
                  end
          end, NodeList),

    SmallerSorted = lists:usort(fun succ_ord_node/2, Smaller),
    EqualSorted = lists:usort(fun succ_ord_node/2, Equal),
    LargerSorted = lists:usort(fun succ_ord_node/2, Larger),
    {SmallerSorted, EqualSorted, LargerSorted}.

%% @doc Creates a sorted nodelist starting at the given node and going clockwise
%%      along the ring (also see succ_ord_node/3).
-spec mk_nodelist(UnorderedNodeList::[node:node_type()], Node::node:node_type()) -> OrderedNodeList::snodelist().
mk_nodelist(NodeList, Node) ->
    {SmallerSorted, EqualSorted, LargerSorted} =
        lsplit_nodelist(NodeList, Node),
    lists:append([EqualSorted, LargerSorted, SmallerSorted]).

%% @doc Creates a neighborhood structure for the given node from a given
%%      (unsorted) node list and limits its predecessor and successor lists to
%%      the given sizes.
-spec mk_neighborhood(NodeList::[node:node_type()], Node::node:node_type(), PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
mk_neighborhood(NodeList, Node, PredsLength, SuccsLength) ->
    NeighborHood = mk_neighborhood(NodeList, Node),
    trunc(NeighborHood, PredsLength, SuccsLength).

%% @doc Creates a neighborhood structure for the given node from a given
%%      (unsorted) node list. Note that in this case, the predecessor and
%%      successor lists will effectively be the same!
-spec mk_neighborhood(NodeList::[node:node_type()], Node::node:node_type()) -> neighborhood().
mk_neighborhood(NodeList, Node) ->
    {SmallerSorted, EqualSorted, LargerSorted} =
        lsplit_nodelist(NodeList, Node),

    if (LargerSorted =:= []) andalso (SmallerSorted =:= []) ->
           Preds = lists:reverse(EqualSorted),
           Succs = EqualSorted;
       true ->
           Neighbors = lists:append(LargerSorted, SmallerSorted),
           Preds = lists:reverse(Neighbors),
           Succs = Neighbors
    end,
    ensure_lists_not_empty(Preds, Node, Succs).

-type filter_fun() :: fun((node:node_type()) -> boolean()).
-type eval_fun() :: fun((node:node_type()) -> any()).

%% @doc Removes the given node (or node with the given Pid) from a node list
%%      (provided for convenience - see lfilter/2).
-spec lremove(NodeOrPid::node:node_type() | comm:mypid() | pid(), snodelist()) -> snodelist().
lremove(NodeOrPid, NodeList) ->
    lfilter(NodeList, fun(N) -> not node:same_process(NodeOrPid, N) end).

%% @doc Removes the given node (or node with the given Pid) from a neighborhood
%%      (provided for convenience - see filter/2).
%%      Note: A neighborhood's base node is never removed!
-spec remove(NodeOrPid::node:node_type() | comm:mypid() | pid(), neighborhood()) -> neighborhood().
remove(NodeOrPid, Neighborhood) ->
    filter(Neighborhood, fun(N) -> not node:same_process(NodeOrPid, N) end).

%% @doc Removes the given node (or node with the given Pid) from a node list
%%      and executes EvalFun for any such occurrence (provided for convenience
%%      - see filter/3).
-spec lremove(NodeOrPid::node:node_type() | comm:mypid() | pid(), snodelist(),
              EvalFun::eval_fun()) -> snodelist().
lremove(NodeOrPid, NodeList, EvalFun) ->
    lfilter(NodeList, fun(N) -> not node:same_process(NodeOrPid, N) end, EvalFun).

%% @doc Removes the given node (or node with the given Pid) from a neighborhood
%%      and executes EvalFun for any such occurrence (provided for convenience,
%%      see filter/3).
%%      Note: A neighborhood's base node is never removed!
-spec remove(NodeOrPid::node:node_type() | comm:mypid() | pid(), neighborhood(),
             EvalFun::eval_fun()) -> neighborhood().
remove(NodeOrPid, Neighborhood, EvalFun) ->
    filter(Neighborhood, fun(N) -> not node:same_process(NodeOrPid, N) end, EvalFun).

%% @doc Keeps any node for which FilterFun returns true in a node list.
-spec lfilter(snodelist(), FilterFun::filter_fun()) -> snodelist().
lfilter(NodeList, FilterFun) ->
    [N || N <- NodeList, FilterFun(N)].

%% @doc Keeps any node for which FilterFun returns true in a neighborhood.
%%      Note: A neighborhood's base node is never removed!
-spec filter(neighborhood(), FilterFun::filter_fun()) -> neighborhood().
filter({Preds, Node, Succs, _NodeIntv, _SuccIntv}, FilterFun) ->
    NewPreds = lfilter(Preds, FilterFun),
    NewSuccs = lfilter(Succs, FilterFun),
    ensure_lists_not_empty(NewPreds, Node, NewSuccs).

%% @doc Keeps any node for which FilterFun returns true in a node list and
%%      executes EvalFun for any other node.
-spec lfilter(snodelist(), FilterFun::filter_fun(), EvalFun::eval_fun()) -> snodelist().
lfilter([N | Rest], FilterFun, EvalFun) ->
    case FilterFun(N) of
        true -> [N | lfilter(Rest, FilterFun, EvalFun)];
        _    -> EvalFun(N),
                lfilter(Rest, FilterFun, EvalFun)
    end;
lfilter([], _FilterFun, _EvalFun) ->
    [].

%% @doc Keeps any node for which FilterFun returns true in a neighborhood
%%      and executes EvalFun for any other node.
%%      Note: A neighborhood's base node is never removed!
-spec filter(neighborhood(), FilterFun::filter_fun(),
             EvalFun::eval_fun()) -> neighborhood().
filter({Preds, Node, Succs, _NodeIntv, _SuccIntv}, FilterFun, EvalFun) ->
    % note: cannot call lfilter/3 on the succ/pred lists (a node may be in both
    %       lists but should have EvalFun evaluated only once)
    {NewPreds, PredsRemoved} = lists:partition(FilterFun, Preds),
    {NewSuccs, SuccsRemoved} = lists:partition(FilterFun, Succs),
    Removed = lists:usort(lists:append(PredsRemoved, SuccsRemoved)),
    lists:foreach(EvalFun, Removed),
    ensure_lists_not_empty(NewPreds, Node, NewSuccs).

%% @doc Keeps any node for which FilterFun returns true in a node list but
%%      produces a node list with at least MinLength elements by adding enough
%%      unmatching nodes. Preserves the order of the list.
-spec lfilter_min_length(snodelist(), FilterFun::filter_fun(), MinLength::non_neg_integer())
        -> snodelist().
lfilter_min_length(NodeList, FilterFun, MinLength) ->
    % first count the number of nodes that match the FilterFun
    SatisfyingCount = lists:foldl(fun(N, Count) ->
                                          case FilterFun(N) of
                                              true  -> Count + 1;
                                              false -> Count
                                          end
                                  end,
                                  0, NodeList),
    % then collect matching nodes and as many unmatching nodes as needed to
    % have a result of at least MinLength elements
    % -> beware not to destroy the order of the list!
    UnsatisfyingNodesToAdd = MinLength - SatisfyingCount,
    NewNodeList =
        case UnsatisfyingNodesToAdd =< 0 of
            true -> [Node || Node <- NodeList, FilterFun(Node)];
            false ->
                AddIfSatisfyingOrMinFun =
                    fun(Node, {ResultList, UnsatisfyingNodesToAdd1}) ->
                            case FilterFun(Node) of
                                true  ->
                                    {[Node | ResultList], UnsatisfyingNodesToAdd1};
                                false when UnsatisfyingNodesToAdd1 =/= 0 ->
                                    {[Node | ResultList], UnsatisfyingNodesToAdd1 - 1};
                                false ->
                                    {ResultList, UnsatisfyingNodesToAdd1}
                            end
                    end,
                % elements are in wrong order, but lists:foldr cannot be applied
                {ResultsReverse, _} = lists:foldl(AddIfSatisfyingOrMinFun,
                                                  {[], UnsatisfyingNodesToAdd},
                                                  NodeList),
                lists:reverse(ResultsReverse)
        end,
    NewNodeList.

%% @doc Keeps any node for which FilterFun returns true in a predecessor and
%%      successor lists but produces a neighborhood with at least MinPredsLength
%%      predecessors and at least MinSuccsLength successors by adding enough
%%      unmatching nodes. If the predecessor or successor list is smaller than
%%      MinPredsLength and MinSuccsLength respectively, the whole list will be
%%      used.
%%      Note: A neighborhood's base node is never removed!
-spec filter_min_length(neighborhood(), FilterFun::filter_fun(), MinPredsLength::non_neg_integer(), MinSuccsLength::non_neg_integer()) -> neighborhood().
filter_min_length({Preds, Node, Succs, _NodeIntv, _SuccIntv}, FilterFun, MinPredsLength, MinSuccsLength) ->
    NewPreds = lfilter_min_length(Preds, FilterFun, MinPredsLength),
    NewSuccs = lfilter_min_length(Succs, FilterFun, MinSuccsLength),
    ensure_lists_not_empty(NewPreds, Node, NewSuccs).

%% @doc Helper function that removes the head of NodeList if is is equal to
%%      Node (using node:same_process/2).
-spec lremove_head_if_eq(NodeList::snodelist(), Node::node:node_type()) -> snodelist().
lremove_head_if_eq([H | T] = NodeList, Node) ->
    case node:same_process(H, Node) of
        false -> NodeList;
        true  -> T
    end;
lremove_head_if_eq([] = NodeList, _Node) ->
    NodeList.

%% @doc Converts a neighborhood to a sorted list of nodes including the
%%      predecessors, the node and its successors. The first element of the
%%      resulting list will be the node itself, afterwards every known node
%%      along the ring towards the first node.
-spec to_list(neighborhood()) -> non_empty_snodelist().
to_list({Preds, Node, Succs, _NodeIntv, _SuccIntv}) ->
    Ord = fun(N1, N2) -> succ_ord_node(N1, N2, Node) end,
    CleanPreds = lremove_head_if_eq(Preds, Node),
    CleanSuccs = lremove_head_if_eq(Succs, Node),
    CleanPredsReversed = lists:reverse(CleanPreds),
    [Node | util:smerge2(CleanSuccs, CleanPredsReversed, Ord)].

%% @doc Removes NodeToRemove from the given list and additionally gets the
%%      resulting list's last element.
-spec lget_last_and_remove(NodeList::snodelist(), NodeToRemove::node:node_type(), ResultList::snodelist()) ->
        {LastNode::node:node_type(), FilteredList::non_empty_snodelist()} |
        {null, []}.
lget_last_and_remove([H | T], NodeToRemove, ResultList) ->
    case node:same_process(H, NodeToRemove) of
        true ->
            lget_last_and_remove(T, NodeToRemove, ResultList);
        false ->
            lget_last_and_remove(T, NodeToRemove, [H | ResultList])
    end;
lget_last_and_remove([], _NodeToRemove, []) ->
    {node:null(), []};
lget_last_and_remove([], _NodeToRemove, [Last | _] = ResultList) ->
    {Last, lists:reverse(ResultList)}.

%% @doc Helper function that adds NewHead to the head of NodeList if is is not
%%      equal to Node (using node:same_process/2).
-spec ladd_head_if_noteq(NewHead::node:node_type(), NodeList::snodelist(), Node::node:node_type()) -> snodelist().
ladd_head_if_noteq(NewHead, NodeList, Node) ->
    case node:same_process(NewHead, Node) of
        false -> [NewHead | NodeList];
        true  -> NodeList
    end.

%% @doc Rebases a sorted node list (as returned by to_list/1, for example) to
%%      the sorted list based on a new first node (without including it).
%%      Removes the NewFirstNode from this list on the fly.
-spec lrebase_list(non_empty_snodelist(), NewFirstNode::node:node_type()) -> snodelist().
lrebase_list([First] = NodeList, NewFirstNode) ->
    case node:same_process(First, NewFirstNode) of
        false -> NodeList;
        true  -> []
    end;
lrebase_list([NewFirstNode | T], NewFirstNode) ->
    lremove(NewFirstNode, T); % just to be sure, remove NewFirstNode from the Tail
lrebase_list([First | T], NewFirstNode) ->
    {LastNode, TFilt} = lget_last_and_remove(T, NewFirstNode, []),
    if LastNode =/= null ->
           NodeListInterval = node:mk_interval_between_nodes(First, LastNode),
           case intervals:in(node:id(NewFirstNode), NodeListInterval) of
               false ->
                   ladd_head_if_noteq(First, TFilt, NewFirstNode);
               true->
                   NewPredsInterval = node:mk_interval_between_nodes(First, NewFirstNode),
                   {L1T, L2} = lists:splitwith(fun(N) -> intervals:in(node:id(N), NewPredsInterval) end, TFilt),
                   L1 = ladd_head_if_noteq(First, L1T, NewFirstNode),
                   lists:append(L2, L1)
           end;
       true ->
           ladd_head_if_noteq(First, [], NewFirstNode)
    end.

%% @doc Merges two lists of nodes into a neighborhood structure with the given
%%      node. Predecessor and successor lists are truncated to the given sizes,
%%      node IDs are updated with the most up-to-date ID from any list.
%%      Neither Node1View nor Node2View should contain BaseNode!
-spec lmerge_helper(Node1View::snodelist(), Node2View::snodelist(), BaseNode::node:node_type(),
                    PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
lmerge_helper(Node1View, Node2View, BaseNode, PredsLength, SuccsLength) ->
    {Node1ViewUpd, Node2ViewUpd} = lupdate_ids(Node1View, Node2View),
    % due to updated IDs, the lists might not be sorted anymore...
    Ord = fun(N1, N2) -> succ_ord_node(N1, N2, BaseNode) end,
    Node1ViewUpdSorted = lists:usort(Ord, Node1ViewUpd),
    Node2ViewUpdSorted = lists:usort(Ord, Node2ViewUpd),

    % favour the newly provided nodes over the present ones (they are probably newer)
    MergedView = util:smerge2(Node1ViewUpdSorted, Node2ViewUpdSorted, Ord,
                              fun(_E1, E2) ->
                                      ?DBG_ASSERT(_E1 =:= E2 orelse not node:same_process(_E1, E2)),
                                      [E2]
                              end),
    Preds = lists:sublist(lists:reverse(MergedView), PredsLength),
    Succs = lists:sublist(MergedView, SuccsLength),
    ensure_lists_not_empty(Preds, BaseNode, Succs).

%% @doc Merges nodes of Neighbors2 into Neighbors1 and truncates the predecessor
%%      and successor lists to the given sizes.
-spec merge(Neighbors1::neighborhood(), Neighbors2::neighborhood(), PredsLength::pos_integer(),
            SuccsLength::pos_integer()) -> neighborhood().
merge(Neighbors1, Neighbors2, PredsLength, SuccsLength) ->
    % note: similar to mk_neighborhood/4
    % create a sorted list of nodes in Neighbors1 and Neighbours2
    [Node1 | Neighbors1View] = to_list(Neighbors1),
    ?DBG_ASSERT(Node1 =:= ?MODULE:node(Neighbors1)),
    Neighbors2View = lrebase_list(to_list(Neighbors2), Node1),
    lmerge_helper(Neighbors1View, Neighbors2View, Node1, PredsLength, SuccsLength).

%% @doc Adds a node to a neighborhood structure and truncates the predecessor
%%      and successor list to the given sizes.
%%      Note: nodes which have only been present in the predecessor (successor)
%%      list may now also appear in the successor (predecessor) list if a list
%%      has been too small.
-spec add_node(Neighbors::neighborhood(), NodeToAdd::node:node_type(), PredsLength::pos_integer(),
               SuccsLength::pos_integer()) -> neighborhood().
add_node({Preds, BaseNode, Succs, _NodeIntv, _SuccIntv}, NodeToAdd, PredsLength, SuccsLength) ->
    case node:same_process(BaseNode, NodeToAdd) of
        false ->
            CleanPreds = lremove_head_if_eq(Preds, BaseNode),
            CleanSuccs = lremove_head_if_eq(Succs, BaseNode),
            UpdateFun = fun(N) ->
                                case node:same_process(N, NodeToAdd) of
                                    true -> node:newer(N, NodeToAdd);
                                    _    -> N
                                end
                        end,
            % create a view of all know (and updated) nodes:
            ViewUpd = lists:append([NodeToAdd | lists:map(UpdateFun, CleanPreds)],
                                   lists:map(UpdateFun, CleanSuccs)),
            % sort the list again
            SuccOrd = fun(N1, N2) -> succ_ord_node(N1, N2, BaseNode) end,
            SuccsUpdSorted = lists:usort(SuccOrd, ViewUpd),
            PredsUpdSorted = lists:reverse(SuccsUpdSorted),

            trunc(add_intervals(PredsUpdSorted, BaseNode, SuccsUpdSorted), PredsLength, SuccsLength);
        true ->
            throw_if_newer(NodeToAdd, BaseNode),
            % eventually
            mk_neighborhood(lists:append(Preds, Succs), BaseNode, PredsLength, SuccsLength)
    end.

%% @doc Adds nodes from the given node list to the given neighborhood structure
%%      and truncates the predecessor and successor list to the given sizes.
%%      Note: nodes which have only been present in the predecessor (successor)
%%      list may now also appear in the successor (predecessor) list if a list
%%      has been too small.
-spec add_nodes(Neighbors::neighborhood(), NodeList::[node:node_type()],
                PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
add_nodes(Neighbors, [NodeToAdd], PredsLength, SuccsLength) ->
    add_node(Neighbors, NodeToAdd, PredsLength, SuccsLength);
add_nodes(Neighbors, [_|_] = NodeList, PredsLength, SuccsLength) ->
    % note: similar to mk_neighborhood/4 and merge/4
    [Node | NeighborsView] = to_list(Neighbors),
    ?DBG_ASSERT(Node =:= ?MODULE:node(Neighbors)),
    {SmallerSorted, EqualSorted, LargerSorted} =
        lsplit_nodelist(NodeList, Node),

    OtherView = if (LargerSorted =:= []) andalso (SmallerSorted =:= []) ->
                       [N || N <- EqualSorted, not node:same_process(N, Node)];
                   true ->
                       lists:append(LargerSorted, SmallerSorted)
                end,
    lmerge_helper(NeighborsView, OtherView, Node, PredsLength, SuccsLength);
add_nodes(Neighbors, [], PredsLength, SuccsLength) ->
    trunc(Neighbors, PredsLength, SuccsLength).

%% @doc Updates the node IDs of the nodes in the neighborhood with the most
%%      up-to-date ID in either its own lists or the given node list.
%%      Note: throws if any node in the NodeList is the same as the base node
%%      but with an updated ID!
-spec update_ids(Neighbors::neighborhood(), NodeList::[node:node_type()]) -> neighborhood().
update_ids({Preds, BaseNode, Succs, _NodeIntv, _SuccIntv}, [_|_] = NodeList) ->
    {PredsUpd, _} = lupdate_ids(Preds, NodeList),
    {SuccsUpd, _} = lupdate_ids(Succs, NodeList),
    _ = [throw_if_newer(N, BaseNode) || N <- NodeList, node:same_process(BaseNode, N)],
    % due to updated IDs, the lists might not be sorted anymore...
    SuccOrd = fun(N1, N2) -> succ_ord_node(N1, N2, BaseNode) end,
    PredOrd = fun(N1, N2) -> succ_ord_node(N2, N1, BaseNode) end,
    SuccsUpdSorted = lists:usort(SuccOrd, SuccsUpd),
    PredsUpdSorted = lists:usort(PredOrd, PredsUpd),
    ensure_lists_not_empty(PredsUpdSorted, BaseNode, SuccsUpdSorted);
update_ids(Neighbors, []) ->
    Neighbors.

%% @doc Defines that N1 is less than or equal to N2 if their IDs are (provided
%%      for convenience).
-spec succ_ord_node(N1::node:node_type(), N2::node:node_type()) -> boolean().
succ_ord_node(N1, N2) -> succ_ord_id(node:id(N1), node:id(N2)).

%% @doc Defines that K1 is less than or equal to K2 if their IDs are.
-spec succ_ord_id(K1::?RT:key(), K2::?RT:key()) -> boolean().
succ_ord_id(K1, K2) -> K1 =< K2.

%% @doc Defines a 'less than or equal' order starting from a base node going
%%      along the ring towards the successor where nodes that are further away
%%      are said to be larger than nodes with smaller distances.
-spec succ_ord_node(node:node_type(), node:node_type(), BaseNode::node:node_type()) -> boolean().
succ_ord_node(N1, N2, BaseNode) ->
    succ_ord_id(node:id(N1), node:id(N2), node:id(BaseNode)).

%% @doc Defines a 'less than or equal' order starting from a base node going
%%      along the ring towards the successor where nodes that are further away
%%      are said to be larger than nodes with smaller distances.
-spec succ_ord_id(K1::?RT:key(), K2::?RT:key(), BaseKey::?RT:key()) -> boolean().
succ_ord_id(K1, K2, BaseKey) ->
    % more understandable version:
%%     (K1 > BaseKey andalso K2 > BaseKey andalso K1 =< K2) orelse
%%     (K1 < BaseKey andalso K2 < BaseKey andalso K1 =< K2) orelse
%%     (K1 > BaseKey andalso K2 < BaseKey) orelse
%%     (K1 =:= BaseKey).
    % (slightly) faster version:
    (K1 =:= BaseKey) orelse
    (K1 > BaseKey andalso K2 < BaseKey) orelse
    (K1 =< K2 andalso (K1 >= BaseKey orelse K2 < BaseKey)).

%% %%  doc Defines a 'less than or equal' order starting from a base node going
%% %%      along the ring towards the predecessor where nodes that are further away
%% %%      are said to be larger than nodes with smaller distances.
%% -spec pred_ord(node:node_type(), node:node_type(), BaseNode::node:node_type()) -> boolean().
%% pred_ord(N1, N2, BaseNode) ->
%%     BaseNodeId = node:id(BaseNode),
%%     (node:id(N1) > BaseNodeId andalso node:id(N2) > BaseNodeId andalso node:id(N1) >= node:id(N2)) orelse
%%     (node:id(N1) < BaseNodeId andalso node:id(N2) < BaseNodeId andalso node:id(N1) >= node:id(N2)) orelse
%%     (node:id(N1) < BaseNodeId andalso node:id(N2) > BaseNodeId) orelse
%%     (node:id(N1) =:= BaseNodeId).

%% @doc Creates a dictionary mapping a node PID to the newest version of its
%%      node object from the given lists.
-spec create_pid_to_node_dict(Dict, NodeLists::[[node:node_type()]]) -> Dict
        when is_subtype(Dict, dict:dict(comm:mypid(), node:node_type())).
create_pid_to_node_dict(Dict, []) ->
    dict:map(fun(_PidX, [NodeX]) -> NodeX;
                (_PidX, [H | RestX]) ->
                     lists:foldl(fun node:newer/2, H, RestX)
             end, Dict);
create_pid_to_node_dict(Dict, [NodeList | Rest]) ->
    DictNew = lists:foldl(
                fun(NodeX, DictX) ->
                        case node:is_valid(NodeX) of
                            true ->
                                dict:append(node:pidX(NodeX), NodeX, DictX);
                            false ->
                                DictX
                        end
                end, Dict, NodeList),
    create_pid_to_node_dict(DictNew, Rest).

%% @doc Removes any node with outdated ID information from the list as well as
%%      any outdated node that shares the same process as Node and any invalid
%%      node.
-spec lremove_outdated(NodeList::[node:node_type()], Node::node:node_type() | null)
        -> [node:node_type()].
lremove_outdated(NodeList, Node) ->
    % make a unique set of updated pids:
    UpdNodes = create_pid_to_node_dict(dict:new(), [NodeList]),
    % now remove all out-dated nodes:
    NodeIsUpToDate = fun(N) ->
                             NInDict = dict:fetch(node:pidX(N), UpdNodes),
                             not node:is_newer(NInDict, N)
                     end,
    NodeListUpd = [N || N <- NodeList, node:is_valid(N),
                        not (node:same_process(N, Node) andalso (node:is_newer(Node, N))),
                        NodeIsUpToDate(N)],
    NodeListUpd.

%% @doc Removes any node with outdated ID information from the list as well as
%%      any invalid node.
-spec lremove_outdated(NodeList::[node:node_type()]) -> [node:node_type()].
lremove_outdated(NodeList) ->
     lremove_outdated(NodeList, node:null()).

%% @doc Updates the node IDs of the nodes in both lists with the most up-to-date
%%      ID in any of the two lists. The returned lists are in the same order as
%%      as the input lists and may now contain duplicates (we could not decide
%%      which to remove here!). Note that due to the updated IDs the order might
%%      not be correct, i.e. according to some ordering function, anymore!
-spec lupdate_ids(L1::[node:node_type()], L2::[node:node_type()])
        -> {L1Upd::[node:node_type()], L2Upd::[node:node_type()]}.
lupdate_ids(L1, L2) ->
    % make a unique set of updated pids:
    UpdNodes = create_pid_to_node_dict(dict:new(), [L1, L2]),

    GetNewNode = fun(Node) -> dict:fetch(node:pidX(Node), UpdNodes) end,
    L1Upd = lists:map(GetNewNode, L1),
    L2Upd = lists:map(GetNewNode, L2),

    {L1Upd, L2Upd}.

%% @doc Returns whether NId is between MyId and Id and not equal to Id.
%%      If MyId and Id are the same, every other id is less than Id.
-spec less_than_id(NId::?RT:key(), Id::?RT:key(), MyId::?RT:key()) -> boolean().
less_than_id(NId, Id, MyId) ->
    % note: succ_ord_id = less than or equal
    (NId =/= Id) andalso (MyId =:= Id orelse
                              succ_ord_id(NId, Id, MyId)).

%% @doc Look-up largest node in the NodeList that has an ID smaller than Id.
%%      NodeList must be sorted with the largest key first (order of a
%%      neighborhood's predecessor list).
%%      Note: this implementation does not use intervals because comparing keys
%%      with succ_ord is (slightly) faster. Also this is faster than a
%%      lists:fold*/3.
-spec best_node_maxfirst(MyId::?RT:key(), Id::?RT:key(), NodeList::snodelist(),
                         LastFound::node:node_type()) -> Result::node:node_type().
best_node_maxfirst(MyId, Id, [H | T], LastFound) ->
    % note: succ_ord_id = less than or equal
    HId = node:id(H),
    case less_than_id(HId, Id, MyId) of
        false -> best_node_maxfirst(MyId, Id, T, LastFound);
        _     -> case succ_ord_id(node:id(LastFound), HId, Id) of
                     true -> H;
                     _    -> LastFound
                 end
    end;
best_node_maxfirst(_MyId, _Id, [], LastFound) -> LastFound.

%% @doc Similar to best_node_maxfirst/4 but with a NodeList that must be sorted
%%      with the smallest key first (order of a neighborhood's successor list).
-spec best_node_minfirst(MyId::?RT:key(), Id::?RT:key(), NodeList::snodelist(),
                         LastFound1::node:node_type(), LastFound2::node:node_type())
        -> Result::node:node_type().
best_node_minfirst(MyId, Id, [H | T], LastFound1, LastFound2) ->
    case less_than_id(node:id(H), Id, MyId) of
        true -> best_node_minfirst(MyId, Id, T, H, LastFound2);
        _    -> best_node_minfirst(MyId, Id, [], LastFound1, LastFound2)
    end;
best_node_minfirst(_MyId, Id, [], LastFound1, LastFound2) ->
    % note: succ_ord_id = less than or equal
    case succ_ord_id(node:id(LastFound1), node:id(LastFound2), Id) of
        true -> LastFound2;
        _    -> LastFound1
    end.

%% @doc Returns the node among all know neighbors (including the base node)
%%      with the largest id smaller than Id.
-spec largest_smaller_than(Neighbors::neighborhood(), Id::?RT:key()) -> node:node_type() | null.
largest_smaller_than({Preds, BaseNode, Succs, _NodeIntv, _SuccIntv}, Id) ->
    BaseNodeId = node:id(BaseNode),
    case BaseNodeId =/= Id of
        true -> largest_smaller_than(Preds, BaseNode, Succs, Id, BaseNode);
        _ -> % take first pred if valid
            Pred = hd(Preds),
            case node:id(Pred) =/= Id of
                true -> Pred;
                _    -> node:null()
            end
    end.

%% @doc Returns the node among all know neighbors (including the base node)
%%      with the largest id smaller than Id given that a previous search found
%%      LastFound as its Node with the largest id smaller than id.
-spec largest_smaller_than(Neighbors::neighborhood(), Id::?RT:key(),
                           LastFound::node:node_type()) -> node:node_type().
largest_smaller_than({Preds, BaseNode, Succs, _NodeIntv, _SuccIntv}, Id, LastFound) ->
    largest_smaller_than(Preds, BaseNode, Succs, Id, LastFound).

%% @doc Helper for largest_smaller_than/2 and largest_smaller_than/3.
-spec largest_smaller_than(Preds::non_empty_snodelist(), BaseNode::node:node_type(),
                           Succs::non_empty_snodelist(), Id::?RT:key(),
                           LastFound::node:node_type()) -> node:node_type().
largest_smaller_than(Preds, BaseNode, Succs, Id, LastFound) ->
    MyId = node:id(BaseNode),
    BestSucc = best_node_minfirst(MyId, Id, [BaseNode | Succs], LastFound, LastFound),
    _Best = best_node_maxfirst(MyId, Id, Preds, BestSucc).
