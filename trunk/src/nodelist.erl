%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

-export([% constructors:
         new_neighborhood/1, new_neighborhood/2, new_neighborhood/3,
         mk_neighborhood/2, mk_neighborhood/4,
         mk_nodelist/2,
         
         % getters:
         node/1, nodeid/1, pred/1, preds/1, succ/1, succs/1,
         has_real_pred/1, has_real_succ/1,
         
         % modifiers:
         trunc/3, trunc_preds/2, trunc_succs/2,
         remove/2, remove/3, filter/2, filter/3,
         lremove/2, lremove/3,lfilter/2, lfilter/3,
         lfilter_min_length/3, filter_min_length/4,
         merge/4, add_node/4, add_nodes/4,
         
         % converters:
         to_list/1,
         
         % miscellaneous:
         lupdate_ids/2, lremove_outdated/1, lremove_outdated/2]).

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([neighborhood/0, snodelist/0, non_empty_snodelist/0]).
-endif.

-type(snodelist() :: [node:node_type()]). % (sorted) node list
-type(non_empty_snodelist() :: [node:node_type(),...]). % non-empty (sorted) node list
-opaque(neighborhood() :: {Preds::non_empty_snodelist(), Node::node:node_type(), Succs::non_empty_snodelist()}).

%% @doc Helper function that throws an exception if the given neighbor is newer
%%      than the BaseNode (requires that both are equal!).
-spec throw_if_newer(Neighbor::node:node_type(), BaseNode::node:node_type()) -> ok.
throw_if_newer(Neighbor, BaseNode) ->
    case BaseNode =/= node:newer(Neighbor, BaseNode) of
        true ->
            throw('cannot create a neighborhood() with a neighbor newer than the node itself');
        false ->
            ok
    end.

%% @doc Creates a new neighborhood structure for the given node.
-spec new_neighborhood(Node::node:node_type()) -> neighborhood().
new_neighborhood(Node) ->
    {[Node], Node, [Node]}.

%% @doc Creates a new neighborhood structure for the given node and a neighbor
%%      (this will be its predecessor and successor).
-spec new_neighborhood(Node::node:node_type(), Neighbor::node:node_type()) -> neighborhood().
new_neighborhood(Node, Neighbor) ->
    case node:equals(Node, Neighbor) of
        true ->
            % the node should always be the first to get a new ID!
            % if not, something went wrong
            throw_if_newer(Neighbor, Node),
            {[Node], Node, [Node]};
        false ->
            {[Neighbor], Node, [Neighbor]}
    end.

%% @doc Creates a new neighborhood structure for the given node, its predecessor
%%      and successor. If the order is wrong, Pred and Succ will be exchanged.
%%      (provided for convenience - special case of mk_neighborhood)
-spec new_neighborhood(Pred::node:node_type(), Node::node:node_type(), Succ::node:node_type()) -> neighborhood().
new_neighborhood(Pred, Node, Succ) ->
    case node:equals(Pred, Node) of
        true ->
            % the node should always be the first to get a new ID!
            % if not, something went wrong
            throw_if_newer(Pred, Node),
            new_neighborhood(Node, Succ);
        false ->
            case node:equals(Succ, Node) of
                true ->
                    throw_if_newer(Succ, Node),
                    {[Pred], Node, [Pred]};
                false ->
                    case node:equals(Pred, Succ) of
                        true ->
                            NewerNode = node:newer(Pred, Succ),
                            {[NewerNode], Node, [NewerNode]};
                        false ->
                            % distinct nodes -> determine order:
                            case succ_ord(Pred, Succ, Node) of
                                true  -> {[Succ], Node, [Pred]};
                                false -> {[Pred], Node, [Succ]}
                            end
                    end
            end
    end.

%% @doc Creates a new neighborhood structure with the given elements.
%%      Only for internal use (to make dialyzer happy with opaque types).
%%      Note: Call this method when passing newly created neighborhood objects
%%      to functions that expect those. Public functions, for example, work on
%%      opaque neighborhood structures. Dialyzer will complain if you provide
%%      the tuple yourself.
-spec new(Preds::non_empty_snodelist(), Node::node:node_type(), Succs::non_empty_snodelist()) -> neighborhood().
new(Preds, Node, Succs) ->
    {Preds, Node, Succs}.

%% @doc Helper function to make sure a (temporary) neighborhood object has
%%      non-empty predecessor and successor lists (fills them with itself if
%%      necessary).
-spec ensure_lists_not_empty(Neighborhood::neighborhood() | {Preds::snodelist(), Node::node:node_type(), Succs::snodelist()}) -> neighborhood().
ensure_lists_not_empty({[], Node, []}) ->
    {[Node], Node, [Node]};
ensure_lists_not_empty({Preds, Node, Succs}) ->
    NewPreds = case Preds of
                   [] -> [lists:last(Succs)];
                   _  -> Preds
               end,
    NewSuccs = case Succs of
                   [] -> [lists:last(Preds)];
                   _  -> Succs
               end,
    {NewPreds, Node, NewSuccs}.

%% @doc Truncates the given neighborhood's predecessor and successor lists to
%%      the given sizes.
-spec trunc(Neighborhood::neighborhood(), PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
trunc({Preds, Node, Succs}, PredsLength, SuccsLength) when (PredsLength > 0) andalso (SuccsLength > 0) ->
    {lists:sublist(Preds, PredsLength), Node, lists:sublist(Succs, SuccsLength)}.

%% @doc Truncates the given neighborhood's predecessor list to the given size.
-spec trunc_preds(neighborhood(), PredsLength::pos_integer()) -> neighborhood().
trunc_preds({Preds, Node, Succs}, PredsLength) when (PredsLength > 0) ->
    {lists:sublist(Preds, PredsLength), Node, Succs}.

%% @doc Truncates the given neighborhood's successor list to the given size.
-spec trunc_succs(neighborhood(), SuccsLength::pos_integer()) -> neighborhood().
trunc_succs({Preds, Node, Succs}, SuccsLength) when (SuccsLength > 0) ->
    {Preds, Node, lists:sublist(Succs, SuccsLength)}.

%% @doc Returns the neighborhood's node.
-spec node(neighborhood()) -> node:node_type().
node({_Preds, Node, _Succs}) ->
    Node.

%% @doc Returns the ID of the neighborhood's node (provided for convenience).
-spec nodeid(neighborhood()) -> ?RT:key().
nodeid({_Preds, Node, _Succs}) ->
    node:id(Node).

%% @doc Returns the neighborhood's predecessor list.
-spec preds(neighborhood()) -> non_empty_snodelist().
preds({Preds, _Node, _Succs}) ->
    Preds.

%% @doc Returns a neighborhood's or a node lists's predecessor (may be the node
%%      itself).
-spec pred(neighborhood()) -> node:node_type().
pred({[Pred | _], _Node, _Succs}) ->
    Pred.

%% @doc Returns whether the neighborhood contains a real predecessor (one not
%%      equal to the own node) or not (provided for convenience).
-spec has_real_pred(neighborhood()) -> boolean().
has_real_pred({[Pred | _], Node, _Succs}) ->
    Pred =/= Node.

%% @doc Returns the neighborhood's successor list.
-spec succs(neighborhood()) -> non_empty_snodelist().
succs({_Preds, _Node, Succs}) ->
    Succs.

%% @doc Returns the neighborhood's or a node lists's successor (may be the node
%%      itself).
-spec succ(neighborhood()) -> node:node_type().
succ({_Preds, _Node, [Succ | _]}) ->
    Succ.

%% @doc Returns whether the neighborhood contains a real predecessor (one not
%%      equal to the own node) or not (provided for convenience).
-spec has_real_succ(neighborhood()) -> boolean().
has_real_succ({_Preds, Node, [Succ | _]}) ->
    Succ =/= Node.

%% @doc Splits the given (unsorted) node list into sorted lists with nodes that
%%      have smaller, equal and larger IDs than the given node. The NodeList may
%%      contain duplicates, i.e. nodes with the same pid but different IDs (and
%%      IDVersions).
-spec lsplit_nodelist(NodeList::[node:node_type()], Node::node:node_type()) -> {Smaller::snodelist(), Equal::snodelist(), Larger::snodelist()}.
lsplit_nodelist(NodeList, Node) ->
    lusplit_nodelist(lremove_outdated(NodeList, Node), Node).

%% @doc Splits the given (unsorted) unique node list (no duplicates, i.e. nodes
%%      with the same pid but different IDs (and IDVersions)) into sorted lists
%%      with nodes that have smaller, equal and larger IDs than the given node.
-spec lusplit_nodelist(NodeList::[node:node_type()], Node::node:node_type()) -> {Smaller::snodelist(), Equal::snodelist(), Larger::snodelist()}.
lusplit_nodelist(NodeList, Node) ->
    {Smaller, LargerOrEqual} =
        lists:partition(
          fun(N) ->
                  case node:equals(N, Node) of
                      true ->
                          throw_if_newer(N, Node),
                          false;
                      false ->
                          node:id(N) < node:id(Node)
                  end
          end, NodeList),

    SmallerSorted = lists:usort(fun succ_ord/2, Smaller),
    {EqualSorted, LargerSorted} =
        lists:splitwith(fun(N) -> node:id(N) =:= node:id(Node) end,
                        lists:usort(fun succ_ord/2, LargerOrEqual)),
    {SmallerSorted, EqualSorted, LargerSorted}.

%% @doc Creates a sorted nodelist starting at the given node and going clockwise
%%      along the ring (also see succ_ord/3).
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

    case (LargerSorted =:= []) andalso (SmallerSorted =:= []) of
        true ->
            Preds = lists:reverse(EqualSorted),
            Succs = EqualSorted;
        false ->
            Neighbors = lists:append([LargerSorted, SmallerSorted]),
            Preds = lists:reverse(Neighbors),
            Succs = Neighbors
    end,
    ensure_lists_not_empty({Preds, Node, Succs}).

%% @doc Removes the given node (or node with the given Pid) from a node list
%%      (provided for convenience - see lfilter/2).
-spec lremove(NodeOrPid::node:node_type() | comm:mypid() | pid(), snodelist()) -> snodelist().
lremove(NodeOrPid, NodeList) ->
    lfilter(NodeList, fun(N) -> not node:equals(NodeOrPid, N) end).

%% @doc Removes the given node (or node with the given Pid) from a neighborhood
%%      (provided for convenience - see filter/2).
%%      Note: A neighborhood's base node is never removed!
-spec remove(NodeOrPid::node:node_type() | comm:mypid() | pid(), neighborhood()) -> neighborhood().
remove(NodeOrPid, Neighborhood) ->
    filter(Neighborhood, fun(N) -> not node:equals(NodeOrPid, N) end).

%% @doc Removes the given node (or node with the given Pid) from a node list
%%      and executes EvalFun for any such occurrence (provided for convenience
%%      - see filter/3).
-spec lremove(NodeOrPid::node:node_type() | comm:mypid() | pid(), snodelist(), EvalFun::fun((node:node_type()) -> any())) -> snodelist().
lremove(NodeOrPid, NodeList, EvalFun) ->
    lfilter(NodeList, fun(N) -> not node:equals(NodeOrPid, N) end, EvalFun).

%% @doc Removes the given node (or node with the given Pid) from a neighborhood
%%      executes EvalFun for any such occurrence (provided for convenience -
%%      see filter/3).
%%      Note: A neighborhood's base node is never removed!
-spec remove(NodeOrPid::node:node_type() | comm:mypid() | pid(), neighborhood(), EvalFun::fun((node:node_type()) -> any())) -> neighborhood().
remove(NodeOrPid, Neighborhood, EvalFun) ->
    filter(Neighborhood, fun(N) -> not node:equals(NodeOrPid, N) end, EvalFun).

%% @doc Keeps any node for which FilterFun returns true in a node list.
-spec lfilter(snodelist(), FilterFun::fun((node:node_type()) -> boolean())) -> snodelist().
lfilter(NodeList, FilterFun) ->
    [N || N <- NodeList, FilterFun(N)].

%% @doc Keeps any node for which FilterFun returns true in a neighborhood.
%%      Note: A neighborhood's base node is never removed!
-spec filter(neighborhood(), FilterFun::fun((node:node_type()) -> boolean())) -> neighborhood().
filter({Preds, Node, Succs}, FilterFun) ->
    NewNeighbors = {lfilter(Preds, FilterFun), Node, lfilter(Succs, FilterFun)},
    ensure_lists_not_empty(NewNeighbors).

%% @doc Keeps any node for which FilterFun returns true in a node list and
%%      executes EvalFun for any other node.
-spec lfilter(snodelist(), FilterFun::fun((node:node_type()) -> boolean()), EvalFun::fun((node:node_type()) -> any())) -> snodelist().
lfilter(NodeList, FilterFun, EvalFun) ->
    {Satisfying, NonSatisfying} = lists:partition(FilterFun, NodeList),
    lists:map(EvalFun, NonSatisfying),
    Satisfying.

%% @doc Keeps any node for which FilterFun returns true in a neighborhood
%%      and executes EvalFun for any other node.
%%      Note: A neighborhood's base node is never removed!
-spec filter(neighborhood(), FilterFun::fun((node:node_type()) -> boolean()), EvalFun::fun((node:node_type()) -> any())) -> neighborhood().
filter({Preds, Node, Succs}, FilterFun, EvalFun) ->
    NewNeighbors = {lfilter(Preds, FilterFun, EvalFun), Node, lfilter(Succs, FilterFun, EvalFun)},
    ensure_lists_not_empty(NewNeighbors).

%% @doc Keeps any node for which FilterFun returns true in a node list but
%%      produces a node list with at least MinLength elements by adding enough
%%      unmatching nodes in the order of the list.
-spec lfilter_min_length(snodelist(), FilterFun::fun((node:node_type()) -> boolean()), MinLength::non_neg_integer()) -> snodelist().
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
-spec filter_min_length(neighborhood(), FilterFun::fun((node:node_type()) -> boolean()), MinPredsLength::non_neg_integer(), MinSuccsLength::non_neg_integer()) -> neighborhood().
filter_min_length({Preds, Node, Succs}, FilterFun, MinPredsLength, MinSuccsLength) ->
    NewNeighbors = {lfilter_min_length(Preds, FilterFun, MinPredsLength), Node, lfilter_min_length(Succs, FilterFun, MinSuccsLength)},
    ensure_lists_not_empty(NewNeighbors).

%% @doc Helper function that removes the head of NodeList if is is equal to
%%      Node (using node:equals/2).
-spec lremove_head_if_eq(NodeList::snodelist(), Node::node:node_type()) -> snodelist().
lremove_head_if_eq([] = NodeList, _Node) ->
    NodeList;
lremove_head_if_eq([H | T] = NodeList, Node) ->
    case node:equals(H, Node) of
        true  -> T;
        false -> NodeList
    end.

%% @doc Converts a neighborhood to a sorted list of nodes including the
%%      predecessors, the node and its successors. The first element of the
%%      resulting list will be the node itself, afterwards every known node
%%      along the ring towards the first node.
-spec to_list(neighborhood()) -> non_empty_snodelist().
to_list({Preds, Node, Succs}) ->
    Ord = fun(N1, N2) -> succ_ord(N1, N2, Node) end,
    CleanPreds = lremove_head_if_eq(Preds, Node),
    CleanSuccs = lremove_head_if_eq(Succs, Node),
    CleanPredsReversed = lists:reverse(CleanPreds),
    [Node | util:smerge2(CleanSuccs, CleanPredsReversed, Ord)].

%% @doc Removes NodeToRemove from the given list and additionally gets the
%%      resulting list's last element.
-spec lget_last_and_remove(NodeList::snodelist(), NodeToRemove::node:node_type(), ResultList::snodelist()) ->
        {LastNode::node:node_type(), FilteredList::non_empty_snodelist()} |
        {null, []}.
lget_last_and_remove([], _NodeToRemove, []) ->
    {node:null(), []};
lget_last_and_remove([], _NodeToRemove, [Last | _] = ResultList) ->
    {Last, lists:reverse(ResultList)};
lget_last_and_remove([H | T], NodeToRemove, ResultList) ->
    case node:equals(H, NodeToRemove) of
        true ->
            lget_last_and_remove(T, NodeToRemove, ResultList);
        false ->
            lget_last_and_remove(T, NodeToRemove, [H | ResultList])
    end.

%% @doc Helper function that adds NewHead to the head of NodeList if is is not
%%      equal to Node (using node:equals/2).
-spec ladd_head_if_noteq(NewHead::node:node_type(), NodeList::snodelist(), Node::node:node_type()) -> snodelist().
ladd_head_if_noteq(NewHead, NodeList, Node) ->
    case node:equals(NewHead, Node) of
        true  -> NodeList;
        false -> [NewHead | NodeList]
    end.

%% @doc Rebases a sorted node list (as returned by to_list/1, for example) to
%%      the sorted list based on a new first node (without including it).
%%      Removes the NewFirstNode from this list on the fly.
-spec lrebase_list(non_empty_snodelist(), NewFirstNode::node:node_type()) -> snodelist().
lrebase_list([First] = NodeList, NewFirstNode) ->
    case node:equals(First, NewFirstNode) of
        true  -> [];
        false -> NodeList
    end;
lrebase_list([NewFirstNode | T], NewFirstNode) ->
    lremove(NewFirstNode, T); % just to be sure, remove NewFirstNode from the Tail
lrebase_list([First | T], NewFirstNode) ->
    {LastNode, TFilt} = lget_last_and_remove(T, NewFirstNode, []),
    case LastNode =/= null of
        true ->
            NodeListInterval = intervals:mk_from_nodes(First, LastNode),
            NewPredsInterval = intervals:mk_from_nodes(First, NewFirstNode),
            case intervals:in(node:id(NewFirstNode), NodeListInterval) of
                false ->
                    ladd_head_if_noteq(First, TFilt, NewFirstNode);
                true->
                    {L1T, L2} = lists:splitwith(fun(N) -> intervals:in(node:id(N), NewPredsInterval) end, TFilt),
                    L1 = ladd_head_if_noteq(First, L1T, NewFirstNode),
                    lists:append(L2, L1)
            end;
        false ->
            ladd_head_if_noteq(First, [], NewFirstNode)
    end.

%% @doc Merges two lists of nodes into a neighborhood structure with the given
%%      node. Predecessor and successor lists are truncated to the given sizes,
%%      node IDs are updated with the most up-to-date ID from any list.
%%      Neither Node1View nor Node2View should contain BaseNode!
-spec lmerge_helper(Node1View::snodelist(), Node2View::snodelist(), BaseNode::node:node_type(), PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
lmerge_helper(Node1View, Node2View, BaseNode, PredsLength, SuccsLength) ->
    {Node1ViewUpd, Node2ViewUpd} = lupdate_ids(Node1View, Node2View),
    % due to updated IDs, the lists might not be sorted anymore...
    Ord = fun(N1, N2) -> succ_ord(N1, N2, BaseNode) end,
    Node1ViewUpdSorted = lists:usort(Ord, Node1ViewUpd),
    Node2ViewUpdSorted = lists:usort(Ord, Node2ViewUpd),

    MergedView = util:smerge2(Node1ViewUpdSorted, Node2ViewUpdSorted, Ord),
    Preds = lists:sublist(lists:reverse(MergedView), PredsLength),
    Succs = lists:sublist(MergedView, SuccsLength),
    ensure_lists_not_empty({Preds, BaseNode, Succs}).

%% @doc Merges nodes of Neighbors2 into Neighbors1 and truncates the predecessor
%%      and successor lists to the given sizes. 
-spec merge(Neighbors1::neighborhood(), Neighbors2::neighborhood(), PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
merge(Neighbors1, Neighbors2, PredsLength, SuccsLength) ->
    % note: similar to mk_neighborhood/4
    Node1 = ?MODULE:node(Neighbors1),
    % create a sorted list of nodes in Neighbors1 and Neighbours2
    [Node1 | Neighbors1View] = to_list(Neighbors1),
    Neighbors2View = lrebase_list(to_list(Neighbors2), Node1),
    lmerge_helper(Neighbors1View, Neighbors2View, Node1, PredsLength, SuccsLength).

%% @doc Adds a node to a neighborhood structure and truncates the predecessor
%%      and successor list to the given sizes.
%%      Note: nodes which have only been present in the predecessor (successor)
%%      list may now also appear in the successor (predecessor) list if a list
%%      has been too small.
-spec add_node(Neighbors::neighborhood(), NodeToAdd::node:node_type(), PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
add_node({Preds, BaseNode, Succs}, NodeToAdd, PredsLength, SuccsLength) ->
    case node:equals(BaseNode, NodeToAdd) of
        true ->
            throw_if_newer(NodeToAdd, BaseNode),
            % eventually
            mk_neighborhood(lists:append(Preds, Succs), BaseNode, PredsLength, SuccsLength);
        false ->
            CleanPreds = lremove_head_if_eq(Preds, BaseNode),
            CleanSuccs = lremove_head_if_eq(Succs, BaseNode),
            UpdateFun = fun(N) ->
                                case node:equals(N, NodeToAdd) of
                                    true -> node:newer(N, NodeToAdd);
                                    _    -> N
                                end
                        end,
            % create a view of all know (and updated) nodes:
            ViewUpd = lists:append([NodeToAdd | lists:map(UpdateFun, CleanPreds)], lists:map(UpdateFun, CleanSuccs)),
            % sort the list again
            SuccOrd = fun(N1, N2) -> succ_ord(N1, N2, BaseNode) end,
            SuccsUpdSorted = lists:usort(SuccOrd, ViewUpd),
            PredsUpdSorted = lists:reverse(SuccsUpdSorted),
            
            ensure_lists_not_empty(trunc(new(PredsUpdSorted, BaseNode, SuccsUpdSorted), PredsLength, SuccsLength))
    end.

%% @doc Adds nodes from the given node list to the given neighborhood structure
%%      and truncates the predecessor and successor list to the given sizes.
%%      Note: nodes which have only been present in the predecessor (successor)
%%      list may now also appear in the successor (predecessor) list if a list
%%      has been too small.
-spec add_nodes(Neighbors::neighborhood(), NodeList::[node:node_type()], PredsLength::pos_integer(), SuccsLength::pos_integer()) -> neighborhood().
add_nodes(Neighbors, [], PredsLength, SuccsLength) ->
    trunc(Neighbors, PredsLength, SuccsLength);
add_nodes(Neighbors, [NodeToAdd], PredsLength, SuccsLength) ->
    add_node(Neighbors, NodeToAdd, PredsLength, SuccsLength);
add_nodes(Neighbors, [_|_] = NodeList, PredsLength, SuccsLength) ->
    % note: similar to mk_neighborhood/4 and merge/4
    Node = ?MODULE:node(Neighbors),
    [Node | NeighborsView] = to_list(Neighbors),
    {SmallerSorted, EqualSorted, LargerSorted} =
        lsplit_nodelist(NodeList, Node),

    OtherView = case (LargerSorted =:= []) andalso (SmallerSorted =:= []) of
                    true ->
                        [N || N <- EqualSorted, not node:equals(N, Node)];
                    false ->
                        lists:append([LargerSorted, SmallerSorted])
                end,
    lmerge_helper(NeighborsView, OtherView, Node, PredsLength, SuccsLength).

%% @doc Defines that N1 is less than or equal to N2 if their IDs are.
-spec succ_ord(N1::node:node_type(), N2::node:node_type()) -> boolean().
succ_ord(N1, N2) ->
    node:id(N1) =< node:id(N2).

%% @doc Defines a 'less than or equal' order starting from a base node going
%%      along the ring towards the successor where nodes that are further away
%%      are said to be larger than nodes with smaller distances.
-spec succ_ord(node:node_type(), node:node_type(), BaseNode::node:node_type()) -> boolean().
succ_ord(N1, N2, BaseNode) ->
    BaseNodeId = node:id(BaseNode),
    (node:id(N1) > BaseNodeId andalso node:id(N2) > BaseNodeId andalso node:id(N1) =< node:id(N2)) orelse
    (node:id(N1) < BaseNodeId andalso node:id(N2) < BaseNodeId andalso node:id(N1) =< node:id(N2)) orelse
    (node:id(N1) > BaseNodeId andalso node:id(N2) < BaseNodeId) orelse
    (node:id(N1) =:= BaseNodeId).

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

%% @doc Inserts or updates the node in Table and returns the newer node from the
%%      (potentially) existing entry and the to-be-inserted one.
-spec ets_insert_newer_node(Table::tid() | atom(), node:node_type()) -> node:node_type().
ets_insert_newer_node(Table, Node) ->
    case node:is_valid(Node) of
        true ->
            EtsItem = {node:pidX(Node), Node},
            case ets:insert_new(Table, EtsItem) of
                false ->
                    Previous = ets:lookup_element(Table, node:pidX(Node), 2),
                    NewerNode = node:newer(Node, Previous),
                    case NewerNode =/= Previous of
                        true  ->
                            ets:insert(Table, EtsItem),
                            NewerNode;
                        false ->
                            NewerNode
                    end;
                true ->
                    Node
            end;
        false -> Node
    end.

%% @doc Removes any node with outdated ID information from the list as well as
%%      any outdated node that equals Node and any invalid node.
-spec lremove_outdated(NodeList::[node:node_type()], Node::node:node_type() | null) -> [node:node_type()].
lremove_outdated(NodeList, Node) ->
    Tab = ets:new(nodelist_helper_lremove_outdated, [set, private]),
    % make a unique table of updated pids:
    EtsInsertNewerNodeFun = fun(N) -> ets_insert_newer_node(Tab, N) end,
    lists:map(EtsInsertNewerNodeFun, NodeList),
    % now remove all out-dated nodes:
    NodeIsUpToDate = fun(N) ->
                             NInTab = ets:lookup_element(Tab, node:pidX(N), 2),
                             (node:newer(N, NInTab) =:= N)
                     end,
    NodeListUpd = [N || N <- NodeList, node:is_valid(N),
                        not (node:equals(N, Node) andalso (node:newer(N, Node) =:= Node)), NodeIsUpToDate(N)],
    ets:delete(Tab),
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
-spec lupdate_ids(L1::[node:node_type()], L2::[node:node_type()]) -> {L1Upd::[node:node_type()], L2Upd::[node:node_type()]}.
lupdate_ids(L1, L2) ->
    L1L2Tab = ets:new(nodelist_helper_lupdate_ids, [set, private]),
    % make a unique table of updated pids:
    EtsInsertNewerNodeFun = fun(N) -> ets_insert_newer_node(L1L2Tab, N) end,
    lists:map(EtsInsertNewerNodeFun, L1),
    lists:map(EtsInsertNewerNodeFun, L2),
    
    GetNewNode = fun(Node) -> ets:lookup_element(L1L2Tab, node:pidX(Node), 2) end,
    L1Upd = lists:map(GetNewNode, L1),
    L2Upd = lists:map(GetNewNode, L2),

    ets:delete(L1L2Tab),
    {L1Upd, L2Upd}.
