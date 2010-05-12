%  @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    cyclon_cache.erl
%%% @author Christian Hennig <hennig@zib.de>
%%% @doc    Cyclon node cache implementation using a list.
%%% @end
%%% Created :  1 Dec 2008 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

-module(cyclon_cache).
-author('hennig@zib.de').
-vsn('$Id $ ').

-include("scalaris.hrl").

%% API

-export([new/0, new/2, size/1,
         add_node/3, remove_node/2, trim/2,
         get_random_subset/2, get_random_nodes/2,
         get_nodes/1, get_ages/1,
         inc_age/1, merge/5,
         pop_random_node/1, pop_oldest_node/1,
         debug_format_by_age/1]).

-type age() :: non_neg_integer().
-type element() :: {node:node_type(), age()}.
-type cache() :: [ element() ].

%% @doc Creates a new and empty node cache.
-spec new() -> cache().
new() ->
    [].

%% @doc Creates a new node cache with the given two nodes and ages 0.
-spec new(node_details:node_type(), node_details:node_type()) -> cache().
new(Node1, Node2) ->
    case node:equals(Node1, Node2) of
        true  -> [{Node1, 0}];
        false -> [{Node2, 0}, {Node1, 0}]
    end.

%% @doc Counts the number of Cache entries.
-spec size(cache()) -> non_neg_integer().
size(Cache) ->
    length(Cache).

%% @doc Returns a random node from the cache.
-spec get_random_node(cache()) -> node:node_type().
get_random_node(Cache) ->
    {Node, _Age} = get_random_element(Cache),
    Node.

%% @doc Returns a random element (node and age) from the cache.
-spec get_random_element(cache()) -> element().
get_random_element(Cache) ->
    Size = cyclon_cache:size(Cache),
    N = randoms:rand_uniform(0, Size) + 1,
    % picks nth element of state
    lists:nth(N, Cache).

%% @doc Removes a random element from the (non-empty!) cache and returns the
%%      resulting cache and the removed node.
-spec pop_random_node([Cache::element(),...]) -> {NewCache::cache(), PoppedNode::node_details:node_type()}.
pop_random_node(Cache) ->
    pop_random_node(Cache, cyclon_cache:size(Cache)).

%% @doc Removes a random element from the (non-empty!) cache and returns the
%%      resulting cache and the removed node.
-spec pop_random_node(Cache::[element(),...], CacheSize::non_neg_integer()) -> {NewCache::cache(), PoppedNode::node_details:node_type()}.
pop_random_node(Cache, CacheSize) ->
    {NewCache, {Node, _Age}} = pop_random_element(Cache, CacheSize),
    {NewCache, Node}.

%% @doc Removes a random element from the (non-empty!) cache and returns the
%%      resulting cache and the removed element.
-spec pop_random_element(Cache::[element(),...], CacheSize::non_neg_integer()) -> {NewCache::cache(), PoppedElement::element()}.
pop_random_element(Cache, CacheSize) ->
    N = randoms:rand_uniform(0, CacheSize),
    % picks nth element of state
    {CacheHead, [Element | CacheTail]} = lists:split(N, Cache),
    NewCache = CacheHead ++ CacheTail,
    {NewCache, Element}.

%% @doc Returns a random subset of size N elements from the cache.
-spec get_random_subset(N::non_neg_integer(), Cache::cache()) -> RandomSubset::cache().
get_random_subset(0, _Cache) ->
    % having this special case here prevents unnecessary calls to cyclon_cache:size()
    new();
get_random_subset(N, Cache) ->
    get_random_subset_helper(N, new(), Cache, cyclon_cache:size(Cache), fun pop_random_element/2).

%% @doc Returns a random subset of size N nodes from the cache.
-spec get_random_nodes(N::non_neg_integer(), Cache::cache()) -> Nodes::[node_details:node_type()].
get_random_nodes(0, _Cache) ->
    % having this special case here prevents unnecessary calls to cyclon_cache:size()
    [];
get_random_nodes(N, Cache) ->
    get_random_subset_helper(N, new(), Cache, cyclon_cache:size(Cache), fun pop_random_node/2).

%% @doc Extracts a random element one-by-one from the Cache until a Subset of
%%      size N is created or all elements of the cache have been taken.
-spec get_random_subset_helper(N::non_neg_integer(), Result::[X], Cache::cache(), CacheSize::non_neg_integer(), PopFun::fun((Cache1::cache(), Cache1Size::non_neg_integer()) -> X)) -> cache().
get_random_subset_helper(0, Subset, _Cache, _CacheSize, _PopFun) ->
    Subset;
get_random_subset_helper(_N, Subset, [] = _Cache, _CacheSize, _PopFun) ->
    Subset;
get_random_subset_helper(N, Subset, Cache, CacheSize, PopFun) ->
    {NewCache, Element} = PopFun(Cache, CacheSize),
    get_random_subset_helper(N - 1, [Element | Subset], NewCache, CacheSize - 1, PopFun).

%% @doc Finds the oldest element (randomized if multiple oldest elements) and
%%      removes it from the cache returning the new cache and this node.
-spec pop_oldest_node(Cache::cache()) -> {NewCache::cache(), PoppedNode::node:node_type()}.
pop_oldest_node(Cache) ->
    {OldElements, _MaxAge} =
        lists:foldl(
          fun ({Node, Age}, {PrevOldElems, MaxAge}) ->
                   if Age > MaxAge ->
                          {[{Node, Age}], Age};
                      Age =:= MaxAge ->
                          {[{Node, Age} | PrevOldElems] , Age};
                      Age < MaxAge ->
                          {PrevOldElems, MaxAge}
                   end
          end,
          {[], 0},
          Cache),
    NodeP = get_random_node(OldElements),
    NewCache = remove_node(NodeP, Cache),
    {NewCache, NodeP}.

%% @doc Increases the age of every element in the cache by 1.
-spec inc_age(Cache::cache()) -> NewCache::cache().
inc_age(Cache) ->
    [{Node, Age + 1} || {Node, Age} <- Cache].

%% @doc Checks whether the cache contains an element with the given Node.
-spec contains_node(Node::node:node_type(), Cache::cache()) -> Result::boolean().
contains_node(Node, Cache) ->
    lists:any(fun({SomeNode, _Age}) -> node:equals(SomeNode, Node) end, Cache).

%% @doc Returns the ages of all nodes in the cache.
-spec get_ages(Cache::cache()) -> Ages::[age()].
get_ages(Cache) ->
    [Age || {_Node, Age} <- Cache].

%% @doc Returns all nodes in the cache (without their ages).
-spec get_nodes(Cache::cache()) -> Nodes::[node:node_type()].
get_nodes(Cache) ->
    [Node || {Node, _Age} <- Cache].

%% @doc Merges MyCache at node MyNode with the ReceivedCache from another node
%%      to whom SendCache has been send. The final cache size will not extend
%%      TargetSize.
%%      This will discard received entries pointing at MyNode and entries
%%      already contained in MyCache, fill up empty slots in the cache with
%%      received entries and further replace elements in MyCache using
%%      replace/5.
-spec merge(MyCache::cache(), MyNode::node_details:node_type(), ReceivedCache::cache(), SendCache::cache(), TargetSize::pos_integer()) -> NewCache::cache().
merge(MyCache, MyNode, ReceivedCache, SendCache, TargetSize) ->
    MyCacheSize = cyclon_cache:size(MyCache),
    ReceivedCache_Filtered =
        [Elem || {Node, _Age} = Elem <- ReceivedCache,
                 not contains_node(Node, MyCache),
                 not node:equals(Node, MyNode)],
    SendCache_Filtered =
        [Elem || {Node, _Age} = Elem <- SendCache,
                 not node:equals(Node, MyNode)],
    {MyC1, ReceivedCacheRest, AddedElements} =
        fillup(MyCache, ReceivedCache_Filtered, TargetSize - MyCacheSize),
    MyC1Size = MyCacheSize + AddedElements,
    replace(MyC1, MyC1Size, ReceivedCacheRest, SendCache_Filtered, TargetSize).

%% @doc Trims the cache to size TargetSize (if necessary) by deleting random
%%      entries as long as the cache is larger than the given TargetSize.
-spec trim(Cache::cache(), CacheSize::non_neg_integer(), TargetSize::pos_integer()) -> NewCache::cache().
trim(Cache, CacheSize, TargetSize) ->
    case CacheSize =< TargetSize of
        true ->
            Cache;
        false ->
            {NewCache, _Element} = pop_random_element(Cache, CacheSize),
            trim(NewCache, CacheSize - 1, TargetSize)
    end.

%% @doc Fills up MyCache with (up to) ToAddCount entries from ReceivedCache,
%%      returning the new cache, the rest of the ReceivedCache and the number of
%%      actually added elements.
-spec fillup(MyCache::cache(), ReceivedCache::cache(), ToAddCount::non_neg_integer()) -> {MyNewCache::cache(), ReceivedCacheRest::cache(), AddedElements::non_neg_integer()}.
fillup(MyCache, ReceivedCache, ToAddCount) ->
    fillup(MyCache, ReceivedCache, ToAddCount, 0).

%% @doc Helper to fill up MyCache with (up to) ToAddCount entries from
%%      ReceivedCache, returning the new cache, the rest of the ReceivedCache
%%      and the number of actually added elements.
-spec fillup(MyCache::cache(), ReceivedCache::cache(), ToAddCount::non_neg_integer(), AddedElements::non_neg_integer()) -> {MyNewCache::cache(), ReceivedCacheRest::cache(), AddedElements::non_neg_integer()}.
fillup(MyCache, ReceivedCache, 0 = _ToAddCount, AddedElements) ->
    {MyCache, ReceivedCache, AddedElements};
fillup(MyCache, [], _ToAddCount, AddedElements) ->
    {MyCache, [], AddedElements};
fillup(MyCache, [Elem | Rest] = _ReceivedCache, ToAddCount, AddedElements) ->
    fillup([Elem | MyCache], Rest, ToAddCount - 1, AddedElements + 1).

%% @doc Updates MyCache to include all entries of ReceivedCache by firstly
%%      replacing entries among SendCache and thirdly by replacing random
%%      entries.
%%      ReceivedCache must not contain the local node and must not contain any
%%      node that MyCache already contains!
%%      SendCache must not contain the local node!
-spec replace(MyCache::cache(), MyCacheSize::non_neg_integer(), ReceivedCache::cache(), SendCache::cache(), TargetSize::pos_integer()) -> MyNewCache::cache().
replace([] = _MyCache, MyCacheSize, ReceivedCache, _SendCache, TargetSize) ->
    % the cache size (although otherwise not needed) should still be correct:
    0 = MyCacheSize,
    trim(ReceivedCache, cyclon_cache:size(ReceivedCache), TargetSize);

replace(MyCache, _MyCacheSize, [], _SendCache, _TargetSize) ->
    MyCache;
replace(MyCache, MyCacheSize, ReceivedCache, [] = _SendCache, TargetSize) ->
    % trim MyCache so it has enough space for all elements of ReceivedCache
    % and add all received elements
    ReceivedCacheSize = cyclon_cache:size(ReceivedCache),
    MyC1 = trim(MyCache, MyCacheSize, TargetSize - ReceivedCacheSize),
    MyC2 = MyC1 ++ ReceivedCache,
    MyC2;

replace(MyCache, _MyCacheSize, ReceivedCache, SendCache, TargetSize) ->
    % filter all nodes from SendCache out of MyCache to make room for entries
    % from ReceivedCache
    {MyC1, SendCache_new} =
        lists:partition(
          fun({Node, _Age}) -> not contains_node(Node, SendCache) end,
          MyCache),
    MyC1Size = cyclon_cache:size(MyC1),
    % trim MyC1 so it has enough space for all elements of ReceivedCache
    ReceivedCacheSize = cyclon_cache:size(ReceivedCache),
    MyC2 = trim(MyC1, MyC1Size, TargetSize - ReceivedCacheSize),
    MyC2Size = util:min(MyC1Size, TargetSize - ReceivedCacheSize),
    % add all received elements to MyC2
    MyC3 = MyC2 ++ ReceivedCache,
    MyC3Size = MyC2Size + ReceivedCacheSize,
    % finally fill up MyC3 (if necessary) with elements from SendCache_new that
    % are not in ReceivedCache and thus not in MyC3
    case MyC3Size < TargetSize of
        true ->
            SendC3 = [Elem || {Node, _Age} = Elem <- SendCache_new,
                              not contains_node(Node, ReceivedCache)],
            {MyC4, _SendC3Rest, _AddedElements} =
                fillup(MyC3, SendC3, TargetSize - MyC3Size),
            MyC4;
        false ->
            MyC3
    end.

%% @doc Adds the given node to the cache or updates its age in the Cache if
%%      present.
%%      Beware: the node will be added to the cache no matter what size it
%%      already has!
-spec add_node(Node::node:node_type(), Age::age(), Cache::cache()) -> NewCache::cache().
add_node(Node, Age, Cache) ->
    case contains_node(Node, Cache) of
        true  -> [{Node, Age} | remove_node(Node, Cache)];
        false -> [{Node, Age} | Cache]
    end.

%% @doc Removes any element with the given Node from the Cache.
-spec remove_node(Node::node:node_type(), Cache::cache()) -> NewCache::cache().
remove_node(Node, Cache) ->
    [Element || {SomeNode, _Age} = Element <- Cache, not node:equals(SomeNode, Node)].

%% @doc Trims the cache to size TargetSize (if necessary) by deleting random
%%      entries as long as the cache is larger than the given TargetSize.
-spec trim(Cache::cache(), TargetSize::pos_integer()) -> NewCache::cache().
trim(Cache, TargetSize) ->
    trim(Cache, cyclon_cache:size(Cache), TargetSize).

%% @doc Returns a list of keys (ages) and string values (nodes) for debug output
%%      used in the web interface.
-spec debug_format_by_age(Cache::cache()) -> KeyValueList::[{Age::age(), Node::string()}].
debug_format_by_age(Cache) ->
    [{Age, lists:flatten(io_lib:format("~p", [Node]))} || {Node, Age} <- Cache].
