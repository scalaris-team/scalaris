% @copyright 2011-2014 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    Merkle tree implementation
%%         with configurable bucketing, branching and hashing.
%%         Underlaying tree structure is an n-ary tree.
%%         After calling gen_hashes the tree is ready to use and sealed.
%% @end
%% @version $Id$
-module(merkle_tree).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([new/1, new/2, new/3,
         keys_to_intervals/2,
         insert/2, insert_list/2,
         lookup/2, size/1, size_detail/1, gen_hash/1, gen_hash/2,
         iterator/1, next/1,
         is_empty/1, is_leaf/1, is_merkle_tree/1,
         get_bucket/1, get_hash/1, get_interval/1, get_childs/1, get_root/1,
         get_item_count/1, get_items/1, get_leaf_count/1, get_leaves/1,
         get_bucket_size/1, get_branch_factor/1,
         get_opt_bucket_size/3,
         store_to_DOT/2, store_graph/2]).
-export([leaf_hash_sha/2]).

% exports for tests
-export([tester_create_hash_fun/1, tester_create_inner_hash_fun/1]).

-compile({inline, [get_hash/1, get_interval/1, node_size/1, decode_key/1,
                   run_leaf_hf/4, run_inner_hf/2]}).

%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

%-define(DOT_SHORTNAME_HASH(X), X). % short node hash in exported merkle tree dot-pictures
-define(DOT_SHORTNAME_HASH(X), X rem 100000000). % last 8 digits

%-define(DOT_SHORTNAME_KEY(X), X). % short node keys in exported merkle tree dot-pictures
-define(DOT_SHORTNAME_KEY(X), b).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([mt_config/0, merkle_tree/0, mt_node/0, mt_node_key/0, mt_size/0,
              mt_bucket/0]).
-export_type([mt_iter/0, mt_config_params/0]).

-type mt_node_key()     :: non_neg_integer().
-type mt_bucket_entry() :: {?RT:key()} | {?RT:key(), any()} | {?RT:key(), any(), any()}. % add more, if needed
-type mt_bucket()       :: [mt_bucket_entry()].
-type mt_size()         :: {InnerNodes::non_neg_integer(),
                            LeafNodes::non_neg_integer(),
                            EmptyLeafNodes::non_neg_integer(),
                            Items::non_neg_integer()}.
-type leaf_hash_fun()   :: fun((mt_bucket(), intervals:interval()) -> binary()).
-type inner_hash_fun()  :: fun(([mt_node_key(),...]) -> mt_node_key()).

-record(mt_config,
        {
         branch_factor  = 2                 :: pos_integer(),   %number of childs per inner node
         bucket_size    = 24                :: pos_integer(),   %max items in a leaf
         leaf_hf        = fun leaf_hash_sha/2  :: leaf_hash_fun(), %hash function for leaf signature creation
         inner_hf       = fun inner_hash_XOR/1 :: inner_hash_fun(),%hash function for inner node signature creation
         keep_bucket    = false             :: boolean()        %false=bucket will be empty after p_bulk_build; true=bucket will be filled
         }).
-type mt_config() :: #mt_config{}.
%only key value pairs of mt_config allowed:
-type mt_config_params() :: [{branch_factor, pos_integer()}
                                 | {bucket_size, pos_integer()}
                                 | {leaf_hf, leaf_hash_fun()}
                                 | {inner_hf, inner_hash_fun()}
                                 | {keep_bucket, boolean()}] | [].

-type mt_leaf() :: { Hash        :: mt_node_key() | nil, %hash of childs/containing items
                     ItemCount   :: non_neg_integer(),   %number of items in the leaf nodes below or in this node
                     Bucket      :: mt_bucket(),         %item storage
                     Interval    :: intervals:interval() %represented interval
                    }.
-type mt_inner() :: {Hash        :: mt_node_key() | nil, %hash of childs/containing items
                     Count       :: non_neg_integer(),   %number of subnodes including itself
                     ItemCount   :: non_neg_integer(),   %number of items in the leaf nodes below or in this node
                     Interval    :: intervals:interval(),%represented interval
                     Child_list  :: [mt_leaf() | mt_inner(),...] %child nodes (in arbitrary order)
                    }.
-type mt_node() :: mt_inner() | mt_leaf().

-type mt_iter()     :: [mt_node() | [mt_node()]].
-type merkle_tree() :: {merkle_tree, mt_config(), Root::mt_node()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the given merkle tree's bucket size (number of elements in a leaf).
-spec get_bucket_size(merkle_tree()) -> pos_integer().
get_bucket_size({merkle_tree, Config, _}) ->
    Config#mt_config.bucket_size.

%% @doc Gets the given merkle tree's branch factor (max number of children of
%%      an inner node).
-spec get_branch_factor(merkle_tree()) -> pos_integer().
get_branch_factor({merkle_tree, Config, _}) ->
    Config#mt_config.branch_factor.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the root node of a merkle tree.
-spec get_root(merkle_tree()) -> mt_node().
get_root({merkle_tree, _, Root}) -> Root.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether the merkle tree or tree node has any children or elements.
-spec is_empty(merkle_tree() | mt_node()) -> boolean().
is_empty({merkle_tree, _, Root}) -> is_empty(Root);
is_empty({_H, _ICnt = 0, _Bkt = [], _I}) -> true;
is_empty({_H, _Cnt, _ICnt, _I, _CL}) -> false;
is_empty({_H, _ICnt, _Bkt, _I}) -> false. % inner node (must have children!)

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Creates a new empty merkle tree with default params for the given
%%      interval. Also creates the hash values.
-spec new(intervals:interval()) -> merkle_tree().
new(I) ->
    new(I, []).

%% @doc Creates a new empty merkle tree with the given params and interval.
%%      Also creates the hash values.
%%      ConfParams = list of tuples defined by {config field name, value}
%%      e.g. [{branch_factor, 32}, {bucket_size, 16}]
-spec new(intervals:interval(), mt_config_params()) -> merkle_tree().
new(I, ConfParams) ->
    new(I, [], ConfParams).

%% @doc Creates a new merkle tree with the given params, interval and
%%      entries from EntryList. Also creates the hash values.
%%      ConfParams = list of tuples defined by {config field name, value}
%%      e.g. [{branch_factor, 32}, {bucket_size, 16}]
-spec new(intervals:interval(), EntryList::mt_bucket(), mt_config_params())
        -> merkle_tree().
new(I, EntryList, ConfParams) ->
    % remove duplicates and speed up keys_to_intervals/2 by sorting
    KeyList = lists:ukeysort(1, EntryList),
    Config = build_config(ConfParams),
    {[Root], _} = build_childs([{I, length(KeyList), KeyList}], Config, [], 0),
    {merkle_tree, Config, Root}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether an interval is present in the merkle tree and returns
%%      the node responsible for it (exact match!).
-spec lookup(intervals:interval(), merkle_tree()) -> mt_node() | not_found.
lookup(I, {merkle_tree, _, Root}) ->
    case intervals:is_subset(I, get_interval(Root)) of
        true  -> lookup_(I, Root);
        false -> not_found
    end.

%% @doc Helper for lookup/2. In contrast to lookup/2, assumes that I is a
%%      subset of the current node's interval.
-spec lookup_(intervals:interval(), mt_node()) -> mt_node() | not_found.
lookup_(I, {_H, _ICnt, _Bkt, I} = Node) ->
    Node;
lookup_(_I, {_H, _ICnt, _Bkt, _NodeI}) ->
    not_found;
lookup_(I, {_H, _Cnt, _ICnt, I, _CL} = Node) ->
    Node;
lookup_(I, {_H, _Cnt, _ICnt, _NodeI, ChildList = [_|_]}) ->
    case lists:dropwhile(fun(X) ->
                                 not intervals:is_subset(I, get_interval(X))
                         end, ChildList) of
        [IChild | _] -> lookup_(I, IChild);
        []       -> not_found
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the node's hash value.
-spec get_hash(merkle_tree() | mt_node()) -> mt_node_key().
get_hash({merkle_tree, _, Root}) -> get_hash(Root);
get_hash({Hash, _Cnt, _ICnt, _I, _CL}) -> Hash;
get_hash({Hash, _ICnt, _Bkt, _I}) -> Hash.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_interval(merkle_tree() | mt_node()) -> intervals:interval().
get_interval({merkle_tree, _, Root}) -> get_interval(Root);
get_interval({_H, _Cnt, _ICnt, I, _CL}) -> I;
get_interval({_H, _ICnt, _Bkt, I}) -> I.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_childs(merkle_tree() | mt_node()) -> [mt_node()].
get_childs({merkle_tree, _, Root}) -> get_childs(Root);
get_childs({_H, _Cnt, _ICnt, _I, [_|_] = Childs}) -> Childs;
get_childs({_H, _ICnt, _Bkt, _I}) -> [].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns the number of items in all buckets in or below this node.
-spec get_item_count(merkle_tree() | mt_node()) -> non_neg_integer().
get_item_count({merkle_tree, _, Root}) -> get_item_count(Root);
get_item_count({_H, _Cnt, ICnt, _I, _CL}) -> ICnt;
get_item_count({_H, ICnt, _Bkt, _I}) -> ICnt.

%% @doc Gets all items in the buckets of the given merkle tree or node list
%%      (in arbitrary order).
-spec get_items(merkle_tree() | mt_node() | [mt_node()])
        -> {Items::mt_bucket(), LeafCount::non_neg_integer()}.
get_items({merkle_tree, _, Root}) -> get_items(Root);
get_items({_H, _Cnt, _ICnt, _I, _CL} = N) -> get_items_([N], [], 0);
get_items({_H, _ICnt, Bkt, _I}) -> {Bkt, 1};
get_items(Nodes) when is_list(Nodes) -> get_items_(Nodes, [], 0).

%% @doc Helper for get_items/1.
-spec get_items_(Iter::mt_iter(), Items::mt_bucket(), LCnt::non_neg_integer())
        -> {Items::mt_bucket(), LeafCount::non_neg_integer()}.
get_items_(Iter, Items, LCnt) ->
    case next(Iter) of
        {Node, Next} ->
            case is_leaf(Node) of
                true  -> get_items_(Next, get_bucket(Node) ++ Items, LCnt + 1);
                false -> get_items_(Next, Items, LCnt)
            end;
        none ->
            {Items, LCnt}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns the number of leafs under a bucket (1 if the requested node is
%%      a leaf node).
%%      NOTE: This scans through the whole sub-tree!
-spec get_leaf_count(merkle_tree() | mt_node()) -> pos_integer().
get_leaf_count({merkle_tree, _, Root}) -> get_leaf_count(Root);
get_leaf_count({_H, _Cnt, _ICnt, _I, _CL} = N) -> get_leaf_count_([N], 0);
get_leaf_count({_H, _ICnt, _Bkt, _I}) -> 1.

%% @doc Helper for get_leaf_count/1.
-spec get_leaf_count_(Iter::mt_iter(), LCnt::non_neg_integer()) -> pos_integer().
get_leaf_count_(Iter, LCnt) ->
    case next(Iter) of
        {Node, Next} ->
            get_leaf_count_(Next, LCnt + ?IIF(is_leaf(Node), 1, 0));
        none -> LCnt
    end.

%% @doc Gets all leaves in the given merkle tree or node list
%%      (in arbitrary order).
-spec get_leaves(merkle_tree() | mt_node() | [mt_node()]) -> [mt_node()].
get_leaves({merkle_tree, _, Root}) -> get_leaves(Root);
get_leaves({_H, _Cnt, _ICnt, _I, _CL} = N) -> get_leaves_([N], []);
get_leaves({_H, _ICnt, _Bkt, _I} = N) -> [N];
get_leaves(Nodes) when is_list(Nodes) -> get_leaves_(Nodes, []).

%% @doc Helper for get_leaves/1.
-spec get_leaves_(Iter::mt_iter(), Acc::[mt_node()]) -> [mt_node()].
get_leaves_(Iter, Acc) ->
    case next(Iter) of
        {Node, Next} ->
            case is_leaf(Node) of
                true  -> get_leaves_(Next, [Node | Acc]);
                false -> get_leaves_(Next, Acc)
            end;
        none -> Acc
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether the given merkle_tree or node is a leaf.
-spec is_leaf(merkle_tree() | mt_node()) -> boolean().
is_leaf({merkle_tree, _, Root}) -> is_leaf(Root);
is_leaf({_H, _ICnt, _Bkt, _I}) -> true;
is_leaf(_) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns true if given term is a merkle tree otherwise false.
-spec is_merkle_tree(any()) -> boolean().
is_merkle_tree({merkle_tree, _, _Root}) -> true;
is_merkle_tree(_) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_bucket(merkle_tree() | mt_node()) -> mt_bucket().
get_bucket({merkle_tree, _, Root}) -> get_bucket(Root);
get_bucket({_H, _ICnt, Bucket, _I}) -> Bucket;
get_bucket(_) -> [].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert_list(mt_bucket(), merkle_tree()) -> merkle_tree().
insert_list(Terms, Tree) ->
    lists:foldl(fun insert/2, Tree, Terms).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert(Key::mt_bucket_entry(), merkle_tree()) -> merkle_tree().
insert(Key, {merkle_tree, Config = #mt_config{keep_bucket = true}, Root} = Tree) ->
    CheckKey = decode_key(Key),
    case intervals:in(CheckKey, get_interval(Root)) of
        true -> {merkle_tree, Config, insert_to_node(Key, CheckKey, Root, Config)};
        false -> Tree
    end.

%% @doc Helper for insert/2 assuming that CheckKey is in the given node's
%%      interval.
-spec insert_to_node(Key::mt_bucket_entry(), CheckKey::?RT:key(),
                     Node::mt_node(), Config::mt_config())
        -> NewNode::mt_node().
insert_to_node(Key, CheckKey, {_H, ItemCount, Bucket, Interval} = N,
               #mt_config{ branch_factor = BranchFactor,
                           bucket_size = BucketSize } = Config) ->
    if ItemCount < Config#mt_config.bucket_size ->
           % leaf node will stay leaf node
           case lists:keymember(element(1, Key), 1, Bucket) of
               false -> {nil, ItemCount + 1, [Key | Bucket], Interval};
               _     -> N
           end;
       ItemCount =:= BucketSize ->
           % former leaf node which will become an inner node
           % (only split here, insert in next iteration)
           ChildI = intervals:split(Interval, BranchFactor),
           NewLeafs = [{nil, CX, BX, IX}
                       || {IX, CX, BX} <- keys_to_intervals(Bucket, ChildI)],
           ?DBG_ASSERT(length(NewLeafs) =:= BranchFactor),
           insert_to_node(Key, CheckKey, {nil, 1 + BranchFactor, BucketSize,
                                          Interval, NewLeafs}, Config)
    end;
insert_to_node(Key, CheckKey, {Hash, Count, ItemCount, Interval,
                               Childs = [_|_]} = Node, Config) ->
    % inner node; insert into a child
    case util:lists_takewith(fun(N) ->
                                     intervals:in(CheckKey, get_interval(N))
                             end, Childs) of
        false ->
            log:log("InsertFailed!"),
            Node;
        {Dest, Rest} ->
            OldSize = node_size(Dest),
            NewDest = insert_to_node(Key, CheckKey, Dest, Config),
            {Hash, Count - OldSize + node_size(NewDest),
             ItemCount + 1, Interval, [NewDest | Rest]}
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Builds a merkle node from the given KeyList assuming that the current
%%      node needs to be split because the KeyList is larger than
%%      Config#mt_config.bucket_size.
-spec p_bulk_build(CurNode::mt_node(), mt_config(), KeyList::mt_bucket())
        -> mt_node().
p_bulk_build({_H, ItemCount, _Bkt, Interval}, Config, KeyList) ->
    ChildsI = intervals:split(Interval, Config#mt_config.branch_factor),
    IKList = keys_to_intervals(KeyList, ChildsI),
    {ChildNodes, NCount} = build_childs(IKList, Config, [], 0),
    % hash the bucket now:
    Hash = run_inner_hf(ChildNodes, Config#mt_config.inner_hf),
    {Hash, 1 + NCount, ItemCount + length(KeyList), Interval, ChildNodes}.

-spec build_childs([{I::intervals:continuous_interval(), Count::non_neg_integer(),
                     mt_bucket()}],
                   mt_config(), Acc::[mt_node()], NCount::non_neg_integer())
        -> {[mt_node()], NCount::non_neg_integer()}.
build_childs([{Interval, Count, Bucket} | T], Config, Acc, NCount) ->
    Node = if Count > Config#mt_config.bucket_size ->
                  p_bulk_build({nil, 0, [], Interval}, Config, Bucket);
              true ->
                  % hash the bucket now:
                  {Bucket1, Hash} = run_leaf_hf(Bucket, Interval,
                                                Config#mt_config.leaf_hf, false),
                  {Hash, Count,
                   ?IIF(Config#mt_config.keep_bucket, Bucket1, []), Interval}
           end,
    build_childs(T, Config, [Node | Acc], NCount + node_size(Node));
build_childs([], _, Acc, NCount) ->
    {Acc, NCount}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Assigns hash values to all nodes in the tree.
-spec gen_hash(merkle_tree()) -> merkle_tree().
gen_hash(Tree = {merkle_tree, #mt_config{keep_bucket = KeepBucket}, _Root}) ->
    gen_hash(Tree, not KeepBucket).

%% @doc Assigns hash values to all nodes in the tree and removes the buckets
%%      afterwards if CleanBuckets is set.
-spec gen_hash(merkle_tree(), CleanBuckets::boolean()) -> merkle_tree().
gen_hash({merkle_tree, Config = #mt_config{inner_hf = InnerHf,
                                           leaf_hf = LeafHf,
                                           keep_bucket = KeepBucket},
          Root}, CleanBuckets) ->
    RootNew = gen_hash_node(Root, InnerHf, LeafHf, KeepBucket, CleanBuckets),
    {merkle_tree, Config#mt_config{keep_bucket = not CleanBuckets}, RootNew}.

%% @doc Helper for gen_hash/2.
-spec gen_hash_node(mt_node(), InnerHf::inner_hash_fun(), LeafHf::leaf_hash_fun(),
                    KeepBucket::boolean(),
                    CleanBuckets::boolean()) -> mt_node().
gen_hash_node({_H, Count, ItemCount, Interval, ChildList = [_|_]},
              InnerHf, LeafHf, OldKeepBucket, CleanBuckets) ->
    % inner node
    NewChilds = [gen_hash_node(X, InnerHf, LeafHf, OldKeepBucket,
                               CleanBuckets) || X <- ChildList],
    Hash = run_inner_hf(NewChilds, InnerHf),
    {Hash, Count, ItemCount, Interval, NewChilds};
gen_hash_node({Hash, _ICnt, _Bkt = [], _I} = N, _InnerHf,
              _LeafHf, false, _CleanBuckets) when Hash =/= nil ->
    % leaf node, no bucket contents, keep_bucket false
    % -> we already hashed the value in p_bulk_build and cannot insert any more
    %    values
    N;
gen_hash_node({_OldHash, ICnt, Bucket, Interval},
              _InnerHf, LeafHf, true, CleanBuckets) ->
    % leaf node, keep_bucket true
    {Bucket1, Hash} = run_leaf_hf(Bucket, Interval, LeafHf, true),
    {Hash, ICnt, ?IIF(CleanBuckets, [], Bucket1), Interval}.

%% @doc Hashes an inner node based on its childrens' hashes.
-spec run_inner_hf([mt_node(),...], InnerHf::inner_hash_fun()) -> mt_node_key().
run_inner_hf(Childs, InnerHf) ->
    InnerHf([get_hash(C) || C <- Childs]).

%% @doc Hashes a leaf with the given bucket.
-spec run_leaf_hf(mt_bucket(), intervals:interval(), LeafHf::leaf_hash_fun(),
                  MaybeUnsorted::boolean()) -> {mt_bucket(), mt_node_key()}.
run_leaf_hf(Bucket0, I, LeafHf, false) ->
    % either reversed or correct (during bulk build due to keys_to_intervals/2)
    Bucket = case Bucket0 of
                 [E1, E2 | _] when element(1, E1) > element(1, E2) ->
                     lists:reverse(Bucket0);
                 _ ->
                     Bucket0
             end,
    ?DBG_ASSERT2(lists:ukeysort(1, Bucket) =:= Bucket, {not_sorted, Bucket}),
    Hash = LeafHf(Bucket, I),
    Size = erlang:bit_size(Hash),
    <<SmallHash:Size/integer-unit:1>> = Hash,
    {Bucket, SmallHash};
run_leaf_hf(Bucket0, I, LeafHf, true) ->
    run_leaf_hf(lists:keysort(1, Bucket0), I, LeafHf, false).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns the total number of nodes in a tree or node (inner nodes and leaves)
-spec size(merkle_tree() | mt_node()) -> non_neg_integer().
size({merkle_tree, _, Root}) -> node_size(Root);
size(Node) -> node_size(Node).

-spec node_size(mt_node()) -> non_neg_integer().
node_size({_H, Count, _ICnt, _I, _CL = [_|_]}) ->
    Count;
node_size({_H, _ICnt, _Bkt, _I}) ->
    1.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns a triple with number of inner nodes, leaf nodes and hashed items.
-spec size_detail(merkle_tree() | [mt_node()]) -> mt_size().
size_detail({merkle_tree, _, Root}) ->
    size_detail_node([Root], 0, 0, 0, 0);
size_detail(Nodes) when is_list(Nodes) ->
    Result = {_Inner, _Leafs, _EmptyLeafs, _Items} = size_detail_node(Nodes, 0, 0, 0, 0),
    % already tested via unit test:
%%     ?DBG_ASSERT(_Leafs =:= lists:sum([get_leaf_count(N) || N <- Nodes])),
%%     ?DBG_ASSERT(_Items =:= lists:sum([get_item_count(N) || N <- Nodes])),
    Result.

-spec size_detail_node([mt_node() | [mt_node()]], InnerNodes::non_neg_integer(),
                       Leafs::non_neg_integer(), EmptyLeafs::non_neg_integer(),
                       Items::non_neg_integer())
        -> mt_size().
size_detail_node([{_H, _Cnt, _ICnt, _I, Childs = [_|_]} | R], Inner, Leafs, EmptyLeafs, Items) ->
    size_detail_node([Childs | R], Inner + 1, Leafs, EmptyLeafs, Items);
size_detail_node([{_H, _ICnt = 0, _Bkt, _I} | R], Inner, Leafs, EmptyLeafs, Items) ->
    size_detail_node(R, Inner, Leafs + 1, EmptyLeafs + 1, Items);
size_detail_node([{_H, ICnt, _Bkt, _I} | R], Inner, Leafs, EmptyLeafs, Items) ->
    size_detail_node(R, Inner, Leafs + 1, EmptyLeafs, Items + ICnt);
size_detail_node([], InnerNodes, Leafs, EmptyLeafs, Items) ->
    {InnerNodes, Leafs, EmptyLeafs, Items};
size_detail_node([[{_H, _Cnt, _ICnt, _I, Childs = [_|_]} | R1] | R2], Inner, Leafs, EmptyLeafs, Items) ->
    size_detail_node([Childs, R1 | R2], Inner + 1, Leafs, EmptyLeafs, Items);
size_detail_node([[{_H, _ICnt = 0, _Bkt, _I} | R1] | R2], Inner, Leafs, EmptyLeafs, Items) ->
    size_detail_node([R1 | R2], Inner, Leafs + 1, EmptyLeafs + 1, Items);
size_detail_node([[{_H, ICnt, _Bkt, _I} | R1] | R2], Inner, Leafs, EmptyLeafs, Items) ->
    size_detail_node([R1 | R2], Inner, Leafs + 1, EmptyLeafs, Items + ICnt);
size_detail_node([[] | R2], Inner, Leafs, EmptyLeafs, Items) ->
    size_detail_node(R2, Inner, Leafs, EmptyLeafs, Items).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns an iterator to visit all tree nodes with next/1.
%      Iterates over all tree nodes from left to right in a deep first manner.
-spec iterator(Tree::merkle_tree()) -> Iter::mt_iter().
iterator({merkle_tree, _, Root}) -> [Root].

-spec iterator_node(Node::mt_node(), mt_iter()) -> mt_iter().
iterator_node({_H, _Cnt, _ICnt, _I, Childs = [_|_]}, Iter1) ->
    [Childs | Iter1];
iterator_node({_H, _ICnt, _Bkt, _I}, Iter1) ->
    Iter1.

-spec next(mt_iter()) -> none | {Node::mt_node(), mt_iter()}.
next([Node | R]) when is_tuple(Node) ->
    {Node, iterator_node(Node, R)};
next([[Node | R1] | R2]) when is_tuple(Node) ->
    {Node, iterator_node(Node, [R1 | R2])};
next([[] | R2]) ->
    next(R2);
next([]) ->
    none.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Stores the tree graph into a png file.
-spec store_graph(merkle_tree(), string()) -> ok.
store_graph(MerkleTree, FileName) ->
    erlang:spawn(fun() -> store_to_DOT_p(MerkleTree, FileName, true) end),
    ok.

% @doc Stores the tree graph into a file in DOT language (for Graphviz or other visualization tools).
-spec store_to_DOT(merkle_tree(), string()) -> ok.
store_to_DOT(MerkleTree, FileName) ->
    erlang:spawn(fun() -> store_to_DOT_p(MerkleTree, FileName, false) end),
    ok.

store_to_DOT_p({merkle_tree, Conf, Root}, FileName, ToPng) ->
    case file:open("../" ++ FileName ++ ".dot", [write]) of
        {ok, Fileid} ->
            io:fwrite(Fileid, "digraph merkle_tree { ~n", []),
            io:fwrite(Fileid, "    style=filled;~n", []),
            store_node_to_DOT(Root, Fileid, 1, 2, Conf),
            io:fwrite(Fileid, "} ~n", []),
            _ = file:truncate(Fileid),
            _ = file:close(Fileid),
            _ = if ToPng ->
                       _ = os:cmd(io_lib:format("dot ../~s.dot -Tpng > ../~s.png", [FileName, FileName])),
                       os:cmd(io_lib:format("rm -f ../~s.dot", [FileName]));
                   true -> ok
                end,
            ok;
        {_, _} ->
            io_error
    end.

-spec store_node_to_DOT(mt_node(), pid(), pos_integer(), pos_integer(), mt_config()) -> pos_integer().
store_node_to_DOT({H, ICnt, _Bkt, I}, Fileid, MyId,
                  NextFreeId, #mt_config{ bucket_size = BuckSize }) ->
    {LBr, _LKey, _RKey, RBr} = intervals:get_bounds(I),
    io:fwrite(Fileid, "    ~p [label=\"~p\\n~s~p,~p~s ; ~p/~p\", shape=box]~n",
              [MyId, ?DOT_SHORTNAME_HASH(H), erlang:atom_to_list(LBr),
               ?DOT_SHORTNAME_KEY(_LKey), ?DOT_SHORTNAME_KEY(_RKey),
               erlang:atom_to_list(RBr), ICnt, BuckSize]),
    NextFreeId;
store_node_to_DOT({H, _Cnt, _ICnt, I, Childs = [_ | RChilds]}, Fileid, MyId,
                  NextFreeId, TConf) ->
    io:fwrite(Fileid, "    ~p -> { ~p", [MyId, NextFreeId]),
    NNFreeId = lists:foldl(fun(_, Acc) ->
                                    io:fwrite(Fileid, ";~p", [Acc]),
                                    Acc + 1
                           end, NextFreeId + 1, RChilds),
    io:fwrite(Fileid, " }~n", []),
    {_, NNNFreeId} = lists:foldl(fun(Node, {NodeId, NextFree}) ->
                                         {NodeId + 1 , store_node_to_DOT(Node, Fileid, NodeId, NextFree, TConf)}
                                 end, {NextFreeId, NNFreeId}, Childs),
    {LBr, _LKey, _RKey, RBr} = intervals:get_bounds(I),
    io:fwrite(Fileid, "    ~p [label=\"~p\\n~s~p,~p~s\"""]~n",
              [MyId, ?DOT_SHORTNAME_HASH(H), erlang:atom_to_list(LBr),
               ?DOT_SHORTNAME_KEY(_LKey), ?DOT_SHORTNAME_KEY(_RKey),
               erlang:atom_to_list(RBr)]),
    NNNFreeId.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Calculates min bucket size to remove S tree levels.
% Formula: N / (v^(log_v(N) - S))
% S = 1.. has to be smaller than log_v(N)
-spec get_opt_bucket_size(N::non_neg_integer(), V::non_neg_integer(), S::pos_integer()) -> pos_integer().
get_opt_bucket_size(_N, 0, _S) -> 1;
get_opt_bucket_size(0, _V, _S) -> 1;
get_opt_bucket_size(N, V, S) ->
    Height = erlang:max(util:ceil(util:log(N, V)) - S, 1),
    util:ceil(N / math:pow(V, Height)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local Functions

-spec build_config(mt_config_params()) -> mt_config().
build_config(ParamList) ->
    lists:foldl(
      fun({Key, Val}, Conf) ->
              case Key of
                  branch_factor  -> ?ASSERT(Val >= 2), Conf#mt_config{ branch_factor = Val };
                  bucket_size    -> Conf#mt_config{ bucket_size = Val };
                  leaf_hf        -> Conf#mt_config{ leaf_hf = Val };
                  inner_hf       -> Conf#mt_config{ inner_hf = Val };
                  keep_bucket    -> Conf#mt_config{ keep_bucket = Val}
              end
      end,
      #mt_config{}, ParamList).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Decodes a key for use by the merkle_tree.
-spec decode_key(Key::mt_bucket_entry()) -> ?RT:key().
decode_key(Key) -> element(1, Key).

%% @doc Inserts Key into its matching interval (represented as a tuple of
%%      bucket tuples). For each key, it starts at Last and continuously
%%      checks further tuples until index High is reached.
%%      PreCond: Key fits into one of the given intervals,
%%               CheckKey is the decoded Key (see decode_key/1)
-spec p_key_in_I(Key::mt_bucket_entry(), CheckKey::?RT:key(), Acc) -> Acc when
    is_subtype(Acc,  {ITuple::tuple(), Last::pos_integer(), High::pos_integer()}).
p_key_in_I(Key, CheckKey, {ITuple, Last, High}) ->
    % find first tuple for key
    {I, Len, Keys} = element(Last, ITuple),
    case intervals:in(CheckKey, I) of
        true  ->
            {setelement(Last, ITuple, {I, Len + 1, [Key | Keys]}), Last, Last};
        false ->
            Next = Last rem tuple_size(ITuple) + 1,
            ?ASSERT2(Next =/= High, loop_detected), % item does not belong in any interval!
            p_key_in_I(Key, CheckKey, {ITuple, Next, High})
    end.

%% @doc Inserts the given keys into the given intervals.
-spec keys_to_intervals(mt_bucket(), [I,...])
        -> Buckets::[{I, Count::non_neg_integer(), mt_bucket()}] when
     is_subtype(I, intervals:continuous_interval()).
keys_to_intervals(KList, IList) ->
    IBucket = [{I, 0, []} || I <- IList],
    % pay attention to the key order - run_leaf_hf/4 relies on it being either
    % correct or reversed (which is exactly what p_key_in_I/3 does)
    {IBucket2, _, _} =
        lists:foldl(fun(Key, Acc) ->
                            p_key_in_I(Key, decode_key(Key), Acc)
                    end, {list_to_tuple(IBucket), 1, 1}, KList),
    tuple_to_list(IBucket2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec inner_hash_XOR(ChildHashes::[mt_node_key(),...]) -> mt_node_key().
inner_hash_XOR([H|T]) ->
    lists:foldl(fun(X, Acc) -> X bxor Acc end, H, T).

%% @doc Leaf hash fun to use for the embedded merkle tree.
-spec leaf_hash_sha(merkle_tree:mt_bucket(), intervals:interval()) -> binary().
leaf_hash_sha([], _I) ->
    <<0:160>>;
leaf_hash_sha([_|_] = Bucket, _I) ->
    ?CRYPTO_SHA(term_to_binary(Bucket)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% allow future versions to create more hash funs by having an integer parameter
-spec tester_create_hash_fun(I::1) -> leaf_hash_fun().
tester_create_hash_fun(1) -> fun leaf_hash_sha/2.

% allow future versions to create more hash funs by having an integer parameter
-spec tester_create_inner_hash_fun(I::1) -> inner_hash_fun().
tester_create_inner_hash_fun(1) -> fun inner_hash_XOR/1.
