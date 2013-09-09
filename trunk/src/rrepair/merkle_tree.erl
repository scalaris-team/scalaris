% @copyright 2011, 2012 Zuse Institute Berlin

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
         insert/2, insert_list/2,
         lookup/2, size/1, size_detail/1, gen_hash/1, gen_hash/2,
         iterator/1, next/1,
         is_empty/1, is_leaf/1, is_merkle_tree/1,
         get_bucket/1, get_hash/1, get_interval/1, get_childs/1, get_root/1,
         get_item_count/1, get_bucket_size/1, get_branch_factor/1,
         get_opt_bucket_size/3,
         store_to_DOT/2, store_graph/2]).

% exports for tests
-export([bulk_build/3]).
-export([tester_create_hash_fun/1, tester_create_inner_hash_fun/1]).

-compile({inline, [get_hash/1, get_interval/1, node_size/1]}).

%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

%-define(DOT_SHORTNAME(X), X). %short Node names in exported merkle tree dot-pictures
-define(DOT_SHORTNAME(X), b).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([mt_config/0, merkle_tree/0, mt_node/0, mt_node_key/0, mt_size/0]).
-export_type([mt_iter/0, mt_config_params/0]).
-endif.

-type mt_node_key()     :: binary().
-type mt_interval()     :: intervals:interval(). 
-type mt_bucket()       :: [?RT:key() | rr_recon:db_entry_enc()].
-type mt_size()         :: {InnerNodes::non_neg_integer(), Leafs::non_neg_integer()}.
-type hash_fun()        :: fun((binary()) -> mt_node_key()).
-type inner_hash_fun()  :: fun(([mt_node_key()]) -> mt_node_key()).

-record(mt_config,
        {
         branch_factor  = 2                 :: pos_integer(),   %number of childs per inner node
         bucket_size    = 24                :: pos_integer(),   %max items in a leaf
         signature_size = 4                 :: pos_integer(),   %node signature size in byte
         leaf_hf        = fun crypto:sha/1  :: hash_fun(),      %hash function for leaf signature creation
         inner_hf       = get_XOR_fun()     :: inner_hash_fun(),%hash function for inner node signature creation -
         keep_bucket    = false             :: boolean()        %false=bucket will be empty after bulk_build; true=bucket will be filled          
         }).
-type mt_config() :: #mt_config{}.
-type mt_config_params() :: [{atom(), term()}] | [].    %only key value pairs of mt_config allowed

-type mt_node() :: { Hash        :: mt_node_key() | nil, %hash of childs/containing items 
                     Count       :: non_neg_integer(),   %in inner nodes number of subnodes including itself, in leaf nodes number of items in the bucket
                     Bucket      :: mt_bucket(),         %item storage
                     Interval    :: mt_interval(),       %represented interval
                     Child_list  :: [mt_node()]
                    }.

-type mt_iter()     :: [mt_node() | [mt_node()]].
-type merkle_tree() :: {merkle_tree, mt_config(), Root::mt_node()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_bucket_size(merkle_tree()) -> pos_integer().
get_bucket_size({merkle_tree, Config, _}) ->
    Config#mt_config.bucket_size.

-spec get_branch_factor(merkle_tree()) -> pos_integer().
get_branch_factor({merkle_tree, Config, _}) -> 
    Config#mt_config.branch_factor.    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_root(merkle_tree()) -> mt_node() | undefined.
get_root({merkle_tree, _, Root}) -> Root;
get_root(_) -> undefined.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec is_empty(merkle_tree()) -> boolean().
is_empty({merkle_tree, _, {_, 0, [], _I, []}}) -> true;
is_empty(_) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new(mt_interval()) -> merkle_tree().
new(I) ->
    new(I, []).

% @doc ConfParams = list of tuples defined by {config field name, value}
%       e.g. [{branch_factor, 32}, {bucket_size, 16}]
-spec new(mt_interval(), mt_config_params()) -> merkle_tree().
new(I, ConfParams) ->
    Config = build_config(ConfParams),
    [Root] = build_childs([{I, 0, []}], Config, []),
    gen_hash({merkle_tree, Config, Root}).

-spec new(mt_interval(), KeyList::mt_bucket(), mt_config_params())
        -> merkle_tree().
new(I, KeyList, ConfParams) ->
    gen_hash(bulk_build(I, KeyList, ConfParams)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether an interval is present in the merkle tree.
-spec lookup(mt_interval(), merkle_tree() | mt_node()) -> mt_node() | not_found.
lookup(I, {merkle_tree, _, Root}) ->
    lookup(I, Root);
lookup(I, {_, _, _, I, _ChildList} = Node) ->
    Node;
lookup(I, {_, _, _, NodeI, _ChildList} = Node) ->
    case intervals:is_subset(I, NodeI) of
        true  -> lookup_(I, Node);
        false -> not_found
    end.

%% @doc Helper for lookup/2. In contrast to lookup/2, assumes that I is a
%%      subset of the current node's interval.
-spec lookup_(mt_interval(), merkle_tree() | mt_node()) -> mt_node() | not_found.
lookup_(I, {_, _, _, I, _ChildList} = Node) ->
    Node;
lookup_(_I, {_, _, _, _NodeI, _ChildList = []}) ->
    not_found;
lookup_(I, {_, _, _, _NodeI, ChildList = [_|_]}) ->
    ?ASSERT(1 >= length([C || C <- ChildList,
                              intervals:is_subset(I, get_interval(C))])),
    case util:first_matching(ChildList,
                             fun(X) ->
                                     intervals:is_subset(I, get_interval(X))
                             end) of
        {ok, IChild} -> lookup_(I, IChild);
        failed       -> not_found
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_hash(merkle_tree() | mt_node()) -> mt_node_key().
get_hash({merkle_tree, _, Node}) -> get_hash(Node);
get_hash({Hash, _, _, _, _}) -> Hash.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_interval(merkle_tree() | mt_node()) -> intervals:interval().
get_interval({merkle_tree, _, Node}) -> get_interval(Node);
get_interval({_, _, _, I, _}) -> I.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_childs(merkle_tree() | mt_node()) -> [mt_node()].
get_childs({merkle_tree, _, Node}) -> get_childs(Node);
get_childs({_, _, _, _, Childs}) -> Childs.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc returns number of items in a bucket / returns 0 if requested node is not a leaf node
-spec get_item_count(merkle_tree() | mt_node()) -> non_neg_integer().
get_item_count({merkle_tree, _, Node}) -> get_item_count(Node);
get_item_count({_, C, _, _, []}) -> C;
get_item_count({_, _, _, _, _}) -> 0.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec is_leaf(merkle_tree() | mt_node()) -> boolean().
is_leaf({merkle_tree, _, Node}) -> is_leaf(Node);
is_leaf({_, _, _, _, []}) -> true;
is_leaf(_) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns true if given term is a merkle tree otherwise false.
-spec is_merkle_tree(any()) -> boolean().
is_merkle_tree({merkle_tree, _, _}) -> true;
is_merkle_tree(_) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_bucket(merkle_tree() | mt_node()) -> mt_bucket().
get_bucket({merkle_tree, _, Root}) -> get_bucket(Root);
get_bucket({_, C, Bucket, _, []}) when C > 0 -> Bucket;
get_bucket(_) -> [].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert_list(mt_bucket(), merkle_tree()) -> merkle_tree().
insert_list(Terms, Tree) ->
    lists:foldl(fun insert/2, Tree, Terms).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert(Key::?RT:key() | rr_recon:db_entry_enc(), merkle_tree()) -> merkle_tree().
insert(Key, {merkle_tree, Config = #mt_config{keep_bucket = true}, Root} = Tree) ->
    CheckKey = decode_key(Key),
    case intervals:in(CheckKey, get_interval(Root)) of
        true -> {merkle_tree, Config, insert_to_node(Key, CheckKey, Root, Config)};
        false -> Tree
    end.

-spec insert_to_node(Key::?RT:key() | rr_recon:db_entry_enc(), CheckKey::?RT:key(),
                     Node::mt_node(), Config::mt_config())
        -> NewNode::mt_node().
insert_to_node(Key, _CheckKey, {_Hash, Count, Bucket, Interval, []} = N, Config)
  when Count >= 0 andalso Count < Config#mt_config.bucket_size ->
    case lists:member(Key, Bucket) of
        false -> {nil, Count + 1, [Key | Bucket], Interval, []};
        _     -> N
    end;

insert_to_node(Key, CheckKey, {_, BucketSize, Bucket, Interval, []},
               #mt_config{ branch_factor = BranchFactor,
                           bucket_size = BucketSize } = Config) ->    
    ChildI = intervals:split(Interval, BranchFactor),
    NewLeafs = [begin 
                    {NewBucket, BCount} = 
                        lists:foldl(fun (K, {List, Sum} = Acc) -> 
                                             case intervals:in(K, I) of 
                                                 true -> {[K | List], Sum + 1}; 
                                                 false -> Acc 
                                             end 
                                    end, {[], 0}, Bucket),
                    {nil, BCount, NewBucket, I, []} 
                end 
                || I <- ChildI],
    insert_to_node(Key, CheckKey, {nil, 1 + BranchFactor, [], Interval, NewLeafs}, Config);

insert_to_node(Key, CheckKey, {Hash, Count, [], Interval, Childs = [_|_]} = Node, Config) ->
    {Dest0, Rest} = lists:partition(fun({_, _, _, I, _}) ->
                                            intervals:in(CheckKey, I)
                                    end, Childs),
    case Dest0 of
        [] -> error_logger:error_msg("InsertFailed!"), Node;
        [Dest|_] ->
            OldSize = node_size(Dest),
            NewDest = insert_to_node(Key, CheckKey, Dest, Config),
            {Hash, Count + (node_size(NewDest) - OldSize), [], Interval, [NewDest|Rest]}
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Builds a merkle tree with all given keys but does not create the hash
%%      values yet (use gen_hash/1 for that).
-spec bulk_build(Interval::mt_interval(), KeyList::mt_bucket(),
                 Params::mt_config_params()) -> MerkleTree::merkle_tree().
bulk_build(I, KeyList, Params) ->
    Config = build_config(Params),
    {merkle_tree, Config,
     hd(build_childs([{I, length(KeyList), KeyList}], Config, []))}.

%% @doc Builds a merkle node from the given KeyList assuming that the current
%%      node needs to be split because the KeyList is larger than
%%      Config#mt_config.bucket_size.
-spec p_bulk_build(CurNode::mt_node(), mt_config(), KeyList::mt_bucket())
        -> mt_node().
p_bulk_build({_, C, _, I, _}, Config, KeyList) ->
    ChildsI = intervals:split(I, Config#mt_config.branch_factor),
    IKList = keys_to_intervals(KeyList, ChildsI),
    ChildNodes = build_childs(IKList, Config, []),
    NCount = lists:foldl(fun(N, Acc) -> Acc + node_size(N) end, 0, ChildNodes),
    {nil, C + NCount, [], I, ChildNodes}.

-spec build_childs([{I::intervals:continuous_interval(), Count::non_neg_integer(),
                     mt_bucket()}], mt_config(), Acc::[mt_node()]) -> [mt_node()].
build_childs([{Interval, Count, Bucket} | T], Config, Acc) ->
    BucketSize = Config#mt_config.bucket_size,
    KeepBucket = Config#mt_config.keep_bucket,
    Node = if Count > BucketSize ->
                  p_bulk_build({nil, 1, [], Interval, []}, Config, Bucket);
              KeepBucket ->
                  % let gen_hash/1 hash the leafs
                  {nil, Count, Bucket, Interval, []};
              true ->
                  % need to hash here since we won't keep the bucket!
                  Hash = run_leaf_hf(lists:usort(Bucket), Interval,
                                     Config#mt_config.leaf_hf,
                                     Config#mt_config.signature_size),
                  {Hash, Count, [], Interval, []}
           end,
    build_childs(T, Config, [Node | Acc]);
build_childs([], _, Acc) ->
    Acc.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Assigns hash values to all nodes in the tree.
-spec gen_hash(merkle_tree()) -> merkle_tree().
gen_hash(Tree = {merkle_tree, #mt_config{keep_bucket = KeepBucket}, _Root}) ->
    gen_hash(Tree, not KeepBucket).

%% @doc Assigns hash values to all nodes in the tree, remove the buckets
%%      afterwards if CleanBuckets is set.
-spec gen_hash(merkle_tree(), CleanBuckets::boolean()) -> merkle_tree().
gen_hash({merkle_tree, Config = #mt_config{inner_hf = InnerHf,
                                           leaf_hf = LeafHf,
                                           signature_size = SigSize,
                                           keep_bucket = KeepBucket},
          Root}, CleanBuckets) ->
    RootNew = gen_hash_node(Root, InnerHf, LeafHf, SigSize, KeepBucket, CleanBuckets),
    {merkle_tree, Config#mt_config{keep_bucket = not CleanBuckets}, RootNew}.

-spec gen_hash_node(mt_node(), InnerHf::inner_hash_fun(), LeafHf::hash_fun(),
                    SigSize::pos_integer(), KeepBucket::boolean(),
                    CleanBuckets::boolean()) -> mt_node().
gen_hash_node({_, Count, [], I, [_|_] = List}, InnerHf, LeafHf, SigSize,
              OldKeepBucket, CleanBuckets) ->    
    NewChilds = [gen_hash_node(X, InnerHf, LeafHf, SigSize, OldKeepBucket,
                               CleanBuckets) || X <- List],
    Hash = InnerHf([get_hash(C) || C <- NewChilds]),
    {Hash, Count, [], I, NewChilds};
gen_hash_node({_, _Count, _Bucket, _I, []} = N, _InnerHf, _LeafHf, _SigSize,
              false, _CleanBuckets) ->
    % if keep_bucket was false, we already hashed the value in bulk_build and
    % cannot insert any more values
    N;
gen_hash_node({_, Count, Bucket, I, [] = Childs}, _InnerHf, LeafHf, SigSize,
              true, CleanBuckets) ->
    Bucket1 = lists:usort(Bucket),
    Hash = run_leaf_hf(Bucket1, I, LeafHf, SigSize),
    {Hash, Count, ?IIF(CleanBuckets, [], Bucket1), I, Childs}.

%% @doc Hashes a leaf with the given (sorted!) bucket.
-spec run_leaf_hf(mt_bucket(), intervals:interval(), LeafHf::hash_fun(),
                  SigSize::pos_integer()) -> mt_node_key().
run_leaf_hf(Bucket, I, LeafHf, SigSize) ->
    BinBucket = case Bucket of
                    [_|_] -> term_to_binary({Bucket, I});
                    []    -> term_to_binary({0, I})
                end,
    Hash = LeafHf(BinBucket),
    Size = erlang:byte_size(Hash),
    if Size > SigSize  ->
           Start = Size - SigSize,
           <<_:Start/binary, SmallHash:SigSize/binary>> = Hash,
           SmallHash;
       true -> Hash
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns the total number of nodes in a tree or node (inner nodes and leafs)
-spec size(merkle_tree() | mt_node()) -> non_neg_integer().
size({merkle_tree, _, Root}) -> node_size(Root);
size(Node) -> node_size(Node).

-spec node_size(mt_node()) -> non_neg_integer().
node_size({_, _, _, _, []}) -> 1;
node_size({_, C, _, _, [_|_]}) -> C.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns a tuple with number of inner nodes and leaf nodes.
-spec size_detail(merkle_tree()) -> mt_size().
size_detail({merkle_tree, _, Root}) ->
    size_detail_node([Root], 0, 0).

-spec size_detail_node([mt_node() | [mt_node()]], InnerNodes::non_neg_integer(),
                       Leafs::non_neg_integer()) -> mt_size().
size_detail_node([{_, _, _, _, [_|_] = Childs} | R], Inner, Leafs) ->
    size_detail_node([Childs | R], Inner + 1, Leafs);
size_detail_node([{_, _, _, _, []} | R], Inner, Leafs) ->
    size_detail_node(R, Inner, Leafs + 1);
size_detail_node([], InnerNodes, Leafs) -> 
    {InnerNodes, Leafs};
size_detail_node([[{_, _, _, _, [_|_] = Childs} | R1] | R2], Inner, Leafs) ->
    size_detail_node([Childs, R1 | R2], Inner + 1, Leafs);
size_detail_node([[{_, _, _, _, []} | R1] | R2], Inner, Leafs) ->
    size_detail_node([R1 | R2], Inner, Leafs + 1);
size_detail_node([[] | R2], Inner, Leafs) -> 
    size_detail_node(R2, Inner, Leafs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns an iterator to visit all tree nodes with next
%      Iterates over all tree nodes from left to right in a deep first manner.
-spec iterator(Tree::merkle_tree()) -> Iter::mt_iter().
iterator({merkle_tree, _, Root}) -> [Root].

-spec iterator_node(Node::mt_node(), mt_iter()) -> mt_iter().
iterator_node({_, _, _, _, [_|_] = Childs}, Iter1) ->
    [Childs | Iter1];
iterator_node({_, _, _, _, []}, Iter1) ->
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
store_node_to_DOT({_, C, _, I, []}, Fileid, MyId, NextFreeId, #mt_config{ bucket_size = BuckSize }) ->
    {LBr, _LKey, _RKey, RBr} = intervals:get_bounds(I),
    io:fwrite(Fileid, "    ~p [label=\"~s~p,~p~s ; ~p/~p\", shape=box]~n",
              [MyId, erlang:atom_to_list(LBr),
               ?DOT_SHORTNAME(_LKey), ?DOT_SHORTNAME(_RKey),
               erlang:atom_to_list(RBr), C, BuckSize]),
    NextFreeId;
store_node_to_DOT({_, _, _ , I, [_|RChilds] = Childs}, Fileid, MyId, NextFreeId, TConf) ->
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
    io:fwrite(Fileid, "    ~p [label=\"~s~p,~p~s\"""]~n",
              [MyId, erlang:atom_to_list(LBr),
               ?DOT_SHORTNAME(_LKey), ?DOT_SHORTNAME(_RKey),
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
                  branch_factor  -> Conf#mt_config{ branch_factor = Val };
                  bucket_size    -> Conf#mt_config{ bucket_size = Val };
                  leaf_hf        -> Conf#mt_config{ leaf_hf = Val };
                  inner_hf       -> Conf#mt_config{ inner_hf = Val };
                  keep_bucket    -> Conf#mt_config{ keep_bucket = Val};
                  signature_size -> Conf#mt_config{ signature_size = Val}
              end
      end,
      #mt_config{}, ParamList).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Decodes a key for use by the merkle_tree.
-spec decode_key(Key::rr_recon:db_entry_enc() | ?RT:key()) -> ?RT:key().
decode_key(Key) ->
    case rr_recon:decodeBlob(Key) of
        {K, _} -> K;
        fail -> Key
    end.

% @doc inserts key into its matching interval
%      precondition: key fits into one of the given intervals
-spec p_key_in_I(Key, CheckKey::?RT:key(), ReverseLeft::[Bucket],
                 Right::[Bucket,...]) -> [Bucket,...] when
    is_subtype(Key,    Key::?RT:key() | rr_recon:db_entry_enc()),
    is_subtype(Bucket, {I::intervals:continuous_interval(), Count::non_neg_integer(), mt_bucket()}).
p_key_in_I(Key, CheckKey, ReverseLeft, [{Interval, C, L} = P | Right]) ->
    case intervals:in(CheckKey, Interval) of
        true  -> lists:reverse(ReverseLeft, [{Interval, C + 1, [Key | L]} | Right]);
        false -> p_key_in_I(Key, CheckKey, [P | ReverseLeft], Right)
    end.

-spec keys_to_intervals(mt_bucket(), [I,...]) -> [{I, Count, mt_bucket()}] when
    is_subtype(I,     intervals:continuous_interval()),
    is_subtype(Count, non_neg_integer()).
keys_to_intervals(KList, IList) ->
    IBucket = [{I, 0, []} || I <- IList],
    lists:foldr(fun(Key, Acc) ->
                        p_key_in_I(Key, decode_key(Key), [], Acc)
                end, IBucket, KList).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_XOR_fun() -> inner_hash_fun().
get_XOR_fun() ->
    fun([H|T]) -> lists:foldl(fun(X, Acc) -> util:bin_xor(X, Acc) end, H, T) end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% allow future versions to create more hash funs by having an integer parameter
-spec tester_create_hash_fun(I::1) -> hash_fun().
tester_create_hash_fun(1) -> fun crypto:sha/1.

% allow future versions to create more hash funs by having an integer parameter
-spec tester_create_inner_hash_fun(I::1) -> inner_hash_fun().
tester_create_inner_hash_fun(1) -> merkle_tree:get_XOR_fun().
