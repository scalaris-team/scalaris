% @copyright 2011 Zuse Institute Berlin

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

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([new/1, new/3, insert/3, empty/0,
         lookup/2, 
         set_root_interval/2, size/1, gen_hashes/1,
         is_empty/1, is_leaf/1, get_bucket/1,
         get_hash/1, get_interval/1, get_childs/1]).

-ifdef(with_export_type_support).
-export_type([mt_config/0, merkle_tree/0, mt_node/0, mt_node_key/0]).
-endif.

%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type mt_node_key()     :: binary() | nil.
-type mt_interval()     :: intervals:interval(). 
-type mt_bucket()       :: orddict:orddict() | nil.
-type hash_fun()        :: fun((binary()) -> mt_node_key()).
-type inner_hash_fun()  :: fun(([mt_node_key()]) -> mt_node_key()).

-record(mt_config,
        {
         branch_factor  = 2                 :: pos_integer(),   %number of childs per inner node
         bucket_size    = 24                :: pos_integer(),   %max items in a leaf
         leaf_hf        = fun crypto:sha/1  :: hash_fun(),      %hash function for leaf signature creation
         inner_hf       = get_XOR_fun()     :: inner_hash_fun(),%hash function for inner node signature creation - 
         gen_hash_on    = value             :: value | key      %node hash will be generated on value or an key         
         }).
-type mt_config() :: #mt_config{}.

-type mt_node() :: { Hash        :: mt_node_key(),       %hash of childs/containing items 
                     Count       :: non_neg_integer(),   %in inner nodes number of subnodes, in leaf nodes bucket size
                     Bucket      :: mt_bucket(),         %item storage
                     Interval    :: mt_interval(),       %represented interval
                     Child_list  :: [mt_node()]
                    }.

%-opaque merkle_tree() :: {mt_config(), Root::mt_node()}.
-type merkle_tree() :: {mt_config(), Root::mt_node()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Empty
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc Insert on an empty tree fail. First operation on an empty tree should be set_interval.
%      Returns an empty merkle tree ready for work.
% @end
-spec empty() -> merkle_tree().
empty() ->
    {#mt_config{}, {nil, 0, nil, intervals:empty(), []}}.

-spec is_empty(merkle_tree()) -> boolean().
is_empty({_, {nil, 0, nil, I, []}}) -> intervals:is_empty(I);
is_empty(_) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% New
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec new(mt_interval()) -> merkle_tree().
new(Interval) ->
    {#mt_config{}, {nil, 0, orddict:new(), Interval, []}}.

-spec new(mt_interval(), mt_config()) -> merkle_tree().
new(Interval, Conf) ->
    {Conf, {nil, 0, orddict:new(), Interval, []}}.

-spec new(mt_interval(), Branch_factor::pos_integer(), Bucket_size::pos_integer()) -> merkle_tree().
new(Interval, BranchFactor, BucketSize) ->
    {#mt_config{ branch_factor = BranchFactor, bucket_size = BucketSize }, 
     {nil, 0, orddict:new(), Interval, []}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Lookup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec lookup(mt_interval(), merkle_tree() | mt_node()) -> mt_node() | not_found.
lookup(I, {_, Root}) ->
    lookup(I, Root);
lookup(I, {_, _, _, I, _} = Node) ->
    Node;
lookup(I, {_, _, _, NodeI, ChildList} = Node) ->
    case intervals:is_subset(I, NodeI) of
        true when length(ChildList) =:= 0 -> Node;
        true -> 
            IChilds = lists:filter(fun({_, _, _, CI, _}) -> intervals:is_subset(I, CI) end, ChildList),
            case length(IChilds) of
                0 -> not_found;
                1 -> [IChild] = IChilds, 
                     lookup(I, IChild);
                _ -> erlang:throw('tree interval not correct splitted')
            end;
        false -> not_found
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Getter for mt_node / merkle_tree (root)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec get_hash(merkle_tree() | mt_node()) -> mt_node_key().
get_hash({_, Node}) -> get_hash(Node);
get_hash({Hash, _, _, _, _}) -> Hash.

-spec get_interval(merkle_tree() | mt_node()) -> intervals:interval().
get_interval({_, Node}) -> get_interval(Node);
get_interval({_, _, _, I, _}) -> I.

-spec get_childs(merkle_tree() | mt_node()) -> [mt_node()].
get_childs({_, Node}) -> get_childs(Node);
get_childs({_, _, _, _, Childs}) -> Childs.

-spec is_leaf(merkle_tree() | mt_node()) -> boolean().
is_leaf({_, Node}) -> is_leaf(Node);
is_leaf({_, _, _, _, []}) -> true;
is_leaf(_) -> false.

-spec get_bucket(merkle_tree | mt_node()) -> [{Key::term(), Value::term()}].
get_bucket({_, Root}) -> get_bucket(Root);
get_bucket({_, C, Bucket, _, []}) when C > 0 -> orddict:to_list(Bucket);
get_bucket(_) -> [].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% set_root_interval
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec set_root_interval(mt_interval(), merkle_tree()) -> merkle_tree().
set_root_interval(I, {Conf, _}) ->
    new(I, Conf).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Insert
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec insert(Key::term(), Val::term(), merkle_tree()) -> merkle_tree().
insert(Key, Val, {Config, Root}) ->
    Changed = insert_to_node(Key, Val, Root, Config),
    {Config, Changed}.


-spec insert_to_node(Key::term(), Val::term(), mt_node(), mt_config()) -> mt_node().

insert_to_node(Key, Val, {Hash, Count, Bucket, Interval, []}, Config) 
  when Count >= 0 andalso Count < Config#mt_config.bucket_size ->
    {Hash, Count + 1, orddict:store(Key, Val, Bucket), Interval, []};

insert_to_node(Key, Val, {_, Count, Bucket, Interval, []}, Config) 
  when Count =:= Config#mt_config.bucket_size ->
    ChildI = intervals:split(Interval, Config#mt_config.branch_factor),
    NewLeafs = lists:map(fun(I) -> 
                              NewBucket = orddict:filter(fun(K, _) -> intervals:in(K, I) end, Bucket),
                              {nil, orddict:size(NewBucket), NewBucket, I, []}
                         end, ChildI),
    insert_to_node(Key, Val, {nil, 1 + Config#mt_config.branch_factor, nil, Interval, NewLeafs}, Config);

insert_to_node(Key, Val, {Hash, Count, nil, Interval, Childs}, Config) ->
    {_Dest, Rest} = lists:partition(fun({_, _, _, I, _}) -> intervals:in(Key, I) end, Childs),
    length(_Dest) =:= 0 andalso ?TRACE("THIS SHOULD NOT HAPPEN! Key=~p ; NodeI=~p", [Key, Interval]),
    Dest = hd(_Dest),
    OldSize = node_size(Dest),
    NewDest = insert_to_node(Key, Val, Dest, Config),
    {Hash, Count + (node_size(NewDest) - OldSize), nil, Interval, [NewDest|Rest]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Generate Signatures/Hashes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec gen_hashes(merkle_tree()) -> merkle_tree().
gen_hashes({Config, Root}) ->
    {Config, gen_hash(Root, Config)}.

gen_hash({_, Count, Bucket, I, []}, Config = #mt_config{ gen_hash_on = HashProp }) ->
    LeafHf = Config#mt_config.leaf_hf,
    Hash = case Count > 0 of
               true ->
                   ToHash = case HashProp of
                                key -> orddict:fetch_keys(Bucket);
                                value -> lists:map(fun({_, V}) -> V end, 
                                                   orddict:to_list(Bucket))
                            end,
                   LeafHf(erlang:term_to_binary(ToHash));
               _ -> LeafHf(term_to_binary(0))
           end,
    {Hash, Count, Bucket, I, []};
gen_hash({_, Count, nil, I, List}, Config) ->    
    NewChilds = lists:map(fun(X) -> gen_hash(X, Config) end, List),
    InnerHf = Config#mt_config.inner_hf,
    Hash = InnerHf(lists:map(fun({H, _, _, _, _}) -> H end, NewChilds)),
    {Hash, Count, nil, I, NewChilds}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec size(merkle_tree() | mt_node()) -> non_neg_integer().
size({_, Root}) ->
    node_size(Root);
size(Node) -> node_size(Node).

-spec node_size(mt_node()) -> non_neg_integer().
node_size({_, _, _, _, []}) ->
    1;
node_size({_, C, _, _, _}) ->
    C.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_XOR_fun() ->
    (fun([H|T]) -> lists:foldl(fun(X, Acc) -> binary_xor(X, Acc) end, H, T) end).

-spec binary_xor(binary(), binary()) -> binary().
binary_xor(A, B) ->
    Size = bit_size(A),
    <<X:Size>> = A,
    <<Y:Size>> = B,
    <<(X bxor Y):Size>>.
