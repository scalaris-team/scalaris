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
%% @doc    Merkle tree (hash tree) implementation
%%         with configurable bucketing, branching and hashing.
%%         The tree will evenly divide its interval with ints subnodes.
%%         If the item count of a leaf node exceeds bucket size the node
%%         will switch to an internal node. The items will be distributed to
%%         its new child nodes which evenly divide the parents interval.
%% @end
%% @version $Id$

-module(merkle_tree).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([new/1, new/2, insert/3, empty/0, is_empty/1,
         set_root_interval/2, size/1]).

-ifdef(with_export_type_support).
-export_type([mt_config/0, merkle_tree/0]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(mt_config,
        {
         branch_factor  = 2                 :: pos_integer(),   %number of childs per inner node
         bucket_size    = 64                :: pos_integer(),   %max items in a leaf
         leaf_hf        = fun crypto:sha/1  :: hash_fun(),      %hash function for leaf signature creation
         inner_hf       = get_XOR_fun()     :: inner_hash_fun() %hash function for inner node signature creation         
         }).
-type mt_config() :: #mt_config{}.

-type hash_fun()        :: fun((binary()) -> mt_node_key()).
-type inner_hash_fun()  :: fun(([mt_node_key()]) -> mt_node_key()).
-type mt_node_key()     :: binary() | nil.
-type mt_interval()     :: intervals:interval(). 
-type mt_bucket()       :: orddict:ordered_dictionary() | nil.

-type mt_node()         ::
                           {Hash        :: mt_node_key(),       %hash of childs/containing items 
                            Count       :: non_neg_integer(),   %in inner nodes number of subnodes, in leaf nodes bucket size
                            Bucket      :: mt_bucket(),         %item storage
                            Interval    :: mt_interval(),       %represented interval
                            Child_list  :: [mt_node()]}.

-opaque merkle_tree() :: {mt_config(), Root::mt_node()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Empty
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc Insert on an empty tree fail. First operation on an empty tree should be set_interval.
-spec empty() -> merkle_tree().
empty() ->
    {#mt_config{}, {nil, 0, nil, intervals:empty(), []}}.

-spec is_empty(merkle_tree()) -> boolean().
is_empty({_, {nil, 0, nil, I, []}}) -> intervals:is_empty(I);
is_empty(_) -> false.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% set_root_interval
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc Returns an empty merkle tree ready for work.
-spec set_root_interval(mt_interval(), merkle_tree()) -> merkle_tree().
set_root_interval(I, {Conf, _}) ->
    new(I, Conf).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% New
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec new(mt_interval()) -> merkle_tree().
new(Interval) ->
    {#mt_config{}, {nil, 0, orddict:new(), Interval, []}}.

-spec new(mt_interval(), mt_config()) -> merkle_tree().
new(Interval, Config) ->
    {Config, {nil, 0, orddict:new(), Interval, []}}.

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
    {[Dest], Rest} = lists:partition(fun({_, _, _, I, _}) -> intervals:in(Key, I) end, Childs),
    OldSize = size_node(Dest),
    NewDest = insert_to_node(Key, Val, Dest, Config),
    {Hash, Count + (size_node(NewDest) - OldSize), nil, Interval, [NewDest|Rest]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Size
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec size(merkle_tree()) -> non_neg_integer().
size({_, Root}) ->
    size_node(Root).

-spec size_node(mt_node()) -> non_neg_integer().
size_node({_, _, _, _, []}) ->
    1;
size_node({_, C, _, _, _}) ->
    C.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_XOR_fun() ->
    (fun([H|T]) -> lists:foldl(fun(X, Acc) -> X bxor Acc end, H, T) end).

