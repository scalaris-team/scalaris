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

%% @author Jens V. Fischer <jensvfischer@gmail.com>
%% @doc    Tests for the gossip_cyclon module
%% @end
%% @version $Id$
-module(gossip_cyclon_SUITE).

-author('jensvfischer@gmail.com').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

-define(NO_OF_NODES, 2).

all() ->
    [
     test_age_inc,
     test_pop_oldest,
     test_cache_size,
     test_select_data,
     test_select_reply_data,
     test_integrate_data
    ].


suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% monitor needs ring :-(
    unittest_helper:make_ring(
      ?NO_OF_NODES,
      [{config, [
                 {monitor_perf_interval, 0},  % deactivate monitor_perf
                 {gossip_log_level_warn, info},
                 {gossip_log_level_error, error}
                ]
       }]),
    unittest_helper:wait_for_stable_ring_deep(),
    monitor:proc_set_value(gossip_cyclon, 'shuffle', rrd:create(60 * 1000000, 3, counter)), % 60s monitoring interval
    Group = pid_groups:group_with(gossip),
    pid_groups:join_as(Group, gossip_cyclon_tester),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Testcases
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Test, that the age field in the returned cache is incremented by select_data
test_age_inc(_Config) ->
    tester:test(?MODULE, age_inc, 1, 250, []).

-spec age_inc(gossip_cyclon:state()) -> true.
age_inc(State) ->
    State1 = {Cache1, _} = consolidate_nodes(State),
    case gossip_cyclon:select_data(State1) of
        {ok, {NewCache, _Node}} ->
            %% check that the age was incremented
            lists:all(fun({Node, Age}) ->
                              {Node, OldAge} = lists:keyfind(Node, 1, Cache1),
                              OldAge =:= Age - 1
                      end, NewCache) andalso
            length(Cache1) =:= length(NewCache) + 1;
        {discard_msg, State2} ->
            ?equals(State2, State1)
    end.


%% @doc Test, that the node sent as selected_peer by select_data is the oldest
%%      node in the given cache.
test_pop_oldest(_Config) ->
    tester:test(?MODULE, pop_oldest, 1, 250, []).

-spec pop_oldest(gossip_cyclon:state()) -> true.
pop_oldest(State) ->
    {Cache1, MyNode} = consolidate_nodes(State),

    %% don't allow multiple entries with same age, otherwise the selection is non-deterministic
    Cache2 = lists:foldl(fun({Node, Age}, AccIn) ->
                                 case lists:keymember(Age, 2, AccIn) of
                                     true -> AccIn;
                                     false -> [{Node, Age}|AccIn]
                                 end
                         end, [], Cache1),

    trace_mpath:start(test_pop_oldest, [{filter_fun, generate_message_filter(selected_peer, 1)}]),
    Ret = gossip_cyclon:select_data({Cache2, MyNode}),
    trace_mpath:stop(),
    Result = case Ret of
        {ok, _State2} ->
            timer:sleep(5),
            [SendEvent] = trace_mpath:get_trace(test_pop_oldest),
            {Oldest, _Age} = lists:last(lists:keysort(2, Cache2)),
            {selected_peer,{gossip_cyclon,default},{cy_cache,[SendNode]}} = element(6, SendEvent),
            Oldest =:= SendNode;
        {discard_msg, State2} ->
            {Cache2, MyNode} =:= State2
    end,
    trace_mpath:cleanup(test_pop_oldest),
    Result.


%% @doc Test, that the cache size stays within the bounds given by the config
%%      and the algorithm.
test_cache_size(_Config) ->
    tester:test(?MODULE, check_cache_size, 5, 250, []).

-spec check_cache_size(gossip_cyclon:data(), gossip_cyclon:data(), pos_integer(),
                       non_neg_integer(), gossip_cyclon:state()) -> true.
check_cache_size(PSubset, QSubset, Ref, Round, State) ->
    PSubset1 = consolidate_nodes(PSubset),
    QSubset1 = consolidate_nodes(QSubset),
    State1 = consolidate_nodes(State),
    ExpectedMaxCacheSize = config:read(gossip_cyclon_cache_size),
    {_Tag, {NewCache1, _Node1}} = gossip_cyclon:select_data(State1),
    {ok, {NewCache2, _Node2}} = gossip_cyclon:select_reply_data(PSubset1, Ref, Round, State1),
    {ok, {NewCache3, _Node3}} = gossip_cyclon:integrate_data({QSubset1, PSubset1}, Round, State1),

    length(NewCache1) =< ExpectedMaxCacheSize andalso
    length(NewCache2) =< ExpectedMaxCacheSize andalso
    length(NewCache3) =< ExpectedMaxCacheSize andalso
    ((L1=length(NewCache1)) + 1 =:= (L2=length(element(1, State1))) orelse L1 =:= L2) andalso
    length(NewCache2) >= length(element(1, State1)) andalso
    length(NewCache3) >= length(element(1, State1)).


%% @doc Test, that the data sent by select_data is a subset of the given Cache.
test_select_data(_Config) ->
    tester:test(?MODULE, select_data, 1, 250, []).

-spec select_data(gossip_cyclon:state()) -> true.
select_data(State) ->
    {Cache1, MyNode} = consolidate_nodes(State),
    trace_mpath:start(test_select_data,
                      [{filter_fun, generate_message_filter(selected_data, 1)}]),
    Ret = gossip_cyclon:select_data({Cache1, MyNode}),
    trace_mpath:stop(),
    Result = case Ret of
        {ok, _} ->
            timer:sleep(5),
            [SendEvent] = trace_mpath:get_trace(test_select_data),
            {selected_data,{gossip_cyclon,default},Subset} = element(6, SendEvent),
            is_subset(Subset, [{MyNode, 0}|Cache1]);
        {discard_msg, State2} ->
            {Cache1, MyNode} =:= State2
    end,
    trace_mpath:cleanup(test_select_data),
    Result.


%% @doc Test, that the data sent by select_reply_data is a subset of the given Cache.
%%      Also tests, that the cache returned by select_reply_data is a subset of
%%      the union of the given Cache and the received Cache (implicit test of the
%%      cache merging).
test_select_reply_data(_Config) ->
    tester:test(?MODULE, select_reply_data, 4, 250, []).

-spec select_reply_data(gossip_cyclon:data(), pos_integer(),non_neg_integer(),
                        gossip_cyclon:state()) -> true.
select_reply_data(PSubset, Ref, Round, State) ->
    State1 = {Cache1, _} = consolidate_nodes(State),
    PSubset1 = consolidate_nodes(PSubset),

    trace_mpath:start(test_select_reply_data,
                      [{filter_fun, generate_message_filter(selected_reply_data, 1)}]),
    {ok, {RetCache, _}} = gossip_cyclon:select_reply_data(PSubset1, Ref, Round, State1),
    trace_mpath:stop(),
    timer:sleep(5),
    [SendEvent] = trace_mpath:get_trace(test_select_reply_data),
    trace_mpath:cleanup(test_select_reply_data),
    {selected_reply_data,{gossip_cyclon,default},{QSubset, PSubset1}, Ref, Round} = element(6, SendEvent),
    is_subset(QSubset, Cache1) andalso
    is_subset(RetCache, lists:usort(Cache1++PSubset1)).


%% @doc Test, that the cache returned by integrate_data is a subset of the union
%%      of the given Cache and the received Cache (implicit test of the cache merging).
test_integrate_data(_Config) ->
    tester:test(?MODULE, integrate_data, 4, 250, []).

-spec integrate_data(gossip_cyclon:data(), gossip_cyclon:data(), non_neg_integer(),
                     gossip_cyclon:state()) -> true.
integrate_data(PSubset, QSubset, Round, State) ->
    State1 = {Cache1, _} = consolidate_nodes(State),
    PSubset1 = consolidate_nodes(PSubset),
    QSubset1 = consolidate_nodes(QSubset),

    {ok, {RetCache, _}} = gossip_cyclon:integrate_data(
                                    {QSubset1, PSubset1}, Round, State1),
    is_subset(RetCache, lists:usort(QSubset1 ++ Cache1)).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Ensure globally (i.e. for all pid's produced by this testsuite), that all
%%      nodes with the same pid are identical in id and version id.
%%      This prevents 'got two nodes with same IDversion but different ID' errors.
%%      Uses the process dictionary to maintain state across calls.
-spec consolidate_nodes(gossip_cyclon:data()) -> gossip_cyclon:data();
                       (gossip_cyclon:state()) -> gossip_cyclon:state().
consolidate_nodes(Data) when is_list(Data) ->
    Data1 = lists:map(fun({Node, Age}) -> {check_node(Node), Age} end, Data),
    deduplicate(Data1);

consolidate_nodes(_State={Nodes, null}) ->
    Nodes1 = consolidate_nodes(Nodes),
    {Nodes1, null};

consolidate_nodes(_State={Nodes, MyNode}) ->
    Nodes1 = consolidate_nodes(Nodes),
    MyNode1 = check_node(MyNode),
    {Nodes1, MyNode1}.


%% @doc Check if a node with the same pid is already present in the process
%%      dictionary.
-spec check_node(node:node_type()) -> node:node_type().
check_node(Node) ->
    case get(Pid=node:pidX(Node)) of
        undefined ->
            put(Pid, Node), Node;
        NodeFromPD ->
            NodeFromPD
    end.


%% @doc Don't allow duplictes (i.e. a cache entry with the same pid (as ensured
%%      by consolidate_node), but with different ages) within one cache.
-spec deduplicate(cyclon_cache:cache()) -> cyclon_cache:cache().
deduplicate(Cache) ->
    lists:foldl(fun(Entry={Node, _}, AccIn) ->
                        case lists:keyfind(Node, 1, AccIn) of
                            false -> [Entry|AccIn];
                            {Node, _} -> AccIn
                        end
                end,
                [], Cache).


%% @doc Checks whether the given Subset is a subset of the given Cache.
%%      Ignores the age field, as this might be updated by the age incrementation.
-spec is_subset(cyclon_cache:cache(), cyclon_cache:cache()) -> boolean().
is_subset(Subset, Cache) ->
    lists:all(fun({Node, _Age}) -> lists:keymember(Node, 1, Cache) end, Subset).


%% @doc Generate a filter function for trace_mpath.
%%      Filters for send events with the given message tag at the given position
%%      (within the message of a send event).
-spec generate_message_filter(atom(), pos_integer()) ->
    fun((trace_mpath:trace_event()) -> boolean()).
generate_message_filter(Tag, Pos) ->
    fun({log_send, _, _, _, _, Message, _}) ->
            element(Pos, Message) =:= Tag;
       (_) ->
            false
    end.

