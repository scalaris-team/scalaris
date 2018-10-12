% @copyright 2012-2018 Zuse Institute Berlin,

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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    This gen_component can be used to cache the intervals dht_nodes
%%         are responsible for. This can be used to send messages directyl to
%%         the dht_nodes, circumventing the routing mechanism. Is the responsible
%%         node not known yet, a separate requests is used to find the responsible
%%         node which used the configured routing protocol.
%%
%%         Note that ring reconfigurations or node failures, interval changes etc.
%%         are not handled by this module! This can lead to inconsistent lookups!
%%
%%         This component is intendet to be used in certain benchmarking settings
%%         in which the overhead of the underlying routing mechanism is not desired.
%% @end
-module(dht_node_cache).
-author('skrzypczak.de').

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).
-export([init/1, on/2]).

-export([cached_send/2, cached_send/3]).
-export([cached_send_to_local_replica/3, cached_send_to_local_replica/4]).

-include("gen_component.hrl").

-type undelivered_msg() :: {?RT:key(), comm:message()}.
-type cached_dht_node() :: {intervals:interval(), comm:mypid()}.
-type local_dht_node() :: {unknown, unknown, unknown} |
                         {intervals:interval(), comm:mypid(), comm:mypid()}.
-type state() :: {
                    [undelivered_msg()],
                    [cached_dht_node()],
                    local_dht_node()
                 }.

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, dht_node_cache}]).

-spec init([]) -> state().
init([]) -> {[], [], {unknown, unknown, unknown}}.


%%%%%%%%% API %%%%%%%%%%%

%% @doc Sends message Message to the dht_node responsible for key Key. Is the
%% dht_node is not known, it is fetched along with its interval and added
%% to the cache.
-spec cached_send(?RT:key(), comm:message(), non_neg_integer()) -> ok.
cached_send(Key, Message, LookupEnvPosition) ->
    NewMessage = setelement(LookupEnvPosition, Message, false),
    cached_send(Key, NewMessage).

%% @doc Sends message Message to the dht_node responsible for key Key. Is the
%% dht_node is not known, it is fetched along with its interval and added
%% to the cache.
-spec cached_send(?RT:key(), comm:message()) -> ok.
cached_send(Key, Message) ->
    Cache = pid_groups:find_a(dht_node_cache),
    comm:send_local(Cache, {cached_send, Key, Message}).

%% @doc Sends a single message Message to the local dht_node if it is reponsible
%% for at least one replica based on replication:get_keys(Key). If not, the message
%% is sent to a remote dht_node responsible for any of the replica keys.
%% Note: If the local node is reponsible for more than one key, any of them may be used.
-spec cached_send_to_local_replica(?RT:key(), non_neg_integer(), comm:message(),
                                   non_neg_integer()) -> ok.
cached_send_to_local_replica(Key, KeyPos, Message, LookupEnvPosition) ->
    NewMessage = setelement(LookupEnvPosition, Message, false),
    cached_send_to_local_replica(Key, KeyPos, NewMessage).

%% @doc Sends a single message Message to the local dht_node if it is reponsible
%% for at least one replica based on replication:get_keys(Key). If not, the message
%% is sent to an other dht_node responsible for any of the replica keys.
%% Note: If the local node is reponsible for more than one key, any of them may be used.
-spec cached_send_to_local_replica(?RT:key(), non_neg_integer(), comm:message()) -> ok.
cached_send_to_local_replica(Key, KeyPos, Message) ->
    Cache = pid_groups:find_a(dht_node_cache),
    comm:send_local(Cache, {cached_send_to_local_replica, Key, KeyPos, Message}).

%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(comm:message(), state()) -> state().
on({cached_send, Key, Msg},  State) ->
    {_Interval, LocalDhtNodeGlobalPid, LocalDhtNodeLocalPid} = local_dht_node(State),
    case responsible_dht_node(State, Key) of
        unknown ->
            NewState = add_undelivered_message(State, Key, Msg),

            Dest = pid_groups:find_a(routing_table),
            LookupEnvelope = dht_node_lookup:envelope(4, {cache_interval_lookup,
                                                        Key, comm:this(), '_'}),
            comm:send_local(Dest, {?lookup_aux, Key, 0, LookupEnvelope}),

            NewState;
        LocalDhtNodeGlobalPid ->
            comm:send_local(LocalDhtNodeLocalPid, Msg),
            State;
        DhtNodePid ->
            comm:send(DhtNodePid, Msg),
            State
    end;

on({cache_interval_lookup_reply, DhtNodePid, _Key, Interval}, State) ->
    {NewState, PoppedMsgs} = pop_undelivered_messages(State, Interval),
    [comm:send(DhtNodePid, Msg) || {_K, Msg} <- PoppedMsgs],

    LocalDhtNode = pid_groups:find_a(dht_node),
    NewState2 =
        case DhtNodePid =:= comm:make_global(LocalDhtNode) of
            true -> set_local_dht_node(State, DhtNodePid, LocalDhtNode, Interval);
            false -> NewState
        end,
    add_dht_node(NewState2, DhtNodePid, Interval);

on({cached_send_to_local_replica, Key, KeyPos, Msg}, State) ->
    {Interval, _GloalPid, LocalDhtNodeLocalPid} = local_dht_node(State),

    case Interval of
       unknown ->
            NewState = add_undelivered_message(State, {local, Key, KeyPos}, Msg),

            Dest = pid_groups:find_a(dht_node),
            This = comm:reply_as(comm:this(), 3, {local_send, Dest, '_'}),
            comm:send_local(Dest, {cache_interval_lookup, Key, This, _Cons=false}),

            NewState;
        _ ->
           send_to_local_if_possible(Key, KeyPos, Msg, LocalDhtNodeLocalPid, Interval),
           State
    end;

on({local_send, LocalPid, {cache_interval_lookup_reply, GlobalPid, _Key, Interval}}, State) ->
    NewState = set_local_dht_node(State, GlobalPid, LocalPid, Interval),
    {NewState2, LocalMsgs} = pop_undelivered_local_messages(NewState),
    [send_to_local_if_possible(Key, KeyPos, Msg, LocalPid, Interval) ||
     {{local, Key, KeyPos}, Msg} <- LocalMsgs],

    NewState2.

send_to_local_if_possible(Key, KeyPos, Msg, LocalDhtPid, LocalInterval) ->
    Keys = replication:get_keys(Key),
    LocalKeys = lists:filter(fun(K) -> intervals:in(K, LocalInterval) end, Keys),
    case LocalKeys of
        [] ->
            %% sending locally is not possible as the local dht node
            %% does not manage any replica... use random key
            Idx = randoms:rand_uniform(1, length(Keys)+1),
            TargetKey = lists:nth(Idx, Keys),
            NewMsg = setelement(KeyPos, Msg, TargetKey),
            comm:send_local(self(), {cached_send, TargetKey, NewMsg});
        _ ->
            TargetKey = hd(LocalKeys),
            NewMsg = setelement(KeyPos, Msg, TargetKey),
            comm:send_local(LocalDhtPid, NewMsg)
    end.

-spec add_undelivered_message(state(), ?RT:key(), comm:message()) -> state().
add_undelivered_message(State, Key, Msg) ->
    MsgList = element(1, State),
    setelement(1, State, [{Key, Msg} | MsgList]).

-spec pop_undelivered_messages(state(), intervals:interval()) -> [undelivered_msg()].
pop_undelivered_messages(State, Interval) ->
    AllMsgs = element(1, State),
    {Matching, Rest} = lists:partition(fun({Key, _Msg}) ->
                                            intervals:in(Key, Interval)
                                       end, AllMsgs),
    {setelement(1, State, Rest), Matching}.

-spec pop_undelivered_local_messages(state()) -> [undelivered_msg()].
pop_undelivered_local_messages(State) ->
    AllMsgs = element(1, State),
    {Matching, Rest} = lists:partition(fun({{local, _Key, _KeyPos}, _Msg}) -> true;
                                          ({_Key, _Msg}) -> false
                                       end, AllMsgs),
    {setelement(1, State, Rest), Matching}.

-spec add_dht_node(state(), comm:mypid(), intervals:interval()) -> state().
add_dht_node(State, DhtNodePid, Interval) ->
    DhtList = element(2, State),
    setelement(2, State, [{Interval, DhtNodePid} | DhtList]).

-spec responsible_dht_node(state() | [cached_dht_node()], ?RT:key()) ->
    unknown | cached_dht_node().
responsible_dht_node([], _Key) -> unknown;
responsible_dht_node([{Interval, Pid} | T], Key) ->
    case intervals:in(Key, Interval) of
        true -> Pid;
        false -> responsible_dht_node(T, Key)
    end;
responsible_dht_node(State, Key) ->
    responsible_dht_node(element(2, State), Key).

-spec set_local_dht_node(state(), comm:mypid(), comm:mypid(), intervals:interval()) -> state().
set_local_dht_node(State, GlobalPid, LocalPid, Interval) ->
    setelement(3, State, {Interval, GlobalPid, LocalPid}).
-spec local_dht_node(state()) -> cached_dht_node().
local_dht_node(State) -> element(3, State).

