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
%%% File    cyclon.erl
%%% @author Christian Hennig <hennig@zib.de>
%%% @doc    Cyclon overlay network.
%%% 
%%% This can be used in order to get random nodes, e.g. by vivaldi and gossip.
%%% @end
%%% Created :  1 Dec 2008 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
%% @reference S. Voulgaris, D. Gavidia, M. van Steen. CYCLON:
%% Inexpensive Membership Management for Unstructured P2P Overlays.
%% Journal of Network and Systems Management, Vol. 13, No. 2, June 2005.

-module(cyclon).
-author('hennig@zib.de').
-vsn('$Id $ ').

-behaviour(gen_component).

-include("../../include/scalaris.hrl").

%% API
-export([start_link/1, init/1, on/2, get_base_interval/0,
         get_subset_rand/1, get_subset_rand/2,
         get_subset_rand_next_interval/1, get_subset_rand_next_interval/2
        ]).

%% -export([get_ages/0, get_ages/1]).

-ifdef(types_not_builtin).
-type reference() :: erlang:reference().
-endif.

%% State of the cyclon process:
%% {Cache, Node, Cycles, Trigger, TriggerState}
%% Node: the scalaris node of this cyclon-task
%% Cycles: the amount of shuffle-cycles
-type(state() :: {cyclon_cache:cache(), node:node_type() | null, integer(), module(), any()}).

% accepted messages of cs_node processes
-type(message() ::
    {trigger} |
    {check_state} |
    {cy_subset, cs_send:mypid(), cyclon_cache:cache()} |
    {cy_subset_response, cyclon_cache:cache(), cyclon_cache:cache()} |
    {get_node_details_response, node_details:node_details_record() |
        [{pred, node_details:node_type()} |
         {node, node_details:node_type()} |
         {succ, node_details:node_type()}]} |
    {get_ages, cs_send:erl_local_pid()} |
    {get_subset_rand, pos_integer(), cs_send:erl_local_pid()} |
    {'$gen_cast', {debug_info, cs_send:erl_local_pid()}}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions that create and send messages to nodes requesting information.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a response message to a request for the ages in the cache.
-spec msg_get_ages_response(cs_send:erl_local_pid(), [cyclon_cache:age()]) -> ok.
msg_get_ages_response(Pid, Ages) ->
    cs_send:send_local(Pid, {cy_ages, Ages}).

%% @doc Sends a response message to a request for (a subset of) the cache.
-spec msg_get_subset_response(cs_send:erl_local_pid(), [cyclon_cache:age()]) -> ok.
msg_get_subset_response(Pid, Cache) ->
    cs_send:send_local(Pid, {cy_cache, Cache}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Getters
%
% Functions that other processes can call to receive information from the gossip
% process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a (local) message to the cyclon process of the requesting
%%      process' group asking for a random subset of the stored nodes.
%%      see on({get_subset_rand, N, SourcePid}, State) and
%%      msg_get_subset_response/2
-spec get_subset_rand(N::pos_integer()) -> ok.
get_subset_rand(N) ->
    get_subset_rand(N, self()).

%% @doc Sends a (local) message to the cyclon process of the requesting
%%      process' group asking for a random subset of the stored nodes to be
%%      send to Pid.
%%      see on({get_subset_rand, N, SourcePid}, State) and
%%      msg_get_subset_response/2
-spec get_subset_rand(N::pos_integer(), Pid::cs_send:erl_local_pid()) -> ok.
get_subset_rand(N, Pid) ->
    CyclonPid = process_dictionary:get_group_member(cyclon),
    cs_send:send_local(CyclonPid, {get_subset_rand, N, Pid}).

%% @doc Sends a delayed (local) message to the cyclon process of the requesting
%%      process' group asking for a random subset of the stored nodes with a
%%      delay equal to the cyclon_interval config parameter.
%%      see on({get_subset_rand, N, SourcePid}, State) and
%%      msg_get_subset_response/2
-spec get_subset_rand_next_interval(N::pos_integer()) -> reference().
get_subset_rand_next_interval(N) ->
    get_subset_rand_next_interval(N, self()).

%% @doc Sends a delayed (local) message to the cyclon process of the requesting
%%      process' group asking for a random subset of the stored nodes to be
%%      send to Pid with a delay equal to the cyclon_interval config parameter.
%%      see on({get_subset_rand, N, SourcePid}, State) and
%%      msg_get_subset_response/2
-spec get_subset_rand_next_interval(N::pos_integer(), Pid::cs_send:erl_local_pid()) -> reference().
get_subset_rand_next_interval(N, Pid) ->
    CyclonPid = process_dictionary:get_group_member(cyclon),
    cs_send:send_local_after(get_base_interval(), CyclonPid, {get_subset_rand, N, Pid}).

% only used in admin.erl with hard-coded messages -> special case, don't provide getters
%% %% @doc Sends a (local) message to the cyclon process of the requesting
%% %%      process' group asking for the age of all stored nodes.
%% %%      see on({get_ages, SourcePid}, State) and
%% %%      msg_get_ages_response/2
%% -spec get_ages() -> ok.
%% get_ages() ->
%%     get_ages(, self()).
%% 
%% %% @doc Sends a (local) message to the cyclon process of the requesting
%% %%      process' group asking for the age of all stored nodes to be send to Pid.
%% %%      see on({get_ages, SourcePid}, State) and
%% %%      msg_get_ages_response/2
%% -spec get_ages(Pid::cs_send:erl_local_pid()) -> ok.
%% get_ages(Pid) ->
%%     CyclonPid = process_dictionary:get_group_member(cyclon),
%%     cs_send:send_local(CyclonPid, {get_ages, Pid}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the cyclon process, registers it with the process dictionary and
%%      returns its pid for use by a supervisor.
-spec start_link(term()) -> {ok, pid()}.
start_link(InstanceId) ->
    Trigger = config:read(cyclon_trigger),
    gen_component:start_link(?MODULE, [Trigger], [{register, InstanceId, cyclon}]).

%% @doc Initialises the module with an empty state.
-spec init([module(),...]) -> state().
init([Trigger]) ->
    request_node_details([node, pred, succ]),
    cs_send:send_local_after(100, self(), {check_state}),
    TriggerState = Trigger:init(?MODULE),
    TriggerState2 = Trigger:trigger_first(TriggerState, 1),
    log:log(info,"[ CY ] Cyclon spawn: ~p~n", [cs_send:this()]),
    {cyclon_cache:new(), null, 0, Trigger, TriggerState2}.

%% @doc message handler
-spec(on/2 :: (message(), state()) -> state()).
on({trigger}, {Cache, Node, Cycles, Trigger, TriggerState} = State)  ->
    NewCache =
        case check_state(State) of
            fail -> Cache;
            _    -> enhanced_shuffle(Cache, Node)
        end,
    TriggerState2 = Trigger:trigger_next(TriggerState, 1),
    {NewCache, Node, Cycles + 1, Trigger, TriggerState2};

on({check_state}, State) ->
    check_state(State),
    State;

on({cy_subset, SourcePid, PSubset}, {Cache, Node, Cycles, Trigger, TriggerState}) ->
    %io:format("subset~n", []),
    % this is received at node Q -> integrate results of node P
    ForSend = cyclon_cache:get_random_subset(get_shuffle_length(), Cache),
    cs_send:send(SourcePid, {cy_subset_response, ForSend, PSubset}),
    NewCache = cyclon_cache:merge(Cache, Node, PSubset, ForSend, get_cache_size()),
    {NewCache, Node, Cycles, Trigger, TriggerState};

on({cy_subset_response, QSubset, PSubset}, {Cache, Node, Cycles, Trigger, TriggerState}) ->
    %io:format("subset_response~n", []),
    % this is received at node P -> integrate results of node Q
    NewCache = cyclon_cache:merge(Cache, Node, QSubset, PSubset, get_cache_size()),
    {NewCache, Node, Cycles, Trigger, TriggerState};

on({get_node_details_response, NodeDetails}, {_, Node, Cycles, Trigger, TriggerState}) ->
    Pred = node_details:get(NodeDetails, pred),
    Succ = node_details:get(NodeDetails, succ),
    Me = case node_details:get(NodeDetails, node) of
             unknown -> Node;
             X -> X
         end,
    Cache = case Pred =/= Node of
            true  -> cyclon_cache:new(Pred, Succ);
            false -> cyclon_cache:new()
    end,
    {Cache, Me, Cycles, Trigger, TriggerState};

on({get_ages, Pid}, {Cache, _Node, _Cycles, _Trigger, _TriggerState} = State) ->
    msg_get_ages_response(Pid, cyclon_cache:get_ages(Cache)),
    State;

on({get_subset_rand, N, Pid}, {Cache, _Node, _Cycles, _Trigger, _TriggerState} = State) ->
    msg_get_subset_response(Pid, cyclon_cache:get_random_nodes(N, Cache)),
    State;

%% on({flush_cache}, {_Cache, Node, _Cycles, Trigger, TriggerState}) ->
%%     request_node_details([pred, succ]),
%%     {cyclon_cache:new(), Node, 0, Trigger, TriggerState};
%% on({start_shuffling}, {Cache, _Node, _Cycles, _Trigger, _TriggerState} = State) ->
%%     cs_send:send_local_after(config:read(cyclon_interval), self(), {shuffle}),
%%     State;

on({'$gen_cast', {debug_info, Requestor}}, {Cache, _Node, _Cycles, _Trigger, _TriggerState} = State)  ->
    DebugCache = cyclon_cache:debug_format_by_age(Cache),
    KeyValueList =
        [{"cache_size",          cyclon_cache:size(Cache)},
         {"cache (age, node):",              ""} | DebugCache],
    cs_send:send_local(Requestor, {debug_info_response, KeyValueList}),
    State;

on(_, _State) ->
    unknown_event.

%% @doc enhanced shuffle with age
enhanced_shuffle(Cache, Node) ->
    Cache_1 = cyclon_cache:inc_age(Cache),
    {NewCache, NodeQ} = cyclon_cache:pop_oldest_node(Cache_1),
    Subset = cyclon_cache:get_random_subset(get_shuffle_length() - 1, NewCache),
    ForSend = cyclon_cache:add_node(Node, 0, Subset),
    %io:format("~p",[length(ForSend)]),
    cs_send:send_to_group_member(node:pidX(NodeQ), cyclon, {cy_subset, cs_send:this(), ForSend}),
    NewCache.

%% @doc simple shuffle without age
simple_shuffle(Cache, Node) ->
    {NewCache, NodeQ} = cyclon_cache:pop_random_node(Cache),
    Subset = cyclon_cache:get_random_subset(get_shuffle_length() - 1, NewCache),
    ForSend = cyclon_cache:add_node(Node, 0, Subset),
    %io:format("~p",[length(ForSend)]),
    cs_send:send_to_group_member(node:pidX(NodeQ), cyclon, {cy_subset, cs_send:this(), ForSend}),
    NewCache.

%% @doc Sends the local node's cs_node a request to tell us some information
%%      about itself.
%%      The node will respond with a
%%      {get_node_details_response, NodeDetails} message.
-spec request_node_details([node_details:node_details_name()]) -> ok.
request_node_details(Details) ->
    CS_Node = process_dictionary:get_group_member(cs_node),
    cs_send:send_local(CS_Node, {get_node_details, cs_send:this(), Details}).

%% @doc Checks the current state. If the cache is empty or the current node is
%%      unknown, the local cs_node will be asked for these values and the check
%%      will be re-scheduled after 1s.
-spec check_state(state()) -> ok | fail.
check_state({Cache, Node, _Cycles, _Trigger, _TriggerState} = _State) ->
    % if the own node is unknown or the cache is empty (it should at least
    % contain the nodes predecessor and successor), request this information
    % from the local cs_node
    NeedsInfo1 = case cyclon_cache:size(Cache) of
                     0 -> [pred, succ];
                     _ -> []
                 end,
    NeedsInfo2 = case Node of
                     null -> [node];
                     _ -> []
                 end,
    NeedsInfo = NeedsInfo1 ++ NeedsInfo2,
    if 
        length(NeedsInfo) > 0 ->
            request_node_details(NeedsInfo),
            cs_send:send_local_after(1000, self(), {check_state}),
            fail;
        true ->
            ok
    end.


%% @doc Gets the cyclon interval set in scalaris.cfg.
-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    _CyclonInterval = 
        case config:read(cyclon_interval) of 
            failed ->
                log:log(warning, "cyclon_interval not defined (see scalaris.cfg), using default (1000)~n"),
                1000;
            X -> X
        end.

%% @doc Gets the cyclon_shuffle_length parameter that defines how many entries
%%      of the cache are exchanged.
-spec get_shuffle_length() -> pos_integer().
get_shuffle_length() ->
    _ShuffleLength = 
        case config:read(cyclon_shuffle_length) of 
            failed ->
                log:log(warning, "cyclon_shuffle_length not defined (see scalaris.cfg), using default (8)~n"),
                8;
            X -> X
        end.

%% @doc Gets the cyclon_cache_size parameter that defines how many entries a
%%      cache should at most have.
-spec get_cache_size() -> pos_integer().
get_cache_size() ->
    _CacheSize = 
        case config:read(cyclon_cache_size) of 
            failed ->
                log:log(warning, "cyclon_cache_size not defined (see scalaris.cfg), using default (20)~n"),
                20;
            X when X > 2 ->
                X;
            _Y -> 
                log:log(warning, "cyclon_cache_size should be larger than 2 (see scalaris.cfg), using minnimum (3)~n"),
                3
        end.
    