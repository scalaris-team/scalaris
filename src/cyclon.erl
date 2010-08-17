%  @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Christian Hennig <hennig@zib.de>
%% @doc Cyclon overlay network.
%% 
%%      This can be used in order to get random nodes, e.g. by vivaldi and
%%      gossip.
%% @end
%% @reference S. Voulgaris, D. Gavidia, M. van Steen. CYCLON:
%% Inexpensive Membership Management for Unstructured P2P Overlays.
%% Journal of Network and Systems Management, Vol. 13, No. 2, June 2005.
%% @version $Id$
-module(cyclon).
-author('hennig@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).

% functions gen_component, the trigger and the config module use
-export([init/1, on/2, get_base_interval/0, check_config/0]).

% helpers for creating getter messages:
-export([get_subset_rand/1, get_subset_rand/2,
         get_subset_rand_next_interval/1, get_subset_rand_next_interval/2]).

%% -export([get_ages/0, get_ages/1]).

%% State of the cyclon process:
%% {Cache, Node, Cycles, TriggerState}
%% Node: the scalaris node of this cyclon-task
%% Cycles: the amount of shuffle-cycles
-type(state() :: {RandomNodes::cyclon_cache:cache(),
                  MyNode::node:node_type() | null,
                  Cycles::integer(), TriggerState::trigger:state()}).

% accepted messages of cyclon process
-type(message() ::
    {trigger} |
    {node_update, Node::node:node_type()} |
    {check_state} |
    {cy_subset, SourcePid::comm:mypid(), PSubset::cyclon_cache:cache()} |
    {cy_subset_response, QSubset::cyclon_cache:cache(), PSubset::cyclon_cache:cache()} |
    {get_node_details_response, node_details:node_details()} |
    {get_ages, SourcePid::comm:erl_local_pid()} |
    {get_subset_rand, N::pos_integer(), SourcePid::comm:erl_local_pid()} |
    {'$gen_cast', {debug_info, Requestor::comm:erl_local_pid()}}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions that create and send messages to nodes requesting information.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a response message to a request for the ages in the cache.
-spec msg_get_ages_response(comm:erl_local_pid(), [cyclon_cache:age()]) -> ok.
msg_get_ages_response(Pid, Ages) ->
    comm:send_local(Pid, {cy_ages, Ages}).

%% @doc Sends a response message to a request for (a subset of) the cache.
-spec msg_get_subset_response(comm:erl_local_pid(), [node:node_type()]) -> ok.
msg_get_subset_response(Pid, Cache) ->
    comm:send_local(Pid, {cy_cache, Cache}).

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
-spec get_subset_rand(N::pos_integer(), Pid::comm:erl_local_pid()) -> ok.
get_subset_rand(N, Pid) ->
    CyclonPid = pid_groups:get_my(cyclon),
    comm:send_local(CyclonPid, {get_subset_rand, N, Pid}).

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
-spec get_subset_rand_next_interval(N::pos_integer(), Pid::comm:erl_local_pid()) -> reference().
get_subset_rand_next_interval(N, Pid) ->
    CyclonPid = pid_groups:get_my(cyclon),
    comm:send_local_after(get_base_interval(), CyclonPid, {get_subset_rand, N, Pid}).

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
%% -spec get_ages(Pid::comm:erl_local_pid()) -> ok.
%% get_ages(Pid) ->
%%     CyclonPid = pid_groups:get_my(cyclon),
%%     comm:send_local(CyclonPid, {get_ages, Pid}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the cyclon process, registers it with the process dictionary and
%%      returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = config:read(cyclon_trigger),
    gen_component:start_link(?MODULE, Trigger,
                             [{pid_groups_join_as, DHTNodeGroup, cyclon}]).

%% @doc Initialises the module with an empty state.
-spec init(module()) -> state().
init(Trigger) ->
    request_node_details([node, pred, succ]),
    dht_node:register_for_node_change(self()),
    comm:send_local_after(100, self(), {check_state}),
    TriggerState = trigger:init(Trigger, ?MODULE),
    TriggerState2 = trigger:now(TriggerState),
    log:log(info,"[ CY ] Cyclon spawn: ~p~n", [comm:this()]),
    {cyclon_cache:new(), null, 0, TriggerState2}.

%% @doc message handler
-spec(on/2 :: (message(), state()) -> state()).
on({trigger}, {Cache, Node, Cycles, TriggerState} = State)  ->
    NewCache =
        case check_state(State) of
            fail -> Cache;
            _    -> enhanced_shuffle(Cache, Node)
        end,
    TriggerState2 = trigger:next(TriggerState),
    {NewCache, Node, Cycles + 1, TriggerState2};

on({node_update, Node}, {Cache, _OldNode, Cycles, TriggerState}) ->
    {Cache, Node, Cycles, TriggerState};

on({check_state}, State) ->
    check_state(State),
    State;

on({cy_subset, SourcePid, PSubset}, {Cache, Node, Cycles, TriggerState}) ->
    %io:format("subset~n", []),
    % this is received at node Q -> integrate results of node P
    ForSend = cyclon_cache:get_random_subset(get_shuffle_length(), Cache),
    comm:send(SourcePid, {cy_subset_response, ForSend, PSubset}),
    NewCache = cyclon_cache:merge(Cache, Node, PSubset, ForSend, get_cache_size()),
    {NewCache, Node, Cycles, TriggerState};

on({cy_subset_response, QSubset, PSubset}, {Cache, Node, Cycles, TriggerState}) ->
    %io:format("subset_response~n", []),
    % this is received at node P -> integrate results of node Q
    NewCache = cyclon_cache:merge(Cache, Node, QSubset, PSubset, get_cache_size()),
    {NewCache, Node, Cycles, TriggerState};

on({get_node_details_response, NodeDetails}, {OldCache, Node, Cycles, TriggerState}) ->
    Me = case node_details:contains(NodeDetails, node) of
             true -> node_details:get(NodeDetails, node);
             _    -> Node
         end,
    Cache =
        case node_details:contains(NodeDetails, pred) andalso
                 node_details:contains(NodeDetails, succ) andalso
                 not node:same_process(node_details:get(NodeDetails, pred), Me) andalso
                 (cyclon_cache:size(OldCache) =< 2) of
            true -> cyclon_cache:new(node_details:get(NodeDetails, pred),
                                     node_details:get(NodeDetails, succ));
            _ -> OldCache
        end,
    {Cache, Me, Cycles, TriggerState};

on({get_ages, Pid}, {Cache, _Node, _Cycles, _TriggerState} = State) ->
    msg_get_ages_response(Pid, cyclon_cache:get_ages(Cache)),
    State;

on({get_subset_rand, N, Pid}, {Cache, _Node, _Cycles, _TriggerState} = State) ->
    msg_get_subset_response(Pid, cyclon_cache:get_random_nodes(N, Cache)),
    State;

%% on({flush_cache}, {_Cache, Node, _Cycles, TriggerState}) ->
%%     request_node_details([pred, succ]),
%%     {cyclon_cache:new(), Node, 0, TriggerState};
%% on({start_shuffling}, {Cache, _Node, _Cycles, _TriggerState} = State) ->
%%     comm:send_local_after(config:read(cyclon_interval), self(), {shuffle}),
%%     State;

on({web_debug_info, Requestor}, {Cache, _Node, _Cycles, _TriggerState} = State) ->
    KeyValueList =
        [{"cache_size", cyclon_cache:size(Cache)},
         {"cache (age, node):", ""} | cyclon_cache:debug_format_by_age(Cache)],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

%% @doc enhanced shuffle with age
enhanced_shuffle(Cache, Node) ->
    Cache_1 = cyclon_cache:inc_age(Cache),
    {NewCache, NodeQ} = cyclon_cache:pop_oldest_node(Cache_1),
    Subset = cyclon_cache:get_random_subset(get_shuffle_length() - 1, NewCache),
    ForSend = cyclon_cache:add_node(Node, 0, Subset),
    %io:format("~p",[length(ForSend)]),
    comm:send_to_group_member(node:pidX(NodeQ), cyclon, {cy_subset, comm:this(), ForSend}),
    NewCache.

%% @doc simple shuffle without age
simple_shuffle(Cache, Node) ->
    {NewCache, NodeQ} = cyclon_cache:pop_random_node(Cache),
    Subset = cyclon_cache:get_random_subset(get_shuffle_length() - 1, NewCache),
    ForSend = cyclon_cache:add_node(Node, 0, Subset),
    %io:format("~p",[length(ForSend)]),
    comm:send_to_group_member(node:pidX(NodeQ), cyclon, {cy_subset, comm:this(), ForSend}),
    NewCache.

%% @doc Sends the local node's dht_node a request to tell us some information
%%      about itself.
%%      The node will respond with a
%%      {get_node_details_response, NodeDetails} message.
-spec request_node_details([node_details:node_details_name()]) -> ok.
request_node_details(Details) ->
    DHT_Node = pid_groups:get_my(dht_node),
    comm:send_local(DHT_Node, {get_node_details, comm:this(), Details}).

%% @doc Checks the current state. If the cache is empty or the current node is
%%      unknown, the local dht_node will be asked for these values and the check
%%      will be re-scheduled after 1s.
-spec check_state(state()) -> ok | fail.
check_state({Cache, Node, _Cycles, _TriggerState} = _State) ->
    % if the own node is unknown or the cache is empty (it should at least
    % contain the nodes predecessor and successor), request this information
    % from the local dht_node
    NeedsInfo1 = case cyclon_cache:size(Cache) of
                     0 -> [pred, succ];
                     _ -> []
                 end,
    NeedsInfo2 = case node:is_valid(Node) of
                     false -> [node];
                     true  -> []
                 end,
    NeedsInfo = NeedsInfo1 ++ NeedsInfo2,
    if 
        length(NeedsInfo) > 0 ->
            request_node_details(NeedsInfo),
            comm:send_local_after(1000, self(), {check_state}),
            fail;
        true ->
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters of the cyclon process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_atom(cyclon_trigger) and
    
    config:is_integer(cyclon_interval) and
    config:is_greater_than(cyclon_interval, 0) and
    
    config:is_integer(cyclon_cache_size) and
    config:is_greater_than(cyclon_cache_size, 2) and
    
    config:is_integer(cyclon_shuffle_length) and
    config:is_greater_than_equal(cyclon_shuffle_length, 1) and
    config:is_less_than_equal(cyclon_shuffle_length, config:read(cyclon_cache_size)).

%% @doc Gets the cyclon interval set in scalaris.cfg.
-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(cyclon_interval).

%% @doc Gets the cyclon_shuffle_length parameter that defines how many entries
%%      of the cache are exchanged.
-spec get_shuffle_length() -> pos_integer().
get_shuffle_length() ->
    config:read(cyclon_shuffle_length).

%% @doc Gets the cyclon_cache_size parameter that defines how many entries a
%%      cache should at most have.
-spec get_cache_size() -> pos_integer().
get_cache_size() ->
    config:read(cyclon_cache_size).
    
