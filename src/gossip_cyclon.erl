%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%  @copyright 2008-2014 Zuse Institute Berlin

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
%% @author Jens V. Fischer <jensvfischer@gmail.com>
%% @doc Gossip based membership management using CYCLON.
%%
%% CYCLON provides an unstructured overlay which can be used to obtain the address
%% of a random node from the entirety of all nodes. This is useful for instance for
%% gossiping algorithms which need a random peers to communicate with.
%%
%% The basic idea is as follows: Every node maintains cache of known peers. At the
%% beginning of every cycle a random peer is chosen from the cache and a random
%% subset of the node's neighbours is exchanged with that peer. The receiving
%% peer uses the subset to update its own cache and also sends back a subset of
%% its cache, which is merged with the cache of the initial peer.
%% For details refer to the given paper.
%% @end
%%
%% @reference S. Voulgaris, D. Gavidia, M. van Steen. CYCLON:
%% Inexpensive Membership Management for Unstructured P2P Overlays.
%% Journal of Network and Systems Management, Vol. 13, No. 2, June 2005.
%%
%% @version $Id$

-module(gossip_cyclon).
-author('hennig@zib.de').
-author('jensvfischer@gmail.com').
-behaviour(gossip_beh).
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

% gossip_beh
-export([init/1, check_config/0, trigger_interval/0, fanout/0,
        select_node/1, select_data/1, select_reply_data/4, integrate_data/3,
        handle_msg/2, notify_change/3, min_cycles_per_round/0, max_cycles_per_round/0,
        round_has_converged/1, web_debug_info/1, shutdown/1]).

-export([rm_check/3,
         rm_send_changes/5]).

% API
-export([get_subset_rand/1, get_subset_rand/2, get_subset_rand/3]).

%% for testing
-export([select_data_feeder/1]).
-export_type([data/0, state/0]).

-define(SEND_TO_GROUP_MEMBER(Pid, Process, Msg), comm:send(Pid, Msg, [{group_member, Process},
                                                                      {?quiet}, {channel, prio}, {no_keep_alive}])).

-define(TRACE_DEBUG(FormatString, Data), ok).
%% -define(TRACE_DEBUG(FormatString, Data),
%%         log:pal("[ Cyclon ~.0p ] " ++ FormatString, [ comm:this() | Data])).

%% print cache at the beginnig of every cycle in a dot friednly format
-define(PRINT_CACHE_FOR_DOT(MyNode, Cache), ok).
%% -define(PRINT_CACHE_FOR_DOT(MyNode, Cache), print_cache_dot(MyNode, Cache)).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type data() :: cyclon_cache:cache().
-type round() :: non_neg_integer().

-type state() :: {Nodes::cyclon_cache:cache(), %% the cache of random nodes
                  MyNode::node:node_type()}. %% the scalaris node of this process

-dialyzer({no_return, print_cache_dot/2}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Config Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%------- External config function (called by gossip module) -------%%

%% @doc The time interval in ms after which a new cycle is triggered by the gossip
%%      module.
-spec trigger_interval() -> pos_integer().
trigger_interval() -> % in ms
    config:read(gossip_cyclon_interval).


%% @doc The fanout (in cyclon always 1).
-spec fanout() -> pos_integer().
fanout() ->
    1.


%% @doc The minimum number of cycles per round.
%%      Returns infinity, as rounds are not implemented by cyclon.
-spec min_cycles_per_round() -> infinity.
min_cycles_per_round() ->
    infinity.


%% @doc The maximum number of cycles per round.
%%      Returns infinity, as rounds are not implemented by cyclon.
-spec max_cycles_per_round() -> infinity.
max_cycles_per_round() ->
    infinity.


%% @doc Gets the cyclon_shuffle_length parameter that defines how many entries
%%      of the cache are exchanged.
-spec shuffle_length() -> pos_integer().
shuffle_length() ->
    config:read(gossip_cyclon_shuffle_length).


%% @doc Gets the cyclon_cache_size parameter that defines how many entries a
%%      cache should at most have.
-spec cache_size() -> pos_integer().
cache_size() ->
    config:read(gossip_cyclon_cache_size).


%% @doc Cyclon doesn't need instantiabilty, so {gossip_cyclon, default} is always
%%      used.
-spec instance() -> {gossip_cyclon, default}.
-compile({inline, [instance/0]}).
instance() ->
    {gossip_cyclon, default}.


-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(gossip_cyclon_interval) and
    config:cfg_is_greater_than(gossip_cyclon_interval, 0) and

    config:cfg_is_integer(gossip_cyclon_cache_size) and
    config:cfg_is_greater_than(gossip_cyclon_cache_size, 2) and

    config:cfg_is_integer(gossip_cyclon_shuffle_length) and
    config:cfg_is_greater_than_equal(gossip_cyclon_shuffle_length, 1) and
    config:cfg_is_less_than_equal(gossip_cyclon_shuffle_length,
                                  config:read(gossip_cyclon_cache_size)).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a (local) message to the gossip module of the requesting process'
%%      group asking for a random subset of the stored nodes.
%%      The response in the form {cy_cache, [Node]} will be send (local) to the
%%      requesting process.
-spec get_subset_rand(N::pos_integer()) -> ok.
get_subset_rand(N) ->
    get_subset_rand(N, self()).

%% @doc Same as get_subset_rand/1 but sends the reply back to the given Pid.
-spec get_subset_rand(N::pos_integer(), Pid::comm:erl_local_pid()) -> ok.
get_subset_rand(N, SourcePid) ->
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {cb_msg, instance(), {get_subset_rand, N, SourcePid}}).

%% @doc Same as get_subset_rand/2 but adds the given delay using msg_delay.
-spec get_subset_rand(N::pos_integer(), Pid::comm:erl_local_pid(),
                      Delay::non_neg_integer()) -> ok.
get_subset_rand(N, SourcePid, Delay) ->
    Pid = pid_groups:get_my(gossip),
    msg_delay:send_local(Delay, Pid,
                         {cb_msg, instance(), {get_subset_rand, N, SourcePid}}).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initiate the gossip_cyclon module. <br/>
%%      Called by the gossip module upon startup. <br/>
%%      The Instance information is ignored, {gossip_cyclon, default} is always used.
-spec init(Args::[proplists:property()]) -> {ok, state()}.
init(Args) ->
    Neighbors = proplists:get_value(neighbors, Args),
    log:log(info, "[ Cyclon ~.0p ] activating...~n", [comm:this()]),
    rm_loop:subscribe(self(), cyclon,
                      fun gossip_cyclon:rm_check/3,
                      fun gossip_cyclon:rm_send_changes/5, inf),
    monitor:proc_set_value(?MODULE, 'shuffle', rrd:create(60 * 1000000, 3, counter)), % 60s monitoring interval
    Cache = case nodelist:has_real_pred(Neighbors) andalso
                     nodelist:has_real_succ(Neighbors) of
                true  -> cyclon_cache:new(nodelist:pred(Neighbors),
                                          nodelist:succ(Neighbors));
                false -> cyclon_cache:new()
            end,

    %% send two manual triggers to speed up startup performance
    %% (setting up a 5 sec trigger (default for cyclon) takes ~6 s)
    comm:send_local(pid_groups:get_my(gossip), {trigger_action, instance()}),
    Delay = trigger_interval() div (2*1000), % half a trigger interval in sec
    msg_delay:send_local(Delay, pid_groups:get_my(gossip), {trigger_action, instance()}),
    {ok, {Cache, nodelist:node(Neighbors)}}.


%% @doc Returns true, i.e. peer selection is done by gossip_cyclon module.
-spec select_node(State::state()) -> {true, state()}.
select_node(State) ->
    {true, State}.


%% @doc Select and prepare the subset of the cache to be sent to the peer. <br/>
%%      Called by the gossip module at the beginning of every cycle. <br/>
%%      The selected exchange data (i.e. the selected subset of the cache) is
%%      sent back to the gossip module as a message of the form
%%      {selected_data, Instance, ExchangeData}.
%%      gossip_trigger -> select_data() is equivalent to cy_shuffle in the old
%%      cyclon module.
-spec select_data(State::state()) -> {ok | discard_msg, state()}.
select_data({Cache, Node}=State) ->
    case check_state(State) of
        fail ->
            ?TRACE_DEBUG("Cycle: ~w: select_data -> fail.", [get_cycle()]),
            {discard_msg, State};
        _    ->
            ?TRACE_DEBUG("Cycle: ~w: select_data -> ok.", [get_cycle()]),
            ?PRINT_CACHE_FOR_DOT(Node, Cache),
            monitor:proc_set_value(?MODULE, 'shuffle',
                                   fun(Old) -> rrd:add_now(1, Old) end),
            Cache1 = cyclon_cache:inc_age(Cache),
            {Cache2, NodeQ} = cyclon_cache:pop_oldest_node(Cache1),
            Subset = cyclon_cache:get_random_subset(shuffle_length() - 1, Cache2),
            ForSend = cyclon_cache:add_node(Node, 0, Subset),
            Pid = pid_groups:get_my(gossip),
            comm:send_local(Pid, {selected_peer, instance(), {cy_cache, [NodeQ]}}),
            comm:send_local(Pid, {selected_data, instance(), ForSend}),
            {ok, {Cache2, Node}}
    end.


%% @doc Process the subset from the requestor (P) and select a subset as reply
%%      data (at Q). <br/>
%%      Called by the behaviour module upon a p2p_exch message. <br/>
%%      PSubset: exchange data (subset) from the p2p_exch request <br/>
%%      Ref: used by the gossip module to identify the request <br/>
%%      Round: ignored, as cyclon does not implement round handling
%%      p2p_exch msg -> seleft_reply_data() is equivalent to cy_subset msg in the
%%      old cyclon module.
-spec select_reply_data(PSubset::data(), Ref::pos_integer(), Round::round(),
    State::state()) -> {ok, state()}.
select_reply_data(PSubset, Ref, Round, {Cache, Node}) ->
    % this is received at node Q -> integrate results of node P
    QSubset = cyclon_cache:get_random_subset(shuffle_length(), Cache),
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {selected_reply_data, instance(), {QSubset, PSubset}, Ref, Round}),
    Cache1 = cyclon_cache:merge(Cache, Node, PSubset, QSubset, cache_size()),
    {ok, {Cache1, Node}}.


%% @doc Integrate the received subset (at node P). <br/>
%%      Called by the behaviour module upon a p2p_exch_reply message. <br/>
%%      QData: the subset from the peer (QSubset) and the subset wich was sent
%%          in the request (PSubset) <br/>
%%      Round: ignored, as cyclon does not implement round handling
%%      Upon finishing the processing of the data, a message of the form
%%      {integrated_data, Instance, RoundStatus} is to be sent to the gossip module.
%%      p2p_exch_reply msg -> integrate_data() is equivalent to the cy_subset_response
%%      msg in the old cyclon module.
-spec integrate_data(QData::{data(), data()}, Round::round(), State::state()) ->
    {ok, state()}.
integrate_data({QSubset, PSubset}, _Round, {Cache, Node}) ->
    Cache1 = cyclon_cache:merge(Cache, Node, QSubset, PSubset, cache_size()),
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {integrated_data, instance(), cur_round}),
    {ok, {Cache1, Node}}.


%% @doc Handle messages
-spec handle_msg(Message, State::state()) -> {ok, state()} when
      is_subtype(Message, {rm_changed, NewNode::node:node_type()} |
                          {get_ages, SourcePid::comm:erl_local_pid()} |
                          {get_subset_rand, N::pos_integer(), SourcePid::comm:erl_local_pid()} |
                          {get_node_details_response, node_details:node_details()} |
                          {get_dht_nodes_response, Nodes::[comm:mypid()]}).

%% replaces the reference to self's dht node with NewNode
handle_msg({rm_changed, NewNode}, {Cache, _Node}) ->
    {ok, {Cache, NewNode}};

%% msg from admin:print_ages()
%% request needs to be sent to the gossip module in the following form:
%% {cb_msg, instance(), {get_ages, Pid}}
handle_msg({get_ages, Pid}, {Cache, _Node} = State) ->
    comm:send_local(Pid, {cy_ages, cyclon_cache:get_ages(Cache)}),
    {ok, State};

%% msg from get_subset_random() (api)
%% also directly requested from api_vm:get_other_vms() (change?)
handle_msg({get_subset_rand, N, Pid}, {Cache, _Node} = State) ->
    comm:send_local(Pid, {cy_cache, cyclon_cache:get_random_nodes(N, Cache)}),
    {ok, State};

%% Response to a get_node_details message from self (via request_node_details()).
%% The node details are used to possibly update Me and the succ and pred are
%% possibly used to populate the cache.
%% Request_node_details() is called in check_state() (i.e. in on_active({cy_shuffle})).
handle_msg({get_node_details_response, NodeDetails}, {OldCache, Node} = State) ->
    case cyclon_cache:size(OldCache) =< 2 of
        true  ->
            Pred = node_details:get(NodeDetails, pred),
            Succ = node_details:get(NodeDetails, succ),
            NewCache =
                lists:foldl(
                  fun(N, CacheX) ->
                          case node:same_process(N, Node) of
                              false -> cyclon_cache:add_node(N, 0, CacheX);
                              true -> CacheX
                          end
                  end, OldCache, [Pred, Succ]),
            case cyclon_cache:size(NewCache) of
                0 -> % try to get the cyclon cache from one of the known_hosts
                    case config:read(known_hosts) of
                        [] -> ok;
                        [_|_] = KnownHosts ->
                            Pid = util:randomelem(KnownHosts),
                            EnvPid = comm:reply_as(comm:this(), 3, {cb_msg, instance(), '_'}),
                            comm:send(Pid, {get_dht_nodes, EnvPid}, [{?quiet}])
                    end;
                _ ->
                    ok
            end,
            {ok, {NewCache, Node}};
        false ->
            {ok, State}
    end;

%% used by the gossip_cyclon_feeder to add nodes to the cyclon cache
handle_msg({add_nodes_to_cache, Nodes}, {OldCache, Node}) ->
    NewCache =
        lists:foldl(
          fun(N, CacheX) ->
                  case node:same_process(N, Node) of
                      false -> cyclon_cache:add_node(N, 0, CacheX);
                      true -> CacheX
                  end
          end, OldCache, Nodes),
    {ok, {NewCache, Node}};

%% Response to get_dht_nodes message from service_per_vm. Contains a list of
%% registered dht nodes from service_per_vm. Initiated in
%% handle_msg({get_node_details_response, _NodeDetails} if the cache is empty.
%% Tries to get a cyclon cache from one of the received nodes if cache is
%% still empty.
%% This happens (i.a.?) when only one node is present. In this case the
%% get_node_details and the get_dht_nodes request are repeated every cycle
%% (TODO is this the intended behaviour?)
handle_msg({get_dht_nodes_response, Nodes}, {Cache, _Node} = State) ->
    Size = cyclon_cache:size(Cache),
    case Nodes of
        [] ->
            {ok, State};
        [_|_] when Size > 0 ->
            {ok, State};
        [Pid | _] ->
            ?SEND_TO_GROUP_MEMBER(Pid, gossip, {p2p_exch, instance(), comm:this(), Cache, 0}),
            {ok, State}
    end.


%% @doc Always returns false, as cyclon does not implement rounds.
-spec round_has_converged(State::state()) -> {false, state()}.
round_has_converged(State) ->
    {false, State}.


%% @doc Notifies the module about changes. <br/>
%%      Changes can be new rounds, leadership changes or exchange failures. All
%%      of them are ignored, as cyclon doesn't use / implements this features.
-spec notify_change(any(), any(), State::state()) -> {ok, state()}.
notify_change(_, _, State) ->
    {ok, State}.


%% @doc Returns a key-value list of debug infos for the Web Interface. <br/>
%%      Called by the gossip module upon {web_debug_info} messages.
-spec web_debug_info(state()) ->
    {KeyValueList::[{Key::string(), Value::string()},...], state()}.
web_debug_info({Cache, _Node}=State) ->
    KeyValueList =
        [{"gossip_cyclon", ""},
         {"cache_size", integer_to_list(cyclon_cache:size(Cache))},
         {"cache (age, node):", ""} | cyclon_cache:debug_format_by_age(Cache)],
    {KeyValueList, State}.


%% @doc Shut down the gossip_cyclon module. <br/>
%%      Called by the gossip module upon stop_gossip_task(CBModule).
-spec shutdown(State::state()) -> {ok, shutdown}.
shutdown(_State) ->
    % nothing to do
    {ok, shutdown}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Filter Function for subscribing to the rm loop
-spec rm_check(Neighbors, Neighbors, Reason) -> boolean() when
      is_subtype(Neighbors, nodelist:neighborhood()),
      is_subtype(Reason, rm_loop:reason()).
rm_check(OldNeighbors, NewNeighbors, _Reason) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors).


%% @doc Exec Function for subscribing to the rm loop.
%%      Sends changes to a subscribed cyclon process when the neighborhood
%%      changes.
-spec rm_send_changes(Pid::pid(), Tag::cyclon,
        OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(),
        Reason::rm_loop:reason()) -> ok.
rm_send_changes(Pid, cyclon, _OldNeighbors, NewNeighbors, _Reason) ->
    comm:send_local(Pid, {cb_msg, instance(), {rm_changed, nodelist:node(NewNeighbors)}}).


%% @doc Checks the current state. If the cache is empty or the current node is
%%      unknown, the local dht_node will be asked for these values and the check
%%      will be re-scheduled after 1s.
-spec check_state(state()) -> ok | fail.
check_state({Cache, _Node} = _State) ->
    NeedsInfo = case cyclon_cache:size(Cache) of
                    0 -> [pred, succ];
                    _ -> []
                end,
    case NeedsInfo of
        [_|_] -> request_node_details(NeedsInfo),
                 fail;
        []    -> ok
    end.


%% @doc Sends the local node's dht_node a request to tell us some information
%%      about itself.
%%      The node will respond with a {get_node_details_response, NodeDetails}
%%      message, which will be envoloped and passed to this module through the
%%      gossip module.
-spec request_node_details([node_details:node_details_name()]) -> ok.
request_node_details(Details) ->
    case pid_groups:get_my(dht_node) of
        failed ->
            ok;
        DHT_Node ->
            This = comm:this(),
            case comm:is_valid(This) of
                true ->
                    EnvPid = comm:reply_as(This, 3, {cb_msg, instance(), '_'}),
                    comm:send_local(DHT_Node, {get_node_details, EnvPid, Details}),
                    ok;
                false -> ok
            end
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Debugging and Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @doc Print the cache in a dot compatible format.
%%      Format: Self -> Reference1; Self -> Reference2 ; ...
%%      Prints references to nodes as local pids, so this produces meaningful
%%      results if all nodes are started in the same Erlang VM.
%%      TODO the fun in foldl only throws a 'has no local return' dialyzer warning
%%              if the PRINT_CACHE_FOR_DOT macro is set to ok
-compile({nowarn_unused_function, {print_cache_dot, 2}}).
-spec print_cache_dot(node:node_type(), data()) -> ok.
print_cache_dot(MyNode, Cache) ->
    Cycle = get_cycle(),
    MyPid = comm:make_local(node:pidX(MyNode)),
    Graph = lists:foldl(
                    fun({Node, _Age}, AccIn) ->
                        % printing MyPid causes dialyzer to think that this method does not have a "local return"
                        [AccIn, io_lib:format("~w -> ~w; ", [MyPid, comm:make_local(node:pidX(Node))])]
                    end, io_lib:format("[Cycle: ~w] ", [Cycle]), Cache),
    log:pal(lists:flatten(Graph)).


%% @doc Simple cycle counting meachanism
%%      This only works, if this function is called excactly once every cycle.
%%      For debugging purposes only, the gossip module provides more
%%      sophisticated cycle counting.
-spec get_cycle() -> non_neg_integer().
-compile({nowarn_unused_function, {get_cycle, 0}}).
get_cycle() ->
    case get(cycles) of
        undefined -> put(cycles, 1), 0;
        Cycle1 -> put(cycles, Cycle1+1), Cycle1
    end.


%% still fails
%% -spec init_feeder(Neighbors::nodelist:neighborhood()) -> {[proplists:property()]}.
%% init_feeder(Neighbors) ->
%%     {[{neighbors, Neighbors}]}.


-spec select_data_feeder(State::state()) -> {state()}.
select_data_feeder(State) ->
    monitor:proc_set_value(?MODULE, 'shuffle', rrd:create(60 * 1000000, 3, counter)), % 60s monitoring interval
    {State}.


%% node_details:node_details_name allows new_key, but
%% dht_node:on({get_node_details, Pid, Which}, State) doesn't
-compile({nowarn_unused_function, {request_node_details_feeder, 1}}).
-spec request_node_details_feeder([node_details:node_details_name()]) ->
    {[node_details:node_details_name()]}.
request_node_details_feeder(Details) ->
    {lists:filter(fun(Detail) -> Detail =/= new_key end, Details)}.

