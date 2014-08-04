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

%% @author Jens V. Fischer <jensvfischer@gmail.com>
%% @doc Gossip based peer sampling.
%% @end
%% @version $Id$
-module(gossip_cyclon).
-behaviour(gossip_beh).
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

% gossip_beh
-export([init/1, check_config/0, trigger_interval/0, fanout/0,
        select_node/1, select_data/1, select_reply_data/4, integrate_data/3,
        handle_msg/2, notify_change/3, min_cycles_per_round/0, max_cycles_per_round/0,
        round_has_converged/1, get_values_best/1, get_values_all/1, web_debug_info/1,
        shutdown/1]).

-export([rm_check/3,
         rm_send_changes/5]).

% API
-export([get_subset_rand/1, get_subset_rand_next_interval/1, get_subset_rand_next_interval/2]).

%% for testing
-export([]).

-ifdef(with_export_type_support).
-endif.

-define(SEND_TO_GROUP_MEMBER(Pid, Process, Msg), comm:send(Pid, Msg, [{group_member, Process},
                                                                      {?quiet}, {channel, prio}, {no_keep_alive}])).

%% -define(TRACE_DEBUG(FormatString, Data), ok).
-define(TRACE_DEBUG(FormatString, Data),
        log:pal("[ Cyclon ~.0p ] " ++ FormatString, [ comm:this() | Data])).

%% print cache at the beginnig of every cycle in a dot friednly format
-define(PRINT_CACHE_FOR_DOT(MyNode, Cache), ok).
%% -define(PRINT_CACHE_FOR_DOT(MyNode, Cache), print_cache_dot(MyNode, Cache)).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type data() :: any().
-type round() :: non_neg_integer().

-type state() :: {Nodes::cyclon_cache:cache(), %% the cache of random nodes
                  MyNode::node:node_type() | null}. %% the scalaris node of this module

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Config Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%------- External config function (called by gossip module) -------%%

%% @doc The time interval in ms after which a new cycle is triggered by the gossip
%%      module.
-spec trigger_interval() -> pos_integer().
trigger_interval() -> % in ms
    config:read(gossip_cyclon_interval).


%% @doc The fanout (number of peers contacted per cycle).
-spec fanout() -> pos_integer().
fanout() ->
    config:read(gossip_cyclon_fanout).


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
    config:read(cyclon_shuffle_length).


%% @doc Gets the cyclon_cache_size parameter that defines how many entries a
%%      cache should at most have.
-spec cache_size() -> pos_integer().
cache_size() ->
    config:read(cyclon_cache_size).

%% @doc Cycon doesn't need instantiabilty, so {gossip_cyclon, default} is always
%%      used.
-spec instance() -> {gossip_cyclon, default}.
-compile({inline, [instance/0]}).
instance() ->
    {gossip_cyclon, default}.

-spec check_config() -> boolean().
check_config() ->
    true.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a (local) message to the gossip module of the requesting process'
%%      group asking for a random subset of the stored nodes.
%%      The response in the form {cy_cache, [Node]} will be send (local) to the
%%      requesting process.
-spec get_subset_rand(N::pos_integer()) -> ok.
get_subset_rand(N) ->
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {cb_msg, instance(), {get_subset_rand, N, self()}}).


%% @doc Same as get_subset_rand/1, but the request is delayed with a delay equal
%%      to the gossip_cyclon_interval config parameter.
-spec get_subset_rand_next_interval(N::pos_integer()) -> reference().
get_subset_rand_next_interval(N) ->
    get_subset_rand_next_interval(N, self()).


%% @doc Same as get_subset_rand_next_interval/1 but sends the reply back to the
%%      given Pid.
-spec get_subset_rand_next_interval(N::pos_integer(), Pid::comm:erl_local_pid()) -> reference().
get_subset_rand_next_interval(N, SourcePid) ->
    Pid = pid_groups:get_my(gossip),
    comm:send_local_after(trigger_interval(), Pid,
                          {cb_msg, instance(), {get_subset_rand, N, SourcePid}}).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initiate the gossip_cyclon module. <br/>
%%      Called by the gossip module upon startup. <br/>
%%      The Instance information is ignored, {gossip_cyclon, default} is always used.
-spec init(Args::[proplist:property()]) -> {ok, state()}.
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
    {ok, {Cache, nodelist:node(Neighbors)}}.


%% @doc Returns true, i.e. peer selection is done by gossip_cyclon module.
-spec select_node(State::state()) -> {true, state()}.
select_node(State) ->
    {true, State}.


%% @doc Select and prepare the cache to be sent to the peer. <br/>
%%      Called by the gossip module at the beginning of every cycle. <br/>
%%      The selected exchange data is sent back to the gossip module as a message
%%      of the form {selected_data, Instance, ExchangeData}.
%%      gossip_trigger -> select_data() is equivalent to cy_shuffle in the old
%%      cyclon module.
-spec select_data(State::state()) -> {ok, state()}.
select_data({Cache, Node}=State) ->
    %% ?TRACE_DEBUG("select_data", []),
    case check_state(State) of
        fail ->
            {retry, State};
        _    ->
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


%% @doc Process the data from the requestor (P) and select reply data (at Q). <br/>
%%      Called by the behaviour module upon a p2p_exch message. <br/>
%%      PData: exchange data from the p2p_exch request <br/>
%%      Ref: used by the gossip module to identify the request <br/>
%%      RoundStatus / Round: ignored, as cyclon does not implement round handling
%%      p2p_exch msg -> seleft_reply_data() is equivalent to cy_subset msg in the
%%      old cyclon module.
-spec select_reply_data(PSubset::data(), Ref::pos_integer(), Round::round(),
    State::state()) -> {discard_msg | ok | retry | send_back, state()}.
select_reply_data(PSubset, Ref, Round, {Cache, Node}) ->
    %% ?TRACE_DEBUG("select_reply_data", []),
    % this is received at node Q -> integrate results of node P
    QSubset = cyclon_cache:get_random_subset(shuffle_length(), Cache),
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {selected_reply_data, instance(), {QSubset, PSubset}, Ref, Round}),
    Cache1 = cyclon_cache:merge(Cache, Node, PSubset, QSubset, cache_size()),
    {ok, {Cache1, Node}}.


%% @doc Integrate the reply data. <br/>
%%      Called by the behaviour module upon a p2p_exch_reply message. <br/>
%%      QData: the reply data from the peer <br/>
%%      RoundStatus / Round: ignored, as cyclon does not implement round handling
%%      Upon finishing the processing of the data, a message of the form
%%      {integrated_data, Instance, RoundStatus} is to be sent to the gossip module.
-spec integrate_data(QData::data(), Round::round(), State::state()) ->
    {discard_msg | ok | retry | send_back, state()}.
integrate_data({QSubset, PSubset}, _Round, {Cache, Node}) ->
    %% cy_subset_response msg <=> p2p_exch_reply msg -> integrate_data()
    Cache1 = cyclon_cache:merge(Cache, Node, QSubset, PSubset, cache_size()),
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {integrated_data, instance(), cur_round}),
    {ok, {Cache1, Node}}.


%% @doc Handle messages
-spec handle_msg(Msg::comm:message(), State::state()) -> {ok, state()}.

%% replaces the reference to self's dht node with NewNode
handle_msg({rm_changed, NewNode}, {Cache, _Node}) ->
    ?TRACE_DEBUG("rm_changed", []),
    {ok, Cache, NewNode};

%% msg from admin:print_ages()
%% request needs to be sent to the gossip module in the following form:
%% {cb_msg, {gossip_cyclon, default}, {get_ages, Pid}}
handle_msg({get_ages, Pid}, {Cache, Node}) ->
    ?TRACE_DEBUG("get_ages", []),
    comm:send_local(Pid, {cy_ages, cyclon_cache:get_ages(Cache)}),
    {ok, {Cache, Node}};

%% msg from get_subset_random() (api)
%% also directly requested from api_vm:get_other_vms() (change?)
handle_msg({get_subset_rand, N, Pid}, {Cache, Node}) ->
    comm:send_local(Pid, {cy_cache, cyclon_cache:get_random_nodes(N, Cache)}),
    {ok, {Cache, Node}};

%% Response to a get_node_details message from self (via request_node_details()).
%% The node details are used to possibly update Me and the succ and pred are
%% possibly used to populate the cache.
%% Request_node_details() is called in check_state() (i.e. in on_active({cy_shuffle})).
handle_msg({get_node_details_response, NodeDetails}, {OldCache, Node}=State) ->
    ?TRACE_DEBUG("get_node_details_response", []),
    case cyclon_cache:size(OldCache) =< 2 of
        true  ->
            %% ?TRACE_DEBUG("get_node_details_response: true", []),
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
                            %% ?TRACE_DEBUG("get_node_details_response: request known-hosts", []),
                            Pid = util:randomelem(KnownHosts),
                            EnvPid = comm:reply_as(comm:this(), 3, {cb_msg, {gossip_cyclon, default}, '_'}),
                            comm:send(Pid, {get_dht_nodes, EnvPid}, [{?quiet}])
                    end;
                _ ->
                    %% ?TRACE_DEBUG("get_node_details_response: ok", []),
                    ok
            end,
            {ok, {NewCache, Node}};
        false ->
            %% ?TRACE_DEBUG("get_node_details_response: false", []),
            {ok, State}
    end;

%% Response to get_dht_nodes message from service_per_vm. Contains a list of
%% registered dht nodes from service_per_vm. Initiated in
%% handle_msg({get_node_details_response, _NodeDetails} if the cache is empty.
%% Tries to get a cyclon cache from one of the received nodes if cache is
%% still empty.
%% This happens (i.a.?) when only one node is present. In this case the
%% get_node_details and the get_dht_nodes request are repeated every cycle
%% (TODO is this the intended behaviour?)
handle_msg({get_dht_nodes_response, Nodes}, {Cache, _Node}=State) ->
    ?TRACE_DEBUG("get_dht_nodes_response", []),
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
-spec round_has_converged(State::state()) -> {boolean(), state()}.
round_has_converged(State) ->
    {false, State}.


%% @doc Notifies the gossip_load module about changes. <br/>
%%      Changes can be new rounds, leadership changes or exchange failures. All
%%      of them are ignored, as cyclon doesn't use / implements this features.
-spec notify_change(_, _, State::state()) -> {ok, state()}.
notify_change(_, _, State) ->
    %% Possible to use key range changes for rm_check() / rm_send_changes() ???
    {ok, State}.


%% @doc Returns the best result. <br/>
%%      Called by the gossip module upon {get_values_best} messages.
-spec get_values_best(State::state()) -> {ok, state()}.
get_values_best(State) ->
    %% use to implement get_subset_rand() api functions??
    {ok, State}.


%% @doc Returns all results. <br/>
%%      Called by the gossip module upon {get_values_all} messages.
-spec get_values_all(State::state()) -> {ok, state()}.
get_values_all(State) ->
    {ok, State}.


%% @doc Returns a key-value list of debug infos for the Web Interface. <br/>
%%      Called by the gossip module upon {web_debug_info} messages.
-spec web_debug_info(state()) ->
    {KeyValueList::[{Key::string(), Value::any()},...], state()}.
web_debug_info(State) ->
    %% web_debug_info (msg)
    {[{"Key", "Value"}], State}.


%% @doc Shut down the gossip_cyclon module. <br/>
%%      Called by the gossip module upon stop_gossip_task(CBModule).
-spec shutdown(State::state()) -> {ok, shutdown}.
shutdown(_State) ->
    % nothing to do
    {ok, shutdown}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Miscellaneous
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec rm_check(Neighbors, Neighbors, Reason) -> boolean() when
      is_subtype(Neighbors, nodelist:neighborhood()),
      is_subtype(Reason, rm_loop:reason()).
rm_check(OldNeighbors, NewNeighbors, _Reason) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors).


%% @doc Sends changes to a subscribed cyclon process when the neighborhood
%%      changes.
-spec rm_send_changes(Pid::pid(), Tag::cyclon,
        OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(),
        Reason::rm_loop:reason()) -> ok.
rm_send_changes(Pid, cyclon, _OldNeighbors, NewNeighbors, _Reason) ->
    ?TRACE_DEBUG("rm_send_changes", []),
    comm:send_local(Pid, {cb_msg, {gossip_cyclon, default}, {rm_changed, nodelist:node(NewNeighbors)}}).


%% @doc Checks the current state. If the cache is empty or the current node is
%%      unknown, the local dht_node will be asked for these values and the check
%%      will be re-scheduled after 1s.
-spec check_state(state()) -> ok | fail.
check_state({Cache, _Node} = _State) ->
    % if the own node is unknown or the cache is empty (it should at least
    % contain the nodes predecessor and successor), request this information
    % from the local dht_node
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
    DHT_Node = pid_groups:get_my(dht_node),
    This = comm:this(),
    EnvPid = comm:reply_as(This, 3, {cb_msg, {gossip_cyclon, default}, '_'}),
    case comm:is_valid(This) of
        true ->
            comm:send_local(DHT_Node, {get_node_details, EnvPid, Details});
        false -> ok
    end.


%% @doc Print the cache in a dot compatible format.
%%      Format: Self -> Reference1; Self -> Reference2 ; ...
%%      Prints references to nodes as local pids, so this produces meaningful
%%      results if all nodes are started in the same Erlang VM.
%%      (Cycles are counted in the gossip module as well for real, the basic cycle
%%      counting performed here only works if this function only called once every cycle).
-compile({nowarn_unused_function, {print_cache_dot, 2}}).
-spec print_cache_dot(node:nodetype(), data()) -> ok.
print_cache_dot(MyNode, Cache) ->
    Cycle = case get(cycles) of
        undefined -> put(cycles, 1), 0;
        Cycle1 -> put(cycles, Cycle1+1), Cycle1
    end,
    MyPid = comm:make_local(node:pidX(MyNode)),
    Graph = lists:foldl(
                    fun({Node, _Age}, AccIn) ->
                        AccIn ++ io_lib:format("~w -> ~w; ", [MyPid, comm:make_local(node:pidX(Node))])
                    end, io_lib:format("[Cycle: ~w] ", [Cycle]), Cache),
    log:pal(lists:flatten(Graph)).

