%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc    T-Man ring maintenance
%% @end
%% @reference Mark Jelasity, Ozalp Babaoglu. T-Man: Gossip-Based Overlay
%% Topology Management. Engineering Self-Organising Systems 2005:1-15
%% @version $Id$
-module(rm_tman).
-author('hennig@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behavior(gen_component).
-behavior(rm_beh).

-export([start_link/1]).
-export([init/1, on/2, leave/0]).

-export([get_base_interval/0, get_min_interval/0, get_max_interval/0,
         check_config/0]).

-type(state_init() :: {Neighborhood   :: nodelist:neighborhood(),
                       RandomViewSize :: pos_integer(),
                       Interval       :: base_interval | min_interval | max_interval,
                       TriggerState   :: trigger:state(),
                       Cache          :: [node:node_type()], % random cyclon nodes
                       Churn          :: boolean()}).
-type(state_uninit() :: {uninit, QueuedMessages::msg_queue:msg_queue(),
                         TriggerState :: trigger:state()}).
-type(state() :: state_init() | state_uninit()).

% accepted messages
-type(message() ::
    {init_rm, Me::node:node_type(), Predecessor::node:node_type(), Successor::node:node_type()} |
    {trigger} |
    {cy_cache, Cache::[node:node_type()]} |
    {rm_buffer, OtherNeighbors::nodelist:neighborhood(), RequestPredsMinCount::non_neg_integer(), RequestSuccsMinCount::non_neg_integer()} |
    {rm_buffer_response, OtherNeighbors::nodelist:neighborhood()} |
    {get_node_details_response, NodeDetails::node_details:node_details()} |
    {zombie, Node::node:node_type()} |
    {crash, DeadPid::comm:mypid()} |
    {check_ring, Token::non_neg_integer(), Master::node:node_type()} |
    {init_check_ring, Token::non_neg_integer()} |
    {notify_new_pred, NewPred::node:node_type()} |
    {notify_new_succ, NewSucc::node:node_type()} |
    {leave, SourcePid::comm:erl_local_pid()} |
    {pred_left, OldPred::node:node_type(), PredsPred::node:node_type()} |
    {succ_left, OldSucc::node:node_type(), SuccsSucc::node:node_type()} |
    {update_id, NewId::?RT:key()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a chord-like ring maintenance process, registers it with the
%%      process dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = config:read(ringmaintenance_trigger),
    gen_component:start_link(?MODULE, Trigger, [{pid_groups_join_as, DHTNodeGroup, ring_maintenance}]).

%% @doc Initialises the module with an uninitialized state.
-spec init(module()) -> {uninit, QueuedMessages::msg_queue:msg_queue(), TriggerState::trigger:state()}.
init(Trigger) ->
    TriggerState = trigger:init(Trigger, ?MODULE),
    comm:send_local(get_pid_dnc(), {subscribe, self()}),
    comm:send_local(get_cs_pid(), {init_rm, self()}),
    {uninit, msg_queue:new(), TriggerState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc message handler
-spec on(message(), state()) -> state().
on({init_rm, Me, Predecessor, Successor}, {uninit, QueuedMessages, TriggerState}) ->
    Neighborhood = nodelist:new_neighborhood(Predecessor, Me, Successor),
    NewTriggerState = trigger:now(TriggerState),
    % make Pred++Succ unique and subscribe to fd:
    fd:subscribe(lists:usort([node:pidX(Predecessor), node:pidX(Successor)])),
    cyclon:get_subset_rand_next_interval(1),
    msg_queue:send(QueuedMessages),
    {Neighborhood, config:read(cyclon_cache_size), min_interval,
     NewTriggerState, [], true};

on(Msg, {uninit, QueuedMessages, TriggerState}) ->
    {uninit, msg_queue:add(QueuedMessages, Msg), TriggerState};

on({trigger},
   {Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn} = State) ->
    % Trigger an update of the Random view
    %
    RndView = get_RndView(RandViewSize, Cache),
    %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
    {Pred, Succ} = get_safe_pred_succ(Neighborhood, RndView),
    %io:format("~p~n",[{Preds,Succs,RndView,Me}]),
    % Test for being alone:
    case nodelist:has_real_pred(Neighborhood) andalso
             nodelist:has_real_succ(Neighborhood) of
        false -> % our node is the only node in the system
            %TODO: do we need to tell the DHT node here? doesn't it already know?
            rm_beh:update_dht_node(Neighborhood),
            % no need to set a new trigger - we will be actively called by
            % any new node and set the trigger then (see handling of
            % notify_new_succ and notify_new_pred)
            State;
        _ -> % there is another node in the system
            RequestPredsMinCount =
                case nodelist:has_real_pred(Neighborhood) of
                    true -> get_pred_list_length() - length(nodelist:preds(Neighborhood));
                    _    -> get_pred_list_length()
                end,
            RequestSuccsMinCount =
                case nodelist:has_real_succ(Neighborhood) of
                    true -> get_succ_list_length() - length(nodelist:succs(Neighborhood));
                    _    -> get_succ_list_length()
                end,
            % send succ and pred our known nodes and request their nodes
            Message = {rm_buffer, Neighborhood, RequestPredsMinCount, RequestSuccsMinCount},
            comm:send_to_group_member(node:pidX(Succ), ring_maintenance, Message),
            case Pred =/= Succ of
                true ->
                    comm:send_to_group_member(node:pidX(Pred), ring_maintenance, Message);
                _ -> ok
            end,
            NewTriggerState = trigger:next(TriggerState, Interval),
            {Neighborhood, RandViewSize, base_interval, NewTriggerState, Cache, Churn}
    end;

% got empty cyclon cache
on({cy_cache, []},
   {_Neighborhood, RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State)  ->
    % ignore empty cache from cyclon
    cyclon:get_subset_rand_next_interval(RandViewSize),
    State;

% got cyclon cache
on({cy_cache, NewCache},
   {Neighborhood, RandViewSize, Interval, TriggerState, _Cache, Churn}) ->
    % increase RandViewSize (no error detected):
    RandViewSizeNew =
        case (RandViewSize < config:read(cyclon_cache_size)) of
            true  -> RandViewSize + 1;
            false -> RandViewSize
        end,
    % trigger new cyclon cache request
    cyclon:get_subset_rand_next_interval(RandViewSizeNew),
    MyRndView = get_RndView(RandViewSizeNew, NewCache),
    NewNeighborhood =
        trigger_update(Neighborhood, MyRndView,
                       nodelist:mk_neighborhood(NewCache, nodelist:node(Neighborhood),
                                                get_pred_list_length(),
                                                get_succ_list_length())),
    {NewNeighborhood, RandViewSizeNew, Interval, TriggerState, NewCache, Churn};

% got shuffle request
on({rm_buffer, OtherNeighbors, RequestPredsMinCount, RequestSuccsMinCount},
   {Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    MyRndView = get_RndView(RandViewSize, Cache),
    MyView = lists:append(MyRndView, nodelist:to_list(Neighborhood)),
    OtherNode = nodelist:node(OtherNeighbors),
    OtherNodeId = node:id(OtherNode),
    OtherLastPredId = node:id(lists:last(nodelist:preds(OtherNeighbors))),
    OtherLastSuccId = node:id(lists:last(nodelist:succs(OtherNeighbors))),
    NeighborsToSendTmp = nodelist:mk_neighborhood(MyView, OtherNode,
                                                  get_pred_list_length(),
                                                  get_succ_list_length()),
    NeighborsToSend =
        nodelist:filter_min_length(
          NeighborsToSendTmp,
          fun(N) ->
                  intervals:in(node:id(N), intervals:new('(', OtherNodeId, OtherLastSuccId, ')')) orelse
                      intervals:in(node:id(N), intervals:new('(', OtherLastPredId, OtherNodeId, ')'))
          end,
          RequestPredsMinCount, RequestSuccsMinCount),
    comm:send_to_group_member(node:pidX(nodelist:node(OtherNeighbors)),
                              ring_maintenance,
                              {rm_buffer_response, NeighborsToSend}),
    NewNeighborhood = trigger_update(Neighborhood, MyRndView, OtherNeighbors),
    {NewNeighborhood, RandViewSize, Interval, TriggerState, Cache, Churn};

on({rm_buffer_response, OtherNeighbors},
   {Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    MyRndView = get_RndView(RandViewSize, Cache),
    NewNeighborhood = trigger_update(Neighborhood, MyRndView, OtherNeighbors),
    % increase RandViewSize (no error detected):
    NewRandViewSize =
        case RandViewSize < config:read(cyclon_cache_size) of
            true ->  RandViewSize + 1;
            false -> RandViewSize
        end,
    {NewNeighborhood, NewRandViewSize, Interval, TriggerState, Cache, Churn};

% we asked another node we wanted to add for its node object -> now add it
on({get_node_details_response, NodeDetails}, State) ->
    update_nodes(State, [node_details:get(NodeDetails, node)], [], null);

% dead-node-cache reported dead node to be alive again
on({zombie, Node}, State) ->
    % this node could potentially be useful as it has been in our state before
    update_nodes(State, [Node], [], null);

% failure detector reported dead node
on({crash, DeadPid}, State) ->
    update_nodes(State, [], [DeadPid], fun dn_cache:add_zombie_candidate/1);

% trigger by admin:dd_check_ring
on({check_ring, Token, Master},
   {Neighborhood, _RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State) ->
    Me = nodelist:node(Neighborhood),
    case {Token, Master} of
        {0, Me} ->
            io:format(" [RM ] CheckRing   OK  ~n");
        {0, _} ->
            io:format(" [RM ] CheckRing  reach TTL in Node ~p not in ~p~n", [Master, Me]);
        {Token, Me} ->
            io:format(" [RM ] Token back with Value: ~p~n",[Token]);
        {Token, _} ->
            {Pred, _Succ} = get_safe_pred_succ(Neighborhood, []),
            comm:send_to_group_member(node:pidX(Pred), ring_maintenance,
                                      {check_ring, Token - 1, Master})
    end,
    State;

% trigger by admin:dd_check_ring
on({init_check_ring, Token},
   {Neighborhood, _RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State) ->
    Me = nodelist:node(Neighborhood),
    {Pred, _Succ} = get_safe_pred_succ(Neighborhood, []),
    comm:send_to_group_member(node:pidX(Pred), ring_maintenance,
                              {check_ring, Token - 1, Me}),
    State;

on({notify_new_pred, NewPred}, State) ->
    % if we do not want to trust notify_new_pred messages to provide an alive node, use this instead:
%%     trigger_update(OldNeighborhood, [], nodelist:new_neighborhood(nodelist:node(OldNeighborhood), NewPred)),
    % we trust NewPred to be alive -> integrate node:
    update_nodes(State, [NewPred], [], null);

on({notify_new_succ, NewSucc}, State) ->
    % same as notify_new_pred
    update_nodes(State, [NewSucc], [], null);

on({leave, SourcePid},
   {Neighborhood, _RandViewSize, _Interval, TriggerState, _Cache, _Churn}) ->
    Me = nodelist:node(Neighborhood),
    Pred = nodelist:pred(Neighborhood),
    Succ = nodelist:succ(Neighborhood),
    comm:send_to_group_member(node:pidX(Succ), ring_maintenance, {pred_left, Me, Pred}),
    comm:send_to_group_member(node:pidX(Pred), ring_maintenance, {succ_left, Me, Succ}),
    comm:send_local(SourcePid, {leave_response}),
    {uninit, msg_queue:new(), TriggerState};

on({pred_left, OldPred, PredsPred}, State) ->
    update_nodes(State, [PredsPred], [OldPred], null);

on({succ_left, OldSucc, SuccsSucc}, State) ->
    update_nodes(State, [SuccsSucc], [OldSucc], null);

on({update_id, NewId},
   {Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    NewMe = node:update_id(nodelist:node(Neighborhood), NewId),
    NewNeighborhood = nodelist:update_node(Neighborhood, NewMe),
    rm_beh:update_dht_node(Neighborhood, NewNeighborhood),
    NewTriggerState = trigger:now(TriggerState), % inform neighbors
    {NewNeighborhood, RandViewSize, Interval, NewTriggerState, Cache, Churn};

on({web_debug_info, Requestor},
   {Neighborhood, _RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State) ->
    comm:send_local(Requestor,
                    {web_debug_info_reply,
                     [{"algorithm", lists:flatten(io_lib:format("~p", [?MODULE]))},
                      {"self", lists:flatten(io_lib:format("~p", [nodelist:node(Neighborhood)]))},
                      {"preds", lists:flatten(io_lib:format("~p", [nodelist:preds(Neighborhood)]))},
                      {"succs", lists:flatten(io_lib:format("~p", [nodelist:succs(Neighborhood)]))}]}),
    State.

%% @doc Notifies the successor and predecessor that the current dht_node is
%%      going to leave / left. Will reset the ring_maintenance state to uninit
%%      and respond with a {leave_response} message.
%%      Note: only call this method from inside the dht_node process!
-spec leave() -> ok.
leave() ->
    comm:send_local(pid_groups:get_my(ring_maintenance),
                    {leave, self()}).

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_atom(ringmaintenance_trigger) and

    config:is_integer(stabilization_interval_base) and
    config:is_greater_than(stabilization_interval_base, 0) and

    config:is_integer(stabilization_interval_min) and
    config:is_greater_than(stabilization_interval_min, 0) and
    config:is_greater_than_equal(stabilization_interval_base, stabilization_interval_min) and

    config:is_integer(stabilization_interval_max) and
    config:is_greater_than(stabilization_interval_max, 0) and
    config:is_greater_than_equal(stabilization_interval_max, stabilization_interval_min) and
    config:is_greater_than_equal(stabilization_interval_max, stabilization_interval_base) and

    config:is_integer(cyclon_cache_size) and
    config:is_greater_than(cyclon_cache_size, 2) and

    config:is_integer(succ_list_length) and
    config:is_greater_than_equal(succ_list_length, 1) and

    config:is_integer(pred_list_length) and
    config:is_greater_than_equal(pred_list_length, 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Get N peers from the cyclon cache.
-spec get_RndView(integer(), [node:node_type()]) -> [node:node_type()].
get_RndView(N, Cache) ->
    lists:sublist(Cache, N).

%% @doc Gets the node's current successor and predecessor in a safe way.
%%      If either is unknown, the random view is used to get a replacement. If
%%      this doesn't help either, the own node is returned as this is the
%%      current node's view.
-spec get_safe_pred_succ(
        Neighborhood::nodelist:neighborhood(), RndView::[node:node_type()]) ->
              {Pred::node:node_type(), Succ::node:node_type()}.
get_safe_pred_succ(Neighborhood, RndView) ->
    case nodelist:has_real_pred(Neighborhood) andalso
             nodelist:has_real_succ(Neighborhood) of
        true -> {nodelist:pred(Neighborhood), nodelist:succ(Neighborhood)};
        _    -> NewNeighbors = nodelist:add_nodes(Neighborhood, RndView, 1, 1),
                {nodelist:pred(NewNeighbors), nodelist:succ(NewNeighbors)}
    end.

% @doc is there churn in the system
-spec has_churn(OldNeighborhood::nodelist:neighborhood(),
                NewNeighborhood::nodelist:neighborhood()) -> boolean().
has_churn(OldNeighborhood, NewNeighborhood) ->
    OldNeighborhood =/= NewNeighborhood.

%% @doc Triggers the integration of new nodes from OtherNeighborhood and
%%      RndView into our Neighborhood by contacting every useful node.
-spec trigger_update(OldNeighborhood::nodelist:neighborhood(),
                     RndView::[node:node_type()],
                     OtherNeighborhood::nodelist:neighborhood())
        -> NewNeighborhood::nodelist:neighborhood().
trigger_update(OldNeighborhood, MyRndView, OtherNeighborhood) ->
    NewNeighborhood1 = nodelist:add_nodes(OldNeighborhood, MyRndView, get_pred_list_length(), get_succ_list_length()),
    NewNeighborhood2 = nodelist:merge(NewNeighborhood1, OtherNeighborhood, get_pred_list_length(), get_succ_list_length()),
    
    OldView = nodelist:to_list(OldNeighborhood),
    NewView = nodelist:to_list(NewNeighborhood2),
    ViewOrd = fun(A, B) ->
                      nodelist:succ_ord_node(A, B, nodelist:node(OldNeighborhood))
              end,
    {_, _, NewNodes} = util:ssplit_unique(OldView, NewView, ViewOrd),
    
    % TODO: add a local cache of contacted nodes in order not to contact them again
    [comm:send(node:pidX(Node), {get_node_details, comm:this(), [node]})
        || Node <- NewNodes],
    % update node ids with information from the other node's neighborhood
    nodelist:update_ids(OldNeighborhood, nodelist:to_list(OtherNeighborhood)).

%% @doc Adds and removes the given nodes from the rm_tman state.
%%      Note: Sets the new RandViewSize to 0 if NodesToRemove is not empty and
%%      the new neighborhood is different to the old one. If either churn
%%      occurred or was already determined, min_interval if chosen for the next
%%      interval, otherwise max_interval. If the successor or predecessor
%%      changes, the trigger will be called immediately.
-spec update_nodes(State::state_init(), NodesToAdd::[node:node_type()],
                   NodesToRemove::[node:node_type() | comm:mypid() | pid()],
                   RemoveNodeEvalFun::fun((node:node_type()) -> any()) | null)
        -> NewState::state_init().
update_nodes(State, [], [], _RemoveNodeEvalFun) ->
    State;
update_nodes({OldNeighborhood, RandViewSize, _Interval, TriggerState, OldCache, Churn},
             NodesToAdd, NodesToRemove, RemoveNodeEvalFun) ->
    % keep all nodes that are not in NodesToRemove - note: NodesToRemove should
    % have 0 or 1 element - so lists:member/2 is not expensive
    FilterFun = fun(N) -> not lists:any(fun(B) -> node:same_process(N, B) end, NodesToRemove) end,
    case is_function(RemoveNodeEvalFun) of
        true ->
            Nbh1 = nodelist:filter(OldNeighborhood, FilterFun, RemoveNodeEvalFun),
            NewCache = nodelist:lfilter(OldCache, FilterFun, RemoveNodeEvalFun);
        _ ->
            Nbh1 = nodelist:filter(OldNeighborhood, FilterFun),
            NewCache = nodelist:lfilter(OldCache, FilterFun)
    end,
    
    NewNeighborhood = nodelist:add_nodes(Nbh1, NodesToAdd,
                                         get_pred_list_length(),
                                         get_succ_list_length()),
    
    rm_beh:update_dht_node(OldNeighborhood, NewNeighborhood),
    rm_beh:update_failuredetector(OldNeighborhood, NewNeighborhood),
    NewChurn = has_churn(OldNeighborhood, NewNeighborhood),
    NewInterval = case Churn orelse NewChurn of
                      true -> min_interval; % increase ring maintenance frequency
                      _    -> max_interval
                  end,
    NewTriggerState =
        case nodelist:pred(OldNeighborhood) =/= nodelist:pred(NewNeighborhood) orelse
                 nodelist:succ(OldNeighborhood) =/= nodelist:succ(NewNeighborhood) of
            true -> trigger:now(TriggerState);
            _    -> TriggerState
        end,
    NewRandViewSize = case NewChurn andalso NodesToRemove =/= [] of
                          true -> 0;
                          _    -> RandViewSize
                      end,
    {NewNeighborhood, NewRandViewSize, NewInterval, NewTriggerState, NewCache, NewChurn}.

-spec get_pid_dnc() -> pid() | failed.
get_pid_dnc() -> pid_groups:get_my(dn_cache).

% get Pid of assigned dht_node
-spec get_cs_pid() -> pid() | failed.
get_cs_pid() -> pid_groups:get_my(dht_node).

-spec get_base_interval() -> pos_integer().
get_base_interval() -> config:read(stabilization_interval_base).

-spec get_min_interval() -> pos_integer().
get_min_interval() -> config:read(stabilization_interval_min).

-spec get_max_interval() -> pos_integer().
get_max_interval() -> config:read(stabilization_interval_max).

-spec get_pred_list_length() -> pos_integer().
get_pred_list_length() -> config:read(pred_list_length).

-spec get_succ_list_length() -> pos_integer().
get_succ_list_length() -> config:read(succ_list_length).
