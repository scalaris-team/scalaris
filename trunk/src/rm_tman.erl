%  @copyright 2009-2011 Zuse Institute Berlin

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

-behavior(rm_beh).

-export([get_base_interval/0, get_min_interval/0, get_max_interval/0]).

-opaque state() :: {Neighbors      :: nodelist:neighborhood(),
                    RandomViewSize :: pos_integer(),
                    Interval       :: base_interval | min_interval | max_interval,
                    TriggerState   :: trigger:state(),
                    Cache          :: [node:node_type()], % random cyclon nodes
                    Churn          :: boolean()}.

% accepted messages of an initialized rm_tman process in addition to rm_loop
-type(custom_message() ::
    {rm_trigger} |
    {{cy_cache, Cache::[node:node_type()]}, rm} |
    {{get_node_details_response, NodeDetails::node_details:node_details()}, rm} |
    {rm, buffer, OtherNeighbors::nodelist:neighborhood(), RequestPredsMinCount::non_neg_integer(), RequestSuccsMinCount::non_neg_integer()} |
    {rm, buffer_response, OtherNeighbors::nodelist:neighborhood()}).

% note include after the type definitions for erlang < R13B04!
-include("rm_beh.hrl").

get_neighbors({Neighbors, _RandViewSize, _Interval, _TriggerState, _Cache, _Churn}) ->
    Neighbors.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initialises the state when rm_loop receives an init_rm message.
init(Me, Pred, Succ) ->
    Trigger = config:read(ringmaintenance_trigger),
    TriggerState1 =
        trigger:init(
          Trigger, fun get_base_interval/0, fun get_min_interval/0,
          fun get_max_interval/0, rm_trigger),
    TriggerState2 = trigger:now(TriggerState1),
    Neighborhood = nodelist:new_neighborhood(Pred, Me, Succ),
    cyclon:get_subset_rand_next_interval(1, comm:self_with_cookie(rm)),
    {Neighborhood, config:read(cyclon_cache_size), min_interval, TriggerState2, [], true}.

unittest_create_state(Neighbors) ->
    Trigger = config:read(ringmaintenance_trigger),
    TriggerState1 =
        trigger:init(
          Trigger, fun get_base_interval/0, fun get_min_interval/0,
          fun get_max_interval/0, rm_trigger),
    {Neighbors, 1, min_interval, TriggerState1, [], true}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop (custom messages not already handled by rm_loop:on/2)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when the module is fully initialized.
on({rm_trigger},
   {Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn} = State) ->
    % Trigger an update of the Random view
    % Test for being alone:
    case nodelist:has_real_pred(Neighborhood) andalso
             nodelist:has_real_succ(Neighborhood) of
        false -> % our node is the only node in the system
            % no need to set a new trigger - we will be actively called by
            % any new node and set the trigger then (see handling of
            % notify_new_succ and notify_new_pred)
            State;
        _ -> % there is another node in the system
            RndView = get_RndView(RandViewSize, Cache),
            %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
            {Pred, Succ} = get_safe_pred_succ(Neighborhood, RndView),
            %io:format("~p~n",[{Preds,Succs,RndView,Me}]),
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
            Message = {rm, buffer, Neighborhood, RequestPredsMinCount, RequestSuccsMinCount},
            comm:send(node:pidX(Succ), Message),
            case Pred =/= Succ of
                true -> comm:send(node:pidX(Pred), Message);
                _    -> ok
            end,
            NewTriggerState = trigger:next(TriggerState, Interval),
            {Neighborhood, RandViewSize, base_interval, NewTriggerState, Cache, Churn}
    end;

% got empty cyclon cache
on({{cy_cache, []}, rm},
   {_Neighborhood, RandViewSize, _Interval, _TriggerState, _Cache, _Churn} = State)  ->
    % ignore empty cache from cyclon
    cyclon:get_subset_rand_next_interval(RandViewSize, comm:self_with_cookie(rm)),
    State;

% got cyclon cache
on({{cy_cache, NewCache}, rm},
   {Neighborhood, RandViewSize, Interval, TriggerState, _Cache, Churn}) ->
    % increase RandViewSize (no error detected):
    RandViewSizeNew =
        case (RandViewSize < config:read(cyclon_cache_size)) of
            true  -> RandViewSize + 1;
            false -> RandViewSize
        end,
    % trigger new cyclon cache request
    cyclon:get_subset_rand_next_interval(RandViewSizeNew, comm:self_with_cookie(rm)),
    MyRndView = get_RndView(RandViewSizeNew, NewCache),
    OtherNeighborhood =
        nodelist:mk_neighborhood(NewCache, nodelist:node(Neighborhood),
                                 get_pred_list_length(), get_succ_list_length()),
    NewNeighborhood = trigger_update(Neighborhood, MyRndView, OtherNeighborhood),
    {NewNeighborhood, RandViewSizeNew, Interval, TriggerState, NewCache, Churn};

% got shuffle request
on({rm, buffer, OtherNeighbors, RequestPredsMinCount, RequestSuccsMinCount},
   {Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}) ->
    MyRndView = get_RndView(RandViewSize, Cache),
    MyView = lists:append(MyRndView, nodelist:to_list(Neighborhood)),
    OtherNode = nodelist:node(OtherNeighbors),
    OtherNodeId = node:id(OtherNode),
    OtherLastPredId = node:id(lists:last(nodelist:preds(OtherNeighbors))),
    OtherLastSuccId = node:id(lists:last(nodelist:succs(OtherNeighbors))),
    % note: the buffer message, esp. OtherNode, might already be outdated
    % and our own view may contain a newer version of the node
    {[OtherNodeUpd], MyViewUpd} = nodelist:lupdate_ids([OtherNode], MyView),
    NeighborsToSendTmp = nodelist:mk_neighborhood(MyViewUpd, OtherNodeUpd,
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
    comm:send(node:pidX(nodelist:node(OtherNeighbors)),
              {rm, buffer_response, NeighborsToSend}),
    NewNeighborhood = trigger_update(Neighborhood, MyRndView, OtherNeighbors),
    {NewNeighborhood, RandViewSize, Interval, TriggerState, Cache, Churn};

on({rm, buffer_response, OtherNeighbors},
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
on({{get_node_details_response, NodeDetails}, rm}, State) ->
    update_nodes(State, [node_details:get(NodeDetails, node)], [], null);

on(_, _State) -> unknown_event.

new_pred(State, NewPred) ->
    % if we do not want to trust notify_new_pred messages to provide an alive node, use this instead:
%%     trigger_update(OldNeighborhood, [], nodelist:new_neighborhood(nodelist:node(OldNeighborhood), NewPred)),
    % we trust NewPred to be alive -> integrate node:
    update_nodes(State, [NewPred], [], null).

new_succ(State, NewSucc) ->
    % similar to new_pred/2
    update_nodes(State, [NewSucc], [], null).

remove_pred(State, OldPred, PredsPred) ->
    update_nodes(State, [PredsPred], [OldPred], null).

remove_succ(State, OldSucc, SuccsSucc) ->
    update_nodes(State, [SuccsSucc], [OldSucc], null).

update_node({Neighborhood, RandViewSize, Interval, TriggerState, Cache, Churn}, NewMe) ->
    NewNeighborhood = nodelist:update_node(Neighborhood, NewMe),
    NewTriggerState = trigger:now(TriggerState), % inform neighbors
    {NewNeighborhood, RandViewSize, Interval, NewTriggerState, Cache, Churn}.

leave(_State) -> ok.

% failure detector reported dead node
crashed_node(State, DeadPid) ->
    update_nodes(State, [], [DeadPid], fun dn_cache:add_zombie_candidate/1).

% dead-node-cache reported dead node to be alive again
zombie_node(State, Node) ->
    % this node could potentially be useful as it has been in our state before
    update_nodes(State, [Node], [], null).

get_web_debug_info(_State) -> [].

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_module(ringmaintenance_trigger) and

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
    % update node ids with information from the other node's neighborhood
    OldNeighborhood2 =
        nodelist:update_ids(OldNeighborhood,
                            nodelist:to_list(OtherNeighborhood)),
    PredL = get_pred_list_length(),
    SuccL = get_succ_list_length(),
    NewNeighborhood1 = 
        nodelist:add_nodes(OldNeighborhood2, MyRndView, PredL, SuccL),
    NewNeighborhood2 =
        nodelist:merge(NewNeighborhood1, OtherNeighborhood, PredL, SuccL),
    
    OldView = nodelist:to_list(OldNeighborhood2),
    NewView = nodelist:to_list(NewNeighborhood2),
    ViewOrd = fun(A, B) ->
                      nodelist:succ_ord_node(A, B, nodelist:node(OldNeighborhood2))
              end,
    {_, _, NewNodes} = util:ssplit_unique(OldView, NewView, ViewOrd),
    
    % TODO: add a local cache of contacted nodes in order not to contact them again
    ThisWithCookie = comm:this_with_cookie(rm),
    case comm:is_valid(ThisWithCookie) of
        true ->
            _ = [comm:send(node:pidX(Node), {get_node_details, ThisWithCookie, [node]})
                 || Node <- NewNodes];
        false -> ok
    end,
    OldNeighborhood2.

%% @doc Adds and removes the given nodes from the rm_tman state.
%%      Note: Sets the new RandViewSize to 0 if NodesToRemove is not empty and
%%      the new neighborhood is different to the old one. If either churn
%%      occurred or was already determined, min_interval if chosen for the next
%%      interval, otherwise max_interval. If the successor or predecessor
%%      changes, the trigger will be called immediately.
-spec update_nodes(State::state(),
                   NodesToAdd::[node:node_type()],
                   NodesToRemove::[node:node_type() | comm:mypid() | pid()],
                   RemoveNodeEvalFun::fun((node:node_type()) -> any()) | null)
        -> NewState::state().
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
