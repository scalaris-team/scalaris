%  @copyright 2009-2017 Zuse Institute Berlin

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

-behaviour(rm_beh).

-opaque state() :: rm_tman_state:state().

-define(FIXMODRN(NEIGHBORS), case modr:is_enabled() of true -> modr:fix_neighborhood(NEIGHBORS); false -> NEIGHBORS end).
-define(FIXMODRS(STATE), case modr:is_enabled() of true -> modr:fix_state(STATE); false -> STATE end).

% accepted messages of an initialized rm_tman process in addition to rm_loop
-type(custom_message() ::
    {rm, once, {cy_cache, Cache::[node:node_type()]}} |
    {rm, {cy_cache, Cache::[node:node_type()]}} |
    {rm, node_info_response, NodeDetails::node_details:node_details()} |
    {rm, buffer, OtherNeighbors::nodelist:neighborhood(), RequestPredsMinCount::non_neg_integer(), RequestSuccsMinCount::non_neg_integer()} |
    {rm, buffer_response, OtherNodes::nodelist:non_empty_snodelist()}).

-define(SEND_OPTIONS, [{channel, prio}, {?quiet}]).

% note include after the type definitions for erlang < R13B04!
-include("rm_beh.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initializes the cyclon cache retrieval "trigger" (un-infected).
-spec init_first() -> ok.
init_first() ->
    gossip_cyclon:get_subset_rand(1, comm:reply_as(self(), 2, {rm, '_'}),
                                  config:read(tman_cyclon_interval)),
    ok.

%% @doc Initialises the state when rm_loop receives an init_rm message.
-spec init(Me::node:node_type(), Pred::node:node_type(),
           Succ::node:node_type()) -> rm_tman_state:state().
init(Me, Pred, Succ) ->
    Neighborhood = nodelist:new_neighborhood(Pred, Me, Succ),
    % ask cyclon once (a repeating trigger is already started in init_first/0)
    gossip_cyclon:get_subset_rand(1, comm:reply_as(self(), 3, {rm, once, '_'})),
    % start by using all available nodes reported by cyclon
    RandViewSize = config:read(gossip_cyclon_cache_size),
    rm_tman_state:init(Neighborhood, RandViewSize, [], true).

-spec unittest_create_state(Neighbors::nodelist:neighborhood()) -> rm_tman_state:state().
unittest_create_state(Neighbors) ->
    rm_tman_state:init(Neighbors, 1, [], true).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop (custom messages not already handled by rm_loop:on/2)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when the module is fully initialized.
-spec handle_custom_message(custom_message(), rm_tman_state:state())
        -> {ChangeReason::{unknown} | {node_discovery}, rm_tman_state:state()} | unknown_event.
% got empty cyclon cache
handle_custom_message({rm, once, {cy_cache, [] = NewCache}}, State) ->
    % loop with msg_delay until a non-empty cache is received
    gossip_cyclon:get_subset_rand(1, comm:reply_as(self(), 3, {rm, once, '_'}), 0),
    add_cyclon_cache(NewCache, State);
handle_custom_message({rm, once, {cy_cache, NewCache}}, State) ->
    add_cyclon_cache(NewCache, State);

% got cyclon cache (as part of a repeating call)
handle_custom_message({rm, {cy_cache, NewCache}}, State) ->
    {ChangeReason, NewState} = add_cyclon_cache(NewCache, State),
    NewRandViewSize = rm_tman_state:get_randview_size(NewState),
    % trigger new cyclon cache request
    gossip_cyclon:get_subset_rand(NewRandViewSize, comm:reply_as(self(), 2, {rm, '_'}),
                                  config:read(tman_cyclon_interval)),
    {ChangeReason, NewState};

% got shuffle request
handle_custom_message({rm, buffer, OtherNeighbors, RequestPredsMinCount, RequestSuccsMinCount},
                      State) ->
    Cache = rm_tman_state:get_cache(State),
    RandViewSize = rm_tman_state:get_randview_size(State),
    Neighborhood = rm_tman_state:get_neighbors(State),
    OtherNodes = nodelist:to_list(OtherNeighbors),
    CacheUpd = element(1, nodelist:lupdate_ids(Cache, OtherNodes)),
    % use only a subset of the cyclon cache in order not to fill the send buffer
    % with potentially non-existing nodes (in contrast, the nodes from our
    % neighbourhood are more likely to still exist!)
    MyRndView = get_RndView(RandViewSize, CacheUpd),
    % note: we can not use the result from the util:split_unique/2 call below
    %       since we also need to (re-)integrate nodes which are in our cyclon
    %       cache but came inside the other nodes's neighbourhood
    %       -> instead integrate the whole other view and use the updated own
    %          neighbourhood
    NewNeighborhood = trigger_update(Neighborhood, MyRndView, OtherNodes),

    MyNodes = nodelist:to_list(NewNeighborhood),
    % do not send nodes already known to the other node:
    {MyViewUpd, _, _} = util:split_unique(
                          lists:append(MyRndView, MyNodes), OtherNodes),
    OtherNode = nodelist:node(OtherNeighbors),
    NeighborsToSendTmp = nodelist:mk_neighborhood(MyViewUpd, OtherNode,
                                                  get_pred_list_length(),
                                                  get_succ_list_length()),
    % only send nodes in between the range of the other node's neighborhood
    % but at least a given number
    OtherNodeUpdId = node:id(OtherNode),
    OtherLastPredId = node:id(lists:last(nodelist:preds(OtherNeighbors))),
    OtherLastSuccId = node:id(lists:last(nodelist:succs(OtherNeighbors))),
    OtherRange = intervals:union(
                   intervals:new('(', OtherNodeUpdId, OtherLastSuccId, ')'),
                   intervals:new('(', OtherLastPredId, OtherNodeUpdId, ')')),
    NeighborsToSend =
        tl(nodelist:to_list(
             nodelist:filter_min_length(
               NeighborsToSendTmp,
               fun(N) -> intervals:in(node:id(N), OtherRange) end,
               RequestPredsMinCount, RequestSuccsMinCount))),
    comm:send(node:pidX(OtherNode),
              {rm, buffer_response, NeighborsToSend}, ?SEND_OPTIONS),
    {{node_discovery}, rm_tman_state:set_neighbors(?FIXMODRN(NewNeighborhood),
                       rm_tman_state:set_cache(CacheUpd, State))};

handle_custom_message({rm, buffer_response, OtherNodes},
                      State) ->
    % similar to "{rm, buffer,...}" handling above:
    Cache = rm_tman_state:get_cache(State),
    RandViewSize = rm_tman_state:get_randview_size(State),
    Neighborhood = rm_tman_state:get_neighbors(State),
    CacheUpd = element(1, nodelist:lupdate_ids(Cache, OtherNodes)),
    MyRndView = get_RndView(RandViewSize, CacheUpd),
    NewNeighborhood = trigger_update(Neighborhood, MyRndView, OtherNodes),

    % increase RandViewSize (no error detected):
    NewRandViewSize =
        case RandViewSize < config:read(gossip_cyclon_cache_size) of
            true ->  RandViewSize + 1;
            false -> RandViewSize
        end,
    {{node_discovery}, rm_tman_state:set_neighbors(?FIXMODRN(NewNeighborhood),
                       rm_tman_state:set_randview_size(NewRandViewSize,
                       rm_tman_state:set_cache(CacheUpd, State)))};

% we asked another node we wanted to add for its node object -> now add it
% (if it is not in the process of leaving the system)
handle_custom_message({rm, node_info_response, NodeDetails}, State) ->
    case node_details:get(NodeDetails, is_leaving) of
        false ->
            Node = node_details:get(NodeDetails, node),
            NewState = update_nodes(State, [Node], [], null),
            {{node_discovery}, NewState};
        true ->
            {{unknown}, ?FIXMODRS(State)}
    end;

handle_custom_message({rm, update_node, Node}, State) ->
    Neighborhood = rm_tman_state:get_neighbors(State),
    NewNeighborhood1 = nodelist:update_ids(Neighborhood, [Node]),
    % message from pred or succ
    % -> update any out-dated nodes between old and new ID of the given node to
    %    prevent wrong pred/succ changed:
    NodePid = node:pidX(Node),
    OldPred = nodelist:pred(Neighborhood),
    OldPredPid = node:pidX(OldPred),
    I = case OldPredPid =:= NodePid andalso
                 OldPredPid =/= node:pidX(nodelist:pred(NewNeighborhood1)) of
            true ->
                MyNodeId = nodelist:nodeid(NewNeighborhood1),
                intervals:new('(', node:id(Node), MyNodeId, ')');
            _ ->
                OldSucc = nodelist:succ(Neighborhood),
                OldSuccPid = node:pidX(OldSucc),
                case OldSuccPid =:= NodePid andalso
                         OldSuccPid =/= node:pidX(nodelist:succ(NewNeighborhood1)) of
                    true ->
                        MyNodeId = nodelist:nodeid(NewNeighborhood1),
                        intervals:new('(', MyNodeId, node:id(Node), ')');
                    _ ->
                        intervals:empty()
                end
        end,
    % now remove all potentially out-dated nodes and try to re-add them with
    % updated information
    NewNeighborhood2 = remove_neighbors_in_interval(NewNeighborhood1, I, null),
    {{unknown}, rm_tman_state:set_neighbors(?FIXMODRN(NewNeighborhood2), State)};

handle_custom_message(_, _State) -> unknown_event.

%% @doc Integrates a non-empty cyclon cache into the own random view and
%%      neighborhood structures and updates the random view size accordingly.
%%      Ignores empty cyclon caches.
-spec add_cyclon_cache(Cache::[node:node_type()], rm_tman_state:state())
        -> {ChangeReason::{unknown} | {node_discovery}, rm_tman_state:state()}.
add_cyclon_cache([], State) ->
    % ignore empty cache from cyclon
    {{unknown}, State};
add_cyclon_cache(NewCache, State) ->
    % increase RandViewSize (no error detected):
    RandViewSize = rm_tman_state:get_randview_size(State),
    Neighborhood = rm_tman_state:get_neighbors(State),
    RandViewSizeNew =
        case (RandViewSize < config:read(gossip_cyclon_cache_size)) of
            true  -> RandViewSize + 1;
            false -> RandViewSize
        end,
    NewNeighborhood = trigger_update(Neighborhood, [], NewCache),
    {{node_discovery}, rm_tman_state:set_neighbors(?FIXMODRN(NewNeighborhood),
                       rm_tman_state:set_randview_size(RandViewSizeNew,
                       rm_tman_state:set_cache(NewCache, State)))}.

-spec trigger_action(State::rm_tman_state:state())
        -> {ChangeReason::rm_loop:reason(), rm_tman_state:state()}.
trigger_action(State) ->
    % Trigger an update of the Random view
    % use only a subset of the cyclon cache in order not to fill the send buffer
    % with potentially non-existing nodes (in contrast, the nodes from our
    % neighbourhood are more likely to still exist!)
    % Test for being alone:
    Neighborhood = rm_tman_state:get_neighbors(State),
    RandViewSize = rm_tman_state:get_randview_size(State),
    Cache = rm_tman_state:get_cache(State),
    Me = nodelist:node(Neighborhood),
    RndView = get_RndView(RandViewSize, Cache),
    {Pred, Succ} = get_safe_pred_succ(Neighborhood, RndView),
    case node:same_process(Pred, Me) andalso node:same_process(Succ, Me) of
        false -> % there is another node in the system
            %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
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
            comm:send(node:pidX(Succ), Message, ?SEND_OPTIONS),
            case Pred =/= Succ of
                true -> comm:send(node:pidX(Pred), Message, ?SEND_OPTIONS);
                _    -> ok
            end;
        _ -> % our node is the only node in the system
            % nothing to do here - we will be actively called by any new node
            % (see new_succ/2 and new_pred/2)
            ok
    end,
    {{unknown}, State}.

-spec new_pred(State::rm_tman_state:state(), NewPred::node:node_type()) ->
          {ChangeReason::rm_loop:reason(), rm_tman_state:state()}.
new_pred(State, NewPred) ->
    % if we do not want to trust notify_new_pred messages to provide an alive node, use this instead:
%%     trigger_update(OldNeighborhood, [], [NewPred]),
    % we trust NewPred to be alive -> integrate node:
    {{node_discovery}, update_nodes(State, [NewPred], [], null)}.

-spec new_succ(State::rm_tman_state:state(), NewSucc::node:node_type())
        -> {ChangeReason::rm_loop:reason(), rm_tman_state:state()}.
new_succ(State, NewSucc) ->
    % similar to new_pred/2
    {{node_discovery}, update_nodes(State, [NewSucc], [], null)}.

-spec update_node(State::rm_tman_state:state(), NewMe::node:node_type())
        -> {ChangeReason::rm_loop:reason(), rm_tman_state:state()}.
update_node(State, NewMe) ->
    Cache = rm_tman_state:get_cache(State),
    Neighborhood = rm_tman_state:get_neighbors(State),
    RandViewSize = rm_tman_state:get_randview_size(State),
    NewNeighborhood1 = nodelist:update_node(Neighborhood, NewMe),
    % -> update any out-dated nodes between old and new ID to prevent wrong
    %    pred/succ changed:
    OldId = nodelist:nodeid(Neighborhood),
    NewId = node:id(NewMe),
    I = case intervals:in(NewId, nodelist:node_range(Neighborhood)) of
            true  -> intervals:new('[', NewId, OldId, ')');
            false -> ?DBG_ASSERT(intervals:in(node:id(NewMe), nodelist:succ_range(Neighborhood))),
                     intervals:new('(', OldId, NewId, ']')
        end,
    NewNeighborhood2 = remove_neighbors_in_interval(NewNeighborhood1, I, null),

    ?DBG_ASSERT2(node:pidX(nodelist:pred(Neighborhood)) =:= node:pidX(nodelist:pred(NewNeighborhood2)),
                 no_pred_change_allowed),
    ?DBG_ASSERT2(node:pidX(nodelist:succ(Neighborhood)) =:= node:pidX(nodelist:succ(NewNeighborhood2)),
                 no_succ_change_allowed),
    % only send pred and succ the new node
    Message = {rm, update_node, NewMe},
    RndView = get_RndView(RandViewSize, Cache),
    {Pred, Succ} = get_safe_pred_succ(NewNeighborhood2, RndView),
    comm:send(node:pidX(Succ), Message, ?SEND_OPTIONS),
    case Pred =/= Succ of
        true -> comm:send(node:pidX(Pred), Message, ?SEND_OPTIONS);
        _    -> ok
    end,
    %% ignore for modr-mode, it is triggered by slide_chord.erl
    {{unknown}, rm_tman_state:set_neighbors(NewNeighborhood2, State)}.

%% @doc Removes all nodes from the given neighborhood which are in the
%%      interval I but keep TolerateNode.
-spec remove_neighbors_in_interval(Neighborhood::nodelist:neighborhood(),
                                   I::intervals:interval(),
                                   TolerateNode::node:node_type() | null)
        -> NewNeighborhood::nodelist:neighborhood().
remove_neighbors_in_interval(Neighborhood, I, TolerateNode) ->
    case intervals:is_empty(I) of
        false ->
            nodelist:filter(
              Neighborhood,
              % note: be resilient in case we have a more up-to-date TolerateNode node info!
              fun(N) -> (not intervals:in(node:id(N), I)) orelse
                            node:same_process(N, TolerateNode) end,
              fun(N) -> contact_new_nodes([N]) end);
        true -> Neighborhood
    end.

-spec contact_new_nodes(NewNodes::[node:node_type()]) -> ok.
contact_new_nodes([_|_] = NewNodes) ->
    % TODO: add a local cache of contacted nodes in order not to contact them again
    This = comm:this(),
    case comm:is_valid(This) of
        true ->
            _ = [begin
                     Pid = node:pidX(Node),
                     comm:send(Pid, {rm, node_info, This, [node, is_leaving]},
                               ?SEND_OPTIONS)
                 end || Node <- NewNodes],
            ok;
        false -> ok
    end;
contact_new_nodes([]) ->
    ok.

%% @doc Failure detector reported dead/changed node.
-spec fd_notify(State::rm_tman_state:state(), Event::fd:event(), DeadPid::comm:mypid(),
                Data::term())
        -> {ChangeReason::rm_loop:reason(), rm_tman_state:state()}.
fd_notify(State, leave, _DeadPid, OldNode) ->
    % graceful leave -> do not add as zombie candidate!
    State2 =
        update_nodes(State, [], [OldNode], null),
    Cache = rm_tman_state:get_cache(State2),
    Neighborhood = rm_tman_state:get_neighbors(State2),
    % try to find a replacement in the cache:
    NewNeighborhood = trigger_update(Neighborhood, [], Cache),
    {{graceful_leave, OldNode},
     rm_tman_state:set_neighbors(?FIXMODRN(NewNeighborhood), State2)};
fd_notify(State, jump, _DeadPid, OldNode) ->
    % remove old node while jumping -> do not add as zombie candidate!
    % the node will be added again (or might already have been added)
    % -> only remove from neighbours if older!
    FilterFun = fun(N) ->
                        ?implies(node:same_process(N, OldNode),
                                 node:is_newer(N, OldNode))
                end,
    State2 =
        update_nodes2(State, [], true, FilterFun, null),
    Cache = rm_tman_state:get_cache(State2),
    Neighborhood = rm_tman_state:get_neighbors(State2),
    % try to find a replacement in the cache:
    NewNeighborhood = trigger_update(Neighborhood, [], Cache),
    {{graceful_leave, OldNode},
     rm_tman_state:set_neighbors(?FIXMODRN(NewNeighborhood), State2)};
fd_notify(State, crash, DeadPid, _Reason) ->
    % crash, i.e. non-graceful leave -> add as zombie candidate
    State2 =
        update_nodes(State, [], [DeadPid], fun dn_cache:add_zombie_candidate/1),
    Cache = rm_tman_state:get_cache(State2),
    Neighborhood = rm_tman_state:get_neighbors(State2),
    % try to find a replacement in the cache:
    NewNeighborhood = trigger_update(Neighborhood, [], Cache),
    {{node_crashed, DeadPid},
     rm_tman_state:set_neighbors(?FIXMODRN(NewNeighborhood), State2)};
fd_notify(State, _Event, _DeadPid, _Data) ->
    {{unknown}, State}.

% dead-node-cache reported dead node to be alive again
-spec zombie_node(State::rm_tman_state:state(), Node::node:node_type())
        -> {ChangeReason::rm_loop:reason(), rm_tman_state:state()}.
zombie_node(State, Node) ->
    % this node could potentially be useful as it has been in our state before
    {{node_discovery}, update_nodes(State, [Node], [], null)}.

-spec get_web_debug_info(State::rm_tman_state:state()) -> [{string(), string()}].
get_web_debug_info(_State) -> [].

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(stabilization_interval_base) and
    config:cfg_is_greater_than(stabilization_interval_base, 0) and

    config:cfg_is_integer(gossip_cyclon_cache_size) and
    config:cfg_is_greater_than(gossip_cyclon_cache_size, 2) and

    config:cfg_is_integer(tman_cyclon_interval) and
    config:cfg_is_greater_than(tman_cyclon_interval, 0) and

    config:cfg_is_integer(succ_list_length) and
    config:cfg_is_greater_than_equal(succ_list_length, 1) and

    config:cfg_is_integer(pred_list_length) and
    config:cfg_is_greater_than_equal(pred_list_length, 1).

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
%%      NOTE: nodes from OtherNeighborhood and RndView compete for the actual
%%            number of contacted nodes, so the (potentially more outdated)
%%            random view should be limited
%%      NOTE: no node is (directly) added by this function, the returned
%%            neighborhood may contain updated node IDs though!
-spec trigger_update(OldNeighborhood::nodelist:neighborhood(),
                     RndView::[node:node_type()],
                     OtherNodes::[node:node_type()])
        -> NewNeighborhood::nodelist:neighborhood().
trigger_update(OldNeighborhood, MyRndView, OtherNodes) ->
    % update node ids with information from the other node's neighborhood
    OldNeighborhood2 = nodelist:update_ids(OldNeighborhood, OtherNodes),
    NewNeighborhood2 =
        nodelist:add_nodes(OldNeighborhood2, lists:append(MyRndView, OtherNodes),
                           get_pred_list_length(), get_succ_list_length()),

    OldView = nodelist:to_list(OldNeighborhood2),
    NewView = nodelist:to_list(NewNeighborhood2),
    ViewOrd = fun(A, B) ->
                      nodelist:succ_ord_node(A, B, nodelist:node(OldNeighborhood2))
              end,
    {_, _, NewNodes} = util:ssplit_unique(OldView, NewView, ViewOrd),

    contact_new_nodes(NewNodes),
    OldNeighborhood2.

%% @doc Adds and removes the given nodes from the rm_tman state.
%%      Note: Sets the new RandViewSize to 0 if NodesToRemove is not empty and
%%      the new neighborhood is different to the old one. If the successor or
%%      predecessor changes, the trigger action will be called immediately.
-spec update_nodes(State::rm_tman_state:state(),
                   NodesToAdd::[node:node_type()],
                   NodesToRemove::[node:node_type() | comm:mypid() | pid()],
                   RemoveNodeEvalFun::fun((node:node_type()) -> any()) | null)
        -> NewState::rm_tman_state:state().
update_nodes(State, NodesToAdd, [], RemoveNodeEvalFun) ->
    update_nodes2(State, NodesToAdd, false, fun(_N) -> true end, RemoveNodeEvalFun);
update_nodes(State, NodesToAdd, [Node], RemoveNodeEvalFun) ->
    FilterFun = fun(N) -> not node:same_process(N, Node) end,
    update_nodes2(State, NodesToAdd, true, FilterFun, RemoveNodeEvalFun);
update_nodes(State, NodesToAdd, [_,_|_] = NodesToRemove, RemoveNodeEvalFun) ->
    FilterFun = fun(N) -> not lists:any(
                            fun(B) -> node:same_process(N, B) end,
                            NodesToRemove)
                end,
    update_nodes2(State, NodesToAdd, true, FilterFun, RemoveNodeEvalFun).

%% @doc Helper for update_nodes/4 with a more generic interface.
%% @see update_nodes/4
-spec update_nodes2(State::rm_tman_state:state(),
                    NodesToAdd::[node:node_type()], MayRemove::boolean(),
                    NodesFilterFun::fun((node:node_type()) -> boolean()),
                    RemoveNodeEvalFun::fun((node:node_type()) -> any()) | null)
        -> NewState::rm_tman_state:state().
update_nodes2(State, [], false, _FilterFun, _RemoveNodeEvalFun) ->
    State;
update_nodes2(OldState,
             NodesToAdd, MayRemove, FilterFun, RemoveNodeEvalFun) ->
    % keep all nodes that are not in NodesToRemove
    % note: NodesToRemove should have 0 or 1 element in most cases
    OldNeighborhood = rm_tman_state:get_neighbors(OldState),
    OldCache = rm_tman_state:get_cache(OldState),
    RandViewSize = rm_tman_state:get_randview_size(OldState),
    OldPredPid = node:pidX(nodelist:pred(OldNeighborhood)),
    OldSuccPid = node:pidX(nodelist:succ(OldNeighborhood)),
    Nbh1 = if is_function(RemoveNodeEvalFun) ->
                  nodelist:filter(OldNeighborhood, FilterFun, RemoveNodeEvalFun);
              true ->
                  nodelist:filter(OldNeighborhood, FilterFun)
           end,
    NewCache = nodelist:lfilter(OldCache, FilterFun),

    NewNeighborhood = nodelist:add_nodes(Nbh1, NodesToAdd,
                                         get_pred_list_length(),
                                         get_succ_list_length()),

    NewChurn = has_churn(OldNeighborhood, NewNeighborhood),
    NewRandViewSize = case NewChurn andalso MayRemove of
                          true -> 0;
                          _    -> RandViewSize
                      end,
    NewPredPid = node:pidX(nodelist:pred(NewNeighborhood)),
    NewSuccPid = node:pidX(nodelist:succ(NewNeighborhood)),
    NewState = rm_tman_state:set_neighbors(NewNeighborhood,
               rm_tman_state:set_randview_size(NewRandViewSize,
               rm_tman_state:set_cache(NewCache,
               rm_tman_state:set_churn(NewChurn, OldState)))),
    if OldPredPid =/= NewPredPid orelse OldSuccPid =/= NewSuccPid ->
           element(2, trigger_action(NewState));
       true -> NewState
    end.

-spec trigger_interval() -> pos_integer().
trigger_interval() -> config:read(stabilization_interval_base) div 1000.

-spec get_pred_list_length() -> pos_integer().
get_pred_list_length() -> config:read(pred_list_length).

-spec get_succ_list_length() -> pos_integer().
get_succ_list_length() -> config:read(succ_list_length).

-spec get_neighbors(rm_tman_state:state()) -> nodelist:neighborhood().
get_neighbors(State) ->
    rm_tman_state:get_neighbors(State).
