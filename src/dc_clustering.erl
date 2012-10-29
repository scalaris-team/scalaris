%  @copyright 2007-2012 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @author Marie Hoffmann <hoffmann@zib.de>
%% @doc    Re-register with boot nodes
%% @end
%% @reference T. Schuett, A. Reinefeld,F. Schintke, M. Hoffmann.
%% Gossip-based Topology Inference for Efficient Overlay Mapping on Data Centers.
%% 9th Int. Conf. on Peer-to-Peer Computing Seattle, Sept. 2009.
%% @version $Id$
-module(dc_clustering).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).
-include("scalaris.hrl").

-export([start_link/1]).

-export([init/1, on_inactive/2, on_active/2,
         activate/0, deactivate/0]).

% state of the clustering loop
-type(state_active() :: {Centroids::dc_centroids:centroids(),
                         LocalEpoch::non_neg_integer(),
                         ResetTriggerState::trigger:state(),
                         ClusterTriggerState::trigger:state()}).
-type(state_inactive() :: {inactive, QueuedMessages::msg_queue:msg_queue(),
                           ResetTriggerState::trigger:state(),
                           ClusterTriggerState::trigger:state()}).
%% -type(state() :: state_active() | state_inactive()).

% accepted messages of an initialized dc_clustering process
-type(message() :: 
    {start_clustering_shuffle} |
    {reset_clustering} |
    {vivaldi_get_coordinate_response, vivaldi:network_coordinate(), vivaldi:error()} |
    {cy_cache, [node:node_type()]} |
    {clustering_shuffle, comm:mypid(), dc_centroids:centroids(), non_neg_integer()} |
    {clustering_shuffle_reply, comm:mypid(), dc_centroids:centroids(), non_neg_integer()} |
    {query_clustering, comm:mypid()}).

%% @doc Sends an initialization message to the node's dc_clustering process.
-spec activate() -> ok | ignore.
activate() ->
    case config:read(dc_clustering_enable) of
        true ->
            Pid = pid_groups:get_my(dc_clustering),
            comm:send_local(Pid, {activate_clustering});
        false ->
            ignore
    end.

%% @doc Deactivates the cyclon process.
-spec deactivate() -> ok | ignore.
deactivate() ->
    case config:read(dc_clustering_enable) of
        true ->
            Pid = pid_groups:get_my(dc_clustering),
            comm:send_local(Pid, {deactivate_clustering});
        false ->
            ignore
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the dc_clustering process, registers it with the process
%%      dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()} | ignore.
start_link(DHTNodeGroup) ->
    case config:read(dc_clustering_enable) of
        true ->
            ResetTrigger = config:read(dc_clustering_reset_trigger),
            ClusterTrigger = config:read(dc_clustering_cluster_trigger),
            gen_component:start_link(?MODULE, fun ?MODULE:on_inactive/2, {ResetTrigger, ClusterTrigger},
                                     [{pid_groups_join_as, DHTNodeGroup, dc_clustering}]);
        false ->
            ignore
    end.

%% @doc Initialises the module with an empty state.
-spec init({module(), module()}) -> state_inactive().
init({ResetTrigger, ClusterTrigger}) ->
    ResetTriggerState = trigger:init(ResetTrigger, fun get_clustering_reset_interval/0, reset_clustering),
    ClusterTriggerState = trigger:init(ClusterTrigger, fun get_clustering_interval/0, start_clustering_shuffle),
    {inactive, msg_queue:new(), ResetTriggerState, ClusterTriggerState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler during start up phase (will change to on_active/2 when a
%%      'activate_clustering' message is received).
-spec on_inactive(message(), state_inactive()) -> state_inactive();
                ({activate_clustering}, state_inactive()) -> {'$gen_component', [{on_handler, Handler::gen_component:handler()}], State::state_active()}.
on_inactive({activate_clustering},
            {inactive, QueuedMessages, ResetTriggerState, ClusterTriggerState}) ->
    log:log(info, "[ Clustering ~.0p ] activating...~n", [comm:this()]),
    ResetTriggerState2 = trigger:now(ResetTriggerState),
    ClusterTriggerState2 = trigger:now(ClusterTriggerState),
    msg_queue:send(QueuedMessages),
    gen_component:change_handler({dc_centroids:empty_centroids_list(), 0, ResetTriggerState2, ClusterTriggerState2},
                                 fun ?MODULE:on_active/2);

on_inactive(Msg = {query_clustering, _Pid},
            {inactive, QueuedMessages, ResetTriggerState, ClusterTriggerState}) ->
    {inactive, msg_queue:add(QueuedMessages, Msg), ResetTriggerState, ClusterTriggerState};

on_inactive({query_epoch, _Pid} = Msg, 
            {inactive, QueuedMessages, ResetTriggerState, ClusterTriggerState}) ->
    {inactive, msg_queue:add(QueuedMessages, Msg), ResetTriggerState, ClusterTriggerState};

on_inactive({web_debug_info, Requestor},
            {inactive, QueuedMessages, _ResetTriggerState, _ClusterTriggerState} = State) ->
    % get a list of up to 50 queued messages to display:
    MessageListTmp = [{"", webhelpers:safe_html_string("~p", [Message])}
                  || Message <- lists:sublist(QueuedMessages, 50)],
    MessageList = case length(QueuedMessages) > 50 of
                      true -> lists:append(MessageListTmp, [{"...", ""}]);
                      _    -> MessageListTmp
                  end,
    KeyValueList = [{"", ""}, {"inactive clustering process", ""}, {"queued messages:", ""} | MessageList],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

on_inactive(_Msg, State) ->
    State.

%% @doc Message handler when the module is fully initialized.
-spec on_active(Message::message(), State::state_active()) -> state_active();
         ({deactivate_clustering}, state_active()) -> {'$gen_component', [{on_handler, Handler::gen_component:handler()}], State::state_inactive()}.
on_active({deactivate_clustering},
          {_Centroids, ResetTriggerState, ClusterTriggerState}) ->
    log:log(info, "[ Clustering ~.0p ] deactivating...~n", [comm:this()]),
    gen_component:change_handler({inactive, msg_queue:new(), ResetTriggerState, ClusterTriggerState},
                                 fun ?MODULE:on_inactive/2);

% ignore activate_clustering messages in active state
% note: remove this if the clustering process is to be deactivated on leave (see
% dht_node_move.erl). In the current implementation we can not distinguish
% between the first join and a re-join but after every join, the process is
% (re-)activated.
on_active({activate_clustering}, State) ->
    State;
    
on_active({start_clustering_shuffle},
          {Centroids, Epoch, ResetTriggerState, ClusterTriggerState}) ->
    % start new clustering shuffle -> gossip communication
    %io:format("~p~n", [State]),
    NewClusterTriggerState = trigger:next(ClusterTriggerState),
    cyclon:get_subset_rand(1),
    {Centroids, Epoch, ResetTriggerState, NewClusterTriggerState};

% ask vivaldi for network coordinate
on_active({reset_clustering},
          {Centroids, Epoch, ResetTriggerState, ClusterTriggerState}) ->
    NewResetTriggerState = trigger:next(ResetTriggerState),
    vivaldi:get_coordinate(),
    {Centroids, Epoch, NewResetTriggerState, ClusterTriggerState};

% reset the local state and start a new epoche
on_active({vivaldi_get_coordinate_response, Coordinate, _Confidence},
          {_Centroids, Epoch, ResetTriggerState, ClusterTriggerState}) ->
          NewEpoch = Epoch + 1,
    {[dc_centroids:new(Coordinate, 1.0)], NewEpoch,ResetTriggerState, ClusterTriggerState};

on_active({cy_cache, []}, State)  ->
    % ignore empty cache from cyclon
    State;

% got random node from cyclon
on_active({cy_cache, [Node] = _Cache},
          {Centroids, Epoch, _ResetTriggerState, _ClusterTriggerState} = State) ->
    %io:format("~p~n",[_Cache]),
    comm:send(node:pidX(Node), {clustering_shuffle, comm:this(), Centroids, Epoch},
              [{group_member, dc_clustering}]),
    State;

% have been asked to shuffle
on_active({clustering_shuffle, RemoteNode, RemoteCentroids, RemoteEpoch}, State) ->
    %io:format("{shuffle, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    handle_shuffle(State, {RemoteNode, RemoteEpoch, RemoteCentroids})
    ;

% got shuffle response
on_active({clustering_shuffle_reply, _RemoteNode, RemoteCentroids, RemoteEpoch}, State) ->
    %io:format("{shuffle_reply, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    %vivaldi_latency:measure_latency(RemoteNode, RemoteCoordinate, RemoteConfidence),
    handle_shuffle(State, {ignore, RemoteEpoch, RemoteCentroids})
    ;

% return my clusters
on_active({query_clustering, Pid}, {Centroids, _Epoch, _ResetTriggerState, _ClusterTriggerState} = State) ->
    comm:send(Pid, {query_clustering_response, Centroids}),
    State
    ;

on_active({query_epoch, Pid},
    {_Centroids, Epoch,_ResetTriggerState, _ClusterTriggerState} = State) ->
    comm:send(Pid, {query_epoch_response, Epoch}),
    State
    .

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec cluster(centroids:centroids(), centroids:centroids()) -> centroids:centroids().
cluster([], RemoteCentroids) -> RemoteCentroids;
cluster(Centroids, RemoteCentroids) ->
    % TODO extract Radius into state
    Radius = case config:read(dc_clustering_radius) of
        failed -> exit(dc_clustering_radius_not_set);
        R when R > 0 -> R;
        _Else -> exit(dc_clustering_radius_invalid)
    end,

    %% TODO avoid ++
    NewCentroids = mathlib:aggloClustering(Centroids ++ RemoteCentroids, Radius),
    NormalizedCentroids = lists:map(fun (Centroid) ->
                dc_centroids:set_relative_size(Centroid,
                    0.5 * dc_centroids:get_relative_size(Centroid))
                end, NewCentroids),
    % XXX fails: 1 = lists:foldr(fun(S, Acc) -> Acc + S end, 0, NormalizedSizes),
    NormalizedCentroids.

%% @doc Gets the clustering reset interval set in scalaris.cfg.
-spec get_clustering_reset_interval() -> pos_integer().
get_clustering_reset_interval() ->
    config:read(dc_clustering_reset_interval).

%% @doc Gets the clustering interval, e.g. how often to calculate clusters, set
%%      in scalaris.cfg.
-spec get_clustering_interval() -> pos_integer().
get_clustering_interval() ->
    config:read(dc_clustering_interval).

-spec handle_shuffle(state_active(),
        {RemoteNode :: comm:mypid() | ignore, RemoteEpoch :: non_neg_integer(),
         RemoteCentroids :: dc_centroids:centroids()})
     -> state_active().
    handle_shuffle({Centroids, Epoch, ResetTriggerState, ClusterTriggerState}, {RemoteNode, RemoteEpoch, RemoteCentroids}) ->
    % Check the epoch to swallow old messages and delete old state:
    %
    % - if the local epoch is less than the remote epoch, we didn't see a certain wave yet and
    % have to reset the local cluster state.
    % - if the local epoch is bigger than the remote epoch, we ignore the information we
    % received and only send our information, as the remote node is behind the last wave
    % we saw.

    % With this, we purge "zombie centroids" (stale centroids far away from any real
    % clusters) over time: clusters can not survive a wave if they seized to exist during
    % the cut.

    {NewEpoch, NewCentroids} =
    if Epoch < RemoteEpoch -> % reset and merge remote information
            vivaldi:get_coordinate(),
            {RemoteEpoch, []}
            ;
        Epoch > RemoteEpoch -> % ignore remote information, keep talking
            case RemoteNode of
                ignore -> ok;
                R -> comm:send(R, {clustering_shuffle_reply, comm:this(), Centroids, Epoch})
            end,
            {Epoch, Centroids};
        true -> % Epoch == RemoteEpoch
            case RemoteNode of
                ignore -> ok;
                R -> comm:send(R, {clustering_shuffle_reply, comm:this(), Centroids, Epoch})
            end,
            {Epoch, cluster(Centroids, RemoteCentroids)}
    end,
    case NewEpoch of % reset needed?
        Epoch -> {NewCentroids, Epoch, ResetTriggerState, ClusterTriggerState};
        _Else ->
            NewResetTriggerState = trigger:next(ResetTriggerState),
            NewClusterTriggerState = trigger:next(ClusterTriggerState),
            % Note: NewEpoch - 1 because we will increment it in the handler for the
            % vivaldi reply
            {NewCentroids, NewEpoch - 1, NewResetTriggerState, NewClusterTriggerState}
    end
    .
