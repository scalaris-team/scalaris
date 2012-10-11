%  @copyright 2007-2011 Zuse Institute Berlin

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

-type(relative_size() :: float()).
-type(centroid() :: vivaldi:network_coordinate()).
-type(centroids() :: [centroid()]).
-type(sizes() :: [relative_size()]).

% state of the clustering loop
-type(state_active() :: {Centroids::centroids(), Sizes::sizes(),
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
    {clustering_shuffle, comm:mypid(), centroids(), sizes()} |
    {clustering_shuffle_reply, comm:mypid(), centroids(), sizes()} |
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
    gen_component:change_handler({[], [], ResetTriggerState2, ClusterTriggerState2},
                                 fun ?MODULE:on_active/2);

on_inactive(Msg = {query_clustering, _Pid},
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
          {_Centroids, _Sizes, ResetTriggerState, ClusterTriggerState}) ->
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
          {Centroids, Sizes, ResetTriggerState, ClusterTriggerState}) ->
    % start new clustering shuffle
    %io:format("~p~n", [State]),
    NewClusterTriggerState = trigger:next(ClusterTriggerState),
    cyclon:get_subset_rand(1),
    {Centroids, Sizes, ResetTriggerState, NewClusterTriggerState};

% ask vivaldi for network coordinate
on_active({reset_clustering},
          {Centroids, Sizes, ResetTriggerState, ClusterTriggerState}) ->
    NewResetTriggerState = trigger:next(ResetTriggerState),
    vivaldi:get_coordinate(),
    {Centroids, Sizes, NewResetTriggerState, ClusterTriggerState};

% reset the local state
on_active({vivaldi_get_coordinate_response, Coordinate, _Confidence},
          {_Centroids, _Sizes, ResetTriggerState, ClusterTriggerState}) ->
    {[Coordinate], [1.0], ResetTriggerState, ClusterTriggerState};

on_active({cy_cache, []}, State)  ->
    % ignore empty cache from cyclon
    State;

% got random node from cyclon
on_active({cy_cache, [Node] = _Cache},
          {Centroids, Sizes, _ResetTriggerState, _ClusterTriggerState} = State) ->
    %io:format("~p~n",[_Cache]),
    comm:send(node:pidX(Node),
              {clustering_shuffle, comm:this(), Centroids, Sizes},
              [{group_member, dc_clustering}]),
    State;

% have been asked to shuffle
on_active({clustering_shuffle, RemoteNode, RemoteCentroids, RemoteSizes},
          {Centroids, Sizes, ResetTriggerState, ClusterTriggerState}) ->
    %io:format("{shuffle, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    comm:send(RemoteNode, {clustering_shuffle_reply,
                           comm:this(),
                           Centroids, Sizes}),
    {NewCentroids, NewSizes} = cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes),
    {NewCentroids, NewSizes, ResetTriggerState, ClusterTriggerState};

% got shuffle response
on_active({clustering_shuffle_reply, _RemoteNode, RemoteCentroids, RemoteSizes},
          {Centroids, Sizes, ResetTriggerState, ClusterTriggerState}) ->
    %io:format("{shuffle_reply, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    %vivaldi_latency:measure_latency(RemoteNode, RemoteCoordinate, RemoteConfidence),
    {NewCentroids, NewSizes} = cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes),
    {NewCentroids, NewSizes, ResetTriggerState, ClusterTriggerState};

% return my clusters
on_active({query_clustering, Pid}, {Centroids, Sizes, _ResetTriggerState, _ClusterTriggerState} = State) ->
    comm:send(Pid, {query_clustering_response, {Centroids, Sizes}}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec cluster(centroids(), sizes(), centroids(), sizes()) -> {centroids(), sizes()}.
cluster(Centroids, Sizes, RemoteCentroids, RemoteSizes) ->
    Radius = config:read(dc_clustering_radius),
    {NewCentroids, NewSizes} = mathlib:aggloClustering(Centroids ++ RemoteCentroids,
                                                       Sizes ++ RemoteSizes, Radius),
    NormalizedSizes = lists:map(fun (S) -> 0.5 * S end, NewSizes),
    {NewCentroids, NormalizedSizes}.

%% @doc Gets the clustering reset interval set in scalaris.cfg.
-spec get_clustering_reset_interval() -> pos_integer().
get_clustering_reset_interval() ->
    config:read(dc_clustering_reset_interval).

%% @doc Gets the clustering interval, e.g. how often to calculate clusters, set
%%      in scalaris.cfg.
-spec get_clustering_interval() -> pos_integer().
get_clustering_interval() ->
    config:read(dc_clustering_interval).
