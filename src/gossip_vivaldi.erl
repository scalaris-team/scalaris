%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%  @copyright 2008-2017 Zuse Institute Berlin

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
%% @author Jens V. Fischer <jensvfischer@gmail.com>
%% @doc Gossip based calculation of Vivaldi coordinates.
%% @end
%%
%% @version $Id$
-module(gossip_vivaldi).
-author('schuett@zib.de').
-author('jensvfischer@gmail.com').
-behaviour(gossip_beh).
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

%API
-export([get_coordinate/0]).

% gossip_beh
-export([init/1, check_config/0, trigger_interval/0, fanout/0,
        select_node/1, select_data/1, select_reply_data/4, integrate_data/3,
        handle_msg/2, notify_change/3, min_cycles_per_round/0, max_cycles_per_round/0,
        round_has_converged/1, web_debug_info/1, shutdown/1]).

%% for testing
-export([update_coordinate/5]).

-export_type([est_error/0, latency/0, network_coordinate/0]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type(network_coordinate() :: [float()]).
-type(est_error() :: float()).
-type(latency() :: number()).

-type state() :: {network_coordinate(), est_error()}.
-type data() :: any().
-type round() :: non_neg_integer().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Config Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%------- External config function (called by gossip module) -------%%

%% @doc The time interval in ms after which a new cycle is triggered by the gossip
%%      module.
-spec trigger_interval() -> pos_integer().
trigger_interval() -> % in ms
    config:read(gossip_vivaldi_interval).


%% @doc The fanout (number of peers contacted per cycle).
-spec fanout() -> pos_integer().
fanout() ->
    1.


%% @doc The minimum number of cycles per round.
%%      Returns infinity, as rounds are not implemented by vivaldi.
-spec min_cycles_per_round() -> infinity.
min_cycles_per_round() ->
    infinity.


%% @doc The maximum number of cycles per round.
%%      Returns infinity, as rounds are not implemented by vivaldi.
-spec max_cycles_per_round() -> infinity.
max_cycles_per_round() ->
    infinity.


%%------------------- Private config functions ---------------------%%

%% @doc The dimensions for the coordinate system.
-spec vivaldi_dimensions() -> pos_integer().
vivaldi_dimensions() ->
    config:read(gossip_vivaldi_dimensions).


%% @doc Vivaldi doesn't need instantiabilty, so {gossip_vivaldi, default} is
%%      always used.
-spec instance() -> {gossip_vivaldi, default}.
-compile({inline, [instance/0]}).
instance() ->
    {gossip_vivaldi, default}.


%% @doc Checks whether config parameters of the vivaldi process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(gossip_vivaldi_interval) and
    config:cfg_is_greater_than(gossip_vivaldi_interval, 0) and

    config:cfg_is_integer(gossip_vivaldi_dimensions) and
    config:cfg_is_greater_than_equal(gossip_vivaldi_dimensions, 2).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends a (local) message with a vivaldi get_coordinate request to the gossip
%%      module of the requesting process' group asking for the current coordinate
%%      and confidence.
-spec get_coordinate() -> ok.
get_coordinate() ->
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {cb_msg, instance(), {get_coordinate, comm:this()}}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initiate the gossip_vivaldi module. <br/>
%%      Called by the gossip module upon startup. <br/>
%%      The Instance information is ignored, {gossip_vivaldi, default} is always used.
-spec init(Args::[any()]) -> {ok, state()}.
init(_Args) ->
    log:log(info, "[ Vivaldi ~.0p ] activating...~n", [comm:this()]),
    comm:send_local(pid_groups:get_my(gossip), {trigger_action, instance()}),
    {ok, {random_coordinate(), 1.0}}.


%% @doc Returns false, i.e. peer selection is done by the gossip module.
-spec select_node(State::state()) -> {false, state()}.
select_node(State) ->
    {false, State}.


%% @doc Select and prepare the data to be sent to the peer. <br/>
%%      The data consists of the coordinates and confidence of this node.
%%      Called by the gossip module at the beginning of every cycle. <br/>
%%      The selected exchange data is to be sent back to the gossip module as a
%%      message of the form {selected_data, Instance, ExchangeData}.
-spec select_data(State::state()) -> {ok, state()}.
select_data(State={Coordinate, Confidence}) ->
    Data = {comm:this(), Coordinate, Confidence},
    comm:send_local(pid_groups:get_my(gossip), {selected_data, instance(), Data}),
    {ok, State}.


%% @doc Process the data from the requestor. <br/>
%%      No reply-data is selected and no select_reply_data_response is sent to
%%      the gossip module, because vivaldi implements a push-only scheme. <br/>
%%      Called by the behaviour module upon a p2p_exch message. <br/>
%%      PData: exchange data from the p2p_exch request <br/>
%%      Ref: used by the gossip module to identify the request <br/>
%%      Round: ignored, as vivaldi does not implement round handling
-spec select_reply_data(PData::data(), Ref::pos_integer(), Round::round(), State::state()) ->
    {discard_msg | ok | retry | send_back, state()}.
select_reply_data(PData, _Ref, _Round, State) ->
    {SourcePid, RemoteCoordinate, RemoteConfidence} = PData,
    %io:format("{shuffle, ~p, ~p}~n", [RemoteCoordinate, RemoteConfidence]),
    _ = vivaldi_latency:measure_latency(SourcePid, RemoteCoordinate, RemoteConfidence),
    {ok, State}.


%% @doc Ignored, vivaldi is a push-only scheme.
-spec integrate_data(QData::data(), Round::round(), State::state()) ->
    {discard_msg | ok | retry | send_back, state()}.
integrate_data(_QData, _Round, State) ->
    {ok, State}.


%% @doc Handle messages
%%      Response from vivaldi_latency after finishing measuring.
-spec handle_msg(Msg::comm:message(), State::state()) -> {ok, state()}.
handle_msg({update_vivaldi_coordinate, Latency, {RemoteCoordinate, RemoteConfidence}},
           {Coordinate, Confidence}) ->
    %io:format("latency is ~pus~n", [Latency]),
    {NewCoordinate, NewConfidence} =
        try
            update_coordinate(RemoteCoordinate, RemoteConfidence,
                              Latency, Coordinate, Confidence)
        catch
            % ignore any exceptions, e.g. badarith
            error:_ -> {Coordinate, Confidence}
        end,
    %% signal gossip module, that the cycle is finished
    comm:send_local(pid_groups:get_my(gossip), {integrated_data, instance(), cur_round}),
    {ok, {NewCoordinate, NewConfidence}};


%% Request for coordinates, usually from get_coordinate() api function
handle_msg({get_coordinate, SourcePid}, State={Coordinate, Confidence}) ->
    comm:send(SourcePid, {vivaldi_get_coordinate_response, Coordinate, Confidence},
              [{channel, prio}]),
    {ok, State}.


%% @doc Always returns false, vivaldi does not implement rounds.
-spec round_has_converged(State::state()) -> {boolean(), state()}.
round_has_converged(State) -> {false, State}.


%% @doc Ignored, vivaldi doesn't use / implements these features.
-spec notify_change(_, _, State::state()) -> {ok, state()}.
notify_change(_, _, State) -> {ok, State}.


%% @doc Returns a key-value list of debug infos for the Web Interface. <br/>
%%      Called by the gossip module upon {web_debug_info} messages.
-spec web_debug_info(state()) ->
    {KeyValueList::[{Key::string(), Value::any()},...], state()}.
web_debug_info(State={Coordinate, Confidence}) ->
    KeyValueList =
        [{"gossip_vivaldi", ""},
         {"coordinate", webhelpers:safe_html_string("~p", [Coordinate])},
         {"confidence", Confidence}],
    {KeyValueList, State}.


%% @doc Shut down the gossip_vivaldi module. <br/>
%%      Called by the gossip module upon stop_gossip_task(CBModule).
-spec shutdown(State::state()) -> {ok, shutdown}.
shutdown(_State) ->
    % nothing to do
    {ok, shutdown}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec random_coordinate() -> network_coordinate().
random_coordinate() ->
    % note: network coordinates are float vectors!
    [ float(randoms:rand_uniform(1, 10)) || _ <- lists:seq(1, vivaldi_dimensions()) ].


-spec update_coordinate(network_coordinate(), est_error(), latency(),
                         network_coordinate(), est_error()) ->
                            {network_coordinate(), est_error()}.
update_coordinate(Coordinate, _RemoteError, _Latency, Coordinate, Error) ->
    % same coordinate
    {Coordinate, Error};
update_coordinate(RemoteCoordinate, RemoteError, Latency, Coordinate, Error) ->
    Cc = 0.5, Ce = 0.5,
    % sample weight balances local and remote error
    W = if (Error + RemoteError) =:= 0.0 -> 0.0;
           (Error + RemoteError) =/= 0.0 -> Error/(Error + RemoteError)
        end,
    % relative error of sample
    Es = abs(mathlib:euclideanDistance(RemoteCoordinate, Coordinate) - Latency) / Latency,
    % update weighted moving average of local error
    Error1 = Es * Ce * W + Error * (1 - Ce * W),
    % update local coordinates
    Delta = Cc * W,
    %io:format('expected latency: ~p~n', [mathlib:euclideanDist(Coordinate, _RemoteCoordinate)]),
    C1 = mathlib:u(mathlib:vecSub(Coordinate, RemoteCoordinate)),
    C2 = mathlib:euclideanDistance(Coordinate, RemoteCoordinate),
    C3 = Latency - C2,
    C4 = C3 * Delta,
    Coordinate1 = mathlib:vecAdd(Coordinate, mathlib:vecMult(C1, C4)),
    %io:format("new coordinate ~p and error ~p~n", [Coordinate1, Error1]),
    {Coordinate1, Error1}.


