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
%% @doc Gossip based calculation of Vivaldi coordinates.
%% @end
%%
%% @version $Id$
-module(gossip_vivaldi).
-behaviour(gossip_beh).
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

% gossip_beh
-export([init/1, check_config/0, trigger_interval/0, fanout/0,
        select_node/1, select_data/1, select_reply_data/4, integrate_data/3,
        handle_msg/2, notify_change/3, min_cycles_per_round/0, max_cycles_per_round/0,
        round_has_converged/1, web_debug_info/1, shutdown/1]).

%% for testing
-export([]).

-ifdef(with_export_type_support).
-endif.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type state() :: any().
-type data() :: any().
-type round() :: non_neg_integer().
-type instance() :: {Module :: gossip_vivaldi, Id :: atom() | uid:global_uid()}.


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
    config:read(gossip_vivaldi_fanout).


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

-spec check_config() -> boolean().
check_config() ->
    true.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Initiate the gossip_vivaldi module. <br/>
%%      Called by the gossip module upon startup. <br/>
%%      The Instance information is ignored, {gossip_vivaldi, default} is always used.
-spec init(Instance::instance()) -> {ok, state()}.
init(_Instance) ->
    {ok, state}.


%% @doc Returns false, i.e. peer selection is done by the gossip module.
-spec select_node(State::state()) -> {true, state()}.
select_node(State) ->
    {false, State}.


%% @doc Select and prepare the data to be sent to the peer. <br/>
%%      The data consists of the coordinates and confidence of this node.
%%      Called by the gossip module at the beginning of every cycle. <br/>
%%      The selected exchange data is to be sent back to the gossip module as a
%%      message of the form {selected_data, Instance, ExchangeData}.
-spec select_data(State::state()) -> {ok, state()}.
select_data(State) ->
    {ok, State}.


%% @doc Process the data from the requestor. <br/>
%%      No reply-data is selected and no select_reply_data_response is sent to
%%      the gossip module, because vivaldi implements a push-only scheme. <br/>
%%      Called by the behaviour module upon a p2p_exch message. <br/>
%%      PData: exchange data from the p2p_exch request <br/>
%%      Ref: used by the gossip module to identify the request <br/>
%%      Round: ignored, as cyclon does not implement round handling
-spec select_reply_data(PData::data(), Ref::pos_integer(), Round::round(), State::state()) ->
    {discard_msg | ok | retry | send_back, state()}.
select_reply_data(_PData, _Ref, _Round, State) ->
    {ok, State}.


%% @doc Empty implementation, because vivaldi implements a push-only scheme. <br/>
-spec integrate_data(QData::data(), Round::round(), State::state()) ->
    {discard_msg | ok | retry | send_back, state()}.
integrate_data(_QData, _Round, State) ->
    {ok, State}.


%% @doc Handle messages
-spec handle_msg(Msg::comm:message(), State::state()) -> {ok, state()}.
handle_msg(_Msg, State) ->
    {ok, State}.


%% @doc Always returns false, as vivaldi does not implement rounds.
-spec round_has_converged(State::state()) -> {boolean(), state()}.
round_has_converged(State) ->
    {false, State}.


%% @doc Notifies the module about changes. <br/>
%%      Changes can be new rounds, leadership changes or exchange failures. All
%%      of them are ignored, as vivaldi doesn't use / implements this features.
-spec notify_change(_, _, State::state()) -> {ok, state()}.
notify_change(_, _, State) ->
    {ok, State}.


%% @doc Returns a key-value list of debug infos for the Web Interface. <br/>
%%      Called by the gossip module upon {web_debug_info} messages.
-spec web_debug_info(state()) ->
    {KeyValueList::[{Key::string(), Value::any()},...], state()}.
web_debug_info(State) ->
    {[{"Key", "Value"}], State}.


%% @doc Shut down the gossip_vivaldi module. <br/>
%%      Called by the gossip module upon stop_gossip_task(CBModule).
-spec shutdown(State::state()) -> {ok, shutdown}.
shutdown(_State) ->
    % nothing to do
    {ok, shutdown}.


