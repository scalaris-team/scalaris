%  @copyright 2008-2011 Zuse Institute Berlin

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
%% @doc    Gossiping behaviour
%% @end
%% @version $Id$
-module(gossip_beh).
-author('jensvfischer@gmail.com').
-vsn('$Id$').

% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-export_type([exch_data/0, round_status/0, instance/0]).

%% cb: callback
-type cb_state() :: any().
-type instance() :: {CBModule::module(), InstanceId:: atom() | uid:global_uid()}.
-type exch_data() :: any().
-type round_status() :: 'current_round' | 'old_round'.

% Erlang version >= R15B
-ifdef(have_callback_support).

-type round() :: non_neg_integer().
-type notify_tag() :: new_round | leader | exch_failure.
-type notify_msg() :: {is_leader|no_leader, NewRange::intervals:interval()} |
        {new_round, round()} | {MsgTag::p2p_exch | p2p_exch_reply, exch_data(), round()}.

% Startup & Shutdown

-callback init([proplists:property()]) -> {ok, cb_state()}.
-callback shutdown(State::cb_state()) -> {ok, shutdown}.

% Gossiping Message Loop

-callback select_node(State::cb_state()) -> {boolean(), cb_state()}.

-callback select_data(State::cb_state()) ->
    % has to initiate {selected_data, CBModule::module(), PData::exch_data()}
    {ok | discard_msg, cb_state()}.

-callback select_reply_data(PData::exch_data(), Ref::pos_integer(), Round::round(), State::cb_state()) ->
    % has to initiate {selected_reply_data, CBModule::module(), QData::exch_data()}
    {ok | discard_msg | retry | send_back, cb_state()}.

-callback integrate_data(Data::exch_data(), Round::round(), State::cb_state()) ->
    % has to initiate {integrated_data, CBModule::module()}
    {ok | discard_msg | retry | send_back, cb_state()}.

-callback handle_msg(Message::comm:message(), State::cb_state()) ->
    {ok, cb_state()}.

% Config and Misc

-callback fanout() -> pos_integer().

-callback trigger_interval() -> pos_integer().

-callback min_cycles_per_round() -> non_neg_integer() | infinity.

-callback max_cycles_per_round() -> pos_integer() | infinity.

-callback round_has_converged(State::cb_state()) -> {boolean(), cb_state()}.

-callback notify_change(notify_tag(), notify_msg(), State::cb_state()) ->
    {ok, cb_state()}.

-callback web_debug_info(State::cb_state()) ->
    {KeyValueList::[{Key::string(), Value::any()},...], cb_state()}.

-callback check_config() -> boolean().

% Erlang version < R15B
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
  [ {init, 1},
    {shutdown, 1},
    {select_node, 1},
    {select_data, 1},
    {select_reply_data, 4},
    {integrate_data, 3},
    {handle_msg, 2},
    {trigger_interval, 0},
    {fanout, 0},
    {min_cycles_per_round, 0},
    {max_cycles_per_round, 0},
    {round_has_converged, 1},
    {notify_change, 3},
    {web_debug_info,1},
    {check_config, 0}
  ];
behaviour_info(_Other) ->
  undefined.

-endif.
