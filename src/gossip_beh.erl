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

-export_type([exch_data/0, round_status/0, cb_return/0, instance/0]).

% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

%% cb: callback
-type cb_state() :: any().
-type instance() :: {CBModule::module(), InstanceId:: atom() | uid:global_uid()}.
-type exch_data() :: any().
-type aggregates() :: any().
-type cb_return() :: {KeyValueList::list({list(), list()}) | discard_msg |
    false | ok | retry | send_back | true, cb_state()}.
-type new_leader_msg() :: {is_leader|no_leader, NewRange::intervals:interval()}.
-type round() :: non_neg_integer().
-type round_status() :: 'current_round' | 'old_round'.
-type notify_keyword() :: new_round | leader | exch_failure.

% Erlang version >= R15B
-ifdef(have_callback_support).

% Startup

-callback init(Instance::instance()) -> cb_return().
-callback init(Instance::instance(), Arg1::any()) -> cb_return().
-callback init(Instance::instance(), Arg1::any(), Arg2::any()) -> cb_return().

% Gossiping Message Loop

-callback select_node(State::cb_state()) -> {true|false, cb_state()}.

-callback select_data(State::cb_state()) ->
    % has to initiate {selected_data, CBModule::module(), PData::exch_data()}
    cb_return().

-callback select_reply_data(PData::exch_data(), Ref::pos_integer(),
    RoundStatus::round_status(), Round::round(), State::cb_state()) ->
    % has to initiate {selected_reply_data, CBModule::module(), QData::exch_data()}
    cb_return().

-callback integrate_data(Data::exch_data(), RoundStatus::round_status(),
    Round::round(), State::cb_state()) ->
    % has to initiate {integrated_data, CBModule::module()}
    cb_return().

-callback handle_msg(Message::comm:message(), State::cb_state()) ->
    cb_return().

% Config and Misc

-callback init_delay() -> non_neg_integer().

-callback trigger_interval() -> pos_integer().

-callback min_cycles_per_round() -> non_neg_integer().

-callback max_cycles_per_round() -> pos_integer().

-callback round_has_converged(State::cb_state()) ->
    {true|false, cb_state()}.

-callback notify_change(notify_keyword(), _|new_leader_msg(), State::cb_state()) -> cb_return().

% Result extraction

-callback get_values_best(State::cb_state()) -> BestValues::aggregates().

-callback get_values_all(State::cb_state()) -> AllValues::aggregates().

-callback web_debug_info(State::cb_state()) ->
    {KeyValueList::[{Key::any(), Value::any()},...], cb_state()}.

% Erlang version < R15B
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
  [ {init, 1},
    {select_node, 1},
    {select_data, 1},
    {select_reply_data, 5},
    {integrate_data, 4},
    {handle_message, 2},
    {init_delay, 0},
    {trigger_interval, 0},
    {min_cycles_per_round, 0},
    {max_cycles_per_round, 0},
    {round_has_converged, 1},
    {notify_change, 2},
    {get_values_best, 1},
    {get_values_all, 1},
    {web_debug_info,1}
  ];
behaviour_info(_Other) ->
  undefined.
-endif.


