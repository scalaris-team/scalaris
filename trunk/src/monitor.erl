%  @copyright 2011 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Monitors data from different processes, e.g. for performance
%%         evaluations.
%% @end
%% @version $Id$
-module(monitor).
-author('kruber@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1, init/1, on/2, check_config/0]).
-export([proc_set_value/3, proc_get_value/2, proc_exists_value/2]).

-type key() :: string().
-type internal_key() :: {'$monitor$', Key::string()}.
-type table_index() :: {Process::atom(), Key::key()}.

-type state() :: Table::tid() | atom().
-type message() ::
    {proc_report, Process::atom(), Key::key(), OldValue::rrd:rrd(), Value::rrd:rrd()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.

%% @doc Converts the given Key to avoid conflicts in erlang:put/get.
-spec to_internal_key(Key::key()) -> internal_key().
to_internal_key(Key) -> {'$monitor$', Key}.

-spec check_report(Process::atom(), Key::key(), Old::Value, New::Value) -> ok.
check_report(Process, Key, OldValue, NewValue) ->
    case OldValue of
        undefined -> ok;
        _ ->
            % check whether to report to the monitor
            % (always report if a new time slot was started)
            SlotOld = rrd:get_slot_start(0, OldValue),
            SlotNew = rrd:get_slot_start(0, NewValue),
            case SlotNew of
                SlotOld -> ok; %nothing to do
                _  -> % new slot -> report to monitor:
                    proc_report_to_my_monitor(Process, Key, OldValue, NewValue)
            end
    end.

%% @doc Sets the value at Key. Either specify a new value or an update function
%%      which generates the new value from the old one.
-spec proc_set_value(Process::atom(), Key::key(),
                     NewValue_or_UpdateFun::term() | fun((Old::Value | undefined) -> New::Value)) -> ok.
proc_set_value(Process, Key, UpdateFun) when is_function(UpdateFun, 1)->
    InternalKey = to_internal_key(Key),
    OldValue = erlang:get(InternalKey),
    NewValue = UpdateFun(OldValue),
    check_report(Process, Key, OldValue, NewValue),
    erlang:put(InternalKey, NewValue);
proc_set_value(Process, Key, NewValue) ->
    InternalKey = to_internal_key(Key),
    OldValue = erlang:put(InternalKey, NewValue),
    check_report(Process, Key, OldValue, NewValue).

%% @doc Checks whether a value exists at Key.
-spec proc_exists_value(Process::atom(), Key::key()) -> boolean().
proc_exists_value(_Process, Key) ->
    erlang:get(to_internal_key(Key)) =/= undefined.

%% @doc Gets the value stored at Key. The key must exist, otherwise no rrd()
%%      structure is returned!
-spec proc_get_value(Process::atom(), Key::key()) -> rrd:rrd().
proc_get_value(_Process, Key) ->
    erlang:get(to_internal_key(Key)).

%% @doc Reports the given value to the process' monitor process.
-spec proc_report_to_my_monitor(Process::atom(), Key::key(), OldValue::rrd:rrd(), Value::rrd:rrd()) -> ok.
proc_report_to_my_monitor(Process, Key, OldValue, Value) ->
    MyMonitor = pid_groups:get_my(monitor),
    % note: it may happen that the new value created a new slot which already
    % discarded all logged data from the previous (unreported) time slot
    % -> send OldValue, too
    comm:send_local(MyMonitor, {proc_report, Process, Key, OldValue, Value}).

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message(), state()) -> state().
on({proc_report, ProcTable, Key, OldValue, _NewValue}, Table) ->
    % note: reporting is always done when a new time slot is created
    % -> use the values from the old value
    TableIndex = {ProcTable, Key},
    MyData = case ets:lookup(Table, TableIndex) of
                 [{TableIndex, X}] -> X;
                 []      ->
                     SlotLength = rrd:get_slot_length(OldValue),
                     OldTime = rrd:get_current_time(OldValue),
                     rrd:create(SlotLength, get_timeslots_to_keep(),
                                rrd:get_type(OldValue),
                                erlang:max(OldTime, OldTime - SlotLength))
             end,
    NewData = rrd:add_nonexisting_timeslots(MyData, OldValue),
    ets:insert(Table, {TableIndex, NewData}),
    Table;

on({web_debug_info, Requestor}, Table) ->
    Keys = get_all_keys(Table),
    GroupedLast5 = [begin
                        KeyData5 = get_last_n(Table, Key, 5),
                        web_debug_info_merge_values(Key, KeyData5)
                    end || Key <- Keys],
    comm:send_local(Requestor, {web_debug_info_reply, [{"last 5 records per key:", ""} | GroupedLast5]}),
    Table.

-spec get_all_keys(Table::tid() | atom()) -> [table_index()].
get_all_keys(Table) ->
    lists:usort(ets:select(Table, [{ {'$1', '$2'},
                                     [],     % guard
                                     ['$1']} % result
                                  ])).

% @doc Reduces the rrd() data to N time slots (the key _must_ exist in the table!).
-spec get_last_n(Table::tid() | atom(), Key::table_index(), N::pos_integer())
        -> Value::rrd:rrd().
get_last_n(Table, Key, N) ->
    [{Key, Data}] = ets:lookup(Table, Key),
    rrd:reduce_timeslots(N, Data).

-spec web_debug_info_dump_fun(rrd:rrd(), From::util:time(), To::util:time(), Value::term())
        -> {From::util:time_utc(), To::util:time_utc(), Diff_in_s::non_neg_integer(), ValueStr::string(), AvgStr::string()}.
web_debug_info_dump_fun(DB, From_, To_, Value) ->
    From = calendar:now_to_universal_time(From_),
    To = calendar:now_to_universal_time(To_),
    Diff_in_s = timer:now_diff(To_, From_) div 1000000,
    AvgStr =
        case rrd:get_type(DB) of
            counter -> io_lib:format(" (avg: ~.2f / s)", [Value / Diff_in_s]);
            event   -> io_lib:format(" (avg: ~.2f / s)", [length(Value) / Diff_in_s]);
            timing  -> io_lib:format(" (avg: ~.2f / s)", [element(3, Value) / Diff_in_s]);
            _       -> ""
        end,
    ValueStr =
        case rrd:get_type(DB) of
            timing  ->
                {Sum, Sum2, Count, Min, Max} = Value,
                Avg = Sum / Count, Avg2 = Sum2 / Count,
                Stddev = math:sqrt(Avg2 - (Avg * Avg)),
                io_lib:format(" avg: ~.2f ms, min: ~.2f ms, max: ~.2f ms, stddev: ~.2f ms",
                              [Avg / 1000, Min / 1000, Max / 1000, Stddev / 1000]);
            _       -> io_lib:format(" ~p", [Value])
        end,
    {From, To, Diff_in_s, lists:flatten(ValueStr), lists:flatten(AvgStr)}.

-spec web_debug_info_merge_values(table_index(), rrd:rrd())
            -> {Key::string(), LastNValues::string()}.
web_debug_info_merge_values(Key, Data) ->
    ValuesLastN =
        [lists:flatten(io_lib:format("~p - ~p UTC (~p s): ~s~s",
                                     [From, To, Diff_in_s, ValueStr, AvgStr]))
         || {From, To, Diff_in_s, ValueStr, AvgStr} <- rrd:dump_with(Data, fun web_debug_info_dump_fun/4)],
    {lists:flatten(io_lib:format("~p", [Key])), string:join(ValuesLastN, "<br />")}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the monitor process, registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, null,
                             [{pid_groups_join_as, DHTNodeGroup, monitor}]).

%% @doc Initialises the module with an empty state.
-spec init(null) -> state().
init(null) ->
    TableName = pid_groups:my_groupname() ++ ":monitor",
    ets:new(list_to_atom(TableName), [ordered_set, protected]).

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(monitor_timeslots_to_keep) and
    config:is_greater_than(monitor_timeslots_to_keep, 0).

-spec get_timeslots_to_keep() -> pos_integer().
get_timeslots_to_keep() ->
    config:read(monitor_timeslots_to_keep).
