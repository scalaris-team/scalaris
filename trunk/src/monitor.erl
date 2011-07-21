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

-ifdef(with_export_type_support).
-export_type([table/0]).
-endif.

-export([start_link/1, init/1, on/2, check_config/0]).
-export([proc_init/1, proc_set_value/3, proc_get_value/2, proc_exists_value/2]).

-type key() :: string().
-type internal_key() :: {'$monitor$', Key::string()}.
-type table_index() :: {ProcTable::pdb:tableid(), Key::key()}.
-opaque table() :: {Process::atom, Table::pdb:tableid()}.

-type state() :: Table::tid() | atom().
-type message() ::
    {proc_report, Process::atom(), Key::key(), OldValue::rrd:rrd(), Value::rrd:rrd()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.


%% @doc Initialises a process' monitor DB. Needs to be called prior to the
%%      other proc_* methods! 
%%      Beware: there is only one monitor table available per process if pdb
%%      uses erlang:put/get!
-spec proc_init(Process::atom()) -> table().
proc_init(Process) ->
    % note: make table name unique if named_table is used
    {Process, pdb:new(Process, [ordered_set, protected])}.

%% @doc Converts the given Key avoid conflicts in case erlang:put/get is used
%%      by pdb.
-spec to_internal_key(Key::key()) -> internal_key().
to_internal_key(Key) -> {'$monitor$', Key}.

%% @doc Sets the value at Key.
-spec proc_set_value(table(), Key::key(), Value::term()) -> ok.
proc_set_value({Process, Table}, Key, Value) ->
    InternalKey = to_internal_key(Key),
    case pdb:get(InternalKey, Table) of
        undefined    ->
            ok;
        {InternalKey, OldValue} ->
            % check whether to report to the monitor
            % (always report if a new time slot was started)
            SlotOld = rrd:get_slot_start(0, OldValue),
            SlotNew = rrd:get_slot_start(0, Value),
            case SlotNew of
                SlotOld -> ok; %nothing to do
                _  -> 
                    proc_report_to_my_monitor(Process, Key, OldValue, Value) % new slot -> report to monitor
            end
    end,
    pdb:set({InternalKey, Value}, Table).

%% @doc Checks whether a value exists at Key.
-spec proc_exists_value(table(), Key::key()) -> boolean().
proc_exists_value({_Process, Table}, Key) -> proc_exists_value_(Table, to_internal_key(Key)).

%% @doc Checks whether a value exists at Key (for internal use).
-spec proc_exists_value_(Table::pdb:tableid(), Key::internal_key()) -> boolean().
proc_exists_value_(Table, InternalKey) ->
    pdb:get(InternalKey, Table) =/= undefined.

%% @doc Gets the value stored at Key. The key must exist!
-spec proc_get_value(table(), Key::key()) -> rrd:rrd().
proc_get_value({_Process, Table}, Key) -> proc_get_value_(Table, to_internal_key(Key)).

%% @doc Gets the value stored at Key (for internal use). The key must exist!
-spec proc_get_value_(Table::pdb:tableid(), Key::internal_key()) -> rrd:rrd().
proc_get_value_(Table, InternalKey) ->
    {InternalKey, Value} = pdb:get(InternalKey, Table),
    Value.

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

-spec web_debug_info_dump_fun(rrd:rrd(), From::util:time(), To::util:time(), Value)
        -> {From::util:time_utc(), To::util:time_utc(), Diff_in_s::non_neg_integer(), Value, Avg::float() | undefined}.
web_debug_info_dump_fun(DB, From_, To_, Value) ->
    From = calendar:now_to_universal_time(From_),
    To = calendar:now_to_universal_time(To_),
    Diff_in_s = timer:now_diff(To_, From_) div 1000000,
    Avg = case rrd:get_type(DB) of
        counter -> Value / Diff_in_s;
        event   -> length(Value) / Diff_in_s;
        _       -> undefined
    end,
    {From, To, Diff_in_s, Value, Avg}.

-spec web_debug_info_merge_values(table_index(), rrd:rrd())
            -> {Key::string(), LastNValues::string()}.
web_debug_info_merge_values(Key, Data) ->
    ValuesLastN =
        [begin
             lists:flatten(
               case Avg of
                   undefined ->
                       io_lib:format("~p - ~p UTC (~p s): ~p",
                                     [From, To, Diff_in_s, Value]);
                   _ ->
                       io_lib:format("~p - ~p UTC (~p s): ~p (avg: ~.2f / s)",
                                     [From, To, Diff_in_s, Value, Avg])
               end)
         end || {From, To, Diff_in_s, Value, Avg} <- rrd:dump_with(Data, fun web_debug_info_dump_fun/4)],
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
