% @copyright 2011 Zuse Institute Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    RRD clone.
%%% @end
%% @version $Id: util.erl 1747 2011-05-27 20:17:36Z lakedaimon300@googlemail.com $
-module(rrd).

-author('schuett@zib.de').
-vsn('$Id: util.erl 1747 2011-05-27 20:17:36Z lakedaimon300@googlemail.com $').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-ifdef(with_export_type_support).
-export_type([rrd/0]).
-endif.

% external API with transparent time handling
-export([create/3, add_now/2, dump/1]).

% external API without transparent time handling
-export([create/4, add/3]).

% for unit-testing only
-export([timestamp2us/1, us2timestamp/1]).

-type timeseries_type() :: gauge | counter.
-type fill_policy_type() :: set_undefined | keep_last_value.
-type time() :: timer:timestamp().
-type internal_time() :: non_neg_integer().
-type timespan() :: pos_integer().

-record(rrd, {slot_length   = ?required(rrd, slot_length)   :: timespan(),
              count         = ?required(rrd, count)         :: pos_integer(),
              type          = ?required(rrd, type)          :: timeseries_type(),
              % index of current slot
              current_index = ?required(rrd, current_index) :: non_neg_integer(),
              % current slot starts at current_time and lasts for step_size
              % time units
              current_time  = ?required(rrd, current_time)  :: internal_time(),
              data          = ?required(rrd, data)          :: array(),
              fill_policy   = ?required(rrd, fill_policy)   :: fill_policy_type()
            }).

-opaque rrd() :: #rrd{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% External API with transparent time handling
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%@doc SlotLength in microseconds
-spec create(SlotLength::timespan(), Count::pos_integer(), Type::timeseries_type()) ->
    rrd().
create(SlotLength, Count, Type) ->
    create(SlotLength, Count, Type, os:timestamp()).

-spec add_now(Value::number(), rrd()) -> rrd().
add_now(Value, DB) ->
    add(os:timestamp(), Value, DB).

-spec dump(rrd()) -> list({time(), number()}).
dump(DB) ->
    Count = DB#rrd.count,
    SlotLength = DB#rrd.slot_length,
    CurrentIndex = DB#rrd.current_index,
    dump_internal(DB#rrd.data,
                  Count, (CurrentIndex + 1) rem Count, CurrentIndex,
                  SlotLength, DB#rrd.current_time - (Count - 1) * SlotLength,
                  []).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Internal API (allows to specify timestamps explicitly
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%@doc StepSize in milliseconds
-spec create(SlotLength::timespan(), Count::pos_integer(), Type::timeseries_type(),
             StartTime::time() | internal_time()) -> rrd().
create(SlotLength, Count, Type, {_, _, _} = StartTime) ->
    #rrd{slot_length = SlotLength, count = Count, type = Type, current_index = 0,
         current_time = timestamp2us(StartTime), data = array:new(Count),
         fill_policy = set_undefined};
create(SlotLength, Count, Type, StartTime) ->
    #rrd{slot_length = SlotLength, count = Count, type = Type, current_index = 0,
         current_time = StartTime, data = array:new(Count),
         fill_policy = set_undefined}.

-spec add(Time::time() | internal_time(), Value::number(), rrd()) -> rrd().
add({_, _, _} = ExternalTime, Value, DB) ->
    Time = timestamp2us(ExternalTime),
    case DB#rrd.type of
        gauge ->
            add_with(Time, Value, DB, fun (_Old, New) -> New end);
        counter ->
            add_with(Time, Value, DB, fun (Old, New) -> Old + New end)
    end;
add(Time, Value, DB) ->
    case DB#rrd.type of
        gauge ->
            add_with(Time, Value, DB, fun (_Old, New) -> New end);
        counter ->
            add_with(Time, Value, DB, fun (Old, New) -> Old + New end)
    end.

-spec add_with(Time::internal_time(), Value::number(), rrd(), fun()) -> rrd().
add_with(Time, Value, DB, F) ->
    SlotLength = DB#rrd.slot_length,
    CurrentTime = DB#rrd.current_time,
    {CurrentTimeSlot,FutureTimeSlot} = get_slot_type(Time, CurrentTime, SlotLength),
    if
        CurrentTimeSlot ->
            CurrentIndex = DB#rrd.current_index,
            update_with(DB, CurrentIndex, Value, F);
        FutureTimeSlot ->
            Delta = (Time - CurrentTime) div SlotLength,
            CurrentIndex = (DB#rrd.current_index + Delta) rem DB#rrd.count,
            % fill with default ???
            FilledDB = fill(DB,
                              (DB#rrd.current_index + 1) rem DB#rrd.count,
                              CurrentIndex,
                              array:get(DB#rrd.current_index, DB#rrd.data)),
            DB#rrd{data = array:set(CurrentIndex, Value, FilledDB#rrd.data),
                   current_index = CurrentIndex,
                   current_time = DB#rrd.current_time + Delta * SlotLength};
        true -> % PastTimeSlot; ignore
            DB
    end.

update_with(DB, CurrentIndex, NewValue, F) ->
    case array:get(CurrentIndex, DB#rrd.data) of
        undefined ->
            DB#rrd{data = array:set(CurrentIndex, NewValue, DB#rrd.data)};
        OldValue ->
            DB#rrd{data = array:set(CurrentIndex, F(OldValue, NewValue), DB#rrd.data)}
    end.

-spec fill(rrd(), pos_integer(), pos_integer(), number()) -> rrd().
fill(DB, _CurrentIndex, _CurrentIndex, _LastValue) ->
    DB;
fill(DB, CurrentGapIndex, CurrentIndex, LastValue) ->
    case DB#rrd.fill_policy of
        set_undefined ->
            fill(DB#rrd{data = array:set(CurrentGapIndex, undefined, DB#rrd.data)},
                 (CurrentGapIndex + 1) rem DB#rrd.count,
                 CurrentIndex, LastValue);
        keep_last_value ->
            fill(DB#rrd{data = array:set(CurrentGapIndex, LastValue, DB#rrd.data)},
                 (CurrentGapIndex + 1) rem DB#rrd.count,
                 CurrentIndex, LastValue)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Private API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
is_current_slot(Time, CurrentTime, StepSize) ->
    CurrentTime =< Time andalso Time < CurrentTime + StepSize.

is_future_slot(Time, CurrentTime, StepSize) ->
    CurrentTime + StepSize =< Time.

get_slot_type(Time, CurrentTime, StepSize) ->
    CurrentSlot = is_current_slot(Time, CurrentTime, StepSize),
    FutureSlot = is_future_slot(Time, CurrentTime, StepSize),
    {CurrentSlot, FutureSlot}.

dump_internal(Data,
              _Count, CurrentIndex, CurrentIndex,
              _StepSize, CurrentTime, Rest) ->
    case array:get(CurrentIndex, Data) of
        undefined ->
            Rest;
        Value ->
            [{us2timestamp(CurrentTime), Value} | Rest]
    end;
dump_internal(Data,
              Count, IndexToFetch, CurrentIndex,
              StepSize, CurrentTime, Rest) ->
    case array:get(IndexToFetch, Data) of
        undefined ->
            dump_internal(Data, Count, (IndexToFetch + 1) rem Count, CurrentIndex,
                          StepSize, CurrentTime + StepSize,
                          Rest);
        Value ->
            dump_internal(Data, Count, (IndexToFetch + 1) rem Count, CurrentIndex,
                          StepSize, CurrentTime + StepSize,
                          [{us2timestamp(CurrentTime), Value} | Rest])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% time calculations
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc convert os:timestamp() to microsecs
-spec timestamp2us(time()) -> internal_time().
timestamp2us({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

-spec us2timestamp(internal_time()) -> time().
us2timestamp(Time) ->
    MicroSecs = Time rem 1000000,
    Time2 = (Time - MicroSecs) div 1000000,
    Secs = Time2 rem 1000000,
    MegaSecs = (Time2 - Secs) div 1000000,
    {MegaSecs, Secs, MicroSecs}.
