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
%% @version $Id$
-module(rrd).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-ifdef(with_export_type_support).
-export_type([rrd/0, timing_type/1, internal_time/0]).
-endif.

% external API with transparent time handling
-export([create/3, add_now/2, check_timeslot_now/1,
         dump/1, dump_with/2, dump_with/3]).

% external API without transparent time handling
-export([create/4, add/3, check_timeslot/2]).

% internal API for the monitor process
-export([get_slot_start/2, reduce_timeslots/2, add_nonexisting_timeslots/2,
         get_type/1, get_slot_length/1, get_current_time/1,
         add_with/4, set_new_update_fun/3, keep_old_update_fun/3]).

% misc
-export([timestamp2us/1, us2timestamp/1, check_config/0]).

% gauge: record newest value of a time slot,
% counter: sum up all values of a time slot,
% event: record every event (incl. timestamp) in a time slot,
% timing: record time spans, store {sum(x), sum(x^2), count(x), min(x), max(x)}
-type timeseries_type() :: gauge | counter | event | timing.
-type fill_policy_type() :: set_undefined | keep_last_value.
-type time() :: util:time().
-type internal_time() :: non_neg_integer().
-type timespan() :: pos_integer().
-type update_fun(T, NewV) :: fun((Time::internal_time(), Old::T | undefined, NewV) -> T).

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
% @doc SlotLength in microseconds
-spec create(SlotLength::timespan(), Count::pos_integer(), Type::timeseries_type()) ->
    rrd().
create(SlotLength, Count, Type) ->
    create(SlotLength, Count, Type, os:timestamp()).

% @doc Note: gauge, counter and timing types accept only number() as value, event
%      accepts any value.
-spec add_now(Value::term(), rrd()) -> rrd().
add_now(Value, DB) ->
    add(os:timestamp(), Value, DB).

% @doc Advances the stored timeslots (if necessary) to the current time.
-spec check_timeslot_now(rrd()) -> rrd().
check_timeslot_now(DB) ->
    check_timeslot(os:timestamp(), DB).

-spec dump(rrd()) -> [{From::time(), To::time(), term()}].
dump(DB) ->
    dump_with(DB, fun(_DB, From, To, X) -> {From, To, X} end).

-type dump_fun_existing(T) :: fun((rrd(), From::time(), To::time(), Value::term()) -> T).
-type dump_fun_nonexisting(T) :: fun((rrd(), From::time(), To::time()) -> ignore | {keep, T}).

-spec dump_with(rrd(), dump_fun_existing(T)) -> [T].
dump_with(DB, FunExist) ->
    dump_with(DB, FunExist, fun(_DB, _From, _To) -> ignore end).

-spec dump_with(rrd(), dump_fun_existing(T), dump_fun_nonexisting(U)) -> [T | U].
dump_with(DB, FunExist, FunNotExist) ->
    SlotLength = DB#rrd.slot_length,
    CurrentIndex = DB#rrd.current_index,
    Count = DB#rrd.count,
    dump_internal(DB, (CurrentIndex + 1) rem Count, CurrentIndex,
                  DB#rrd.current_time - (Count - 1) * SlotLength, [], FunExist, FunNotExist).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Internal API (allows to specify timestamps explicitly)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%@doc StepSize in milliseconds
-spec create(SlotLength::timespan(), Count::pos_integer(), Type::timeseries_type(),
             StartTime::time() | internal_time()) -> rrd().
create(SlotLength, Count, Type, {_, _, _} = StartTime) ->
    create(SlotLength, Count, Type, timestamp2us(StartTime));
create(SlotLength, Count, Type, StartTime) ->
    #rrd{slot_length = SlotLength, count = Count, type = Type, current_index = 0,
         current_time = StartTime, data = array:new(Count),
         fill_policy = set_undefined}.

-spec set_new_update_fun(Time::internal_time(), Old::T | undefined, New::T) -> T when is_subtype(T, number()).
set_new_update_fun(_Time, _Old, New) -> New.

-spec counter_update_fun(Time::internal_time(), Old::T | undefined, New::T) -> T when is_subtype(T, number()).
counter_update_fun(_Time, undefined, New) -> New;
counter_update_fun(_Time, Old, New) -> Old + New.

-spec event_update_fun(Time::internal_time(), Old::[{internal_time(), T}] | undefined, NewV)
        -> [{internal_time(), T | NewV}].
event_update_fun(Time, undefined, New) -> [{Time, New}];
event_update_fun(Time, Old, New) -> lists:append(Old, [{Time, New}]).

-type timing_type(T) :: {Sum::T, Sum2::T, Count::pos_integer(), Min::T, Max::T, Hist::histogram:histogram()}.
-spec timing_update_fun(Time::internal_time(), Old::timing_type(T) | undefined, New::T)
        -> timing_type(T) when is_subtype(T, number()).
timing_update_fun(_Time, undefined, New) ->
    Hist = histogram:create(get_timing_hist_size()),
    {New, New*New, 1, New, New, histogram:add(New, Hist)};
timing_update_fun(_Time, {Sum, Sum2, Count, Min, Max, Hist}, New) ->
    {Sum + New, Sum2 + New*New, Count + 1, erlang:min(Min, New), erlang:max(Max, New),
     histogram:add(New, Hist)}.

-spec keep_old_update_fun(Time::internal_time(), Old::T | undefined, NewV) -> T | NewV.
keep_old_update_fun(_Time, undefined, New) -> New;
keep_old_update_fun(_Time, Old, _New) -> Old.

% @doc Note: gauge, counter and timing types accept only number() as value, event
%      accepts any value.
-spec add(Time::time() | internal_time(), Value::term(), rrd()) -> rrd().
add({_, _, _} = ExternalTime, Value, DB) ->
    add(timestamp2us(ExternalTime), Value, DB);
add(Time, Value, DB) ->
    case DB#rrd.type of
        gauge ->
            add_with(Time, Value, DB, fun set_new_update_fun/3);
        counter ->
            add_with(Time, Value, DB, fun counter_update_fun/3);
        event ->
            add_with(Time, Value, DB, fun event_update_fun/3);
        timing ->
            add_with(Time, Value, DB, fun timing_update_fun/3)
    end.

% @doc Advances the stored timeslots (if necessary) to the given time.
-spec check_timeslot(Time::time() | internal_time(), rrd()) -> rrd().
check_timeslot({_, _, _} = ExternalTime, DB) ->
    check_timeslot(timestamp2us(ExternalTime), DB);
check_timeslot(Time, DB) ->
    add_with(Time, undefined, DB, fun keep_old_update_fun/3).

-spec add_with(Time::internal_time(), NewV, rrd(), update_fun(term(), NewV)) -> rrd().
add_with(Time, Value, DB, F) ->
    SlotLength = DB#rrd.slot_length,
    CurrentTime = DB#rrd.current_time,
    {CurrentTimeSlot, FutureTimeSlot} = get_slot_type(Time, CurrentTime, SlotLength),
    if
        CurrentTimeSlot ->
            CurrentIndex = DB#rrd.current_index,
            update_with(DB, CurrentIndex, Time, Value, F);
        FutureTimeSlot ->
            Delta = (Time - CurrentTime) div SlotLength,
            CurrentIndex = (DB#rrd.current_index + Delta) rem DB#rrd.count,
            % fill with default ???
            FilledDB = fill(DB, (DB#rrd.current_index + 1) rem DB#rrd.count,
                            CurrentIndex,
                            array:get(DB#rrd.current_index, DB#rrd.data)),
            DB#rrd{data = array:set(CurrentIndex, F(Time, undefined, Value), FilledDB#rrd.data),
                   current_index = CurrentIndex,
                   current_time = DB#rrd.current_time + Delta * SlotLength};
        true -> % PastTimeSlot; ignore
            DB
    end.

-spec update_with(rrd(), CurrentIndex::non_neg_integer(), Time::internal_time(), NewV, update_fun(term(), NewV)) -> rrd().
update_with(DB, CurrentIndex, Time, NewValue, F) ->
    case array:get(CurrentIndex, DB#rrd.data) of
        undefined ->
            DB#rrd{data = array:set(CurrentIndex, F(Time, undefined, NewValue), DB#rrd.data)};
        OldValue ->
            DB#rrd{data = array:set(CurrentIndex, F(Time, OldValue, NewValue), DB#rrd.data)}
    end.

-spec fill(rrd(), non_neg_integer(), non_neg_integer(), term()) -> rrd().
fill(DB, CurrentIndex, CurrentIndex, _LastValue) ->
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
% Internal API for the monitor
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the start of the given slot.
%%      Some examples of values for SlotIdx:
%%      previous slot: -1, current slot: 0, next slot: 1
-spec get_slot_start(SlotIdx::integer(), DB::rrd()) -> internal_time().
get_slot_start(0, DB) ->
    DB#rrd.current_time; % minor optimization
get_slot_start(SlotIdx, DB) ->
    DB#rrd.current_time + SlotIdx * DB#rrd.slot_length.

%% @doc Reduces the number of time slots.
-spec reduce_timeslots(SlotCount::pos_integer(), DB::rrd()) -> rrd().
reduce_timeslots(SlotCount, DB) when SlotCount >= DB#rrd.count ->
    DB#rrd{data = array:resize(SlotCount, DB#rrd.data),
           count = SlotCount};
reduce_timeslots(SlotCount, DB) ->
    Count = DB#rrd.count,
    CurrentIndex = DB#rrd.current_index,
    NewDB = DB#rrd{data = array:new(SlotCount), count = SlotCount,
                   current_index = 0},
    EndIdx = (CurrentIndex + Count - SlotCount + 1) rem Count,
    copy_data(DB, NewDB#rrd.current_index, CurrentIndex, EndIdx, NewDB).

%% @doc Adds to DB all non-existing time slots from OtherDB that are newer than
%%      or are in the current time slot of DB. Both structures must have the
%%      same type and the same slot borders!
-spec add_nonexisting_timeslots(DB::rrd(), OtherDB::rrd()) -> rrd().
add_nonexisting_timeslots(#rrd{type = T, slot_length = L} = DB,
                          #rrd{type = T, slot_length = L} = OtherDB) ->
    OtherCurrentIdx = OtherDB#rrd.current_index,
    OtherStartIdx = (OtherCurrentIdx + 1) rem OtherDB#rrd.count,
    add_nonexisting_timeslots_internal(DB, OtherDB, OtherStartIdx, OtherCurrentIdx).

-spec add_nonexisting_timeslots_internal(DB::rrd(), OtherDB::rrd(), OtherStart::non_neg_integer(), OtherEnd::non_neg_integer()) -> rrd().
add_nonexisting_timeslots_internal(DB, OtherDB, OtherEnd, OtherEnd) ->
    add_nonexisting_timeslots_internal2(DB, OtherDB, OtherEnd);
add_nonexisting_timeslots_internal(DB, OtherDB, OtherIndex, OtherEnd) ->
    NewDB = add_nonexisting_timeslots_internal2(DB, OtherDB, OtherIndex),
    add_nonexisting_timeslots_internal(NewDB, OtherDB, (OtherIndex + 1) rem OtherDB#rrd.count, OtherEnd).

-spec add_nonexisting_timeslots_internal2(DB::rrd(), OtherDB::rrd(), OtherIndex::non_neg_integer()) -> rrd().
add_nonexisting_timeslots_internal2(DB, OtherDB, OtherIndex) ->
    OtherCount = OtherDB#rrd.count,
    OtherCurrentIdx = OtherDB#rrd.current_index,
    OtherIndexTime = get_slot_start((OtherIndex - OtherCurrentIdx - OtherCount) rem OtherCount, OtherDB),
    OtherValue = array:get(OtherIndex, OtherDB#rrd.data),
    add_with(OtherIndexTime, OtherValue, DB, fun keep_old_update_fun/3).

-spec get_slot_length(DB::rrd()) -> timespan().
get_slot_length(DB) -> DB#rrd.slot_length.

-spec get_type(DB::rrd()) -> timeseries_type().
get_type(DB) -> DB#rrd.type.

-spec get_current_time(DB::rrd()) -> internal_time().
get_current_time(DB) ->
    DB#rrd.current_time.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Private API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec is_current_slot(Time::internal_time(), CurrentTime::internal_time(), StepSize::timespan()) -> boolean().
is_current_slot(Time, CurrentTime, StepSize) ->
    CurrentTime =< Time andalso Time < CurrentTime + StepSize.

-spec is_future_slot(Time::internal_time(), CurrentTime::internal_time(), StepSize::timespan()) -> boolean().
is_future_slot(Time, CurrentTime, StepSize) ->
    CurrentTime + StepSize =< Time.

-spec get_slot_type(Time::internal_time(), CurrentTime::internal_time(), StepSize::timespan()) -> {boolean(), boolean()}.
get_slot_type(Time, CurrentTime, StepSize) ->
    CurrentSlot = is_current_slot(Time, CurrentTime, StepSize),
    FutureSlot = is_future_slot(Time, CurrentTime, StepSize),
    {CurrentSlot, FutureSlot}.

-spec dump_internal(rrd(), CurrentIdx::non_neg_integer(), EndIdx::non_neg_integer(),
                    CurrentTime::internal_time(), Rest::[T],
                    dump_fun_existing(T), dump_fun_nonexisting(U)) -> [T | U].
dump_internal(DB, EndIndex, EndIndex, CurrentTime, Rest, FunExist, FunNotExist) ->
    dump_internal2(DB, EndIndex, CurrentTime, Rest, FunExist, FunNotExist);
dump_internal(DB, IndexToFetch, EndIndex, CurrentTime, Rest, FunExist, FunNotExist) ->
    NewRest = dump_internal2(DB, IndexToFetch, CurrentTime, Rest, FunExist, FunNotExist),
    Count = DB#rrd.count,
    dump_internal(DB, (IndexToFetch + 1) rem Count, EndIndex,
                  CurrentTime + DB#rrd.slot_length, NewRest, FunExist, FunNotExist).

dump_internal2(DB, CurrentIndex, CurrentTime, Rest, FunExist, FunNotExist) ->
    Data = DB#rrd.data,
    From = us2timestamp(CurrentTime),
    To = us2timestamp(CurrentTime + DB#rrd.slot_length),
    case array:get(CurrentIndex, Data) of
        undefined -> case FunNotExist(DB, From, To) of
                         ignore -> Rest;
                         {keep, X} -> [X | Rest]
                     end;
        Value     -> [FunExist(DB, From, To, Value) | Rest]
    end.

-spec copy_data(rrd(), IndexToWrite::non_neg_integer(), Start::non_neg_integer(), End::non_neg_integer(), Acc::rrd()) -> rrd().
copy_data(DB, IndexToWrite, EndIndex, EndIndex, AccDB) ->
    copy_data2(DB, IndexToWrite, EndIndex, AccDB);
copy_data(DB, IndexToWrite, CurrentIndex, EndIndex, AccDB) ->
    NewAccDB = copy_data2(DB, IndexToWrite, CurrentIndex, AccDB),
    AccCount = AccDB#rrd.count,
    Count = DB#rrd.count,
    copy_data(DB, (IndexToWrite + AccCount - 1) rem AccCount,
              (CurrentIndex + Count - 1) rem Count, EndIndex, NewAccDB).

-spec copy_data2(rrd(), IndexToWrite::non_neg_integer(), Start::non_neg_integer(), Acc::rrd()) -> rrd().
copy_data2(DB, IndexToWrite, CurrentIndex, AccDB) ->
    CurrentData = array:get(CurrentIndex, DB#rrd.data),
    AccData = array:set(IndexToWrite, CurrentData, AccDB#rrd.data),
    AccDB#rrd{data = AccData}.

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% miscellaneous
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters of the rrd module exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(rrd_timing_hist_size) and
    config:is_greater_than_equal(rrd_timing_hist_size, 0).

-spec get_timing_hist_size() -> non_neg_integer().
get_timing_hist_size() ->
    config:read(rrd_timing_hist_size).
