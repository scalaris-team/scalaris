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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Round-Robin-Database (RRD) clone.
%% @end
%% @version $Id$
-module(rrd).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export_type([rrd/0, data_type/0,
              gauge_type/0, counter_type/0, timing_type/0, event_type/1,
              timeseries_type/0, internal_time/0, timespan/0]).

% external API with transparent time handling
-export([create/3,
         add_now/2, check_timeslot_now/1,
         merge/2,
         dump/1, dump_with/2, dump_with/3]).

% external API without transparent time handling
-export([create/4, add/3, check_timeslot/2]).

% internal API for the monitor process
-export([get_slot_start/2, reduce_timeslots/2, add_nonexisting_timeslots/2,
         get_type/1, get_count/1, get_slot_length/1, get_current_time/1,
         get_value/2, get_value_by_offset/2, get_all_values/2,
         add_with/4, timing_with_hist_merge_fun/3]).

% misc
-export([check_config/0]).

% gauge: record newest value of a time slot,
% counter: sum up all values of a time slot,
% event: record every event (incl. timestamp) in a time slot,
% histogram: record all values in a time slot using an approximative histogram
% timing: record time spans, store {sum(x), sum(x^2), count(x), min(x), max(x)}
% timing_with_hist: record time spans, store {sum(x), sum(x^2), count(x), min(x), max(x), histogram(x)}
-type timeseries_type() :: gauge | counter | event |
                           {histogram, Size::non_neg_integer()} |
                           {histogram_rt, Size::non_neg_integer(), BaseKey::histogram_rt:base_key()} |
                           {timing | timing_with_hist, us | ms | s | count | percent}.
-type fill_policy_type() :: set_undefined | keep_last_value.
-type time() :: erlang_timestamp().
-type internal_time() :: non_neg_integer(). % default: micro seconds since Epoch
-type timespan() :: pos_integer().
-type update_fun(T, NewV) :: fun((Time::internal_time(), Old::T | undefined, NewV) -> T).

-type gauge_type() :: number().
-type counter_type() :: number().
-type histogram_type() :: histogram:histogram() | histogram_rt:histogram().
-type timing_type() :: {Sum::number(), Sum2::number(), Count::pos_integer(), Min::number(), Max::number(), Hist::histogram:histogram()}.
-type event_type(T) :: [{internal_time(), T}].
-type data_type() :: gauge_type() | counter_type() | histogram_type() | timing_type() | event_type(term()).

-record(rrd, {slot_length   = ?required(rrd, slot_length)   :: timespan(),
              count         = ?required(rrd, count)         :: pos_integer(),
              type          = ?required(rrd, type)          :: timeseries_type(),
              % index of current slot
              current_index = ?required(rrd, current_index) :: non_neg_integer(),
              % current slot starts at current_time and lasts for step_size
              % time units
              current_time  = ?required(rrd, current_time)  :: internal_time(),
              data          = ?required(rrd, data)          :: array:array(data_type() | undefined),
              fill_policy   = ?required(rrd, fill_policy)   :: fill_policy_type()
            }).

-opaque rrd() :: #rrd{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% External API with transparent time handling
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Creates a new rrd() record. SlotLength in microseconds, Count is the
%%      number of time slots to keep, type the type of rrd to create.
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

-spec dump(rrd()) -> [{From::time(), To::time(), data_type()}].
dump(DB) ->
    dump_with(DB, fun(_DB, From, To, X) ->
                          {util:us2timestamp(From), util:us2timestamp(To), X}
                  end).

-type dump_fun_existing(T) :: fun((rrd(), From::internal_time(), To::internal_time(), Value::data_type()) -> T).
-type dump_fun_nonexisting(T) :: fun((rrd(), From::internal_time(), To::internal_time()) -> ignore | {keep, T}).

-spec dump_with(rrd(), dump_fun_existing(T)) -> [T].
dump_with(DB, FunExist) ->
    dump_with(DB, FunExist, fun(_DB, _From, _To) -> ignore end).

-spec dump_with(rrd(), dump_fun_existing(T), dump_fun_nonexisting(U)) -> [T | U].
dump_with(DB, FunExist, FunNotExist) ->
    CurrentIndex = DB#rrd.current_index,
    Count = DB#rrd.count,
    dump_internal(DB, (CurrentIndex + 1) rem Count, CurrentIndex,
                  DB#rrd.current_time - (Count - 1) * DB#rrd.slot_length, [],
                  FunExist, FunNotExist).

%% @doc Merges any value of DB2 which is in the current or a future time slot
%%      of DB1 into it and returns a new rrd.
%%      Note: gauge rrd values will only be updated if no previous value
%%      existed since we can not determine which value is newer.
%%      For any value from DB2 uses (StartTime + EndTime) div 2 as the time for
%%      adding it.
-spec merge(DB1::rrd(), DB2::rrd()) -> rrd().
merge(DB1 = #rrd{type = Type}, DB2 = #rrd{type = Type}) ->
    MergeFun = case Type of
        gauge   -> fun gauge_merge_fun/3;
        counter -> fun counter_merge_fun/3;
        event   -> fun event_merge_fun/3;
        {timing, _} -> fun timing_merge_fun/3;
        {timing_with_hist, _} -> fun timing_with_hist_merge_fun/3
    end,
    DataL = dump_with(DB2,
                      fun(_DB, StartTime, EndTime, X) ->
                              {(StartTime + EndTime) div 2, X}
                      end),
    lists:foldl(fun({TimeX, ValueX}, DBX) ->
                        add_with(TimeX, ValueX, DBX, MergeFun)
                end, DB1, DataL).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% External API without transparent time handling and some internal API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%@doc StepSize in milliseconds
-spec create(SlotLength::timespan(), Count::pos_integer(), Type::timeseries_type(),
             StartTime::time() | internal_time()) -> rrd().
create(SlotLength, Count, Type, {_, _, _} = StartTime) ->
    create(SlotLength, Count, Type, util:timestamp2us(StartTime));
create(SlotLength, Count, Type, StartTime) ->
    #rrd{slot_length = SlotLength, count = Count, type = Type, current_index = 0,
         current_time = StartTime, data = array:new(Count),
         fill_policy = set_undefined}.

% @doc Note: gauge, counter and timing types accept only number() as value, event
%      accepts any value.
-spec add(Time::time() | internal_time(), Value::term(), rrd()) -> rrd().
add({_, _, _} = ExternalTime, Value, DB) ->
    add_with(util:timestamp2us(ExternalTime), Value, DB, select_fun(DB));
add(Time, Value, DB) ->
    add_with(Time, Value, DB, select_fun(DB)).

-spec select_fun(DB::rrd()) -> update_fun(data_type(), term()).
select_fun(DB) ->
    case DB#rrd.type of
        gauge -> fun gauge_update_fun/3;
        counter -> fun counter_update_fun/3;
        event -> fun event_update_fun/3;
        {histogram, Size} ->
            fun(Time, OldValue, Value) ->
                    histogram_update_fun(Time, OldValue, Value, Size)
            end;
        {histogram_rt, Size, BaseKey} ->
            fun(Time, OldValue, Value) ->
                    histogram_rt_update_fun(Time, OldValue, Value, Size, BaseKey)
            end;
        {timing, _} -> fun timing_update_fun/3;
        {timing_with_hist, _} -> fun timing_with_hist_update_fun/3
    end.

% @doc Advances the stored timeslots (if necessary) to the given time.
-spec check_timeslot(Time::time() | internal_time(), rrd()) -> rrd().
check_timeslot({_, _, _} = ExternalTime, DB) ->
    add_with(util:timestamp2us(ExternalTime), undefined, DB, fun keep_old_update_fun/3);
check_timeslot(Time, DB) ->
    add_with(Time, undefined, DB, fun keep_old_update_fun/3).

-spec add_with(Time::internal_time(), NewV, rrd(), update_fun(data_type(), NewV)) -> rrd().
add_with(Time, Value, DB = #rrd{slot_length = SlotLength,
                                current_time = CurrentTime,
                                current_index = CurrentIndex,
                                data = Data}, F) ->
    case is_current_slot(Time, CurrentTime, SlotLength) of
        true ->
            OldValue = array:get(CurrentIndex, Data),
            DB#rrd{data = array:set(CurrentIndex, F(Time, OldValue, Value), Data)};
        _ ->
            case is_future_slot(Time, CurrentTime, SlotLength) of
                true ->
                    Count = DB#rrd.count,
                    FillPolicy = DB#rrd.fill_policy,
                    Delta = (Time - CurrentTime) div SlotLength,
                    NewIndex = (CurrentIndex + Delta) rem Count,
                    FilledData =
                        if Delta =< Count ->
                                fill(Data, FillPolicy, Count,
                                     (CurrentIndex + 1) rem Count,
                                     NewIndex, CurrentIndex);
                           FillPolicy =:= set_undefined ->
                               % need to wipe all data clean so that old data
                               % is not falsely presented for new time slots
                               % as an optimisation, simply create a new array here
                               array:new(Count);
                           true ->
                               % fill everything:
                               fill(Data, FillPolicy, Count,
                                    (CurrentIndex + 1) rem Count,
                                    CurrentIndex, CurrentIndex)
                        end,
                    DB#rrd{data = array:set(NewIndex, F(Time, undefined, Value), FilledData),
                           current_index = NewIndex,
                           current_time = DB#rrd.current_time + Delta * SlotLength};
                _ -> % PastTimeSlot; ignore
                    DB
            end
    end.

-spec fill(Array, fill_policy_type(), Count::pos_integer(),
           StartIndex::non_neg_integer(), StopIndex::non_neg_integer(),
           IndexLastValue::non_neg_integer()) -> Array
        when is_subtype(Array, array:array(data_type() | undefined)).
fill(Data, FillPolicy, Count, StartIndex, StopIndex, IndexLastValue) ->
    case FillPolicy of
        set_undefined ->
            fill(Data, Count, StartIndex, StopIndex, undefined);
        keep_last_value ->
            fill(Data, Count, StartIndex, StopIndex, array:get(IndexLastValue, Data))
    end.

-spec fill(Array, Count::pos_integer(), StartIndex::non_neg_integer(),
           StopIndex::non_neg_integer(), NewValue::data_type() | undefined) -> Array
        when is_subtype(Array, array:array(data_type() | undefined)).
fill(Data, _Count, StopIndex, StopIndex, _NewValue) ->
    Data;
fill(Data, Count, CurrentGapIndex, StopIndex, NewValue) ->
    fill(array:set(CurrentGapIndex, NewValue, Data), Count,
         (CurrentGapIndex + 1) rem Count,
         StopIndex, NewValue).

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

-spec get_count(DB::rrd()) -> pos_integer().
get_count(DB) -> DB#rrd.count.

-spec get_current_time(DB::rrd()) -> internal_time().
get_current_time(DB) ->
    DB#rrd.current_time.

%% @doc Gets the value at the given time or 'undefined' if there is no value.
-spec get_value(DB::rrd(), Time::time() | internal_time()) -> undefined | data_type().
get_value(DB, {_, _, _} = Time) ->
    get_value(DB, util:timestamp2us(Time));
get_value(DB, InternalTime) when is_integer(InternalTime) ->
    case time2slotidx(InternalTime, DB#rrd.current_time, DB#rrd.slot_length,
                      DB#rrd.current_index, DB#rrd.count) of
        future -> undefined;
        past -> undefined;
        Slot ->
            array:get(Slot, DB#rrd.data)
    end.

%% @doc If SlotOffset is 0, gets the current value, otherwise the value in a
%%      previous slot the given offset away from the current one.
%%      May return 'undefined' if there is no value.
-spec get_value_by_offset(DB::rrd(), SlotOffset::non_neg_integer()) -> undefined | data_type().
get_value_by_offset(DB, 0) ->
    array:get(DB#rrd.current_index, DB#rrd.data); % minor optimization
get_value_by_offset(DB, SlotOffset) when SlotOffset < DB#rrd.count ->
    Count = DB#rrd.count,
    Index = (DB#rrd.current_index + Count - SlotOffset) rem Count,
    array:get(Index, DB#rrd.data);
get_value_by_offset(DB, SlotOffset) when SlotOffset >= DB#rrd.count ->
    % rare use case
    get_value_by_offset(DB, SlotOffset rem DB#rrd.count).

%% @doc Gets all values as list from newest to oldest (desc) or from
%%      oldest to newest (asc). Values may be undefined.
-spec get_all_values(asc | desc, DB::rrd()) -> [undefined | data_type()].
get_all_values(desc, DB) ->
    [get_value_by_offset(DB, Offset) || Offset <- lists:seq(0, DB#rrd.count-1)];
get_all_values(asc, DB) ->
    [get_value_by_offset(DB, Offset) || Offset <- lists:seq(DB#rrd.count-1, 0, -1)].

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

-spec time2slotidx(Time::internal_time(), CurrentTime::internal_time(),
                   StepSize::timespan(), CurrentSlot::non_neg_integer(),
                   SlotCount::pos_integer()) -> non_neg_integer()| future | past.
time2slotidx(Time, CurrentTime, StepSize, CurrentSlot, SlotCount) ->
    case is_current_slot(Time, CurrentTime, StepSize) of
        true -> CurrentSlot;
        _ ->
            case is_future_slot(Time, CurrentTime, StepSize) of
                true -> future;
                _ ->
                    SlotOffset = SlotCount + (Time - CurrentTime) div StepSize,
                    if SlotOffset > 0 -> (CurrentSlot + SlotOffset) rem SlotCount;
                       true           -> past
                    end
            end
    end.

-spec dump_internal(rrd(), CurrentIdx::non_neg_integer(), EndIdx::non_neg_integer(),
                    CurrentTime::internal_time(), Rest::[T],
                    dump_fun_existing(T), dump_fun_nonexisting(U)) -> [T | U].
dump_internal(DB, EndIndex, EndIndex, CurrentTime, Rest, FunExist, FunNotExist) ->
    dump_internal2(DB, EndIndex, CurrentTime, Rest, FunExist, FunNotExist);
dump_internal(DB, IndexToFetch, EndIndex, CurrentTime, Rest, FunExist, FunNotExist) ->
    NewRest = dump_internal2(DB, IndexToFetch, CurrentTime, Rest, FunExist, FunNotExist),
    dump_internal(DB, (IndexToFetch + 1) rem DB#rrd.count, EndIndex,
                  CurrentTime + DB#rrd.slot_length, NewRest, FunExist, FunNotExist).

dump_internal2(DB, CurrentIndex, CurrentTime, Rest, FunExist, FunNotExist) ->
    From = CurrentTime,
    To = CurrentTime + DB#rrd.slot_length,
    case array:get(CurrentIndex, DB#rrd.data) of
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

-spec gauge_update_fun(Time::internal_time(), Old::T | undefined, New::T) -> T when is_subtype(T, number()).
gauge_update_fun(_Time, _Old, New) -> New.

-spec counter_update_fun(Time::internal_time(), Old::T | undefined, New::T) -> T when is_subtype(T, number()).
counter_update_fun(_Time, undefined, New) -> New;
counter_update_fun(_Time, Old, New) -> Old + New.

-spec event_update_fun(Time::internal_time(), Old::event_type(T) | undefined, NewV)
        -> event_type(T | NewV).
event_update_fun(Time, undefined, New) -> [{Time, New}];
event_update_fun(Time, Old, New) -> lists:append(Old, [{Time, New}]).

-spec histogram_update_fun(Time::internal_time(), Old::histogram_type() | undefined, NewV::number(), Size::non_neg_integer())
        -> histogram:histogram().
histogram_update_fun(_Time, undefined, New, Size) ->
    Hist = histogram:create(Size),
    histogram:add(New, Hist);
histogram_update_fun(_Time, Hist, New, _Size) ->
    histogram:add(New, Hist).

-spec histogram_rt_update_fun(Time::internal_time(), Old::histogram_type() | undefined, NewV::no_op | number(), Size::non_neg_integer(), BaseKey::histogram_rt:base_key())
        -> histogram_rt:histogram().
histogram_rt_update_fun(Time, undefined, New, Size, BaseKey) ->
    Hist = histogram_rt:create(Size, BaseKey),
    histogram_rt_update_fun(Time, Hist, New, Size, BaseKey);
histogram_rt_update_fun(_Time, Hist, no_op, _Size, _BaseKey) ->
    Hist;
histogram_rt_update_fun(_Time, Hist, New, _Size, _BaseKey) ->
    histogram_rt:add(New, Hist).

-spec timing_update_fun(Time::internal_time(), Old::timing_type() | undefined, New::number())
        -> timing_type().
timing_update_fun(_Time, undefined, New) ->
    {New, New*New, 1, New, New, histogram:create(0)};
timing_update_fun(_Time, {Sum, Sum2, Count, Min, Max, Hist}, New) ->
    {Sum + New, Sum2 + New*New, Count + 1, erlang:min(Min, New), erlang:max(Max, New), Hist}.

-spec timing_with_hist_update_fun(Time::internal_time(), Old::timing_type() | undefined, New::number())
        -> timing_type().
timing_with_hist_update_fun(_Time, undefined, New) ->
    Hist = histogram:create(get_timing_hist_size()),
    {New, New*New, 1, New, New, histogram:add(New, Hist)};
timing_with_hist_update_fun(_Time, {Sum, Sum2, Count, Min, Max, Hist}, New) ->
    {Sum + New, Sum2 + New*New, Count + 1, erlang:min(Min, New), erlang:max(Max, New),
     histogram:add(New, Hist)}.

-spec keep_old_update_fun(Time::internal_time(), Old::T | undefined, NewV) -> T | NewV.
keep_old_update_fun(_Time, undefined, New) -> New;
keep_old_update_fun(_Time, Old, _New) -> Old.

-spec gauge_merge_fun(Time::internal_time(), Old::T | undefined, New::T | undefined)
        -> T | undefined when is_subtype(T, number()).
gauge_merge_fun(_Time, undefined, New) -> New;
gauge_merge_fun(_Time, Old, _New) -> Old.

-spec counter_merge_fun(Time::internal_time(), Old::T | undefined, New::T | undefined)
        -> T | undefined when is_subtype(T, number()).
counter_merge_fun(_Time, undefined, New) ->
    New;
counter_merge_fun(_Time, Old, undefined) ->
    Old;
counter_merge_fun(_Time, Old, New) ->
    Old + New.

-spec event_merge_fun(Time::internal_time(), Old::event_type(T) | undefined, event_type(NewV) | undefined)
        -> event_type(T | NewV) | undefined.
event_merge_fun(_Time, undefined, New) ->
    New;
event_merge_fun(_Time, Old, undefined) ->
    Old;
event_merge_fun(_Time, Old, New) ->
    lists:usort(lists:append(Old, New)).

-spec timing_merge_fun(Time::internal_time(), Old::timing_type() | undefined, New::timing_type() | undefined)
        -> timing_type() | undefined.
timing_merge_fun(_Time, undefined, New) ->
    New;
timing_merge_fun(_Time, Old, undefined) ->
    Old;
timing_merge_fun(_Time, {Sum, Sum2, Count, Min, Max, Hist},
                  {NewSum, NewSum2, NewCount, NewMin, NewMax, _NewHist}) ->
    {Sum + NewSum, Sum2 + NewSum2, Count + NewCount,
     erlang:min(Min, NewMin), erlang:max(Max, NewMax), Hist}.

-spec timing_with_hist_merge_fun(Time::internal_time(), Old::timing_type() | undefined, New::timing_type() | undefined)
        -> timing_type() | undefined.
timing_with_hist_merge_fun(_Time, undefined, New) ->
    New;
timing_with_hist_merge_fun(_Time, Old, undefined) ->
    Old;
timing_with_hist_merge_fun(_Time, {Sum, Sum2, Count, Min, Max, Hist},
                  {NewSum, NewSum2, NewCount, NewMin, NewMax, NewHist}) ->
    {Sum + NewSum, Sum2 + NewSum2, Count + NewCount,
     erlang:min(Min, NewMin), erlang:max(Max, NewMax), histogram:merge(Hist, NewHist)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% miscellaneous
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether config parameters of the rrd module exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(rrd_timing_hist_size) and
    config:cfg_is_greater_than_equal(rrd_timing_hist_size, 0).

-spec get_timing_hist_size() -> non_neg_integer().
get_timing_hist_size() ->
    config:read(rrd_timing_hist_size).
