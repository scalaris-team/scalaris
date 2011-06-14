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

-ifdef(with_export_type_support).
-export_type([rrd/0]).
-endif.
-export([create/3, add_now/2, dump/1]).

-export([create/4, add/3]).

-record(rrd, {step_size :: float(),
              count :: pos_integer(),
              type :: timeseries_type(),
              % index of current slot
              current_index :: pos_integer(),
              % current slot starts at current_time and lasts for step_size
              % time units
              current_time :: time(),
              data :: array()
            }).

-opaque rrd() :: #rrd{}.


-type timeseries_type() :: gauge | counter.
-type time() :: float(). % | {integer(), integer(), integer()}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% External API with transparent time handling
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%@doc StepSize in milliseconds
-spec create(StepSize::time(), Count::pos_integer(), Type::timeseries_type()) ->
    rrd().
create(StepSize, Count, Type) ->
    create(StepSize, Count, Type, erlang:now()).

-spec add_now(Value::number(), rrd()) -> rrd().
add_now(Value, DB) ->
    add(erlang:now(), Value, DB).

dump(DB) ->
    Count = DB#rrd.count,
    StepSize = DB#rrd.step_size,
    CurrentIndex = DB#rrd.current_index,
    dump_internal(DB#rrd.data,
                  Count, (CurrentIndex + 1) rem Count, CurrentIndex,
                  StepSize, DB#rrd.current_time - (Count - 1) * StepSize,
                  []).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Internal API (allows to specify timestamps explicitly
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%@doc StepSize in milliseconds
-spec create(StepSize::time(), Count::pos_integer(), Type::timeseries_type(),
             StartTime::time()) -> rrd().
create(StepSize, Count, Type, StartTime) ->
    #rrd{step_size = StepSize, count = Count, type = Type, current_index = 0,
         current_time = StartTime, data = array:new(Count)}.

add(Time, Value, DB) ->
    case DB#rrd.type of
        gauge ->
            add_gauge(Time, Value, DB);
        counter ->
            add_counter(Time, Value, DB)
    end.

add_gauge(Time, Value, DB) ->
    StepSize = DB#rrd.step_size,
    CurrentTime = DB#rrd.current_time,
    {CurrentTimeSlot,FutureTimeSlot} = get_slot_type(Time, CurrentTime, StepSize),
    if
        CurrentTimeSlot ->
            CurrentIndex = DB#rrd.current_index,
            DB#rrd{data = array:set(CurrentIndex, Value, DB#rrd.data)};
        FutureTimeSlot ->
            Delta = (Time - CurrentTime) div StepSize,
            CurrentIndex = DB#rrd.current_index + Delta,
            % fill with default ???
            DB#rrd{data = array:set(CurrentIndex, Value, DB#rrd.data),
                   current_index = CurrentIndex,
                   current_time = DB#rrd.current_time + Delta * StepSize};
        true -> % PastTimeSlot; ignore
            DB
    end.

add_counter(Time, Value, DB) ->
    DB.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Private API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
is_current_slot(Time, CurrentTime, StepSize) ->
    CurrentTime =< Time andalso Time < CurrentTime + StepSize.

is_future_slot(Time, StartTime, StepSize) ->
    StartTime + StepSize =< Time.

get_slot_type(Time, CurrentTime, StepSize) ->
    CurrentSlot = is_current_slot(Time, CurrentTime, StepSize),
    FutureSlot = is_future_slot(Time, CurrentTime, StepSize),
    {CurrentSlot, FutureSlot}.

dump_internal(Data,
              Count, CurrentIndex, CurrentIndex,
              StepSize, CurrentTime, Rest) ->
    case array:get(CurrentIndex, Data) of
        undefined ->
            Rest;
        Value ->
            [{CurrentTime, Value} | Rest]
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
                          [{CurrentTime, Value} | Rest])
    end.
