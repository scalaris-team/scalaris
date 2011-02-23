% @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc Methods for working with the gossip state.
%% @version $Id$
-module(gossip_state).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([state/0, values/0, values_internal/0,
              avg/0, avg2/0, stddev/0, size_inv/0, size/0, avg_kr/0, min/0,
              max/0, round/0, triggered/0, msg_exch/0, converge_avg_count/0]).
-endif.

%% Externally usable exports:
-export([get/2]).

%% Exports only meant for use by gossip.erl:
-export([% constructors:
         new_state/0, new_state/1,
         new_internal/0, new_internal/7,
         % getters:
         % setters:
         set/3,
         set_values/2,
         set_initialized/1,
         reset_triggered/1,
         inc_triggered/1,
         reset_msg_exch/1,
         inc_msg_exch/1,
         reset_converge_avg_count/1,
         inc_converge_avg_count/1,
         % misc.
         conv_state_to_extval/1,
         calc_size_ldr/1,
         calc_size_kr/1,
         calc_stddev/1]).

%% Gossip types
-type(avg() :: float() | unknown).
-type(avg2() :: float() | unknown).
-type(stddev() :: float() | unknown).
-type(size_inv() :: float() | unknown).
-type(size() :: float() | unknown).
-type(avg_kr() :: number() | unknown).
-type(min() :: non_neg_integer() | unknown).
-type(max() :: non_neg_integer() | unknown).
-type(round() :: non_neg_integer()).
-type(triggered() :: non_neg_integer()).
-type(msg_exch() :: non_neg_integer()).
-type(converge_avg_count() :: non_neg_integer()).

% record of gossip values for use by other modules
-record(values, {avg       = unknown :: avg(),  % average load
                 stddev    = unknown :: stddev(), % standard deviation of the load
                 size_ldr  = unknown :: size(), % estimated ring size: 1/(average of leader=1, others=0)
                 size_kr   = unknown :: size(), % estimated ring size based on average key ranges
                 min       = unknown :: min(), % minimum load
                 max       = unknown :: max(), % maximum load
                 triggered = 0 :: triggered(), % how often the trigger called since the node entered/created the round
                 msg_exch  = 0 :: msg_exch() % how often messages have been exchanged with other nodes
}).
-type(values() :: #values{}). %% @todo: mark as opaque (dialyzer currently crashes if set)

% internal record of gossip values used for the state
-record(values_internal, {avg       = unknown :: avg(), % average load
                          avg2      = unknown :: avg2(), % average of load^2
                          size_inv  = unknown :: size_inv(), % 1 / size
                          avg_kr    = unknown :: avg_kr(), % average key range (distance between nodes in the address space)
                          min       = unknown :: min(),
                          max       = unknown :: max(),
                          round     = 0 :: round()
}).
-type(values_internal() :: #values_internal{}). %% @todo: mark as opaque (dialyzer currently crashes if set)

% state of the gossip process
-record(state, {values             = #values_internal{} :: values_internal(),  % stored (internal) values
                initialized        = false :: boolean(), % load and range information from the local node have been integrated?
                triggered          = 0 :: triggered(), % how often the trigger called since the node entered/created the round
                msg_exch           = 0 :: msg_exch(), % how often messages have been exchanged with other nodes
                converge_avg_count = 0 :: converge_avg_count() % how often all values based on averages have changed less than epsilon percent
}).
-opaque(state() :: #state{}).

%%
%% Constructors
%%

%% @doc creates a new record holding values for (internal) use by gossip.erl
-spec new_internal() -> values_internal().
new_internal() -> #values_internal{}.

%% @doc creates a new record holding values for (internal) use by gossip.erl
-spec new_internal(avg(), avg2(), size_inv(), avg_kr(), min(), max(), round())
                    -> values_internal().
new_internal(Avg, Avg2, Size_inv, AvgKR, Min, Max, Round) ->
    #values_internal{avg=Avg, avg2=Avg2, size_inv=Size_inv, avg_kr=AvgKR,
                     min=Min, max=Max, round=Round}.

%% @doc creates a new record holding the (internal) state of the gossip module
%%      (only use in gossip.erl!)
-spec new_state() -> state().
new_state() -> #state{}.

%% @doc creates a new record holding the (internal) state of the gossip module
%%      (only use in gossip.erl!)
-spec new_state(values_internal()) -> state().
new_state(Values) when is_record(Values, values_internal) ->
    #state{values=Values, initialized=false}.

%%
%% Getters
%%

%% @doc Gets information from a values, values_internal or state record.
%%      Allowed keys include:
%%      <ul>
%%        <li>avgLoad = average load,</li>
%%        <li>stddev = standard deviation of the load among the nodes,</li>
%%        <li>size_ldr = leader-based size estimate,</li>
%%        <li>size_kr = size estimate based on average key range calculation,</li>
%%        <li>size = size_kr if not unknown, otherwise size_ldr,</li>
%%        <li>minLoad = minimum load,</li>
%%        <li>maxLoad = maximum load,</li>
%%        <li>triggered = how often the trigger called since the node entered/created the round,</li>
%%        <li>msg_exch = how often messages have been exchanged with other nodes,</li>
%%        <li>avgLoad2 = average of load^2,</li>
%%        <li>size_inv = 1 / size_ldr,</li>
%%        <li>avg_kr = average key range (distance between nodes in the address space),</li>
%%        <li>round = gossip round the node is in,</li>
%%        <li>values = internal values stored in a gossip state,</li>
%%        <li>initialized = load and range information from the local node have been integrated?,</li>
%%        <li>converge_avg_count = how often all values based on averages have changed less than epsilon percent</li>
%%      </ul>
%%      See type spec for details on which keys are allowed on which records.
-spec get(values() | values_internal() | state(), avgLoad) -> avg();
         (values()                              , stddev) -> stddev();
         (values()                              , size_ldr) -> size();
         (values()                              , size_kr) -> size();
         (values()                              , size) -> size();
         (values() | values_internal() | state(), minLoad) -> min();
         (values() | values_internal() | state(), maxLoad) -> max();
         (values() | state()                    , triggered) -> triggered();
         (values() | state()                    , msg_exch) -> msg_exch();
         (values_internal() | state()           , avgLoad2) -> avg2();
         (values_internal() | state()           , size_inv) -> size_inv();
         (values_internal() | state()           , avg_kr) -> avg_kr();
         (values_internal() | state()           , round) -> round();
         (state()                               , values) -> values_internal();
         (state()                               , initialized) -> boolean();
         (state()                               , converge_avg_count) -> converge_avg_count().
get(#values{avg=Avg, stddev=Stddev, size_ldr=Size_ldr, size_kr=Size_kr, min=Min,
            max=Max, triggered=Triggered, msg_exch=MessageExchanges}, Key) ->
    case Key of
        avgLoad -> Avg;
        stddev -> Stddev;
        size_ldr -> Size_ldr;
        size_kr -> Size_kr;
        minLoad -> Min;
        maxLoad -> Max;
        triggered -> Triggered;
        msg_exch -> MessageExchanges;
        % convenience getters:
        size ->
            % favor key range based calculations over leader-based
            case Size_kr of
                unknown -> Size_ldr;
                _       -> Size_kr
            end
    end;
get(#values_internal{avg=Avg, avg2=Avg2, size_inv=Size_inv, avg_kr=AvgKR,
                     min=Min, max=Max, round=Round}, Key) ->
    case Key of
        avgLoad -> Avg;
        avgLoad2 -> Avg2;
        size_inv -> Size_inv;
        avg_kr -> AvgKR;
        minLoad -> Min;
        maxLoad -> Max;
        round -> Round
    end;
get(#state{values=InternalValues, initialized=Initialized, triggered=Triggered,
           msg_exch=MessageExchanges, converge_avg_count=ConvergeAvgCount}, Key) ->
    case Key of
        values -> InternalValues;
        initialized -> Initialized;
        triggered -> Triggered;
        msg_exch -> MessageExchanges;
        converge_avg_count -> ConvergeAvgCount;
        % convenience getters:
        avgLoad -> get(InternalValues, avgLoad);
        minLoad -> get(InternalValues, minLoad);
        maxLoad -> get(InternalValues, maxLoad);
        avgLoad2 -> get(InternalValues, avgLoad2);
        size_inv -> get(InternalValues, size_inv);
        avg_kr -> get(InternalValues, avg_kr);
        round -> get(InternalValues, round)
    end.

%%
%% Setters
%%

%% @doc Sets information in a values_internal record.
%%      Allowed keys include:
%%      <ul>
%%        <li>avgLoad = average load,</li>
%%        <li>avgLoad2 = average of load^2,</li>
%%        <li>size_inv = 1 / size_ldr,</li>
%%        <li>avg_kr = average key range (distance between nodes in the address space),</li>
%%        <li>minLoad = minimum load,</li>
%%        <li>maxLoad = maximum load,</li>
%%        <li>round = gossip round the node is in,</li>
%%      </ul>
-spec set(values_internal() | state(), avgLoad, avg()) -> values_internal();
         (values_internal() | state(), minLoad, min()) -> values_internal();
         (values_internal() | state(), maxLoad, max()) -> values_internal();
         (values_internal() | state(), avgLoad2, avg2()) -> values_internal();
         (values_internal() | state(), size_inv, size_inv()) -> values_internal();
         (values_internal() | state(), avg_kr, avg_kr()) -> values_internal();
         (values_internal() | state(), round, round()) -> values_internal().
set(InternalValues, Key, Value) when is_record(InternalValues, values_internal) ->
    case Key of
        avgLoad -> InternalValues#values_internal{avg = Value};
        avgLoad2 -> InternalValues#values_internal{avg2 = Value};
        size_inv -> InternalValues#values_internal{size_inv = Value};
        avg_kr -> InternalValues#values_internal{avg_kr = Value};
        minLoad -> InternalValues#values_internal{min = Value};
        maxLoad -> InternalValues#values_internal{max = Value};
        round -> InternalValues#values_internal{round = Value}
    end;
set(State, Key, Value) when is_record(State, state) ->
    State#state{values=set(State#state.values, Key, Value)}.

%$ @doc Sets the values of a gossip state.
-spec set_values(state(), values_internal()) -> state().
set_values(State, Values)
  when (is_record(State, state) andalso is_record(Values, values_internal)) ->
    State#state{values=Values}.

%$ @doc Sets that local load and key range information have been integrated into
%%      the values of a gossip state.
-spec set_initialized(state()) -> state().
set_initialized(State) when is_record(State, state) ->
    State#state{initialized=true}.

%% @doc Resets how often the trigger called the module to 0.
-spec reset_triggered(state()) -> state().
reset_triggered(State) when is_record(State, state) ->
    State#state{triggered=0}.

%% @doc Increases how often the trigger called the module by 1.
-spec inc_triggered(state()) -> state().
inc_triggered(State) when is_record(State, state) ->
    State#state{triggered=(get(State, triggered) + 1)}.

%% @doc Resets how many information exchanges have been performed to 0.
-spec reset_msg_exch(state()) -> state().
reset_msg_exch(State) when is_record(State, state) ->
    State#state{msg_exch=0}.

%% @doc Increases how many information exchanges have been performed by 1.
-spec inc_msg_exch(state()) -> state().
inc_msg_exch(State) when is_record(State, state) ->
    State#state{msg_exch=(get(State, msg_exch) + 1)}.

%% @doc Resets how many information exchanges have been performed to 0.
-spec reset_converge_avg_count(state()) -> state().
reset_converge_avg_count(State) when is_record(State, state) ->
    State#state{converge_avg_count=0}.

%% @doc Increases how many information exchanges have been performed by 1.
-spec inc_converge_avg_count(state()) -> state().
inc_converge_avg_count(State) when is_record(State, state) ->
    State#state{converge_avg_count=(get(State, converge_avg_count) + 1)}.

%%
%% Miscellaneous
%%

%% @doc Extracts and calculates the size field from the internal record of
%%      values.
-spec calc_size_ldr(values_internal() | state()) -> size().
calc_size_ldr(State) when is_record(State, values_internal) ->
    Size_inv = get(State, size_inv),
    if
        Size_inv =< 0        -> 1.0;
        Size_inv =:= unknown -> unknown;
        true                 -> 1.0 / Size_inv
    end;
calc_size_ldr(State) when is_record(State, state) ->
    calc_size_ldr(get(State, values)).

%% @doc Extracts and calculates the size_kr field from the internal record of
%%      values.
-spec calc_size_kr(values_internal() | state()) -> size().
calc_size_kr(State) when is_record(State, values_internal) ->
    AvgKR = get(State, avg_kr),
    try
        if
            AvgKR =< 0        -> 1.0;
            AvgKR =:= unknown -> unknown;
            true              -> ?RT:n() / AvgKR
        end
    catch % ?RT:n() might throw
        throw:not_supported -> unknown
    end;
calc_size_kr(State) when is_record(State, state) ->
    calc_size_kr(get(State, values)).

%% @doc Extracts and calculates the standard deviation from the internal record
%%      of values.
-spec calc_stddev(values_internal() | state()) -> stddev().
calc_stddev(State) when is_record(State, values_internal) ->
    Avg = get(State, avgLoad),
    Avg2 = get(State, avgLoad2),
    case (Avg =:= unknown) orelse (Avg2 =:= unknown) of
        true -> unknown;
        false ->
            Tmp = Avg2 - (Avg * Avg),
            case (Tmp >= 0) of
                true  -> math:sqrt(Tmp);
                false -> unknown
            end
    end;
calc_stddev(State) when is_record(State, state) ->
    calc_stddev(get(State, values)).

%% @doc Converts the internal value record to the external one.
-spec conv_state_to_extval(state()) -> values().
conv_state_to_extval(State) when is_record(State, state) ->
    #values{avg       = get(State, avgLoad),
            stddev    = calc_stddev(State),
            size_ldr  = calc_size_ldr(State),
            size_kr   = calc_size_kr(State),
            min       = get(State, minLoad),
            max       = get(State, maxLoad),
            triggered = get(State, triggered),
            msg_exch  = get(State, msg_exch)}.
