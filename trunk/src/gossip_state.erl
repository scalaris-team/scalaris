%  Copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : gossip_state.erl
%%% Author  : Nico Kruber <kruber@zib.de>
%%% Description : Methods for working with the gossip state.
%%%
%%% Created : 25 Feb 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @author Nico Kruber <kruber@zib.de>
%% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(gossip_state).

-author('kruber@zib.de').
-vsn('$Id$ ').

%%
%% Exported Functions
%%

%%
%% Externally usable exports (some also work on internal states)
%%

% constructors
-export([new/0, new/7]).

% getters
-export([get_avgLoad/1,
		 get_stddev/1,
		 get_size/1,
		 get_size_kr/1,
		 get_minLoad/1,
		 get_maxLoad/1,
		 get_triggered/1,
		 get_msg_exch/1]).

% setters
-export([set_avgLoad/2,
		 set_stddev/2,
		 set_size/2,
		 set_minLoad/2,
		 set_maxLoad/2]).

%%
%% Exports only meant for use by gossip.erl
%%
% constructors
-export([new_state/0, new_state/1,
		 new_internal/0, new_internal/7]).

% getters
-export([get_avgLoad2/1,
		 get_size_n/1,
		 get_round/1,
		 get_values/1,
		 is_initialized/1,
		 get_converge_avg_count/1]).

% setters
-export([set_avgLoad2/2,
		 set_size_n/2,
		 set_size_kr/2,
		 set_round/2,
		 set_values/2,
		 set_initialized/1,
		 reset_triggered/1,
		 inc_triggered/1,
		 reset_msg_exch/1,
		 inc_msg_exch/1,
		 reset_converge_avg_count/1,
		 inc_converge_avg_count/1]).

-export([conv_state_to_extval/1,
		 calc_size/1,
		 calc_size_kr/1,
		 calc_stddev/1]).

%%
%% Gossip types
%%
-type(avg() :: number() | unknown).
-type(avg2() :: number() | unknown).
-type(stddev() :: float() | unknown).
-type(size_n() :: float() | unknown).
-type(size() :: number() | unknown).
-type(size_kr() :: number() | unknown).
-type(min() :: non_neg_integer() | unknown).
-type(max() :: non_neg_integer() | unknown).
-type(round() :: non_neg_integer()).
-type(triggered() :: non_neg_integer()).
-type(msg_exch() :: non_neg_integer()).
-type(converge_avg_count() :: non_neg_integer()).

% record of gossip values for use by other modules
-record(values, {avg       = unknown :: avg(),  % average load
                 stddev    = unknown :: stddev(), % standard deviation of the load
                 size      = unknown :: size(), % estimated ring size
                 size_kr   = unknown :: size_kr(), % estimated ring size
                 min       = unknown :: min(), % minimum load
                 max       = unknown :: max(), % maximum load
                 triggered = 0 :: triggered(), % how often the trigger called since the node entered/created the round
                 msg_exch  = 0 :: msg_exch() % how often messages have been exchanged with other nodes
}).
-type(values() :: #values{}).

% internal record of gossip values used for the state
-record(values_internal, {avg       = unknown :: avg(), % average load
                          avg2      = unknown :: avg2(), % average of load^2
                          size_n    = unknown :: size_n(), % 1 / size
                          size_kr   = unknown :: size_kr(), % size based on key range (distance between nodes) in the address space
                          min       = unknown :: min(),
                          max       = unknown :: max(),
						  round     = 0 :: round()
}).
-type(values_internal() :: #values_internal{}).

% state of the gossip process
-record(state, {values             = #values_internal{} :: values_internal(),  % stored (internal) values
                initialized        = false :: bool(), % load and range information from the local node have been integrated?
                triggered          = 0 :: triggered(), % how often the trigger called since the node entered/created the round
                msg_exch           = 0 :: msg_exch(), % how often messages have been exchanged with other nodes
                converge_avg_count = 0 :: converge_avg_count() % how often all values based on averages have changed less than epsilon percent
}).
-type(state() :: #state{}).

%%
%% Constructors
%%

%% @doc creates a new record holding values for use by external modules
-spec new() -> values().
new() -> #values{}.

%% @doc creates a new record holding values for use by external modules
-spec new(avg(), stddev(), size(), min(), max(), triggered(), msg_exch()) -> values().
new(Avg, Stddev, Size, Min, Max, Triggered, MessageExchanges) ->
	#values{avg=Avg, stddev=Stddev, size=Size, min=Min, max=Max,
            triggered=Triggered, msg_exch=MessageExchanges}.

%% @doc creates a new record holding values for (internal) use by gossip.erl
-spec new_internal() -> values_internal().
new_internal() -> #values_internal{}.

%% @doc creates a new record holding values for (internal) use by gossip.erl
-spec new_internal(avg(), avg2(), size_n(), size_kr(), min(), max(), round())
                    -> values_internal().
new_internal(Avg, Avg2, Size_n, Size_kr, Min, Max, Round) ->
	#values_internal{avg=Avg, avg2=Avg2, size_n=Size_n, size_kr=Size_kr,
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

%% @doc Gets the average load from a gossip state's values.
-spec get_avgLoad(values() | values_internal() | state) -> avg().
get_avgLoad(#values{avg=Avg}) -> Avg;
get_avgLoad(#values_internal{avg=Avg}) -> Avg;
get_avgLoad(#state{values=Values}) -> get_avgLoad(Values).

%% @doc Gets the standard deviation of the loads load from a gossip state's values.
-spec get_stddev(values()) -> stddev().
get_stddev(#values{stddev=Stddev}) -> Stddev.

%% @doc Gets the size from a gossip state's values.
-spec get_size(values()) -> size().
get_size(#values{size=Size}) -> Size.

%% @doc Gets the minimum load from a gossip state's values.
-spec get_minLoad(values() | values_internal() | state) -> min().
get_minLoad(#values{min=Min}) -> Min;
get_minLoad(#values_internal{min=Min}) -> Min;
get_minLoad(#state{values=Values}) -> get_minLoad(Values).

%% @doc Gets the maximum load from a gossip state's values.
-spec get_maxLoad(values() | values_internal() | state) -> max().
get_maxLoad(#values{max=Max}) -> Max;
get_maxLoad(#values_internal{max=Max}) -> Max;
get_maxLoad(#state{values=Values}) -> get_maxLoad(Values).

%% @doc Gets the average load from a gossip state's values.
-spec get_avgLoad2(values_internal() | state) -> avg2().
get_avgLoad2(#values_internal{avg2=Avg2}) -> Avg2;
get_avgLoad2(#state{values=Values}) -> get_avgLoad2(Values).

%% @doc Gets the 1/size value from a gossip state's values.
-spec get_size_n(values_internal() | state) -> size_n().
get_size_n(#values_internal{size_n=Size_n}) -> Size_n;
get_size_n(#state{values=Values}) -> get_size_n(Values).

%% @doc Gets the size based on key range from a gossip state's values.
-spec get_size_kr(values_internal() | state) -> size_kr().
get_size_kr(#values_internal{size_kr=Size_kr}) -> Size_kr;
get_size_kr(#state{values=Values}) -> get_size_kr(Values).

%% @doc Gets the round from a gossip state's values.
-spec get_round(values_internal() | state) -> round().
get_round(#values_internal{round=Round}) -> Round;
get_round(#state{values=Values}) -> get_round(Values).

%% @doc Gets how often the trigger called the module.
-spec get_triggered(values() | state()) -> triggered().
get_triggered(#values{triggered=Triggered}) -> Triggered;
get_triggered(#state{triggered=Triggered}) -> Triggered.

%% @doc Gets how many information exchanges have been performed to create a
%%      gossip state.
-spec get_msg_exch(values() | state()) -> msg_exch().
get_msg_exch(#values{msg_exch=MessageExchanges}) -> MessageExchanges;
get_msg_exch(#state{msg_exch=MessageExchanges}) -> MessageExchanges.

%$ @doc Gets the values a gossip state stores.
-spec get_values(state()) -> values_internal().
get_values(#state{values=Values}) -> Values.

%$ @doc Checks whether local load and key range information have been integrated
%%      into the values of a gossip state.
-spec is_initialized(state()) -> bool().
is_initialized(#state{initialized=Initialized}) -> Initialized.

%$ @doc Returns how often the changes in all average-based values have been less
%%      than epsilon percent in the given gossip state.
-spec get_converge_avg_count(state()) -> converge_avg_count().
get_converge_avg_count(#state{converge_avg_count=ConvergeAvgCount}) -> ConvergeAvgCount.

%%
%% Setters
%%

%% @doc Sets the average load field of a values or state type.
-spec set_avgLoad(values(), avg()) -> values()
                ; (values_internal(), avg()) -> values_internal()
                ; (state(), avg()) -> state().
set_avgLoad(Values, Avg) when is_record(Values, values) -> Values#values{avg=Avg};
set_avgLoad(Values, Avg) when is_record(Values, values_internal) -> Values#values_internal{avg=Avg};
set_avgLoad(State, Avg) when is_record(State, state) -> State#state{values=set_avgLoad(State#state.values, Avg)}.

%% @doc Sets the standard deviation field of a values type.
-spec set_stddev(values(), stddev()) -> values().
set_stddev(Values, Stddev) when is_record(Values, values) -> Values#values{stddev=Stddev}.

%% @doc Sets the size field of a values type.
-spec set_size(values(), size()) -> values().
set_size(Values, Size) when is_record(Values, values) -> Values#values{size=Size}.

%% @doc Sets the minimum load field of a values or state type.
-spec set_minLoad(values(), min()) -> values()
                ; (values_internal(), min()) -> values_internal()
                ; (state(), min()) -> state().
set_minLoad(Values, Min) when is_record(Values, values) -> Values#values{min=Min};
set_minLoad(Values, Min) when is_record(Values, values_internal) -> Values#values_internal{min=Min};
set_minLoad(State, Min) when is_record(State, state) -> State#state{values=set_minLoad(State#state.values, Min)}.

%% @doc Sets the maximum load field of a values or state type.
-spec set_maxLoad(values(), max()) -> values()
                ; (values_internal(), max()) -> values_internal()
                ; (state(), max()) -> state().
set_maxLoad(Values, Max) when is_record(Values, values) -> Values#values{max=Max};
set_maxLoad(Values, Max) when is_record(Values, values_internal) -> Values#values_internal{max=Max};
set_maxLoad(State, Max) when is_record(State, state) -> State#state{values=set_maxLoad(State#state.values, Max)}.

%% @doc Sets the average square load field of a values or state type.
-spec set_avgLoad2(values_internal(), avg2()) -> values_internal()
                 ; (state(), avg2()) -> state().
set_avgLoad2(Values, Avg2) when is_record(Values, values_internal) -> Values#values_internal{avg2=Avg2};
set_avgLoad2(State, Avg2) when is_record(State, state) -> State#state{values=set_avgLoad2(State#state.values, Avg2)}.

%% @doc Sets the average size_n (1/size) field of a values or state type.
-spec set_size_n(values_internal(), size_n()) -> values_internal()
               ; (state(), size_n()) -> state().
set_size_n(Values, Size_n) when is_record(Values, values_internal) -> Values#values_internal{size_n=Size_n};
set_size_n(State, Size_n) when is_record(State, state) -> State#state{values=set_size_n(State#state.values, Size_n)}.

%% @doc Sets the average size (based on key ranges) field of a values or state type.
-spec set_size_kr(values_internal(), size_kr()) -> values_internal()
               ; (state(), size_kr()) -> state().
set_size_kr(Values, Size_kr) when is_record(Values, values_internal) -> Values#values_internal{size_kr=Size_kr};
set_size_kr(State, Size_kr) when is_record(State, state) -> State#state{values=set_size_kr(State#state.values, Size_kr)}.

%% @doc Sets the round field of a values or state type.
-spec set_round(values_internal(), round()) -> values_internal()
               ; (state(), round()) -> state().
set_round(Values, Round) when is_record(Values, values_internal) -> Values#values_internal{round=Round};
set_round(State, Round) when is_record(State, state) -> State#state{values=set_round(State#state.values, Round)}.

%$ @doc Sets the values of a gossip state.
-spec set_values(state(), values_internal()) -> state().
set_values(State, Values)
  when (is_record(State, state) and is_record(Values, values_internal)) ->
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
	State#state{triggered=(get_triggered(State) + 1)}.

%% @doc Resets how many information exchanges have been performed to 0.
-spec reset_msg_exch(state()) -> state().
reset_msg_exch(State) when is_record(State, state) ->
	State#state{msg_exch=0}.

%% @doc Increases how many information exchanges have been performed by 1.
-spec inc_msg_exch(state()) -> state().
inc_msg_exch(State) when is_record(State, state) ->
	State#state{msg_exch=(get_msg_exch(State) + 1)}.

%% @doc Resets how many information exchanges have been performed to 0.
-spec reset_converge_avg_count(state()) -> state().
reset_converge_avg_count(State) when is_record(State, state) ->
	State#state{converge_avg_count=0}.

%% @doc Increases how many information exchanges have been performed by 1.
-spec inc_converge_avg_count(state()) -> state().
inc_converge_avg_count(State) when is_record(State, state) ->
	State#state{converge_avg_count=(get_converge_avg_count(State) + 1)}.

%%
%% Miscellaneous
%%

%% @doc extracts and calculates the size field from the internal record of
%%      values
-spec calc_size(values_internal() | state) -> size().
calc_size(State) when is_record(State, values_internal) ->
	Size_n = gossip_state:get_size_n(State),
	if
		Size_n =< 0        -> 1.0;
		Size_n =:= unknown -> unknown;
		true               -> 1.0 / Size_n
	end;
calc_size(State) when is_record(State, state) ->
	calc_size(get_values(State)).

get_addr_size() ->
	rt_simple:n().

%% @doc extracts and calculates the size_kr field from the internal record of
%%      values
-spec calc_size_kr(values_internal() | state) -> size().
calc_size_kr(State) when is_record(State, values_internal) ->
	Size_kr = gossip_state:get_size_kr(State),
	if
		Size_kr =< 0        -> 1.0;
		Size_kr =:= unknown -> unknown;
		true                -> get_addr_size() / Size_kr
	end;
calc_size_kr(State) when is_record(State, state) ->
	calc_size_kr(get_values(State)).

%% @doc extracts and calculates the standard deviation from the internal record
%%      of values
-spec calc_stddev(values_internal()) -> avg().
calc_stddev(State) when is_record(State, values_internal) ->
	Avg = gossip_state:get_avgLoad(State),
	Avg2 = gossip_state:get_avgLoad2(State),
	case (Avg =:= unknown) or (Avg2 =:= unknown) of
		true -> unknown;
		false -> math:sqrt(Avg2 - (Avg * Avg))
	end;
calc_stddev(State) when is_record(State, state) ->
	calc_stddev(get_values(State)).

%% @doc converts the internal value record to the external one
-spec conv_state_to_extval(state()) -> values().
conv_state_to_extval(State) when is_record(State, state) ->
	#values{avg       = get_avgLoad(State),
            stddev    = calc_stddev(State),
            size      = calc_size(State),
            size_kr   = calc_size_kr(State),
            min       = get_minLoad(State),
            max       = get_maxLoad(State),
            triggered = get_triggered(State),
            msg_exch  = get_msg_exch(State)}.
