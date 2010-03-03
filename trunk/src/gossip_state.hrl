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

-author('kruber@zib.de').
-vsn('$Id$ ').

% gossip types
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
                have_load          = false :: bool(), % load information from the local node has been integrated?
                have_kr            = false :: bool(), % key range information from the local node has been integrated?
                triggered          = 0 :: triggered(), % how often the trigger called since the node entered/created the round
                msg_exch           = 0 :: msg_exch(), % how often messages have been exchanged with other nodes
                converge_avg_count = 0 :: converge_avg_count() % how often all values based on averages have changed less than epsilon percent
}).
-type(state() :: #state{}).
