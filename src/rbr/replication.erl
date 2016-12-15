% @copyright 2014-2016 Zuse Institute Berlin,

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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    Implementation of replication as redundancy strategy
%% @end
%% @version $Id$
-module(replication).
-author('skrzypczak@zib.de').
-vsn('$Id:$ ').

-include("scalaris.hrl").
-include("client_types.hrl").

-export([get_keys/1]).
-export([write_values_for_keys/2]).
-export([quorum_accepted/1, quorum_denied/1]).
-export([collect_read_value/3]).
-export([collect_newer_read_value/3]).
-export([collect_older_read_value/3]).
-export([get_read_value/2]).
-export([skip_write_through/1]).

%% @doc Returns the replicas of the given key.
-spec get_keys(?RT:key()) -> [?RT:key()].
get_keys(Key) ->
    ?RT:get_replica_keys(Key).

%% @doc Returns a list of values (based on WriteValue) wich should 
%% be written to the Keys passed as argument 
-spec write_values_for_keys([?RT:key()], client_value()) -> [client_value()].
write_values_for_keys(Keys, WriteValue) ->
    [WriteValue || _K <- Keys].

%% @doc Returns if enough acks for majority have been collected.
-spec quorum_accepted(integer()) -> boolean().
quorum_accepted(AccCount) ->
    R = config:read(replication_factor),
    quorum:majority_for_accept(R) =< AccCount.

%% @doc Returns if enough denies for majority have been collected.
-spec quorum_denied(integer()) -> boolean().
quorum_denied(DeniedCount) ->
    R = config:read(replication_factor),
    quorum:majority_for_deny(R) =< DeniedCount.

%% @doc Handles a new read reply of a round newer than the newest round
%% seen in the past.
-spec collect_newer_read_value(client_value(), client_value(), module()) -> client_value().
collect_newer_read_value(_Collected, NewValue, _DataType) ->
    NewValue.

%% @doc Handles a new read reply of a previous round.
-spec collect_older_read_value(client_value(), client_value(), module()) -> client_value().
collect_older_read_value(Collected, _NewValue, _DataType) ->
    Collected.

%% @doc Handles a new read reply of the current round.
-spec collect_read_value(client_value(), client_value(), module()) -> client_value().
collect_read_value(Collected, NewValue, DataType) ->
    case NewValue of
        Collected -> Collected;
        DifferingVal ->
            ct:log("Consistency based on value comparison: ~p <-> ~p (~p)",
                   [Collected, NewValue, DataType]),
            %% if this happens, consistency is probably broken by
            %% too weak (wrong) content checks...?

            %% unfortunately the following statement is not always
            %% true: As we also update parts of the value (set
            %% write lock) it can happen that the write lock is
            %% set on a quorum inluding an outdated replica, which
            %% can store a different value than the replicas
            %% up-to-date. This is not a consistency issue, as the
            %% tx write the new value to the entry.  But what in
            %% case of rollback? We cannot safely restore the
            %% latest value then by just removing the write_lock,
            %% but would have to actively write the former value!

            %% We use a user defined value selector to chose which
            %% value is newer. If a data type (leases for example)
            %% only allows consistent values at any time, this
            %% callback can check for violation by checking
            %% equality of all replicas. If a data type allows
            %% partial write access (like kv_on_cseq for its
            %% locks) we need an ordering of the values, as it
            %% might be the case, that a writelock which was set
            %% on an outdated replica had to be rolled back. Then
            %% replicas with newest paxos time stamp may exist
            %% with differing value and we have to chose that one
            %% with the highest version number for a quorum read.
            MaxFunModule =
                case erlang:function_exported(DataType, max, 2) of
                    true  -> DataType;
                        %% this datatype has not defined their own max fun
                        %% therefore use the default util:max
                     _     -> util
                end,
            MaxFunModule:max(Collected, DifferingVal)
    end.

%% @doc Returns the read value base on all previously collected read replies
-spec get_read_value(client_value(), prbr:read_filter()) -> client_value().
get_read_value(ReadValue, _ReadFilter) -> ReadValue.

%% @doc Decide whether to skip a write through based on the read value
-spec skip_write_through(client_value()) -> boolean().
skip_write_through(_ReadValue) -> false.

