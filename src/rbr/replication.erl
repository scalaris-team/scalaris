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

-export([get_keys/1]).
-export([write_values_for_keys/2]).
-export([quorum_accepted/1, quorum_denied/1]).
-export([collect_read_value/2, collect_read_value/3]).
-export([get_read_value/1]).

get_keys(Key) ->
    ?RT:get_replica_keys(Key).

write_values_for_keys(Keys, WriteValue) ->
    [{K, WriteValue} || K <- Keys].

quorum_accepted(AccCount) ->
    R = config:read(replication_factor),
    quorum:majority_for_accept(R) =< AccCount.

quorum_denied(DeniedCount) ->
    R = config:read(replication_factor),
    quorum:majority_for_deny(R) =< DeniedCount.

collect_read_value(NewValue, _DataType) ->
    NewValue.
collect_read_value(Collected, NewValue, DataType) ->
    case NewValue of
        Collected -> Collected;
        DifferingVal ->
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

get_read_value(ReadValue) -> ReadValue.