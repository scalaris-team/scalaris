% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Unit tests for src/db_ets.erl.
%%% @end
%% @version $Id$
-module(db_ets_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-define(TEST_DB, db_ets).

-include("db_SUITE.hrl").

%all() -> tests_avail() ++ [get_chunk].
all() -> [get_chunk].

%% @doc Specify how often a read/write suite can be executed in order not to
%%      hit a timeout (depending on the speed of the DB implementation).
-spec max_rw_tests_per_suite() -> pos_integer().
max_rw_tests_per_suite() ->
    10000.


get_chunk(Config) ->
    tester_get_chunk(Config).

-spec prop_get_chunk(Keys::list(?RT:key()), Begin::?RT:key(), End::?RT:key()) -> true.
prop_get_chunk(Keys2, Begin, End) ->
    case Begin /= End of
        true ->
            Keys = lists:usort(Keys2),
            DB = db_ets:new(),
            DB2 = fill_db(DB, Keys),
            ChunkSize = 5,
            Interval = intervals:new('[', Begin, End, ')'),
            {Next, Chunk} = db_ets:get_chunk(DB2, Interval, ChunkSize),
            db_ets:close(DB2),
            case length(Chunk) /= length(lists:usort(Chunk)) of
                true ->
                    ct:pal("chunk contains duplicates ~p ~p ~p ~p", [Chunk, Keys, Begin, End]),
                    false;
                false ->
                    ExpectedChunkSize = util:min(count_keys_in_range(Keys, Interval),
                                            ChunkSize),
                    case ExpectedChunkSize /= length(Chunk) of
                        true ->
                            ct:pal("chunk has wrong size ~p ~p ~p ~p", [Chunk, Keys, Begin, End]),
                            false;
                        false ->
                            case check_chunk_entries_are_in_interval(Chunk, Interval) of
                                false ->
                                    ct:pal("check_chunk_entries_are_in_interval failed ~p ~p ~p ~p",
                                           [Chunk, Keys, Begin, End]),
                                    false;
                                true ->
                                    true
                            end
                    end
            end;
        false ->
            true
    end.

tester_get_chunk(_Config) ->
    tester:test(?MODULE, prop_get_chunk, 3, rw_suite_runs(10)).

fill_db(DB, []) ->
    DB;
fill_db(DB, [Key | Rest]) ->
    fill_db(db_ets:write(DB, Key, "Value", 1), Rest).

check_chunk_entries_are_in_interval(Entries, Interval) ->
    lists:all(fun (Entry) ->
                      intervals:in(db_entry:get_key(Entry), Interval)
              end, Entries).

count_keys_in_range(Keys, Interval) ->
    length(lists:filter(fun(Key) ->
                                intervals:in(Key, Interval)
                        end, Keys)).
