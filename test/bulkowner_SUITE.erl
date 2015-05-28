%  @copyright 2008, 2011, 2012 Zuse Institute Berlin

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
%% @doc    Unit tests for src/bulkowner.erl
%% @end
%% @version $Id$
-module(bulkowner_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() -> [tester_count].

suite() -> [ {timetrap, {seconds, 10}} ].

init_per_suite(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    Config.

end_per_suite(_Config) ->
    ok.

tester_count(_Config) ->
    tester:test(?MODULE, count, 0, 1000),
    ?expect_no_message().

-spec count() -> ok.
count() ->
    Entries = [begin
                   RandInt = uid:get_pids_uid(),
                   db_entry:new(
                     ?RT:hash_key(erlang:integer_to_list(RandInt)),
                     X, RandInt)
               end || X <- lists:seq(1, 16)],
    db_generator:insert_db(Entries),
    Total = reduce(Entries),
    Keys = [db_entry:get_key(Entry) || Entry <- Entries],
    Id = uid:get_global_uid(),
    I = intervals:from_elements(Keys),
    bulkowner:issue_bulk_owner(Id, I, {bulk_read_entry, comm:this()}),
    ?equals(collect(Id, 0, Total, 0), Total),
    db_generator:remove_keys(Keys),
    ok.

collect(Id, Sum, ExpSum, Msgs) ->
%%     if Msgs =< 0 -> ok;
%%        true      -> ct:pal("sum after ~p msgs: ~p~n", [Msgs, Sum])
%%     end,
    if
        Msgs =:= 4 -> Sum; % 4 nodes -> max 4 messages
        Sum =:= ExpSum -> Sum;
        true ->
            receive
                {bulkowner, reply, Id, {bulk_read_entry_response, _NowDone, Data}} ->
                    collect(Id, Sum + reduce(Data), ExpSum, Msgs + 1)
            end
    end.

-spec reduce(db_dht:db_as_list()) -> integer().
reduce(Entries) ->
    lists:foldl(fun(E, Acc) -> db_entry:get_value(E) + Acc end, 0, Entries).
