%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc Database based on db_gb_trees that verifies the correct use of the DB
%%      interface.
%% @end
%% @version $Id$
-module(db_verify_use).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_beh).

-type(db()::{gb_tree(), Counter::non_neg_integer()}).

% Note: must include db_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("db_beh.hrl").

-include("db_common.hrl").

%% -define(TRACE0(Fun), io:format("~w: ~w()~n", [self(), Fun])).
%% -define(TRACE1(Fun, Par1), io:format("~w: ~w(~w)~n", [self(), Fun, Par1])).
%% -define(TRACE2(Fun, Par1, Par2), io:format("~w: ~w(~w, ~w)~n", [self(), Fun, Par1, Par2])).
%% -define(TRACE3(Fun, Par1, Par2, Par3), io:format("~w: ~w(~w, ~w, ~w)~n", [self(), Fun, Par1, Par2, Par3])).
-define(TRACE0(Fun), Fun, ok).
-define(TRACE1(Fun, Par1), Fun, Par1, ok).
-define(TRACE2(Fun, Par1, Par2), Fun, Par1, Par2, ok).
-define(TRACE3(Fun, Par1, Par2, Par3), Fun, Par1, Par2, Par3, ok).
-spec verify_counter(Counter::non_neg_integer()) -> ok.
verify_counter(Counter) ->
    case erlang:get(?MODULE) of
        Counter   -> ok;
        undefined -> erlang:error({counter_undefined, Counter});
        _         -> erlang:error({counter_mismatch, Counter, erlang:get(?MODULE)})
    end.

-spec update_counter(Counter::non_neg_integer()) -> NewCounter::non_neg_integer().
update_counter(Counter) ->
    NewCounter = Counter + 1,
    erlang:put(?MODULE, NewCounter),
    NewCounter.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc initializes a new database
new(NodeId) ->
    ?TRACE1(new, NodeId),
    case erlang:get(?MODULE) of
        undefined -> ok;
        _         -> erlang:error({counter_defined, erlang:get(?MODULE)})
    end,
    erlang:put(?MODULE, 0),
    {gb_trees:empty(), 0}.

%% delete DB (missing function)
close(DB) ->
    ?TRACE1(close, DB),
    erlang:erase(?MODULE),
    ok.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned.
get_entry({DB, Counter} = DB_, Key) ->
    ?TRACE2(get_entry, DB_, Key),
    verify_counter(Counter),
    case gb_trees:lookup(Key, DB) of
        {value, Entry} -> Entry;
        none           -> db_entry:new(Key)
    end.

%% @doc Gets an entry from the DB. If there is no entry with the given key,
%%      an empty entry will be returned. The first component of the result
%%      tuple states whether the value really exists in the DB.
get_entry2({DB, Counter} = DB_, Key) ->
    ?TRACE2(get_entry2, DB_, Key),
    verify_counter(Counter),
    case gb_trees:lookup(Key, DB) of
        {value, Entry} -> {true, Entry};
        none           -> {false, db_entry:new(Key)}
    end.

%% @doc Inserts a complete entry into the DB.
set_entry({DB, Counter} = DB_, Entry) ->
    ?TRACE2(set_entry, DB_, Entry),
    verify_counter(Counter),
    {gb_trees:enter(db_entry:get_key(Entry), Entry, DB), update_counter(Counter)}.

%% @doc Updates an existing (!) entry in the DB.
update_entry({DB, Counter} = DB_, Entry) ->
    ?TRACE2(update_entry, DB_, Entry),
    verify_counter(Counter),
    {gb_trees:update(db_entry:get_key(Entry), Entry, DB), update_counter(Counter)}.

%% @doc Removes all values with the given entry's key from the DB.
delete_entry({DB, Counter} = DB_, Entry) ->
    ?TRACE2(delete_entry, DB_, Entry),
    verify_counter(Counter),
    {gb_trees:delete_any(db_entry:get_key(Entry), DB), update_counter(Counter)}.

%% @doc Returns the number of stored keys.
get_load({DB, Counter} = DB_) ->
    ?TRACE1(get_load, DB_),
    verify_counter(Counter),
    gb_trees:size(DB).

%% @doc Adds all db_entry objects in the Data list.
add_data({DB, Counter} = DB_, Data) ->
    ?TRACE2(add_data, DB_, Data),
    verify_counter(Counter),
    {lists:foldl(fun(DBEntry, Tree) -> set_entry(Tree, DBEntry) end, DB, Data), update_counter(Counter)}.

%% @doc Splits the database into a database (first element) which contains all
%%      keys in MyNewInterval and a list of the other values (second element).
split_data({DB, Counter} = DB_, MyNewInterval) ->
    ?TRACE2(split_data, DB_, MyNewInterval),
    verify_counter(Counter),
    {MyList, HisList} =
        lists:partition(fun(DBEntry) ->
                                (not db_entry:is_empty(DBEntry)) andalso
                                    intervals:in(db_entry:get_key(DBEntry), MyNewInterval)
                        end,
                        gb_trees:values(DB)),
    % note: building [{Key, Val}] from MyList should be more memory efficient
    % than using gb_trees:to_list(DB) above and removing Key from the tuples in
    % HisList
    {{gb_trees:from_orddict([ {db_entry:get_key(DBEntry), DBEntry} ||
                                DBEntry <- MyList]), update_counter(Counter)}, HisList}.

%% @doc Get key/value pairs in the given range.
get_range_kv({DB, Counter} = DB_, Interval) ->
    ?TRACE2(get_range_kv, DB_, Interval),
    verify_counter(Counter),
    F = fun (_Key, DBEntry, Data) ->
                case (not db_entry:is_empty(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Get key/value/version triples of non-write-locked entries in the given range.
get_range_kvv({DB, Counter} = DB_, Interval) ->
    ?TRACE2(get_range_kvv, DB_, Interval),
    verify_counter(Counter),
    F = fun (_Key, DBEntry, Data) ->
                case (not db_entry:is_empty(DBEntry)) andalso
                         (not db_entry:get_writelock(DBEntry)) andalso
                         intervals:in(db_entry:get_key(DBEntry), Interval) of
                    true -> [{db_entry:get_key(DBEntry),
                              db_entry:get_value(DBEntry),
                              db_entry:get_version(DBEntry)} | Data];
                    _    -> Data
                end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Gets db_entry objects in the given range.
get_range_entry({DB, Counter} = DB_, Interval) ->
    ?TRACE2(get_range_entry, DB_, Interval),
    verify_counter(Counter),
    F = fun (_Key, DBEntry, Data) ->
                 case (not db_entry:is_empty(DBEntry)) andalso
                          intervals:in(db_entry:get_key(DBEntry), Interval) of
                     true -> [DBEntry | Data];
                     _    -> Data
                 end
        end,
    util:gb_trees_foldl(F, [], DB).

%% @doc Returns all DB entries.
get_data({DB, Counter} = DB_) ->
    ?TRACE1(get_data, DB_),
    verify_counter(Counter),
    gb_trees:values(DB).
