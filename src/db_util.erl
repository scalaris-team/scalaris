% @copyright 2015 Zuse Institute Berlin,

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
%% @author Nico Kruber <kruber@zib.de>
%% @doc    DB back-end utilities.
%% @end
%% @version $Id$
-module(db_util).
-author('schuett@zib.de').
-author('kruber@zib.de').

-include("scalaris.hrl").

-export([get_name/1, get_subscriber_name/1,
         get_recoverable_dbs/0, parse_table_name/1]).

%% @doc Initializes a new database.
-spec get_name(DBName::nonempty_string() | atom() | tuple()) -> nonempty_string().
get_name(DBName) when is_atom(DBName) ->
    get_name(erlang:atom_to_list(DBName));
get_name(DBName = {Name, Id}) when is_tuple(DBName) ->
    get_name(erlang:atom_to_list(Name) ++ "-" ++ erlang:integer_to_list(Id));
get_name(DBName) ->
    ?DBG_ASSERT(not lists:member($+, DBName)),
    RandomName = randoms:getRandomString(),
    DBName ++ "+" ++ pid_groups:group_to_filename(pid_groups:my_groupname())
        ++ "+" ++ RandomName.

-spec get_subscriber_name(DBName::nonempty_string()) -> nonempty_string().
get_subscriber_name(DBName) ->
    ?DBG_ASSERT(not lists:member($#, DBName)),
    DBName ++ "#subscribers".

-spec get_recoverable_dbs()
        -> [{DB_type::nonempty_string(), PID_group::pid_groups:groupname(), DB_name::nonempty_string()}].
get_recoverable_dbs() ->
    Tables = (config:read(db_backend)):get_persisted_tables(),
    %% io:format("tables list: ~w~n", [Tables]),
    %% creating tuples with DB_names different parts : {DB_type, PID_group, DB_name}
    [parse_table_name(Table) || Table <- Tables].

-spec parse_table_name(Table::nonempty_string() | atom())
        -> {DB_type::nonempty_string(), PID_group::pid_groups:groupname(), Table::nonempty_string()}.
parse_table_name(Table) when is_atom(Table) ->
    parse_table_name(erlang:atom_to_list(Table));
parse_table_name(Table) ->
    {string:sub_word(Table, 1, $+),
     pid_groups:filename_to_group(string:sub_word(Table, 2, $+)),
     Table}.
