%% @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc JSON API for access to raw DHT functions.
%% @version $Id$
-module(api_json_dht_raw).
-author('schintke@zib.de').
-vsn('$Id$').

-export([handler/2]).

% for api_json:
-export([range_read/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(range_read, [From, To])          -> range_read(From, To);

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

-spec range_read(intervals:key(), intervals:key()) -> api_json_tx:result().
range_read(From, To) ->
    {ErrorCode, Data} = api_dht_raw:range_read(From, To),
    {struct, [{status, atom_to_list(ErrorCode)}, {value, data_to_json(Data)}]}.

-spec data_to_json(Data::[db_entry:entry()]) ->
            {array, [{struct, [{key, ?RT:key()} | 
                               {value, api_json_tx:value()} |
                               {version, client_version()}]
                     }]}.
data_to_json(Data) ->
    {array, [ {struct, [{key, db_entry:get_key(DBEntry)},
                        api_json_tx:value_to_json(db_entry:get_value(DBEntry)),
                        {version, db_entry:get_version(DBEntry)}]} ||
              DBEntry <- Data]}.
