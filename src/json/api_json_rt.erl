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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc JSON API for using routing tables.
%% @version $Id$
-module(api_json_rt).
-author('schuett@zib.de').

-export([handler/2]).

% for api_json:
-export([get_replication_factor/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(get_replication_factor, [])      -> get_replication_factor();

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

%% interface for rt calls
-spec get_replication_factor() -> {struct, [{Key::atom(), Value::term()}]}.
get_replication_factor() ->
    {struct, [{status, "ok"}, {value, api_rt:get_replication_factor()}]}.
