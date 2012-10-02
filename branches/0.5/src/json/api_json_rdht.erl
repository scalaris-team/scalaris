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
%% @doc JSON API for accessing replicated DHT functions.
%% @version $Id$
-module(api_json_rdht).
-author('schintke@zib.de').
-vsn('$Id$').

-export([handler/2]).

% for api_json:
-export([delete/1, delete/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";
handler(delete, [Key])                   -> delete(Key);
handler(delete, [Key, Timeout])          -> delete(Key, Timeout);

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

-type delete_result() :: {struct, [{failure, string()} |
                                        {ok, non_neg_integer()} |
                                        {results, {array, [string()]}}]}.
-spec delete(client_key()) -> delete_result().
delete(Key) ->
    delete(Key, 2000).

-spec delete(client_key(), Timeout::pos_integer()) -> delete_result().
delete(Key, Timeout) ->
    case api_rdht:delete(Key, Timeout) of
        {fail, Reason, NumOK, StateList} ->
            {struct, [{failure, atom_to_list(Reason)},
                      {ok, NumOK},
                      {results, {array, [atom_to_list(X) || X <- StateList]}}]};
        {ok, NumOk, StateList} ->
            {struct, [{ok, NumOk},
                      {results, {array, [atom_to_list(X) || X <- StateList ]}}]}
    end.
