%% @copyright 2013 Zuse Institute Berlin

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

%% @author Ufuk Celebi <celebi@zib.de>
%% @doc JSON API for polling autoscale VM requests.
%% @version $Id$
-module(api_json_autoscale).
-author('celebi@zib.de').
-vsn('$Id$').

-export([handler/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% @doc main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(check_config , [])    -> check_config();
handler(pull_scale_req  , []) -> pull_scale_req();
handler(lock_scale_req, [])   -> lock_scale_req();
handler(unlock_scale_req, []) -> unlock_scale_req();

handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

%% @doc Call api_autoscale and return:
%%        {'status': 'ok'} -or-
%%        {'status': 'error'}
-spec check_config() -> {struct, [{Key::atom(), Value::term()}]}.
check_config() ->
    {struct, [{status, case api_autoscale:check_config() of
                           true  -> "ok";
                           false -> "error"
                       end}]}.

%% @doc Call api_autoscale and return:
%%        {'status': 'ok', 'value': number} -or-
%%        {'status': 'error', 'reason': reason}
-spec pull_scale_req() -> {struct, [{Key::atom(), Value::term()}]}.
pull_scale_req() ->
    {Status, Value} = api_autoscale:pull_scale_req(),
    {struct, [{status, atom_to_list(Status)},
              case erlang:is_integer(Value) of
                  true  -> {value, Value};
                  false -> {reason, atom_to_list(Value)}
              end]}.
%% @doc Call api_autoscale and return:
%%        {'status': 'ok'} -or-
%%        {'status': 'error', 'reason': reason}
-spec lock_scale_req() -> {struct, [{Key::atom(), Value::term()}]}.
lock_scale_req() ->
    case api_autoscale:lock_scale_req() of
        ok ->
            {struct, [{status, "ok"}]};
        {error, Reason} ->
            {struct, [{status, "error"}, {reason, atom_to_list(Reason)}]}
    end.

%% @doc Call api_autoscale and return:
%%        {'status': 'ok'} -or-
%%        {'status': 'error', 'reason': reason}
-spec unlock_scale_req() -> {struct, [{Key::atom(), Value::term()}]}.
unlock_scale_req() ->
    case api_autoscale:unlock_scale_req() of
        ok ->
            {struct, [{status, "ok"}]};
        {error, Reason} ->
            {struct, [{status, "error"}, {reason, atom_to_list(Reason)}]}
    end.
