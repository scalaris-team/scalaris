%  @copyright 2012-2017 Zuse Institute Berlin

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
%% @doc    Common functions for yaws handlers in docroot/api.
%% @end
%% @version $Id$

out(A) ->
    Peer = if is_tuple(A#arg.clisock) andalso
              element(1, A#arg.clisock) =:= ssl ->
                   {ssl, SSLSocket} = A#arg.clisock,
                   ssl:peername(SSLSocket);
              true ->
                   inet:peername(A#arg.clisock)
           end,
    {ok, {IP, _}} = Peer,
    A2 = A#arg{state = [{ip, IP}]},
    case A2#arg.clidata of
        {partial,_} -> send(A2, 413); % send error code 413 (Request Entity Too Large)
        _ -> yaws_rpc:handler_session(A2, {?MODULE, handler})
    end.

handler([{ip, _IP}] = _State, {call, Operation, {array, Params}}, Session) ->
    {true, 0, Session, {response, forward_to_handler(Operation, Params)}}.

send(Args, StatusCode) -> send(Args, StatusCode, "").

send(_Args, StatusCode, Payload) ->
    [{status, StatusCode},
     {content, "application/json", Payload},
     {header, {content_length, lists:flatlength(Payload) }}].
