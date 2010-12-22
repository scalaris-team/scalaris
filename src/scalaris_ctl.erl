% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc scalaris cli
%% @version $Id$
-module(scalaris_ctl).
-author('schuett@zib.de').
-vsn('$Id$').

-export([start/0, process/1]).

-spec start() -> no_return().
start() ->
    case init:get_plain_arguments() of
        [NodeName | Args] ->
            Node = list_to_atom(NodeName),
            io:format("~p~n", [Node]),
            case rpc:call(Node, ?MODULE, process, [Args]) of
                {badrpc, Reason} ->
                    io:format("badrpc to ~p: ~p~n", [Node, Reason]),
                    init:stop(1),
                    receive nothing -> ok end;
                _ ->
                    init:stop(0),
                    receive nothing -> ok end
            end;
        _ ->
            print_usage(),
            init:stop(1),
            receive nothing -> ok end
    end.

-spec print_usage() -> ok.
print_usage() ->
    io:format("usage info~n", []).

-spec process([string()]) -> ok.
process(["stop"]) ->
    init:stop();
process(Args) ->
    io:format("got process(~p)~n", [Args]).
