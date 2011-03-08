% @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc Starting and stopping the scalaris app.
%% @version $Id$
-module(scalaris).
-author('schuett@zib.de').
-vsn('$Id$').

%% functions called by Erlangs init module, triggered via command line
%% (bin/scalarisctl and erl ... '-s scalaris')
-export([start/0, stop/0]).
%% (bin/scalarisctl and erl ... '-s scalaris cli')
-export([cli/0, process/1]).

%% functions called by application:start(scalaris)
%% triggered by ?MODULE:start/0.
-behaviour(application).
-export([start/2, stop/1]).

%% functions called by Erlangs init module, triggered via command line
%% (bin/scalarisctl and erl ... '-s scalaris')
-spec start() -> ok | {error, Reason::term()}.
start() ->
    application:load(
      {application, scalaris,
       [{description, "scalaris"},
        {vsn, "0.2"},
        {mod, {scalaris, []}},
        {registered, []},
        {applications, [kernel, stdlib]},
        {env, []}
       ]}),
    application:start(scalaris).

-spec stop() -> ok | {error, Reason::term()}.
stop() ->
    application:stop(scalaris).

%% functions called by application:start(scalaris)
%% triggered by ?MODULE:start/0.
-spec start(StartType::normal, StartArgs::[])
        -> {ok, Pid::pid()} | ignore |
           {error, Error::{already_started, Pid::pid()} | term()}.
start(normal, []) ->
    _ = pid_groups:start_link(),
    sup_scalaris:start_link().

-spec stop(any()) -> ok.
stop(_State) ->
    ok.

%% functions called by Erlangs init module, triggered via command line
%% (bin/scalarisctl and erl ... '-s scalaris cli')
-spec cli() -> ok | {error, Reason::term()}.
cli() ->
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
