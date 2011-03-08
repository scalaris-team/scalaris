%  @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc This is the scalaris2 application module.
%% @end
%% @version $Id$
-module(scalaris2).
-author('schuett@zib.de').
-vsn('$Id$').

%% functions called by Erlangs init module, triggered via command line
%% (bin/scalarisctl and erl ... '-s scalaris2')
-export([start/0, stop/0]).

%% functions called by application:start(scalaris2)
%% triggered by ?MODULE:start/0.
-behaviour(application).
-export([start/2, stop/1]).

%% functions called by Erlangs init module, triggered via command line
%% (bin/scalarisctl and erl ... '-s scalaris2')
-spec start() -> ok | {error, Reason::term()}.
start() ->
    %% tracer:start(),
    io:format("scalaris2~n", []),
    application:load({application, scalaris2,
                      [{description, "scalaris2"},
                       {vsn, "1.0"},
                       {mod, {scalaris2, []}},
                       {registered, []},
                       {applications, [kernel, stdlib]},
                       {env, []}
                      ]}),
    application:start(scalaris2).

-spec stop() -> ok | {error, Reason::term()}.
stop() ->
    application:stop(scalaris2).

%% functions called by application:start(scalaris2)
%% triggered by ?MODULE:start/0.
-spec start(StartType::normal, StartArgs::[])
        -> {ok, Pid::pid()} | ignore |
           {error, Error::{already_started, Pid::pid()} | term()}.
start(normal, []) ->
    _ = pid_groups:start_link(),
    sup_scalaris2:start_link().

-spec stop(any()) -> ok.
stop(_State) ->
    ok.
