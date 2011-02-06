% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc scalaris2 application file
%% @version $Id$
-module(scalaris2_app).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(application).

-export([start/2, stop/1]).

-spec start(StartType::normal, StartArgs::[])
        -> {ok, Pid::pid()} | ignore |
           {error, Error::{already_started, Pid::pid()} | term()}.
start(normal, []) ->
    _ = pid_groups:start_link(),
    sup_scalaris2:start_link().

-spec stop(any()) -> ok.
stop(_State) ->
    ok.
