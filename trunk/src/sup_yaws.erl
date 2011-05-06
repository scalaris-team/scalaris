%% @copyright 2011 Zuse Institute Berlin

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
%% @version $Id$
-module(sup_yaws).
-author('kruber@zib.de').
-vsn('$Id$').

-behaviour(supervisor).

-export([start_link/0, init/1, check_config/0]).

-spec start_link() -> {ok, Pid::pid()} | ignore |
                      {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link() ->
    Id = io_lib:format("yaws@~p", [node()]),
    Docroot = config:read(docroot),
    GconfList = [{max_connections, 800},
                 {logdir, config:read(log_path)},
                 {id, Id}
                ],
    SconfList = [{docroot, Docroot},
                 {port, config:read(yaws_port)},
                 {listen, {0,0,0,0}},
                 {allowed_scripts, [yaws]},
                 {partial_post_size, config:read(yaws_max_post_data)},
                 {flags, [{access_log, false}]} % {deflate, true}
                ],
    
    % load yaws application (required by yaws_api:embedded_start_conf)
    _ = application:load(
          {application, yaws,
           [{description, "yaws WWW server"},
            {vsn, "%VSN%"},
            {modules, []},
            {registered, []},
            {mod, {yaws_app, []}},
            {env, []},
            {applications, [kernel, stdlib]}]}),
    
    {ok, SCList, GC, ChildSpecs} =
        yaws_api:embedded_start_conf(Docroot, SconfList, GconfList, Id),
    
    X = supervisor:start_link(?MODULE, ChildSpecs),
    
    %% now configure Yaws
    yaws_api:setconf(GC, SCList),
    
    X.

-spec init(ChildSpecs) -> {ok, {{one_for_all, MaxRetries::pos_integer(),
                                      PeriodInSeconds::pos_integer()},
                         ChildSpecs}}.
init(ChildSpecs) ->
    {ok, {{one_for_all, 10, 1}, ChildSpecs}}.

%% @doc Checks whether config parameters of the cyclon process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_string(docroot) and
    config:is_string(log_path) and
    config:is_port(yaws_port) and
        
    case config:read(yaws_max_post_data) of
        nolimit -> true;
        _ -> config:is_integer(yaws_max_post_data) and
             config:is_greater_than(yaws_max_post_data, 0)
    end.
