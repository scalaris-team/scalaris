%% @copyright 2011-2017 Zuse Institute Berlin

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
-module(sup_yaws).
-author('kruber@zib.de').

-behaviour(supervisor).

-export([start_link/0, init/1, check_config/0]).

-include("yaws.hrl").

-spec start_link() -> {ok, Pid::pid()} | ignore |
                      {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link() ->
    % preload api_json modules to make atoms known to list_to_existing_atom/1
    _ = api_json:module_info(),
    _ = api_json_dht_raw:module_info(),
    _ = api_json_monitor:module_info(),
    _ = api_json_rdht:module_info(),
    _ = api_json_tx:module_info(),
    Id = io_lib:format("yaws@~p", [node()]),
    Docroot = config:read(docroot),
    AuthDirs = case config:read(yaws_auth) of
                   UserPwds = [_|_] -> [#auth{dir = "/",
                                              docroot = Docroot,
                                              realm = "Scalaris Web Interface",
                                              users = UserPwds}];
                   _                -> []
               end,
    GconfList = [{max_connections, 800},
                 {logdir, config:read(log_path)},
                 {include_dir, [Docroot ++ "/api"]},
                 {id, Id}
                ],
    SconfList2 = [{docroot, Docroot},
                 {port, config:read(yaws_port)},
                 {listen, {0,0,0,0}},
                 {allowed_scripts, [yaws]},
                 {partial_post_size, config:read(yaws_max_post_data)},
                 {authdirs, AuthDirs},
                 {flags, [{access_log, false}]} % {deflate, true}
                ],
    SconfList = case config:read(yaws_ssl) of
                    true ->
                        _ = application:ensure_all_started(ssl), %% >= R16B02
                        SSL= {ssl, [
%% https://jamielinux.com/docs/openssl-certificate-authority/
                                    {keyfile, config:read(yaws_keyfile)},
                                    {certfile, config:read(yaws_certfile)},
                                    {cacertfile, config:read(yaws_cacertfile)},
                                    {verify, verify_peer},
                                    {fail_if_no_peer_cert, true},
                                    {password, config:read(yaws_sslpassword)}
                                   ]},
                        io:format("~p~n", [SSL]),
                        [SSL | SconfList2];
                    _ -> SconfList2
                end,

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

    util:if_verbose("Yaws listening at port ~p.~n", [config:read(yaws_port)]),
    X = supervisor:start_link(?MODULE, ChildSpecs),

    %% now configure Yaws
    ok = yaws_api:setconf(GC, SCList),
    %% remove yaws error logger, as we have our own
    error_logger:delete_report_handler(yaws_log_file_h),
    X.

-spec init(ChildSpecs) -> {ok, {{one_for_all, MaxRetries::pos_integer(),
                                      PeriodInSeconds::pos_integer()},
                         ChildSpecs}} when is_subtype(ChildSpecs, [supervisor:child_spec()]).
init(ChildSpecs) ->
    {ok, {{one_for_all, 10, 1}, ChildSpecs}}.

%% @doc Checks whether yaws config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_string(docroot) and
    config:cfg_is_string(log_path) and
    config:cfg_is_port(yaws_port) and
    config:cfg_is_list(yaws_auth,
                       fun(X) ->
                               case X of
                                   {User, Password} when is_list(User)
                                     andalso is_list(Password) -> true;
                                   _ -> false
                               end
                       end, "{\"User\", \"Password\"}") and

    case config:read(yaws_max_post_data) of
        nolimit -> true;
        _ -> config:cfg_is_integer(yaws_max_post_data) and
             config:cfg_is_greater_than(yaws_max_post_data, 0)
    end.
