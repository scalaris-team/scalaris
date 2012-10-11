%  @copyright 2010-2012 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    tester_helper.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    helper functions for tester's users
%%% @end
%%% Created :  16 Aug 2012 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(tester_helper).

-author('schuett@zib.de').
-vsn('$Id$').

-export([
         % instrument a module
         load_with_export_all/1,
         load_without_export_all/1
         ]).

-include("tester.hrl").
-include("unittest.hrl").


-spec load_with_export_all(Module::module()) -> ok.
load_with_export_all(Module) ->
    MyOptions = [return_errors,
                 {parse_transform, tester_helper_parse_transform},
                 binary],
    ct:pal("Reload ~p module with 'export_all'.~n", [Module]),
    reload_with_options(Module, MyOptions).

-spec load_without_export_all(Module::module()) -> ok.
load_without_export_all(Module) ->
    MyOptions = [return_errors,
                 binary],
    ct:pal("Reload ~p module normally.~n", [Module]),
    reload_with_options(Module, MyOptions).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% reload function
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc we assume the standard scalaris layout, i.e. we are currently
% in a ct_run... directory underneath a scalaris checkout. The ebin
% directory should be in ../ebin

reload_with_options(Module, MyOptions) ->
    Src = get_file_for_module(Module),
    %ct:pal("~p", [file:get_cwd()]),
    Options = get_compile_flags_for_module(Module),
    %ct:pal("~p", [Options]),
    {ok, CurCWD} = file:get_cwd(),
    ok = fix_cwd_scalaris(),
    case compile:file(Src, lists:append(MyOptions, Options)) of
        {ok,_ModuleName,Binary} ->
            case Module of
                config -> sys:suspend(config);
                _ -> ok
            end,
            {module, Module} = code:load_binary(Module, Src, Binary),
            code:soft_purge(Module), %% remove old code
%% check_old_code not available in Erlang < R14B04
%%             Old = [ X || X <- erlang:loaded(),
%%                          true =:= erlang:check_old_code(X)],
%%             case Old of
%%                 [] -> ok;
%%                 _ -> ct:pal("Some modules have old code after 2nd soft_purge: ~.0p~n", [Old])
%%             end,
            case Module of
                config -> sys:resume(config);
                _ -> ok
            end,
            %ct:pal("~p", [code:is_loaded(Module)]),
            ok;
        {ok,_ModuleName,Binary,_Warnings} ->
            %ct:pal("~p", [_Warnings]),
            case Module of
                config -> sys:suspend(config);
                _ -> ok
            end,
            {module, Module} = erlang:load_module(Module, Binary),
            code:soft_purge(Module), %% remove old code
%% check_old_code not available in Erlang < R14B04
            %% Old = [ X || X <- erlang:loaded(), true =:= erlang:check_old_code(X)],
            %% case Old of
            %%     [] -> ok;
            %%     _ -> ct:pal("Some modules have old code after 2nd soft_purge: ~.0p~n", [Old])
            %% end,
            case Module of
                config -> sys:resume(config);
                _ -> ok
            end,
            %ct:pal("~w", [erlang:load_module(Module, Binary)]),
            ok;
        X ->
            ct:pal("1: ~p", [X]),
            ok
    end,
    ok = file:set_cwd(CurCWD),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% misc. helper functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec get_file_for_module(module()) -> string().
get_file_for_module(Module) ->
    % we have to be in $SCALARIS/ebin to find the beam file
    {ok, CurCWD} = file:get_cwd(),
    ok = fix_cwd_ebin(),
    Res = beam_lib:chunks(Module, [compile_info]),
    ok = file:set_cwd(CurCWD),
    case Res of
        {ok, {Module, [{compile_info, Options}]}} ->
            {source, Filename} = lists:keyfind(source, 1, Options),
            Filename;
        X ->
            ct:pal("~w ~p", [Module, X]),
            ct:pal("~p", [file:get_cwd()]),
            timer:sleep(1000),
            ct:fail(unknown_module)
    end.

-spec get_compile_flags_for_module(module()) -> list().
get_compile_flags_for_module(Module) ->
    % we have to be in $SCALARIS/ebin to find the beam file
    {ok, CurCWD} = file:get_cwd(),
    ok = fix_cwd_ebin(),
    Res = beam_lib:chunks(Module, [compile_info]),
    ok = file:set_cwd(CurCWD),
    case Res of
        {ok, {Module, [{compile_info, Options}]}} ->
            {options, Opts} = lists:keyfind(options, 1, Options),
            Opts;
        X ->
            ct:pal("~w ~w", [Module, X]),
            timer:sleep(1000),
            ct:fail(unknown_module),
            []
    end.

% @doc set cwd to $SCALARIS/ebin
-spec fix_cwd_ebin() -> ok | {error, Reason::file:posix()}.
fix_cwd_ebin() ->
    case file:get_cwd() of
        {ok, CurCWD} ->
            case string:rstr(CurCWD, "/ebin") =/= (length(CurCWD) - 4 + 1) of
                true -> file:set_cwd("../ebin");
                _    -> ok
            end;
        Error -> Error
    end.

% @doc set cwd to $SCALARIS
-spec fix_cwd_scalaris() -> ok | {error, Reason::file:posix()}.
fix_cwd_scalaris() ->
    file:set_cwd("..").
