%  @copyright 2010-2014 Zuse Institute Berlin

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
%% @doc    helper functions for tester's users
%% @end
%% @version $Id$
-module(tester_helper).
-author('schuett@zib.de').
-vsn('$Id$').

-export([
         % instrument a module
         load_with_export_all/1,
         load_without_export_all/1,
         % helper
         get_src_and_flags_for_module/1,
         get_src_and_flags_for_module/2
        ]).

-include("tester.hrl").
-include("unittest.hrl").


-spec load_with_export_all(Module::module()) -> ok.
load_with_export_all(Module) ->
    MyOptions = [return_errors,
                 export_all,
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
    {Src, Options} = get_src_and_flags_for_module(Module),
    %ct:pal("~p", [file:get_cwd()]),
    %ct:pal("~p", [Options]),
    {ok, CurCWD} = file:get_cwd(),
    ok = fix_cwd_scalaris(),
    case compile:file(Src, lists:append(MyOptions, Options)) of
        {ok,_ModuleName,Binary} ->
            %% config no longer needs explicit code reload callbacks
            %% case Module of
            %%     config -> sys:suspend(config);
            %%     _ -> ok
            %% end,
            {module, Module} = code:load_binary(Module, Src, Binary),
            code:soft_purge(Module), %% remove old code
%% check_old_code not available in Erlang < R14B04
%%             Old = [ X || X <- erlang:loaded(),
%%                          true =:= erlang:check_old_code(X)],
%%             case Old of
%%                 [] -> ok;
%%                 _ -> ct:pal("Some modules have old code after 2nd soft_purge: ~.0p~n", [Old])
%%             end,
            %% config no longer needs explicit code reload callbacks
            %% case Module of
            %%     config -> sys:resume(config);
            %%     _ -> ok
            %% end,
            %ct:pal("~p", [code:is_loaded(Module)]),
            ok;
        {ok,_ModuleName,Binary,_Warnings} ->
            %ct:pal("~p", [_Warnings]),
            %% config no longer needs explicit code reload callbacks
            %% case Module of
            %%     config -> sys:suspend(config);
            %%     _ -> ok
            %% end,
            {module, Module} = erlang:load_module(Module, Binary),
            code:soft_purge(Module), %% remove old code
%% check_old_code not available in Erlang < R14B04
            %% Old = [ X || X <- erlang:loaded(), true =:= erlang:check_old_code(X)],
            %% case Old of
            %%     [] -> ok;
            %%     _ -> ct:pal("Some modules have old code after 2nd soft_purge: ~.0p~n", [Old])
            %% end,
            %% config no longer needs explicit code reload callbacks
            %% case Module of
            %%     config -> sys:resume(config);
            %%     _ -> ok
            %% end,
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
-spec get_src_and_flags_for_module(module())
        -> {FileName::string(), Flags::list()}.
get_src_and_flags_for_module(Module) ->
    get_src_and_flags_for_module(Module, ["ebin", "test"]).

-spec get_src_and_flags_for_module(module(), RelativePaths::[string(),...])
        -> {FileName::string(), Flags::list()}.
get_src_and_flags_for_module(Module, [Path | PathL]) ->
    % assume we are in a sub-directory of $SCALARIS to find the beam file
    Res = beam_lib:chunks(
            lists:append(["../", Path, "/", erlang:atom_to_list(Module), ".beam"]),
            [compile_info]),
    case Res of
        {ok, {Module, [{compile_info, Options}]}} ->
            {source, Source} = lists:keyfind(source, 1, Options),
            {options, Opts} = lists:keyfind(options, 1, Options),
            {Source, Opts};
        _ when PathL =/= [] ->
            get_src_and_flags_for_module(Module, PathL);
        X ->
            ct:pal("~w ~p", [Module, X]),
            ct:pal("~p", [file:get_cwd()]),
            timer:sleep(1000),
            ct:fail(unknown_module)
    end.

% @doc set cwd to $SCALARIS
-spec fix_cwd_scalaris() -> ok | {error, Reason::file:posix()}.
fix_cwd_scalaris() ->
    file:set_cwd("..").
