% @copyright 2015-2016 Zuse Institute Berlin

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
%% @doc Test collect_function_info module
%% @end

-module(collect_module_info_SUITE).
-author('schuett@zib.de').

-compile(export_all).

-include_lib("kernel/include/file.hrl").

-include_lib("unittest.hrl").

all() ->
    [test_collect_function_info].


suite() ->
    [
     {timetrap, {seconds, 300}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

test_collect_function_info(_Config) ->
    Modules = get_modules(code:get_path()),
    ParseState = tester_parse_state:new_parse_state(),
    CollectorFun =
        fun (Module, InnerParseState) ->
                tester_collect_function_info:unittest_collect_module_info(Module, InnerParseState)
        end,
    _ParseState2 = lists:foldl(CollectorFun, ParseState, Modules),
    ok.

get_modules(Paths) ->
    lists:flatten([get_modules_intern(Path) || Path <- Paths]).

get_modules_intern(Path) ->
    {ok, Files} = file:list_dir(Path),
    [erlang:list_to_atom(string:left(File, length(File) - 5)) || File <- Files,
                                                                 length(File) > 5,
                                                                 string:right(File, 5) =:= ".beam",
                                                                 has_abstract_code(File)].

-spec has_abstract_code(string()) -> boolean().
has_abstract_code(FileName) ->
    ModuleFile = code:where_is_file(FileName),
    case beam_lib:chunks(ModuleFile, [abstract_code]) of
        {ok, {_Module, [{abstract_code, {_AbstVersion, _AbstractCode}}]}} ->
            true;
        {ok, {_Module, [{abstract_code, no_abstract_code}]}} ->
            false
    end.
