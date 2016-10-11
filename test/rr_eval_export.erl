% @copyright 2010-2016 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    Export helper functions for replica repair evaluation.
%% @see    rr_eval_admin
-module(rr_eval_export).

-author('malange@informatik.hu-berlin.de').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([write_csv/2, write_raw/2]).

-export([create_file/1, write_ds/2, write_row/2, close_file/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TAB, 9).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([file_type/0, property/0, params/0, data_set/0]).

-type file_type() :: gnuplot | latex_table.

-type file_property() :: {filename, string()} |
                         {dir, string()} |
                         {comment, [string()]} |        %first file lines
                         {column_names,    [atom()]}.
-type gnuplot_property() :: {caption, string()} |
                            {gp_xlabel, string()} |
                            {gp_ylabel, string()} |
                            {set_yrange, string()} |
                            {set_xrange, string()} |
                            {set_logscale, x | y | both} |
                            {gnuplot_args, string()}. %starting with using after plot "filename" ..

-type property() :: file_property() |
                    gnuplot_property().
-type params() :: [property()].

-type data_set_property() ::
          {comment, [string()]}.

-type data_set() :: {
                     Properties ::  [data_set_property()],
                     Table      ::  [[any()]]
                    }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% sequentical data file ceation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec create_file([file_property()]) -> {IoDevice::file:io_device(), FilePath::string()}.
create_file(Options) ->
    FileName = proplists:get_value(filename, Options, ?MODULE),
    CommentList = proplists:get_value(comment, Options, []),
    Columns = proplists:get_value(column_names, Options, []),
    Dir = case proplists:get_value(dir, Options, null) of
              null -> throw({create_file_error, "missing dest directory - parameter 'dir'"});
              S -> S
          end,
    _DirOk = filelib:ensure_dir(Dir ++ "/"),
    FilePath = filename:join([Dir, FileName]),
    
    _ = case file:open(FilePath, [write]) of
            {ok, IoDevice} ->
                _ = [io:fwrite(IoDevice, "# ~s~n", [CL]) || CL <- CommentList],
                if Columns =/= [] ->
                       io:fwrite(IoDevice, "# Columns: ", []),
                       {_, ColStr} =
                           lists:foldl(
                             fun(Col, {I, Str}) ->
                                     {I + 1, io_lib:format("~s~c~B_~s", [Str, ?TAB, I, Col])}
                             end, {1, ""}, Columns),
                       io:fwrite(IoDevice, "~s~n", [ColStr]);
                   true -> ok
                end,
                {IoDevice, FilePath};
            {error, _Reason} ->
                ErrMsg = io_lib:format("IO_ERROR - ~p~nFile=~s~nDirOk=~p",
                                       [_Reason, FilePath, _DirOk]),
                throw({io_error, ErrMsg})
        end.

-spec write_ds(IoDevice::file:io_device(), data_set()) -> ok.
write_ds(IoDevice, {DSProp, Table}) ->
    Comments = proplists:get_value(comment, DSProp, []),
    _ = [io:fwrite(IoDevice, "# ~s~n", [CL]) || CL <- Comments],
    _ = [write_row(IoDevice, Row) || Row <- Table],
    ok.

-spec write_row(IoDevice::file:io_device(), Row::[any()]) -> ok.
write_row(IoDevice, Row) ->
    _ = [io:fwrite(IoDevice, "~p~c", [Field, ?TAB]) || Field <- Row],
    io:fwrite(IoDevice, "~n", []).

-spec close_file(IoDevice::file:io_device()) -> ok.
close_file(IoDevice) ->
    io:fwrite(IoDevice, "~n~n", []),
    _ = file:close(IoDevice),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec write_csv(DataSets::[data_set()], Options::[file_property()]) -> ok | io_error.
write_csv(DataSets, Options) ->
    {IoDevice, _FilePath} = create_file(Options),
    lists:foreach(fun(DS) -> write_ds(IoDevice, DS) end, DataSets),
    close_file(IoDevice).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec write_raw([any()], [file_property()]) -> ok | io_error.
write_raw(DataSet, Options) ->
    FileName = proplists:get_value(filename, Options, ?MODULE),
    CommentList = proplists:get_value(comment, Options, []),
    Dir = case proplists:get_value(dir, Options, null) of
              null -> throw({create_file_error, "missing dest directory - parameter 'dir'"});
              S -> S
          end,
    _ = filelib:ensure_dir(Dir ++ "/"),
    FilePath =  filename:join([Dir, FileName]),

    case file:open(FilePath, [write]) of
        {ok, Fileid} ->
            _ = [io:fwrite(Fileid, "# ~s~n", [CL]) || CL <- CommentList],
            _ = [io:fwrite(Fileid, "~p~n", [Line]) || Line <- DataSet],
            _ = file:truncate(Fileid),
            _ = file:close(Fileid),
            ok;
        {_, _} ->
            io_error
    end.
