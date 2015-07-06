% @copyright 2010-2015 Zuse Institute Berlin

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
%% @version $Id $
-module(rr_eval_export).

-author('malange@informatik.hu-berlin.de').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([write_to_file/3, write_csv/2, write_raw/2]).

-export([create_file/1, append_ds/2, append_line/2, close_ds/1]).

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

-spec create_file([file_property()]) -> FilePath::string().
create_file(Options) ->
    FileName = proplists:get_value(filename, Options, ?MODULE),
    CommentList = proplists:get_value(comment, Options, []),
    Columns = proplists:get_value(column_names, Options, []),
    Dir = case proplists:get_value(dir, Options, null) of
              null -> throw({create_file_error, "missing dest directory - parameter {dir, path}"});
              S -> S
          end,
    _DirOk = filelib:ensure_dir(Dir ++ "/"),
    FilePath = filename:join([Dir, FileName]),
    
    _ = case file:open(FilePath, [write]) of
            {ok, Fileid} ->
                _ = [io:fwrite(Fileid, "# ~s~n", [CL]) || CL <- CommentList],
                if
                    Columns =/= [] ->
                        io:fwrite(Fileid, "# Columns: ", []),
                        {_, ColStr} = lists:foldl(fun(Col, {I, Str}) ->
                                                          {I + 1, io_lib:format("~s~c~p_~s", [Str, ?TAB, I, Col])}
                                                  end, {1, ""}, Columns),
                        io:fwrite(Fileid, "~s~n", [ColStr]);
                    true -> ok
                end,
                
                _ = file:truncate(Fileid),
                file:close(Fileid);
            {error, _Reason} ->
                ErrMsg = io_lib:format("IO_ERROR - ~p~nFile=~s~nDirOk=~p", [_Reason, FilePath, _DirOk]),
                throw({io_error, ErrMsg})
        end,
    FilePath.

-spec append_ds(FilePath::string(), data_set()) -> ok | io_error.
append_ds(FilePath, DS) ->
    case file:open(FilePath, [append]) of
        {ok, Fileid} ->
            write_data_set(Fileid, DS),
            _ = file:close(Fileid),
            ok;
        {_, _} ->
            io_error
    end.

-spec append_line(FilePath::string(), Row::[any()]) -> ok | io_error.
append_line(FilePath, Row) ->
    case file:open(FilePath, [append]) of
        {ok, FileId} ->
            write_row(FileId, Row),
            _ = file:close(FileId),
            ok;
        {_, _} ->
            io_error
    end.

-spec close_ds(FilePath::string()) -> ok | io_error.
close_ds(FilePath) ->
    case file:open(FilePath, [append]) of
        {ok, Fileid} ->
            io:fwrite(Fileid, "~n~n", []),
            _ = file:close(Fileid),
            ok;
        {_, _} ->
            io_error
    end.

-spec write_data_set(file:io_device(), data_set()) -> ok.
write_data_set(FileId, {DSProp, Table}) ->
    Comments = proplists:get_value(comment, DSProp, []),
    _ = [io:fwrite(FileId, "# ~s~n", [CL]) || CL <- Comments],
    _ = [write_row(FileId, Row) || Row <- Table],
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec write_csv(DataSets::[data_set()], Options::[file_property()]) -> ok | io_error.
write_csv(DataSets, Options) ->
    FileName = proplists:get_value(filename, Options, ?MODULE),
    CommentList = proplists:get_value(comment, Options, []),
    Columns = proplists:get_value(column_names, Options, []),
    Dir = case proplists:get_value(dir, Options, null) of
              null -> throw({create_file_error, "missing dest directory - parameter {dir, path}"});
              S -> S
          end,
    _ = filelib:ensure_dir(Dir ++ "/"),
    FilePath = filename:join([Dir, FileName]),
    
    case file:open(FilePath, [write]) of
        {ok, Fileid} ->
            _ = [io:fwrite(Fileid, "# ~s~n", [CL]) || CL <- CommentList],
            if
                Columns =/= [] ->
                    io:fwrite(Fileid, "# Columns: ", []),
                    _ = [io:fwrite(Fileid, "~p~c", [Col, ?TAB]) || Col <- Columns],
                    io:fwrite(Fileid, "~n", []);
                true -> ok
            end,
            
            lists:foreach(fun(DS) ->
                                  write_data_set(Fileid, DS),
                                  io:fwrite(Fileid, "~n~n", [])
                          end, DataSets),
            
            _ = file:truncate(Fileid),
            _ = file:close(Fileid),
            ok;
        {_, _} ->
            io_error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec write_raw([any()], [file_property()]) -> ok | io_error.
write_raw(DataSet, Options) ->
    FileName = proplists:get_value(filename, Options, ?MODULE),
    CommentList = proplists:get_value(comment, Options, []),
    Dir = case proplists:get_value(dir, Options, null) of
              null -> throw({create_file_error, "missing dest directory - parameter {dir, path}"});
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec write_to_file(OutputType, Table, Options) -> ok | io_error when
    is_subtype(OutputType,  file_type()),
    is_subtype(Table,       [[any()]]),
    is_subtype(Options,     params()).

write_to_file(gnuplot, Table, Options) ->
    FileName = proplists:get_value(filename, Options, ?MODULE),
    Caption = proplists:get_value(caption, Options, io_lib:format("GNUPLOT BY ~p", [?MODULE])),
    Comment = proplists:get_value(comment, Options, null),
    Header = proplists:get_value(column_names, Options, ["none"]),
    PlotArgs = proplists:get_value(gnuplot_args, Options, null),
    XLabel =  proplists:get_value(gp_xlabel, Options, "X"),
    YLabel =  proplists:get_value(gp_ylabel, Options, "Y"),
    Dir = case proplists:get_value(dir, Options, null) of
              null -> throw({create_file_error, "missing dest directory - parameter {dir, path}"});
              S -> S
          end,
    _ = filelib:ensure_dir(Dir ++ "/"),
    FilePath = filename:join([Dir, FileName]),
    FileNameNoExt = filename:basename(FileName, filename:extension(FileName)),

    case file:open(FilePath, [write]) of
        {ok, Fileid} ->
            io:fwrite(Fileid, "# ~s~n", [Caption]),
            Comment =/= null andalso
                io:fwrite(Fileid, "# ~s~n", [Comment]),
            
            io:fwrite(Fileid, "# ", []),
            _ = [io:fwrite(Fileid, "~p~c", [ColTitle, ?TAB]) || ColTitle <- Header],
            io:fwrite(Fileid, "~n", []),
            _ = [write_row(Fileid, Row) || Row <- Table],
            _ = file:truncate(Fileid),
            _ = file:close(Fileid),
            
            if PlotArgs =/= null ->
                   case file:open(filename:join([Dir, string:join([FileNameNoExt, "gp"], ".")]), [write]) of
                       {ok, ScriptId} ->
                           io:fwrite(ScriptId, "set t svg~n", []),
                           
                           io:fwrite(ScriptId, "# Solid Background~n", []),
                           io:fwrite(ScriptId, "set object 1 rect from screen 0, 0, 0 to screen 1, 1, 0 behind~n", []),
                           io:fwrite(ScriptId, "set object 1 rect fc  rgb \"white\"  fillstyle solid 1.0~n", []),
                           
                           io:fwrite(ScriptId, "set title \"~s\"~n", [Caption]),
                           io:fwrite(ScriptId, "set xlabel \"~s\"~n", [XLabel]),
                           io:fwrite(ScriptId, "set ylabel \"~s\"~n", [YLabel]),
                           
                           case proplists:get_value(set_logscale, Options, none) of
                               none -> ok;
                               x -> io:fwrite(ScriptId, "set logscale x~n", []);
                               y -> io:fwrite(ScriptId, "set logscale y~n", []);
                               both -> io:fwrite(ScriptId, "set logscale~n", [])
                           end,
                           
                           proplists:is_defined(set_yrange, Options) andalso
                               io:fwrite(ScriptId, "set yrange [~s]~n", [proplists:get_value(set_yrange, Options)]),
                           proplists:is_defined(set_xrange, Options) andalso
                               io:fwrite(ScriptId, "set xrange [~s]~n", [proplists:get_value(set_xrange, Options)]),

                           io:fwrite(ScriptId, "set o \"~s.svg\"~n", [FileNameNoExt]),
                           io:fwrite(ScriptId, "plot \"~s\" ~s~n", [FileName, PlotArgs]),
                           _ = file:truncate(ScriptId),
                           _ = file:close(ScriptId);
                       _ -> ok
                   end;
               true -> ok
            end;
        {_, _} ->
            io_error
    end;

write_to_file(latex_table, Table, Options) ->
    FileName = proplists:get_value(filename, Options, io_lib:format("~p.tex", [?MODULE])),
    Caption = proplists:get_value(caption, Options, io_lib:format("GNUPLOT BY ~p", [?MODULE])),
    Columns = proplists:get_value(column_names, Options, ["none"]),
    Dir = case proplists:get_value(dir, Options, null) of
              null -> throw({create_file_error, "missing dest directory - parameter {dir, path}"});
              S -> S
          end,
    FilePath = filename:join([Dir, FileName]),
    
    case file:open(FilePath, [write]) of
        {ok, Fileid} ->
            _ = [io:fwrite(Fileid, Text, []) ||
                   Text <-
                       ["\\documentclass[a4paper,10pt]{article}~n",
                        "\\usepackage[utf8x]{inputenc}~n",
                        "\\begin{document}~n~n",
                        io_lib:format("\\begin{table}[t]~n~c\\centering~n", [?TAB])
                       ]
                ],
            io:fwrite(Fileid, "~c\\begin{tabular}{|", [?TAB]),
            lists:foreach(fun(_) -> io:fwrite(Fileid, "c|", []) end,
                          Columns),
            io:fwrite(Fileid, "}~n", []),
            
            LastCol = lists:last(Columns),
            lists:foreach(fun(Col) -> case Col =:= LastCol of
                                          false -> io:fwrite(Fileid, "~p~c&", [Col, ?TAB]);
                                          _ ->  io:fwrite(Fileid, "~p\\\\~n", [Col])
                                      end
                          end,
                          Columns),
            
            io:fwrite(Fileid, "~c\\hline~n", [?TAB]),
                        
            lists:foreach(
              fun(Row) ->
                      LastField = lists:last(Row),
                      io:fwrite(Fileid, "~c", [?TAB]),
                      lists:foreach(
                        fun(Field) ->
                                case type_of(Field) of
                                    float -> io:fwrite(Fileid, "~.5f", [Field]);
                                    _ -> io:fwrite(Fileid, "~p", [Field])
                                end,
                                case Field =:= LastField of
                                    false -> io:fwrite(Fileid, "~c&", [?TAB]);
                                    true -> io:fwrite(Fileid, "\\\\~n", [])
                                end
                        end, Row)
              end,
              Table),
            
            io:fwrite(Fileid, "~c\\end{tabular}~n\\caption{~s}~n\\end{table}", [?TAB, Caption]),
            io:fwrite(Fileid, "\\end{document}", []),
            
            _ = file:truncate(Fileid),
            _ = file:close(Fileid),
            
            ok;
        {_, _} ->
            io_error
    end.

-spec write_row(file:io_device(), Row::[any()]) -> ok.
write_row(FileId, Row) ->
    _ = [case type_of(Field) of
             float -> io:fwrite(FileId, "~f~c", [Field, ?TAB]);
             _ -> io:fwrite(FileId, "~p~c", [Field, ?TAB])
         end || Field <- Row],
    io:fwrite(FileId, "~n", []),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

type_of(X) when is_float(X) -> float;
type_of(X) when is_integer(X) -> integer;
type_of(_) -> some.


