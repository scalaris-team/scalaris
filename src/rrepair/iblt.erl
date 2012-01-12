% @copyright 2011 Zuse Institute Berlin

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
%% @doc    Invertible Bloom Lookup Table
%%         Operations: Insert, Delete, Get, ListEntries
%% @end
%% @reference M. T. Goodrich, M. Mitzenmacher
%%          <em>Invertible Bloom Lookup Tables</em> 
%%          2011 ArXiv e-prints. 1101.2245
%% @version $Id$

-module(iblt).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([new/2, insert/3, delete/3, get/2, list_entries/1]).
-export([get_fpr/1, get_item_count/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type key()     :: integer().
-type value()   :: integer().
-type cell()    :: {Count   :: non_neg_integer(),
                    KeySum  :: key(),
                    ValSum  :: value()}.

-type table() :: [] | [{ColNr :: pos_integer(), Cells :: [cell()]}].

-record(iblt, {
               hfs        = ?required(iblt, hfs) :: ?REP_HFS:hfs(),    %HashFunctionSet
               table      = []                   :: table(),
               cell_count = 0                    :: non_neg_integer(), 
               col_size   = 0                    :: non_neg_integer(), %cells per column
               item_count = 0                    :: non_neg_integer()  %number of inserted items
               }).

-opaque iblt() :: #iblt{}. 

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new(?REP_HFS:hfs(), pos_integer()) -> iblt().
new(Hfs, CellCount) ->
    K = ?REP_HFS:size(Hfs),
    CCount = resize(CellCount, K), 
    ColSize = erlang:round(CCount / K),
    SubTable = [{0, 0 ,0} || _ <- lists:seq(1, ColSize)],
    Table = [ {I, SubTable} || I <- lists:seq(1, K)],
    #iblt{
          hfs = Hfs, 
          table = Table, 
          cell_count = CCount, 
          col_size = ColSize,
          item_count = 0
          }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert(iblt(), key(), value()) -> iblt().
insert(IBLT, Key, Value) ->
    change_table(IBLT, add, Key, Value).

-spec delete(iblt(), key(), value()) -> iblt().
delete(IBLT, Key, Value) ->
    change_table(IBLT, remove, Key, Value).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec change_table(iblt(), add | remove, key(), value()) -> iblt().
change_table(#iblt{ hfs = Hfs, table = T, item_count = ItemCount, col_size = ColSize } = IBLT, 
             Operation, Key, Value) ->
    %TODO calculate each column in a separate process
    NT = lists:foldl(
           fun({ColNr, Col}, NewT) ->
                   NCol = change_cell(Col, 
                                      ?REP_HFS:apply_val(Hfs, ColNr, Key) rem ColSize, 
                                      Key, Value, Operation),
                   [{ColNr, NCol} | NewT]
           end, [], T),
    IBLT#iblt{ table = NT, item_count = ItemCount + case Operation of
                                                        add -> 1;
                                                        remove -> -1
                                                    end}.   

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec change_cell([cell()], pos_integer(), key(), value(), add | remove) -> [cell()].
change_cell(Column, CellNr, Key, Value, Operation) ->    
    {HeadL, [Cell | TailL]} = lists:split(CellNr, Column),
    {Count, KeySum, ValSum} = Cell,
    case Operation of
        add -> lists:flatten([HeadL, {Count + 1, KeySum + Key, ValSum + Value}, TailL]);
        remove when Count > 0 -> lists:flatten([HeadL, {Count - 1, KeySum - Key, ValSum - Value}, TailL]);
        remove when Count =:= 0 -> lists:flatten([HeadL, {0, KeySum, ValSum}, TailL])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get(iblt(), key()) -> value() | not_found.
get(#iblt{ table = T } = IBLT, Key) ->
    p_get(T, IBLT, Key).

-spec p_get(Table, IBLT, Key) -> Result when
    is_subtype(Table,     table()),
    is_subtype(IBLT,      iblt()),
    is_subtype(Key,       key()),
    is_subtype(Result,    value() | not_found).
p_get([], _, _) -> not_found;
p_get([{ColNr, Col} | T], #iblt{ hfs = Hfs, col_size = ColSize} = IBLT, Key) ->
    {Count, KeySum, ValSum} = 
        lists:nth((?REP_HFS:apply_val(Hfs, ColNr, Key) rem ColSize) + 1, Col),
    if
        Count =:= 0 -> p_get(T, IBLT, Key);
        Count =:= 1 andalso KeySum =:= Key -> ValSum;
        true -> p_get(T, IBLT, Key)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec list_entries(iblt()) -> [{key(), value()}].
list_entries(IBLT) ->
    p_list_entries(IBLT, []).

-spec p_list_entries(iblt(), [{key(), value()}]) -> [{key(), value()}].
p_list_entries(#iblt{ table = T } = IBLT, Acc) ->
    case get_any_entry(T, []) of
        [] -> Acc;
        L ->
            NewIBLT = lists:foldl(fun({Key, Val}, NT) -> 
                                          delete(NT, Key, Val) 
                                  end, IBLT, L),
            p_list_entries(NewIBLT, lists:append([L, Acc]))
    end.

-spec get_any_entry(table(), [{key(), value()}]) -> [{key() | value()}].
get_any_entry([], Acc) -> 
    Acc;
get_any_entry([{_, Col} | T], Acc) ->
    Result = [{Key, Val} || {Count, Key, Val} <- Col, Count =:= 1],
    if 
        Result =:= [] -> get_any_entry(T, lists:append([Result, Acc]));
        true -> Result
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc calculates actual false positive rate depending on saturation degree
-spec get_fpr(iblt()) -> float().
get_fpr(#iblt{  hfs = Hfs, cell_count = M, item_count = N }) ->
    K = ?REP_HFS:size(Hfs),
    math:pow(1 - math:pow(math:exp(1), (-K*N)/M), K).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_item_count(iblt()) -> non_neg_integer().
get_item_count(#iblt{ item_count = C }) -> 
    C.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Increases Val until Val rem Div == 0.
-spec resize(pos_integer(), pos_integer()) -> pos_integer().
resize(Val, Div) when Val rem Div == 0 -> 
    Val;
resize(Val, Div) when Val rem Div /= 0 -> 
    resize(Val + 1, Div).
