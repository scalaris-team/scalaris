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
%% @reference
%%          1) M.T. Goodrich, M. Mitzenmacher
%%             <em>Invertible Bloom Lookup Tables</em>
%%             2011 ArXiv e-prints. 1101.2245
%%          2) D. Eppstein, M.T. Goodrich, F. Uyeda, G. Varghese
%%             <em>Whats the Difference? Efficient Set Reconciliation without Prior Context</em>
%%             2011 SIGCOMM'11 Vol.41(4)
%% @version $Id$
-module(iblt).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-include("record_helpers.hrl").
-include("scalaris.hrl").

-define(REP_HFS, hfs_lhsp). %HashFunctionSet selection for usage by bloom filter

-export([new/2, new/3, insert/3, delete/3, get/2, list_entries/1]).
-export([is_element/2]).
-export([get_fpr/1, get_prop/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([options/0]).

-type value()   :: integer().
-type cell()    :: {Count       :: non_neg_integer(),
                    KeySum      :: binary(),
                    KeyHashSum  :: non_neg_integer(),   %sum c(x) of all inserted keys x, for c = any hashfunction not in hfs
                    ValSum      :: value(),
                    ValHashSum  :: non_neg_integer()}.  %sum c(y) of all inserted values y, for c = any hashfunction not in hfs

-type table() :: [] | [{ColNr :: pos_integer(), Cells :: [cell()]}].

-record(iblt, {
               hfs        = ?required(iblt, hfs) :: ?REP_HFS:hfs(),    %HashFunctionSet
               table      = []                   :: table(),
               cell_count = 0                    :: non_neg_integer(),
               col_size   = 0                    :: non_neg_integer(), %cells per column
               item_count = 0                    :: non_neg_integer()  %number of inserted items
               }).

-type iblt() :: #iblt{}.

-type option()         :: prime.
-type options()        :: [] | [option()].
-type cell_operation() :: add | remove.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new(?REP_HFS:hfs() | non_neg_integer(), pos_integer()) -> iblt().
new(Hfs, CellCount) ->
    new(Hfs, CellCount, [prime]).

-spec new(?REP_HFS:hfs() | non_neg_integer(), pos_integer(), options()) -> iblt().
new(HfCount, CellCount, Options) when is_integer(HfCount) ->
    new(?REP_HFS:new(HfCount), CellCount, Options);
new(Hfs, CellCount, Options) ->
    K = ?REP_HFS:size(Hfs),
    {Cells, ColSize} = case proplists:get_bool(prime, Options) of
                            true ->
                                CCS = prime:get_nearest(util:ceil(CellCount / K)),
                                {CCS * K, CCS};
                            false ->
                                RCC = bloom:resize(CellCount, K),
                                {RCC, erlang:round(RCC / K)}
                        end,
    SubTable = [{0, <<0>> ,0, 0, 0} || _ <- lists:seq(1, ColSize)],
    Table = [ {I, SubTable} || I <- lists:seq(1, K)],
    #iblt{
          hfs = Hfs,
          table = Table,
          cell_count = Cells,
          col_size = ColSize,
          item_count = 0
          }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert(iblt(), ?RT:key(), value()) -> iblt().
insert(IBLT, Key, Value) ->
    change_table(IBLT, add, Key, Value).

-spec delete(iblt(), ?RT:key(), value()) -> iblt().
delete(IBLT, Key, Value) ->
    change_table(IBLT, remove, Key, Value).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec operation_val(cell_operation()) -> 1 | -1.
operation_val(add) -> 1;
operation_val(remove) -> -1.

-spec change_table(iblt(), cell_operation(), ?RT:key(), value()) -> iblt().
change_table(#iblt{ hfs = Hfs, table = T, item_count = ItemCount, col_size = ColSize } = IBLT,
             Operation, Key, Value) ->
    NT = lists:foldl(
           fun({ColNr, Col}, NewT) ->
                   NCol = change_cell(Col,
                                      ?REP_HFS:apply_val(Hfs, ColNr, Key) rem ColSize,
                                      encode_key(Key), Value, Operation),
                   [{ColNr, NCol} | NewT]
           end, [], T),
    IBLT#iblt{ table = NT, item_count = ItemCount + operation_val(Operation)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec change_cell([cell()], pos_integer(), Key::binary(), value(), cell_operation()) -> [cell()].
change_cell(Column, CellNr, Key, Value, Operation) ->
    {HeadL, [Cell | TailL]} = lists:split(CellNr, Column),
    {Count, KeySum, KHSum, ValSum, VHSum} = Cell,
    lists:append(HeadL, [{Count + operation_val(Operation),
                          util:bin_xor(KeySum, Key),
                          KHSum bxor checksum_fun(Key),
                          ValSum bxor Value,
                          VHSum bxor checksum_fun(Value)} | TailL]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get(iblt(), ?RT:key()) -> value() | not_found.
get(#iblt{ table = T, hfs = Hfs, col_size = ColSize }, Key) ->
    p_get(T, Hfs, ColSize, Key).

-spec p_get(table(), ?REP_HFS:hfs(), non_neg_integer(), ?RT:key()) -> value() | not_found.
p_get([], _, _, _) -> not_found;
p_get([{ColNr, Col} | T], Hfs, ColSize, Key) ->
    Cell = lists:nth((?REP_HFS:apply_val(Hfs, ColNr, Key) rem ColSize) + 1, Col),
    {_, _, _, Val, _} = Cell,
    case is_pure(Cell) of
        true -> Val;
        false -> p_get(T, Hfs, ColSize, Key)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc a cell is pure if count = -1 or 1, and checksum of key and value correspond to their checksums
-spec is_pure(cell()) -> boolean().
is_pure({Count, Key, KeyCheck, Val, ValCheck}) ->
    (Count =:= 1 orelse Count =:= -1) andalso
        checksum_fun(Key) =:= KeyCheck andalso
        checksum_fun(Val) =:= ValCheck.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc lists all correct entries of this structure
%     correct entries can be retrieved out of pure cells
%     a pure cell := count = 1 and check_sum(keySum)=keyHashSum and check_sum(valSum)=valHashSum
-spec list_entries(iblt()) -> [{?RT:key(), value()}].
list_entries(IBLT) ->
    p_list_entries(IBLT, []).

-spec p_list_entries(iblt(), Acc) -> Acc when is_subtype(Acc, [{?RT:key(), value()} | [{?RT:key(), value()}]]).
p_list_entries(#iblt{ table = T } = IBLT, Acc) ->
    case get_any_entry(T, []) of
        [] -> lists:flatten(Acc);
        [_|_] = L ->
            NewIBLT = lists:foldl(fun({Key, Val}, NT) ->
                                          delete(NT, Key, Val)
                                  end, IBLT, L),
            p_list_entries(NewIBLT, [L, Acc])
    end.

% tries to find any pure entry
-spec get_any_entry(table(), [{?RT:key(), value()}]) -> [{?RT:key(), value()}].
get_any_entry([], Acc) ->
    Acc;
get_any_entry([{_, Col} | T], Acc) ->
    Result = [{decode_key(Key), Val} || {_, Key, _, Val, _} = Cell <- Col,
                                        is_pure(Cell)],
    if
        Result =:= [] -> get_any_entry(T, lists:append(Result, Acc));
        true -> Result
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec is_element(iblt(), ?RT:key()) -> boolean().
is_element(#iblt{ hfs = Hfs, table = T, col_size = ColSize }, Key) ->
    Found = lists:foldl(
              fun({ColNr, Col}, Count) ->
                      {C, _, _, _, _} = lists:nth((?REP_HFS:apply_val(Hfs, ColNr, Key) rem ColSize) + 1, Col),
                      Count + if C > 0 -> 1;
                                 true -> 0 end
                      end, 0, T),
    Found =:= ?REP_HFS:size(Hfs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc calculates actual false positive rate depending on saturation degree
-spec get_fpr(iblt()) -> float().
get_fpr(#iblt{  hfs = Hfs, cell_count = M, item_count = N }) ->
    K = ?REP_HFS:size(Hfs),
    math:pow(1 - math:exp((-K * N) / M), K).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_prop(atom(), iblt()) -> any().
get_prop(Prop, IBLT) ->
    case Prop of
        item_count -> IBLT#iblt.item_count;
        col_size -> IBLT#iblt.col_size;
        cell_count -> IBLT#iblt.cell_count
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Hash function for checksum building.
-spec checksum_fun(binary() | integer()) -> non_neg_integer().
checksum_fun(X) when is_integer(X) -> erlang:crc32(integer_to_list(X));
checksum_fun(X) -> erlang:crc32(X).

-spec encode_key(?RT:key()) -> binary().
encode_key(Key) ->
    term_to_binary(Key).

-spec decode_key(binary()) -> ?RT:key().
decode_key(BinKey) ->
    binary_to_term(BinKey).
