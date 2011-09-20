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
%% @doc    Stateful Bloom Filter Approximate State Machine (SBS-ACSM) implemenation
%% @end
%% @reference F. Bonomi, M. Mitzenmacher, R. Panigrahy
%%          Beyond Bloom Filters: From Approximate Membership Checks to Approximate State Machines 
%%          2006 in ACM SIGCOMM 36(4)
%%          http://portal.acm.org/citation.cfm?id=1159950
%% @version $Id$

-module(sbf).
%% 
%% -include("record_helpers.hrl").
%% 
%% -behaviour(bloom_beh).
%% 
%% -include("scalaris.hrl").
%% 
%% %% Types
%% -record(sbf, {
%%                 cellCount     = 0 		                      :: integer(),		%cell-count
%%                 cellSize      = 4                             :: integer(),     %cell bit-size - req: cellSize mod 2 = 0
%%                 filter        = <<>>                          :: binary(),		%length = cellCount*cellSize bits
%%                 hfs           = ?required(bloom, hfs)         :: ?REP_HFS:hfs(),%HashFunctionSet
%%                 expItems      = ?required(bloom, expItems)    :: integer(),		%extected number of items
%%                 targetFPR     = ?required(bloom, targetFPR)   :: float(), 		%target false-positive-rate
%%                 addedItems    = 0                             :: integer()      %number of inserted items
%%                }).
%% 
%% -type bloom_filter_t() :: #sbf{}.
%% 
%% -include("bloom_beh.hrl").
%% 
%% %% API  
%% 
%% % @doc creates a new sbf
%% new_(N, FPR, Hfs) ->
%%     CellSize = case config:cfg_is_integer(sbf_cell_size) of
%%                    true -> config:read(sbf_cell_size);
%%                    _ -> 8
%%                end,
%%     %BF bit size should fit into an even number of bytes
%%     CellCount = calc_least_size(N, FPR),
%%     Size = resize(CellCount * CellSize, 8),  
%%     #bloom{
%%            cellCount = CellCount,
%%            cellSize = CellSize,
%%            filter = <<0:Size>>,                            
%%            hfs = Hfs,                              
%%            expItems = N, 
%%            targetFPR = calc_FPR(CellCount, N, calc_HF_num(CellCount, N)),
%%            addedItems = 0
%%           }.
%% 
%% % @doc adds a range of items
%% add_list_(Sbf, Items) ->
%%     #sbf{
%%            cellCount = CellCount, 
%%            hfs = Hfs, 
%%            addedItems = FilledCount,
%%            filter = Filter
%%           } = Sbf,
%%     Pos = lists:append([{apply(element(1, Hfs), apply_val, [Hfs, Key]), Val} || {Key, Val} <- Items]),
%%     Positions = lists:map(fun({Pos, Val}) -> {Pos rem CellCount, Val} end, Pos),
%%     NewFilter = set_Cells(Filter, Positions),
%%     Sbf#sbf{
%%             filter = NewFilter, 
%%             addedItems = FilledCount + length(Items)
%%            }.
%% 
%% % @doc returns true if item is found
%% is_element_(Bloom, Item) -> 
%%     #bloom{
%%            size = BFSize,          
%%            hfs = Hfs, 
%%            filter = Filter
%%           } = Bloom,
%%     Pos = apply(element(1, Hfs), apply_val, [Hfs, Item]), 
%%     Positions = lists:map(fun(X) -> X rem BFSize end, Pos),
%%     check_Bits(Filter, Positions).
%% 
%% 
%% %% @doc joins two bloom filter, returned bloom filter represents their union
%% join_(#bloom{size = Size1, expItems = ExpItem1, addedItems = Items1, targetFPR = Fpr1,
%%              filter = F1, hfs = Hfs}, 
%%       #bloom{size = Size2, expItems = ExpItem2, addedItems = Items2, targetFPR = Fpr2,
%%              filter = F2}) ->
%%     NewSize = erlang:max(Size1, Size2),
%%     <<F1Val : Size1>> = F1,
%%     <<F2Val : Size2>> = F2,
%%     NewFVal = F1Val bor F2Val,
%%     #bloom{
%%            size = NewSize,
%%            filter = <<NewFVal:NewSize>>,                            
%%            expItems = erlang:max(ExpItem1, ExpItem2), 
%%            targetFPR = erlang:min(Fpr1, Fpr2),
%%            hfs = Hfs,                              
%%            addedItems = Items1 + Items2 %approximation            
%%            }.
%% 
%% %% @doc checks equality of two bloom filters
%% equals_(Bloom1, Bloom2) ->
%%     #bloom{
%%            size = Size1, 
%%            addedItems = Items1,
%%            filter = Filter1
%%           } = Bloom1,
%%     #bloom{
%%            size = Size2, 
%%            addedItems = Items2,
%%            filter = Filter2
%%           } = Bloom2,
%%     Size1 =:= Size2 andalso
%%         Items1 =:= Items2 andalso
%%         Filter1 =:= Filter2.
%% 
%% % @doc bloom filter debug information
%% print_(Bloom) -> 
%%     #bloom{
%%            expItems = MaxItems, 
%%            targetFPR = TargetFPR,
%%            size = Size,
%%            hfs = Hfs,
%%            addedItems = NumItems
%%           } = Bloom,
%%     HCount = apply(element(1, Hfs), hfs_size, [Hfs]), 
%%     io:format("bloom_filter: bloom~n"
%%               "Size~16b Bit~n"
%%               "Size~16.2f kb~n---~n"
%%               "Bits Per Item~16.2f~n"
%%               "MaxItems~12b~n"
%%               "DestFPR
%% ~13.4f~n"
%%               "---~n"
%%               "HashFunNum~10b~n"
%%               "ActItemNum~10b~n"
%%               "ActFPR~14.4f~n", 
%%               [Size,
%%               (Size div 8) / 1024,
%%                Size / MaxItems,
%%                MaxItems,
%%                TargetFPR,
%%                HCount,
%%                NumItems,
%%                calc_FPR(Size, NumItems, HCount)]
%%              ),
%%     ok. 