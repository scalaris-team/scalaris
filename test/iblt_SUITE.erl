%  @copyright 2010-2011 Zuse Institute Berlin
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
%%% File    art_SUITE.erl
%%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%%% @doc    Tests for art module (approximate reconciliation tree).
%%% @end
%%% Created : 11/11/2011 by Maik Lange <MLange@informatik.hu-berlin.de>
%%%-------------------------------------------------------------------
%% @version $Id: $

-module(iblt_SUITE).

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").


-define(HFS, hfs_lhsp).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() -> [
          tester_insert,
          tester_delete,
          tester_get
         ].

suite() ->
    [
     {timetrap, {seconds, 15}}
    ].

init_per_suite(Config) ->
    _ = crypto:start(),
    Config.

end_per_suite(_Config) ->
    crypto:stop(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_insert(1..8, 10..100, ?RT:key(), ?DB:version()) -> true.
prop_insert(HFCount, CellCount, Key, Value) ->
    IBLT = iblt:new(?REP_HFS:new(HFCount), CellCount),
    IBLT2 = iblt:insert(IBLT, Key, Value),
    ?equals(iblt:get_item_count(IBLT), 0),
    ?equals(iblt:get_item_count(IBLT2), 1),    
    ?equals(iblt:get(IBLT, Key), not_found),
    ?equals(iblt:get(IBLT2, Key), Value).
  
tester_insert(_) ->
    tester:test(?MODULE, prop_insert, 4, 100, [{threads, 2}]).    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_delete(1..8, 10..100, ?RT:key(), ?DB:version()) -> true.
prop_delete(HFCount, CellCount, Key, Value) ->
    IBLT = iblt:new(?REP_HFS:new(HFCount), CellCount),
    IBLT2 = iblt:insert(IBLT, Key, Value),
    ?equals(iblt:get_item_count(IBLT2), 1),
    ?equals(iblt:get(IBLT2, Key), Value),
    IBLT3 = iblt:delete(IBLT2, Key, Value),
    ?equals(iblt:get_item_count(IBLT3), 0),
    ?equals(iblt:get(IBLT3, Key), not_found).
  
tester_delete(_) ->
    tester:test(?MODULE, prop_delete, 4, 100, [{threads, 2}]).    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_get(1..8, 10..100, [{?RT:key(), ?DB:version()}]) -> true.
prop_get(HFCount, CellCount, Items) ->
    IBLT = lists:foldl(fun({Key, Ver}, _IBLT) -> iblt:insert(_IBLT, Key, Ver) end,
                       iblt:new(?REP_HFS:new(HFCount), CellCount), 
                       Items),
    ?equals(iblt:get_item_count(IBLT), length(Items)),
    FSum = lists:foldl(fun({Key, _}, {_IBLT, Sum}) -> 
                               case iblt:get(_IBLT, Key) of
                                   not_found -> Sum;
                                   _ -> Sum + 1
                               end 
                       end, 
                       {IBLT, 0}, Items), 
    ?assert(FSum =:= length(Items)).
  
tester_get(_) ->
    tester:test(?MODULE, prop_delete, 4, 100, [{threads, 2}]). 

