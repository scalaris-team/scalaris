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
%%% File    bloom_SUITE.erl
%%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%%% @doc    Tests for the bloom filter module.
%%% @end
%%% Created : 06/04/2011 by Maik Lange <MLange@informatik.hu-berlin.de>
%%%-------------------------------------------------------------------
%% @version $Id: gossip_SUITE.erl 1629 2011-03-30 09:28:04Z kruber@zib.de $

-module(bloom_SUITE).

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

-define(BLOOM, bloom).
-define(HFS, hfs_lhsp_md5).

-define(Fpr_Test_NumTests, 30).
-define(Fpr_Test_DestFPR, 0.001).
-define(Fpr_Test_ElementNum, 10000).

all() -> [
		  bloom_addSimple,
		  bloom_addRange,
		  %fpr_test_seq, %slow
          fpr_test_parallel
		 ].

bloom_addSimple(_) -> 
	BF = newBloom(10, 0.1),
	B1 = ?BLOOM:add(BF, "10"),
	B2 = ?BLOOM:add(B1, 10),
	B3 = ?BLOOM:add(B2, 10.3),
	?BLOOM:is_element(B3, "10") 
	and not ?BLOOM:is_element(B3, "100")
	and ?BLOOM:is_element(B3, 10.3).

bloom_addRange(_) ->	
	BF = newBloom(10, 0.1),
	Elements = lists:seq(1,10,1),
	BF1 = ?BLOOM:addRange(BF, Elements),
	Results = [ ?BLOOM:is_element(BF1, Item) || Item <- Elements],
	io:format("Elements: ~p~n", [Elements]),
	io:format("Results: ~p~n", [Results]),
	not lists:member(false, Results)
	and not ?BLOOM:is_element(BF1, "Not").

%% @doc run "?Fpr_Test_NumTests"-times measure_fp in parallel
fpr_test_parallel(_) ->
    FalsePositives = parallel_test(?MODULE, 
                                   measure_fp, 
                                   [?Fpr_Test_DestFPR, ?Fpr_Test_ElementNum], 
                                   ?Fpr_Test_NumTests, 
                                   fun(X,Y) -> X+Y end),
    print_fpr_test_result(FalsePositives).

%% @doc run "?Fpr_Test_NumTests"-times measure_fp sequential
fpr_test_seq(_) ->
    FalsePositives = lists:sum([measure_fp(?Fpr_Test_DestFPR, ?Fpr_Test_ElementNum) || _ <- lists:seq(1, ?Fpr_Test_NumTests, 1)]),
    print_fpr_test_result(FalsePositives).
    
print_fpr_test_result(FalsePositives) ->
    AvgFpr = FalsePositives / ?Fpr_Test_NumTests,
    ?BLOOM:print(newBloom(?Fpr_Test_ElementNum, ?Fpr_Test_DestFPR)),
    io:format("DestFpr: ~f~nMeasured Fpr: ~f~n", [?Fpr_Test_DestFPR, AvgFpr]).    

%% @doc adds numbers from 1 to "Elements" to bloom filter "BF"
addIntSeq(BF, 1) ->
    ?BLOOM:add(BF, 1);
addIntSeq(BF, Elements) ->
    BF2 = addIntSeq(BF, Elements - 1),
    ?BLOOM:add(BF2, Elements).

%% @doc checks how mutch integers from "From" to "To" are element of bloom filter "BF"
checkIntSeq(BF, To, To) ->
    case ?BLOOM:is_element(BF, To) of
        true -> 1;
        false -> 0
    end;
checkIntSeq(BF, From, To) ->
    Res = case ?BLOOM:is_element(BF, From) of
              true -> 1;
              false -> 0
          end,
    Res + checkIntSeq(BF, From + 1, To).

%% @doc measures false positives by adding 1..MaxElements into a new BF
%%      and checking number of found items which are not in the BF
measure_fp(DestFpr, MaxElements) ->
	BF = newBloom(MaxElements, DestFpr),
    BF1 = addIntSeq(BF, MaxElements),
	NumNotIn = trunc(10 / DestFpr),
    NumFound = checkIntSeq(BF1, MaxElements + 1, MaxElements + 1 + NumNotIn),
	NumFound / NumNotIn.

newBloom(ElementNum, Fpr) ->
	HFCount = ?BLOOM:calc_HF_numEx(ElementNum, Fpr),
	Hfs = ?HFS:new(HFCount),
	?BLOOM:new(ElementNum, Fpr, Hfs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec parallel_test(module(), atom(), [term()], number(), function()) -> any().
parallel_test(Module, F, Args, Repetitions, ResAggregationF) ->
    %start workers
    parallel_run_start(Module, F, Args, Repetitions),
    %collect results   
    parallel_collect(Repetitions, ResAggregationF).

parallel_collect(0, Result, _AggF) ->
    Result;
parallel_collect(ResNum, Result, ResAggregationF) -> 
    receive
        {parallel_result, R} -> 
            parallel_collect(ResNum - 1, ResAggregationF(Result, R), ResAggregationF)
    end.

parallel_collect(ResNum, ResAggregationF) -> 
    receive
        {parallel_result, R} -> 
            parallel_collect(ResNum - 1, R, ResAggregationF)
    end.

parallel_run(SourcePid, Module, Function, Args) ->
    Res = (catch apply(Module, Function, Args)),
    SourcePid ! {parallel_result, Res}.

parallel_run_start(Module, Function, Args, 1) ->
    spawn_opt(?MODULE, parallel_run, [self(), Module, Function, Args], [{priority, low}]);
parallel_run_start(Module, Function, Args, Repetitions) ->    
    spawn_opt(?MODULE, parallel_run, [self(), Module, Function, Args], [{priority, low}]),
    parallel_run_start(Module, Function, Args, Repetitions - 1).




