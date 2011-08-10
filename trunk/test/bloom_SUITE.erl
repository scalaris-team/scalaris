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
%%% @doc    Tests for bloom filter module.
%%% @end
%%% Created : 06/04/2011 by Maik Lange <MLange@informatik.hu-berlin.de>
%%%-------------------------------------------------------------------
%% @version $Id: $

-module(bloom_SUITE).

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

-define(BLOOM, bloom).
-define(HFS, hfs_lhsp_md5).

-define(Fpr_Test_NumTests, 30).
-define(Fpr_Test_DestFPR, 0.001).
-define(Fpr_Test_ElementNum, 1000).

all() -> [
		  add,
		  addRange,
          join,
          equals,
          fpr_test_parallel,
          %fprof,
          time_measurement
		 ].

add(_) -> 
	BF = newBloom(10, 0.1),
	B1 = ?BLOOM:add(BF, "10"),
	B2 = ?BLOOM:add(B1, 10),
	B3 = ?BLOOM:add(B2, 10.3),
    ?equals(?BLOOM:is_element(B3, "10"), true),
    ?equals(?BLOOM:is_element(B3, "100"), false),
    ?equals(?BLOOM:is_element(B3, 10.3), true).

addRange(_) ->	
	BF = newBloom(10, 0.1),
	Elements = lists:seq(1,10,1),
	BF1 = ?BLOOM:addRange(BF, Elements),
	Results = [ ?BLOOM:is_element(BF1, Item) || Item <- Elements],
	ct:pal("Elements: ~p~nResults: ~p~n", [Elements, Results]),
	?equals(lists:member(false, Results), false),
    ?equals(?BLOOM:is_element(BF1, "Not"), false),
    ?equals(?BLOOM:is_element(BF1, 2), true).

join(_) ->
    BF1 = for_to_ex(1, 10, fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(30, 0.1)),
    BF2 = for_to_ex(11, 20, fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(30, 0.1)),
    BF3 = ?BLOOM:join(BF1, BF2),
    NumFound = for_to_ex(1, 20, 
                         fun(I) -> case ?BLOOM:is_element(BF3, I) of
                                       true -> 1;
                                       false -> 0
                                   end
                         end,
                         fun(X,Y) -> X+Y end, 0),
    ct:pal("join NumFound=~B~n", [NumFound]),
    ?assert(NumFound > 10 andalso NumFound =< 20),
    ok.

equals(_) ->
    BF1 = for_to_ex(1, 10, fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(30, 0.1)),
    BF2 = for_to_ex(1, 10, fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(30, 0.1)),    
    ?equals(?BLOOM:equals(BF1, BF2), true),
    ok.

%% @doc ?Fpr_Test_NumTests-fold parallel run of measure_fp
fpr_test_parallel(_) ->
    FalsePositives = util:p_repeatAndAccumulate(
                       fun measure_fp/2, 
                       [?Fpr_Test_DestFPR, ?Fpr_Test_ElementNum], 
                       ?Fpr_Test_NumTests, 
                       fun(X, Y) -> X + Y end, 
                       0),
    AvgFpr = FalsePositives / ?Fpr_Test_NumTests,
    ?BLOOM:print(newBloom(?Fpr_Test_ElementNum, ?Fpr_Test_DestFPR)),
    ct:pal("~nDestFpr: ~f~nMeasured Fpr: ~f~n", [?Fpr_Test_DestFPR, AvgFpr]),
    ?assert(?Fpr_Test_DestFPR >= AvgFpr orelse ?Fpr_Test_DestFPR*1.3 >= AvgFpr),
    ok.   

%% @doc measures false positives by adding 1..MaxElements into a new BF
%%      and checking number of found items which are not in the BF
measure_fp(DestFpr, MaxElements) ->
	BF = newBloom(MaxElements, DestFpr),
    BF1 = for_to_ex(1, MaxElements, fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, BF),
	NumNotIn = trunc(10 / DestFpr),
    %count found items which should not be in the bloom filter 
    NumFound = for_to_ex(MaxElements + 1, 
                         MaxElements + 1 + NumNotIn, 
                         fun(I) -> case ?BLOOM:is_element(BF1, I) of
                                       true -> 1;
                                       false -> 0
                                   end
                         end,
                         fun(X,Y) -> X+Y end,
                         0),
	NumFound / NumNotIn.

fprof(_) ->
    fprof:trace(start, "bloom_fprof.trace"),
    BF = newBloom(100, 0.1),
    _ = ?BLOOM:add(BF, 5423452345),
    fprof:trace(stop),
    fprof:profile(file, "bloom_fprof.trace"),
    fprof:analyse([{dest, "bloom_fprof.analysis"}, {cols, 120}]),    
    ok.

time_measurement(_) ->
    %parameter
    ExecTimes = 100, 
    BFSize = 1000, % req: mod 2 = 0
    
    %measure Build times
    {BTMin, BTMax, BTMed, BTAvg} = measure_util:time_avg(fun() -> for_to_ex(1, round(BFSize / 2), fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(BFSize, 0.1)) end,
                                       [], ExecTimes, false),
    %measure join time
    BF1 = for_to_ex(1, 
                    round(BFSize / 2), 
                    fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(BFSize, 0.1)),
    BF2 = for_to_ex(round(BFSize / 2) + 1, 
                    BFSize, 
                    fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(BFSize, 0.1)),
    {JTMin, JTMax, JTMed, JTAvg} = measure_util:time_avg(fun() -> ?BLOOM:join(BF1, BF2) end, [], ExecTimes, false),
    
    %print results
    ct:pal("EXECUTION TIMES in microseconds~n"
           "PARAMETER - ExecTimes=[~w] - BFSize=[~w]~n"
           "BuildTimes - Min=[~w] Max=[~w] Med=[~w] Avg=[~w]~n"
           "JoinTimes  - Min=[~w] Max=[~w] Med=[~w] Avg=[~w]",
           [ExecTimes, BFSize, BTMin, BTMax, BTMed, BTAvg, JTMin, JTMax, JTMed, JTAvg]),
    ok.

newBloom(ElementNum, Fpr) ->
	HFCount = ?BLOOM:calc_HF_numEx(ElementNum, Fpr),
	Hfs = ?HFS:new(HFCount),
	?BLOOM:new(ElementNum, Fpr, Hfs).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% UTILS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

for_to_ex(I, N, Fun, AccuFun, Accu) ->
    NewAccu = AccuFun(Fun(I), Accu),
    if 
        I < N ->
            for_to_ex(I + 1, N, Fun, AccuFun, NewAccu);
        I =:= N ->
            NewAccu;
        I > N ->
            failed
    end.