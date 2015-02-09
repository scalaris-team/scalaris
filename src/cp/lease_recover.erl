% @copyright 2007-2014 Zuse Institute Berlin

%  Licensed under the Apache License, Version 2.0 (the "License");
%  you may not use this file except in compliance with the License.
%  You may obtain a copy of the License at
%
%      http://www.apache.org/licenses/LICENSE-2.0
%
%  Unless required by applicable law or agreed to in writing, software
%  distributed under the License is distributed on an "AS IS" BASIS,
%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%  See the License for the specific language governing permissions and
%  limitations under the License.

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Recover leases.
%% @end
%% @version $$
-module(lease_recover).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([recover/4]).

-spec recover(prbr:state(), prbr:state(), prbr:state(), prbr:state()) -> lease_list:lease_list().
recover(Leases1, Leases2, Leases3, Leases4) ->
    %% Leases1 = filter(get_leases(leases_db1)),
    %% Leases2 = filter(get_leases(leases_db2)),
    %% Leases3 = filter(get_leases(leases_db3)),
    %% Leases4 = filter(get_leases(leases_db4)),
    ExtractLease = fun({_Id, Lease}) -> Lease end,
    Candidates = lists:map(ExtractLease,
                   filter(lists:append(prbr:tab2list(Leases4), 
                                       lists:append(prbr:tab2list(Leases3), 
                                                    lists:append(prbr:tab2list(Leases2), 
                                                                 prbr:tab2list(Leases1)))))),
    %% io:format("candidates ~p~n", [Candidates]),
    case length(Candidates) of
        0 -> lease_list:empty();
        1 -> % one potentially active lease: set active lease
            [Lease] = Candidates,
            lease_list:make_lease_list(Lease, [], []);
        2 -> % could be an ongoing split or an ongoing merge: finish operation
            ts = nyi, % ts: not yet implemented
            lease_list:empty()
    end.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec filter([{?RT:key(), l_on_cseq:lease_t()}]) -> [{?RT:key(), l_on_cseq:lease_t()}].
filter(Leases) ->
    F = fun({Id, L}) ->
                %% log:log("~w ~w~n", [Id, L]),
                LeaseId = l_on_cseq:get_id(L),
                Id =:= LeaseId andalso %% is first replica?
                    l_on_cseq:is_live_aux_field(L) andalso
                    l_on_cseq:has_timed_out(L)
        end,
    lists:filter(F, Leases).

