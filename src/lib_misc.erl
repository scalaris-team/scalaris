%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : lib_misc.erl
%%% Author  : Thorsten Schuett <schuett@csr-pc11.zib.de>
%%% Description : misc functions
%%%
%%% Created : 12 Oct 2007 by Thorsten Schuett <schuett@csr-pc11.zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(lib_misc).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([pmap1/2, make_workers/2, submit_task/2, wait/1]).

pmap1(F, L) ->
    S = self(),
    Ref = erlang:make_ref(),
    lists:foreach(fun(I) ->
                    spawn(fun() -> do_f1(S, Ref, F, I) end)
            end, L),
    %% gather the results
    gather1(length(L), Ref, []).

do_f1(Parent, Ref, F, I) ->
    cs_send:send_local(Parent , {Ref, (catch F(I))}).

gather1(0, _, L) -> L;
gather1(N, Ref, L) ->
    receive
        {Ref, Ret} -> gather1(N-1, Ref, [Ret|L])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% experimental
make_workers(F, N) ->
    spawn(fun () -> loop(F, N, 0) end).

submit_task(Task, Pid) ->
    cs_send:send_local(Pid , {new_task, Task}).

wait(_Pid) ->
    ok. %%FIXME

loop(F, Max, Current) when Current < Max ->
    receive
	{new_task, Task} ->
	    Self = self(),
	    spawn(fun () -> worker(F, Task, Self) end),
	    loop(F, Max, Current + 1);
	{task_done} ->
	    loop(F, Max, Current - 1)
    end;
loop(F, Max, Current) ->
    receive
	{task_done} ->
	    loop(F, Max, Current - 1)
    end.

worker(F, Task, Owner) ->
    F(Task),
    cs_send:send_local(Owner , {task_done}).
