%  Copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : unittest.hrl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : contains unittest helpers
%%%
%%% Created :  14 Mar 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007 Thorsten Schuett
%% @version $Id$

-author('schuett@zib.de').
-vsn('$Id$ ').

-define(assert(Boolean), myassert(Boolean, ??Boolean)).
-define(equals(X, Y),
        fun(A) ->
                case A of
                    Y -> ok;
                    Any ->
                        ct:pal("Failed: Stacktrace ~p~n",
                               [erlang:get_stacktrace()]),
                        ct:fail(lists:flatten(
                            io_lib:format("~p evaluated to ~p which is"
                                          "not the expected ~p",
                                          [??X, Any, ??Y])))
                end
        end(X)).

myassert(true, _Reason) ->
    ok;
myassert(false, Reason) ->
    ct:fail(Reason).


-define(expect_message(Msg),
        receive
            Msg -> ok
        after
            1000 ->
                ActualMessage =
                    receive
                        X -> X
                    after
                        0 -> unknown
                    end,
                ct:pal("expected message ~p but got ~p", [??Msg, ActualMessage]),
                ?assert(false)
        end).

% maybe also define a expect_message macro which ignores certain messages, similar to this function:
%% expect_message(Msg, IgnoredMessage) ->
%%     receive
%%         IgnoredMessage ->
%%             ct:pal("ignored ~p", [IgnoredMessage]),
%%             expect_message(Msg, IgnoredMessage);
%%         Msg ->
%%             ok
%%     after
%%         1000 ->
%%             ActualMessage = receive
%%                                 X ->
%%                                     X
%%                             after
%%                                 0 ->
%%                                     unknown
%%                             end,
%%             ct:pal("expected message ~p but got ~p", [Msg, ActualMessage]),
%%             ?assert(false)
%%     end.
