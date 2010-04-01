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

%% @doc Fails the currently run unit test with a reason that is made of the
%%      given Date formatted using the Format string (see io_lib:format/2).
-spec ct_fail(Format::atom() | string() | binary(), Data::[term()]) -> none().
ct_fail(Format, Data) ->
    Reason = lists:flatten(io_lib:format(Format, Data)),
    ct:fail(Reason).

-define(assert(Boolean), myassert(Boolean, ??Boolean)).

-define(equals(Actual, Expected),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                % Expected might be a function call which is not allowed in case statements
                ExpectedVal = Expected,
                case Actual of
                    ExpectedVal -> ok;
                    Any ->
                        ct:pal("Failed: Stacktrace ~p~n",
                               [erlang:get_stacktrace()]),
                        ct_fail("~p evaluated to \"~p\" which is "
                               "not the expected ~p that evaluates to \"~p\"",
                               [??Actual, Any, ??Expected, ExpectedVal])
                end
        end()).

-define(equals_pattern(Actual, ExpectedPattern),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                case Actual of
                    ExpectedPattern -> ok;
                    Any ->
                        ct:pal("Failed: Stacktrace ~p~n",
                               [erlang:get_stacktrace()]),
                        ct_fail("~p evaluated to \"~p\" which is "
                               "not the expected ~p",
                               [??Actual, Any, ??ExpectedPattern])
                end
        end()).

-define(implies(A, B), (not (A)) orelse (B)).

myassert(true, _Reason) ->
    ok;
myassert(false, Reason) ->
    ct:fail(Reason).

-define(expect_message_timeout(MsgPattern, Timeout),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                receive
                    MsgPattern -> ok
                after
                    Timeout ->
                        ActualMessage =
                            receive
                                X -> X
                            after
                                0 -> no_message
                            end,
                        ct_fail("expected message ~p but got \"~p\"", [??MsgPattern, ActualMessage])
                end
        end()).
-define(expect_message(MsgPattern), ?expect_message_timeout(MsgPattern, 1000)).

-define(expect_no_message_timeout(Timeout),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                receive
                    ActualMessage ->
                        ct_fail("expected no message but got \"~p\"", [ActualMessage])
                after
                    Timeout -> ok
                end
        end()).
-define(expect_no_message(), ?expect_no_message_timeout(100)).

consume_message(Message, Timeout) ->
    receive
        Message -> ok
    after
        Timeout -> ok
    end.

-define(expect_message_ignore_timeout(MsgPattern, IgnoredMessage, Timeout),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                % ignore two ignored messages, then wait for Timeout ignoring all ignored messages and do a final receive with the expected message
                receive
                    IgnoredMessage ->
                        ct:pal("ignored \"~p\"", [IgnoredMessage]),
                        receive
                            IgnoredMessage ->
                                ct:pal("ignored \"~p\"", [IgnoredMessage]),
                                consume_message(IgnoredMessage, Timeout),
                                receive
                                    IgnoredMessage ->
                                        ct:pal("ignored \"~p\" for the last time", [IgnoredMessage]),
                                    MsgPattern -> ok
                                after
                                    0 ->
                                        ActualMessage =
                                            receive
                                                X -> X
                                            after
                                                0 -> no_message
                                            end,
                                        ct_fail("expected message ~p but got \"~p\"", [??MsgPattern, ActualMessage])
                                end
                            MsgPattern -> ok
                        after
                            Timeout ->
                                ActualMessage =
                                    receive
                                        X -> X
                                    after
                                        0 -> no_message
                                    end,
                                ct_fail("expected message ~p but got \"~p\"", [??MsgPattern, ActualMessage])
                        end
                    MsgPattern -> ok
                after
                    Timeout ->
                        ActualMessage =
                            receive
                                X -> X
                            after
                                0 -> no_message
                            end,
                        ct_fail("expected message ~p but got \"~p\"", [??MsgPattern, ActualMessage])
                end
        end()).
-define(expect_message_ignore(MsgPattern, IgnoredMessage), ?expect_message_ignore(MsgPattern, IgnoredMessage, 1000)).

%TODO: enhance expect_message_ignore to allow lists of ignored messages
%TODO: find a way to implement the expect_message_ignore macro in a recursive way similar to the following template:

% maybe also define a expect_message macro which ignores certain messages, similar to this function:
%% expect_message_ignore(MsgPattern, IgnoredMessage) ->
%%     receive
%%         IgnoredMessage ->
%%             ct:pal("ignored ~p", [IgnoredMessage]),
%%             expect_message_ignore(MsgPattern, IgnoredMessage);
%%         MsgPattern ->
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
%%             ct_fail("expected message ~p but got ~p", [MsgPattern, ActualMessage])
%%     end.
