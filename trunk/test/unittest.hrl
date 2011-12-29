%  Copyright 2008-2011 Zuse Institute Berlin
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

-ifdef(have_ctline_support).
-compile({parse_transform, ct_line}).
-endif.

%% @doc Fails the currently run unit test with a reason that is made of the
%%      given Date formatted using the Format string (see io_lib:format/2).
%% -spec ct_fail(Format::atom() | string() | binary(), Data::[term()]) -> no_return().
-define(ct_fail(Format, Data),
        % if possible, do not use a function to silence dialyzer warnings about
        % functions with no return
        case erlang:whereis(ct_test_ring) of
            undefined -> ok;
            _         -> unittest_helper:print_ring_data()
        end,
        ct:fail(lists:flatten(io_lib:format(Format, Data)))).

-define(assert(Boolean),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                case Boolean of
                    true  -> true;
                    UnittestAny ->
                        unittest_helper:macro_equals_failed(
                          UnittestAny, true, ??Boolean, true)
                end
        end()).

-define(equals(Actual, Expected),
        unittest_helper:macro_equals(Actual, Expected, ??Actual, ??Expected)).

-define(equals_w_note(Actual, Expected, Note),
        unittest_helper:macro_equals(Actual, Expected, ??Actual, ??Expected, Note)).

-define(equals_pattern(Actual, ExpectedPattern),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                case Actual of
                    ExpectedPattern -> true;
                    UnittestAny ->
                        unittest_helper:macro_equals_failed(
                          UnittestAny, ??ExpectedPattern, ??Actual, ??ExpectedPattern)
                end
        end()).

-define(equals_pattern_w_note(Actual, ExpectedPattern, Note),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                case Actual of
                    ExpectedPattern -> true;
                    UnittestAny ->
                        unittest_helper:macro_equals_failed(
                          UnittestAny, ??ExpectedPattern, ??Actual, ??ExpectedPattern, Note)
                end
        end()).

-define(expect_exception(Cmd, ExceptionType, ExceptionPattern),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                try Cmd of
                    UnittestAny ->
                        UnittestExpExceptionStr = "exception " ++ ??ExceptionType ++ ":" ++ ??ExceptionPattern,
                        unittest_helper:macro_equals_failed(
                          UnittestAny, UnittestExpExceptionStr, ??Cmd, UnittestExpExceptionStr)
                catch
                    ExceptionType:ExceptionPattern -> true;
                    UnittestOtherType:UnittestOtherException ->
                        UnittestExpExceptionStr = "exception " ++ ??ExceptionType ++ ":" ++ ??ExceptionPattern,
                        UnittestActExceptionStr = lists:flatten(io_lib:format("exception ~.0p:~.0p", [UnittestOtherType, UnittestOtherException])),
                        unittest_helper:macro_equals_failed(
                          UnittestActExceptionStr, UnittestExpExceptionStr, ??Cmd, UnittestExpExceptionStr)
                end
        end()).

-define(implies(A, B), (not (A)) orelse (B)).

-define(expect_message_timeout(MsgPattern, Timeout),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                receive
                    MsgPattern -> true
                after
                    Timeout ->
                        UnittestActualMessage =
                            receive
                                X -> X
                            after
                                0 -> no_message
                            end,
                        ?ct_fail("expected message ~s but got \"~.0p\"~n", [??MsgPattern, UnittestActualMessage])
                end
        end()).
-define(expect_message(MsgPattern), ?expect_message_timeout(MsgPattern, 1000)).

-define(expect_no_message_timeout(Timeout),
        unittest_helper:expect_no_message_timeout(Timeout)).
-define(expect_no_message(), ?expect_no_message_timeout(100)).

-define(consume_message(Message, Timeout),
    receive
        Message -> true
    after
        Timeout -> timeout
    end).

-define(consume_all_messages(Message),
        % recursive anonymous function:
        fun() ->
                Fun = fun(F) ->
                              case ?consume_message(Message, 0) of
                                  ok -> F(F);
                                  timeout -> true
                              end
                      end,
                Fun(Fun)
        end()).

-define(expect_message_ignore_timeout(MsgPattern, IgnoredMessage, Timeout),
        % wrap in function so that the internal variables are out of the calling function's scope
        fun() ->
                % ignore two ignored messages, then wait for Timeout ignoring all ignored messages and do a final receive with the expected message
                receive
                    IgnoredMessage ->
                        ct:pal("ignored \"~.0p\"~n", [IgnoredMessage]),
                        receive
                            IgnoredMessage ->
                                ct:pal("ignored \"~.0p\"~n", [IgnoredMessage]),
                                ?consume_message(IgnoredMessage, Timeout),
                                receive
                                    IgnoredMessage ->
                                        ct:pal("ignored \"~.0p\" for the last time~n", [IgnoredMessage]),
                                    MsgPattern -> true
                                after
                                    0 ->
                                        UnittestActualMessage =
                                            receive
                                                X -> X
                                            after
                                                0 -> no_message
                                            end,
                                        ?ct_fail("expected message ~s but got \"~.0p\"~n", [??MsgPattern, UnittestActualMessage])
                                end
                            MsgPattern -> true
                        after
                            Timeout ->
                                UnittestActualMessage =
                                    receive
                                        X -> X
                                    after
                                        0 -> no_message
                                    end,
                                ?ct_fail("expected message ~s but got \"~.0p\"~n", [??MsgPattern, UnittestActualMessage])
                        end
                    MsgPattern -> true
                after
                    Timeout ->
                        UnittestActualMessage =
                            receive
                                X -> X
                            after
                                0 -> no_message
                            end,
                        ?ct_fail("expected message ~s but got \"~.0p\"~n", [??MsgPattern, UnittestActualMessage])
                end
        end()).
-define(expect_message_ignore(MsgPattern, IgnoredMessage), ?expect_message_ignore(MsgPattern, IgnoredMessage, 1000)).

%TODO: enhance expect_message_ignore to allow lists of ignored messages
%TODO: find a way to implement the expect_message_ignore macro in a recursive way similar to the following template:

% maybe also define a expect_message macro which ignores certain messages, similar to this function:
%% expect_message_ignore(MsgPattern, IgnoredMessage) ->
%%     receive
%%         IgnoredMessage ->
%%             ct:pal("ignored ~.0p~n", [IgnoredMessage]),
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
%%             ?ct_fail("expected message ~.0p but got ~.0p", [MsgPattern, ActualMessage])
%%     end.
