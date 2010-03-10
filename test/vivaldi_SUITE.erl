%%%-------------------------------------------------------------------
%%% File    : vivaldi_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/vivaldi.erl
%%%
%%% Created :  18 Feb 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(vivaldi_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-import(intervals).

-include_lib("unittest.hrl").

all() ->
    [on_trigger, on_vivaldi_shuffle].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    error_logger:tty(true),
    Owner = self(),
    Pid = spawn(fun () ->
                        crypto:start(),
                        process_dictionary:start_link(),
                        config:start_link(["scalaris.cfg"]),
                        comm_port:start_link(),
                        timer:sleep(1000),
                        comm_port:set_local_address({127,0,0,1},14195),
                        application:start(log4erl),
                        Owner ! {continue},
                        receive
                            {done} ->
                                ok
                        end
                end),
    receive
        {continue} ->
            ok
    end,
    % extend vivaldi shuffle interval
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    error_logger:tty(false),
    exit(Pid, kill),
    Config.

on_trigger(Config) ->
    process_dictionary:register_process(vivaldi_group, cyclon, self()),
    VivaldiModule = vivaldi:new(trigger_periodic),
    InitialState = VivaldiModule:init([vivaldi_group, []]),
    NewState = VivaldiModule:on({trigger}, InitialState),

    expect_message({get_subset, 1, self()}),
    ?equals(element(1, InitialState), element(1, NewState)),
    ?equals(element(2, InitialState), element(2, NewState)),
    Config.

on_vivaldi_shuffle(Config) ->
    VivaldiModule = vivaldi:new(trigger_periodic),
    InitialState = VivaldiModule:init([vivaldi_group, []]),
    _NewState = VivaldiModule:on({vivaldi_shuffle, cs_send:this(), [0.0, 0.0], 1.0},
                                 InitialState),

    expect_message({vivaldi_shuffle_reply, cs_send:this(), element(1, InitialState),
                    element(2, InitialState)}, {trigger}),
    Config.

expect_message(Msg) ->
    receive
        Msg ->
            ok
    after
        1000 ->
            ActualMessage = receive
                                X ->
                                    X
                            after
                                0 ->
                                    unknown
                            end,
            ct:pal("expected message ~p but got ~p", [Msg, ActualMessage]),
            ?assert(false)
    end.

expect_message(Msg, IgnoredMessage) ->
    receive
        IgnoredMessage ->
            ct:pal("ignored ~p", [IgnoredMessage]),
            expect_message(Msg, IgnoredMessage);
        Msg ->
            ok
    after
        1000 ->
            ActualMessage = receive
                                X ->
                                    X
                            after
                                0 ->
                                    unknown
                            end,
            ct:pal("expected message ~p but got ~p", [Msg, ActualMessage]),
            ?assert(false)
    end.
