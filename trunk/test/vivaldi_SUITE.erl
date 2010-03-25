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
    gen_component:kill(process_dictionary),
    error_logger:tty(false),
    exit(Pid, kill),
    Config.

on_trigger(Config) ->
    process_dictionary:register_process(vivaldi_group, cyclon, self()),
    VivaldiModule = vivaldi,
    InitialState = VivaldiModule:init(trigger_periodic),
    NewState = VivaldiModule:on({trigger}, InitialState),

    Self = self(),
    ?expect_message({get_subset_rand, 1, Self}),
    ?equals(InitialState, NewState),
    Config.

on_vivaldi_shuffle(Config) ->
    config:write(vivaldi_count_measurements, 1),
    config:write(vivaldi_measurements_delay, 0),
    VivaldiModule = vivaldi,
    InitialState = VivaldiModule:init(trigger_periodic),
    _NewState = VivaldiModule:on({vivaldi_shuffle, cs_send:this(), [0.0, 0.0], 1.0},
                                 InitialState),
    receive
        {ping, SourcePid} -> cs_send:send(SourcePid, {pong})
    end,

    ?expect_message({update_vivaldi_coordinate, _Latency, {[0.0, 0.0], 1.0}}),
    Config.
