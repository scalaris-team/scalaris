%%%-------------------------------------------------------------------
%%% File    : intervals_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/intervals.erl
%%%
%%% Created :  1 Apr 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(tester_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include_lib("unittest.hrl").

all() ->
    []. %tester_create_value

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    Config.

end_per_suite(_Config) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% tester:test/3
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(prop_binary/1 :: (binary()) -> boolean()).
prop_binary(X) ->
    case X of
        <<"42">> ->
            true;
        _ ->
            false
    end.

-spec(prop_integer/1 :: (integer()) -> boolean()).
prop_integer(X) ->
    X =/= 42.

-spec(prop_non_neg_integer/1 :: (non_neg_integer()) -> boolean()).
prop_non_neg_integer(X) ->
    is_integer(X).

-spec(prop_float/1 :: (float()) -> boolean()).
prop_float(X) ->
    case X of
    3.14 -> true;
       _ -> false
    end.

-spec(prop_node/1 :: (node:node_type()) -> boolean()).
prop_node(X) ->
    case is_integer(node:id_version(X)) of
        false ->
            ct:pal("~w", [X]),
            false;
        true ->
            true
    end.

%% @doc Creates a ring with Size rangom IDs.
%%      Passes Options to the supervisor, e.g. to set config variables, specify
%%      a {config, [{Key, Value},...]} option.
-spec make_ring(Size::pos_integer(), Options::[tuple()]) -> pid().
make_ring(Size, Options) ->
    _ = unittest_helper:fix_cwd(),
    error_logger:tty(true),
    case ets:info(config_ets) of
        undefined -> ok;
        _         -> ct:fail("Trying to create a new ring although there is already one.")
    end,
    Pid = unittest_helper:start_process(
            fun() ->
                    ct:pal("Trying to build Scalaris~n"),
                    erlang:register(ct_test_ring, self()),
                    randoms:start(),
                    {ok, _GroupsPid} = pid_groups:start_link(),
                    NewOptions = unittest_helper:prepare_config(Options),
                    _ = sup_scalaris:start_link(boot, NewOptions),
                    tester_scheduler:start_scheduling(),
                    boot_server:connect(),
                    _ = admin:add_node([{first}]),
                    _ = admin:add_nodes(Size - 1),
                    ok
            end),
%%     timer:sleep(1000),
    unittest_helper:check_ring_size(Size),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size(Size),
    ct:pal("Scalaris has booted with ~p node(s)...~n", [Size]),
    unittest_helper:print_ring_data(),
    Pid.

tester_create_value(_Config) ->
    Res = tester:test_with_scheduler([
                           {gen_component, "/home/schuett/zib/scalaris/src/gen_component.erl"},
                           {comm, "/home/schuett/zib/scalaris/src/comm.erl"}
                          ], fun () ->
                                     %tester_scheduler_test:test(),
                                     make_ring(1, []),
                                     unittest_helper:stop_ring()
                             end,
                                    [{white_list, [pid_groups,
                                                  comm_server]}]),
    ct:pal("~w", [Res]),
    %tester:test(?MODULE, prop_integer, 1, 100).
    %tester:test(?MODULE, prop_float, 1, 100).
    %tester:test(?MODULE, prop_create_value, 1, 10).
    ok.

tester_create_value2(_Config) ->
    tester:test(?MODULE, prop_integer, 1, 100).

is_valid(_) ->
    true.
