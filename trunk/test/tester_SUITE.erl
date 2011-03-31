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
    [test_is_binary]. %tester_scheduler_ring_1_tx, tester_scheduler_ring_4]. %


suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% tester:test/3
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_is_binary(Value::binary()) -> true.
prop_is_binary(Value) ->
    is_binary(Value).

test_is_binary(_Config) ->
    tester:test(?MODULE, prop_is_binary, 1, 25).

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
                    _ = sup_scalaris:start_link(NewOptions),
                    tester_scheduler:start_scheduling(),
                    mgmt_server:connect(),
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

tester_scheduler_ring_4(_Config) ->
    Res = tester:test_with_scheduler([gen_component, comm],
                                     fun () ->
                                             make_ring(16, []),
                                             unittest_helper:stop_ring()
                                     end,
                                     [{white_list, [pid_groups,
                                                    comm_server]}]),
    ct:pal("~w", [Res]),
    ok.

%                           {gen_component, "/home/schuett/zib/scalaris/src/gen_component.erl"},
%                           {comm, "/home/schuett/zib/scalaris/src/comm.erl"}

tester_scheduler_ring_1_tx(_Config) ->
    Test = fun () ->
                   EmptyTLog = api_tx:new_tlog(),
                   make_ring(1, []),
                   ?equals(api_tx:read("foo"), {fail, not_found}),
                   ?equals(api_tx:write("foo", "bar"), {ok}),
                   ?equals(api_tx:read("foo"), {ok, "bar"}),
                   ?equals_pattern(api_tx:req_list(EmptyTLog,
                                                   [{read, "B"}, {read, "B"},
                                                    {write, "A", 8}, {read, "A"}, {read, "A"},
                                                    {read, "A"}, {write, "B", 9},
                                                    {commit}]),
                                   {_TLog, [{fail,not_found}, {fail,not_found},
                                            {ok}, {ok, 8}, {ok, 8},
                                            {ok, 8}, {ok},
                                            {ok}]}),
                   unittest_helper:stop_ring()
           end,
    _Res = tester:test_with_scheduler([gen_component, comm],
                                      Test,
                                      [{white_list, [pid_groups,
                                                     %sync call on register
                                                     mgmt_server,
                                                     comm_server,
                                                     %sync call on get_dht_nodes
                                                     service_per_vm ]}],
                                      4),
    ok.

get_cwd() ->
    case file:get_cwd() of
        {ok, CurCWD} ->
            case string:rstr(CurCWD, "/bin") =/= (length(CurCWD) - 4 + 1) of
                true -> file:set_cwd("../bin");
                _    -> ok
            end;
        Error -> Error
    end.
