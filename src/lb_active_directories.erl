%  @copyright 2014 Zuse Institute Berlin

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

%% @author Maximilian Michels <michels@zib.de>
%% @doc Implementation of a modified version of the paper below. This implementation 
%%      doesn't use virtual servers but can still benefit from the load balancing
%%      algorithm's attributes, respectively the load directories and the emergency
%%      transfer of load. In addition, gossipping information has been added to improve
%%      the balancing process.
%%
%%      Many-to-Many scheme
%%
%% @reference B. Godfrey, S. Surana, K. Lakshminarayanan, R. Karp, and I. Stoica
%%            "Load balancing in dynamic structured peer-to-peer systems"
%%            Performance Evaluation, vol. 63, no. 3, pp. 217-240, 2006.
%%
%% @version $Id$
-module(lb_active_directories).
-author('michels@zib.de').
-vsn('$Id$').

-behavior(lb_active_beh).
%% implements
-export([init/0, check_config/0]).
-export([handle_msg/2, handle_dht_msg/2]).
-export([get_web_debug_kv/1]).

-include("scalaris.hrl").
-include("record_helpers.hrl").

%% Defines the number of directories
%% e.g. 1 implies a central directory
-define(NUM_DIRECTORIES, 1).

%-define(TRACE(X,Y), ok).
-define(TRACE(X,Y), io:format(X,Y)).

-type directory_name() :: string().

-record(directory, {name         = ?required(directory, name) :: directory_name(), 
                    last_balance = nil                        :: nil | os:timestamp(),
                    pool         = gb_sets:new()              :: gb_set(), %% TODO really gb_set here? gb_tree is good enough
                    num_reported = 0                          :: non_neg_integer()
                    }).

-record(reassign, {light = ?required(reassign, from) :: lb_info:lb_info(),
                   heavy = ?required(reassign, to)   :: lb_info:lb_info()
                   }).

-type reassign() :: #reassign{}.
-type schedule() :: [reassign()].

-record(state, {my_dirs = [] :: [directory_name()],
                threshold_periodic  = 0.5, %% k_p = (1 + average directory utilization) / 2
                threshold_emergency = 1.0,  %% k_e
                schedule = [] :: schedule()
                }).

-type directory() :: #directory{}.

-type state() :: #state{}.

-type trigger() :: publish_trigger | directory_trigger.

-type dht_message() :: none.


%%%%%%%%%%%%%%%% Initialization %%%%%%%%%%%%%%%%%%%%%%%

-spec init() -> state().
init() ->
    create_directories(?NUM_DIRECTORIES),
    %% post load to random directory
    request_dht_load(),
    trigger(publish_trigger),
    trigger(directory_trigger),
    request_dht_range(),
    This = comm:this(),
    rm_loop:subscribe(
       self(), ?MODULE, fun rm_loop:subscribe_dneighbor_change_slide_filter/3,
       fun(_,_,_,_,_) -> comm:send_local(self(), {get_state, This, my_range}) end, inf),
    #state{}.

%%%%%%%%%%%%%%%% Process Messages %%%%%%%%%%%%%%%%%%%%%%%

handle_msg({publish_trigger}, State) ->
    trigger(publish_trigger),
    case emergency of %% emergency when load(node) > k_e
      true ->
         %post_load(),
         %get && perform_transfer()
         State;
      _ ->
          % get && perform transfer() without overloading
          Schedule = State#state.schedule,
          perform_transfer(Schedule),
          % post_load()
          % request dht load to post it in the directory afterwards
          request_dht_load(),
          State#state{schedule = []}
    end;

%% we received load because of publish load trigger or emergency
handle_msg({post_load, LoadInfo}, State) ->
    ?TRACE("Posting load ~p~n", [LoadInfo]),
    Directory = get_random_directory(),
    DirKey = Directory#directory.name,
    post_load_to_directory(LoadInfo, DirKey),
    %% TODO Emergency Threshold has been already checked at the node overloaded...
    EmergencyThreshold = State#state.threshold_emergency,
    case lb_info:get_load(LoadInfo) > EmergencyThreshold of
        true  ->
            ?TRACE("Emergency in post_load~n", []),
            MySchedule = State#state.schedule,
            Schedule = directory_routine(DirKey, emergency, MySchedule),
            perform_transfer(Schedule);
        false -> ok
    end,
    State;

handle_msg({directory_trigger}, State) ->
    trigger(directory_trigger),
    ?TRACE("~p My Directories: ~p~n", [self(), State#state.my_dirs]),
    %% Threshold k_p = Average laod in directory
    %% Threshold k_e = 1 meaning full capacity
    %% Upon receipt of load information:
    %%      add_to_directory(load_information),
    %%      case node_overloaded of
    %%          true -> compute_reassign(directory_load, node, k_e);
    %%          _    -> compute_reassign(directory_load, node, k_p),
    %%                  clear_directory()
    %%      end
    %% compute_reassign:
    %%  for every node from heavist to lightest in directory 
    %%    if l_n / c_n > k 
    %%       balance such that (l_n + l_x) / c_n gets minimized
    %%  return assignment
    MyDirKeys = State#state.my_dirs,
    NewSchedule = manage_directories(MyDirKeys),
    State#state{schedule = NewSchedule};

handle_msg({get_state_response, MyRange}, State) ->
    Directories = get_all_directory_keys(),
    MyDirectories = [int_to_str(Dir) || Dir <- Directories, intervals:in(Dir, MyRange)],
    ?TRACE("~p: I am responsible for ~p~n", [self(), MyDirectories]),
    State#state{my_dirs = MyDirectories};

handle_msg(Msg, State) ->
    ?TRACE("Unknown message: ~p~n", [Msg]),
    State.

%%%%%%%%%%%%%%%%%% DHT Node interaction %%%%%%%%%%%%%%%%%%%%%%%

%% @doc Load balancing messages received by the dht node.
-spec handle_dht_msg(dht_message(), dht_node_state:state()) -> dht_node_state:state().
handle_dht_msg({request_load, ReplyPid}, DhtState) ->
    %MyNodeDetails = dht_node_state:details(DhtState),
    %Capacity = node_details:get
    %% TODO make this more generic
    NodeDetails = dht_node_state:details(DhtState),
    LoadInfo = lb_info:new(NodeDetails),
    comm:send_local(ReplyPid, {post_load, LoadInfo}),
    DhtState;

handle_dht_msg(Msg, DhtState) ->
    ?TRACE("Unknown message: ~p~n", [Msg]),
    DhtState.

%%%%%%%%%%%%%%%%% Directory Management %%%%%%%%%%%%%%%

manage_directories(DirKeys) ->
    manage_directories(DirKeys, []).

manage_directories([], Schedule) ->
    Schedule;
manage_directories([DirKey | Other], Schedule) ->
    DirSchedule = directory_routine(DirKey, periodic, Schedule),
    manage_directories(Other, Schedule ++ DirSchedule).

-spec directory_routine(directory_name(), periodic | emergency, schedule()) -> schedule().
directory_routine(DirKey, Type, Schedule) ->
    %% Because of the lack of virtual servers/nodes, the load
    %% balancing is differs from the paper here. We try to
    %% balance the most loaded node with the least loaded
    %% node.
    %% TODO Some preference should be given to neighboring
    %%      nodes to avoid too many jumps.
    {TLog, Directory} = get_directory(DirKey),
    case dir_is_empty(Directory) of
        true -> Schedule; %% TODO why return old schedule here?
        false ->
            Pool = Directory#directory.pool,
            K = case Type of
                    periodic ->
                        %% clear directory only for periodic
                        NewDirectory = dir_clear_load(Directory),
                        set_directory(TLog, NewDirectory),
                        AvgUtil = gb_sets:fold(fun(El, Acc) -> Acc + lb_info:get_load(El) end, 0, Pool) / gb_sets:size(Pool),
                        %% TODO calculate threshold according to metric
                        (1 + AvgUtil) / 2;
                    emergency ->
                        1.0
                end,
            ?TRACE("Threshold: ~p~n", [K]),
            LightNodes = gb_sets:filter(fun(El) -> lb_info:get_load(El) =< K end, Pool),
            HeavyNodes = gb_sets:filter(fun(El) -> lb_info:get_load(El) >  K end, Pool),
            ScheduleNew = find_matches(LightNodes, HeavyNodes, []),
            ?TRACE("New schedule: ~p~n", [ScheduleNew]),
            ScheduleNew
    end.

-spec find_matches(gb_set(), gb_set(), []) -> [schedule()]. 
find_matches(LightNodes, HeavyNodes, Result) ->
    case gb_sets:size(LightNodes) > 0 andalso gb_sets:size(HeavyNodes) > 0 of
        true ->
            {LightNode, LightNodes2} = gb_sets:take_smallest(LightNodes),
            {HeavyNode, HeavyNodes2} = gb_sets:take_largest(HeavyNodes),
            find_matches(LightNodes2, HeavyNodes2, [#reassign{light = LightNode, heavy = HeavyNode} | Result]);
        false ->
            %% TODO we want to have the heaviest load first, this could be more efficient...
            lists:reverse(Result)
    end.

-spec get_all_directory_keys() ->  ?RT:key().
get_all_directory_keys() ->
    [get_directory_key_by_number(N) || N <- lists:seq(1, ?NUM_DIRECTORIES)].

-spec get_random_directory_key() ->  ?RT:key().
get_random_directory_key() ->
    Rand = randoms:rand_uniform(1, ?NUM_DIRECTORIES+1),
    get_directory_key_by_number(Rand).

-spec get_directory_key_by_number(pos_integer()) -> ?RT:key().
get_directory_key_by_number(N) when N > 0 ->
    ?RT:hash_key("lb_active_dir" ++ int_to_str(N)).

%% selects two directories at random and returns the one which least nodes reported to
get_random_directory() ->
    {_TLog1, RandDir1} = get_directory(int_to_str(get_random_directory_key())),
    {_TLog2, RandDir2} = get_directory(int_to_str(get_random_directory_key())),
    case RandDir1#directory.num_reported >= RandDir2#directory.num_reported of
        true -> RandDir1;
        _    -> RandDir2
    end.

%% @doc Check if directories exist, if not create them.
-spec create_directories(non_neg_integer()) -> ok.
create_directories(0) ->
    ok;
create_directories(N) when N > 0 ->
    Key = int_to_str(get_directory_key_by_number(N)),
    TLog = api_tx:new_tlog(),

    case api_tx:read(TLog, Key) of
        {_TLog2, {ok, _Value}} ->
            create_directories(N-1);
        {TLog2, {fail, not_found}} ->
            {TLog3, _Result} = api_tx:write(TLog2, Key, #directory{name = Key}),
            case api_tx:commit(TLog3) of
                {ok} ->
                    create_directories(N-1);
                {fail, abort, [Key]} ->
                    create_directories(N)
            end
    end.

%% TODO make the three below more generic...
-spec pop_load_in_directory(directory_name()) -> directory().
pop_load_in_directory(DirKey) ->
    TLog = api_tx:new_tlog(),
    case api_tx:read(TLog, DirKey) of
        {TLog2, {ok, Directory}} ->
            case api_tx:req_list(TLog2, [{write, DirKey, dir_clear_load(Directory)}, {commit}]) of
                {[], ok, ok} -> Directory;
                Error -> log:log(warn, "~p: Failed to save directory ~p because of failed transaction: ~p", [?MODULE, DirKey, Error]),
                         timer:sleep(10),
                         pop_load_in_directory(DirKey)
            end;
        {_TLog2, {fail, not_found}} ->
            log:log(warn, "~p: Directory not found while posting load. This should never happen...", [?MODULE])
    end.

-spec post_load_to_directory(lb_info:lb_info(), directory_name()) -> ok.
post_load_to_directory(Load, DirKey) ->
    TLog = api_tx:new_tlog(),
    case api_tx:read(TLog, DirKey) of
        {TLog2, {ok, Content}} ->
            ContentNew = dir_add_load(Load, Content),
            {TLog3, _Result} = api_tx:write(TLog2, DirKey, ContentNew),
            case api_tx:commit(TLog3) of
                {ok} ->
                    ok;
                {fail, abort, [DirKey]} ->
                    log:log(warn, "~p: Failed to write to directory, retrying...", [?MODULE]),
                    timer:sleep(10),
                    post_load_to_directory(Load, DirKey)
            end;
        {_TLog2, {fail, not_found}} ->
            log:log(warn, "~p: Directory not found while posting load. This should never happen...", [?MODULE]),
            ok
    end.

-spec get_directory(directory_name()) -> {api_tx:tlog(), directory()}.
get_directory(DirKey) ->
    TLog = api_tx:new_tlog(),
    case api_tx:read(TLog, DirKey) of
        {TLog2, {ok, Directory}} ->
            %?TRACE("~p: Got directory: ~p~n", [?MODULE, Directory]),
            {TLog2, Directory};
        _ -> 
            log:log(warn, "~p: Directory not found while posting load. This should never happen...", [?MODULE]),
            get_directory(DirKey)
    end.

-spec set_directory(api_tx:tlog(), directory()) -> ok | failed.
set_directory(TLog, Directory) ->
    DirKey = Directory#directory.name,
    case api_tx:req_list(TLog, [{write, DirKey, Directory}, {commit}]) of
        {[], [{ok}, {ok}]} -> 
            ok;
        Error ->
            log:log(warn, "~p: Failed to save directory ~p because of failed transaction: ~p", [?MODULE, DirKey, Error]),
            % TODO What to do here?
            failed
    end.

%%%%%%%%%%%%%%%% Directory record %%%%%%%%%%%%%%%%%%%%%%

-spec dir_add_load(lb_info:lb_info(), directory()) -> directory().
dir_add_load(Load, Directory) ->
    Pool = Directory#directory.pool,
    PoolNew = gb_sets:add(Load, Pool),
    NumReported = Directory#directory.num_reported,
    Directory#directory{pool = PoolNew, num_reported = NumReported + 1}.

-spec dir_clear_load(directory()) -> directory().
dir_clear_load(Directory) ->
    Directory#directory{pool = gb_sets:new(), num_reported = 0}.

%% dir_set_schedule(Schedule, Directory) ->
%%     Directory#directory{schedule = Schedule}.

-spec dir_is_empty(directory()) -> boolean().
dir_is_empty(Directory) ->
    Pool = Directory#directory.pool,
    gb_sets:is_empty(Pool).

%%%%%%%%%%%%%% Reassignments %%%%%%%%%%%%%%%%%%%%%

-spec perform_transfer(schedule()) -> ok.
perform_transfer([]) ->
    ok;
perform_transfer([#reassign{light = LightNode, heavy = HeavyNode} | Other]) ->
    ?TRACE("~p: Reassigning ~p (light: ~p) and ~p (heavy: ~p)~n", [?MODULE, lb_info:get_node(LightNode), lb_info:get_load(LightNode), lb_info:get_node(HeavyNode), lb_info:get_load(HeavyNode)]),
    lb_active:balance_nodes(LightNode, HeavyNode),
%%     case lb_info:neighbors(LightNode, HeavyNode) of
%%         true  -> comm:send(node:pidX(DhtNode), {lb_active, {slide, LightNode}});
%%         false -> comm:send(node:pidX(DhtNode), {lb_active, {jump, LightNode}})
%%     end,
    perform_transfer(Other).

%%%%%%%%%%%%
%% State
%%
-spec state_get(capacity, state()) -> number();
               (my_dirs,  state()) -> [directory_name()].
state_get(Key, #state{my_dirs = Dirs}) ->
    case Key of
        my_dirs  -> Dirs
    end.

%%%%%%%%%%%%
%% Helpers
%%

-spec request_dht_range() -> ok.
request_dht_range() ->
    MyDHT = pid_groups:get_my(dht_node),
    comm:send_local(MyDHT, {get_state, comm:this(), my_range}).

-spec request_dht_load() -> ok.
request_dht_load() ->
    MyDHT = pid_groups:find_a(dht_node),
    comm:send_local(MyDHT, {lb_active, {request_load, self()}}).

-spec int_to_str(integer()) -> string().
int_to_str(N) ->
    erlang:integer_to_list(N).

-spec trigger(trigger()) -> ok.
trigger(Trigger) ->
    Interval = config:read(lb_active_interval),
    msg_delay:send_trigger(Interval div 1000, {Trigger}).

-spec get_web_debug_kv(state()) -> [{string(), string()}].
get_web_debug_kv(State) ->
    [{"state", webhelpers:html_pre("~p", [State])}].

-spec check_config() -> boolean().
check_config() ->
    % lb_active_interval => publish_interval
    % lb_active_intervak => balance_interval
    % emergency treshold
    true.