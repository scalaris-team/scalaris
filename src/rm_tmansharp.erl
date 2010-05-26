%  @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Christian Hennig <hennig@zib.de>
%%% @doc    T-Man ring maintenance
%%% @end
%% @version $Id$
-module(rm_tmansharp).
-author('hennig@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([init/1, on/2]).

-behavior(rm_beh).
-behavior(gen_component).

-export([start_link/1, check_config/0]).

% unit testing
-export([merge/2, rank/2, get_pred/1, get_succ/1, get_preds/1, get_succs/1]).

-type(state() :: {Id             :: ?RT:key(),
                  Me             :: node:node_type(),
                  View           :: nodelist:nodelist(),
                  RandViewSize   :: pos_integer(),
                  Interval       :: pos_integer(),
                  AktToken       :: any(),
                  AktPred        :: node:node_type(),
                  AktSucc        :: node:node_type(),
                  RandomCache    :: [node:node_type()]} % random cyclon nodes}
     | {uninit, QueuedMessages::[cs_send:message()]}).

% accepted messages
-type(message() ::
    {init, Id::?RT:key(), Me::node_details:node_type(), Predecessor::node_details:node_type(), Successor::node:node_type()} |
    {stabilize, AktToken::any()} |
    {cy_cache, Cache::nodelist:nodelist()} |
    {rm_buffer, Q::node:node_type(), Buffer_q::nodelist:nodelist()} |
    {rm_buffer_response, Buffer_p::nodelist:nodelist()} |
    {zombie, Node::node:node_type()} |
    {crash, DeadPid::cs_send:mypid()} |
    {'$gen_cast', {debug_info, Requestor::cs_send:erl_local_pid()}} |
    {check_ring, Token::non_neg_integer(), Master::node:node_type()} |
    {init_check_ring, Token::non_neg_integer()} |
    {notify_new_pred, Pred::node:node_type()} |
    {notify_new_succ, Succ::node:node_type()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc spawns a chord-like ring maintenance process
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    start_link(InstanceId, []).

-spec start_link(instanceid(), [any()]) -> {ok, pid()}.
start_link(InstanceId, Options) ->
   gen_component:start_link(?MODULE, [InstanceId, Options], [{register, InstanceId, ring_maintenance}]).

-spec init([instanceid() | [any()]]) -> {uninit, QueuedMessages::[]}.
init(_Args) ->
    log:log(info,"[ RM ~p ] starting ring maintainer TMAN~n", [self()]),
    dn_cache:subscribe(),
    cs_send:send_local(get_cs_pid(), {init_rm, self()}),
    {uninit, []}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc message handler
-spec on(message(), state()) -> state().
on({init, Id, Me, Predecessor, Successor}, {uninit, QueuedMessages}) ->
    fd:subscribe(lists:usort([node:pidX(Node) || Node <- [Predecessor | Successor]])),
    Token = 0,
    cs_send:send_local_after(0, self(), {stabilize, Token}),
    cs_send:send_queued_messages(QueuedMessages),
    {Id, Me, [Predecessor | Successor], config:read(cyclon_cache_size), stabilizationInterval_min(), Token, Predecessor, Successor, []};

on(Msg, {uninit, QueuedMessages}) ->
    {uninit, [Msg | QueuedMessages]};

on({stabilize, AktToken},
   {_Id, Me, View, RandViewSize, Interval, AktToken, _AktPred, _AktSucc, RandomCache} = State) -> % new stabilization interval
    % Triger an update of the Random view
    cyclon:get_subset_rand(RandViewSize),
    RndView = get_RndView(RandViewSize,RandomCache),
    %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
    P = selectPeer(rank(View++RndView,node:id(Me)),Me),
    %io:format("~p~n",[{Preds,Succs,RndView,Me}]),
    % Test for being alone:
    case node:equals(P, Me) of
        true ->
            rm_beh:update_neighbors(nodelist:new_neighborhood(Me));
        false ->
            Msg = {rm_buffer, Me, extractMessage(View++[Me]++RndView, P)},
            cs_send:send_to_group_member(node:pidX(P), ring_maintenance, Msg)
    end,
    cs_send:send_local_after(Interval, self(), {stabilize, AktToken}),
    State;

on({stabilize,_}, State) ->
    State;

on({cy_cache, NewCache},
   {Id, Me, View, RandViewSize, Interval, AktToken, AktPred, AktSucc, _RandomCache}) ->
    {Id, Me, View, RandViewSize, Interval, AktToken, AktPred, AktSucc, NewCache};

on({rm_buffer, Q, Buffer_q},
   {Id, Me, View, RandViewSize, Interval, AktToken, AktPred, AktSucc, RandomCache}) ->
    RndView = get_RndView(RandViewSize, RandomCache),
    Msg = {rm_buffer_response, extractMessage(View++[Me]++RndView, Q)},
    cs_send:send_to_group_member(node:pidX(Q), ring_maintenance, Msg),
    %io:format("after_send~p~n", [self()]),
    NewView = rank(View++Buffer_q++RndView, node:id(Me)),
    %io:format("after_rank~p~n", [self()]),
    %SuccsNew=get_succs(NewView),
    %PredsNew=get_preds(NewView),
    {NewAktPred, NewAktSucc} = update_dht_node(Me, NewView, AktPred, AktSucc),
    update_failuredetector(View, NewView),
    NewInterval = new_interval(View, NewView, Interval),
    NewToken = AktToken + 1,
    cs_send:send_local_after(NewInterval, self(), {stabilize, NewToken}),
    %io:format("loop~p~n", [self()]),
    {Id, Me, NewView, RandViewSize, NewInterval, NewToken, NewAktPred, NewAktSucc, RandomCache};

on({rm_buffer_response, Buffer_p},
   {Id, Me, View, RandViewSize, Interval, AktToken, AktPred, AktSucc, RandomCache})->
    RndView = get_RndView(RandViewSize, RandomCache),
    %log:log(debug, " [RM | ~p ] RNDVIEW: ~p", [self(),RndView]),
    Buffer = rank(View++Buffer_p++RndView, node:id(Me)),
    %io:format("after_rank~p~n",[self()]),
    ViewSize = config:read(succ_list_length) + config:read(pred_list_length),
    NewView = lists:sublist(Buffer, ViewSize),
    {NewAktPred, NewAktSucc} = update_dht_node(Me, View, AktPred, AktSucc),
    update_failuredetector(View, NewView),
    NewInterval = new_interval(View, NewView, Interval),
    %inc RandViewSize (no error detected)
    RandViewSizeNew = case RandViewSize < config:read(cyclon_cache_size) of
                          true  -> RandViewSize+1;
                          false -> RandViewSize
                      end,
    NewToken = AktToken + 1,
    cs_send:send_local_after(NewInterval, self(), {stabilize, NewToken}),
    {Id, Me, NewView, RandViewSizeNew, NewInterval, NewToken, NewAktPred, NewAktSucc, RandomCache};

on({zombie, Node},
   {Id, Me, View, RandViewSize, Interval, AktToken, AktPred, AktSucc, RandomCache}) ->
    NewToken = AktToken + 1,
    erlang:send(self(), {stabilize, NewToken}),
    %TODO: Inform Cyclon !!!!
    {Id, Me, View, RandViewSize, Interval, NewToken, AktPred, AktSucc, [Node|RandomCache]};

on({crash, DeadPid},
   {Id, Me, View, _RandViewSize, _Interval, AktToken, AktPred, AktSucc, RandomCache}) ->
    NewView = filter(DeadPid, View),
    NewCache = filter(DeadPid, RandomCache),
    update_failuredetector(View, NewView),
    NewToken = AktToken + 1,
    erlang:send(self(), {stabilize, NewToken}),
    {Id, Me, NewView, 0, stabilizationInterval_min(), NewToken, AktPred, AktSucc, NewCache};

on({'$gen_cast', {debug_info, Requestor}},
   {_Id, Me, View, _RandViewSize, _Interval, _AktToken, _AktPred, _AktSucc, _RandomCache} = State) ->
    cs_send:send_local(Requestor,
                       {debug_info_response,
                        [{"self", lists:flatten(io_lib:format("~p", [Me]))},
                         {"pred", lists:flatten(io_lib:format("~p", [get_preds(View)]))},
                         {"succs", lists:flatten(io_lib:format("~p", [get_succs(View)]))}]}),
    State;

on({check_ring, 0, Me},
   {_Id, Me, _View, _RandViewSize, _Interval, _AktToken, _AktPred, _AktSucc, _RandomCache} = State) ->
    io:format(" [RM ] CheckRing   OK  ~n"),
    State;

on({check_ring, Token, Me},
   {_Id, Me, _View, _RandViewSize, _Interval, _AktToken, _AktPred, _AktSucc, _RandomCache} = State) ->
    io:format(" [RM ] Token back with Value: ~p~n", [Token]),
    State;

on({check_ring, 0, Master},
   {_Id, Me, _View, _RandViewSize, _Interval, _AktToken, _AktPred, _AktSucc, _RandomCache} = State) ->
    io:format(" [RM ] CheckRing  reach TTL in Node ~p not in ~p~n", [Master, Me]),
    State;

on({check_ring, Token, Master},
   {_Id, _Me, _View, _RandViewSize, _Interval, _AktToken, AktPred, _AktSucc, _RandomCache} = State) ->
    cs_send:send_to_group_member(node:pidX(AktPred), ring_maintenance, {check_ring, Token-1, Master}),
    State;

on({init_check_ring, Token},
   {_Id, Me, _View, _RandViewSize, _Interval, _AktToken, AktPred, _AktSucc, _RandomCache} = State) ->
    cs_send:send_to_group_member(node:pidX(AktPred), ring_maintenance, {check_ring, Token-1, Me}),
    State;

on({notify_new_pred, _NewPred}, State) ->
    %% @TODO use the new predecessor info
    State;

on({notify_new_succ, _NewSucc}, State) ->
    %% @TODO use the new successor info
    State;

on(_, _State) ->
    unknown_event.

%% @doc Checks whether config parameters of the rm_tmansharp process exist and
%%      are valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(stabilization_interval_min) and
    config:is_greater_than(stabilization_interval_min, 0) and

    config:is_integer(stabilization_interval_max) and
    config:is_greater_than(stabilization_interval_max, 0) and
    config:is_greater_than_equal(stabilization_interval_max, stabilization_interval_min) and

    config:is_integer(cyclon_cache_size) and
    config:is_greater_than(cyclon_cache_size, 2) and

    config:is_integer(succ_list_length) and
    config:is_greater_than_equal(succ_list_length, 0) and

    config:is_integer(pred_list_length) and
    config:is_greater_than_equal(pred_list_length, 0).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc merge two successor lists into one
%%      and sort by identifier
rank(MergedList, Id) ->
    %io:format("--------------------------------- ~p ~n",[Id]),
    %io:format("in: ~p ~p ~n",[self(),MergedList]),
    Order = fun(A, B) ->
            node:id(A) =< node:id(B)
            %A=<B
        end,
    Larger  = lists:usort(Order, [X || X <- MergedList, node:id(X) >  Id]),
    Equal   = lists:usort(Order, [X || X <- MergedList, node:id(X) =:= Id]),
    Smaller = lists:usort(Order, [X || X <- MergedList, node:id(X) <  Id]),

    H1 = Larger++Smaller,
    Half = length(H1) div 2,
    {Succs,Preds} = lists:split(Half,H1),
    Return=lists:sublist(merge(Succs,lists:reverse(Preds)),10), %config:read(succ_list_length)+config:read(pred_list_length)

    %io:format("return: ~p ~p ~n",[self(),Return]),
    A =case Return of
        []  -> Equal;
        _   -> Return
    end,
    %io:format("out: ~p ~p ~n",[self(),A]),
    A.

selectPeer([],Me) ->
    Me;
selectPeer(View,_) ->
    NTH = randoms:rand_uniform(1, 3),
    case (NTH=<length(View)) of
        true -> lists:nth( NTH,View);
        false -> lists:nth(length(View),View)
    end.

extractMessage(View,P) ->
    lists:sublist(rank(View,node:id(P)),10).

merge([H1|T1],[H2|T2]) ->
    [H1,H2]++merge(T1,T2);
merge([],[T|H]) ->
    [T|H];
merge([],X) ->
    X;
merge(X,[]) ->
    X;
merge([],[]) ->
    [].

get_succs([T]) ->
    [T];
get_succs(View) ->
    get_every_nth(View,1,0).
get_preds([T]) ->
    [T];
get_preds(View) ->
    get_every_nth(View,1,1).

get_succ([H|_]) ->
    H.

get_pred([H|T]) ->
    case T of
        []  -> H;
        _   -> get_succ(T)
    end.

get_every_nth([],_,_) ->
    [];
get_every_nth([H|T],Nth,Offset) ->
    case Offset of
        0 ->  [H|get_every_nth(T,Nth,Nth)];
        _ ->  get_every_nth(T,Nth,Offset-1)
    end.

%-spec(filter/2 :: (cs_send:mypid(), list(node:node_type()) -> list(node:node_type()).
filter(_Pid, []) ->
    [];
filter(Pid, [Succ | Rest]) ->
    case node:equals(Pid, Succ) of
	true ->
        %Hook for DeadNodeCache
        dn_cache:add_zombie_candidate(Succ),

	    filter(Pid, Rest);
	false ->
	    [Succ | filter(Pid, Rest)]
    end.

%% @doc get a peer form the cycloncache which is alive
get_RndView(N,Cache) ->
     lists:sublist(Cache, N).

% @doc Check if change of failuredetector is necessary
update_failuredetector(OldView,NewView) ->
    case (NewView =/= OldView) of
        true ->
            NewPids = [node:pidX(Node) || Node <- util:minus(NewView,OldView)],
            OldPids = [node:pidX(Node) || Node <- util:minus(OldView, NewView)],
            fd:update_subscriptions(OldPids, NewPids);
        false ->
            ok
    end.
	
% @doc informed the dht_node for new [succ|pred] if necessary
-spec update_dht_node(Me::node:node_type(), View::nodelist:nodelist(), AktPred::node:node_type(), AktSucc::node:node_type()) -> {NewAktPred::node:node_type(), NewAktSucc::node:node_type()}.
update_dht_node(Me, View, AktPred, AktSucc) ->
    NewAktPred = get_pred(View),
    NewAktSucc = get_succ(View),
    
    case (AktPred =/= NewAktPred) orelse (AktSucc =/= NewAktSucc) of
        true ->
            rm_beh:update_neighbors(nodelist:new_neighborhood(NewAktPred, Me, NewAktSucc));
        false ->
            ok
    end,
    {NewAktPred, NewAktSucc}.

% @doc adapt the Tman-interval
new_interval(View,NewView,Interval) ->
    case (View==NewView) of
        true ->
            case (Interval >= stabilizationInterval_max() ) of
                true -> stabilizationInterval_max();
                false -> Interval + ((stabilizationInterval_max() - stabilizationInterval_min()) div 10)
            end;
        false ->
            case (Interval - (stabilizationInterval_max()-stabilizationInterval_min()) div 2) =< (stabilizationInterval_min()  ) of
                true -> stabilizationInterval_min() ;
                false -> Interval - (stabilizationInterval_max()-stabilizationInterval_min()) div 2
            end
    end.

% print_view(Me,View) ->
%     io:format("[~p] -> ",[node:pidX(Me)]),
%     [io:format("~p",[node:pidX(Node)]) || Node <- View],
%     io:format("~n").

% @private

% get Pid of assigned dht_node
get_cs_pid() ->
    process_dictionary:get_group_member(dht_node).

%% @doc the interval between two stabilization runs Max
%% @spec stabilizationInterval_max() -> integer() | failed
stabilizationInterval_max() ->
    config:read(stabilization_interval_max).

%% @doc the interval between two stabilization runs Min
%% @spec stabilizationInterval_min() -> integer() | failed
stabilizationInterval_min() ->
    config:read(stabilization_interval_min).
