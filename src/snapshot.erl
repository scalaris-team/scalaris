%  @copyright 2012 Zuse Institute Berlin

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

%% @author Stefan Keidel <keidel@informatik.hu-berlin.de>
%% @doc Generic snapshot-related functions (utils)
%% @version $Id$
-module(snapshot).
-author('keidel@informatik.hu-berlin.de').
-vsn('$Id$').
-include("scalaris.hrl").

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(_X,_Y), ok).

-export([snapshot_is_done/1,on_do_snapshot/3,on_local_snapshot_is_done/1]).

-spec snapshot_is_done(dht_node_state:state()) -> boolean().
snapshot_is_done(DHTNodeState) ->
    SnapState = dht_node_state:get(DHTNodeState, snapshot_state),
    DB = dht_node_state:get(DHTNodeState,db),
    ?TRACE("~p snapshot:snapshot_is_done: ~p ~p ~p ~n",[comm:this(),snapshot_state:is_in_progress(SnapState),?DB:snapshot_is_running(DB),?DB:snapshot_is_lockfree(DB)]),
    case {snapshot_state:is_in_progress(SnapState),?DB:snapshot_is_running(DB),?DB:snapshot_is_lockfree(DB)} of
        {true,true,true} -> true;
        _ ->
            ?TRACE("~p snapshot:snapshot_is_done db: ~p~n",[comm:this(),DB]),
            ?TRACE("~p snapshot:snapshot_is_done db data: ~p~n",[comm:this(),?DB:get_data(DB)]),
            ?TRACE("~p snapshot:snapshot_is_done snapshot data: ~p~n",[comm:this(),?DB:get_snapshot_data(DB)]),
            false
    end.
    
-spec on_do_snapshot(non_neg_integer(),any(),dht_node_state:state()) -> dht_node_state:state().
on_do_snapshot(SnapNumber, Leader, DHTNodeState) ->
    ?TRACE("~p snapshot:on_do_snapshot: ~p ~p~n",[comm:this(),SnapNumber,Leader]),
    SnapState = dht_node_state:get(DHTNodeState, snapshot_state),
    ?TRACE("~p snapshot:on_do_snapshot: snapshot state is ~p~n",[comm:this(),SnapState]),
    case snapshot_state:is_in_progress(SnapState) of
        true -> % old snapshot is still running or current snapshot is already running
            case snapshot_state:get_number(SnapState) < SnapNumber of
                true -> % currently running snapshot is old
                    ?TRACE("~p snapshot:on_do_snapshot: new snapshot arrived~n",[comm:this()]),
                    msg_snapshot_leaders_err("New snapshot arrived",
                                             snapshot_state:get_number(SnapState),
                                             dht_node_state:get(DHTNodeState,my_range),
                                             snapshot_state:get_leaders(SnapState)),
                    NewState = delete_and_init_snapshot(SnapNumber,Leader,DHTNodeState);
                false -> % the current snapshot is the same as the incoming one or newer
                    case snapshot_state:get_number(SnapState) =:= SnapNumber of
                        true ->
                            % additional msg for current snapshot -> add leader to dht node state for later messaging
                            NewSnapState = snapshot_state:add_leader(SnapState, Leader),
                            NewState = dht_node_state:set_snapshot_state(DHTNodeState, NewSnapState);
                        false ->
                            % old snapshot -> ignore (or error msg?)
                            NewState = DHTNodeState
                    end
            end;
        false ->
            % no snapshot is progress -> init new
            ?TRACE("~p snapshot:on_do_snapshot: init new snapshot~n",[comm:this()]),
            NewState = delete_and_init_snapshot(SnapNumber,Leader,DHTNodeState)
    end,
    % check if snapshot is already done (i.e. there were no active transactions when the snapshot arrived)
    case snapshot:snapshot_is_done(NewState) of
        true ->
            ?TRACE("~p snapshot:on_do_snapshot: snapshot is done~n",[comm:this()]),
            comm:send(comm:this(), {local_snapshot_is_done});
        false ->
            ?TRACE("~p snapshot:on_do_snapshot: snapshot is not done~n",[comm:this()]),
            ok
    end,
    % return
    NewState.

-spec on_local_snapshot_is_done(dht_node_state:state()) -> dht_node_state:state().
on_local_snapshot_is_done(DHTNodeState) ->
    ?TRACE("~p snapshot:on_local_snapshot_is_done~n",[comm:this()]),
    Db = dht_node_state:get(DHTNodeState,db),
    SnapState = dht_node_state:get(DHTNodeState,snapshot_state),
    
    % collect local state and send it
    SnapNumber = snapshot_state:get_number(SnapState),
    Data = ?DB:join_snapshot_data(Db),
    Leaders = snapshot_state:get_leaders(SnapState),
    DBRange = dht_node_state:get(DHTNodeState,my_range),
    msg_snapshot_leaders(Data,SnapNumber,DBRange,Leaders),

    % cleanup
    NewDB = ?DB:delete_snapshot(dht_node_state:get(DHTNodeState,db)),
    NewSnapState = snapshot_state:stop_progress(SnapState),
    NewState = dht_node_state:set_snapshot_state(DHTNodeState, NewSnapState),

    dht_node_state:set_db(NewState, NewDB).

msg_snapshot_leaders(Data,SnapNumber,DBRange,[Leader | RestOfLeaders]) ->
    comm:send(Leader, {local_snapshot_done,comm:this(),SnapNumber,DBRange,Data}),
    msg_snapshot_leaders(Data,SnapNumber,DBRange,RestOfLeaders);
msg_snapshot_leaders(_Data,_SnapNumber,_DBRange,[]) ->
    ok.

msg_snapshot_leaders_err(Msg,SnapNumber,DBRange,[Leader | RestOfLeaders]) ->
    comm:send(Leader, {local_snapshot_failed,comm:this(),SnapNumber,DBRange,Msg}),
    msg_snapshot_leaders_err(Msg,SnapNumber,DBRange,RestOfLeaders);
msg_snapshot_leaders_err(_Msg,_SnapNumber,_DBRange,[]) ->
    ok.

-spec delete_and_init_snapshot(non_neg_integer(),any(),dht_node_state:state()) -> dht_node_state:state().
delete_and_init_snapshot(SnapNumber,Leader,DHTNodeState) ->
    NewDB = ?DB:init_snapshot(?DB:delete_snapshot(dht_node_state:get(DHTNodeState,db))),
    ?TRACE("new DB: ~p~n",[NewDB]),
    TmpSnapState = snapshot_state:new(SnapNumber, true, [Leader]),
    TmpState = dht_node_state:set_db(DHTNodeState, NewDB),
    dht_node_state:set_snapshot_state(TmpState, TmpSnapState).

