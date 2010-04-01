%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : webhelpers.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created : 16 Apr 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%%            2008 onScale solutions
%% @version $Id$
-module(webhelpers).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("yaws_api.hrl").
-include("../include/scalaris.hrl").

-export([getLoadRendered/0, getRingChart/0, getRingRendered/0,
         getIndexedRingRendered/0, lookup/1, set_key/2, isPost/1,
         getVivaldiMap/0]).


%% @doc checks whether the current request is a post operation
%% @spec isPost(A) -> boolean()
%%   A = http_request
isPost(A) ->
    Method = (A#arg.req)#http_request.method,
    Method == 'POST'.

%%%-----------------------------Lookup/Put---------------------------

lookup(Key) ->
%%    timer:tc(cs_api, read, [Key]).
    timer:tc(transaction_api, quorum_read, [Key]).

set_key(Key, Value) ->
    timer:tc(transaction_api, single_write, [Key, Value]).

%%%-----------------------------Load----------------------------------

getLoad() ->
    boot_server:node_list(),
    Nodes =
        receive
            {get_list_response,X} ->
                X
        after 2000 ->
            {failed}
        end,
    get_load(Nodes).
    
get_load([Head | Tail]) ->
    cs_send:send(Head , {get_load, cs_send:this()}),
    receive
	{get_load_response, Node, Value} -> [{ok, Node, Value} | get_load(Tail)]
    after
	2000 ->
	    [{failed, Head} | get_load(Tail)]
    end;
get_load([]) ->
    [].


%%%-----------------------------Load----------------------------------

getLoadRendered() ->
    Load = getLoad(),
    {table, [{bgcolor, "#cccccc"}, {border, "0"}, {cellpadding, "2"}, {cellspacing, "2"}, {width, "90%"}],
     [{tr, [],
       [
	{td, [{bgcolor, "#336699"}, {width, "48%"}], {font, [{color, "white"}], "Node"}},
	{td, [{bgcolor, "#336699"}, {valign, "top"}, {width, "16%"}], {font, [{color, "white"}], "Load"}}
       ]},
      renderLoad(Load)
     ]
    }.

renderLoad([{ok, Node, Value} | Tail]) ->
    [{tr, [], 
      [
       {td, [], io_lib:format('~p', [Node])},
       {td, [], io_lib:format('~p', [Value])}
      ]}, renderLoad(Tail)];
renderLoad([{failed, Node} | Tail]) ->
    [{tr, [], 
      [
       {td, [], io_lib:format('~p', [Node])},
       {td, [], "-"}
      ]}, renderLoad(Tail)];
renderLoad([]) ->
    [].

%%%--------------------------Vivaldi-Map------------------------------
getVivaldiMap() ->
    boot_server:node_list(),
    Nodes =
        receive
            {get_list_response,X} ->
            X
        after 2000 ->
            log:log(error,"[ WH ] boot_server:node_list failed~n"),
            {failed}
        end,
    CC_list = lists:map(fun (Pid) -> get_vivaldi(Pid) end, Nodes),
    renderVivaldiMap(CC_list,Nodes).

get_vivaldi(Pid) ->
    cs_send:send_to_group_member(Pid,vivaldi, {get_coordinate,cs_send:this()}),
    receive
        {vivaldi_get_coordinate_response,Coordinate,_Confidence} ->
            Coordinate
    end.

renderVivaldiMap(CC_list,Nodes) ->
    {Min,Max} = get_min_max(CC_list),
    %io:format("Min: ~p Max: ~p~n",[Min,Max]),
    Xof=(lists:nth(1, Max)-lists:nth(1, Min))*0.1,
    Yof=(lists:nth(2, Max)-lists:nth(2, Min))*0.1,
    %io:format("~p ~p {~p ~p} ~n",[Min,Max,Xof,Yof]),
    Vbx=lists:nth(1, Min)-Xof,
    Vby=lists:nth(2, Min)-Yof,
    Vbw=(lists:nth(1, Max)-lists:nth(1, Min))+Xof*2,
    Vbh=(lists:nth(2, Max)-lists:nth(2, Min))+Yof*2,
    
    R=(Xof+Yof)*0.1,
    Head=io_lib:format("<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"~p,~p,~p,~p\">~n",
                       [Vbx,Vby,Vbw,Vbh])++
         io_lib:format("<line x1=\"~p\" y1=\"~p\" x2=\"~p\" y2=\"~p\" stroke=\"#111111\" stroke-width=\"~p\" />~n",
                       [lists:nth(1, Min)-R*1.5,lists:nth(2, Min),lists:nth(1, Min)-R*1.5,lists:nth(2, Max),R*0.1])++
        io_lib:format("<line x1=\"~p\" y1=\"~p\" x2=\"~p\" y2=\"~p\" stroke=\"#111111\" stroke-width=\"~p\" />~n",
                       [lists:nth(1, Min),lists:nth(2, Max)+R*1.5,lists:nth(1, Max),lists:nth(2, Max)+R*1.5,R*0.1])++
        io_lib:format("<text x=\"~p\" y=\"~p\" transform=\"rotate(90,~p,~p)\" style=\"font-size:~ppx;\"> ~p micro seconds </text>~n",
                       [lists:nth(1, Min)-R*4,
                        lists:nth(2, Min)+(lists:nth(2, Max)-lists:nth(2, Min))/3,
                        lists:nth(1, Min)-R*4,
                        lists:nth(2, Min)+(lists:nth(2, Max)-lists:nth(2, Min))/3,
                        R*2,
                        floor(lists:nth(1, Max)-lists:nth(1, Min))])++
        io_lib:format("<text x=\"~p\" y=\"~p\" style=\"font-size:~ppx;\"> ~p micro seconds </text>~n",
                       [lists:nth(1, Min)+(lists:nth(1, Max)-lists:nth(1, Min))/3,
                        lists:nth(2, Max)+R*4,
                         R*2,
                        floor(lists:nth(2, Max)-lists:nth(2, Min))])                    ,
        

    Content=gen_Nodes(CC_list,Nodes,R),
    Foot="</svg>",
    Head++Content++Foot.

floor(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T - 1
    end.



gen_Nodes([],_,_) ->
    "";
gen_Nodes([H|T],[HN|TN],R) ->
    Hi=255,
    Lo=0,

    
    S1 =  pid(HN),

    random:seed(S1,S1,S1),
    random:uniform(Hi-Lo)+Lo-1,
    C1 = random:uniform(Hi-Lo)+Lo-1,
    C2 = random:uniform(Hi-Lo)+Lo-1,
    C3 = random:uniform(Hi-Lo)+Lo-1,
    io_lib:format("<circle cx=\"~p\" cy=\"~p\" r=\"~p\" style=\"fill:rgb( ~p, ~p ,~p) ;\" />~n",[lists:nth(1, H),lists:nth(2, H),R,C1,C2,C3])
    ++gen_Nodes(T,TN,R).

get_min_max([]) ->
    {[],[]};
get_min_max([H|T]) ->
    {lists:foldl(fun(A,B) -> min_list(A,B) end,H ,T),
     lists:foldl(fun(A,B) -> max_list(A,B) end,H ,T)}.


min_list(L1,L2) ->
    lists:zipwith(fun(X,Y) ->
                   case X < Y of
                       true -> X;
                       _ -> Y
                   end
                   end, L1, L2).
max_list(L1,L2) ->
    lists:zipwith(fun(X,Y) ->
                   case X > Y of
                       true -> X;
                       _ -> Y
                   end
                   end, L1, L2).

-ifdef(TCP_LAYER).
pid({{A,B,C,D},I,_}) ->
    A+B+C+D+I.
-endif.

-ifdef(BUILTIN).
pid(X) ->
    list_to_integer(lists:nth(1, string:tokens(erlang:pid_to_list(X),"<>."))).
-endif.



%%%-----------------------------Ring----------------------------------

getRingChart() ->
    RealRing = statistics:get_ring_details(),
    Ring = lists:filter(fun (X) -> is_valid(X) end, RealRing),
    RingSize = length(Ring),
    if
	RingSize == 0 ->
	    {p, [], "empty ring"};
	RingSize == 1 ->
	    {img, [{src, "http://chart.apis.google.com/chart?cht=p&chco=008080&chd=t:1&chs=600x350"}], ""};
	true ->
            try
              PieURL = renderRingChart(Ring),
              LPie = length(PieURL),
              if LPie < 1023 ->
                      {p, [], [{img, [{src, PieURL}], ""}]};
                 true ->
                      throw({urlTooLong, PieURL})
              end
            catch
                _:_ -> {p, [], "Sorry, pie chart not available (too many nodes or other error)."}
            end
    end.

renderRingChart(Ring) ->
    URLstart = "http://chart.apis.google.com/chart?cht=p&chco=008080",
    Sizes = lists:map(
              fun ({ok,Node}) ->
                       Me_tmp = get_id(node_details:get(Node, node)),
                       Pred_tmp = get_id(hd(node_details:get(Node, predlist))),
                       if
                           (null == Me_tmp) orelse (null == Pred_tmp) ->
                               io_lib:format("1.0", []); % guess the size
                           true ->
                               Tmp = (Me_tmp - Pred_tmp) * 100 /
                                         16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,
                               Diff = if
                                          Tmp < 0.0 ->
                                              (Me_tmp +
                                                   16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
                                              - Pred_tmp) * 100 /
                                                  16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF;
                                          true -> Tmp
                                      end,
                               io_lib:format("~f", [Diff])
                       end
              end, Ring),
    Hostinfos = lists:map(
                  fun ({ok,Node}) ->
                           node_details:get(Node, hostname) ++ " (" ++
                               integer_to_list(node_details:get(Node, load)) ++
                               ")"
                  end, Ring),
    CHD = "chd=t:" ++ tl(
            lists:foldl(fun(X,S) -> S ++ "," ++ X end, "", Sizes)),
    CHS = "chs=600x350",
    CHL = "chl=" ++ tl(
            lists:foldl(fun(X,S) -> S ++ "|" ++ X end, "", Hostinfos)),
    URLstart ++ "&" ++ CHD ++ "&" ++ CHS ++ "&" ++ CHL.

getRingRendered() ->
    RealRing = statistics:get_ring_details(),
    Ring = lists:filter(fun (X) -> is_valid(X) end, RealRing),
    RingSize = length(Ring),
    if
	RingSize == 0 ->
	    {p, [], "empty ring"};
	true ->
	    {p, [],
	      [
	      {table, [{bgcolor, '#CCDCEE'}, {width, "100%"}],
	       [
		{tr, [{bgcolor, '#000099'}],
		 [
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Total Load"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Average Load"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load (std. deviation)"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Real Ring Size"}}}
		 ]
		},
		{tr, [],
		 [
                               {td, [], io_lib:format('~p', [statistics:get_total_load(Ring)])},
                               {td, [], io_lib:format('~p', [statistics:get_average_load(Ring)])},
                               {td, [], io_lib:format('~p', [statistics:get_load_std_deviation(Ring)])},
                               {td, [], io_lib:format('~p', [RingSize])}
                           ]
		}
	       ]
	      },
	      {br, []},
	      {table, [{bgcolor, '#CCDCEE'}, {width, "100%"}],
	       [{tr, [{bgcolor, '#000099'}],
		 [
		  {td, [{align, "center"}, {width,"200px"}], {strong, [], {font, [{color, "white"}], "Host"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Pred"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Node"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Succ"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "RTSize"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load"}}}
		 ]},
		lists:map(fun (Node) -> renderRing(Node) end, Ring)
	       ]
	      }
	     ]
	    }
    end.

renderRing({ok, Details}) ->
    Hostname = node_details:get(Details, hostname),
    PredList = node_details:get(Details, predlist),
    Node = node_details:get(Details, node),
    SuccList = node_details:get(Details, succlist),
    RTSize = node_details:get(Details, rt_size),
    Load = node_details:get(Details, load),
    {tr, [], 
      [
       {td, [], [get_flag(Hostname), io_lib:format('~p', [Hostname])]},
       {td, [], io_lib:format('~p', [lists:map(fun get_id/1, PredList)])},
       {td, [], io_lib:format('~p', [get_id(Node)])},
       {td, [], io_lib:format('~p', [lists:map(fun get_id/1, SuccList)])},
       {td, [], io_lib:format('~p', [RTSize])},
       {td, [], io_lib:format('~p', [Load])}
      ]};
renderRing({failed}) ->
    {tr, [], 
      [
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"}
      ]}.

getIndexedRingRendered() ->
    RealRing = statistics:get_ring_details(),
    Ring = lists:filter(fun (X) -> is_valid(X) end, RealRing),
    RingSize = length(Ring),
    if
	RingSize == 0 ->
	    {p, [], "empty ring"};
	true ->
	    {p, [],
	      [
	      {table, [{bgcolor, '#CCDCEE'}, {width, "100%"}],
	       [
		{tr, [{bgcolor, '#000099'}],
		 [
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Total Load"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Average Load"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load (std. deviation)"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Real Ring Size"}}}
		 ]
		},
		{tr, [],
		 [
		  {td, [], io_lib:format('~p', [statistics:get_total_load(Ring)])},
		  {td, [], io_lib:format('~p', [statistics:get_average_load(Ring)])},
		  {td, [], io_lib:format('~p', [statistics:get_load_std_deviation(Ring)])},
		  {td, [], io_lib:format('~p', [RingSize])}
		 ]
		}
	       ]
	      },
	      {br, []},
	      {table, [{bgcolor, '#CCDCEE'}, {width, "100%"}],
	       [{tr, [{bgcolor, '#000099'}],
		 [
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Host"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Pred Offset"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Node Index"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Succ Offsets"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "RTSize"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load"}}}
		 ]},
		lists:map(fun (Node) -> renderIndexedRing(Node, Ring) end, Ring)
	       ]
	      }
	     ]
	    }
    end.

renderIndexedRing({ok, Details}, Ring) ->
    Hostname = node_details:get(Details, hostname),
    PredList = node_details:get(Details, predlist),
    Node = node_details:get(Details, node),
    SuccList = node_details:get(Details, succlist),
    RTSize = node_details:get(Details, rt_size),
    Load = node_details:get(Details, load),
    MyIndex = get_indexed_id(Node, Ring),
    NIndex = length(Ring),
    PredIndex = lists:map(fun(Pred) -> get_indexed_pred_id(Pred, Ring, MyIndex, NIndex) end, PredList),
    SuccIndices = lists:map(fun(Succ) -> get_indexed_succ_id(Succ, Ring, MyIndex, NIndex) end, SuccList),
    [FirstSuccIndex|_] = SuccIndices,
    {tr, [],
      [
       {td, [], [get_flag(Hostname), io_lib:format('~p', [Hostname])]},
       case hd(PredIndex) == -1 of
           true->
               {td, [], io_lib:format('~p', [PredIndex])};
           false ->
               {td, [], io_lib:format('<span style="color:red">~p</span>', [PredIndex])}
       end,
       {td, [], io_lib:format('~p: ~p', [MyIndex, get_id(Node)])},
       case is_list(FirstSuccIndex) orelse FirstSuccIndex =/= 1 of
           true -> {td, [], io_lib:format('<span style="color:red">~p</span>', [SuccIndices])};
           false -> {td, [], io_lib:format('~p', [SuccIndices])}
       end,
       {td, [], io_lib:format('~p', [RTSize])},
       {td, [], io_lib:format('~p', [Load])}
      ]};

renderIndexedRing({failed}, _Ring) ->
    {tr, [],
      [
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"}
      ]}.

%%%-----------------------------Misc----------------------------------

get_id(Node) ->
    IsNull = node:is_null(Node),
    if
	IsNull ->
	    null;
	true ->
	    node:id(Node)
    end.

get_indexed_pred_id(Node, Ring, MyIndex, NIndex) ->
    case get_indexed_id(Node, Ring) of
        "null" -> "null";
        "none" -> "none";
        Index -> ((Index-MyIndex+NIndex) rem NIndex)-NIndex
    end.

get_indexed_succ_id(Node, Ring, MyIndex, NIndex) ->
    case get_indexed_id(Node, Ring) of
        "null" -> "null";
        "none" -> "none";
        Index -> (Index-MyIndex+NIndex) rem NIndex
    end.

get_indexed_id(Node, Ring) ->
    case node:is_null(Node) of
        true -> "null";
        false -> get_indexed_id(Node, Ring, 0)
    end.

get_indexed_id(Node, [{ok, Details}|Ring], Index) ->
    case node:id(Node) =:= node:id(node_details:get(Details, node)) of
        true -> Index;
        false -> get_indexed_id(Node, Ring, Index+1)
    end;

get_indexed_id(_Node, [], _Index) ->
    "none".

get_flag(Hostname) ->
    Country = string:substr(Hostname, 1 + string:rchr(Hostname, $.)),
    URL = string:concat("icons/", string:concat(Country, ".gif")),
    {img, [{src, URL}, {width, 26}, {height, 16}], []}.

is_valid({ok, _}) ->
    true;
is_valid({failed}) ->
    false.
