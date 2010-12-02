% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
%            2008 onScale solutions

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc web helpers module for Bootstrap server to generated the web interface
%% @version $Id$
-module(webhelpers).
-author('schuett@zib.de').
-vsn('$Id$').

-include("yaws_api.hrl").
-include("scalaris.hrl").

-export([getLoadRendered/0, getRingChart/0, getRingRendered/0,
         getIndexedRingRendered/0, lookup/1, set_key/2, delete_key/2, isPost/1,
         getVivaldiMap/0, pid_to_name/1, pid_to_name2/1]).

-opaque attribute_type() :: {atom(), string()}.
-ifdef(forward_or_recursive_types_are_not_allowed).
-type html_type() :: {atom(), [attribute_type()], term()}.
-else.
-type html_type() :: {atom(), [attribute_type()], html_type() | string()}.
-endif.

%% @doc Checks whether the current request is a post operation.
-spec isPost(A::#arg{req::#http_request{method::atom()}}) -> boolean().
isPost(A) ->
    Method = (A#arg.req)#http_request.method,
    Method =:= 'POST'.

%%%-----------------------------Lookup/Put/delete---------------------

-spec lookup(Key::?RT:key())
        -> {TimeInMs::integer(),
            Result::{Value::?DB:value(), Version::?DB:version()} |
                    {fail, not_found} | {fail, timeout} | {fail, fail}}.
lookup(Key) ->
%%    timer:tc(cs_api, read, [Key]).
    timer:tc(transaction_api, quorum_read, [Key]).

-spec set_key(Key::?RT:key(), Value::?DB:value())
        -> {TimeInMs::integer(),
            Result::commit | userabort | {fail, not_found} | {fail, timeout} |
                    {fail, fail} | {fail, abort}}.
set_key(Key, Value) ->
    timer:tc(transaction_api, single_write, [Key, Value]).

-spec delete_key(Key::?RT:key(), Timeout::pos_integer())
        -> {TimeInMs::integer(),
            Result::{ok, pos_integer(), list()} | {fail, timeout} |
                    {fail, timeout, pos_integer(), list()} |
                    {fail, node_not_found}}.
delete_key(Key, Timeout) ->
    timer:tc(transaction_api, delete, [Key, Timeout]).

%%%-----------------------------Load----------------------------------

-spec getLoad() -> [{ok, Node::comm:mypid(), Load::node_details:load()} | {failed, Node::comm:mypid()}].
getLoad() ->
    boot_server:node_list(),
    Nodes =
        receive
            {get_list_response, X} -> X
        after 2000 -> []
        end,
    get_load(Nodes).
    
-spec get_load([comm:mypid()]) -> [{ok, Node::comm:mypid(), Load::node_details:load()} | {failed, Node::comm:mypid()}].
get_load([Head | Tail]) ->
    comm:send(Head, {get_node_details, comm:this(), [load]}),
    receive
        {get_node_details_response, NodeDetails} ->
            [{ok, Head, node_details:get(NodeDetails, load)} | get_load(Tail)]
    after 2000 ->
            [{failed, Head} | get_load(Tail)]
    end;
get_load([]) ->
    [].


%%%-----------------------------Load----------------------------------

-spec getLoadRendered() -> html_type().
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

-spec renderLoad([{failed, Node::node:node_type()} | {ok, Node::node:node_type(), non_neg_integer()}]) -> [html_type()].
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

-spec getVivaldiMap() -> SVG::string().
getVivaldiMap() ->
    boot_server:node_list(),
    Nodes =
        receive
            {get_list_response, X} -> X
        after 2000 ->
            log:log(error,"[ WH ] Timeout getting node list from boot server"),
            throw('boot_server_timeout')
        end,
    This = self(),
    [erlang:spawn(
       fun() ->
               SourcePid = comm:get(This, comm:this_with_cookie(Pid)),
               comm:send_to_group_member(Pid, vivaldi, {get_coordinate, SourcePid})
       end) || Pid <- Nodes],
    CC_list = get_vivaldi(Nodes, [], 0),
    renderVivaldiMap(CC_list, Nodes).

-spec get_vivaldi(Pids::[comm:mypid()], [vivaldi:network_coordinate()], TimeInMS::non_neg_integer()) -> [vivaldi:network_coordinate()].
get_vivaldi([], Coords, _TimeInS) -> Coords;
get_vivaldi(Pids, Coords, TimeInMS) ->
    Continue =
        if
            TimeInMS =:= 2000 ->
                log:log(error,"[ WH ]: 2sec Timeout waiting for vivaldi_get_coordinate_response from ~p",[Pids]),
                continue;
            TimeInMS >= 6000 ->
                log:log(error,"[ WH ]: 6sec Timeout waiting for vivaldi_get_coordinate_response from ~p",[Pids]),
                stop;
            true -> continue
    end,
    case Continue of
        continue ->
            receive
                {{vivaldi_get_coordinate_response, Coordinate, _Confidence}, Pid} ->
                    get_vivaldi(lists:delete(Pid, Pids), [Coordinate | Coords],
                                TimeInMS)
            after
                10 ->
                    get_vivaldi(Pids, Coords, TimeInMS + 10)
            end;
        _ -> Coords
    end.

-spec renderVivaldiMap(CC_list::[vivaldi:network_coordinate()], Nodes::[comm:mypid()]) -> SVG::string().
renderVivaldiMap(_CC_list, []) ->
    "<svg xmlns=\"http://www.w3.org/2000/svg\"/>";
renderVivaldiMap(CC_list, Nodes) ->
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
                        floor(lists:nth(2, Max)-lists:nth(2, Min))]),

    Content=gen_Nodes(CC_list, Nodes, R),
    Foot="</svg>",
    Head++Content++Foot.

-spec floor(X::number()) -> integer().
floor(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T - 1
    end.

-spec gen_Nodes(CC_list::[vivaldi:network_coordinate()], Nodes::[comm:mypid()], R::float()) -> string().
gen_Nodes([],_,_) ->
    "";
gen_Nodes([H|T],[HN|TN],R) ->
    Hi=255,
    Lo=0,
    
    S1 =  pid(HN),

    random:seed(S1,S1,S1),
    C1 = random:uniform(Hi-Lo)+Lo-1,
    C2 = random:uniform(Hi-Lo)+Lo-1,
    C3 = random:uniform(Hi-Lo)+Lo-1,
    io_lib:format("<circle cx=\"~p\" cy=\"~p\" r=\"~p\" style=\"fill:rgb( ~p, ~p ,~p) ;\" />~n",[lists:nth(1, H),lists:nth(2, H),R,C1,C2,C3])
    ++gen_Nodes(T,TN,R).

%% @doc Gets the smallest and largest coordinates in each dimension of all
%%      vectors in the given list.
-spec get_min_max(Vectors::[vivaldi:network_coordinate()])
        -> {vivaldi:network_coordinate(), vivaldi:network_coordinate()}.
get_min_max([]) ->
    {[], []};
get_min_max([H | T]) ->
    lists:foldl(fun(A, {Min, Max}) ->
                        {min_list(A, Min), max_list(A, Max)}
                end, {H, H}, T).

%% @doc Gets the smallest coordinate in each dimension of the given vectors.
-spec min_list(L1::vivaldi:network_coordinate(), L2::vivaldi:network_coordinate()) -> vivaldi:network_coordinate().
min_list(L1, L2) ->
    lists:zipwith(fun(X, Y) ->
                          case X < Y of
                              true -> X;
                              _ -> Y
                          end
                  end, L1, L2).

%% @doc Gets the largest coordinate in each dimension of the given vectors.
-spec max_list(L1::vivaldi:network_coordinate(), L2::vivaldi:network_coordinate()) -> vivaldi:network_coordinate().
max_list(L1, L2) ->
    lists:zipwith(fun(X, Y) ->
                          case X > Y of
                              true -> X;
                              _ -> Y
                          end
                  end, L1, L2).

-spec pid(comm:mypid()) -> integer().
-ifdef(TCP_LAYER).
pid({{A,B,C,D},I,_}) ->
    A+B+C+D+I.
-endif.

-ifdef(BUILTIN).
pid(X) ->
    list_to_integer(lists:nth(1, string:tokens(erlang:pid_to_list(X),"<>."))).
-endif.

-spec pid_to_name(Pid::pid()) -> string().
pid_to_name(Pid) ->
    case pid_groups:group_and_name_of(Pid) of
        failed -> erlang:pid_to_list(Pid);
        X      -> pid_to_name2(X)
    end.

-spec pid_to_name2({pid_groups:groupname(), pid_groups:pidname()}) -> string().
pid_to_name2({GrpName, PidName}) ->
    lists:flatten(io_lib:format("~s:~w", [GrpName, PidName])).

%%%-----------------------------Ring----------------------------------

-spec getRingChart() -> html_type().
getRingChart() ->
    RealRing = statistics:get_ring_details(),
    Ring = [NodeDetails || {ok, NodeDetails} <- RealRing],
    RingSize = length(Ring),
    if
        RingSize =:= 0 ->
            {p, [], "empty ring"};
        RingSize =:= 1 ->
            {img, [{src, "http://chart.apis.google.com/chart?cht=p&chco=008080&chd=t:1&chs=600x350"}], ""};
        true ->
            try
              PieURL = renderRingChart(Ring),
              LPie = length(PieURL),
              if
                  LPie =:= 0  -> throw({urlTooShort, PieURL});
                  LPie < 1023 -> {p, [], [{img, [{src, PieURL}], ""}]};
                  true        -> throw({urlTooLong, PieURL})
              end
            catch
                _:_ -> {p, [], "Sorry, pie chart not available (too many nodes or other error)."}
            end
    end.

-spec renderRingChart(Ring::[node_details:node_details(),...]) -> string().
renderRingChart(Ring) ->
    try
        URLstart = "http://chart.apis.google.com/chart?cht=p&chco=008080",
        Sizes = [ begin
                      Me_tmp = node:id(node_details:get(Node, node)),
                      Pred_tmp = node:id(node_details:get(Node, pred)),
                      MaxKey = ?RT:n() - 1,
                      Diff = case (Me_tmp - Pred_tmp) of
                                 X when X < 0 -> X + MaxKey;
                                 Y            -> Y
                             end * 100 / MaxKey,
                      io_lib:format("~f", [Diff])
                  end || Node <- Ring ],
        Hostinfos = [ node_details:get(Node, hostname) ++ " (" ++
                          integer_to_list(node_details:get(Node, load)) ++ ")"
                    || Node <- Ring ],
        CHD = "chd=t:" ++ string:join(Sizes, ","),
        CHS = "chs=600x350",
        CHL = "chl=" ++ string:join(Hostinfos, "|"),
        URLstart ++ "&" ++ CHD ++ "&" ++ CHS ++ "&" ++ CHL
    catch % keys might not support subtraction or ?RT:n() might throw
        throw:not_supported -> "";
        error:badarith -> ""
    end.

-spec getRingRendered() -> html_type().
getRingRendered() ->
    RealRing = statistics:get_ring_details(),
    Ring = [X || X = {ok, _} <- RealRing],
    RingSize = length(Ring),
    if
        RingSize =:= 0 ->
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
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Preds"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Node"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Succs"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "RTSize"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load"}}}
                 ]},
                lists:append([renderRing(X) ||  X = {ok, _} <- RealRing],
                             [renderRing(X) ||  X = {failed, _} <- RealRing])
               ]
              }
             ]
            }
    end.

-spec renderRing(Node::statistics:ring_element()) -> html_type().
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
       {td, [], io_lib:format('~p', [lists:map(fun node:id/1, PredList)])},
       {td, [], io_lib:format('~p', [node:id(Node)])},
       {td, [], io_lib:format('~p', [lists:map(fun node:id/1, SuccList)])},
       {td, [], io_lib:format('~p', [RTSize])},
       {td, [], io_lib:format('~p', [Load])}
      ]};
renderRing({failed, Pid}) ->
    {tr, [], 
      [
       {td, [], "-"},
       {td, [], "-"},
       {td, [], io_lib:format('- (~p)', [Pid])},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"}
      ]}.

-spec getIndexedRingRendered() -> html_type().
getIndexedRingRendered() ->
    RealRing = statistics:get_ring_details(),
    % table with mapping node_id -> index
    _ = ets:new(webhelpers_indexed_ring, [ordered_set, private, named_table]),
    _ = [begin
             Size = ets:info(webhelpers_indexed_ring, size),
             ets:insert(webhelpers_indexed_ring,
                        {node:id(node_details:get(NodeDetails, node)), Size})
         end || {ok, NodeDetails} <- RealRing],
    RingSize = ets:info(webhelpers_indexed_ring, size),
    EHtml =
        if
        RingSize =:= 0 ->
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
                  {td, [], io_lib:format('~p', [statistics:get_total_load(RealRing)])},
                  {td, [], io_lib:format('~p', [statistics:get_average_load(RealRing)])},
                  {td, [], io_lib:format('~p', [statistics:get_load_std_deviation(RealRing)])},
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
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Preds Offset"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Node Index"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Succs Offsets"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "RTSize"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load"}}}
                 ]},
                lists:append([renderIndexedRing(X) ||  X = {ok, _} <- RealRing],
                             [renderIndexedRing(X) ||  X = {failed, _} <- RealRing])
               ]
              }
             ]
            }
    end,
    ets:delete(webhelpers_indexed_ring),
    EHtml.

%% @doc Renders an indexed ring into ehtml.
%%      Precond: existing webhelpers_indexed_ring ets table with
%%      node_id -> index mapping.
-spec renderIndexedRing(Node::statistics:ring_element()) -> html_type().
renderIndexedRing({ok, Details}) ->
    Hostname = node_details:get(Details, hostname),
    PredList = node_details:get(Details, predlist),
    Node = node_details:get(Details, node),
    SuccList = node_details:get(Details, succlist),
    RTSize = node_details:get(Details, rt_size),
    Load = node_details:get(Details, load),
    MyIndex = get_indexed_id(Node),
    PredIndex = lists:map(fun(Pred) -> get_indexed_pred_id(Pred, MyIndex) end, PredList),
    SuccIndices = lists:map(fun(Succ) -> get_indexed_succ_id(Succ, MyIndex) end, SuccList),
    [FirstSuccIndex|_] = SuccIndices,
    {tr, [],
      [
       {td, [], [get_flag(Hostname), io_lib:format('~p', [Hostname])]},
       case hd(PredIndex) =:= -1 of
           true->
               {td, [], io_lib:format('~p', [PredIndex])};
           false ->
               {td, [], io_lib:format('<span style="color:red">~p</span>', [PredIndex])}
       end,
       {td, [], io_lib:format('~p: ~p', [MyIndex, node:id(Node)])},
       case is_list(FirstSuccIndex) orelse FirstSuccIndex =/= 1 of
           true -> {td, [], io_lib:format('<span style="color:red">~p</span>', [SuccIndices])};
           false -> {td, [], io_lib:format('~p', [SuccIndices])}
       end,
       {td, [], io_lib:format('~p', [RTSize])},
       {td, [], io_lib:format('~p', [Load])}
      ]};

renderIndexedRing({failed, Pid}) ->
    {tr, [],
      [
       {td, [], "-"},
       {td, [], "-"},
       {td, [], io_lib:format('- (~p)', [Pid])},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"}
      ]}.

%%%-----------------------------Misc----------------------------------

dead_node() ->
    "dead node?".

-spec get_indexed_pred_id(Node::node:node_type(),
                          MyIndex::non_neg_integer() | string()) -> integer() | string().
get_indexed_pred_id(Node, MyIndex) ->
    NodeIndex = get_indexed_id(Node),
    RingSize = ets:info(webhelpers_indexed_ring, size),
    case NodeIndex =:= dead_node() orelse MyIndex =:= dead_node() of
        true -> dead_node();
        _    ->
            case NodeIndex =:= MyIndex of
                true -> 0;
                _    ->
                    ((NodeIndex - MyIndex + RingSize) rem RingSize) - RingSize
            end
    end.

-spec get_indexed_succ_id(Node::node:node_type(),
                          MyIndex::non_neg_integer() | string()) -> integer() | string().
get_indexed_succ_id(Node, MyIndex) ->
    NodeIndex = get_indexed_id(Node),
    RingSize = ets:info(webhelpers_indexed_ring, size),
    case NodeIndex =:= dead_node() orelse MyIndex =:= dead_node() of
        true -> dead_node();
        _    -> (NodeIndex - MyIndex + RingSize) rem RingSize
    end.

-spec get_indexed_id(Node::node:node_type()) -> non_neg_integer() | string().
get_indexed_id(Node) ->
    case ets:lookup(webhelpers_indexed_ring, node:id(Node)) of
        [] -> dead_node();
        [{_, Index}] -> Index
    end.

-spec get_flag(Hostname::node_details:hostname()) -> html_type().
get_flag(Hostname) ->
    Country = string:substr(Hostname, 1 + string:rchr(Hostname, $.)),
    URL = string:concat("icons/", string:concat(Country, ".gif")),
    {img, [{src, URL}, {width, 26}, {height, 16}], []}.
