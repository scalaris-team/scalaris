% @copyright 2007-2018 Zuse Institute Berlin,

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
%% @doc web helpers module for mgmt server to generate the web interface
%% @version $Id$
-module(webhelpers).
-author('schuett@zib.de').
-vsn('$Id$').

-include("yaws_api.hrl").
-include("scalaris.hrl").
-include("client_types.hrl").

-export([getRingChart/0, getRingRendered/0, getIndexedRingRendered/0,
         get_and_cache_ring/0, flush_ring_cache/0,
         getDCClustersAndNodes/0,
         getGossipRendered/0, getVivaldiMap/0,
         getMonitorClientData/0, getMonitorRingData/0,
         lookup/1, set_key/2, delete_key/2, isPost/1,
         safe_html_string/1, safe_html_string/2, html_pre/2,
         format_nodes/1, format_centroids/1,
         lookup_kvoncseq/1, set_key_kvoncseq/2
     ]).

-type attribute_type() :: {atom(), string()}.
-type html_type() :: {atom(), [attribute_type()], html_type() | [html_type()] | string()}.

% countries for which icons are available in the docroot/icons directory:
-define(COUNTRIES, ["aa", "au", "br", "ch", "cz", "edu", "fr", "hk", "il",
                    "it", "kr", "no", "pr", "se", "tw", "at", "be", "ca", "cn",
                    "de", "es", " gr", "hu", "in", "jp", "nl", "pl", "pt",
                    "sg", "uk", "us"]).

% @doc Checks whether the current request is a post operation.
-spec isPost(A::#arg{req::#http_request{method::atom()}}) -> boolean().
isPost(A) ->
    Method = (A#arg.req)#http_request.method,
    Method =:= 'POST'.

%%%-----------------------------Lookup/Put/delete---------------------

-spec lookup(Key::?RT:key())
        -> {TimeInMs::integer(),
            Result::{Value::db_dht:value(), Version::client_version()} |
                    {fail, not_found} | {fail, timeout} | {fail, fail}}.
lookup(Key) ->
    util:tc(api_tx, read, [Key]).

-spec set_key(Key::?RT:key(), Value::db_dht:value())
        -> {TimeInMs::integer(),
            Result::commit | userabort | {fail, not_found} | {fail, timeout} |
                    {fail, fail} | {fail, abort}}.
set_key(Key, Value) ->
    util:tc(api_tx, write, [Key, Value]).

-spec delete_key(Key::?RT:key(), Timeout::pos_integer())
        -> {TimeInMs::integer(),
            Result::{ok, pos_integer(), list()} | {fail, timeout} |
                    {fail, timeout, pos_integer(), list()}}.
delete_key(Key, Timeout) ->
    util:tc(api_rdht, delete, [Key, Timeout]).

%%%-----------------------------Lookup/Put/delete (kvoncseq)----------

lookup_kvoncseq(Key) ->
    kv_on_cseq:read(Key).

set_key_kvoncseq(Key, Value) ->
    kv_on_cseq:write(Key, Value).

%%%--------------------------Vivaldi-Map------------------------------
-spec getVivaldiMap() -> [{{comm:mypid(), node_details:hostname()}, gossip_vivaldi:network_coordinate()}].
getVivaldiMap() ->
    Nodes = [{
                node:pidX(node_details:get(Node, node))
                , node_details:get(Node, hostname)
            } || {ok, Node} <- get_and_cache_ring()],
    NodePids = [P || {P, _} <- Nodes],
    This = comm:this(),
    _ = [erlang:spawn(
           fun() ->
                   SourcePid = comm:reply_as(This, 2, {webhelpers, '_', Pid}),
                   comm:send(Pid, {cb_msg, {gossip_vivaldi, default}, {get_coordinate, SourcePid}}, [{group_member, gossip}])
           end) || Pid <- NodePids],
    lists:zip(Nodes, get_vivaldi(NodePids, [], 0))
    .

-spec get_vivaldi(Pids::[comm:mypid()], [gossip_vivaldi:network_coordinate()],
                  TimeInMS::non_neg_integer()) -> [gossip_vivaldi:network_coordinate()].
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
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV(
                    {webhelpers, {vivaldi_get_coordinate_response, Coordinate, _Confidence}, Pid}, %% ->
                    get_vivaldi(lists:delete(Pid, Pids), [Coordinate | Coords],
                                TimeInMS)
                  )
            after
                10 ->
                    get_vivaldi(Pids, Coords, TimeInMS + 10)
            end;
        _ -> Coords
    end.

% @doc Choose a random color for a Pid
-spec color(comm:mypid()) -> string().
color(Pid) ->
    Hash = erlang:phash2(Pid, 256*256*256),
    R = Hash rem 256,
    G = (Hash div 256) rem 256,
    B = (Hash div (256*256)) rem 256,
    io_lib:format("rgb(~p,~p,~p)",[R,G,B]).

% @doc Get a string representation for a vivaldi coordinate
-spec format_coordinate([gossip_vivaldi:network_coordinate(),...]) -> string().
format_coordinate([X,Y]) ->
    io_lib:format("[~p,~p]", [X,Y]).

% @doc Format Nodes as returned by getVivaldiMap() into JSON.
-spec format_nodes([{{comm:mypid(), node_details:hostname()}
                     , gossip_vivaldi:network_coordinate()}]) -> string().
format_nodes(Nodes) ->
    % order nodes according to their datacenter (designated by color)
    NodesTree = lists:foldl(
        fun({{NodeName, NodeHost}, Coords}, Acc) ->
            Key = color(NodeName),
            PriorValue = gb_trees:lookup(Key, Acc),
            V = case PriorValue of
                none -> [];
                {value, PV} -> PV
            end,
            gb_trees:enter(Key, [io_lib:format("{\"name\":\"~p\",\"coords\":~s,\"host\":\"~s\"}"
                                               , [comm:make_local(NodeName),format_coordinate(Coords),NodeHost]) | V]
                           , Acc)
    end, gb_trees:empty(), Nodes),

    "[" ++ util:gb_trees_foldl(fun(Color, DCNodes, Acc) ->
                NodeString = string:join(
                        [io_lib:format("{\"color\":\"~s\",\"info\":~s}",
                                        [Color,NodeInfo])
                         || NodeInfo <- DCNodes ], ","),
                Sep = case Acc of
                    "" -> "";
                    _ -> ","
                end,
                NodeString ++ Sep ++ Acc
    end, "", NodesTree) ++ "]".
%%%--------------------------DC Clustering------------------------------
-spec getDCClustersAndNodes() -> {[{comm:mypid(), gossip_vivaldi:network_coordinate()}],
        dc_centroids:centroids(), non_neg_integer(), float()} | disabled.
getDCClustersAndNodes() ->
    case config:read(dc_clustering_enable) of
        true ->
            This = comm:this(),
            ClusteringProcess = pid_groups:find_a(dc_clustering),
            comm:send_local(ClusteringProcess, {query_clustering, This}),
            comm:send_local(ClusteringProcess, {query_my, local_epoch, This}),
            comm:send_local(ClusteringProcess, {query_my, radius, This}),

            % note: receive wrapped in anonymous functions to allow
            %       ?SCALARIS_RECV in multiple receive statements
            Centroids = fun() ->
                trace_mpath:thread_yield(),
                receive
                ?SCALARIS_RECV({query_clustering_response, Cs}, Cs)
            after 2000 ->
                    log:log(error,"[ WH ] Timeout getting query_clustering_response from dc_clustering"),
                    throw('dc_clustering_timeout')
            end end(),

            Epoch = fun() ->
                trace_mpath:thread_yield(),
                receive
                ?SCALARIS_RECV({query_my_response, local_epoch, E}, E)
            after 2000 ->
                    log:log(error,"[ WH ] Timeout getting local_epoch from dc_clustering"),
                    throw('dc_clustering_timeout')
            end end(),

            Radius = fun() ->
                trace_mpath:thread_yield(),
                receive
                ?SCALARIS_RECV({query_my_response, radius, R}, R)
            after 2000 ->
                    log:log(error,"[ WH ] Timeout getting radius from dc_clustering"),
                    throw('dc_clustering_timeout')
            end end(),

            Nodes = getVivaldiMap(),

            {Nodes, Centroids, Epoch, Radius};
        _ -> disabled
    end.

format_centroid(Centroid) ->
    {Coords, Radius} = dc_centroids:get_coordinate_and_relative_size(Centroid),
    FormatString = "{\"coords\":~p, \"radius\":~f}",
    io_lib:format(FormatString, [Coords, Radius])
    .

%% @doc Convert a list of centroids into a JSON string
-spec format_centroids(Centroids :: [dc_centroids:centroid()]) -> string().
format_centroids(Centroids) ->
    "["
    ++ lists:flatten([format_centroid(C) || C <- Centroids])
    ++ "]"
    .

%%%-----------------------------Ring----------------------------------

-spec get_and_cache_ring() -> statistics:ring().
get_and_cache_ring() ->
    case erlang:get(web_scalaris_ring) of
        undefined ->
            Ring =
                case whereis(mgmt_server) of
                    undefined -> statistics:get_ring_details_neighbors(1); % get neighbors only
                    _         -> statistics:get_ring_details()
                end,
            % mapping node_id -> index
            _ = util:map_with_nr(
                  fun(RingE, NrX) ->
                          case RingE of
                              {ok, NodeDetailsX} ->
                                  NodeX = node_details:get(NodeDetailsX, node),
                                  erlang:put({web_scalaris_ring_idx, node:id(NodeX)}, NrX),
                                  erlang:put(web_scalaris_ring_size, NrX),
                                  ok;
                              _ -> ok
                          end
                  end, Ring, 1),
            erlang:put(web_scalaris_ring, Ring);
        Ring -> ok
    end,
    Ring.

-spec get_indexed_id_by_node(Node::node:node_type()) -> non_neg_integer() | undefined.
get_indexed_id_by_node(Node) ->
    get_indexed_id_by_id(node:id(Node)).

-spec get_indexed_id_by_id(NodeId::?RT:key()) -> non_neg_integer() | undefined.
get_indexed_id_by_id(NodeId) ->
    erlang:get({web_scalaris_ring_idx, NodeId}).

-spec get_ring_size() -> non_neg_integer() | undefined.
get_ring_size() ->
    erlang:get(web_scalaris_ring_size).

-spec flush_ring_cache() -> ok.
flush_ring_cache() ->
    erlang:erase(web_scalaris_ring),
    ok.

-spec extract_ring_info([node_details:node_details()])
        -> [{Label::string(), Value::string(), Known::boolean()}].
extract_ring_info([]) ->
    [];
extract_ring_info([RingE]) ->
    Node = node_details:get(RingE, node),
    MyId = node:id(Node),
    PredId = node:id(node_details:get(RingE, pred)),
    MyIndexStr = case get_indexed_id_by_id(MyId) of
                     undefined -> "";
                     MyIndex -> integer_to_list(MyIndex) ++ ": "
                 end,
    NodePid = node:pidX(Node),
    case comm:get_ip(NodePid) of
        {NodeIP1, NodeIP2, NodeIP3, NodeIP4} -> ok;
        _ -> NodeIP1 = NodeIP2 = NodeIP3 = NodeIP4 = 0
    end,
    NodePort = comm:get_port(NodePid),
    YawsPort = node:yawsPort(Node),
    Label = lists:flatten(io_lib:format("~s<a href=\\\"http://~B.~B.~B.~B:~B/ring.yaws\\\">~s / ~B.~B.~B.~B:~B</a> (~B)",
                  [MyIndexStr,
                   NodeIP1, NodeIP2, NodeIP3, NodeIP4, YawsPort,
                   node_details:get(RingE, hostname),
                   NodeIP1, NodeIP2, NodeIP3, NodeIP4, NodePort,
                   node_details:get(RingE, load)])),
    case MyId =:= PredId of
        true -> [{Label, "100.00", true}];
        _ ->
            Diff = ?RT:get_range(PredId, MyId) * 100 / ?RT:n(),
            Me_val = io_lib:format("~f", [Diff]),
            Unknown_val = io_lib:format("~f", [100 - Diff]),
            [{Label, Me_val, true}, {"unknown", Unknown_val, false}]
    end;
extract_ring_info([First | _Rest] = Ring) ->
    extract_ring_info(Ring, First, []).

-spec extract_ring_info(Ring::[node_details:node_details()], First::node_details:node_details(),
                        Acc::[T | [T]]) -> [T]
        when is_subtype(T, {Label::string(), Value::string(), Known::boolean()}).
extract_ring_info([RingE], First, Acc) ->
    NewAcc = [extract_ring_info2(RingE, First) | Acc],
    lists:flatten(lists:reverse(NewAcc));
extract_ring_info([RingE1, RingE2 | Rest], First, Acc) ->
    NewAcc = [extract_ring_info2(RingE1, RingE2) | Acc],
    extract_ring_info([RingE2 | Rest], First, NewAcc).

-spec extract_ring_info2(RingE1::node_details:node_details(), RingE2::node_details:node_details())
        -> [{Label::string(), Value::string(), Known::boolean()}].
extract_ring_info2(RingE1, RingE2) ->
    E1_Node = node_details:get(RingE1, node),
    E1_MyId = node:id(E1_Node),
    E1_PredId = node:id(node_details:get(RingE1, pred)),
    E2_PredId = node:id(node_details:get(RingE2, pred)),
    E1_Diff = ?RT:get_range(E1_PredId, E1_MyId) * 100 / ?RT:n(),
    E1_MyIndexStr = case get_indexed_id_by_id(E1_MyId) of
                        undefined -> "";
                        E1_MyIndex -> integer_to_list(E1_MyIndex) ++ ": "
                    end,
    E1_NodePid = node:pidX(E1_Node),
    case comm:get_ip(E1_NodePid) of
        {E1_NodeIP1, E1_NodeIP2, E1_NodeIP3, E1_NodeIP4} -> ok;
        _ -> E1_NodeIP1 = E1_NodeIP2 = E1_NodeIP3 = E1_NodeIP4 = 0
    end,
    E1_NodePort = comm:get_port(E1_NodePid),
    E1_YawsPort = node:yawsPort(E1_Node),
    E1_Label = lists:flatten(io_lib:format("~s<a href=\\\"http://~B.~B.~B.~B:~B/ring.yaws\\\">~s / ~B.~B.~B.~B:~B</a> (~B)",
                  [E1_MyIndexStr,
                   E1_NodeIP1, E1_NodeIP2, E1_NodeIP3, E1_NodeIP4, E1_YawsPort,
                   node_details:get(RingE1, hostname),
                   E1_NodeIP1, E1_NodeIP2, E1_NodeIP3, E1_NodeIP4, E1_NodePort,
                   node_details:get(RingE1, load)])),
    E1_Me_val = io_lib:format("~f", [E1_Diff]),
    case E1_MyId =:= E2_PredId of
        true -> [{E1_Label, E1_Me_val, true}];
        _ -> % add an "unknown" slice as there is another (but unknown) node:
            Diff2 = ?RT:get_range(E1_MyId, E2_PredId) * 100 / ?RT:n(),
            Unknown_val = io_lib:format("~f", [Diff2]),
            [{E1_Label, E1_Me_val, true}, {"unknown", Unknown_val, false}]
    end.

-spec getRingChart() -> [html_type()].
getRingChart() ->
    RealRing = get_and_cache_ring(),
    Ring = [NodeDetails || {ok, NodeDetails} <- RealRing],
    case length(Ring) of
        0 -> [];
        RingSize ->
            try
                Data = extract_ring_info(Ring),
                ColorAlphaInc = 1 / RingSize,
                {_, DataStr0, ColorStr0} =
                    lists:foldr(
                      fun({Label, Value, Known}, {I, DStr, CStr}) ->
                              {CurColor, NewI} =
                                  case Known of
                                      true ->
                                          Alpha = case (1 - I * ColorAlphaInc) of
                                                      X when X > 0.2 -> X;
                                                      _ -> 0.2
                                                  end,
                                          AlphaStr = io_lib:format("~.2f", [Alpha]),
                                          {lists:flatten(["$.color.make(0, 128, 128, ", AlphaStr, ").toString()"]), I -1};
                                      _ -> {"$.color.make(255, 255, 255, 1).toString()", I}
                                  end,
                              CurData = lists:append(["{ label: \"", Label, "\", data: ", Value, " }"]),
                              NewData = case DStr of
                                            [] -> CurData;
                                            _  -> lists:append([CurData, ", ", DStr])
                                        end,
                              NewColors = case CStr of
                                              [] -> CurColor;
                                              _  -> lists:append([CurColor, ", ", CStr])
                                          end,
                              {NewI, NewData, NewColors}
                      end, {RingSize - 1, "", ""}, Data),
                DataStr = lists:flatten(["var ring = [", DataStr0, "];\n"
                                         "var colors = [", ColorStr0, "];\n"]),
                [{script, [{type, "text/javascript"}], lists:append(["$(function() {\n", DataStr, "plot_ring(ring, colors);\n});\n"])},
                 {table, [],
                  [{tr, [],
                    [{td, [], {'div', [{id, "ring"}, {style, "width: 600px; height: 350px"}], []}},
                     {td, [], {'div', [{id, "ringhover"}, {style, "width: 100px; height: 350px"}], []}}]}]}
                ]
            catch % ?RT methods might throw
                throw:not_supported -> [{p, [], "Sorry, pie chart not available (unknown error)."}]
            end
    end.

-spec getRingRendered() -> html_type().
getRingRendered() ->
    RealRing = get_and_cache_ring(),
    Ring = [X || X = {ok, _} <- RealRing],
    RingSize = length(Ring),
    if
        RingSize =:= 0 ->
            {p, [], "empty ring"};
        true ->
            {p, [],
              [
              {table, [{bgcolor, "#CCDCEE"}, {width, "100%"}],
               [%% items
                {tr, [{bgcolor, "#000099"}],
                 [
                  {td, [{width, "10%"}], {strong, [], {font, [{color, "white"}], ""}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Total"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Average"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Std. Deviation"}}}
                 ]
                },
                {tr, [],
                 [
                  {td, [{bgcolor, "#000099"}], {strong, [], {font, [{color, "white"}], "Items"}}},
                  {td, [], io_lib:format("~p", [statistics:get_total_load(load, RealRing)])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_average_load(load, RealRing))])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_load_std_deviation(load, RealRing))])}
                 ]
                },
                %% load
                {tr, [],
                 [
                  {td, [{bgcolor, "#000099"}], {strong, [], {font, [{color, "white"}], "Load"}}},
                  {td, [], io_lib:format("-", [])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_average_load(load2, RealRing))])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_load_std_deviation(load2, RealRing))])}
                 ]
                },
                %% requests
                {tr, [],
                 [
                  {td, [{bgcolor, "#000099"}], {strong, [], {font, [{color, "white"}], "Requests"}}},
                  {td, [], io_lib:format("~p", [statistics:get_total_load(load3, RealRing)])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_average_load(load3, RealRing))])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_load_std_deviation(load3, RealRing))])}
                 ]
                }
               ]
              },
              {br, []},
              {table, [{bgcolor, "#CCDCEE"}, {width, "100%"}],
               [{tr, [{bgcolor, "#000099"}],
                 [
                  {td, [{align, "center"}, {width,"350px"}], {strong, [], {font, [{color, "white"}], "Host"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Preds"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Node"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Succs"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "RTSize"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Items"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Requests"}}}
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
    Items = node_details:get(Details, load),
    Load = node_details:get(Details, load2),
    Requests = node_details:get(Details, load3),
    MyIndexStr = case get_indexed_id_by_node(Node) of
                     undefined -> dead_node();
                     MyIndex -> MyIndex
                 end,
    NodeListFun =
        fun(NodeX) ->
                case get_indexed_id_by_node(NodeX) of
                    undefined -> lists:flatten(
                                   io_lib:format("<b>~p</b>", [node:id(NodeX)]));
                    X         -> X
                end
        end,
    NodePid = node:pidX(Node),
    case comm:get_ip(NodePid) of
        {NodeIP1, NodeIP2, NodeIP3, NodeIP4} -> ok;
        _ -> NodeIP1 = NodeIP2 = NodeIP3 = NodeIP4 = 0
    end,
    NodePort = comm:get_port(NodePid),
    YawsPort = node:yawsPort(Node),
    {tr, [],
      [
       {td, [], [get_flag(Hostname),
                 io_lib:format("<a href=\"http://~B.~B.~B.~B:~B/ring.yaws\">~p (~B.~B.~B.~B:~B)</a>",
                               [NodeIP1, NodeIP2, NodeIP3, NodeIP4, YawsPort,
                                Hostname,
                                NodeIP1, NodeIP2, NodeIP3, NodeIP4, NodePort])]},
       {td, [], io_lib:format("~.100p", [[NodeListFun(N) || N <- PredList]])},
       {td, [], io_lib:format("~p:&nbsp;~p", [MyIndexStr, node:id(Node)])},
       {td, [], io_lib:format("~.100p", [[NodeListFun(N) || N <- SuccList]])},
       {td, [], io_lib:format("~p", [RTSize])},
       {td, [], io_lib:format("~p", [Items])},
       {td, [], io_lib:format("~p", [Load])},
       {td, [], io_lib:format("~p", [Requests])}
      ]};
renderRing({failed, Pid}) ->
    {tr, [],
      [
       {td, [], "-"},
       {td, [], "-"},
       {td, [], io_lib:format("- (~p)", [Pid])},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"}
      ]}.

-spec getIndexedRingRendered() -> html_type().
getIndexedRingRendered() ->
    RealRing = get_and_cache_ring(),
    RingSize = get_ring_size(),
    EHtml =
        if
        RingSize =:= 0 ->
            {p, [], "empty ring"};
        true ->
            {p, [],
              [
              {table, [{bgcolor, "#CCDCEE"}, {width, "100%"}],
               [%% items
                {tr, [{bgcolor, "#000099"}],
                 [
                  {td, [{width, "10%"}], {strong, [], {font, [{color, "white"}], ""}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Total"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Average"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Std. Deviation"}}}
                 ]
                },
                {tr, [],
                 [
                  {td, [{bgcolor, "#000099"}], {strong, [], {font, [{color, "white"}], "Items"}}},
                  {td, [], io_lib:format("~p", [statistics:get_total_load(load, RealRing)])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_average_load(load, RealRing))])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_load_std_deviation(load, RealRing))])}
                 ]
                },
                %% load
                {tr, [],
                 [
                  {td, [{bgcolor, "#000099"}], {strong, [], {font, [{color, "white"}], "Load"}}},
                  {td, [], io_lib:format("-", [])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_average_load(load2, RealRing))])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_load_std_deviation(load2, RealRing))])}
                 ]
                },
                %% requests
                {tr, [],
                 [
                  {td, [{bgcolor, "#000099"}], {strong, [], {font, [{color, "white"}], "Requests"}}},
                  {td, [], io_lib:format("~p", [statistics:get_total_load(load3, RealRing)])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_average_load(load3, RealRing))])},
                  {td, [], io_lib:format("~p", [erlang:round(statistics:get_load_std_deviation(load3, RealRing))])}
                 ]
                }
               ]
              },
              {br, []},
              {table, [{bgcolor, "#CCDCEE"}, {width, "100%"}],
               [{tr, [{bgcolor, "#000099"}],
                 [
                  {td, [{align, "center"}, {width,"350px"}], {strong, [], {font, [{color, "white"}], "Host"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Preds Offset"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Node"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Succs Offsets"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "RTSize"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Items"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load"}}},
                  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Requests"}}}
                 ]},
                lists:append([renderIndexedRing(X) ||  X = {ok, _} <- RealRing],
                             [renderIndexedRing(X) ||  X = {failed, _} <- RealRing])
               ]
              }
             ]
            }
    end,
    EHtml.

%% @doc Renders an indexed ring into ehtml.
%%      Precond: existing Ring from get_and_cache_ring/0 with
%%      node_id -> index mapping.
-spec renderIndexedRing(Node::statistics:ring_element()) -> html_type().
renderIndexedRing({ok, Details}) ->
    Hostname = node_details:get(Details, hostname),
    PredList = node_details:get(Details, predlist),
    Node = node_details:get(Details, node),
    SuccList = node_details:get(Details, succlist),
    RTSize = node_details:get(Details, rt_size),
    Items = node_details:get(Details, load),
    Load = node_details:get(Details, load2),
    Requests = node_details:get(Details, load3),
    MyIndex = get_indexed_id_by_node(Node),
    PredIndex = lists:map(fun(Pred) -> get_indexed_pred_id(Pred, MyIndex) end, PredList),
    SuccIndices = lists:map(fun(Succ) -> get_indexed_succ_id(Succ, MyIndex) end, SuccList),
    [FirstSuccIndex|_] = SuccIndices,
    MyIndexStr = case MyIndex of
                     undefined -> dead_node();
                     X -> X
                 end,
    NodePid = node:pidX(Node),
    case comm:get_ip(NodePid) of
        {NodeIP1, NodeIP2, NodeIP3, NodeIP4} -> ok;
        _ -> NodeIP1 = NodeIP2 = NodeIP3 = NodeIP4 = 0
    end,
    NodePort = comm:get_port(NodePid),
    YawsPort = node:yawsPort(Node),
    {tr, [],
      [
       {td, [], [get_flag(Hostname),
                 io_lib:format("<a href=\"http://~B.~B.~B.~B:~B/ring.yaws\">~p (~B.~B.~B.~B:~B)</a>",
                               [NodeIP1, NodeIP2, NodeIP3, NodeIP4, YawsPort,
                                Hostname,
                                NodeIP1, NodeIP2, NodeIP3, NodeIP4, NodePort])]},
       case hd(PredIndex) =:= -1 of
           true->
               {td, [], io_lib:format("~p", [PredIndex])};
           false ->
               {td, [], io_lib:format("<span style=\"color:red\">~p</span>", [PredIndex])}
       end,
       {td, [], io_lib:format("~p:&nbsp;~p", [MyIndexStr, node:id(Node)])},
       case is_list(FirstSuccIndex) orelse FirstSuccIndex =/= 1 of
           true -> {td, [], io_lib:format("<span style=\"color:red\">~p</span>", [SuccIndices])};
           false -> {td, [], io_lib:format("~p", [SuccIndices])}
       end,
       {td, [], io_lib:format("~p", [RTSize])},
       {td, [], io_lib:format("~p", [Items])},
       {td, [], io_lib:format("~p", [Load])},
       {td, [], io_lib:format("~p", [Requests])}
      ]};

renderIndexedRing({failed, Pid}) ->
    {tr, [],
      [
       {td, [], "-"},
       {td, [], "-"},
       {td, [], io_lib:format("- (~p)", [Pid])},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"}
      ]}.


%%%-----------------------------Gossip----------------------------------

-type gossip_pv() :: {PidName::string(), gossip_load:load_info()}.
-type gossip_key() :: avgLoad | stddev | size_ldr | size_kr | minLoad | maxLoad.

-spec getGossip() -> [gossip_pv()].
getGossip() ->
    GossipPids = pid_groups:find_all(gossip),
    [begin
         comm:send_local(Pid, {cb_msg, {gossip_load, default}, {gossip_get_values_best, self()}}),
         trace_mpath:thread_yield(),
         receive
             ?SCALARIS_RECV(
                 {gossip_get_values_best_response, BestValues}, %% ->
                 {pid_groups:pid_to_name(Pid), BestValues}
               )
         end
     end || Pid <- GossipPids].

-spec getGossipRendered() -> html_type().
getGossipRendered() ->
    Values = getGossip(),
    { table, [{bgcolor, "#CCDCEE"}, {width, "100%"}], renderGossip(Values) }.

-spec renderGossip([gossip_pv()]) -> [html_type()].
renderGossip([]) -> [];
renderGossip([V1]) ->
    renderGossip2(V1, {false, V1}, {false, V1});
renderGossip([V1, V2]) ->
    renderGossip2(V1, {true, V2}, {false, V2});
renderGossip([V1, V2, V3 | Rest]) ->
    lists:append(
      renderGossip2(V1, {true, V2}, {true, V3}),
      renderGossip(Rest)).

-spec renderGossip2(gossip_pv(), {boolean(), gossip_pv()}, {boolean(), gossip_pv()}) -> [html_type()].
renderGossip2(PV1, PVE2, PVE3) ->
    [renderGossipHead(PV1, PVE2, PVE3),
     renderGossipData(PV1, PVE2, PVE3, "Size (leader election)",
                      size_ldr, fun(V) -> safe_html_string("~.2f", [V]) end),
     renderGossipData(PV1, PVE2, PVE3, "Size (key range)",
                      size_kr, fun(V) -> safe_html_string("~.2f", [V]) end),
     renderGossipData(PV1, PVE2, PVE3, "Average load",
                      avgLoad, fun(V) -> safe_html_string("~.2f", [V]) end),
     renderGossipData(PV1, PVE2, PVE3, "Maximum load",
                      maxLoad, fun(V) -> safe_html_string("~B", [V]) end),
     renderGossipData(PV1, PVE2, PVE3, "Minimum load",
                      minLoad, fun(V) -> safe_html_string("~B", [V]) end),
     renderGossipData(PV1, PVE2, PVE3, "Standard deviation of the load",
                      stddev, fun(V) -> safe_html_string("~.2f", [V]) end)
     ].

-spec renderGossipHead(gossip_pv(), {boolean(), gossip_pv()}, {boolean(), gossip_pv()}) -> html_type().
renderGossipHead({P1, _V1}, {P2Exists, PV2}, {P3Exists, PV3}) ->
    ValueP1 = P1,
    {TD_V2_1, TD_V2_2} =
        case P2Exists of
            true ->
                P2 = element(1, PV2),
                {{td, [{align, "left"}], {strong, [], {font, [{color, "white"}], P2}}},
                 {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Value"}}}};
            _ ->
                {{td, [{bgcolor, "#D5DEDE"}, {align, "left"}], "&nbsp;"},
                 {td, [{bgcolor, "#D5DEDE"}, {align, "center"}], "&nbsp;"}}
              end,
    {TD_V3_1, TD_V3_2} =
        case P3Exists of
            true ->
                P3 = element(1, PV3),
                {{td, [{align, "left"}], {strong, [], {font, [{color, "white"}], P3}}},
                 {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Value"}}}};
            _ ->
                {{td, [{bgcolor, "#D5DEDE"}, {align, "left"}], "&nbsp;"},
                 {td, [{bgcolor, "#D5DEDE"}, {align, "center"}], "&nbsp;"}}
              end,
    {tr, [{bgcolor, "#000099"}],
     [{td, [{align, "left"}], {strong, [], {font, [{color, "white"}], ValueP1}}},
      {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Value"}}},
      {td, [{bgcolor, "#D5DEDE"}, {width, "25pt"}], "&nbsp;"},
      TD_V2_1, TD_V2_2,
      {td, [{bgcolor, "#D5DEDE"}, {width, "25pt"}], "&nbsp;"},
      TD_V3_1, TD_V3_2
     ]}.

-spec renderGossipData(gossip_pv(), {boolean(), gossip_pv()}, {boolean(), gossip_pv()},
                       string(), gossip_key(), fun((term()) -> string())) -> html_type().
renderGossipData({_P1, V1}, {P2Exists, PV2}, {P3Exists, PV3}, Name, Key, Fun) ->
    ValueV1 = format_gossip_value(V1, Key, Fun),
    {TD_V2_1, TD_V2_2} =
        case P2Exists of
            true ->
                V2 = element(2, PV2),
                {{td, [{align, "left"}], Name},
                 {td, [{align, "left"}], format_gossip_value(V2, Key, Fun)}};
            _ ->
                {{td, [{bgcolor, "#D5DEDE"}, {align, "left"}], "&nbsp;"},
                 {td, [{bgcolor, "#D5DEDE"}, {align, "left"}], "&nbsp;"}}
        end,
    {TD_V3_1, TD_V3_2} =
        case P3Exists of
            true ->
                V3 = element(2, PV3),
                {{td, [{align, "left"}], Name},
                 {td, [{align, "left"}], format_gossip_value(V3, Key, Fun)}};
            _ ->
                {{td, [{bgcolor, "#D5DEDE"}, {align, "left"}], "&nbsp;"},
                 {td, [{bgcolor, "#D5DEDE"}, {align, "left"}], "&nbsp;"}}
        end,
    {tr, [],
     [{td, [{align, "left"}], Name},
      {td, [{align, "left"}], ValueV1},
      {td, [{bgcolor, "#D5DEDE"}, {width, "25pt"}], "&nbsp;"},
      TD_V2_1, TD_V2_2,
      {td, [{bgcolor, "#D5DEDE"}, {width, "25pt"}], "&nbsp;"},
      TD_V3_1, TD_V3_2
     ]}.

-spec format_gossip_value(gossip_load:load_info(), Key::gossip_key(),
                          fun((term()) -> string())) -> string().
format_gossip_value(Value, Key, Fun) ->
    case gossip_load:load_info_get(Key, Value) of
        unknown -> "n/a";
        X       -> Fun(X)
    end.


%%%-----------------------------Monitoring----------------------------------

-spec getMonitorClientData() -> html_type().
getMonitorClientData() ->
    ClientMonitor = pid_groups:pid_of(clients_group, monitor),
    {_CountD, CountPerSD, AvgMsD, MinMsD, MaxMsD, StddevMsD, _HistMsD} =
        case statistics:getTimingMonitorStats(ClientMonitor, [{api_tx, 'req_list'}], list) of
            []                           -> {[], [], [], [], [], [], []};
            [{api_tx, 'req_list', Data}] -> Data
        end,
    AvgMinMaxMsD = lists:zipwith3(fun([Time, Avg], [Time, Min], [Time, Max]) ->
                                          [Time, Avg, Avg - Min, Max - Avg]
                                  end, AvgMsD, MinMsD, MaxMsD),

    VMMonitor = pid_groups:pid_of(basic_services, monitor),
    MemMonKeys = [{monitor_perf, X} || X <- [mem_total, mem_processes, mem_system,
                                             mem_atom, mem_binary, mem_ets]],
    case statistics:getGaugeMonitorStats(VMMonitor, MemMonKeys, list, 1024.0 * 1024.0) of
        [] -> MemTotalD = MemProcD = MemSysD = MemAtomD = MemBinD = MemEtsD = [];
        [{monitor_perf, mem_total, MemTotalD},
         {monitor_perf, mem_processes, MemProcD},
         {monitor_perf, mem_system, MemSysD},
         {monitor_perf, mem_atom, MemAtomD},
         {monitor_perf, mem_binary, MemBinD},
         {monitor_perf, mem_ets, MemEtsD}] -> ok
    end,

    IOMonKeys = [{monitor_perf, X} || X <- [%rcv_count,
                                            rcv_bytes, %send_count,
                                            send_bytes]],
    case statistics:getGaugeMonitorStats(VMMonitor, IOMonKeys, list, 1) of
        [] -> %RcvCntD0 = SendCntD0 =
            RcvBytesD0 = SendBytesD0 = [];
        [%{monitor_perf, rcv_count, RcvCntD0},
         {monitor_perf, rcv_bytes, RcvBytesD0},
         %{monitor_perf, send_count, SendCntD0},
         {monitor_perf, send_bytes, SendBytesD0}] -> ok
    end,
%%     RcvCntD = get_diff_data(lists:reverse(RcvCntD0)),
    RcvBytesD = get_diff_data(lists:reverse(RcvBytesD0)),
%%     SendCntD = get_diff_data(lists:reverse(SendCntD0)),
    SendBytesD = get_diff_data(lists:reverse(SendBytesD0)),

    DataStr =
        lists:flatten(
          ["\n",
%%            "var count_data = ",           io_lib:format("~p", [CountD]), ";\n",
           "var count_per_s_data = ",     io_lib:format("~p", [CountPerSD]), ";\n",
           "var avg_min_max_ms_data = ",  io_lib:format("~p", [AvgMinMaxMsD]), ";\n",
%%            "var avg_ms_data = ",          io_lib:format("~p", [AvgMsD]), ";\n",
%%            "var min_ms_data = ",          io_lib:format("~p", [MinMsD]), ";\n",
%%            "var max_ms_data = ",          io_lib:format("~p", [MaxMsD]), ";\n",
%%            "var hist_ms_data = ",         io_lib:format("~p", [HistMsD]), ";\n",
           "var stddev_ms_data = ",       io_lib:format("~p", [StddevMsD]), ";\n",

           "var mem_total_data = ",       io_lib:format("~p", [MemTotalD]), ";\n",
           "var mem_processes_data = ",   io_lib:format("~p", [MemProcD]), ";\n",
           "var mem_system_data = ",      io_lib:format("~p", [MemSysD]), ";\n",
           "var mem_atom_data = ",        io_lib:format("~p", [MemAtomD]), ";\n",
           "var mem_binary_data = ",      io_lib:format("~p", [MemBinD]), ";\n",
           "var mem_ets_ms_data = ",      io_lib:format("~p", [MemEtsD]), ";\n"

%%            "var io_rcv_count_data = ",    io_lib:format("~p", [RcvCntD]), ";\n",
           "var io_rcv_bytes_data = ",    io_lib:format("~p", [RcvBytesD]), ";\n",
%%            "var io_send_count_data = ",   io_lib:format("~p", [SendCntD]), ";\n",
           "var io_send_bytes_data = ",   io_lib:format("~p", [SendBytesD]), ";\n"
          ]),
    {script, [{type, "text/javascript"}], DataStr}.

%% @doc In a list containing [Time, Value] elements, compute the differences of
%%      any two consecutive values and return a list of [Time, DiffToPrevious].
%%      PreCond: values must increase monotonically!
-spec get_diff_data([T]) -> [T].
get_diff_data([]) -> [];
get_diff_data([H | T]) -> lists:reverse([H | get_diff_data(H, T)]).

-spec get_diff_data(Last::T, Rest::[T]) -> [T].
get_diff_data([_TimeLast, _ValueLast], []) -> [];
get_diff_data([_TimeLast, ValueLast], [Cur = [TimeCur, ValueCur] | Rest])
  when ValueCur >= ValueLast ->
    [[TimeCur, ValueCur - ValueLast] | get_diff_data(Cur, Rest)].

-spec getMonitorRingData() -> [html_type()].
getMonitorRingData() ->
    case pid_groups:pid_of(basic_services, monitor) of
        failed ->
            Prefix = {p, [], "NOTE: no monitor_perf in this VM"},
            DataRR = DataLH = DataTX = {[], [], [], [], [], [], []}, ok;
        Monitor ->
            Prefix = [],
            ReqKeys = [{monitor_perf, 'agg_read_read'}, {dht_node, 'agg_lookup_hops'}, {api_tx, 'agg_req_list'}],
            case statistics:getTimingMonitorStats(Monitor, ReqKeys, list) of
                [] -> DataRR = DataLH = DataTX = {[], [], [], [], [], [], []}, ok;
                [{monitor_perf, 'agg_read_read', DataRR},
                 {dht_node, 'agg_lookup_hops', DataLH},
                 {api_tx, 'agg_req_list', DataTX}] -> ok
            end
    end,
    {_RRCountD, _RRCountPerSD, RRAvgMsD, RRMinMsD, RRMaxMsD, RRStddevMsD, _RRHistMsD} = DataRR,
    {_LHCountD, _LHCountPerSD, LHAvgCountD, LHMinCountD, LHMaxCountD, LHStddevCountD, _LHHistCountD} = DataLH,
    {_TXCountD, TXCountPerSD, TXAvgCountD, TXMinCountD, TXMaxCountD, TXStddevCountD, _TXHistCountD} = DataTX,
    RRAvgMinMaxMsD = lists:zipwith3(fun([Time, Avg], [Time, Min], [Time, Max]) ->
                                            [Time, Avg, Avg - Min, Max - Avg]
                                    end, RRAvgMsD, RRMinMsD, RRMaxMsD),
    LHAvgMinMaxCountD = lists:zipwith3(fun([Time, Avg], [Time, Min], [Time, Max]) ->
                                               [Time, Avg, Avg - Min, Max - Avg]
                                       end, LHAvgCountD, LHMinCountD, LHMaxCountD),
    TXAvgMinMaxCountD = lists:zipwith3(fun([Time, Avg], [Time, Min], [Time, Max]) ->
                                               [Time, Avg, Avg - Min, Max - Avg]
                                       end, TXAvgCountD, TXMinCountD, TXMaxCountD),
    DataStr =
        lists:flatten(
          ["\n",
           "var rr_avg_min_max_ms_data = ",    io_lib:format("~p", [RRAvgMinMaxMsD]), ";\n",
           "var rr_stddev_ms_data = ",         io_lib:format("~p", [RRStddevMsD]), ";\n",
           "var lh_avg_min_max_count_data = ", io_lib:format("~p", [LHAvgMinMaxCountD]), ";\n",
           "var lh_stddev_count_data = ",      io_lib:format("~p", [LHStddevCountD]), ";\n",
           "var tx_count_per_s_data = ",       io_lib:format("~p", [TXCountPerSD]), ";\n",
           "var tx_avg_min_max_ms_data = ",    io_lib:format("~p", [TXAvgMinMaxCountD]), ";\n",
           "var tx_stddev_ms_data = ",         io_lib:format("~p", [TXStddevCountD]), ";\n"]),
    lists:flatten([Prefix, {script, [{type, "text/javascript"}], DataStr}]).


%%%-----------------------------Misc----------------------------------

dead_node() ->
    "dead node?".

-spec get_indexed_pred_id(Node::node:node_type(),
                          MyIndex::non_neg_integer() | string()) -> integer() | string().
get_indexed_pred_id(Node, MyIndex) ->
    NodeIndex = get_indexed_id_by_node(Node),
    RingSize = get_ring_size(),
    if NodeIndex =:= undefined orelse MyIndex =:= undefined -> dead_node();
       true ->
           case NodeIndex =:= MyIndex of
               true -> 0;
               _    ->
                   ((NodeIndex - MyIndex + RingSize) rem RingSize) - RingSize
           end
    end.

-spec get_indexed_succ_id(Node::node:node_type(),
                          MyIndex::non_neg_integer() | string()) -> integer() | string().
get_indexed_succ_id(Node, MyIndex) ->
    NodeIndex = get_indexed_id_by_node(Node),
    RingSize = get_ring_size(),
    if NodeIndex =:= undefined orelse MyIndex =:= undefined -> dead_node();
       true -> (NodeIndex - MyIndex + RingSize) rem RingSize
    end.

-spec get_flag(Hostname::node_details:hostname()) -> html_type().
get_flag(Hostname) ->
    Country = string:substr(Hostname, 1 + string:rchr(Hostname, $.)),
    URL = case lists:member(Country, ?COUNTRIES) of
              true  -> "icons/" ++ Country ++ ".gif";
              false -> "icons/unknown.gif"
          end,
    {img, [{src, URL}, {width, 26}, {height, 16}], []}.

%% @doc Formats the data with the format string and escapes angle brackets with
%%      their HTML counter parts so that content is not mis-interpreted as HTML
%%      tags.
-spec safe_html_string(io:format(), Data::[term()]) -> string().
safe_html_string(Format, Data) ->
    safe_html_string(io_lib:format(Format, Data)).

%% @doc Escapes angle brackets within the string with their HTML counter parts
%%      so that content is not mis-interpreted as HTML tags.
-spec safe_html_string(io_lib:chars()) -> string().
safe_html_string(String) ->
    lists:flatten([case C of
                       $< -> "&lt;";
                       $> -> "&gt;";
                       _ -> C
                   end || C <- lists:flatten(String)]).

%% @doc Pre-formats a string (useful for reading erlang terms)
-spec html_pre(io:format(), Data::[term()]) -> string().
html_pre(Format, Data) ->
    "<pre>" ++ lists:flatten(io_lib:format(Format, Data)) ++ "</pre>".
