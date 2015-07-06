% @copyright 2012-2015 Zuse Institute Berlin

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

%% @author Maik Lange <lakedaimon300@googlemail.com>
%% @doc    Helper functions for replica repair evaluation, defining evaluation
%%         data.
%% @see    rr_eval_admin
%% @version $Id:  $
-module(rr_eval_point).

-include("scalaris.hrl").

-export([generate_ep/3, generate_ep_rounds/4, column_names/0, mp_column_names/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TYPES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([measure_point/0, eval_point/0, point_id/0]).

-type point_id()      :: non_neg_integer().
-type measure_point() :: {
                          ID          :: point_id(),  %test-set unique id
                          Iteration   :: non_neg_integer(), %iteration count
                          Round       :: non_neg_integer(), %round
                          Missing     :: integer(),   % missing replicas
                          Regen       :: integer(),   % regenerated
                          Outdated    :: integer(),   % outdated replaces
                          Updated     :: integer(),   % updated replicas
                          BW_RC_Size  :: integer(),   % number of recon-msg bytes
                          BW_RC_Msg   :: integer(),   % number of recon messages
                          BW_RC2_Size :: integer(),   % number of recon2-msg bytes
                          BW_RC2_Msg  :: integer(),   % number of recon2 messages
                          BW_RS_Size  :: integer(),   % number of transmitted resolve bytes
                          BW_RS_Msg   :: integer(),   % number of resolve messages
                          BW_RS_KVV   :: integer()    % number of kvv-triple send
                          }.

-type eval_point() :: {
                       ID               :: point_id(),  %test-set unique id
                       %PARAMETER
                       NodeCount        :: integer(),
                       DataCount        :: integer(),
                       FProb            :: 0..100,
                       Round            :: integer(),
                       ReconP1E         :: float(),
                       Merkle_Bucket    :: pos_integer(),
                       Merkle_Branch    :: pos_integer(),
                       Art_Corr_Fac     :: non_neg_integer(),
                       Art_Leaf_Fpr     :: float(),
                       Art_Inner_Fpr    :: float(),
                       % AVG MEASUREMENT POINT
                       Missing          :: float(),   % missing replicas
                       Regen            :: float(),   % regenerated
                       Outdated         :: float(),   % outdated replaces
                       Updated          :: float(),   % updated replicas
                       BW_RC_Size       :: float(),   % number of recon-msg bytes
                       BW_RC_Msg        :: float(),   % number of recon messages
                       BW_RC2_Size      :: float(),   % number of recon2-msg bytes
                       BW_RC2_Msg       :: float(),   % number of recon2 messages
                       BW_RS_Size       :: float(),   % number of transmitted resolve bytes
                       BW_RS_Msg        :: float(),   % number of resolve messages
                       BW_RS_KVV        :: float(),   % number of kvv-triple send
                       % STD. DEVIATION OF MEASUREMENT
                       SD_Missing       :: float(),
                       SD_Regen         :: float(),
                       SD_Outdated      :: float(),
                       SD_Updated       :: float(),
                       SD_BW_RC_Size    :: float(),
                       SD_BW_RC_Msg     :: float(),
                       SD_BW_RC2_Size    :: float(),
                       SD_BW_RC2_Msg     :: float(),
                       SD_BW_RS_Size    :: float(),
                       SD_BW_RS_Msg     :: float(),
                       SD_BW_RS_KVV     :: float(),
                       % MIN/MAX OF AVG
                       MIN_Missing      :: integer(),
                       MAX_Missing      :: integer(),
                       MIN_Regen        :: integer(),
                       MAX_Regen        :: integer(),
                       MIN_Outdated     :: integer(),
                       MAX_Outdated     :: integer(),
                       MIN_Updated      :: integer(),
                       MAX_Updated      :: integer(),
                       MIN_BW_RC_Size   :: integer(),
                       MAX_BW_RC_Size   :: integer(),
                       MIN_BW_RC_Msg    :: integer(),
                       MAX_BW_RC_Msg    :: integer(),
                       MIN_BW_RC2_Size  :: integer(),
                       MAX_BW_RC2_Size  :: integer(),
                       MIN_BW_RC2_Msg   :: integer(),
                       MAX_BW_RC2_Msg   :: integer(),
                       MIN_BW_RS_Size   :: integer(),
                       MAX_BW_RS_Size   :: integer(),
                       MIN_BW_RS_Msg    :: integer(),
                       MAX_BW_RS_Msg    :: integer(),
                       MIN_BW_RS_KVV    :: integer(),
                       MAX_BW_RS_KVV    :: integer(),
                       % ADDITIONAL PARAMETERS
                       RC_METHOD        :: rr_recon:method(),
                       RING_TYPE        :: rr_eval_admin:ring_type(),
                       DDIST            :: rr_eval_admin:data_distribution(),
                       FTYPE            :: db_generator:failure_type(),
                       FDIST            :: rr_eval_admin:fail_distribution(),
                       DTYPE            :: db_generator:db_type(),
                       TPROB            :: 0..100
                      }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc list of eval_point field names
-spec column_names() -> [atom()].
column_names() ->
    [id,
     %Parameter
     nodes, dbsize, fprob, round,
     recon_p1e, merkle_bucket, merkle_branch, art_corr_factor, art_leaf_fpr,
     art_inner_fpr,
     % Avg Measure
     missing, regen, outdated, updated,
     bw_rc_size, bw_rc_msg, bw_rc2_size, bw_rc2_msg,
     bw_rs_size, bw_rs_msg, bw_rs_kvv,
     % Std Deviation
     sd_missing, sd_regen, sd_outdated, sd_updated,
     sd_bw_rc_size, sd_bw_rc_msg, sd_bw_rc2_size, sd_bw_rc2_msg,
     sd_bw_rs_size, sd_bw_rs_msg, sd_bw_rs_kvv,
     % Min/Max
     min_missing, max_missing, min_regen, max_regen,
     min_outdated, max_outdated, min_updated, max_updated,
     min_bw_rc_size, max_bw_rc_size, min_rc_msg, max_rc_msg,
     min_bw_rc2_size, max_bw_rc2_size, min_rc2_msg, max_rc2_msg,
     min_bw_rs_size, max_bw_rs_size, min_bw_rs_msg, max_bw_rs_msg,
     min_bw_rs_kvv, max_bw_rs_kvv,
     % additional parameters originally missing
     rc_method, ring_type, ddist, ftype, fdist, dtype, tprob
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc list of measurement_point field names
-spec mp_column_names() -> [atom()].
mp_column_names() ->
    [id, iteration, round, missing, regen, outdated, updated,
     bw_rc_size, bw_rc_msg, bw_rc2_size, bw_rc2_msg,
     bw_rs_size, bw_rs_msg, bw_rs_kvv].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec generate_ep_rounds(point_id(), rr_eval_admin:ring_setup(), [measure_point()],
                         Rounds::non_neg_integer()) -> [eval_point()].
generate_ep_rounds(ID, Conf, MPList, Rounds) ->
    lists:foldl(fun(Round, Acc) ->
                        RData = [X || X <- MPList, element(3, X) =:= Round],
                        EP = generate_ep(ID + Round - 1, Conf, RData),
                        [EP | Acc]
                end,
                [],
                lists:seq(1, Rounds)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec generate_ep(point_id(), rr_eval_admin:ring_setup(), [measure_point()])
        -> eval_point().
generate_ep(ID,
            {{scenario, RingType, DDist, FType, FDist, DType, TProb},
             {ring_config, NC, DC, FProb, _FDest, _Rounds},
             {rc_config, RCMethod, RcP1E, MBU, MBR, ArtCF, ArtLF, ArtIF, _AlignToBytes}},
            MP) ->
    %% MEAN %%
    %DB stats
    {MeanM, ErrM}       = mean_w_error(4, MP),
    {MeanR, ErrR}       = mean_w_error(5, MP),
    {MeanO, ErrO}       = mean_w_error(6, MP),
    {MeanU, ErrU}       = mean_w_error(7, MP),
    %RC, RC2, RS
    {MeanRCS, ErrRCS}   = mean_w_error(8, MP),
    {MeanRCM, ErrRCM}   = mean_w_error(9, MP),
    {MeanRC2S, ErrRC2S} = mean_w_error(10, MP),
    {MeanRC2M, ErrRC2M} = mean_w_error(11, MP),
    {MeanRSS, ErrRSS}   = mean_w_error(12, MP),
    {MeanRSM, ErrRSM}   = mean_w_error(13, MP),
    {MeanRSK, ErrRSK}   = mean_w_error(14, MP),
    
    %% MIN/MAX %%
    %DB stats
    {MinM, MaxM}        = min_max_element(4, MP),
    {MinR, MaxR}        = min_max_element(5, MP),
    {MinO, MaxO}        = min_max_element(6, MP),
    {MinU, MaxU}        = min_max_element(7, MP),
    %RC, RC2, RS
    {MinRCS, MaxRCS}    = min_max_element(8, MP),
    {MinRCM, MaxRCM}    = min_max_element(9, MP),
    {MinRC2S, MaxRC2S}  = min_max_element(10, MP),
    {MinRC2M, MaxRC2M}  = min_max_element(11, MP),
    {MinRSS, MaxRSS}    = min_max_element(12, MP),
    {MinRSM, MaxRSM}    = min_max_element(13, MP),
    {MinRSK, MaxRSK}    = min_max_element(14, MP),

    {ID,
     NC, 4 * DC, FProb, element(3, hd(MP)),
     RcP1E, MBU, MBR, ArtCF, ArtLF, ArtIF,
     MeanM, MeanR, MeanO, MeanU,
     MeanRCS, MeanRCM, MeanRC2S, MeanRC2M, MeanRSS, MeanRSM, MeanRSK,
     ErrM, ErrR, ErrO, ErrU,
     ErrRCS, ErrRCM, ErrRC2S, ErrRC2M, ErrRSS, ErrRSM, ErrRSK,
     MinM, MaxM, MinR, MaxR, MinO, MaxO, MinU, MaxU,
     MinRCS, MaxRCS, MinRCM, MaxRCM, MinRC2S, MaxRC2S, MinRC2M, MaxRC2M,
     MinRSS, MaxRSS, MinRSM, MaxRSM, MinRSK, MaxRSK,
     RCMethod, RingType, dist_to_name(DDist), FType, dist_to_name(FDist),
     DType, TProb}.

-spec dist_to_name(Dist::random | uniform | {binomial, P::float()}) -> atom().
dist_to_name({binomial, P}) ->
    DistName = lists:flatten(io_lib:format("binomial_~f", [P])),
    try erlang:list_to_existing_atom(DistName)
    catch error:badarg -> erlang:list_to_atom(DistName)
    end;
dist_to_name(Dist) ->
    Dist.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec mean_w_error(integer(), [tuple()]) -> {Mean::float(), StdError::float()}.
mean_w_error(_ElementPos, []) ->
    {0, 0};
mean_w_error(ElementPos, [_|_] = TList) ->
    {Sum, Sum2} = lists:foldl(fun(T, {X1, X2}) ->
                                      E = element(ElementPos, T),
                                      {X1 + E, X2 + E * E}
                              end, {0, 0}, TList),
    Len = length(TList),
    Mean = Sum / Len,
    {Mean, math:sqrt(Sum2 / Len - Mean * Mean)}.

-spec min_max_element(Element::integer(), [tuple()])
        -> {Min::integer(), Max::integer()}.
min_max_element(_Pos, []) ->
    {0, 0};
min_max_element(Pos, [_|_] = TList) ->
    List = [element(Pos, X) || X <- TList],
    { lists:min(List), lists:max(List) }.
