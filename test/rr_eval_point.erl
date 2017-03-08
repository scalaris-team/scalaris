% @copyright 2012-2016 Zuse Institute Berlin

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
%% @author Nico Kruber <kruber@zib.de>
%% @doc    Helper functions for replica repair evaluation, defining evaluation
%%         data.
%% @see    rr_eval_admin
%% @version $Id:  $
-module(rr_eval_point).

-include("rr_records.hrl").

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
                          BW_RS_KVV   :: integer(),   % number of kvv-triple send
                          Fr_p1       :: float() | '-', % effective worst-case probability of phase 1
                          Fr_p2       :: float() | '-', % effective worst-case probability of phase 2
                          Fr          :: float() | '-', % effective total worst-case probability
                          MerkleInner :: non_neg_integer() | '-', % number of inner nodes on the initiator in case of Merkle sync
                          MerkleLeaf  :: non_neg_integer() | '-', % number of total leaf nodes on the initiator in case of Merkle sync
                          MerkleEmptyLeaf :: non_neg_integer() | '-'  % number of empty leaf nodes on the initiator in case of Merkle sync
                          }.

-type eval_point() :: {
                       ID               :: point_id(),  %test-set unique id
                       %PARAMETER
                       NodeCount        :: integer(),
                       DataCount        :: integer(),
                       FRate            :: 0..100,
                       Round            :: integer(),
                       ReconFR          :: float(),
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
                       SD_BW_RC2_Size   :: float(),
                       SD_BW_RC2_Msg    :: float(),
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
                       RING_TYPE        :: ring_type(),
                       DDIST            :: data_distribution(),
                       FTYPE            :: db_generator:failure_type(),
                       FDIST            :: fail_distribution(),
                       DTYPE            :: db_generator:db_type(),
                       TPROB            :: 0..100,
                       Merkle_NumTrees  :: pos_integer(),
                       % AVG, STD, MIN, MAX effective failure rate of phase 1
                       MeanFr_p1        :: float() | '-',
                       ErrFr_p1         :: float() | '-',
                       MinFr_p1         :: float() | '-',
                       MaxFr_p1         :: float() | '-',
                       % AVG, STD, MIN, MAX effective failure rate  of phase 2
                       MeanFr_p2        :: float() | '-',
                       ErrFr_p2         :: float() | '-',
                       MinFr_p2         :: float() | '-',
                       MaxFr_p2         :: float() | '-',
                       % AVG, STD, MIN, MAX total effective failure rate
                       MeanFr           :: float() | '-',
                       ErrFr            :: float() | '-',
                       MinFr            :: float() | '-',
                       MaxFr            :: float() | '-',
                       % misc
                       ExpectedDelta    :: number(),
                       % AVG, STD, MIN, MAX number of merkle tree inner nodes
                       MeanMerkleInner  :: float() | '-',
                       ErrMerkleInner   :: float() | '-',
                       MinMerkleInner   :: float() | '-',
                       MaxMerkleInner   :: float() | '-',
                       % AVG, STD, MIN, MAX number of total merkle tree leaf nodes
                       MeanMerkleLeaf   :: float() | '-',
                       ErrMerkleLeaf    :: float() | '-',
                       MinMerkleLeaf    :: float() | '-',
                       MaxMerkleLeaf    :: float() | '-',
                       % AVG, STD, MIN, MAX number of empty merkle tree leaf nodes
                       MeanMerkleEmptyLeaf :: float() | '-',
                       ErrMerkleEmptyLeaf  :: float() | '-',
                       MinMerkleEmptyLeaf  :: float() | '-',
                       MaxMerkleEmptyLeaf  :: float() | '-'
                      }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc list of eval_point field names
-spec column_names() -> [atom()].
column_names() ->
    [id,
     %Parameter
     nodes, dbsize, fprob, round,
     recon_fail_rate, merkle_bucket, merkle_branch, art_corr_factor, art_leaf_fpr,
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
     rc_method, ring_type, ddist, ftype, fdist, dtype, tprob, merkle_num_trees,
     % AVG, STD, MIN, MAX effective failure rate of phase 1 and 2
     fr_p1, sd_fr_p1, min_fr_p1, max_fr_p1,
     fr_p2, sd_fr_p2, min_fr_p2, max_fr_p2,
     fr, sd_fr, min_fr, max_fr,
     expected_delta,
     merkle_inner, sd_merkle_inner, min_merkle_inner, max_merkle_inner,
     merkle_leaf, sd_merkle_leaf, min_merkle_leaf, max_merkle_leaf,
     merkle_empty_leaf, sd_merkle_empty_leaf, min_merkle_empty_leaf, max_merkle_empty_leaf
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc list of measurement_point field names
-spec mp_column_names() -> [atom()].
mp_column_names() ->
    [id, iteration, round, missing, regen, outdated, updated,
     bw_rc_size, bw_rc_msg, bw_rc2_size, bw_rc2_msg,
     bw_rs_size, bw_rs_msg, bw_rs_kvv, fr_p1, fr_p2, fr,
     merkle_inner, merkle_leaf, merkle_empty_leaf].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec generate_ep_rounds(point_id(), ring_setup(), [measure_point()],
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

-spec generate_ep(point_id(), ring_setup(), [measure_point()])
        -> eval_point().
generate_ep(ID,
            {#scenario{ring_type = RingType, data_distribution = DDist,
                       data_failure_type = FType, fail_distribution = FDist,
                       data_type = DType, trigger_prob = TProb},
             #ring_config{node_count = NC, data_count = DC, data_failure_rate = FRate},
             #rc_config{recon_method = RCMethod, recon_fail_rate = RcFR,
                        expected_delta = ExpDelta,
                        merkle_bucket= MBU, merkle_branch = MBR,
                        merkle_num_trees = MNT, art_corr_factor = ArtCF,
                        art_leaf_fpr = ArtLF, art_inner_fpr = ArtIF}},
            MP) ->
    %% MEAN, STDDEV, MIN, MAX %%
    %DB stats
    {MeanM, ErrM, MinM, MaxM}       = mean_w_error(4, MP),
    {MeanR, ErrR, MinR, MaxR}       = mean_w_error(5, MP),
    {MeanO, ErrO, MinO, MaxO}       = mean_w_error(6, MP),
    {MeanU, ErrU, MinU, MaxU}       = mean_w_error(7, MP),
    %RC, RC2, RS
    {MeanRCS, ErrRCS, MinRCS, MaxRCS}   = mean_w_error(8, MP),
    {MeanRCM, ErrRCM, MinRCM, MaxRCM}   = mean_w_error(9, MP),
    {MeanRC2S, ErrRC2S, MinRC2S, MaxRC2S} = mean_w_error(10, MP),
    {MeanRC2M, ErrRC2M, MinRC2M, MaxRC2M} = mean_w_error(11, MP),
    {MeanRSS, ErrRSS, MinRSS, MaxRSS}   = mean_w_error(12, MP),
    {MeanRSM, ErrRSM, MinRSM, MaxRSM}   = mean_w_error(13, MP),
    {MeanRSK, ErrRSK, MinRSK, MaxRSK}   = mean_w_error(14, MP),
    
    {MeanFr_p1, ErrFr_p1, MinFr_p1, MaxFr_p1} =
        try mean_w_error(15, MP)
        catch _:_ -> {'-', '-', '-', '-'}
        end,
    {MeanFr_p2, ErrFr_p2, MinFr_p2, MaxFr_p2} =
        try mean_w_error(16, MP)
        catch _:_ -> {'-', '-', '-', '-'}
        end,
    {MeanFr, ErrFr, MinFr, MaxFr} =
        try mean_w_error(17, MP)
        catch _:_ -> {'-', '-', '-', '-'}
        end,

    {MeanMerkleI, ErrMerkleI, MinMerkleI, MaxMerkleI} =
        try mean_w_error(18, MP)
        catch _:_ -> {'-', '-', '-', '-'}
        end,
    {MeanMerkleL, ErrMerkleL, MinMerkleL, MaxMerkleL} =
        try mean_w_error(19, MP)
        catch _:_ -> {'-', '-', '-', '-'}
        end,
    {MeanMerkleEmptyL, ErrMerkleEmptyL, MinMerkleEmptyL, MaxMerkleEmptyL} =
        try mean_w_error(20, MP)
        catch _:_ -> {'-', '-', '-', '-'}
        end,

    {ID,
     NC, 4 * DC, FRate, element(3, hd(MP)),
     RcFR, MBU, MBR, ArtCF, ArtLF, ArtIF,
     MeanM, MeanR, MeanO, MeanU,
     MeanRCS, MeanRCM, MeanRC2S, MeanRC2M, MeanRSS, MeanRSM, MeanRSK,
     ErrM, ErrR, ErrO, ErrU,
     ErrRCS, ErrRCM, ErrRC2S, ErrRC2M, ErrRSS, ErrRSM, ErrRSK,
     MinM, MaxM, MinR, MaxR, MinO, MaxO, MinU, MaxU,
     MinRCS, MaxRCS, MinRCM, MaxRCM, MinRC2S, MaxRC2S, MinRC2M, MaxRC2M,
     MinRSS, MaxRSS, MinRSM, MaxRSM, MinRSK, MaxRSK,
     RCMethod, RingType, dist_to_name(DDist), FType, dist_to_name(FDist),
     DType, TProb, MNT,
     MeanFr_p1, ErrFr_p1, MinFr_p1, MaxFr_p1,
     MeanFr_p2, ErrFr_p2, MinFr_p2, MaxFr_p2,
     MeanFr, ErrFr, MinFr, MaxFr,
     ExpDelta,
     MeanMerkleI, ErrMerkleI, MinMerkleI, MaxMerkleI,
     MeanMerkleL, ErrMerkleL, MinMerkleL, MaxMerkleL,
     MeanMerkleEmptyL, ErrMerkleEmptyL, MinMerkleEmptyL, MaxMerkleEmptyL}.

-spec dist_to_name(Dist::random | uniform | {binomial, P::float()}) -> atom().
dist_to_name({binomial, P}) ->
    DistName = lists:flatten(io_lib:format("binomial_~f", [P])),
    try erlang:list_to_existing_atom(DistName)
    catch error:badarg -> erlang:list_to_atom(DistName)
    end;
dist_to_name(Dist) ->
    Dist.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec mean_w_error(integer(), [tuple()])
        -> {Mean::float(), StdError::float(), Min::X, Max::X}
        when is_subtype(X, number()).
mean_w_error(_ElementPos, []) ->
    {0.0, 0.0, 0, 0};
mean_w_error(ElementPos, [H | TL]) ->
    HE = element(ElementPos, H),
    % Note: calculate the standard deviation via shifted values to be more
    %       accurate with floating point values
    %       -> https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Computing_shifted_data
    {Len, Sum, SumStd1, SumStd2, Min, Max} =
        lists:foldl(
          fun(T, {L, SummAcc, SumStd1Acc, SumStd2Acc, Min, Max}) ->
                  E = element(ElementPos, T),
                  E2 = E - HE,
                  {L + 1, SummAcc + E, SumStd1Acc + E2, SumStd2Acc + E2 * E2,
                   erlang:min(E, Min), erlang:max(E, Max)}
          end, {1, HE, 0, 0, HE, HE}, TL),
    % pay attention to possible loss of precision here:
    {Sum / Len, math:sqrt((Len * SumStd2 - SumStd1 * SumStd1) / (Len * Len)), Min, Max}.
