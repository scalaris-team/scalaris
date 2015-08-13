%% Flexible Routing Tables
%% Grouped Flexible Routing Table
%% (needs to be commented out to deactivate, setting to false isn't enough!)
%% -define(GFRT, true).

-define(RT, rt_frt).
% first valid key:
-define(MINUS_INFINITY, 0).
-define(MINUS_INFINITY_TYPE, 0).
% first invalid key:
-define(PLUS_INFINITY, 16#100000000000000000000000000000000).
-define(PLUS_INFINITY_TYPE, 16#100000000000000000000000000000000).
