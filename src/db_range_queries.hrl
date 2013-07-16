-spec get_range(db_t(), intervals:interval()) -> [{?RT:key(), any()}].
get_range(_DBName, []) ->
    [];
get_range(DBName, Interval) ->
    {Start, End} = case intervals:get_bounds(Interval) of
        {'(', S, E, ')'} -> {S + 1, E - 1};
        {'(', S, E, ']'} -> {S + 1, E};
        {'[', S, E, ')'} -> {S, E - 1};
        {'[', S, E, ']'} -> {S, E}
    end,
    [Entry || K <- lists:seq(Start, End), 
              Entry <- [get(DBName, K)], 
              Entry =/= {}].
    
