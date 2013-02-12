-module(api_autoscale).

-include("scalaris.hrl").
-compile(export_all).

toggle_alarm(Name) ->
	api_dht_raw:unreliable_lookup(?RT:hash_key("0"), {send_to_group_member, autoscale,
                                        {toggle_alarm, Name}}).
toggle_alarms(Names) ->
	lists:foreach(fun toggle_alarm/1, Names).

deactivate_alarms () ->
	api_dht_raw:unreliable_lookup(?RT:hash_key("0"), {send_to_group_member, autoscale,
                                        {deactivate_alarms}}).