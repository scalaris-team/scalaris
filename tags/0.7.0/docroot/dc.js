var DC = {
    previous_point: null
    , get_map: function(map) {
        $.get(map, function(data) {
            $("div#loading > p").text("Received data, setting up chart..");
            data = JSON.parse(data);
            $("#zoom_out").prop("disabled", true);

            // flot styling and data conversion
            var dc = 0
                , hosts = {}
                , enable_legend = false
                ;

            var nodes = $.map(data.nodes, function(d) {
                // hostname not seen before
                if (hosts[d.info.host] === undefined) {
                    hosts[d.info.host] = {};
                }

                // VM on hostname not seen before
                // The VM is determined by its unique color
                if(hosts[d.info.host][d.color] === undefined) {
                    dc += 1;
                    hosts[d.info.host][d.color] = true;
                    enable_legend = true;
                } else {
                    enable_legend = false;
                }
                options = {
                    color: d.color
                    , data: [d.info.coords]
                    , info: d.info
                };
                if (enable_legend) {
                    options.label = {
                        dc: "dc " + hosts[d.info.host]
                        , name: d.info.name
                        , host: d.info.host
                    };
                }
                return options;
            });

            if (data.centroids){
                $.each(data.centroids, function(_index, c){
                    var centroid = {
                        color: "black"
                        , data: [c.coords]
                        , label: {host: "centroids"}
                        , info: {
                            host: "centroid"
                            , radius: c.radius
                        }
                    };
                    nodes.push(centroid);
                });

            }

            // print remaining data
            var stats = $("#stats").empty(),
                append = [];
            $.each(data, function(id){
                if (id !== "nodes" && id !== "centroids") {
                    append.push("<li>"+ id + ": " + data[id] + "</li>");
                }
            });

            if (append.length > 0) {
                $("#stats").append("<h2>Additional Data</h2>")
                    .append("<ul></ul>");
                $("#stats ul").append(append.join("\n"));
            }

            $("div#loading > p").text("");

            var plot_options = {
                series: {
                    points: {show:true}
                    , hoverable: true
                    , clickable: true
                }
                , grid: {
                    hoverable: true
                    , clickable: true
                }
                , legend: {
                    show: true
                    , labelFormatter: function(label, series) {
                        return label.host;
                    }
                    , container: $("#legend")
                }
                , selection: {
                    mode: "xy"
                }
            };

            // finally create the plot
            function redraw(options) {
                var additional_sets,
                    ranges;

                if (options){
                    additional_sets = options.additional_sets;
                    ranges = options.ranges;
                }
                
                var data = nodes.slice(); // copy array
                if (additional_sets) {
                    data.unshift(additional_sets);
                }

                if (ranges) {
                    $.extend(plot_options,
                        {
                            xaxis: {min: ranges.xaxis.from, max: ranges.xaxis.to}
                            , yaxis: {min: ranges.yaxis.from, max: ranges.yaxis.to}
                        }
                    );
                    $("#zoom_out").prop("disabled", false);
                }

                $.plot($("#graph"), data, plot_options);
            }

            redraw();

            /* Eventhandler for clicks and selections */

            $("#zoom_out").click(function() {
               $(this).prop("disabled", true);
               delete plot_options.xaxis;
               delete plot_options.yaxis;
               redraw();
            });

            $("#graph").bind("plotselected", function (event, ranges) {
                redraw({ranges: ranges});
            });

            var previous_point;
            $("#graph").bind("plothover", function(event, pos, item) {
                if (item) {
                    if (previousPoint !== item.dataIndex) {
                        previousPoint = item.dataIndex;

                        $("#tooltip").remove();
                        var x = item.datapoint[0].toFixed(2),
                            y = item.datapoint[1].toFixed(2);

                        var label;

                        if (item.series.info) {
                            if (item.series.info.name !== undefined) {
                                label = item.series.info.name + "@" + item.series.info.host;
                            } else {
                                // for centroids
                                label = DC.formatCentroidLabel(item.series.info.host
                                                               , item.series.info.radius);
                            }
                            DC.showTooltip(item.pageX, item.pageY, label);
                        }
                    }
                } else {
                    $("#tooltip").remove();
                    previousPoint = null;            
                }
            });

            var previousClickedPoint = null;
            $("#graph").bind("plotclick", function(event, pos, item){
                if(item) {
                    if (previousClickedPoint !== item) {
                        if (previousClickedPoint) {
                            var x = parseFloat(item.datapoint[0].toFixed(2)),
                                y = parseFloat(item.datapoint[1].toFixed(2));
                            var prev_x = parseFloat(previousClickedPoint.item.datapoint[0].toFixed(2)),
                                prev_y = parseFloat(previousClickedPoint.item.datapoint[1].toFixed(2));

                            /* connect points */
                            /* calculate distance */
                            var distance = Math.sqrt(Math.pow(x-prev_x,2) + Math.pow(y-prev_y,2));

                            line_between_nodes = {
                                data: [[x,y],[prev_x, prev_y]]
                                , lines: {show: true}
                                , points: {
                                    show: false
                                    , radius: 0
                                }
                            };
                            redraw({additional_sets: line_between_nodes});

                            $("#distanceLine").remove();
                            DC.showTooltip((pos.pageX+previousClickedPoint.pos.pageX)/2,
                                (pos.pageY+previousClickedPoint.pos.pageY)/2, distance,
                                "distanceLine"
                                );
                        }
                        previousClickedPoint = {
                            item: item
                            , pos: pos
                        };
                    }
                } else {
                    previousClickedPoint = null;
                    $("#distanceLine").remove();
                    redraw();
                }
            });
        });
    }
    , setup: function() {
        var loc = location.href.split("/")
        .pop()
        .split(".")[0]
        ;

        // set up graph
        if (loc === "vivaldi" || loc === "dc") {
            DC.get_map(loc + "Map.yaws");
        }
    }
    , formatCentroidLabel: function(host, radius){
        return "<p><b>" + host + "</b><br/>radius: " + radius + "</p>";
    }
    , showTooltip: function (x, y, contents, id) {
        if (!id){
            id = "tooltip";
        }
        $('<div id="' + id + '">' + contents + '</div>').css({
            position: 'absolute',
            display: 'none',
            top: y + 5,
            left: x + 5,
            border: '1px solid #fdd',
            padding: '2px',
            'background-color': '#fee',
            opacity: 0.80
        }).appendTo("body").fadeIn(200);
    }
};

$(function(){
    DC.setup();
});
