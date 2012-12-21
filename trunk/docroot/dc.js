var DC = {
    previous_point: null
    , get_map: function(map) {
        $.get(map, function(data) {
            data = JSON.parse(data);

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

            // finally create the plot
            $.plot($("#graph"), nodes, {
                series: {
                    points: {show:true}
                    , hoverable: true
                    , clickable: true
                }
                , grid: {
                    hoverable: true
                }
                , legend: {
                    show: true
                    , labelFormatter: function(label, series) {
                        return label.host;
                    }
                    , container: $("#legend")
                }
            });

            $("#graph").bind("plothover", function(event, pos, item) {
                if (item) {
                    if (previousPoint !== item.dataIndex) {
                        previousPoint = item.dataIndex;

                        $("#tooltip").remove();
                        var x = item.datapoint[0].toFixed(2),
                        y = item.datapoint[1].toFixed(2);

                        var label;

                        if (item.series.info.name !== undefined) {
                            label = item.series.info.name + "@" + item.series.info.host;
                        } else {
                            // for centroids
                            label = DC.formatCentroidLabel(item.series.info.host
                                                       ,item.series.info.radius
                            );
                        }

                        DC.showTooltip(item.pageX
                            , item.pageY
                            , label
                            );
                    }
                }
                else {
                    $("#tooltip").remove();
                    previousPoint = null;            
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
    , showTooltip: function (x, y, contents) {
        $('<div id="tooltip">' + contents + '</div>').css({
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
