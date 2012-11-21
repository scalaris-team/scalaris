var DC = {
    get_map: function(map) {
        $.get(map, function(data){
            data = JSON.parse(data);

            // flot styling and data conversion
            var dc = 0;
            var nodes = $.map(data.nodes, function(d) {
                dc += 1;
                return {
                    color: d.color
                    , data: d.coords
                    , label: "dc " + dc
                };
            });

            if (data.centroids){
                var centroids = {
                    color: "black"
                    , data: data.centroids
                    , label: "centroids"
                };

                nodes.push(centroids);
            }

            // print remaining data
            var stats = $("#stats").empty();
            var append = []
            $.each(data, function(id){
                if (id != "nodes" && id != "centroids") {
                    append.push("<li>"+ id + ": " + data[id] + "</li>");
                }
            });

            if (append.length > 0) {
                $("#stats").append("<h2>Additional Data</h2>")
                    .append("<ul></ul>");
                $("#stats ul").append(append.join("\n"));
            }

            // finally create the plot
            $.plot($("#graph"), nodes,{
                series: {
                    points: {show:true}
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
};

$(function(){
    DC.setup();
});
