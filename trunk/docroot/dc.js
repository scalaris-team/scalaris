var w = 800,
    h = 500,
    updateinterval = 5, // seconds
    timer = null
    ;
var canvas = d3.selectAll("#graph")
                .append("svg")
                    .attr("width", w)
                    .attr("height", h);

function setUpdateInterval(i) {
    updateinterval = i;
}

function setTimer(interval) {
    if (timer) {
        clearTimeout(timer);
    }
    timer = setTimeout(updateVisualization, 1000*updateinterval);
}

function stopTimer() {
    if (timer)
        clearTimeout(timer);
}

var input = d3.select("input#updateInterval");
    input.on("change", function(){ 
        setUpdateInterval(this.value);
    })
    ;

input
    .attr("value", updateinterval)
    ;

var clustering = {
    circleradius: 5,
    canvas: canvas,
    updateEnabled: true,
    createMap: function(data) {
        var circleradius = this.circleradius;

        // statistics
        var stats = d3.select("#stats")
            .select("ul")
            .selectAll("li")
                .data([
                        ["Clusters", data.clusters.length]
                        , ["Mean relative cluster size", data.clusters.reduce(function(acc,b) {
                            return acc + b.size;
                        }, 0) / data.clusters.length]
                        , ["Zombie of size 0", data.clusters.reduce(function(acc, b) {
                            return b.size > 0 ? acc : acc + 1;
                        }, 0)]
                        , ["Current epoch", data.epoch]
                        , ["Cluster radius", data.cluster_radius]
                        ])
                    .text(function(d) { return d.join(": "); });

        stats.enter()
            .append("li")
            .text(function(d) { return d.join(": "); });

        stats.exit().remove() ;

        // construct the scales
        var xscale = d3.scale.linear()
            .domain(data.domain[0])
            .range([0,w])
            .nice(),
            yscale = d3.scale.linear()
                .domain(data.domain[1])
                .range([0,h])
                .nice();

        // add nodes
        this.canvas.append("g")
            .selectAll("circle")
            .data(data.nodes)
            .enter().append("circle")
            .attr("r", "1")
            .attr("cx", function(d){ return xscale(d.coords[0]); })
            .attr("cy", function(d){ return yscale(d.coords[1]); })
            .attr("r", this.circleradius)
            .attr("fill", function(d) { return d.color; })
            .attr("stcallyle","node")
            ;

        // add clusters
        this.canvas.append("g")
            .selectAll("circle")
            .data(data.clusters)
            .enter()
            .append("circle")
            .attr("cx", function(d){ return xscale(d.coords[0]); })
            .attr("cy", function(d){ return yscale(d.coords[1]); })
            .attr("r", function(d){return d.size * circleradius;})
            .attr("fill", "black")
            .attr("style","node")
            ;

        this.canvas.append("g")
            .selectAll("circle")
            .data(data.clusters)
            .enter()
            .append("circle")
            .attr("cx", function(d){ return xscale(d.coords[0]); })
            .attr("cy", function(d){ return yscale(d.coords[1]); })
            .attr("r", this.circleradius)
            .attr("fill", "none")
            .attr("stroke", "black")
            .attr("style","node")
            ;

        {
            // Draw lines to indicate where the cluster borders are
            this.canvas.append("g")
                .selectAll("line")
                    .data(data.clusters)
                .enter().append("line")
                    .attr("x1", function(d){ return xscale(d.coords[0] - data.cluster_radius); })
                    .attr("y1", function(d){ return yscale(d.coords[1] - data.cluster_radius); })
                    .attr("x2", function(d){ return xscale(d.coords[0] + data.cluster_radius); })
                    .attr("y2", function(d){ return yscale(d.coords[1] + data.cluster_radius); })
                    .attr("r", this.circleradius)
                    .attr("stroke", "yellow")
                ;
            this.canvas.append("g")
                .selectAll("line")
                    .data(data.clusters)
                .enter().append("line")
                    .attr("x1", function(d){ return xscale(d.coords[0] - data.cluster_radius); })
                    .attr("y1", function(d){ return yscale(d.coords[1] + data.cluster_radius); })
                    .attr("x2", function(d){ return xscale(d.coords[0] + data.cluster_radius); })
                    .attr("y2", function(d){ return yscale(d.coords[1] - data.cluster_radius); })
                    .attr("r", this.circleradius)
                    .attr("stroke", "yellow")
                ;
        }

        // axis
        var xaxis = d3.svg.axis()
                .scale(xscale)
                .orient("bottom")
            ,
            yaxis = d3.svg.axis()
                .scale(yscale)
                .orient("right")
                ;

        this.canvas.append("g")
            .attr("class", "axis")
            .call(xaxis)
            ;

        this.canvas.append("g")
            .attr("class", "axis")
            .call(yaxis)
            ;
    },
    redoCanvas: function() {
        clustering.canvas.remove();
        clustering.canvas = d3.selectAll("#graph")
            .append("svg")
            .attr("width", w)
            .attr("height", h);
    }

};

function updateVisualization() {
    clustering.redoCanvas();

    var updater = (function(this_clustering, data) {
        clustering.createMap.bind(this_clustering)(data);
    }).bind(this, clustering);
    d3.json("dcMap.yaws", updater);

    if (clustering.updateEnabled) {
        setTimer(updateinterval);
    }
}

function disableUpdate() {
    clustering.updateEnabled = false;
}

function enableUpdate() {
    clustering.updateEnabled = true;
}

function toggleUpdate() {
    clustering.updateEnabled = !clustering.updateEnabled;
    d3.select("#toggleUpdate")
        .text(clustering.updateEnabled ? "Disable automatic update" : "Enable automatic update")
        ;
    if(clustering.updateEnabled) {
        updateVisualization();
    } else {
        stopTimer();
    }
}

if (clustering.updateEnabled) {
    setTimer(updateinterval);
}

updateVisualization();
