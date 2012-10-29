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
        var stats = d3.select("#numClusters")
            .selectAll("p")
                .data([
                        ["Clusters", data.clusters.length]
                        , ["Mean relative cluster size", data.clusters.reduce(function(acc,b) {
                            return acc + b.size;
                      }, 0) / data.clusters.length]
                        , ["Zombie of size 0", data.clusters.reduce(function(acc, b) {
                            return b.size > 0 ? acc : acc + 1;
                        }, 0)]
                        ])
                    .text(function(d) { return d.join(": "); });

        stats.enter()
            .append("p")
            .text(function(d) { return d.join(": "); });

        stats.exit().remove() ;

        // construct the scales
        var domainx = data.domain[0],
            domainy = data.domain[1];

        var borderx = (domainx[1] - domainx[0]) * 0.2,
            bordery = (domainy[1] - domainy[0]) * 0.2;

        var xdomain = [domainx[0] - borderx,
            domainx[1] + borderx],
            ydomain = [domainy[0] - bordery,
            domainy[1] + bordery];

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

var histogram = {
    createHistogram: function (data) {
        var w = 300,
            h = 500;
        var histogram = d3.layout.histogram()
            (data.clusters.map(function (c) { return c.size; }));

        var x = d3.scale.ordinal()
            .domain(histogram.map(function(d) { return d.x; }))
            .rangeRoundBands([0, w]);

        var y = d3.scale.linear()
            .domain([0, d3.max(histogram, function(d) { return d.y; })])
            .range([0, h]);

        d3.select("#histogram > svg").remove();

        var vis = d3.select("#histogram").append("svg")
                .attr("id", "histogram")
                .attr("width", w)
                .attr("height", h),
            g = vis 
                .append("svg:g")
                .attr("transform", "translate(1,0.5)")
            ;

        g.selectAll("rect")
            .data(histogram)
            .enter().append("svg:rect")
            .attr("transform", function(d) {
                return "translate(" + x(d.x) + "," + (h - y(d.y)) + ")"; })
            .attr("width", x.rangeBand())
            .attr("y", function(d) { return y(d.y); })
            .attr("height", 0)
            .transition()
            .duration(750)
            .attr("y", 0)
            .attr("height", function(d) { return y(d.y); })
            ;
    }
}

function updateVisualization() {
    clustering.redoCanvas();

    var updater = (function(this_clustering, this_histogram, data) {
        clustering.createMap.bind(this_clustering)(data);
        //histogram.createHistogram.bind(this_histogram)(data);
    }).bind(this, clustering, histogram);
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
        clustering.updateMap.bind(this)()
    } else {
        stopTimer();
    }
}

if (clustering.updateEnabled) {
    setTimer(updateinterval);
}

updateVisualization();
