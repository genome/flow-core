function make_charts(json_url) {
    d3.json(json_url, function(data) {
        // find all fds
        var charts = {};
        var cmdlines = {};
        for (var i = 0; i < data.length; ++i) {
            var time = data[i].time;
            var stat = data[i].status;
            for (var pid in stat) {
                if (!(pid in charts)) {
                    cmdlines[pid] = stat[pid].cmdline.join(" ");
                    charts[pid] = {
                        "fd": {},
                        "mem": {"title": "Memory (RSS)", "type": null, "data": []},
                        "cpu_percent": {"title": "CPU %", type: null, "data": []}};
                }
                for (var fd in stat[pid]['fd_info']) {
                    if (!(fd in charts[pid])) {
                        var fd_info = stat[pid]['fd_info'];
                        var file = fd_info[fd]['real_path'];
                        var type = fd_info[fd]['type'];
                        var title = "fd #" + fd + ": " + file;
                        charts[pid]["fd"][fd] = {"title": title, "type": type, "data": []};
                    }
                }
            }
        }

        for (var i = 0; i < data.length; ++i) {
            var time = data[i].time;
            var stat = data[i].status;
            for (pid in charts) {
                if (!(pid in stat)) {
                    continue;
                }
                var rss = stat[pid].memory_info[0];
                var cpu = stat[pid].cpu_percent;
                charts[pid].mem.data.push({"x": time, "y": rss});
                charts[pid].cpu_percent.data.push({"x": time, "y": cpu});

                for (fd in charts[pid].fd) {
                    var fdpos = 0;
                    if (pid in stat && fd in stat[pid]['fd_info']) {
                        var fd_info = stat[pid]['fd_info'];
                        fdpos = fd_info[fd]['pos']
                    }
                    charts[pid].fd[fd].data.push({"x": time, "y": fdpos});
                }
            }
        }

        var container = document.getElementById("chart_container");
        if (container) {
            container.parentNode.removeChild(container);
        }

        container = d3.select("body").append("div")
                .attr("id", "chart_container");

        for (var pid in charts) {
            process_status(pid, cmdlines[pid], charts[pid], container);
        }

    });
}

function process_status(pid, cmdline, charts, parent_elt) {
    var chr_collapse = "▼";
    var chr_expand = "▶";
    var outer = parent_elt.append("div");
    var title_div = outer.append("div")
            .attr("class", "boxhead")
        ;

    var toggler = title_div.append("span")
            .style("padding-right", "5px")
            .text(chr_collapse)
            ;

    title_div.append("span").text(cmdline + " pid (" + pid + ")")

    var body = outer.append("div")
            .attr("class", "boxbody")
            .property("hidden", false)
        ;

    title_div.on("click", function () {
            if (body.property("hidden")) {
                body.property("hidden", false);
                body.style("visibility", "visible");
                toggler.text(chr_collapse);
            } else {
                body.property("hidden", true);
                body.style("visibility", "hidden");
                toggler.text(chr_expand);
            }
        });

    var mchart = new timechart(charts.mem.data, body,
        charts.mem.title, charts.mem.type);
    mchart.draw();

    var cchart = new timechart(charts.cpu_percent.data, body,
        charts.cpu_percent.title,
        charts.cpu_percent.type);
    cchart.draw();


    for (var fd in charts.fd) {
        c = charts.fd[fd];
        var chart = new timechart(c.data, body,
            c.title, c.type);
        chart.draw();
    }
}

function timechart(data, parent_elt, title, type) {
    this._use_area = true;
    this._use_lines = true;
    this._margin = [30,100,30,0];
    this._width = 800;
    this._height = 100;
    this._tick_padding_x = 2;
    this._tick_padding_y = 3;
    this._rate_data = [];
    this._title_style = {
        "font-size": "16px",
        "font-weight": "bold",
    };

    for (var i = 0; i < data.length; ++i) {
        var elt = Object();
        data[i].x = new Date(data[i].x*1000);
        elt.x = data[i].x;
        if (i == 0) {
            elt.y = 0
        } else {
            var tstep = data[i].x - data[i-1].x;
            elt.y = (data[i].y - data[i-1].y) / tstep;
        }
        this._rate_data.push(elt);
    }

    this._raw_data = data;
    this._data = this._raw_data;

    this.use_area = function(val) { this._use_area = val; }
    this.use_lines = function(val) { this._use_lines = val; }

    var x = d3.time.scale().range([0, this._width]);
    var y = d3.scale.linear().range([this._height, 0]);
    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom")
        .tickSize(-this._height, 0)
        .tickPadding(this._tick_padding_y)
        .tickFormat(d3.time.format("%X"))
        ;

    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("right")
        .tickSize(-this._width)
        .tickPadding(this._tick_padding_y)
        .ticks(4)
        ;

    var area = d3.svg.area()
        .interpolate("step-after")
        .x(function(d) { return x(d.x); })
        .y0(y(0))
        .y1(function(d) { return y(d.y); });

    var line = d3.svg.line()
        .interpolate("step-after")
        .x(function(d) { return x(d.x); })
        .y(function(d) { return y(d.y); });

    var div = parent_elt.append("div")
            .style("text-align", "center")
        ;

    div.append("h3")
            //.style(this._title_style)
            .text(title);

    if (type != null) {
        div.append("h4")
                //.style(this._title_style)
                .text(type);
    }

    var svg = div.append("svg:svg")
            .attr("width", this._width + this._margin[1] + this._margin[3])
            .attr("height", this._height + this._margin[0] + this._margin[2])
            .style("vertical-align", "text-top")
        .append("svg:g")
            .attr("transform", "translate(" + this._margin[3] + "," + this._margin[0] + ")")
        ;

    var gradient = svg.append("svg:defs").append("svg:linearGradient")
        .attr("id", "gradient")
        .attr("x2", "0%")
        .attr("y2", "100%")
        ;

    gradient.append("svg:stop")
        .attr("offset", "0%")
        .attr("stop-color", "#cdd")
        .attr("stop-opacity", .5);

    gradient.append("svg:stop")
        .attr("offset", "100%")
        .attr("stop-color", "#666")
        .attr("stop-opacity", 1);

    svg.append("svg:clipPath")
        .attr("id", "clip")
      .append("svg:rect")
        .attr("x", x(0))
        .attr("y", y(1))
        .attr("width", x(1) - x(0))
        .attr("height", y(0) - y(1))
    ;

    svg.append("svg:g")
        .attr("class", "y axis")
        .attr("transform", "translate(" + this._width + ",0)");

    svg.append("svg:path")
        .attr("class", "area")
        .attr("clip-path", "url(#clip)")
        .style("fill", "url(#gradient)");

    svg.append("svg:g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + this._height + ")");

    svg.append("svg:path")
        .attr("class", "line")
        .attr("clip-path", "url(#clip)");

    svg.append("svg:rect")
        .attr("class", "pane")
        .attr("width", this._width)
        .attr("height", this._height)
        .call(d3.behavior.zoom().on("zoom", this.zoom));


    x.domain([
        d3.min(this._data, function(d) { return d.x; }),
        d3.max(this._data, function(d) { return d.x; }),
        ]);

    y.domain([
        d3.min(this._data, function(d) { return d.y; }),
        d3.max(this._data, function(d) { return d.y; }),
        ]);

    svg.select("path.area").data([this._data]);
    svg.select("path.line").data([this._data]);

    this.derivative = function(val) {
        if (val == true) {
            this._data = this._rate_data;
            this._dbutton
                .text("x")
                .on("click", function () { me.derivative(false); })
                ;

        } else {
            this._data = this._raw_data;
            this._dbutton
                .text("d/dx")
                .on("click", function () { me.derivative(true); })
        }
        svg.select("path.area").data([this._data]);
        svg.select("path.line").data([this._data]);
        x.domain([
            d3.min(this._data, function(d) { return d.x; }),
            d3.max(this._data, function(d) { return d.x; }),
            ]);

        y.domain([
            d3.min(this._data, function(d) { return d.y; }),
            d3.max(this._data, function(d) { return d.y; }),
            ]);


        this.draw();
        return this;
    }


    this.draw = function() {
        svg.select("g.x.axis").call(xAxis);
        svg.select("g.y.axis").call(yAxis);
        svg.select("path.area").attr("d", area);
        svg.select("path.line").attr("d", line);
    }

    this.zoom = function() {
      d3.event.transform(x); // TODO d3.behavior.zoom should support extents
      this.draw();
    }

    var me = this;
    var controls = div.append("span")
        .style("vertical-align", "center")
        ;

    this._dbutton = controls.append("button")
        .style("width", "80px")
        .text("d/dx")
        .on("click", function () { me.derivative(true); })
        ;

    return this;
}
