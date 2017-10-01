
function plot_visits_time(data){

  var event_times = [];
  var total = [];
  var unique = [];

   for (var i=0; i<data.length; i++) {
        event_times.push(data[i]['event_time']);
        if (data[i]['type'] == 'total_visits') {
          total.push(data[i]['value']);
        } else if (data[i]['type'] == 'unique_visits') {
          unique.push(data[i]['value']);
        }
   }

  var trace1 = {
    type: "scatter",
    mode: "markers",
    name: 'total',
    x: event_times,
    y: total,
    line: {color: '#17BECF'}
  };

  var trace2 = {
    type: "scatter",
    mode: "markers",
    name: 'unique',
    x: event_times,
    y: unique,
    line: {color: '#7F7F7F'}
  };

  var data = [trace1, trace2];

  var layout = {
    title: 'Website requests over time',
     yaxis: {title: '# visits'},
  };

  Plotly.newPlot('chart_clicks', data, layout);
}


function plot_volume_time(data){

  var event_times = [];
  var human = [];
  var crawler = [];

   for (var i=0; i<data.length; i++) {
        event_times.push(data[i]['event_time']);
        if (data[i]['type'] == 'volume_human') {
          human.push(data[i]['value']);
        } else if (data[i]['type'] == 'volume_crawler') {
          crawler.push(data[i]['value']);
        }
   }

  var trace1 = {
    type: "scatter",
    mode: "markers",
    name: 'human',
    x: event_times,
    y: human,
    line: {color: '#17BECF'}
  };

  var trace2 = {
    type: "scatter",
    mode: "markers",
    name: 'crawler',
    x: event_times,
    y: crawler,
    line: {color: '#7F7F7F'}
  };

  var data = [trace1, trace2];

  var layout = {
    title: 'Website traffic volume over time',
    yaxis: {title: 'volume (MB)'},
  };

  Plotly.newPlot('chart_volume', data, layout);
}


function plot_4xx_time(data){

  var event_times = [];
  var count = [];

   for (var i=0; i<data.length; i++) {
        event_times.push(data[i]['event_time']);
        count.push(data[i]['value']);
   }

  var trace1 = {
    type: "scatter",
    mode: "markers",
    name: 'HTTP 4xx',
    x: event_times,
    y: count,
    line: {color: '#17BECF'}
  };

  var data = [trace1];

  var layout = {
    title: 'HTTP status 4XX over time',
  };

  Plotly.newPlot('chart_4xx_time', data, layout);
}



function start_graphs(){
  load_dashboards();
  interval_updates();
}


function load_dashboards(){

  // visits plot
  $.getJSON($SCRIPT_ROOT + 'api/metric/total_visits,unique_visits/600', function(data) {
    plot_visits_time(data)
  });

  // volume plot
  $.getJSON($SCRIPT_ROOT + 'api/metric/volume_human,volume_crawler/600', function(data) {
    plot_volume_time(data)
  });

  // HTTP4xx plot
  $.getJSON($SCRIPT_ROOT + 'api/metric/4xx/600', function(data) {
    plot_4xx_time(data)
  });

}


function interval_updates(){

  // visits update
  var interval_vists = setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/metric/total_visits,unique_visits/600', function(data) {
      var event_times = [];
      var total = [];
      var unique = [];

     for (var i=0; i<data.length; i++) {
          event_times.push(data[i]['event_time']);
          if (data[i]['type'] == 'total_visits') {
            total.push(data[i]['value']);
          } else if (data[i]['type'] == 'unique_visits') {
            unique.push(data[i]['value']);
          }
     }

      var data_update = {
        x: [event_times, event_times],
        y: [total, unique]
      }

      Plotly.update('chart_clicks', data_update);
    });
  }, 5000);
  
  // volume updates
  var interval_volume = setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/metric/volume_human,volume_crawler/600', function(data) {
      var event_times = [];
      var human = [];
      var crawler = [];

     for (var i=0; i<data.length; i++) {
          event_times.push(data[i]['event_time']);
          if (data[i]['type'] == 'volume_human') {
            human.push(data[i]['value']);
          } else if (data[i]['type'] == 'volume_crawler') {
            crawler.push(data[i]['value']);
          }
     }

      var data_update = {
        x: [event_times, event_times],
        y: [human, crawler]
      }

      Plotly.update('chart_volume', data_update);
    });
  }, 5000);

  // 4xx updates
  var interval_4xx = setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/metric/4xx/600', function(data) {
      var event_times = [];
      var count = [];

      for (var i=0; i<data.length; i++) {
          event_times.push(data[i]['event_time']);
          count.push(data[i]['value']);
      }

      var data_update = {
        x: [event_times],
        y: [count]
      }

      Plotly.update('chart_4xx_time', data_update);
    });
  }, 5000);


}









/*

// dashboard: website requests (total and unqiue)
Plotly.d3.csv("static/visits.csv", function(err, rows){
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}

var trace1 = {
  type: "scatter",
  mode: "markers",
  name: 'total',
  x: unpack(rows, 'time'),
  y: unpack(rows, 'total'),
  line: {color: '#17BECF'}
}

var trace2 = {
  type: "scatter",
  mode: "markers",
  name: 'unique',
  x: unpack(rows, 'time'),
  y: unpack(rows, 'unique'),
  line: {color: '#7F7F7F'}
}

var data = [trace1,trace2];

var layout = {
  title: 'Website requests over time',
   yaxis: {title: '# visits'},
};

Plotly.newPlot('chart_clicks', data, layout);
})


// dashboard: volume requested (human and crawler)
Plotly.d3.csv("static/volume.csv", function(err, rows){
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}

var trace1 = {
  type: "scatter",
  mode: "markers",
  name: 'crawler',
  x: unpack(rows, 'time'),
  y: unpack(rows, 'crawler'),
  line: {color: '#17BECF'}
}

var trace2 = {
  type: "scatter",
  mode: "markers",
  name: 'human',
  x: unpack(rows, 'time'),
  y: unpack(rows, 'human'),
  line: {color: '#7F7F7F'}
}

var data = [trace1,trace2];

var layout = {
  title: 'Website traffic volume over time',
  yaxis: {title: 'volume (MB)'},
};

Plotly.newPlot('chart_volume', data, layout);
});


// Chart with top 10 visitors by clicks
Plotly.d3.csv("static/visits_top10_clicks.csv", function(err, rows){
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}

var data = [
  {
    x: unpack(rows, 'ip'),
    y: unpack(rows, 'visits'),
    type: 'bar'
  }
]

var layout = {
  title: 'Top 10 visitors by clicks (past minute)',
  yaxis: {title: '# visits'},
};

Plotly.newPlot('chart_top_ip_visits', data, layout);
});

// Chart with top 10 visitors by volume
Plotly.d3.csv("static/visits_top10_volume.csv", function(err, rows){
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}

var data = [
  {
    x: unpack(rows, 'ip'),
    y: unpack(rows, 'volume'),
    type: 'bar'
  }
]

var layout = {
  title: 'Top 10 visitors by volume (past minute)',
  yaxis: {title: 'volume (MB)'},
};

Plotly.newPlot('chart_top_ip_volume', data, layout);
});


// dashboard: 4xx count by time
Plotly.d3.csv("static/code_count.csv", function(err, rows){
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}

var trace1 = {
  type: "scatter",
  mode: "markers",
  name: 'HTTP 4xx',
  x: unpack(rows, 'time'),
  y: unpack(rows, 'count'),
  line: {color: '#17BECF'}
}


var data = [trace1];

var layout = {
  title: 'HTTP status 4XX over time',
};

Plotly.newPlot('chart_4xx_time', data, layout);
});


*/