
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
     font: {size: 18}
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
    font: {size: 18}
  };

  Plotly.newPlot('chart_volume', data, layout);
}


function plot_HTTP_code_time(data, name, title, div_id){

  var event_times = [];
  var count = [];

   for (var i=0; i<data.length; i++) {
        event_times.push(data[i]['event_time']);
        count.push(data[i]['value']);
   }

  var trace1 = {
    type: "scatter",
    mode: "markers",
    name: name,
    x: event_times,
    y: count,
    line: {color: '#17BECF'}
  };

  var data = [trace1];

  var layout = {
    title: title,
    yaxis: {title: 'count'},
    font: {size: 18}
  };

  Plotly.newPlot(div_id, data, layout);

}

function plot_top10_clicks(data){

  var ip = [];
  var count = [];

   for (var i=0; i<data.length; i++) {
        ip.push(data[i]['ip']);
        count.push(data[i]['value']);
   }

  var data = [
    {
      x: ip,
      y: count,
      type: 'bar'
    }
  ]

  var layout = {
    title: 'Top 10 visitors by clicks (past minute)',
    yaxis: {title: '# visits'},
    font: {size: 18}
  };

  Plotly.newPlot('chart_top_ip_visits', data, layout);
}

function plot_top10_volume(data){

  var ip = [];
  var count = [];

   for (var i=0; i<data.length; i++) {
        ip.push(data[i]['ip']);
        count.push(data[i]['value']);
   }

  var data = [
    {
      x: ip,
      y: count,
      type: 'bar'
    }
  ]

  var layout = {
    title: 'Top 10 visitors by volume (past minute)',
    yaxis: {title: 'volume (MB)'},
    font: {size: 18}
  };

  Plotly.newPlot('chart_top_ip_volume', data, layout);
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

  // HTTP1xx plot
  $.getJSON($SCRIPT_ROOT + 'api/metric/1xx/600', function(data) {
    plot_HTTP_code_time(data, 'HTTP 1xx', 'HTTP status 1XX over time', 'chart_1xx_time');
  });

  // HTTP2xx plot
  $.getJSON($SCRIPT_ROOT + 'api/metric/2xx/600', function(data) {
    plot_HTTP_code_time(data, 'HTTP 2xx', 'HTTP status 2XX over time', 'chart_2xx_time');
  });

  // HTTP3xx plot
  $.getJSON($SCRIPT_ROOT + 'api/metric/3xx/600', function(data) {
    plot_HTTP_code_time(data, 'HTTP 3xx', 'HTTP status 3XX over time', 'chart_3xx_time');
  });

  // HTTP4xx plot
  $.getJSON($SCRIPT_ROOT + 'api/metric/4xx/600', function(data) {
    plot_HTTP_code_time(data, 'HTTP 4xx', 'HTTP status 4XX over time', 'chart_4xx_time');
  });

  // HTTP5xx plot
  $.getJSON($SCRIPT_ROOT + 'api/metric/5xx/600', function(data) {
    plot_HTTP_code_time(data, 'HTTP 5xx', 'HTTP status 5XX over time', 'chart_5xx_time');
  });


  // top 10 clicks
  $.getJSON($SCRIPT_ROOT + 'api/top10/clicks', function(data) {
    plot_top10_clicks(data)
  });

  // top 10 volume
  $.getJSON($SCRIPT_ROOT + 'api/top10/volume', function(data) {
    plot_top10_volume(data)
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

  //1xx updates
  var interval_4xx = setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/metric/1xx/600', function(data) {
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

      Plotly.update('chart_1xx_time', data_update);
    });
  }, 5000);

  //2xx updates
  var interval_4xx = setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/metric/2xx/600', function(data) {
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

      Plotly.update('chart_2xx_time', data_update);
    });
  }, 5000);

  //3xx updates
  var interval_4xx = setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/metric/3xx/600', function(data) {
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

      Plotly.update('chart_3xx_time', data_update);
    });
  }, 5000);

  //4xx updates
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

  //5xx updates
  var interval_5xx = setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/metric/5xx/600', function(data) {
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

      Plotly.update('chart_5xx_time', data_update);
    });
  }, 5000);

  // top 10 clicks update
  var interval_top10_clicks= setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/top10/clicks', function(data) {
      var ip = [];
      var count = [];

      for (var i=0; i<data.length; i++) {
          ip.push(data[i]['ip']);
          count.push(data[i]['value']);
      }

      var data_update = {
        x: [ip],
        y: [count]
      }

      Plotly.update('chart_top_ip_visits', data_update);
    });
  }, 60000); 

  // top 10 volume update
  var interval_top10_clicks= setInterval(function() {

    $.getJSON($SCRIPT_ROOT + 'api/top10/volume', function(data) {
      var ip = [];
      var count = [];

      for (var i=0; i<data.length; i++) {
          ip.push(data[i]['ip']);
          count.push(data[i]['value']);
      }

      var data_update = {
        x: [ip],
        y: [count]
      }

      Plotly.update('chart_top_ip_volume', data_update);
    });
  }, 60000);  

}
