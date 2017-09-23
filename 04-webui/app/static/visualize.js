

Plotly.d3.csv("static/total_visits.csv", function(err, rows){
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}



var trace1 = {
  type: "scatter",
  mode: "lines",
  name: 'Requests over time',
  x: unpack(rows, 'time'),
  y: unpack(rows, 'count'),
  line: {color: '#17BECF'}
}



var data = [trace1];

var layout = {
  title: 'Website requests over time',
};

Plotly.newPlot('chart1', data, layout);
});

Plotly.d3.csv("static/total_volume.csv", function(err, rows){
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}


var trace1 = {
  type: "scatter",
  mode: "lines",
  name: 'Requests over time',
  x: unpack(rows, 'time'),
  y: unpack(rows, 'volume(b)'),
  line: {color: '#17BECF'}
}



var data = [trace1];

var layout = {
  title: 'Website requests over time',
};

Plotly.newPlot('chart2', data, layout);
});