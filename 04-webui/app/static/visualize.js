
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
};

Plotly.newPlot('chart1', data, layout);
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
  title: 'Website requests over time',
};

Plotly.newPlot('chart2', data, layout);
});


// Chart with top 10 visitors by click
Plotly.d3.csv("static/visits_top10.csv", function(err, rows){
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
  title: 'Top 10 visitors (past minute)',
};

Plotly.newPlot('chart_top_ip_visits', data, layout);
});