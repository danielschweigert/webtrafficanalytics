
// dashboard: website requests (total and unqiue)
Plotly.d3.csv("static/visits.csv", function(err, rows){
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}

var trace1 = {
  type: "scatter",
  mode: "lines",
  name: 'total',
  x: unpack(rows, 'time'),
  y: unpack(rows, 'total'),
  line: {color: '#17BECF'}
}

var trace2 = {
  type: "scatter",
  mode: "lines",
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
});