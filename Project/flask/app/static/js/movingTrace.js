function rand() {
  return Math.random();
}

var graph = document.getElementById('graph');

var time = new Date();

var data = [{
  x: [time],
  y: [rand],
  mode: 'lines',
  line: {color: '#80CAF6'}
}]

Plotly.plot(graph, data);

var cnt = 0;

var interval = setInterval(function() {

  var time = new Date();

  var update = {
  x:  [[time]],
  y: [[rand()]]
  }

  var olderTime = time.setMinutes(time.getMinutes() - 1);
  var futureTime = time.setMinutes(time.getMinutes() + 1);

  var minuteView = {
        xaxis: {
          type: 'date',
          range: [olderTime,futureTime]
        }
      };

  Plotly.relayout(graph, minuteView);
  Plotly.extendTraces(graph, update, [0])

  if(cnt === 100) clearInterval(interval);
}, 1000);
