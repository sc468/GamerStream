// receive the data from rethinkDB -> flask -> here
var socket = io('http://' + document.domain + ':' + location.port);

socket.on('connect', function() {
    console.log("connected");
});

socket.on('components', function(msg) {
    var newData = msg.data.new_val;
    updateTable(newData);
});

socket.on('error', function(err) {
    console.log("error returned: ", err);
});

function updateTable(record) {

    if (parseInt(record['userid']) < 5) {
      
        var label = 'userid_'+record['userid'];
  
        // update status of each user in the table 
        if (record['status'] == 'safe') {
            document.getElementById(label).innerHTML = record['status'];
            document.getElementById(label).style.backgroundColor = '#82E0AA';
        } else {
            var time_pattern = new RegExp("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", "m");
            var reMatch = time_pattern.exec(record['time']);
            document.getElementById(label).innerHTML = record['status'] + ': ' +reMatch;
            document.getElementById(label).style.backgroundColor = '#E74C3C';
        }
    }
};

