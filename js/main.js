function parseResponse(response) {
    var main_area = document.getElementById("main_area");
    var para = document.createElement('p');
    para.id = 'get_dump';
    response_string = JSON.stringify(response);
    para.innerHTML = "<p>" + response_string + "</p>";
    main_area.appendChild(para);
}

function updateTable(json) {
    result = jQuery.parseJSON(json);
    var table = document.getElementById("Available_Flights_Tb");
    
    /*var rowCount = table.rows.length;
    for (var i = rowCount - 1; i > 0; i--) {
        table.deleteRow(i);
    }*/				
    
    for(var k in result) {
        var student = result[k];
        name = student.name;
        age = student.age;
        loc = student.location;
        var row = table.insertRow(-1);
        var cell1 = row.insertCell(0);
        var cell2 = row.insertCell(1);
        var cell3 = row.insertCell(2);
        cell1.innerHTML = name;
        cell2.innerHTML = age;
        cell3.innerHTML = loc;
    } 
}

function addflights() {				
    var settings = {
        "url": "https://test.api.amadeus.com/v2/shopping/flight-offers?" +
        "originLocationCode=" + document.getElementById("from_input").value +
        "&destinationLocationCode=" + document.getElementById("to_input").value +
        "&departureDate=" + document.getElementById("date_input").value +
        "&adults=1",
        "method": "GET",
        "timeout": 0,
        "headers": {
          "Authorization": "Bearer RPdPUzPVf23y3YbqnBqzxndygOxx"
        },
      };
    $.ajax(settings).done(function (response) {
        parseResponse(response);
    });
}

function callPythonFindFlights(){
  var from_val = document.getElementById("from_input").value;
  var to_val = document.getElementById("to_input").value;
  var date_val = document.getElementById("date_input").value;

  url = "http://localhost:3000/searchFlights?" +
  "from=" + from_val +
  "&to=" + to_val +
  "&date=" + date_val;

  fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded"
    },
  }).then(function(response) {
    return response.json();
  }).then(function(data) {
    console.log(data)
    parseResponse(data)
  });
}


function sortTable(n) {
    var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
    table = document.getElementById("Available_Flights_Tb");
    switching = true;
    // Set the sorting direction to ascending:
    dir = "asc";
    /* Make a loop that will continue until
    no switching has been done: */
    while (switching) {
      // Start by saying: no switching is done:
      switching = false;
      rows = table.rows;
      /* Loop through all table rows (except the
      first, which contains table headers): */
      for (i = 1; i < (rows.length - 1); i++) {
        // Start by saying there should be no switching:
        shouldSwitch = false;
        /* Get the two elements you want to compare,
        one from current row and one from the next: */
        x = rows[i].getElementsByTagName("TD")[n];
        y = rows[i + 1].getElementsByTagName("TD")[n];
        /* Check if the two rows should switch place,
        based on the direction, asc or desc: */
        if (dir == "asc") {
          if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
            // If so, mark as a switch and break the loop:
            shouldSwitch = true;
            break;
          }
        } else if (dir == "desc") {
          if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
            // If so, mark as a switch and break the loop:
            shouldSwitch = true;
            break;
          }
        }
      }
      if (shouldSwitch) {
        /* If a switch has been marked, make the switch
        and mark that a switch has been done: */
        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
        switching = true;
        // Each time a switch is done, increase this count by 1:
        switchcount ++;
      } else {
        /* If no switching has been done AND the direction is "asc",
        set the direction to "desc" and run the while loop again. */
        if (switchcount == 0 && dir == "asc") {
          dir = "desc";
          switching = true;
        }
      }
    }
  }

function setInitialState(){
    var table = document.getElementById("Available_Flights_Tb");
    for (var i = 0; i < table.rows[0].cells.length; i++) {
        var col = table.rows[0].cells[i];
        var checkbox = document.getElementById("checkbox" + i);
        if (col.style.display !== "none") {
            checkbox.checked = true;
        } else {
            checkbox.checked = false;
        }
    }
}

function populateDropdown() {
    // Get the table
    var table = document.getElementById("Available_Flights_Tb");
    // Get the number of columns
    var numCols = table.rows[0].cells.length;
    // Create a dropdown option for each column
    for (var i = 0; i < numCols; i++) {
        var colName = table.rows[0].cells[i].innerHTML;
        var dropdown_menu = document.getElementById("dropdown-content");
        if(!document.getElementById('list_'+ colName)){
            var list = document.createElement('list');
            list.id = 'list_' + colName;
            list.innerHTML = "<a href='#' class='showhideCols' data-value='option" + i + "' tabIndex='-1'><input type='checkbox' id = 'checkbox" + i + "' onchange = 'toggleColumn(" + i + ")'/>"+colName+"</a>";
            dropdown_menu.appendChild(list);
        }
    }
    setInitialState();
}

function toggleColumn(colIndex) {
    var table = document.getElementById("Available_Flights_Tb");
    var checkbox = document.getElementById("checkbox" + colIndex);
    for (var i = 0; i < table.rows.length; i++) {
        var col = table.rows[i].cells[colIndex];
        if (checkbox.checked) {
            col.style.display = "";
        } else {
            col.style.display = "none";
        }
    }
}

