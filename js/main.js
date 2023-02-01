function addPagination() {
  var itemsPerPage = 10; // number of entries per page
  var table = $("#Available_Flights_Tb tbody");
  var numPages = Math.ceil(table.find("tr").length / itemsPerPage);

  table.find("tr").hide();
  table.find("tr").slice(0, itemsPerPage).show();

  $("#pagination #prev").click(function() {
    if ($(this).data("page") > 1) {
      table.find("tr").hide();
      table.find("tr").slice($(this).data("page") * itemsPerPage - itemsPerPage - itemsPerPage, $(this).data("page") * itemsPerPage - itemsPerPage).show();
      $(this).data("page", $(this).data("page") - 1);
      $("#pagination #next").data("page", $(this).data("page"));
    }
  });

  $("#pagination #next").click(function() {
    if ($(this).data("page") < numPages) {
      table.find("tr").hide();
      table.find("tr").slice($(this).data("page") * itemsPerPage, $(this).data("page") * itemsPerPage + itemsPerPage).show();
      $(this).data("page", $(this).data("page") + 1);
      $("#pagination #prev").data("page", $(this).data("page"));
    }
  });

  $("#pagination #prev").data("page", 1);
  $("#pagination #next").data("page", 2);
}


function updateTable(tableId, response) {
  let tableBody = "";
  data = JSON.parse(response);
    for(let i = 0; i < data.length; i++) {
      let flight = data[i];
      tableBody += "<tr>";
      tableBody += "<td>" + flight.departure + "</td>";
      tableBody += "<td>" + flight.arrival + "</td>";
      tableBody += "<td>" + flight["IATA Code"] + "</td>";
      tableBody += "<td>" + flight.Stops + "</td>";
      tableBody += "<td>" + flight.Price + "</td>";
      tableBody += "</tr>";
  }
  var new_table_html = document.getElementById(tableId).innerHTML + tableBody;
  document.getElementById(tableId).innerHTML = new_table_html;
  addPagination();
}

function parseResponse(response) {
    updateTable("Available_Flights_Tb", response);
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

