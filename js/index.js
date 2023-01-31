const express = require('express')
const {spawn} = require('child_process');
var app = express();
const cors = require('cors');
app.use(cors({
    origin: '*'
}));

const port = 3000

app.get('/searchFlights', (req, res) => {
    var from_val = req.query.from;
    var to_val = req.query.to;
    var date_val = req.query.date;
    console.log(from_val);
    console.log(to_val);
    console.log(date_val);
    var dataToSend;
    // spawn new child process to call the python script
    const python = spawn('python', ['../python/run.py', from_val, to_val, date_val]);
    // collect data from script
    python.stdout.on('data', function (data) {
      console.log('Pipe data from python script ...');
      dataToSend = data.toString();
      console.log("start data to send")
      console.log(dataToSend)
      console.log("end data to send")
    });
    // in close event we are sure that stream from child process is closed
    python.on('close', (code) => {
    console.log(`child process close all stdio with code ${code}`);
      var printdata;
      // launching secondary python script to print data
      const python2 = spawn('python', ['../python/printJson.py']);
      // collect data from script
      python2.stdout.on('data', function (data) {
        console.log('Print Pipe data from python script ...');
        printdata = data.toString();
        console.log("Print start data to send")
        console.log(printdata)
        console.log("Print end data to send")
      });
      python2.on('close', (code) => {
        console.log(`print child process close all stdio with code ${code}`);
        res.json(printdata);
      });
    });
  })

app.listen(port, () => console.log(`Example app listening on port 
${port}!`))