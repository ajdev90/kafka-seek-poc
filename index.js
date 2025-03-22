const express = require('express');
const app = express();
const port = 3000;
const bodyParser = require('body-parser');
const kafkaService = require('./kafkaConsumerService');
app.use(bodyParser.json());

app.post('/retry', (req, res) => {
  let offset = req.body.offset;
  let partition = req.body.partition;
  console.log(`offset=${offset} and partition=${partition}`);
  kafkaService.run(offset, partition);
  res.send(`Request accepted`);
})

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})
