const config = require('./config');
const async = require('async');
const express = require('express');
const bodyParser = require('body-parser');
const AWS = require('aws-sdk');
AWS.config = new AWS.Config();
AWS.config.region = 'us-west-2';
AWS.config.accessKeyId = 'testing';
AWS.config.secretAccessKey = 'testing';

const DynamoDbLocal = require('dynamodb-local');
const dynamoLocalPort = 8000;

const app = express();

const db = new AWS.DynamoDB({ endpoint: new AWS.Endpoint('http://localhost:8000') });


const valueToString = (value) => {
  if(typeof value === 'string') {
    return {S:value}
  }
  return {S:JSON.stringify(value)};
}

const attributeMap = {
  'userName': valueToString,
  'displayName': valueToString,
  'name': valueToString,
  'emails': valueToString,
  'title': valueToString,
  'orgId': valueToString,
  'department': valueToString,
  'preferredLanguage': valueToString,
  'locale': valueToString,
  'timezone': valueToString,
  'addresses': valueToString,
  'phoneNumbers': valueToString,
  'ims': valueToString,
  'photos': valueToString,
  'licenseID': valueToString,
  'userSettings': valueToString,
  'sipAddresses': valueToString,
  'accountStatus': valueToString,
  'entitlements': valueToString,
  'userPreferences': valueToString,
  'managedOrgs': valueToString,
};
const attributes = Object.keys(attributeMap);

const translateUserToItem = (user) => {
  const item = {id: {S:user.id}};
  for(let attr of attributes) {
    if(user[attr] !== undefined) {
      item[attr] = attributeMap[attr](user[attr]);
    }
  }
  return item;
}

const createUser = (user) => {
  const params = { 
    Item: translateUserToItem(user),
    TableName: 'users'
  };
  db.putItem(params, (err, res) => {
    console.log(err);
    console.log(res);
  });
}

const initModifyParams = (user) => {
  const names = {};
  const values = {};
  let exp = '';
  if(user.meta && user.meta.attributes) {
    /*  deletion of some attributes  */
    exp += 'REMOVE ';
    for(let attr of user.meta.attributes) {
      names[`#${attr}`] = attr;
      exp += `#${attr},`
    }
    exp = exp.slice(0,-1);
  }
  if(exp.length) {
    exp += ' ';
  }
  exp += 'SET ';
  for(let attr of attributes) {
    if(user[attr] !== undefined) {
      exp += `#${attr}=:${attr},`
      values[`:${attr}`] = attributeMap[attr](user[attr]);
      names[`#${attr}`] = attr;
    }
  }
  exp = exp.slice(0,-1);
  return {
    ExpressionAttributeValues: values,
    ExpressionAttributeNames: names,
    UpdateExpression: exp };
}

const modifyUser = (user) => {
  const params = Object.assign(initModifyParams(user),
			       {Key: {'id': {S:user.id}},
				TableName: 'users',
				ReturnValues: 'ALL_NEW'
			       });
  db.updateItem(params, (err, res) => {
    console.log(err);
    console.log(res);
  });
}

const deleteUser = (user) => {
  const params = {
    Key: {'id':{S:user.id}},
    TableName: 'users'
  };
  db.deleteItem(params, (err, res) => {
    console.log(err);
    console.log(res);
  });
}


async.series([
  (done) => {
    /* launch local db instance  */
    DynamoDbLocal.launch(dynamoLocalPort, null, ['-inMemory'])
      .then(function () {
	return done();
      });
  },
  (done) => {
    /* create user table  */
    const tableParams = {
      TableName: 'users',
      AttributeDefinitions: [{ AttributeName: 'id',
			       AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'id',
		    KeyType: 'HASH' }],
      ProvisionedThroughput: { ReadCapacityUnits: 10,
			       WriteCapacityUnits: 10 }
      
    };
    db.createTable(tableParams, (err, data) => {
      console.log(data);
      return done(err);
    });
  },
], (err) => {
  if(err) {
    console.log(err);
    process.exit(1);
  }
  /*  webhook handler for user events  */
  app.put('/message-eq-user-event', [bodyParser.json(), (req, res) => {
    res.end();
    if(!req.body || !req.body.changeType) { 
      console.log(`Event body not received or invalid`);
    }
    switch(req.body.changeType) {
    case 'create':
      if(req.body.meta && req.body.meta.movedFrom) {
	/*  this is a move action to another org.  */
	modifyUser(req.body);
      }
      else {
	createUser(req.body);
      }
      break;
    case 'modify':
      modifyUser(req.body);
      break;
    case 'delete':
      if(req.body.meta && req.body.meta.movedTo) {
	/*  this is a move action to another org.  */
	return;
      }
      deleteUser(req.body);
      break;
    default:
      console.log(`Invalid changeType ${req.body.changeType}`);
    }
  }]);
  /*  error handler  */
  app.use((err, req, res, next) => {
    console.log(`Error handling webhook event ${err}`)
    res.end();
  });
  var server = app.listen(config.port, () => {
    const gracefulShutdown = () => {
      server.close(() => {
	DynamoDbLocal.stop(dynamoLocalPort);
      });
    }
    process.on('SIGTERM', gracefulShutdown);
    process.on('SIGINT', gracefulShutdown);
    console.log(`Listening on ${config.port}`);
  });
});


