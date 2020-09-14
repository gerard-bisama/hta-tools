'use strict'

const express = require('express');
const URI = require('urijs');
const isJSON = require('is-json');
const path = require('path');
var btoa = require('btoa');
const _ = require('underscore');
const {createLogger,format,transports} = require('winston');
const { combine, timestamp, label, printf } = format;
const customLibrairy=require('./lib.js');
const myFormat = printf(({ level, message, label, timestamp,operationType,action,result }) => {
    return `${timestamp},${level},${label},${operationType},${action},${result},${message}`;
  });
// Config

const importConfig = require('../config/import_config')
var port = importConfig.app.port;
const dhis2Token = `Basic ${btoa(importConfig.dhis2Server.username+':'+importConfig.dhis2Server.password)}`;
var logger=null;
var indexName;
var appOperationType="import";
var logFileName;
var filePath;
var typeOperation ={
    startTheService:"Start",
    stopTheService:"Stop",
    getData:"Get",
    postData:"Post",
    putData:"Put",
    deleteData:"Delete",
    normalProcess:"Process"
};
var typeResult={
    success:"Success",
    failed:"Failed",
    iniate:"Initiate",
    ongoing:"ongoing"
};
var levelType={
    info:"info",
    error:"error",
    warning:"warn"
};
var dhisResource={
  tei:"trackedEntityInstances",
  enrollment:"enrollments",
  event:"events"
}
//----------------------------Define logger information -------------------------------------/


/**
 * setupApp - configures the http server for this mediator
 *
 * @return {express.App}  the configured http server
 */
function errorHandler(err, req, res, next) {
    if (res.headersSent) {
      return next(err);
    }
    res.status(500);
    res.render('error', { error: err });
}

/************************* Main app entry point****************************************** */
function setupApp () {
    const app = express()
    app.use(errorHandler);
    app.get("/test",(req, res)=>{
        //res.send("Is is working!");
        var stooges = [{name: 'moe', age: 40}, {name: 'larry', age: 50}, {name: 'curly', age: 60}];
        let sorted=_.sortBy(stooges, 'age');
        res.send(sorted);
    });//end get(/error)
    app.get("/importprofile",(req, res)=>{
      logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"/importprofile",result:typeResult.iniate,
      message:`Start the import profile and PEC process`});
      var filePath=importConfig.app.dataFilePath;
      customLibrairy.readCSVProfilePECFile(filePath,function(patientData){
        //console.log(patientData);
        logger.log({level:levelType.info,operationType:typeOperation.getData,action:"readCSVProfilePECFile",
        result:typeResult.success,message:`Return ${patientData.length} records from datafile`});
        //split  the array into chunk array from a certain dimension
        let listCreatedTEI=customLibrairy.buildTEI(patientData,importConfig.app.trackedEntityTypeId,
          importConfig.programRulesAgeGroup,importConfig.app.programId);
        let listCreatedEvent=customLibrairy.buildProvenanceEvents(patientData,importConfig.app.programId,
          importConfig.app.programStages.provenancePatient,importConfig.patientRefereOptionSet);

        //Then sort by createdDate
        let listCreatedTEISortByDate=_.sortBy(listCreatedTEI,'created');
        let listCreatedEventSortByDate=_.sortBy(listCreatedEvent,'eventDate');
        
        //return res.send(listCreatedTEISortByDate);
        let chunckedTEI=[];
        let chunckedEvents=[];
        if(importConfig.app.teiInsertBatchSize<listCreatedTEISortByDate.length)
        {
          //then chunck the array
          let tempArray=chunckTEI(listCreatedTEISortByDate,importConfig.app.teiInsertBatchSize);
          chunckedTEI=chunckedTEI.concat(tempArray);
          tempArray=[];
          tempArray=chunckEvents(listCreatedEventSortByDate,importConfig.app.teiInsertBatchSize);
          chunckedEvents=chunckedEvents.concat(tempArray);
        }
        else
        {
          chunckedTEI=chunckedTEI.concat(listCreatedTEISortByDate);
          chunckedEvents=chunckEvents.concat(listCreatedEventSortByDate);
        }

        //return res.send(listCreatedTEISortByDate);

        logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"readCSVProfilePECFile",
        result:typeResult.ongoing,message:`Break successfully TEI instance list  list to chuncks`});
        //Now insert the TEI 
        saveDataList2Dhis(dhis2Token,dhisResource.tei,chunckedTEI,(resOperation)=>{
          logger.log({level:levelType.info,operationType:typeOperation.postData,action:"/saveDataList2Dhis",
        result:typeResult.success,message:`Insert successfully ${resOperation.length} TEI`});
          //console.log(resOperation);
          saveDataList2Dhis(dhis2Token,dhisResource.event,chunckedEvents,(resOperationEvent)=>{
            logger.log({level:levelType.info,operationType:typeOperation.postData,action:"/saveDataList2Dhis",
            result:typeResult.success,message:`Insert successfully ${resOperationEvent.length} Events`});
            console.log(resOperationEvent);
            res.send("Import process done!");
          });

          
        })


        //res.send("Import process done!");
        //res.send(chunckedEnrollments);
      });
    });//end get(/error)
    
    return app
}
function chunckTEI(array,size){
  const chunked_arr = [];
  let copied = [...array]; // ES6 destructuring
  const numOfChild = Math.ceil(copied.length / size); // Round up to the nearest integer
  for (let i = 0; i < numOfChild; i++) {
    var newTEIcollection={
      trackedEntityInstances:copied.splice(0, size)
    };
    //chunked_arr.push(copied.splice(0, size));
    chunked_arr.push(newTEIcollection);
  }
  return chunked_arr;

}
function chunckEvents(array,size){
  const chunked_arr = [];
  let copied = [...array]; // ES6 destructuring
  const numOfChild = Math.ceil(copied.length / size); // Round up to the nearest integer
  for (let i = 0; i < numOfChild; i++) {
    var newEventcollection={
      events:copied.splice(0, size)
    };
    //chunked_arr.push(copied.splice(0, size));
    chunked_arr.push(newEventcollection);
  }
  return chunked_arr;

}
function saveDataList2Dhis(dhis2Token,dhisResource,listData,callback){
  let localNeedle = require('needle');
  let localAsync = require('async');
  let dicOperationResults=[];
  localNeedle.defaults(
      {
          open_timeout: 600000
      });
  var url= URI(importConfig.dhis2Server.url).segment(dhisResource);
  url = url.toString();
  let options={headers:{'Content-Type': 'application/json','Authorization':dhis2Token}};
  let listAlreadyExistedResources=[];
  localAsync.eachSeries(listData, function(metadata, nextResource) {
    let compter=1;
    localNeedle.post(url,JSON.stringify(metadata),options,function(err,resp){
      if(err)
      {
          logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
                      message:`${err.Error}`});
          nextResource(err);

      }
      dicOperationResults.push({
        httpStatus:resp.body.httpStatus,
        //metadata:`index-${compter}`
        metadata:JSON.stringify( metadata)
      });
      compter++;
      if (resp.statusCode && (resp.statusCode < 200 || resp.statusCode > 399)) {
        if(resp.statusCode==409)
        {
          //console.log(metadata);
          logger.log({level:levelType.warning,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
            message:`Code: ${resp.statusCode}. Impossible de creer une ressource  qui existe deja`});
          
        }
        else{
          logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
            message:`Code d'erreur http: ${resp.statusCode}`});
        }
      }
      nextResource();
      
    });//end localNeedle
  },(err)=>{
    if(err)
    {
      logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${dhisResource}`,result:typeResult.failed,
      message:`${err.Error}`});
    }
    callback(dicOperationResults);
    
  });//end localAsync
  

}


/*****************************************Start the app********************* */
function start (callback) {
    filePath=importConfig.app.appDirectory;
    
    if(appOperationType=="import")
    {
        indexName="import";
    }
    indexName+=`_${new Date().toISOString().split("T")[0]}.log`;
    logFileName=path.join(filePath,`/logs/${indexName}.log`);
    logger = createLogger({
        format: combine(
          label({ label: "htaUtilities" }),
          timestamp(),
          myFormat
        ),
        transports: [new transports.Console(),
            new transports.File({ filename: logFileName })
        ]
      });
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'
    let app = setupApp()
    logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"App start up",
        result:typeResult.success,message:`App start up successfuly`});
    const server = app.listen(port, () => callback(server))
}
exports.start = start
if (!module.parent) {
    // if this script is run directly, start the server
    start(() => console.log(`Listening on ${port}...`))
  }