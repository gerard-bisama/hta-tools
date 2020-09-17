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
        let listCreatedEvent=[];
        if(importConfig.app.syncFirstPeriod)
        {
          listCreatedEvent=customLibrairy.buildProvenanceEvents(patientData,importConfig.app.programId,
            importConfig.app.programStages.provenancePatient,importConfig.patientRefereOptionSet);
        }
        
        //return res.send(listCreatedEvent);
        //listCreatedEvent=[];
        let listSuiviEvent=customLibrairy.buildSuiviEvents(patientData,importConfig.app.programId,
          importConfig.app.programStages.suiviPatient,importConfig.statutPatientOptionSet);
        listCreatedEvent=listCreatedEvent.concat(listSuiviEvent);
        //return res.send(listSuiviEvent);

        //Then sort by createdDate
        let listCreatedTEISortByDate=_.sortBy(listCreatedTEI,'created');
        //listCreatedTEISortByDate=[];
        let listCreatedEventSortByDate=_.sortBy(listCreatedEvent,'eventDate');
        //let listCreatedEventSortByDate=_.sortBy(listSuiviEvent,'eventDate');

        
        //return res.send(` Events :${listCreatedEventSortByDate.length}`);
        let chunckedTEI=[];
        let chunckedEvents=[];
        let tempArray=[];
        if(importConfig.app.teiInsertBatchSize<listCreatedTEISortByDate.length)
        {//then chunck the array
          tempArray=chunckTEI(listCreatedTEISortByDate,importConfig.app.teiInsertBatchSize);
          chunckedTEI=chunckedTEI.concat(tempArray);
        }
        else
        {
          var newTEIcollection={
            trackedEntityInstances:listCreatedTEISortByDate
          };
          chunckedTEI.push(newTEIcollection);
        }
        tempArray=[];
        if(importConfig.app.teiInsertBatchSize<listCreatedEventSortByDate.length)
        {
          tempArray=chunckEvents(listCreatedEventSortByDate,importConfig.app.teiInsertBatchSize);
          chunckedEvents=chunckedEvents.concat(tempArray);
        }
        else{
          //chunckedEvents=chunckedEvents.concat(listCreatedEventSortByDate);
          var newEvent={
            events:listCreatedEventSortByDate
          };
          chunckedEvents.push(newEvent);
        }
        logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"processDataFile",
        result:typeResult.ongoing,message:`${chunckedTEI.length} chuncks for the TEI`});
        logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"processDataFile",
        result:typeResult.ongoing,message:`${chunckedEvents.length} chuncks for the the events`});
        //return res.send( chunckedEvents[0]);

        logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"readCSVProfilePECFile",
        result:typeResult.ongoing,message:`Break successfully TEI instance list  list to chuncks`});
        //Now insert the TEI 
        saveDataList2Dhis(dhis2Token,dhisResource.tei,chunckedTEI,(resOperation)=>{
          logger.log({level:levelType.info,operationType:typeOperation.postData,action:"/saveTEI",
        result:typeResult.success,message:`Insert successfully ${resOperation.length} TEI`});
          //console.log(resOperation);
          saveDataList2Dhis(dhis2Token,dhisResource.event,chunckedEvents,(resOperationEvent)=>{
            logger.log({level:levelType.info,operationType:typeOperation.postData,action:"/saveEvents",
            result:typeResult.success,message:`Insert successfully ${resOperationEvent.length} Events`});
            //console.log(resOperationEvent);
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
  let counter=0;
  localAsync.eachSeries(listData, function(metadata, nextResource) {
    let compter=1;
    //console.log(metadata);
    setTimeout(function(){
      console.log(`-------------- Wait for ${importConfig.app.timeoutDelay} sec before the next loop=>${counter} | chunks of ${dhisResource} ------------------------`);
      //console.log(`${JSON.stringify(metadata)}`);
      counter++;
      localNeedle.post(url,JSON.stringify(metadata),options,function(err,resp){
        if(err)
        {
            logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
                        message:`${err.Error}`});
            nextResource(err);
  
        }
  
        let dicOperationMetadata=[];
        if(dhisResource == "trackedEntityInstances")
        {
          for(let trackedIntities of metadata.trackedEntityInstances)
          {
            dicOperationMetadata.push (trackedIntities.trackedEntityInstance);
          }
        }
        if(dhisResource == "events")
        {
          for(let events of metadata.events)
          {
            dicOperationMetadata.push (events.trackedEntityInstance+"-"+events.programStage);
          }
          
        }
        /* console.log(JSON.stringify(resp.body.response.importSummaries));
        console.log(`################importSummaries#############################`); */
        dicOperationResults.push({
          httpStatus:resp.body.httpStatus,
          //metadata:`index-${compter}`
          metadata:dicOperationMetadata
        });
        compter++;
        if (resp.statusCode && (resp.statusCode < 200 || resp.statusCode > 399)) {
          if(resp.statusCode==409)
          {
            //console.log(metadata);
            logger.log({level:levelType.warning,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
              message:`Code: ${resp.statusCode}. Impossible de creer une ressource  qui existe deja`});
            /* console.log(`################importSummaries#############################`);
            console.log(JSON.stringify(resp.body.response.importSummaries)); */
            
            
          }
          else{
            logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
              message:`Code d'erreur http: ${resp.statusCode}`});
          }
        }
        nextResource();
        
      });//end localNeedle
    },importConfig.app.timeoutDelay);

    
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

  process.on('uncaughtException', err => {
    console.log(err);
    logger.log({level:levelType.error,operationType:typeOperation.stopTheService,action:`arret anormal du mediateur sur l'action `,result:typeResult.failed,
    message:`Stop the mediator on ${port}...`})
    process.exit(1)
    //globalRes.redirect("/error");
  });
  process.on('SIGTERM', signal => {
    logger.log({level:levelType.info,operationType:typeOperation.stopTheService,action:"Arret du mediateur",result:typeResult.success,
    message:`Arret normal du mediateur`})
    process.exit(0)
  });
  process.on('SIGINT', signal => {
  logger.log({level:levelType.error,operationType:typeOperation.stopTheService,action:"Arret brusque du mediateur",result:typeResult.success,
  message:`Arret anormal du mediateur`})
  process.exit(0)
  })
