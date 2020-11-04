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
  event:"events",
  orgUnit: "organisationUnits"
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
    });//end get(/importprofile)
    app.get("/updateagegroup",(req, res)=>{
      logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"/updateagegroup",result:typeResult.iniate,
      message:`Start the update of the agegroup process`});
      getListOrgUnit(dhis2Token,function(listOrgUnits){
        //console.log(listOrgUnits);
        logger.log({level:levelType.info,operationType:typeOperation.getData,action:"getListOrgUnit",
        result:typeResult.success,message:`Return ${listOrgUnits.length} OrgUnits`});
        //Now loops htrouh orgunit to get entitytracker lists
        getListTrackedEntities(dhis2Token,listOrgUnits,function(listTEIs)
        {
          console.log(`TEI returned: ${listTEIs.length}`);
          //res.send(listTEIs);
          if(listTEIs.length>0)
          {
            let listModifiedTEIs=customLibrairy.updateTEIAgeGoup(listTEIs,importConfig.programRulesAgeGroup,
              importConfig.attributeIds.ageAttributeId,importConfig.attributeIds.ageGroupAttributeId);
            /*let newTEIcollection = listModifiedTEIs.splice(0, 5);
            console.log("----------------------Collection to modify");
            console.log(newTEIcollection);*/
            //return res.send(listModifiedTEIs);
            updateDataList2Dhis(dhis2Token,dhisResource.tei,listModifiedTEIs,function(resultUpdateTEIs){
              //console.log();
              console.log("Update operation on the agegroup completed!");
              res.send(resultUpdateTEIs);
            });//end updateDataList2Dhis
          }
          else
          {
            res.send("No TEI to update");
          }
        })//end getListTrackedEntities

        //res.send("updatedone");
      })

    });//end get(/updateagegroup)
    
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
            console.log(`################importSummaries#############################`);
            console.log(JSON.stringify(resp.body.response.importSummaries)); 
            
            
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
function updateDataList2Dhis(dhis2Token,dhisResource,listData,callback){
  let localNeedle = require('needle');
  let localAsync = require('async');
  let dicOperationResults=[];
  localNeedle.defaults(
      {
          open_timeout: 600000
      });
  var url= "";
  let options={headers:{'Content-Type': 'application/json','Authorization':dhis2Token}};
  let listAlreadyExistedResources=[];
  let counter=0;
  localAsync.eachSeries(listData, function(metadata, nextResource) {
    let compter=1;
    if(dhisResource=="trackedEntityInstances")
    {
      url= URI(importConfig.dhis2Server.url).segment(dhisResource).segment(metadata.trackedEntityInstance);
    }
    url = url.toString();
    //console.log(metadata);
    setTimeout(function(){
      console.log(`-------------- Wait for ${importConfig.app.timeoutDelayUpdateTEI} sec before the next loop=>${counter} | chunks of ${dhisResource} ------------------------`);
      //console.log(`${JSON.stringify(metadata)}`);
      counter++;
      localNeedle.put(url,JSON.stringify(metadata),options,function(err,resp){
        if(err)
        {
            logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
                        message:`${err.Error}`});
            nextResource(err);
  
        }
  
        let dicOperationMetadata=[];
        if(dhisResource == "trackedEntityInstances")
        {
            dicOperationMetadata.push (metadata.trackedEntityInstance);
        }
        if(dhisResource == "events")
        {
          dicOperationMetadata.push (metadata.trackedEntityInstance+"-"+metadata.programStage);
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
            console.log(`################importSummaries#############################`);
            console.log(JSON.stringify(resp.body.response.importSummaries)); 
            
            
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
function getListOrgUnit(dhis2Toke,callbackMain){
  let localNeedle = require('needle');
  localNeedle.defaults(
      {
          open_timeout: 600000
      });
  let localAsync = require('async');
  var resourceData = [];
  var url="";
  url= URI(importConfig.dhis2Server.url).segment(dhisResource.orgUnit).segment(`${importConfig.app.orgUnits.OrgUnit}.json`);
  if(importConfig.app.orgUnits.isParent)
  {
    url.addQuery('includeDescendants', true);
    url.addQuery('pageSize',importConfig.app.orgUnits.pageSizeToPull);
    url.addQuery('level',3);
  }
  else
  {
    return callbackMain([{code:"code",id:importConfig.app.orgUnits.OrgUnit,displayName:"Name"}]);
  }
  url.addQuery('fields',"id,code,displayName");
  url = url.toString();
  console.log(`GetOrgunits => ${url}`);
  localAsync.whilst(
      callback => {
          return callback(null, url !== false);
        },
      callback => {
          
          var options={headers:{'Authorization':dhis2Token}};
          localNeedle.get(url,options, function(err, resp) {
              //url = false;
              if (err) {
                logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                message:`${err.Error}`});
                return callback(true, false);
              }
              if (resp.statusCode && (resp.statusCode < 200 || resp.statusCode > 399)) {
        logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                      message:`Code d'erreur http: ${resp.statusCode}`});
                  return callback(true, false);
              }
              var body = resp.body;
              if (!body.organisationUnits) {
        logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                      message:`Ressources invalid retournees par DHIS2`});
                  return callback(true, false);
              }
              if (body.pager) {
                if(body.pager.total === 0)
                {
                  logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                      message:`Pas de ressources retournees par DHIS - page: ${body.pager.page}`});
                  return callback(true, false);
                }
        
              }
              url=false;
              if (body.organisationUnits && body.organisationUnits.length > 0) {
                if(body.pager)
                {
                  console.log(`${body.pager.page}/${body.pager.pageCount}`);
                }
                
                resourceData = resourceData.concat(body.organisationUnits);
                  //force return only one loop data
                  //return callback(true, false);
              }
              if(body.pager)
              {
                const next = body.pager.nextPage;
                if(next)
                {
                    url = next;
                }
                return callback(null, url);
              }
              else{
                return callback(true, false);
              }
              

              
          })//end of needle.get
            
      },//end callback 2
      err=>{
          return callbackMain(resourceData);

      }
  );//end of async.whilst
}
function getListTrackedEntities(dhis2Toke,listOrgUnits,callbackMain){
  let localNeedle = require('needle');
  localNeedle.defaults(
      {
          open_timeout: 600000
      });
  let localAsync = require('async');
  var resourceData = [];
  var url="";
  let counter=1;
  localAsync.eachSeries(listOrgUnits, function(orgUnit, nextResource) {
    url= URI(importConfig.dhis2Server.url).segment(`${dhisResource.tei}.json`)
    
    setTimeout(function(){
      console.log(`-------------- Wait for ${importConfig.app.timeoutDelay} sec before the next loop=>${counter} | chunks of ${dhisResource.tei} ------------------------`);
      
      counter++;
      url.addQuery('ou',orgUnit.id);
      url.addQuery('fields',"created,trackedEntityInstance,orgUnit,trackedEntityType,attributes,enrollments");
      url = url.toString();
      console.log(`GetTrackedEntities=> ${url}`);
      localAsync.whilst(
        callback => {
            return callback(null, url !== false);
          },
        callback => {
            
            var options={headers:{'Authorization':dhis2Token}};
            //console.log("Enter 1")
            localNeedle.get(url,options, function(err, resp) {
                //url = false;
                
                if (err) {
                  logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                  message:`${err}`});
                  return callback(true, false);
                }
                if (resp.statusCode && (resp.statusCode < 200 || resp.statusCode > 399)) {
          logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                        message:`Code d'erreur http: ${resp.statusCode}`});
                    return callback(true, false);
                }
                var body = resp.body;
                if (!body.trackedEntityInstances) {
          logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                        message:`Ressources invalid retournees par DHIS2`});
                    return callback(true, false);
                }
                if (body.pager) {
                  if(body.pager.total === 0)
                  {
                    logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                        message:`Pas de ressources retournees par DHIS - page: ${body.pager.page}`});
                    return callback(true, false);
                  }
          
                }
                url=false;
                //console.log("----------------------------------------------------");
                if (body.trackedEntityInstances && body.trackedEntityInstances.length > 0) {
                  if(body.pager)
                  {
                    console.log(`${body.pager.page}/${body.pager.pageCount}`);
                  }
                  
                  resourceData = resourceData.concat(body.trackedEntityInstances);
                  //console.log("----------------------------------------------------")
                  console.log(`TEI nbre : ${resourceData.length}`);
                    //force return only one loop data
                    //return callback(true, false);
                }
                if(body.pager)
                {
                  const next = body.pager.nextPage;
                  if(next)
                  {
                      url = next;
                  }
                  return callback(null, url);
                }
                else{
                  return callback(true, false);
                  //nextResource();
                }
                
                
                
            })//end of needle.get
              
        },//end callback 2
        err=>{
            //return callbackMain(resourceData);
            // callback(true, false);
            nextResource();
  
        }
        //
    );//end of async.whilst
   
    },importConfig.app.timeoutDelay);
    //nextResource();
  },(err)=>{
    return callbackMain(resourceData);
  });//end of localAsync.eachSeries
  
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