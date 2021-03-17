'use strict'

const express = require('express');
const alasql=require('alasql');
const URI = require('urijs');
const isJSON = require('is-json');
const path = require('path');
var btoa = require('btoa');
const _ = require('underscore');
const moment = require('moment');
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
    app.get('/testgroupby', (req, res) => {
      /*var data=[ 
        { "category" : "Search Engines", "hits" : 5, "bytes" : 50189, "date":"2020-01-02" },
        { "category" : "Content Server", "hits" : 1, "bytes" : 17308 , "date":"2020-01-04" },
        { "category" : "Content Server", "hits" : 1, "bytes" : 47412 , "date":"2020-01-02" },
        { "category" : "Search Engines", "hits" : 1, "bytes" : 7601 , "date":"2020-05-02" },
        { "category" : "Business", "hits" : 1, "bytes" : 2847 , "date":"2020-05-02" },
        { "category" : "Content Server", "hits" : 1, "bytes" : 24210 , "date":"2020-06-02" },
        { "category" : "Internet Services", "hits" : 1, "bytes" : 3690 , "date":"2020-06-02" },
        { "category" : "Search Engines", "hits" : 6, "bytes" : 613036 , "date":"2020-06-02" },
        { "category" : "Search Engines", "hits" : 1, "bytes" : 2858 , "date":"2020-06-02" } 
         ];*/
        /*
        var data=[{"uuid":"id0001","sex":"M","visite":"2020-01-02","age":"12-22","_max":12},
                {"uuid":"id0001","sex":"M","visite":"2020-02-02","age":"12-22","_max":15},
                {"uuid":"id0001","sex":"M","visite":"2020-03-03","age":"12-22","_max":12},
                {"uuid":"id0002","sex":"F","visite":"2020-01-02","age":"23-29","_max":11},
                {"uuid":"id0002","sex":"F","visite":"2020-02-02","age":"23-29","_max":13},
                {"uuid":"id0002","sex":"F","visite":"2020-04-03","age":"23-29","_max":14},
                {"uuid":"id0003","sex":"M","visite":"2020-07-02","age":"30-35","_max":16},
                {"uuid":"id0003","sex":"M","visite":"2020-05-02","age":"30-35","_max":17},
                {"uuid":"id0003","sex":"M","visite":"2020-04-03","age":"30-35","_max":16}
      ]
      */
     var data=[{id:"id1",qty:10},{id:"id2",qty:15},{id:"id3",qty:15},{id:"id4",qty:20}];
         /*var result = alasql('SELECT category, sum(hits) AS hits, count(bytes) as bytes \
        FROM ? \
        GROUP BY category \
        ORDER BY bytes DESC',[data]);*/
        var result = alasql('SELECT t1.id,t1.qty,sum(t2.qty) as sum_cumul from ? as t1 \
        join ? as t2 on t1.id >= t2.id group by t1.id,t1.qty order by t1.id',[data,data]);


      console.log(result);
      res.send(result);
      });
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
        let listSuiviEvent=customLibrairy.buildSuiviEvents(patientData,importConfig.app.programId,
          importConfig.app.programStages.suiviPatient,importConfig.statutPatientOptionSet);
        listCreatedEvent=listCreatedEvent.concat(listSuiviEvent);
        //return res.send(listCreatedTEI);
        //GenerateValueEndPointFor each entities
        //listCreatedTEI=listCreatedTEI.splice(0,5);
        let teiNumber=listCreatedTEI.length;
        let listTEIs=listCreatedTEI;
        generateValueEndPoint(teiNumber,function(listGeneratedValues){
          console.log(`Generated values returned: ${listGeneratedValues.length}`);
          let listTEI2Process=listTEIs;
          let listModifiedTEIs=customLibrairy.updateUuid(listTEI2Process,listGeneratedValues,importConfig.attributeIds.uuidAttributeId);
          //return res.send(listModifiedTEIs);
          //Then sort by createdDate
          let listCreatedTEISortByDate=_.sortBy(listModifiedTEIs,'created');
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
          //return res.send( chunckedEvents);

          logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"readCSVProfilePECFile",
          result:typeResult.ongoing,message:`Break successfully TEI instance list  list to chuncks`});
          //Now insert the TEI 
          saveDataList2Dhis(dhis2Token,dhisResource.tei,chunckedTEI,(resOperation)=>{
            logger.log({level:levelType.info,operationType:typeOperation.postData,action:"/saveTEI",
          result:typeResult.success,message:`Insert successfully ${resOperation.length} TEI`});
            //console.log(resOperation);
            //console.log(chunckedTEI);
            //return res.send(chunckedEvents);
            saveDataList2Dhis(dhis2Token,dhisResource.event,chunckedEvents,(resOperationEvent)=>{
              logger.log({level:levelType.info,operationType:typeOperation.postData,action:"/saveEvents",
              result:typeResult.success,message:`Insert successfully ${resOperationEvent.length} Events`});
              //console.log(resOperationEvent);
              res.send("Import process done!");
            });
  
            
          })
          //res.send({source:listTEI2Process,changed:listModifiedTEIs});
        })//end generateValueEndPoint






        
        //return res.send(listCreatedEvent);
        //listCreatedEvent=[];
        //res.send("Import process done!");
        //res.send(chunckedEnrollments);
      });
    });//end get(/importprofile)
    app.get("/importtotal/:filename",(req, res)=>{
      logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"/importprofile",result:typeResult.iniate,
      message:`Start the import profile and PEC process`});
      let oMetadata=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="totalScreenedByMonth");
      var filePath=path.join(importConfig.app.dataFilePath,req.params.filename);
      customLibrairy.readCSVTotalFile(filePath,function(totalData){
        //console.log();
        //return res.send(totalData);
        if(totalData.length>0)
        {
          let listOrgUnitsName=totalData.map(function(item){return item['orgunits']});
          //return res.send(totalData);
          getListOrgUnitByNames(listOrgUnitsName,function(resolvedOrgUnitsList){
            //return res.send(resolvedOrgUnitsList);
            let adxPayLoad=customLibrairy.buildADXPayloadFromAggregatedTotal(totalData,resolvedOrgUnitsList,
              oMetadata);
            //return res.send(adxPayLoad);
            saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
              console.log(`------------Finished! ADX payload posted--------------------------`);
              res.send(adxSaveResults);
            });

          })
        }
        else{
          logger.log({level:levelType.info,operationType:typeOperation.normalProcess,action:"/importtotal",result:typeResult.failed,
      message:`File does not contains any data`});
        }
      
        //res.send(totalData);
      })//end customLibrairy.readCSVTotalFile

    });//end get(/importtatal)
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
    app.get("/generateuuid",(req, res)=>{
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
          let value2Generate=importConfig.app.value2Generate;
          let teiNumber= value2Generate>0?value2Generate:listTEIs.length;
          if(teiNumber>0)
          {
            //GenerateValueEndPointFor each entities
            generateValueEndPoint(teiNumber,function(listGeneratedValues){
              console.log(`Generated values returned: ${listGeneratedValues.length}`);
              let listTEI2Process=listTEIs;
              let listModifiedTEIs=customLibrairy.updateUuid(listTEI2Process,listGeneratedValues,importConfig.attributeIds.uuidAttributeId);
              //return res.send(listModifiedTEIs);
              updateDataList2Dhis(dhis2Token,dhisResource.tei,listModifiedTEIs,function(resultUpdateTEIs){
                //console.log();
                console.log("Update operation on the uuid completed!");
                res.send(resultUpdateTEIs);
              });//end updateDataList2Dhis
              //res.send({source:listTEI2Process,changed:listModifiedTEIs});
            })//end generateValueEndPoint

          }
          else
          {
            res.send("No TEI to update");
          }
        })//end getListTrackedEntities

        //res.send("updatedone");
      })

    });//end get(/generateuuid)
    //This generade 'Documented newly diagnosed per month'
    app.get("/generate_newlydiagpermonth",(req, res)=>{
      let newlyDiagnosesMeta=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="NewlyDiagnosedByMonth");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:newlyDiagnosesMeta.queryElement.stageId,
          startDate:importConfig.periodDataElementGenerated.startDate,
          endDate:importConfig.periodDataElementGenerated.endDate,
          dimensionOrgUnits:newlyDiagnosesMeta.queryElement.dimensionOrgUnits,
          dimensionIds:newlyDiagnosesMeta.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
            //console.log(`Counter: ${counter}`);
            /*
            console.log(`Counter: ${counter}`);
            console.log(row);
            console.log("-----------------");
            */
           let cleanedDate=row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else{
            resetDayOne=cleanedDate;
           }

           //let resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
            var jsonElement={
              uuid:row[newlyDiagnosesMeta.queryHeaders.uuid.index],
              ageGroup:row[newlyDiagnosesMeta.queryHeaders.ageGroup.index],
              sex:row[newlyDiagnosesMeta.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateDiagnosed:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        //return res.send(listEventElements);
        /*let result = alasql('SELECT SUM(_index) as nb,DATE(dateDiagnosed),MONTH(dateDiagnosed)  as month,YEAR(dateDiagnosed) as year,ageGroup,sex \
        FROM ? \
        GROUP BY YEAR(dateDiagnosed),MONTH(dateDiagnosed),ageGroup,sex \
        ORDER BY DATE(dateDiagnosed)',[listEventElements]);*/

        let listEventRecords = alasql('SELECT COUNT(uuid) as nb,dateDiagnosed as dateEvent,sex,ageGroup \
        FROM ? \
        GROUP BY dateDiagnosed,sex,ageGroup ',[listEventElements]);
        let metadataConfig={
          dataElementId:newlyDiagnosesMeta.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        console.log(`Counter ${listEventElements.length}`)
        //return res.send(listEventElements);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecords)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });
    app.get("/generate_cumulativediagpermonth",(req, res)=>{
      let newlyDiagnosesMeta=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="cumulativeDiagnosedByMonth");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:newlyDiagnosesMeta.queryElement.stageId,
          startDate:importConfig.periodDataElementGenerated.startDate,
          endDate:importConfig.periodDataElementGenerated.endDate,
          dimensionOrgUnits:newlyDiagnosesMeta.queryElement.dimensionOrgUnits,
          dimensionIds:newlyDiagnosesMeta.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
            
           let cleanedDate=row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else{
            resetDayOne=cleanedDate;
           }

           //let resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
            var jsonElement={
              uuid:row[newlyDiagnosesMeta.queryHeaders.uuid.index],
              ageGroup:row[newlyDiagnosesMeta.queryHeaders.ageGroup.index],
              sex:row[newlyDiagnosesMeta.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateDiagnosed:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        //return res.send(listEventElements);
        let listEventRecords = alasql('SELECT COUNT(uuid) as nb,dateDiagnosed as dateEvent,sex,ageGroup \
        FROM ? \
        GROUP BY dateDiagnosed,sex,ageGroup ',[listEventElements]);
        //return res.send(listEventRecords);
        //Now cumulate them per month
        let listEventRecordsCumulated=alasql('select t1.dateEvent as dateEvent,t1.sex,t1.ageGroup,t1.nb,SUM(t2.nb) as nb_cumul \
        from ? as t1 \
        inner join ? as t2 on (t1.dateEvent >= t2.dateEvent and t1.sex=t2.sex and t1.ageGroup=t2.ageGroup ) \
        group by t1.dateEvent,t1.sex,t1.ageGroup,t1.nb \
        order by t1.dateEvent,t1.sex,t1.ageGroup \
        ',[listEventRecords,listEventRecords]);
        return res.send(listEventRecordsCumulated);
        let metadataConfig={
          dataElementId:newlyDiagnosesMeta.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        console.log(`Counter ${listEventElements.length}`)
        //return res.send(listEventElements);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecords)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });
    app.get("/generate_newlydiagperquarter/:quarterNumber",(req, res)=>{
      let metaData=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="NewlyDiagnosedPerQuarter");
        //let districId=
        let quarterNumber=parseInt(req.params.quarterNumber);
        let momentObject=moment(importConfig.periodDataElementGenerated.startDate);
        let startDate=momentObject.quarter(quarterNumber).format('Y-MM-DD');
        let endDate=momentObject.quarter(quarterNumber+1).subtract(1,'ms').format('Y-MM-DD');
        //if()
        //let endDate=momentObject.quarter(quarterNumber+1).format('Y-MM-DD');
        //return res.send(metaData);
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:metaData.queryElement.stageId,
          startDate:startDate,
          endDate:endDate,
          dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
          dimensionIds:metaData.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
            //console.log(`Counter: ${counter}`);
            /*
            console.log(`Counter: ${counter}`);
            console.log(row);
            console.log("-----------------");
            */
           let cleanedDate=row[metaData.queryHeaders.dateDiagnosed.index]!=""?row[metaData.queryHeaders.dateDiagnosed.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else{
            resetDayOne=cleanedDate;
           }

           //let resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
            var jsonElement={
              uuid:row[metaData.queryHeaders.uuid.index],
              ageGroup:row[metaData.queryHeaders.ageGroup.index],
              sex:row[metaData.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateDiagnosed:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        
        let newCleanedList=[];
        for(let oElement of listEventElements)
        {
          newCleanedList.push(
            {
              uuid:oElement.uuid,
              ageGroup:oElement.ageGroup,
              sex:oElement.sex
            }
          )
        }
        let listEventRecordsDistinct = alasql('SELECT distinct(uuid) as uuid,ageGroup,sex \
        FROM ? \
        group by uuid,ageGroup,sex',[newCleanedList]);

        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,sex,ageGroup \
        FROM ? \
        GROUP BY sex,ageGroup ',[listEventRecordsDistinct]);
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        console.log(`Counter ${listEventElements.length}`)
        //return res.send(listEventElements);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXQuarterPayload(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0],startDate)
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });
    app.get("/generate_newlytreatpermonth",(req, res)=>{
      let newlyDiagnosesMeta=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="NewlyTreatedByMonth");
        //let districId=
        
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:newlyDiagnosesMeta.queryElement.stageId,
          startDate:importConfig.periodDataElementGenerated.startDate,
          endDate:importConfig.periodDataElementGenerated.endDate,
          dimensionOrgUnits:newlyDiagnosesMeta.queryElement.dimensionOrgUnits,
          dimensionIds:newlyDiagnosesMeta.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          //console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
            //console.log(`Counter: ${counter}`);
            /*
            console.log(`Counter: ${counter}`);
            console.log(row);
            console.log("-----------------");
            */
           //console.log(newlyDiagnosesMeta.queryHeaders);
           let cleanedDate=row[newlyDiagnosesMeta.queryHeaders.dateVisite.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateVisite.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else
           {
            resetDayOne=cleanedDate
           }
            var jsonElement={
              uuid:row[newlyDiagnosesMeta.queryHeaders.uuid.index],
              ageGroup:row[newlyDiagnosesMeta.queryHeaders.ageGroup.index],
              sex:row[newlyDiagnosesMeta.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateVisite:resetDayOne,
              traitement:row[newlyDiagnosesMeta.queryHeaders.traitement.index]
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        //return res.send(listEventElements);
        let listEventElementsWithTraitement = alasql('SELECT uuid,ageGroup,sex,dateVisite,traitement \
        FROM ? \
        WHERE traitement <> ""',[listEventElements]);
        /*console.log(`All patient = ${listEventElements.length}`);
        console.log(`Patient with traitement = ${result.length}`);
        return res.send({});*/
        /*let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);*/
        let newCleanedList=[];
        for(let oElement of listEventElementsWithTraitement)
        {
          newCleanedList.push(
            {
              uuid:oElement.uuid,
              ageGroup:oElement.ageGroup,
              sex:oElement.sex,
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateVisite:oElement.dateVisite,
            }
          )
        }
        let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[newCleanedList]);
        //now count rows group by agegrou and sex
        //console.log(`Counter : ${listEventElements.length}`);
        //return res.send(listEventRecords);
        /*let duplicateList=alasql('SELECT uuid,COUNT(uuid) as nb \
        FROM ? \
        GROUP BY uuid having COUNT(uuid) > 1 ',[listEventRecords]);*/
        //return res.send(duplicateList);
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,dateVisite as dateEvent,sex,ageGroup \
        FROM ? \
        GROUP BY dateVisite,sex,ageGroup ',[listEventRecords]);
        //return res.send(listEventRecords);
        let metadataConfig={
          dataElementId:newlyDiagnosesMeta.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        console.log(`Original list: ${listEventRecords.length}`);
        console.log(`Event grouped list: ${listEventRecordsGrouped.length}`);
        /*
        let sum=0;
        for(let oEvent of listEventRecordsGrouped){
          sum+=oEvent.nb;
        } 
        console.log(`sum ${sum}`);
        return res.send(listEventRecordsGrouped);
         */
        
        //return res.send(listEventRecordsGrouped);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_newlytreatpermonth
    app.get("/generate_newlytreatperquarter/:quarterNumber",(req, res)=>{
      let metaData=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="NewlyTreatedPerQuarter");
        //let districId=
        let quarterNumber=parseInt(req.params.quarterNumber);
        let momentObject=moment(importConfig.periodDataElementGenerated.startDate);
        let startDate=momentObject.quarter(quarterNumber).format('Y-MM-DD');
        let endDate=momentObject.quarter(quarterNumber+1).subtract(1,'ms').format('Y-MM-DD');
        //if()
        //let endDate=momentObject.quarter(quarterNumber+1).format('Y-MM-DD');
        //return res.send(metaData);
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:metaData.queryElement.stageId,
          startDate:startDate,
          endDate:endDate,
          dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
          dimensionIds:metaData.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          let listEventElements=[];
          let counter=1;
          //return res.send(eventRows);
          for(let row of eventRows)
          {
            
            var jsonElement={
              uuid:row[metaData.queryHeaders.uuid.index],
              ageGroup:row[metaData.queryHeaders.ageGroup.index],
              sex:row[metaData.queryHeaders.sex.index],
              traitement:row[metaData.queryHeaders.traitement.index]
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        //return res.send(listEventElements);
        let listEventElementsWithTraitement = alasql('SELECT uuid,ageGroup,sex,traitement \
        FROM ? \
        WHERE traitement <> ""',[listEventElements]);
        let newCleanedList=[];
        for(let oElement of listEventElementsWithTraitement)
        {
          newCleanedList.push(
            {
              uuid:oElement.uuid,
              ageGroup:oElement.ageGroup,
              sex:oElement.sex
            }
          )
        }
        let listEventRecords = alasql('SELECT distinct(uuid) as uuid,ageGroup,sex \
        FROM ? \
        group by uuid,ageGroup,sex',[newCleanedList]);
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,sex,ageGroup \
        FROM ? \
        GROUP BY sex,ageGroup ',[listEventRecords]);
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        
        //return res.send(listEventRecordsGrouped);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXQuarterPayload(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0],startDate);
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_newlytreatpermonth
    app.get("/generate_patdocusystandiagpermonth",(req, res)=>{
      let newlyDiagnosesMeta=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="patientWithDocumentedSyst");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:newlyDiagnosesMeta.queryElement.stageId,
          startDate:importConfig.periodDataElementGenerated.startDate,
          endDate:importConfig.periodDataElementGenerated.endDate,
          dimensionOrgUnits:newlyDiagnosesMeta.queryElement.dimensionOrgUnits,
          dimensionIds:newlyDiagnosesMeta.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          //console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
            //console.log(`Counter: ${counter}`);
            /*
            console.log(`Counter: ${counter}`);
            console.log(row);
            console.log("-----------------");
            */
           //console.log(newlyDiagnosesMeta.queryHeaders);
           let cleanedDate=row[newlyDiagnosesMeta.queryHeaders.dateVisite.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateVisite.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else
           {
            resetDayOne=cleanedDate
           }
            var jsonElement={
              uuid:row[newlyDiagnosesMeta.queryHeaders.uuid.index],
              ageGroup:row[newlyDiagnosesMeta.queryHeaders.ageGroup.index],
              sex:row[newlyDiagnosesMeta.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateVisite:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        //return res.send(listEventElements);
        /*let result = alasql('SELECT SUM(_index) as nb,DATE(dateDiagnosed),MONTH(dateDiagnosed)  as month,YEAR(dateDiagnosed) as year,ageGroup,sex \
        FROM ? \
        GROUP BY YEAR(dateDiagnosed),MONTH(dateDiagnosed),ageGroup,sex \
        ORDER BY DATE(dateDiagnosed)',[listEventElements]);*/
        /*let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);*/
        let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);
        //now count rows group by agegrou and sex
        console.log(`Counter : ${listEventElements.length}`);
        // res.send(listEventRecords);
        /*let duplicateList=alasql('SELECT uuid,COUNT(uuid) as nb \
        FROM ? \
        GROUP BY uuid having COUNT(uuid) > 1 ',[listEventRecords]);*/
        //return res.send(duplicateList);
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,dateVisite as dateEvent,sex,ageGroup \
        FROM ? \
        GROUP BY dateVisite,sex,ageGroup ',[listEventRecords]);
        //return res.send(listEventRecords);
        let metadataConfig={
          dataElementId:newlyDiagnosesMeta.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        console.log(`Original list: ${listEventRecords.length}`);
        console.log(`Event grouped list: ${listEventRecordsGrouped.length}`);
        /*
        let sum=0;
        for(let oEvent of listEventRecordsGrouped){
          sum+=oEvent.nb;
        } 
        console.log(`sum ${sum}`);
        return res.send(listEventRecordsGrouped);
         */
        
        //return res.send(listEventRecordsGrouped);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_newlytreatpermonth
    app.get("/generate_pacontrolepermonth",(req, res)=>{
      let newlyDiagnosesMeta=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="patientWithControledBPMonth");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:newlyDiagnosesMeta.queryElement.stageId,
          startDate:importConfig.periodDataElementGenerated.startDate,
          endDate:importConfig.periodDataElementGenerated.endDate,
          dimensionOrgUnits:newlyDiagnosesMeta.queryElement.dimensionOrgUnits,
          dimensionIds:newlyDiagnosesMeta.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          //console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
            //console.log(`Counter: ${counter}`);
            /*
            console.log(`Counter: ${counter}`);
            console.log(row);
            console.log("-----------------");
            */
           //console.log(newlyDiagnosesMeta.queryHeaders);
           let cleanedDate=row[newlyDiagnosesMeta.queryHeaders.dateVisite.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateVisite.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else
           {
            resetDayOne=cleanedDate
           }
            var jsonElement={
              uuid:row[newlyDiagnosesMeta.queryHeaders.uuid.index],
              ageGroup:row[newlyDiagnosesMeta.queryHeaders.ageGroup.index],
              sex:row[newlyDiagnosesMeta.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateVisite:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        //console.log(`Result Height: ${listEventElements.length}`)
        //return res.send(listEventElements);
        /*let result = alasql('SELECT SUM(_index) as nb,DATE(dateDiagnosed),MONTH(dateDiagnosed)  as month,YEAR(dateDiagnosed) as year,ageGroup,sex \
        FROM ? \
        GROUP BY YEAR(dateDiagnosed),MONTH(dateDiagnosed),ageGroup,sex \
        ORDER BY DATE(dateDiagnosed)',[listEventElements]);*/
        /*let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);*/
        let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);
        //now count rows group by agegrou and sex
        console.log(`Counter : ${listEventElements.length}`);
        //return res.send(listEventRecords);
        /*let duplicateList=alasql('SELECT uuid,COUNT(uuid) as nb \
        FROM ? \
        GROUP BY uuid having COUNT(uuid) > 1 ',[listEventRecords]);*/
        //return res.send(duplicateList);
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,dateVisite as dateEvent,sex,ageGroup \
        FROM ? \
        GROUP BY dateVisite,sex,ageGroup ',[listEventRecords]);
        //return res.send(listEventRecords);
        let metadataConfig={
          dataElementId:newlyDiagnosesMeta.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        console.log(`Original list: ${listEventElements.length}`);
        console.log(`Event grouped list: ${listEventRecordsGrouped.length}`);
        /*
        let sum=0;
        for(let oEvent of listEventRecordsGrouped){
          sum+=oEvent.nb;
        } 
        console.log(`sum ${sum}`);
        return res.send(listEventRecordsGrouped);
         */
        //return res.send(listEventRecordsGrouped);
        //return res.send(listEventRecords);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_pacontrolepermonth
    app.get("/generate_pacontroleperquarter/:quarterNumber",(req, res)=>{
      
        let metaData=importConfig.dataElements2Generate.find(
          dataElementMeta=>dataElementMeta.name=="patientWithControledBPQuarter");
          //let districId=
          let quarterNumber=parseInt(req.params.quarterNumber);
          let momentObject=moment(importConfig.periodDataElementGenerated.startDate);
          let startDate=momentObject.quarter(quarterNumber).format('Y-MM-DD');
          let endDate=momentObject.quarter(quarterNumber+1).subtract(1,'ms').format('Y-MM-DD');
          //if()
          //let endDate=momentObject.quarter(quarterNumber+1).format('Y-MM-DD');
          //return res.send(metaData);
          let eventQuery={
            programId:importConfig.app.programId,
            stageId:metaData.queryElement.stageId,
            startDate:startDate,
            endDate:endDate,
            dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
            dimensionIds:metaData.queryElement.dimensionIds
          }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          //console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
           //console.log(newlyDiagnosesMeta.queryHeaders);
           let cleanedDate=row[metaData.queryHeaders.dateVisite.index]!=""?row[metaData.queryHeaders.dateVisite.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else
           {
            resetDayOne=cleanedDate
           }
            var jsonElement={
              uuid:row[metaData.queryHeaders.uuid.index],
              ageGroup:row[metaData.queryHeaders.ageGroup.index],
              sex:row[metaData.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateVisite:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);
        //now count rows group by agegrou and sex
        //console.log(`Counter : ${listEventElements.length}`);
        let newCleanedList=[];
        for(let oElement of listEventRecords)
        {
          newCleanedList.push(
            {
              uuid:oElement.uuid,
              ageGroup:oElement.ageGroup,
              sex:oElement.sex
            }
          )
        }
        let listEventRecordsDistinct = alasql('SELECT distinct(uuid) as uuid,ageGroup,sex \
        FROM ? \
        group by uuid,ageGroup,sex',[newCleanedList]);
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,sex,ageGroup \
        FROM ? \
        GROUP BY sex,ageGroup ',[listEventRecordsDistinct]);
        //return res.send(listEventRecords);
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXQuarterPayload(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0],startDate)
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_pacontrolepermonth
    app.get("/generate_patientfollowupmonth",(req, res)=>{
      let newlyDiagnosesMeta=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="patientFollowUpByMonth");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:newlyDiagnosesMeta.queryElement.stageId,
          startDate:importConfig.periodDataElementGenerated.startDate,
          endDate:importConfig.periodDataElementGenerated.endDate,
          dimensionOrgUnits:newlyDiagnosesMeta.queryElement.dimensionOrgUnits,
          dimensionIds:newlyDiagnosesMeta.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          //console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
            //console.log(`Counter: ${counter}`);
            /*
            console.log(`Counter: ${counter}`);
            console.log(row);
            console.log("-----------------");
            */
           //console.log(newlyDiagnosesMeta.queryHeaders);
           let cleanedDate=row[newlyDiagnosesMeta.queryHeaders.dateVisite.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateVisite.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else
           {
            resetDayOne=cleanedDate
           }
            var jsonElement={
              uuid:row[newlyDiagnosesMeta.queryHeaders.uuid.index],
              ageGroup:row[newlyDiagnosesMeta.queryHeaders.ageGroup.index],
              sex:row[newlyDiagnosesMeta.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateVisite:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        //return res.send(listEventElements);
        /*let result = alasql('SELECT SUM(_index) as nb,DATE(dateDiagnosed),MONTH(dateDiagnosed)  as month,YEAR(dateDiagnosed) as year,ageGroup,sex \
        FROM ? \
        GROUP BY YEAR(dateDiagnosed),MONTH(dateDiagnosed),ageGroup,sex \
        ORDER BY DATE(dateDiagnosed)',[listEventElements]);*/
        /*let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);*/
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,ageGroup,sex,dateVisite as dateEvent \
        FROM ? \
        GROUP BY dateVisite,sex,ageGroup ',[listEventElements]);
        //now count rows group by agegrou and sex
        console.log(`Counter : ${listEventRecordsGrouped.length}`);
        //return res.send(listEventRecordsGrouped);
        /*let duplicateList=alasql('SELECT uuid,COUNT(uuid) as nb \
        FROM ? \
        GROUP BY uuid having COUNT(uuid) > 1 ',[listEventRecords]);*/
        //return res.send(duplicateList);
        //return res.send(listEventRecordsGrouped);
        let metadataConfig={
          dataElementId:newlyDiagnosesMeta.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        //console.log(`Original list: ${listEventElements.length}`);
        //console.log(`Event grouped list: ${listEventRecordsGrouped.length}`);
        /*
        let sum=0;
        for(let oEvent of listEventRecordsGrouped){
          sum+=oEvent.nb;
        } 
        console.log(`sum ${sum}`);
        return res.send(listEventRecordsGrouped);
         */
        
        //return res.send(listEventRecords);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_patientfollowupmonth
    app.get("/generate_patientfollowupperquarter/:quarterNumber",(req, res)=>{
      
        let metaData=importConfig.dataElements2Generate.find(
          dataElementMeta=>dataElementMeta.name=="patientFollowUpPerQuarter");
        let quarterNumber=parseInt(req.params.quarterNumber);
        let momentObject=moment(importConfig.periodDataElementGenerated.startDate);
        let startDate=momentObject.quarter(quarterNumber).format('Y-MM-DD');
        let endDate=momentObject.quarter(quarterNumber+1).subtract(1,'ms').format('Y-MM-DD');
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:metaData.queryElement.stageId,
          startDate:startDate,
          endDate:endDate,
          dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
          dimensionIds:metaData.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          let listEventElements=[];
          let counter=1;
          for(let row of eventRows)
          {
           let cleanedDate=row[metaData.queryHeaders.dateVisite.index]!=""?row[metaData.queryHeaders.dateVisite.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else
           {
            resetDayOne=cleanedDate
           }
          var jsonElement={
            uuid:row[metaData.queryHeaders.uuid.index],
            ageGroup:row[metaData.queryHeaders.ageGroup.index],
            sex:row[metaData.queryHeaders.sex.index],
            dateVisite:resetDayOne
          }
          counter++;
          listEventElements.push(jsonElement);
          
        }
        let newCleanedList=[];
        for(let oElement of listEventElements)
        {
          newCleanedList.push(
            {
              uuid:oElement.uuid,
              ageGroup:oElement.ageGroup,
              sex:oElement.sex
            }
          )
        }
          let listEventRecordsDistinct = alasql('SELECT distinct(uuid) as uuid,ageGroup,sex \
          FROM ? \
          group by uuid,ageGroup,sex',[newCleanedList]);

        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,ageGroup,sex \
        FROM ? \
        GROUP BY ageGroup,sex',[listEventRecordsDistinct]);
        //now count rows group by agegrou and sex
        //console.log(`Counter : ${listEventRecordsGrouped.length}`);
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXQuarterPayload(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0],startDate)
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_patientfollowupmonth
    app.get("/generate_docdiagpatientmonth",(req, res)=>{
      let newlyDiagnosesMeta=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="patientDocumentedDiagnosed");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:newlyDiagnosesMeta.queryElement.stageId,
          startDate:importConfig.periodDataElementGenerated.startDate,
          endDate:importConfig.periodDataElementGenerated.endDate,
          dimensionOrgUnits:newlyDiagnosesMeta.queryElement.dimensionOrgUnits,
          dimensionIds:newlyDiagnosesMeta.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          //console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
            //console.log(`Counter: ${counter}`);
            /*
            console.log(`Counter: ${counter}`);
            console.log(row);
            console.log("-----------------");
            */
           //console.log(newlyDiagnosesMeta.queryHeaders);
           let cleanedDate=row[newlyDiagnosesMeta.queryHeaders.dateVisite.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateVisite.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else
           {
            resetDayOne=cleanedDate
           }
            var jsonElement={
              uuid:row[newlyDiagnosesMeta.queryHeaders.uuid.index],
              ageGroup:row[newlyDiagnosesMeta.queryHeaders.ageGroup.index],
              sex:row[newlyDiagnosesMeta.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateVisite:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        //return res.send(listEventElements);
        /*console.log(`All patient = ${listEventElements.length}`);
        console.log(`Patient with traitement = ${result.length}`);
        return res.send({});*/
        /*let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);*/
        let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);
        //now count rows group by agegrou and sex
        //console.log(`Counter : ${listEventElements.length}`);
        //return res.send(listEventRecords);
        /*let duplicateList=alasql('SELECT uuid,COUNT(uuid) as nb \
        FROM ? \
        GROUP BY uuid having COUNT(uuid) > 1 ',[listEventRecords]);*/
        //return res.send(duplicateList);
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,dateVisite as dateEvent,sex,ageGroup \
        FROM ? \
        GROUP BY dateVisite,sex,ageGroup ',[listEventRecords]);
        //return res.send(listEventRecords);
        let metadataConfig={
          dataElementId:newlyDiagnosesMeta.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        console.log(`Original list: ${listEventRecords.length}`);
        console.log(`Event grouped list: ${listEventRecordsGrouped.length}`);
        /*
        let sum=0;
        for(let oEvent of listEventRecordsGrouped){
          sum+=oEvent.nb;
        } 
        console.log(`sum ${sum}`);
        return res.send(listEventRecordsGrouped);
         */
        
        //return res.send(listEventRecordsGrouped);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_newlytreatpermonth
    app.get("/generate_docdiagpatientperquarter/:quarterNumber",(req, res)=>{
      let metaData=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="patientDocumentedDiagnosedPerQuater");
        //let districId=
      let quarterNumber=parseInt(req.params.quarterNumber);
      let momentObject=moment(importConfig.periodDataElementGenerated.startDate);
      let startDate=momentObject.quarter(quarterNumber).format('Y-MM-DD');
      let endDate=momentObject.quarter(quarterNumber+1).subtract(1,'ms').format('Y-MM-DD');
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:metaData.queryElement.stageId,
          startDate:startDate,
          endDate:endDate,
          dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
          dimensionIds:metaData.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          //console.log("Event Rows returned!!!");
          let counter=1;
          for(let row of eventRows)
          {
           let cleanedDate=row[metaData.queryHeaders.dateVisite.index]!=""?row[metaData.queryHeaders.dateVisite.index].split(" ")[0]:"";
           //set day to 01;
           let resetDayOne="";
           if(cleanedDate!="")
           {
            resetDayOne=cleanedDate.split("-")[0]+"-"+cleanedDate.split("-")[1]+"-"+"01";
           }
           else
           {
            resetDayOne=cleanedDate
           }
            var jsonElement={
              uuid:row[metaData.queryHeaders.uuid.index],
              ageGroup:row[metaData.queryHeaders.ageGroup.index],
              sex:row[metaData.queryHeaders.sex.index],
              //dateDiagnosed:row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index]!=""?row[newlyDiagnosesMeta.queryHeaders.dateDiagnosed.index].split(" ")[0]:""
              dateVisite:resetDayOne
            }
            counter++;
            listEventElements.push(jsonElement);
            
          }
        let listEventRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,sex,ageGroup ',[listEventElements]);

        let newCleanedList=[];
        for(let oElement of listEventRecords)
        {
          newCleanedList.push(
            {
              uuid:oElement.uuid,
              ageGroup:oElement.ageGroup,
              sex:oElement.sex
            }
          )
        }
        let listEventRecordsDistinct = alasql('SELECT distinct(uuid) as uuid,ageGroup,sex \
        FROM ? \
        group by uuid,ageGroup,sex',[newCleanedList]);
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,sex,ageGroup \
        FROM ? \
        GROUP BY sex,ageGroup ',[listEventRecordsDistinct]);

        /*
        let listEventRecordsGrouped = alasql('SELECT COUNT(uuid) as nb,dateVisite as dateEvent,sex,ageGroup \
        FROM ? \
        GROUP BY dateVisite,sex,ageGroup ',[listEventRecords]);
        */
        //return res.send(listEventRecords);
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        /*console.log(`Original list: ${listEventRecords.length}`);
        console.log(`Event grouped list: ${listEventRecordsGrouped.length}`);
        */
        
        //return res.send(listEventRecordsGrouped);
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        //return res.send(listEventsChangedAgeGroup2Id);
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXQuarterPayload(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0],startDate)
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_newlytreatpermonth
    app.get("/generate_sbpbaseline",(req, res)=>{
      let metaData=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="sbpBaseLine");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:metaData.queryElement.stageId,
          startDate:metaData.period.startDate,
          endDate:metaData.period.endDate,
          dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
          dimensionIds:metaData.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          let counter=1;
          for(let row of eventRows)
          {
            let cleanedDate=row[metaData.queryHeaders.eventDate.index]!=""?row[metaData.queryHeaders.eventDate.index].split(" ")[0]:"";
           
            var jsonElement={
              uuid:row[metaData.queryHeaders.uuid.index],
              ageGroup:row[metaData.queryHeaders.ageGroup.index],
              sex:row[metaData.queryHeaders.sex.index],
              dateVisite:cleanedDate,
              moySyst:parseFloat(row[metaData.queryHeaders.moySyst.index])
            }
            counter++;
            listEventElements.push(jsonElement);
          }
        let listMinRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,ageGroup,sex ',[listEventElements]);
        let listEventMin=[];
        for(let minRecord of listMinRecords)
        {
          let minEvents=listEventElements.find(oEvent=>oEvent.uuid==minRecord.uuid && oEvent.ageGroup==minRecord.ageGroup
            && oEvent.dateVisite==minRecord.dateVisite);
          listEventMin.push(minEvents);
        }
        let listEventMinNormalized=[];
        for(let record of listEventMin)
        {
          let resetDayOne="";
          if(record.dateVisite!="")
          {
            resetDayOne=record.dateVisite.split("-")[0]+"-"+record.dateVisite.split("-")[1]+"-"+"01";
          }
          else
          {
            resetDayOne=cleanedDate
          }
          listEventMinNormalized.push({
            uuid:record.uuid,
            moySyst:record.moySyst,
            ageGroup:record.ageGroup,
            sex:record.sex,
            dateVisite:resetDayOne
          })
        }
        
        let listEventRecords = alasql('SELECT AVG(moySyst) as moySyst,ageGroup,sex,dateVisite \
        FROM ? \
        GROUP BY ageGroup,sex,dateVisite ',[listEventMinNormalized]);
        
        //return res.send(listEventRecords);
        //Fixed digit after comma
        let listEventRecordsGrouped=[];
        for(let event of listEventRecords){
          /*
          listEventRecordsGrouped.push({
            nb:event.moySyst.toFixed(2),
            ageGroup:event.ageGroup,
            sex:event.sex,
            dateEvent:event.dateVisite
          })*/
          
          let momentObject=moment(importConfig.periodDataElementGenerated.startDate);
          for(let i=0;i<12;i++)
          {
            listEventRecordsGrouped.push({
              nb:event.moySyst.toFixed(2),
              ageGroup:event.ageGroup,
              sex:event.sex,
              dateEvent:i==0?momentObject.add(0,'M').format().split("T")[0]:momentObject.add(1,'M').format().split("T")[0]
            })
          }
        }
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
        //return res.send(listEventRecordsGrouped);
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_sbpbaseline
    app.get("/generate_sbpbaselinechange",(req, res)=>{
      let metaData=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="sbpBaseLineChange");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:metaData.queryElement.stageId,
          startDate:metaData.period.startDate,
          endDate:metaData.period.endDate,
          dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
          dimensionIds:metaData.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          let counter=1;
          for(let row of eventRows)
          {
            let cleanedDate=row[metaData.queryHeaders.eventDate.index]!=""?row[metaData.queryHeaders.eventDate.index].split(" ")[0]:"";
           
            var jsonElement={
              uuid:row[metaData.queryHeaders.uuid.index],
              ageGroup:row[metaData.queryHeaders.ageGroup.index],
              sex:row[metaData.queryHeaders.sex.index],
              dateVisite:cleanedDate,
              moySyst:parseFloat(row[metaData.queryHeaders.moySyst.index])
            }
            counter++;
            listEventElements.push(jsonElement);
          }
        let listMinRecords = alasql('SELECT uuid,ageGroup,sex,MIN(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,ageGroup,sex ',[listEventElements]);
        let listEventMin=[];
        //return res.send(listMinRecords);
        for(let minRecord of listMinRecords)
        {
          let minEvents=listEventElements.find(oEvent=>oEvent.uuid==minRecord.uuid && oEvent.ageGroup==minRecord.ageGroup
            && oEvent.dateVisite==minRecord.dateVisite);
            let resetDayOne="";
            if(minRecord.dateVisite!="")
            {
             resetDayOne=minRecord.dateVisite.split("-")[0]+"-"+minRecord.dateVisite.split("-")[1]+"-"+"01";
            }
            else
            {
             resetDayOne=minRecord.dateVisite
            }

          listEventMin.push(
            {
              uuid:minEvents.uuid,
              ageGroup:minEvents.ageGroup,
              sex:minEvents.sex,
              moySyst:minEvents.moySyst,
              dateVisite:resetDayOne
            }
          );
        }
        
        let listEventRecords = alasql('SELECT AVG(moySyst) as moySyst,ageGroup,sex,dateVisite \
        FROM ? \
        GROUP BY ageGroup,sex,dateVisite ',[listEventMin]);
        
        //return res.send(listEventRecords);
        //Fixed digit after comma
        let listEventRecordsGrouped=[];
        for(let event of listEventRecords){
          
          listEventRecordsGrouped.push({
            nb:event.moySyst.toFixed(2),
            ageGroup:event.ageGroup,
            sex:event.sex,
            dateEvent:event.dateVisite
          })
        }
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
        //return res.send(listEventRecordsGrouped);
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_sbpbaselinechange
    app.get("/generate_sbplastvisite",(req, res)=>{
      let metaData=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="sbpLastVisit");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:metaData.queryElement.stageId,
          startDate:metaData.period.startDate,
          endDate:metaData.period.endDate,
          dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
          dimensionIds:metaData.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          let counter=1;
          for(let row of eventRows)
          {
            let cleanedDate=row[metaData.queryHeaders.eventDate.index]!=""?row[metaData.queryHeaders.eventDate.index].split(" ")[0]:"";
           
            var jsonElement={
              uuid:row[metaData.queryHeaders.uuid.index],
              ageGroup:row[metaData.queryHeaders.ageGroup.index],
              sex:row[metaData.queryHeaders.sex.index],
              dateVisite:cleanedDate,
              moySyst:parseFloat(row[metaData.queryHeaders.moySyst.index])
            }
            counter++;
            listEventElements.push(jsonElement);
          }
        let listMinRecords = alasql('SELECT uuid,ageGroup,sex,MAX(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,ageGroup,sex ',[listEventElements]);
        let listEventMin=[];
        for(let minRecord of listMinRecords)
        {
          let minEvents=listEventElements.find(oEvent=>oEvent.uuid==minRecord.uuid && oEvent.ageGroup==minRecord.ageGroup
            && oEvent.dateVisite==minRecord.dateVisite);
          listEventMin.push(minEvents);
        }
        let listEventMinNormalized=[];
        for(let record of listEventMin)
        {
          let resetDayOne="";
          if(record.dateVisite!="")
          {
            resetDayOne=record.dateVisite.split("-")[0]+"-"+record.dateVisite.split("-")[1]+"-"+"01";
          }
          else
          {
            resetDayOne=cleanedDate
          }
          listEventMinNormalized.push({
            uuid:record.uuid,
            moySyst:record.moySyst,
            ageGroup:record.ageGroup,
            sex:record.sex,
            dateVisite:resetDayOne
          })
        }
        
        let listEventRecords = alasql('SELECT AVG(moySyst) as moySyst,ageGroup,sex,dateVisite \
        FROM ? \
        GROUP BY ageGroup,sex,dateVisite ',[listEventMinNormalized]);
        
        //return res.send(listEventRecords);
        //Fixed digit after comma
        let listEventRecordsGrouped=[];
        for(let event of listEventRecords){
          /*
          listEventRecordsGrouped.push({
            nb:event.moySyst.toFixed(2),
            ageGroup:event.ageGroup,
            sex:event.sex,
            dateEvent:event.dateVisite
          })*/
          
          let momentObject=moment(importConfig.periodDataElementGenerated.startDate);
          for(let i=0;i<12;i++)
          {
            listEventRecordsGrouped.push({
              nb:event.moySyst.toFixed(2),
              ageGroup:event.ageGroup,
              sex:event.sex,
              dateEvent:i==0?momentObject.add(0,'M').format().split("T")[0]:momentObject.add(1,'M').format().split("T")[0]
            })
          }
        }
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
        //return res.send(listEventRecordsGrouped);
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_sbpLastVisite
    app.get("/generate_sbplastvisitechange",(req, res)=>{
      let metaData=importConfig.dataElements2Generate.find(
        dataElementMeta=>dataElementMeta.name=="sbpLastVisitChange");
        //let districId=
        let eventQuery={
          programId:importConfig.app.programId,
          stageId:metaData.queryElement.stageId,
          startDate:metaData.period.startDate,
          endDate:metaData.period.endDate,
          dimensionOrgUnits:metaData.queryElement.dimensionOrgUnits,
          dimensionIds:metaData.queryElement.dimensionIds
        }
        getEventAnalytics(eventQuery,function(eventRows){
          //return res.send(eventRows);
          let listEventElements=[];
          let counter=1;
          for(let row of eventRows)
          {
            let cleanedDate=row[metaData.queryHeaders.eventDate.index]!=""?row[metaData.queryHeaders.eventDate.index].split(" ")[0]:"";
           
            var jsonElement={
              uuid:row[metaData.queryHeaders.uuid.index],
              ageGroup:row[metaData.queryHeaders.ageGroup.index],
              sex:row[metaData.queryHeaders.sex.index],
              dateVisite:cleanedDate,
              moySyst:parseFloat(row[metaData.queryHeaders.moySyst.index])
            }
            counter++;
            listEventElements.push(jsonElement);
          }
        let listMinRecords = alasql('SELECT uuid,ageGroup,sex,MAX(dateVisite) as dateVisite \
        FROM ? \
        GROUP BY uuid,ageGroup,sex ',[listEventElements]);
        let listEventMin=[];
        //return res.send(listMinRecords);
        for(let minRecord of listMinRecords)
        {
          let minEvents=listEventElements.find(oEvent=>oEvent.uuid==minRecord.uuid && oEvent.ageGroup==minRecord.ageGroup
            && oEvent.dateVisite==minRecord.dateVisite);
            let resetDayOne="";
            if(minRecord.dateVisite!="")
            {
             resetDayOne=minRecord.dateVisite.split("-")[0]+"-"+minRecord.dateVisite.split("-")[1]+"-"+"01";
            }
            else
            {
             resetDayOne=minRecord.dateVisite
            }

          listEventMin.push(
            {
              uuid:minEvents.uuid,
              ageGroup:minEvents.ageGroup,
              sex:minEvents.sex,
              moySyst:minEvents.moySyst,
              dateVisite:resetDayOne
            }
          );
        }
        
        let listEventRecords = alasql('SELECT AVG(moySyst) as moySyst,ageGroup,sex,dateVisite \
        FROM ? \
        GROUP BY ageGroup,sex,dateVisite ',[listEventMin]);
        
        //return res.send(listEventRecords);
        //Fixed digit after comma
        let listEventRecordsGrouped=[];
        for(let event of listEventRecords){
          
          listEventRecordsGrouped.push({
            nb:event.moySyst.toFixed(2),
            ageGroup:event.ageGroup,
            sex:event.sex,
            dateEvent:event.dateVisite
          })
        }
        let metadataConfig={
          dataElementId:metaData.dataElementId,
          ageGroupCode:importConfig.categoryComboCode.ageGroupCode,
          sexCode:importConfig.categoryComboCode.sexCode
        };
        let listEventsChangedSex2Id=customLibrairy.replaceCodeCaterogiesByIdsForSex(importConfig.categoryOptions,
          listEventRecordsGrouped)
        //return res.send(listEventRecordsGrouped);
          let listEventsChangedAgeGroup2Id=customLibrairy.replaceCodeCaterogiesByIdsForAgeGroup(importConfig.categoryOptions,
            listEventsChangedSex2Id)
        
        console.log(`Ready to post ADX payload 2 dhis2`);
        let adxPayLoad=customLibrairy.buildADXPayloadFromNewlyDiagnosedPatient(listEventsChangedAgeGroup2Id,metadataConfig,
          eventQuery.dimensionOrgUnits[0])
        //return res.send(adxPayLoad);
        saveAdxData2Dhis(adxPayLoad,(adxSaveResults)=>{
          console.log(`------------Finished! ADX payload posted--------------------------`);
          res.send(adxSaveResults);
        });
        
        })//end GetEventAnaltytics

    });//end /generate_sbplastvisitechange
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
function generateValueEndPoint(teiNumber,callback){
  let localNeedle = require('needle');
  let localAsync = require('async');
  let dicOperationResults=[];
  let listData=[];
  var resourceData = [];
  localNeedle.defaults(
      {
          open_timeout: 600000
      });
  for(let i=0;i<teiNumber;i++)
  {
    listData.push(i);
  }
  var url= URI(importConfig.dhis2Server.url).segment("trackedEntityAttributes").segment("SMLeL7kXzf4").segment("generate.json");
  url = url.toString();
  let options={headers:{'Content-Type': 'application/json','Authorization':dhis2Token}};
  let listAlreadyExistedResources=[];
  let counter=0;
  localAsync.eachSeries(listData, function(indexData, nextResource) {
    let compter=1;
    //console.log(metadata);
    setTimeout(function(){
      console.log(`-------------- Wait for ${importConfig.app.timeoutDelay} sec before the next loop=>${counter} | chunks of ${dhisResource} ------------------------`);
      //console.log(`${JSON.stringify(metadata)}`);
      counter++;
      localNeedle.get(url,options,function(err,resp){
        if(err)
        {
            logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
                        message:`${err.Error}`});
            nextResource(err);
        }
        if (resp.statusCode && (resp.statusCode < 200 || resp.statusCode > 399)) {
          logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                        message:`Code d'erreur http: ${resp.statusCode}`});
                        nextResource(err);
        }
        var body = resp.body;
        /*console.log(`---- Body ------------`);
        console.log(body);*/
        if(body)
        {
          resourceData.push({
            generaredUuid:body.value
          })
        }

        nextResource();
        
      });//end localNeedle
    },0);

    
  },(err)=>{
    if(err)
    {
      logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${dhisResource}`,result:typeResult.failed,
      message:`${err}`});
    }
    callback(resourceData);
    
  });//end localAsync
  

}
function getListOrgUnitByNames(listOrgUnitsName,callback){
  let localNeedle = require('needle');
  let localAsync = require('async');
  let dicOperationResults=[];
  let listData=[];
  var resourceData = [];
  localNeedle.defaults(
      {
          open_timeout: 600000
      });
  var url="";
  let options={headers:{'Content-Type': 'application/json','Authorization':dhis2Token}};
  let counter=0;
  
  localAsync.eachSeries(listOrgUnitsName, function(orgUnitName, nextResource) {
    let compter=1;
    //console.log(metadata);
    url= URI(importConfig.dhis2Server.url).segment(`${dhisResource.orgUnit}.json`);
    url.addQuery("query",orgUnitName);
    url.addQuery("fields",'id,name');
    url.addQuery("paging",false);
    url = url.toString();
    console.log(`url=> ${url}`);
    console.log(`-------------- Wait for ${importConfig.app.timeoutDelay} sec before the next loop=>${counter} | chunks of ${dhisResource} ------------------------`);
      //console.log(`${JSON.stringify(metadata)}`);
    counter++;
    localNeedle.get(url,options,function(err,resp){
      if(err)
      {
          logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
                      message:`${err.Error}`});
          nextResource(err);  
      }
      if (resp.statusCode && (resp.statusCode < 200 || resp.statusCode > 399)) {
        logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                      message:`Code d'erreur http: ${resp.statusCode}`});
                      nextResource(err);
      }
      var body = resp.body;
      /*console.log(`---- Body ------------`);
      console.log(body);*/
      if(body ) 
      {
        resourceData=resourceData.concat(body.organisationUnits)
        //console.log(`${body.organisationUnits}`)
        //nextResource({Error:'this is the error'});
      }

      nextResource();
      //return callback(resourceData);
      
    });//end localNeedle

    
  },(err)=>{
    if(err)
    {
      logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${dhisResource}`,result:typeResult.failed,
      message:`${err}`});
    }
    callback(resourceData);
    
  });//end localAsync
  

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
              message:`Code d'erreur http: ${resp.body}`});
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
            console.log(JSON.stringify(resp.body)); 
            
            
          }
          else{
            logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/${url}`,result:typeResult.failed,
              message:`Code d'erreur http: ${resp.httpStatus}`});
              console.log(JSON.stringify(resp.body)); 
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
      url.addQuery('paging',false);
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
function getEventAnalytics(eventQuery,callbackMain)
{
  let localNeedle = require('needle');
  localNeedle.defaults(
      {
          open_timeout: 600000
      });
  let localAsync = require('async');
  var resourceData = [];
  var url="";
  url= URI(importConfig.dhis2Server.url).segment("analytics").segment("events").segment("query").segment(eventQuery.programId);
  url.addQuery('startDate',eventQuery.startDate);
  url.addQuery('endDate',eventQuery.endDate);
  url.addQuery('stage',eventQuery.stageId);
  let orgUnits="ou:";
  let counterOu=0;
  for(let dimensionOu of eventQuery.dimensionOrgUnits)
  {
    if(counterOu==0)
    {
      orgUnits+=dimensionOu
    }
    else{
      orgUnits+=";"+dimensionOu
    }
    counterOu++;
  }

  url.addQuery('dimension',orgUnits);
  for(let dimensionId of eventQuery.dimensionIds)
  {
    url.addQuery('dimension',dimensionId);
  }
  url.addQuery('skipMeta',true);
  url.addQuery('paging',false);

  url = url.toString();
  console.log(`eventAnalytics => ${url}`);
  //return callbackMain([]);
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
              console.log("Body returned!!!!");
              if (!body.rows) {
        logger.log({level:levelType.error,operationType:typeOperation.getData,action:`/${url}`,result:typeResult.failed,
                      message:`Ressources invalid retournees par DHIS2`});
                  return callback(true, false);
              }
              url=false;
              if (body.rows && body.rows.length > 0) {
                
                resourceData = resourceData.concat(body.rows);
                  //force return only one loop data
                  //return callback(true, false);
              }
              return callback(true, false);
              

              
          })//end of needle.get
            
      },//end callback 2
      err=>{
          return callbackMain(resourceData);

      }
  );//end of async.whilst

}
function saveAdxData2Dhis(adxPayload,callback){
  let localNeedle = require('needle');
  var parseString = require('xml2js').parseString;
  let dicOperationResults=[];
  localNeedle.defaults(
      {
          open_timeout: 600000
      });
  var url= URI(importConfig.dhis2Server.url).segment("dataValueSets");
  url.addQuery("dataElementIdScheme","UID");
  url.addQuery("orgUnitIdScheme","UID");
  url = url.toString();
  //console.log(`Adx url=> ${url}`);
  let options={headers:{'Content-Type': 'application/adx+xml','Authorization':dhis2Token}};
  localNeedle.post(url,adxPayload,options,function(err,resp){
    if(err)
    {
        logger.log({level:levelType.error,operationType:typeOperation.postData,action:`/dataValueSets`,result:typeResult.failed,
                    message:`${err.Error}`});
        callback({});
    }
    callback(resp.body);
    
  });//end localNeedle
  

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
