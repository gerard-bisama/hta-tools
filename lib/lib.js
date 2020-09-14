const path = require('path');
const fs = require('fs');
const csv=require('csvtojson');
const moment = require('moment');
const url=require('url');
var csvHeaderProfilePEC=[]
var csvHeaderProfilePEC=csvHeaderProfilePEC.concat(['OrgUnitID','num_dossier','TEI','Sexe' ,'Age',
'Date_diagnostic HTA','Unite_de_traitement','Provenance','Date_visite','PA _Bras_Gauche_Dias',
'PA _Bras_Gauche_Syst','MOY_SYSTO','PA _Bras_Droit_Syst','PA _Bras_Droit_Dias','Niveau_RCV','Traitement',
'Date_prochain_RDV','Statut_patient']);
const profilePECCsvConverter={
    noheader:false,
    trim:true,
    headers:csvHeaderProfilePEC
};

exports.readCSVProfilePECFile=function readCSVProfilePECFile(filePath,callback)
{
    var fileRecords=[];
    csv(profilePECCsvConverter).fromFile(filePath).then((jsonObj)=>{
        fileRecords=fileRecords.concat(jsonObj);
        callback(fileRecords);
    });
}
exports.buildTEI=function buildTEI(fileData,trackedEntityTypeId,ageGroupRange,programId){
    //console.log(ageGroupRange);
    let createdTEI=[];
    for(let record of fileData)
	{
        if(record.num_dossier=="" && record.Sexe=="")
        {
            continue;
        }
        var  optionsForSexeMasc=['masculin','m'];
        var optionsForSexeFem=['feminin','féminin','f'];
        var sexe;
        let createdDate=getValidDate(record['Date_diagnostic HTA']);
        if(optionsForSexeMasc.includes(record.Sexe.toLowerCase().trim())){
            sexe='M';
        }
        if(optionsForSexeFem.includes(record.Sexe.toLowerCase().trim())){
            sexe='F';
        }
        //Generate age group now
        let generateAgeGroup="";
        for(let ageGroup of ageGroupRange){
            let limitMax=ageGroup.interval[1];
            let limitMin=ageGroup.interval[0];
            if(record.Age >= limitMin && record.Age<= limitMax)
            {
                generateAgeGroup=ageGroup.value;
                break;
            }
        }
        //get DateOfBirth Estimation from the age
        let ageInMilliseconds=record.Age*365*24*60*60*1000;
        let dateOfBirth=new Date(new Date('2020-01-01').getTime()-ageInMilliseconds);
        let tei={
            //trackedEntity:record.TEI,
            created:createdDate,
            trackedEntityInstance:record.TEI,
            orgUnit:record.OrgUnitID,
            trackedEntityType:trackedEntityTypeId,
            attributes:[
                {
                    attribute:"xCB53k0Rb41",
                    value:record.num_dossier
                },
                {
                    attribute:"iYMDdwJ0Kzk",
                    value:sexe
                },
                {
                    attribute:"PGvhNwKGKkH",
                    value:dateOfBirth.toISOString().split("T")[0]
                },
                
                {
                    attribute:"pleUVP7m8LX",
                    value:record.Age
                },
                {
                    attribute:"vjNskFa2nwh",
                    value:generateAgeGroup
                }

            ],
            enrollments:[
                {
                    orgUnit:record.OrgUnitID,
                    program:programId,
                    enrollmentDate:createdDate,
                    incidentDate:createdDate,
                    status:"ACTIVE"
                }
            ]
        };
        createdTEI.push(tei);
    }
    return createdTEI;
}
exports.buildProvenanceEvents=function buildProvenanceEvents(fileData,programId,programStageId,patientReferenceOptionSets)
{
    let createdEvents=[];
    for(let record of fileData)
	{
        let createdDate=getValidDate(record['Date_diagnostic HTA']);
        let oEvent={
            program: programId,
            orgUnit: record.OrgUnitID,
            eventDate: createdDate,
            trackedEntityInstance:record.TEI,
            programStage:programStageId,
            //status: "ACTIVE",
            status:"COMPLETED",
            dataValues:[
                { dataElement: "JHdsvWnBIXG", value: record['Unite_de_traitement'] },
                { dataElement: "WfCKF3dicir", value: createdDate.toISOString().split("T")[0] },
                //{ dataElement: "zZciiFrfmpg", value: createdDate.toISOString().split("T")[0] }
            ]
        };
        
        if(record['Provenance']!=""){
            if(record['Provenance'].toLowerCase().replace(/\s+/g, '').includes("venudelui")||record['Provenance'].toLowerCase().replace(/\s+/g, '').includes("venuedelui"))
            {
                oEvent.dataValues.push( { dataElement: "WMukKGoo8zm", value: true });
            }
            else{
                oEvent.dataValues.push( { dataElement: "WMukKGoo8zm", value: false });
                //console.log(patientReferenceOptionSets);
                var referePar=patientReferenceOptionSets.find(reference=>
                    record['Provenance'].toLowerCase().includes(reference.code));
                if(referePar)
                {
                    oEvent.dataValues.push( { dataElement: "okLzPWiQlXN", value: referePar.value });
                }
                oEvent.dataValues.push({ dataElement: "zZciiFrfmpg", value: createdDate.toISOString().split("T")[0] });

            }
        }
        createdEvents.push(oEvent);
    }
    return createdEvents;
}
exports.buildSuiviEvents=function buildSuiviEvents(fileData,programId,programStageId,patientReferenceOptionSets)
{
    let createdEvents=[];
    for(let record of fileData)
	{
        let createdDate=getValidDate(record['Date_visite']);
        let oEvent={
            program: programId,
            orgUnit: record.OrgUnitID,
            eventDate: createdDate,
            trackedEntityInstance:record.TEI,
            programStage:programStageId,
            status: "ACTIVE",
            dataValues:[
                { dataElement: "JHdsvWnBIXG", value: record['Unite_de_traitement'] },
                { dataElement: "WfCKF3dicir", value: createdDate.toISOString().split("T")[0] },
                //{ dataElement: "zZciiFrfmpg", value: createdDate.toISOString().split("T")[0] }
            ]
        };
        
        if(record['Provenance']!=""){
            if(record['Provenance'].toLowerCase().replace(/\s+/g, '').includes("venudelui")||record['Provenance'].toLowerCase().replace(/\s+/g, '').includes("venuedelui"))
            {
                oEvent.dataValues.push( { dataElement: "WMukKGoo8zm", value: true });
            }
            else{
                oEvent.dataValues.push( { dataElement: "WMukKGoo8zm", value: false });
                //console.log(patientReferenceOptionSets);
                var referePar=patientReferenceOptionSets.find(reference=>
                    record['Provenance'].toLowerCase().includes(reference.code));
                if(referePar)
                {
                    oEvent.dataValues.push( { dataElement: "okLzPWiQlXN", value: referePar.value });
                }
                oEvent.dataValues.push({ dataElement: "zZciiFrfmpg", value: createdDate.toISOString().split("T")[0] });

            }
        }
        createdEvents.push(oEvent);
    }
    return createdEvents;
}
exports.buildTEIEnrollment=function buildTEIEnrollment(fileData,programId){
    let createdEnrollment=[];
    for(let record of fileData)
	{
        
        createdDate=getValidDate(record['Date_diagnostic HTA']);
        let enrollement={
            trackedEntityInstance:record.TEI,
            orgUnit:record.OrgUnitID,
            program:programId,
            enrollmentDate:createdDate,
            incidentDate:createdDate,
            status:"ACTIVE"
        };
        createdEnrollment.push(enrollement);
    }
    return createdEnrollment;
}
function getValidDate(dateString)
{
    if(dateString.includes("-")&& dateString.split("-").length==3)
    {
        return new Date(dateString);
    }
    else if(dateString.includes("/")&& dateString.split("/").length==3)
    {
        return new Date(dateString);
    }
    else{
        return new Date();
    }
    
}   

exports.getValidDate=getValidDate;