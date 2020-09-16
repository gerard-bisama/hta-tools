const path = require('path');
const fs = require('fs');
const csv=require('csvtojson');
const moment = require('moment');
const url=require('url');
var csvHeaderProfilePEC=[]
var csvHeaderProfilePEC=csvHeaderProfilePEC.concat(['OrgUnitID','num_dossier','TEI','Sexe' ,'Age',
'Date_diagnostic_HTA','Unite_de_traitement','Provenance','Date_visite','PA_Bras_Gauche_Dias',
'PA_Bras_Gauche_Syst','MOY_SYSTO','PA_Bras_Droit_Syst','PA_Bras_Droit_Dias','Niveau_RCV','PA_controlee','Traitement',
'Suivi_MDH','Observance_traitement','Date_prochain_RDV','Statut_patient']);
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
       
        /* if((record.num_dossier && record.num_dossier=="") && (record.Sexe && record.Sexe=="") 
        && (record.Age && record.Age=="")) */
        if(record.num_dossier=="" && record.Sexe=="" && record.Age=="")
        {
            continue;
        }
        var  optionsForSexeMasc=['masculin','m'];
        var optionsForSexeFem=['feminin','féminin','f'];
        var sexe;
        let createdDate;
        let dateDiagnostic;
        if(record['Date_diagnostic_HTA'] && record['Date_diagnostic_HTA']!="")
        {
            dateDiagnostic=record['Date_diagnostic_HTA'];
        }
        else if(record['Date_visite'] && record['Date_visite']!="")
        {
            dateDiagnostic=record['Date_visite'];
        }
        let returnedDate=getValidDate(dateDiagnostic);
        if(returnedDate==null)
        {
            createdDate=new Date();
        }
        else
        {
            createdDate=returnedDate;
        }
        //let createdDate=getValidDate(record['Date_diagnostic HTA']);
        if(record.Sexe && record.Sexe!="")
        {
            if(optionsForSexeMasc.includes(record.Sexe.toLowerCase().trim())){
                sexe='M';
            }
            if(optionsForSexeFem.includes(record.Sexe.toLowerCase().trim())){
                sexe='F';
            }
        }
        
        //Generate age group now
        let generateAgeGroup;
        let dateOfBirth;
        if(record.Age && record.Age!="")
        {
            for(let ageGroup of ageGroupRange){
                let limitMax=ageGroup.interval[1];
                let limitMin=ageGroup.interval[0];
                if(record.Age >= limitMin && record.Age<= limitMax)
                {
                    generateAgeGroup=ageGroup.value;
                    break;
                }
            }
            let ageInMilliseconds=record.Age*365*24*60*60*1000;
            dateOfBirth=new Date(new Date('2020-01-01').getTime()-ageInMilliseconds);
        }
        
        //get DateOfBirth Estimation from the age
       
        
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
                }
                /*, {
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
                } */

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
        if(sexe)
        {
            tei.attributes.push({
                attribute:"iYMDdwJ0Kzk",
                value:sexe
            });
        }
        if(dateOfBirth)
        {
            tei.attributes.push(
                {
                    attribute:"PGvhNwKGKkH",
                    value:dateOfBirth.toISOString().split("T")[0]
                }
            );
            tei.attributes.push({
                attribute:"pleUVP7m8LX",
                value:record.Age
            });
            tei.attributes.push({
                attribute:"vjNskFa2nwh",
                value:generateAgeGroup
            });
        }

        
        createdTEI.push(tei);
    }
    return createdTEI;
}
exports.buildProvenanceEvents=function buildProvenanceEvents(fileData,programId,programStageId,patientReferenceOptionSets)
{
    let createdEvents=[];
    for(let record of fileData)
	{
        let createdDate;
        let dateDiagnostic;
        if(record['Date_diagnostic_HTA'] && record['Date_diagnostic_HTA']!="")
        {
            dateDiagnostic=record['Date_diagnostic_HTA'];
        }
        else if(record['Date_visite'] && record['Date_visite']!="")
        {
            dateDiagnostic=record['Date_visite'];
        }
        let returnedDate=getValidDate(dateDiagnostic);
        if(returnedDate==null)
        {
            createdDate=new Date();
        }
        else
        {
            createdDate=returnedDate;
        }
        //let createdDate=getValidDate(record['Date_diagnostic HTA']);
        /* console.log(`Processed TEI: ${record.TEI}`);
        console.log("###################################"); */
        let oEvent={
            program: programId,
            orgUnit: record.OrgUnitID,
            eventDate: createdDate,
            trackedEntityInstance:record.TEI,
            programStage:programStageId,
            //status: "ACTIVE",
            status:"ACTIVE",
            dataValues:[
                //{ dataElement: "JHdsvWnBIXG", value: record['Unite_de_traitement'] },
                //{ dataElement: "WfCKF3dicir", value: createdDate.toISOString().split("T")[0] },
                //{ dataElement: "zZciiFrfmpg", value: createdDate.toISOString().split("T")[0] }
            ]
        };
        if(record['Unite_de_traitement'] && record['Unite_de_traitement']!="")
        {
            oEvent.dataValues.push({ dataElement: "JHdsvWnBIXG", value: record['Unite_de_traitement'] });
        }
        if(returnedDate!=null)
        {
            oEvent.dataValues.push({ dataElement: "WfCKF3dicir", value: createdDate.toISOString().split("T")[0] });
        }
        if(record['Provenance'] && record['Provenance']!=""){
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
                if(returnedDate!=null)
                {
                    oEvent.dataValues.push({ dataElement: "zZciiFrfmpg", value: createdDate.toISOString().split("T")[0] });
                }
                
            }
        }
        createdEvents.push(oEvent);
    }
    return createdEvents;
}
exports.buildSuiviEvents=function buildSuiviEvents(fileData,programId,programStageId,statutPatientOptionSet)
{
    let createdEvents=[];
    for(let record of fileData)
	{
        let createdDate;
        let dateDiagnostic;
        if(record['Date_visite'] && record['Date_visite']!="")
        {
            dateDiagnostic=record['Date_visite'];
        }
        else if(record['Date_diagnostic_HTA'] && record['Date_diagnostic_HTA']!="")
        {
            dateDiagnostic=record['Date_diagnostic_HTA'];
        }
        let returnedDate=getValidDate(dateDiagnostic);
        if(returnedDate==null)
        {
            continue;
        }
        else{
            createdDate=returnedDate;
        }
        //let createdDate=getValidDate(record['Date_visite']);
        
        let oEvent={
            program: programId,
            orgUnit: record.OrgUnitID,
            eventDate: createdDate,
            trackedEntityInstance:record.TEI,
            programStage:programStageId,
            status: "ACTIVE",
            dataValues:[
                /*{ dataElement: "G9wQG7w9GqF", value: createdDate.toISOString().split("T")[0] },
                { dataElement: "t0fchUhCuPc", value: record['PA_Bras_Gauche_Dias'] },
                { dataElement: "k0l9iAY1P7g", value: record['PA_Bras_Gauche_Syst'] },
                { dataElement: "iU1pq8kluwL", value: record['MOY_SYSTO'] },
                { dataElement: "pdlCDCu9jiC", value: record['PA_Bras_Droit_Dias'] },
                { dataElement: "oxVe6o7Fn3I", value: record['PA_Bras_Droit_Syst'] }*/

            ]
        };
        if(record['PA_Bras_Gauche_Dias'] && record['PA_Bras_Gauche_Dias']!=""){
            oEvent.dataValues.push({ dataElement: "t0fchUhCuPc", value: record['PA_Bras_Gauche_Dias'] });
        }
        if(record['PA_Bras_Gauche_Syst'] && record['PA_Bras_Gauche_Syst']!=""){
            oEvent.dataValues.push({ dataElement: "k0l9iAY1P7g", value: record['PA_Bras_Gauche_Syst'] });
        }
        if(record['MOY_SYSTO'] && record['MOY_SYSTO']!=""){
            oEvent.dataValues.push({ dataElement: "iU1pq8kluwL", value: record['MOY_SYSTO'] });
        }
        if(record['PA_Bras_Droit_Dias'] && record['PA_Bras_Droit_Dias']!=""){
            oEvent.dataValues.push({ dataElement: "pdlCDCu9jiC", value: record['PA_Bras_Droit_Dias'] });
        }
        if(record['PA_Bras_Droit_Syst'] && record['PA_Bras_Droit_Syst']!=""){
            oEvent.dataValues.push({ dataElement: "oxVe6o7Fn3I", value: record['PA_Bras_Droit_Syst'] });
        }

        if(record['PA_controlee'] && record['PA_controlee']!=""){
            let paControlee;
            if(record['PA_controlee'].toLowerCase().trim()=="oui")
            {
                paControlee=true;
            }
            else if(record['PA_controlee'].toLowerCase().trim()=="non")
            {
                paControlee=false;
            }
            oEvent.dataValues.push({ dataElement: "rrKGcWkFGv5", value: paControlee });
        }
        if(record['Traitement'] && record['Traitement']!=""){
            oEvent.dataValues.push({ dataElement: "wDk1IkO7kXQ", value: record['Traitement'] });
        }
        if(record['Suivi_MDH'] && record['Suivi_MDH']!="")
        {
            if(record['Suivi_MDH'].toLowerCase().trim().includes("oui"))
            {
                
                oEvent.dataValues.push({ dataElement: "rd5fO82lcgo", value: "Oui" });
            }
            else if(record['Suivi_MDH'].toLowerCase().trim().includes("non"))
            {
                oEvent.dataValues.push({ dataElement: "rd5fO82lcgo", value: "Non" });
            }
            else if(record['Suivi_MDH'].toLowerCase().trim().includes("parfois"))
            {
                oEvent.dataValues.push({ dataElement: "rd5fO82lcgo", value: "Parfois" });
            }
        }
        if(record['Observance_traitement'] && record['Observance_traitement']!="")
        {
            if(record['Observance_traitement'].toLowerCase().trim().includes("oui"))
            {
                
                oEvent.dataValues.push({ dataElement: "xfJBUxPIKWl", value: "Oui" });
            }
            else if(record['Observance_traitement'].toLowerCase().trim().includes("non"))
            {
                oEvent.dataValues.push({ dataElement: "xfJBUxPIKWl", value: "Non" });
            }
            else if(record['Observance_traitement'].toLowerCase().trim().includes("parfois"))
            {
                oEvent.dataValues.push({ dataElement: "xfJBUxPIKWl", value: "Parfois" });
            }
        }

        if(record['Date_prochain_RDV'] && record['Date_prochain_RDV']!="")
        {
            let dateRDV=getValidDate(record['Date_prochain_RDV']);
            if(dateRDV!=null){
                oEvent.dataValues.push({ dataElement: "fYcYjzdEEM6", value: dateRDV.toISOString().split("T")[0] });
            }
            
        }
        if(record['Statut_patient'] && record['Statut_patient']!="")
        {

            if(record['Statut_patient'].toLowerCase().trim().includes('suivi')){
                var statutPatient=statutPatientOptionSet.find(statut=>statut.code=="suivi");
                oEvent.dataValues.push({ dataElement: "IGrYxqhn6yT", value:statutPatient.value});
            }
            else{
                var statutPatient=statutPatientOptionSet.find(statut=>statut.code=="autre");
                oEvent.dataValues.push({ dataElement: "IGrYxqhn6yT", value:statutPatient.value});
                oEvent.dataValues.push({ dataElement: "wSbntS1AK09", value:record['Statut_patient'].toLowerCase()});
            }
        }
        if(oEvent.dataValues.length==0){
            continue;//skip this event if it does not contains at least one attribute
        }
        if(oEvent.dataValues.length==1 && oEvent.dataValues[0].dataElement=="iU1pq8kluwL" &&  oEvent.dataValues[0].value==0){
            continue;//skip this event if it does contains only MoySyst=0
        }
        oEvent.dataValues.push({ dataElement: "G9wQG7w9GqF", value: createdDate.toISOString().split("T")[0] });
        createdEvents.push(oEvent);
    }
    return createdEvents;
}
exports.buildTEIEnrollment=function buildTEIEnrollment(fileData,programId){
    let createdEnrollment=[];
    for(let record of fileData)
	{
        
        createdDate=getValidDate(record['Date_diagnostic_HTA']);
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
//The standard format in the file should be mm/jj/aaaa
function getValidDate(dateString)
{
    if(dateString)
    {
        if(dateString.includes("-")&& dateString.split("-").length==3)
        {
            let rebuiltDateString="";
            if( parseInt( dateString.split("-")[0])>12)
            {
                rebuiltDateString=`${dateString.split("-")[1]}-${dateString.split("-")[0]}-${dateString.split("-")[2]}`
            }
            else
            {
                rebuiltDateString=dateString;
            }
            return new Date(rebuiltDateString+" GMT");
        }
        else if(dateString.includes("/")&& dateString.split("/").length==3)
        {
            let rebuiltDateString="";
            if( parseInt( dateString.split("/")[0])>12)
            {
                rebuiltDateString=`${dateString.split("/")[1]}/${dateString.split("/")[0]}/${dateString.split("/")[2]}`
            }
            else
            {
                rebuiltDateString=dateString;
            }
            return new Date(rebuiltDateString+" GMT");
        }
        else{
            return null;
        }
    }
    else
    {
        return null;
    }
    
    
}   

exports.getValidDate=getValidDate;