# App to load CSV data in the the DHIS2 Tracker and also used to generate aggregated data from individual case data
This app is used to import data from CSV to tracker and to compute aggregated data from tracked entities and related events. This is not a generic app that can be used to any DHIS2 instance but it may required it hard code the formula of indicator in the source code.
The program is composed of one non repetitive envent 'provenancePatient' and other repetitive events.

## Prerequisites and installation
Before you run this app, you will need to install nodejs >=v10.13.0.
After the clone of the source, install the package with ```npm install```

## Configuration
### The app section
```
"app":{
        "port":"8001",
        "appDirectory":"/yourpath/hta_utility_tools",
        "dataFilePath":"/yourpath/data/hta_december_update1.csv",
        "teiInsertBatchSize":50,
        "programId":"eTkUEzlMDZY",
        "trackedEntityTypeId":"nN29POeOxvv",
        "programStages":{
            "provenancePatient":"UD8R4GcuOsO",
            "suiviPatient":"xVMoiMMEaqj"
        },
        "orgUnits":{
            "isParent":true,
            "pageSizeToPull":50,
            "OrgUnit":"dist0000102"
        },
        "timeoutDelay":0,
        "timeoutDelayUpdateTEI":0,
        "syncFirstPeriod":false,
        "value2Generate":0
    }
```
* appDirectory: the directory when app is installed. Used for loggings
* dataFilePath: the full path of datafile used to import tracked entities and related events
* teiInsertBatchSize: size of bundle to insert per request. Default to 50
* programId: Id of the program
* trackedEntityTypeId: Id of the type of the entity
* programStages: programStages label and corresponding ID. 
* orgUnits: Org unit ID to pull the events data to generate aggretated entities. ```isParent=true``` means that all the child of the current orgUnit will be used. ```pageSizeToPull``` number of events to pull per request, default to 50. ```OrgUnit``` is the orgUnit Id used to pull TEI and events.
* syncFirstPeriod: ```True``` if we want to import tracked attributes and provenance patient, and ```false``` if we want to sync only the event 'SuiviPatient'. 
The file to import must contain entity attributes and one event (either Provenance related data or Suivi de patient).The structure of the CSV file to import will contain the following columns:
```
orgunitid,num_dossier,TEI,sexe,age,date_diagnostic_hta,unite_de_traitement,provenance,date_visite,pa_bras_gauche_dias,pa_bras_gauche_syst,moy_systo,pa_bras_droit_syst,pa_bras_droit_dias,niveau_rcv,pa_controlee,traitement,suivi_mdh,observance_traitement,date_prochain_rdv,statut_patient.
```
To import TEI and 'Provenance' event, all these field could be provided, this is considered as the initial insert process. Then we can update the tracked entities with suivi event by using the same file structere but this case there is no need to provide the field below, they can remain empty.
```
num_dossier,sexe,age,date_diagnostic_hta,unite_de_traitement,provenance,date_visite
```
* programRulesAgeGroup: this contains the ageGroup defined in the program rules for being generated during the importation as defined in DHIS2. Since this is not subject to change, this is defined in the app to avoid request to DHIS2 
* attributeIds: attribute use for the grouping during the generation of the aggregated data
* patientRefereOptionSet: this contains the patient reference information as defined in DHIS2. Since this is not subject to change, this is defined in the app to avoid request to DHIS2. Same for 'statutPatientOptionSet' and 'categoryComboCode'
* periodDataElementGenerated: Date interval used to generate the indicator.
* dataElements2Generate: This contains the list of indicator to generate defined as metadata. For every indicators the corresponding endpoint is developed and the calculation rules is hardcorded.



