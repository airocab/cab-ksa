var mysql   = require('mysql');
var AWS     = require('aws-sdk');
var https   = require('https');
var config  = {};
var CONNECTION; 
var sns;
var mongojs     = require('mongojs')
var db = mongojs(process.env.MONGODB_CONNECTION);

config.GOOGLEKEY            = 'AIzaSyDsF5M5q0AWpHc4rgBBJ_w_rEr3ysDCGGM';
config.SNS_accessKeyId      = 'AKIAJBQG3Y325KV45M3Q';
config.SNS_secretAccessKey  = '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';
config.SNS_REGION           = 'us-west-2';
config.TIME_ONLINE           = (60000*10);
config.DB                   =  {}
config.DB.host              =  '72.55.138.39';
config.DB.user              =  'cabserve_UseR';
config.DB.password          =  'k[#F=al.IM2@';
config.DB.database          =  'cabserve_DB';
config.ELM_API              =  'EE5F22B0-7706-4197-A84B-90E446A5F086';
config.PassengerAlertBefore =   (60000*15)
config.LaterTripDriver      =   (60000*15)
AWS.config.update({accessKeyId: config.SNS_accessKeyId, secretAccessKey:config.SNS_secretAccessKey,region:config.SNS_REGION});
sns = new AWS.SNS();
config.SendPush=function(endPoint,title,message,trip)
{
    var type='new request'
    var APN_msg=message.substring(0,90)
    //if(trip.tripStatus == 2 )type='assigned'
    if(trip.tripStatus == 3 )type='accepted'
    else if(trip.tripStatus == 4 )type='arrived'
    else if(trip.tripStatus == 5 )type='pickedup'
    else if(trip.tripStatus == 6 )type='dropoff'
    else if(trip.tripStatus == 7 )type='canceled'
    var Sound = (type=='new request')?'new_request':type;
    var container = {
        default : message,
        GCM:{
            data: {
                type:type,
                title: title,
                message: message,
                soundname:Sound,
                trip:trip
            }
        },
        APNS:
        {
            aps : { 
                type:type,
                alert:APN_msg,
                sound:'default',
                badge:1,
                trip:JSON.stringify({
                    driverFName:trip.driverFName,
                    driverLName:trip.driverLName,
                    driverRate:trip.driverRate,
                    brandName:trip.brandName,
                    modelTitle:trip.modelTitle,
                    carYear:trip.carYear,
                    driverMobile:trip.driverMobile,
                    tripStatus:trip.tripStatus,
                    tripId:trip.tripId,
                    tripTime:trip.tripTime,
                    tripKm:trip.tripKm,
                    tripCost:trip.tripCost,
                    payBy:trip.payBy,
                    offerId:trip.offerId,
                    offerMaxValue:trip.offerMaxValue
                })
            }
        },
        APNS_SANDBOX:
        {
            aps : { 
                type:type,
                alert:APN_msg,
                sound:'default',
                badge:1,
                trip:{
                    driverFName:trip.driverFName,
                    driverLName:trip.driverLName,
                    driverRate:trip.driverRate,
                    brandName:trip.brandName,
                    modelTitle:trip.modelTitle,
                    carYear:trip.carYear,
                    driverMobile:trip.driverMobile,
                    tripStatus:trip.tripStatus,
                    tripId:trip.tripId,
                    tripTime:trip.tripTime,
                    tripKm:trip.tripKm,
                    tripCost:trip.tripCost,
                    payBy:trip.payBy,
                    offerId:trip.offerId,
                    offerMaxValue:trip.offerMaxValue
                }
            }
        }
    };   
    container.GCM = JSON.stringify(container.GCM);
    container.APNS = JSON.stringify(container.APNS);
    container.APNS_SANDBOX = JSON.stringify(container.APNS_SANDBOX);
    sns.publish({TargetArn: endPoint,MessageStructure: 'json',Message: JSON.stringify(container)},function(err,data){ });
}
config.Connect=function(multipleStatements)
{
    var Options = config.DB
    Options.multipleStatements = (multipleStatements)?true:false;
    var ConObj = mysql.createConnection(Options);
    ConObj.connect();
    return ConObj;
}
config.format=function(sql,Obj)
{
    return mysql.format(sql,Obj);
}
config.GetDate = function(Type){
    var currentDate = new Date();
    if(Type=='tomorrow')currentDate = new Date(currentDate.getTime() + 24 * 60 * 60 * 1000);
    var day = currentDate.getDate()
    var month = currentDate.getMonth() + 1
    var year = currentDate.getFullYear()
    if(Type=='tomorrow' || Type=='today')
        return year+"-"+month+"-"+day;
    else if(Type=='now')
        return new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '')
}
/* Queries */
config.Query = {};
config.Query.TripCounts=function(req)
{
    var Tomorrow = config.GetDate('tomorrow');
    var Today = config.GetDate('today');
    var Now = config.GetDate('now');
    var Network = (req.query.networkId >0)?' AND tripNetwork='+req.query.networkId:'';
    var Query = "SELECT count(*) as total,DATE(tripDueDate) as TripDate FROM trips "+
    "WHERE tripNow=0 AND (tripDueDate>='"+Now+"' "+Network+" AND tripDueDate<='"+Tomorrow+" 23:59:59') group BY TripDate ORDER BY TripDate ASC; "+
    "SELECT count(*) as total FROM trips WHERE tripStatus=6 "+Network+" AND DATE(tripAcceptDate)= '"+Today+"'; "+
    "SELECT count(*) as total FROM trips WHERE tripStatus>=3 "+Network+" AND tripStatus<=5 AND DATE(tripAcceptDate)= '"+Today+"';"
    return Query;
}
config.Query.NearBy=function(req)
{
    var coords = [];
    var Query = {};
    var NewDate =new Date(new Date().getTime()-config.TIME_ONLINE).toISOString();
    var limit = parseInt(req.query.limit) || 10;
    var maxDistance = req.query.distance || 20;
    maxDistance = maxDistance * 1000;
    coords[0] = parseFloat(req.query.longitude);
    coords[1] = parseFloat(req.query.latitude)
    Query.when = {'$gte':new Date(NewDate)}
    if(req.query.payBy)
    {
        if(req.query.payBy=='visa')Query.driver_paymethod = {'$ne':1}
        else if(req.query.payBy=='cash')
        {
            Query.driver_paymethod = {'$ne':2}
            Query.driverCredit = {'$gt':0}
        }
    }
    Query.driverAvailable=1;
    if(!req.query.AllDrivers || req.query.AllDrivers=='free')
    {
        Query.tripId =0
    }
    if(req.query.AllDrivers && req.query.AllDrivers=='busy')
    {
        Query.tripId ={'$ne':0}
    }
    if(req.query.levelId)
    {
        Query.levelId=parseInt(req.query.levelId);
    }
    if(req.query.NotDriver)
    {
        Query.driverId={'$ne':parseInt(req.query.NotDriver)};
    }
    if(req.query.networkId && parseInt(req.query.networkId)>0)
    {
        Query.networkId=parseInt(req.query.networkId);;
    }
    var Return =  {
        "spherical": true,
        "near": {"type":"Point","coordinates":coords},
        "minDistance" : 0,
        "distanceField":"dis",
        "distanceMultiplier": 0.001,
        "num":limit,
        "maxDistance" : maxDistance,
        query:Query
    }
    return Return;
}
config.isNumber = function (n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}
config.Query.DriverObj=function(Obj,Type)
{
    if(Type=='insert')
    {
        Obj.tripId = 0
        Obj.tripStatus = 0
    }
    Obj.when=new Date();
    Obj.geo={"type": "Point","coordinates": [0,0]}
    Object.keys(Obj).forEach(function(key){
        if(key.indexOf('Id') > -1 || key=='tripStatus' || key=='driver_paymethod' || key =='driverAvailable')Obj[key] = parseInt(Obj[key]);
        else if(key=='driverCredit')Obj[key] = parseFloat(Obj[key]);
        else if(key=='longitude' || key=='latitude')
        {
            if(key=='longitude')Obj.geo.coordinates[0]=parseFloat(Obj.longitude)
            else Obj.geo.coordinates[1]=parseFloat(Obj.latitude)
            delete Obj[key];
        }
    });
    return Obj;
}
config.Query.EstimateFare=function(req,res)
{
    var perKm = parseFloat(req.query.perKm);
    var perMin = parseFloat(req.query.perMin) ;
    var tripTime = parseFloat(req.query.tripTime) ;
    var minCost = parseFloat(req.query.minCost);
    var costPerKm;
    var costPerMin = tripTime * perMin;
    var cost;
    var tripKm;
    var options = {
        host: 'maps.googleapis.com',
        port: '443',
        path: '/maps/api/distancematrix/json?key='+config.GOOGLEKEY+'&mode=driving&language=lang&sensor=false&origins='+req.query.from + '&destinations='+req.query.to,
        method: 'GET',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
    };
    https.request(options, function(res2) {
        res2.setEncoding('utf8');
        res2.on('data', function (chunk) {
            chunk = JSON.parse(chunk);
            if(chunk.rows && chunk.rows.length && chunk.rows[0] && 
                chunk.rows[0].elements && chunk.rows[0].elements.length && chunk.rows[0].elements[0] 
                && chunk.rows[0].elements[0].distance && chunk.rows[0].elements[0].distance.value)
                {
                    tripKm = chunk.rows[0].elements[0].distance.value/ 1000
                    costPerKm = (chunk.rows[0].elements[0].distance.value * perKm) / 1000
                }
                else
                {
                    tripKm = -1
                    costPerKm = (1 * perKm) / 1000
                }
                cost = costPerKm +costPerMin
                if(minCost > cost)cost = minCost
                cost = cost.toFixed(2);
                costPerMin = costPerMin.toFixed(2);
                costPerKm = costPerKm.toFixed(2);
                res.json({
                    cost:cost,
                    costPerKm:costPerKm,
                    costPerMin:costPerMin,
                    tripTime:tripTime.toFixed(2),
                    tripKm:tripKm,
                });
        });
    }).end();
}

config.Query.CalCost=function(results,req,res)
{
    var t1 = new Date(results[0].tripPickUpdate);
    var t2 = new Date(results[0].tripFinishDate);
    var diffMs = (t2 - t1);
    var diffMins = (diffMs / 60000).toFixed(2); // minutes
    var options = {
        host: 'safecabapi.mybluemix.net',
        port: '443',
        path: '/api/estimateFare?from='+results[0].tripRealFrom.lat+','+results[0].tripRealFrom.lng
            +'&to='+results[0].tripRealDropoff.lat+','+results[0].tripRealDropoff.lng
            +'&perKm='+results[0].tripPerKm+'&perMin='+results[0].tripPerMinute+'&minCost='+results[0].tripMinCost+'&tripTime='+diffMins,
        method: 'GET',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
    };
    https.request(options, function(res2) {
        res2.setEncoding('utf8');
        res2.on('data', function (chunk) {
            chunk = JSON.parse(chunk);
            var connection2 = config.Connect(false);
            connection2.query('UPDATE trips set ? where tripId='+parseInt(req.params.tripId),{
                tripCost:chunk.cost,
                tripTime:chunk.tripTime,
                tripKm:chunk.tripKm
            }, function(err, result) {
                results[0].tripCost=chunk.cost;
                results[0].tripTime=chunk.tripTime;
                results[0].tripKm=chunk.tripKm;
                if(req.body.DontSendNot)
                {
                    //res.json(results[0]);
                } 
                else
                {
                    if(results[0].driverEndPoint)
                        config.SendPush(results[0].driverEndPoint,'Update Request','Your Trip Was Updated',results[0]);
                    if(results[0].passengerEndPoint)
                        config.SendPush(results[0].passengerEndPoint,'Update Request','Your Trip Was Updated',results[0]);
                }
                res.json(results[0]);
            });
            connection2.end(function(err) {});
        });
    }).end();
}
config.TripPutRequest = function(req, res) {
    var SELECT = 'SELECT * FROM trips as tr '+
            'LEFT JOIN drivers as dr ON dr.driverId=tr.tripDriverId '+
            'LEFT JOIN passengers as ps ON ps.passengerId=tr.tripPassengerId '+
            'LEFT JOIN brands as br ON br.brandId=dr.brandId '+
            'LEFT JOIN models as mo ON mo.modelId=dr.modelId '+
            'LEFT JOIN levels as lv ON lv.levelId=dr.levelId '+
            'where tr.tripId='+req.body.tripId
    var connection0 = config.Connect(false);
    connection0.query(SELECT, function(err, results) {
        if (err) throw err;
        if(results[0].tripStatus ==1 || results[0].tripStatus ==2)
        {
            results[0].tripFrom = JSON.parse(results[0].tripFrom);
            results[0].tripTo = JSON.parse(results[0].tripTo);
            var geoNear = config.Query.NearBy({
                query:{
                    longitude:results[0].tripFrom.lng,
                    latitude:results[0].tripFrom.lat,
                    payBy:results[0].payBy,
                    levelId:results[0].tripLevelId,
                    NotDriver:(results[0].tripDriverId)?results[0].tripDriverId:0
                }
            })
            var Drivers = db.collection('drivers')
            Drivers.aggregate([{$geoNear: geoNear}], function (err, docs) {
                if (err && res) res.send(err);
                console.log('docs.err : ',err,' | docs.length : ',docs.length,' | geoNear : ',JSON.stringify(geoNear));
                if(docs && docs.length && docs[0] && docs[0].endPoint)
                {
                    results[0].tripDriverId = docs[0].driverId
                    results[0].driverEndPoint=docs[0].endPoint
                    results[0].tripNetwork=docs[0].networkId
                    var UPDATE = 'UPDATE trips set tripDriverId='+docs[0].driverId+',driverEndPoint="'+docs[0].endPoint+'" where tripId='+parseInt(req.body.tripId);
                    var connection2 = config.Connect(false);
                    connection2.query(UPDATE, function(err, results) {});
                    connection2.end(function(err) {});
                    config.SendPush(docs[0].endPoint,'New Request',results[0].tripFromAddress,results[0]);
                    if(res)res.json(results[0]);
                }
                else
                {
                    if(res)res.json(results[0]);
                    else
                    {
                        var UPDATE = 'UPDATE trips set  driverEndPoint="",tripFailedToAssign=1,tripStatus=7 where tripId='+parseInt(req.body.tripId);
                        var connection2 = config.Connect(false);
                        connection2.query(UPDATE, function(err, results) {});
                        connection2.end(function(err) {});
                    }
                }
            });
        }
    });
    connection0.end(function(err) {});
}
module.exports = config;