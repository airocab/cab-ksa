var config      = require('./config');
var express     = require('express');
var bodyParser  = require('body-parser');
var morgan      = require('morgan');
var mongojs     = require('mongojs')
var schedule    = require('node-schedule');
var https       = require('https');
var port        = process.env.OPENSHIFT_NODEJS_PORT;
var app         = express();
var connection;

app.use(morgan('dev'));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var db = mongojs(process.env.MONGODB_CONNECTION);
var router = express.Router();
router.use(function(req, res, next) {next();}); 
router.get('/', function(req, res) {res.json({ message: 'Api !' });});
/* TripCounts Start */
router.route('/TripCounts').get(function(req, res) {
    var Query = config.Query.TripCounts(req);
    connection = config.Connect(true);
    connection.query(Query, function(err, results) {
        if (err) res.send(err);
        else res.json(results);
    })
    connection.end(function(err) {});
});
/* counts Start */
router.route('/counts').get(function(req, res) {
    var NewDate = new Date(new Date().getTime()-config.TIME_ONLINE).toISOString();
    var Query = {when:{'$gte':new Date(NewDate)},driverAvailable:1};
    if(req.query.networkId>0)Query.networkId = req.query.networkId
    var Drivers = db.collection('drivers')
    Drivers.find(Query).count(function(BusyErr, busy) {
        if (BusyErr) res.send(BusyErr);
        Query.tripId=0;
        Drivers.find(Query).count(function(FreeErr, free) {
            if (FreeErr) res.send(FreeErr);
            res.json({free:free,busy:(busy-free)});
        });
    });
});
/* updateMarkers Start */
router.route('/updateMarkers').get(function(req, res) {
    var Obj={}
    var Drivers = db.collection('drivers')
    Drivers.find({}, function (err, doc) {
        if (err) res.send(err);
        for(var i =0;i<doc.length;i++)
        {
            var rr = Math.floor(Math.random()*(9-1+1)+1);
            rr = rr*0.0001
            Obj.when=new Date()
            Obj.geo={"type": "Point","coordinates": [doc[i].geo.coordinates[0]+rr,doc[i].geo.coordinates[1]+rr]}
            Drivers.findAndModify({
                query: { driverId: parseInt(doc[i].driverId) },
                update: { $set: Obj },
                new: true
            }, function (err, doc2, lastErrorObject) {
                
            })
        }
        res.json(doc);
    })
});
/* nearby Start */
router.route('/nearby').get(function(req, res) {
    var geoNear = config.Query.NearBy(req);
    var Drivers = db.collection('drivers')
    Drivers.aggregate([{$geoNear: geoNear}], function (err, docs) {
        if (err) res.send(err);
        res.json(docs);
    });
});
/* Drivers Start */
router.route('/drivers').post(function(req, res) {
    var Obj = config.Query.DriverObj(req.body,'insert');
    var Drivers = db.collection('drivers')
    Drivers.save(Obj, function (err, doc) {
        if (err) res.send(err);
        res.json(doc);
    })
}).get(function(req, res) {
    var Drivers = db.collection('drivers')
    Drivers.find({}, function (err, docs) {if (err) res.send(err);res.json(docs);});
});
router.route('/drivers/:driverId').get(function(req, res) {
    var Drivers = db.collection('drivers')
    Drivers.find({driverId: parseInt(req.params.driverId)}, function (err, doc) {
        if (err) res.send(err);
        res.json(doc);
    })
})
.delete(function(req, res) {
    var Drivers = db.collection('drivers')
    Drivers.remove({ driverId: parseInt(req.params.driverId) },function(err, doc){if (err) res.send(err);res.json(doc);})
})
.put(function(req, res) {// update the bear with this id
    var Obj = config.Query.DriverObj(req.body,'update');
    var Drivers = db.collection('drivers')
    Drivers.findAndModify({
        query: { driverId: parseInt(req.params.driverId) },
        update: { $set: Obj },
        new: true
    }, function (err, doc, lastErrorObject) {
            if (err) res.send(err);
            res.json(doc);
    })
});
/* Location Start */
router.route('/locations').post(function(req, res) {
    db = mongojs(process.env.MONGODB_CONNECTION);
    var DR = db.collection('drivers');
    var Obj={
        "latitude": parseFloat(req.body.latitude),
        "longitude": parseFloat(req.body.longitude),
        "driverId": parseInt(req.body.driverId),
        when:new Date()
    }
    if(config.ELM_API)
    {
        DR.find({driverId: parseInt(req.body.driverId)}, function (err, doc) {
            if(doc && doc.length && doc[0] && doc[0].vehicleReferenceNumber)
            {
                Obj.vehicleReferenceNumber=doc[0].vehicleReferenceNumber
                Obj.captainReferenceNumber=doc[0].captainReferenceNumber
                var data = JSON.stringify({
                    apiKey:config.ELM_API,
                    vehicleReferenceNumber:doc[0].vehicleReferenceNumber,
                    currentLatitude:parseFloat(req.body.latitude),
                    currentLongitude:parseFloat(req.body.longitude),
                    hasCustomer:(req.body.tripId)?1:0
                });
                var ELM = {host: 'wasl.elm.sa',port: 443,method: 'POST',
                    path: '/WaslPortalWeb/rest/LocationRegistration/send',
                    headers: {
                        'Content-Type': 'application/json; charset=utf-8',
                        'Content-Length': data.length
                    }
                } 
                var Elm_Req = https.request(ELM, function(res) {
                    var msg = '';
                    res.setEncoding('utf8');
                    res.on('data', function (chunk) {console.log('LocationRegistration',chunk);});
                })
                Elm_Req.write(data);
                Elm_Req.end();
            }
        });
    }
    if(req.body.tripId)Obj.tripId = req.body.tripId
    var Locations = db.collection('locations')
    Locations.save(Obj, function (err, doc) {
        var Obj = config.Query.DriverObj(req.body,'update');
        var Drivers = db.collection('drivers')
        Drivers.findAndModify({
            query: { driverId: parseInt(req.body.driverId) },
            update: { $set: Obj },
            new: true
        }, function (err, doc, lastErrorObject) {
            if (err) res.send(err);
            res.json(doc);
        })
    })
})
.get(function(req, res) {
    var Locations = db.collection('locations')
    Locations.find().limit(100).sort({when:-1},function(err, locations) {
        if (err) res.send(err);
        res.json(locations);
    });
});
router.route('/locations/:Column/:Value').get(function(req, res) {
    var Where = {}
    Where[req.params.Column]=parseInt(req.params.Value)
    var Locations = db.collection('locations')
    Locations.find(Where).limit(100).sort({when:-1}, function(err, location) {
        if (err) res.send(err);
        res.json(location);
    });
})
 /* Dispatcher Start */
router.route('/trip').post(function(req, res) {
    var TripObj = req.body.trip; 
    var PassengerObj = req.body.passenger
    if(typeof TripObj.from !== 'object')TripObj.from = JSON.parse(TripObj.from);
    if(parseInt(TripObj.tripNow)==1)
    {
        var geoNear = config.Query.NearBy({
            query:{
                longitude:(TripObj.from.location)?TripObj.from.location.lng:0.0,
                latitude:(TripObj.from.location)?TripObj.from.location.lat:0.0,
                payBy:TripObj.payBy,
                levelId:TripObj.tripLevelId,
                networkId:(req.body.networkId)?req.body.networkId:false
            }
        })
        //
        var Drivers = db.collection('drivers')
        Drivers.aggregate([{$geoNear: geoNear}], function (err, docs) {
            if (err) res.send(err);
            console.log('docs.err : ',err,' | docs.length : ',docs.length,' | geoNear : ',JSON.stringify(geoNear));
            var tripDriversQueue=[];
            if(docs.length)
            {
                for(var i =0;i<docs.length;i++)
                {
                    tripDriversQueue.push(docs[i].driverId);
                }
            }
            var DbObj  = {
                tripCreateDate: new Date()
                ,tripFrom: JSON.stringify(TripObj.from.location)
                ,tripTo: JSON.stringify(TripObj.to.location) 
                ,tripCountryId:TripObj.tripCountryId 
                ,tripCityId:(docs && docs.length && docs[0].driverCityId)?docs[0].driverCityId:TripObj.tripCityId 
                ,tripPackageId:TripObj.tripPackageId
                ,tripNow:TripObj.tripNow
                ,tripPassengerId:PassengerObj.passengerId
                ,passengerEndPoint:PassengerObj.endPoint
                ,tripNote:TripObj.tripNote
                ,tripFromAddress:TripObj.from.address
                ,tripToAdress:TripObj.to.address
                ,tripType:TripObj.tripType
                ,tripLevelId:TripObj.tripLevelId
                ,payBy:TripObj.payBy
                ,tripPerKm:TripObj.tripPerKm
                ,tripPerMinute:TripObj.tripPerMinute
                ,tripMinCost:TripObj.tripMinCost
                ,tripStart:TripObj.tripStart
                ,tripWaitingPerHour:TripObj.tripWaitingPerHour
                ,tripDriversQueue:tripDriversQueue.join(',')
                ,tripDriverId:(docs.length)?docs[0].driverId:0
                ,tripFailedToAssign:(docs.length)?0:1
                ,trpCancelReasonId:(docs.length)?'':-1
                ,driverEndPoint:(docs.length)?docs[0].endPoint:''
                ,tripNetwork:(docs.length)?docs[0].networkId:0
            };
            if(TripObj.tripSource)DbObj.tripSource=TripObj.tripSource
            if(TripObj.offerId)DbObj.offerId=TripObj.offerId
            if(TripObj.offerMaxValue)DbObj.offerMaxValue=TripObj.offerMaxValue
            var Prices = config.format('SELECT * FROM prices WHERE ?',{
                typeId:TripObj.tripType,
                levelId:TripObj.tripLevelId,
                cityId:(docs.length)?docs[0].driverCityId:TripObj.tripCityId,
                packageId:(TripObj.tripPackageId)?TripObj.tripPackageId:1
            });
            Prices = Prices.split(',').join(' and ')
            var SELECT = 'SELECT * FROM trips as tr '+
                    'LEFT JOIN drivers as dr ON dr.driverId=tr.tripDriverId '+
                    'LEFT JOIN passengers as ps ON ps.passengerId=tr.tripPassengerId '+
                    'LEFT JOIN brands as br ON br.brandId=dr.brandId '+
                    'LEFT JOIN models as mo ON mo.modelId=dr.modelId '+
                    'LEFT JOIN levels as lv ON lv.levelId=dr.levelId '+
                    'where tr.tripId='
            connection = config.Connect(false);
            connection.query(Prices, function(err, P) {
                if(P && P.length && P[0])
                {
                    DbObj.tripPerKm = P[0].perKm
                    DbObj.tripPerMinute= P[0].perMinute
                    DbObj.tripMinCost= (parseInt(TripObj.tripNow)==1)?P[0].minNow:P[0].minLater
                    DbObj.tripStart= (parseInt(TripObj.tripNow)==1)?P[0].nowStart:P[0].laterStart
                    DbObj.tripWaitingPerHour= P[0].WaitingperHour
                }
                var INSERT = config.format('INSERT INTO trips SET ?',DbObj);
                connection.query(INSERT, function(err, result) {
                    if (err) throw err;
                    SELECT = SELECT+result.insertId;
                    connection.query(SELECT, function(err, results) {
                        if (err) throw err;
                        results[0].tripFrom = JSON.parse(results[0].tripFrom);
                        results[0].tripTo = JSON.parse(results[0].tripTo);
                        if(results[0].tripNow==1)
                        {
                            var date = new Date(new Date().getTime() + 25000);
                            var j = schedule.scheduleJob(date, function(){
                                config.TripPutRequest({body:{tripId:results[0].tripId}},false);
                            });
                            if(docs && docs.length && docs[0] && docs[0].endPoint)
                                config.SendPush(docs[0].endPoint,'New Request',TripObj.from.address,results[0]);
                        }
                        res.json(results[0]);
                    });
                    connection.end(function(err) {});
                });
            });
        });
    }
    else
    {
        var DbObj  = {
            tripCreateDate: new Date()
            ,tripFrom: JSON.stringify(TripObj.from.location)
            ,tripTo: JSON.stringify(TripObj.to.location) 
            ,tripCountryId:TripObj.tripCountryId 
            ,tripCityId:TripObj.tripCityId 
            ,tripPackageId:TripObj.tripPackageId
            ,tripNow:TripObj.tripNow
            ,tripPassengerId:PassengerObj.passengerId
            ,passengerEndPoint:PassengerObj.endPoint
            ,tripNote:TripObj.tripNote
            ,tripFromAddress:TripObj.from.address
            ,tripToAdress:TripObj.to.address
            ,tripType:TripObj.tripType
            ,tripLevelId:TripObj.tripLevelId
            ,payBy:TripObj.payBy
            ,tripPerKm:TripObj.tripPerKm
            ,tripPerMinute:TripObj.tripPerMinute
            ,tripMinCost:TripObj.tripMinCost
            ,tripStart:TripObj.tripStart
            ,tripDueDate:TripObj.tripDueDate
            ,tripWaitingPerHour:TripObj.tripWaitingPerHour
        };
        if(TripObj.tripSource)DbObj.tripSource=TripObj.tripSource
        if(TripObj.offerId)DbObj.offerId=TripObj.offerId
        if(TripObj.offerMaxValue)DbObj.offerMaxValue=TripObj.offerMaxValue
        var INSERT = config.format('INSERT INTO trips SET ?',DbObj);
        var SELECT = 'SELECT * FROM trips as tr '+
                'LEFT JOIN drivers as dr ON dr.driverId=tr.tripDriverId '+
                'LEFT JOIN passengers as ps ON ps.passengerId=tr.tripPassengerId '+
                'LEFT JOIN brands as br ON br.brandId=dr.brandId '+
                'LEFT JOIN models as mo ON mo.modelId=dr.modelId '+
                'LEFT JOIN levels as lv ON lv.levelId=dr.levelId '+
                'where tr.tripId='
        connection = config.Connect(false);
        connection.query(INSERT, function(err, result) {
            if (err) throw err;
            SELECT = SELECT+result.insertId;
            connection.query(SELECT, function(err, results) {
                if (err) throw err;
                results[0].tripFrom = JSON.parse(results[0].tripFrom);
                results[0].tripTo = JSON.parse(results[0].tripTo);
                res.json(results[0]);
                var date = new Date(new Date(TripObj.tripDueDate).getTime() - config.LaterTripDriver);
                var date0 = new Date(new Date(TripObj.tripDueDate).getTime() -(config.LaterTripDriver-60000));
                var date1 = new Date(new Date(TripObj.tripDueDate).getTime() -(config.LaterTripDriver-120000));
                var date2 = new Date(new Date(TripObj.tripDueDate).getTime() - config.PassengerAlertBefore);
                var j0 = schedule.scheduleJob(date0, function(){
                    config.TripPutRequest({body:{tripId:results[0].tripId}},false);
                });
                var j = schedule.scheduleJob(date, function(){
                    config.TripPutRequest({body:{tripId:results[0].tripId}},false);
                });
                var j1 = schedule.scheduleJob(date1, function(){
                    config.TripPutRequest({body:{tripId:results[0].tripId}},false);
                });
                var j2 = schedule.scheduleJob(date2, function(){
                    config.SendPush(results[0].passengerEndPoint,'Later Trip','Your Trip Will Start Soon after 30 Min',{});
                });
            });
            connection.end(function(err) {});
        });
    }
    
}).put(config.TripPutRequest)
router.route('/trip/:tripId').post(function(req, res) {
    if(req.body.trip.tripStatus==2)req.body.trip.tripAsignDate=new Date();
    else if(req.body.trip.tripStatus==3)req.body.trip.tripAcceptDate=new Date();
    else if(req.body.trip.tripStatus==4)req.body.trip.tripArriveDate=new Date();
    else if(req.body.trip.tripStatus==5)req.body.trip.tripPickUpdate=new Date();
    else if(req.body.trip.tripStatus==6)req.body.trip.tripFinishDate=new Date();
    else if(req.body.trip.tripStatus==7)req.body.trip.tripCancelDate=new Date();
    
    var UPDATE = config.format('UPDATE trips set ? where tripId='+parseInt(req.params.tripId),req.body.trip);
    var SELECT = 'SELECT * FROM trips as tr '+
        'LEFT JOIN drivers as dr ON dr.driverId=tr.tripDriverId '+
        'LEFT JOIN passengers as ps ON ps.passengerId=tr.tripPassengerId '+
        'LEFT JOIN brands as br ON br.brandId=dr.brandId '+
        'LEFT JOIN models as mo ON mo.modelId=dr.modelId '+
        'LEFT JOIN levels as lv ON lv.levelId=dr.levelId '+
        'where tr.tripId=?'
    SELECT = config.format(SELECT,[parseInt(req.params.tripId)]);
    connection = config.Connect(true);
    connection.query(UPDATE+';'+SELECT, function(err, results) {
        if (err) throw err;
        results[0] = results[1][0];
        results[0].tripFrom = JSON.parse(results[0].tripFrom);
        results[0].tripTo = JSON.parse(results[0].tripTo);
        results[0].tripRealDropoff = JSON.parse(results[0].tripRealDropoff);
        results[0].tripRealFrom = JSON.parse(results[0].tripRealFrom);
        if(results[0].tripStatus==6)
        {
            if(req.body.DontSendNot)
                res.json(results[0]);
            else
                config.Query.CalCost(results,req,res)
            if(config.ELM_API && results[0].vehicleReferenceNumber && results[0].captainReferenceNumber)
            {
                var data = JSON.stringify({
                    apiKey:config.ELM_API,
                    vehicleReferenceNumber:results[0].vehicleReferenceNumber,
                    captainReferenceNumber:results[0].captainReferenceNumber,
                    distanceInMeters:results[0].tripKm*1000,
                    durationInSeconds:results[0].tripTime*60,
                    customerRating:results[0].tripRate*20,
                    customerWaitingTimeInSeconds:0,
                    originCityNameInArabic:'',
                    destinationCityNameInArabic:'',
                    originLatitude:results[0].tripRealFrom.lat,
                    originLongitude:results[0].tripRealFrom.lng,
                    destinationLatitude:results[0].tripRealDropoff.lat,
                    destinationLongitude:results[0].tripRealDropoff.lng,
                    pickupTimestamp:results[0].tripPickUpdate,
                    dropoffTimestamp:results[0].tripFinishDate,

                });
                var ELM = {host: 'wasl.elm.sa',port: 443,method: 'POST',
                    path: '/WaslPortalWeb/rest/TripRegistration/send',
                    headers: {
                        'Content-Type': 'application/json; charset=utf-8',
                        'Content-Length': data.length
                    }
                } 
                console.log('TripRegistration : ',data);
                var Elm_Req = https.request(ELM, function(res) {
                    var msg = '';
                    res.setEncoding('utf8');
                    res.on('data', function (chunk) {console.log('TripRegistration : ',chunk);});
                })
                Elm_Req.write(data);
                Elm_Req.end();
            }
        }
        else
        {
            if(req.body.DontSendNot)
            {
                //res.json(results[0]);
            }
            else
            {
                if(results[0].driverEndPoint && results[0].tripStatus!=2)
                    config.SendPush(results[0].driverEndPoint,'Update Request','Your Trip Was Updated',results[0]);
                if(results[0].passengerEndPoint)
                    config.SendPush(results[0].passengerEndPoint,'Update Request','Your Trip Was Updated',results[0]);
            }
            res.json(results[0]);
        }
    });
    connection.end(function(err) {});
})
 /* Dispatcher End */
 router.route('/estimateFare').get(function(req, res) {
    config.Query.EstimateFare(req,res);
});
// REGISTER OUR ROUTES -------------------------------
//CORS middleware
var allowCrossDomain = function(req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
}
app.use(allowCrossDomain);
app.use('/api', router);
app.listen(port);
