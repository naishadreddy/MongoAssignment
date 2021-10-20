minsize = {$match:{population:{$gt:1000}}}
sortbysize = { $sort : { population: -1 }}
groupByCountry = { $group : { _id: "$country", allCities : { $addToSet : "$$ROOT" }}}
citySliced = { $project : { citySliced: {$slice:["$allCities",0,15]} }}
unwind = {$unwind:{path:"$citySliced",includeArrayIndex:'false'}}
format = { $project : { _id: { $concat: [ "$citySliced.city_ascii", "_", "$citySliced.iso2" ] } , position:["$citySliced.lng","$citySliced.lat"] , country: "$citySliced.country" }}
newcollection = { $out : "cities" }
db.worldcities.aggregate([minsize,sortbysize,groupByCountry,citySliced,unwind,format,newcollection])


firstN = { $sample: { size: 200} }
addidone = { $group: { _id: null, planes : { $push : { currentLocation :"$position" }}}}
unwind = { $unwind : {path: "$planes", includeArrayIndex: "id" }}
format = {$project : { _id : {$concat : ["CARGO",{$toString:"$id"}]},currentLocation: "$planes.currentLocation", heading:{$literal:0}, route: []}}
asplanes = { $out: "planes"}
db.cities.aggregate([firstN,addidone,unwind,format,asplanes])