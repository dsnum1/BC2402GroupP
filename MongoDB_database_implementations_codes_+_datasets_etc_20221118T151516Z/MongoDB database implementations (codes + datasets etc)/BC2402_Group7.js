// Jai Sri Ram

use "Greenworld2022"


//***************************************************************************************************************
//                      GROUP 7 BC2402
//***************************************************************************************************************

/*
    DATA TYPE CONVERSION
    We must ensure that all the data columns have the correct format. Except the _id, ISO code, and the country name, 
    all the data columns must be double. Currently all the field names are strings.

*/


db.owid_energy_data.find()
    .forEach(function(data){
        data.year = parseInt(data.year);
        var keys = Object.keys(data);
        keys.forEach(function(key){
                        if(key=="_id" || key == "country" || key=="iso_code" || key=="year"){
                        }
                        else{
                            var x = parseFloat(data[key])
                            data[key] = x
                        }
            }
     )
     db.owid_energy_data.save(data);
    }
)

/*

The database provided to us had a structured and relational style. As a result, there were many countries with
fields that had empty values. For example, Singapore has no nuclear energy. Therefore, it has an empty value for many 
fields. We can delete such fields because noSQL is schema less. This would have been a problem in mySQL. However, we can 
take advantage of this feature and implement a smaller data collection which will be very fast to query.


We are going to store this in a new collection. just in case some error happens called owid_energy_database.
*/

db.createCollection("owid_database");           // create owid_database collection
db.owid_energy_data.find()                      // for every record in the owid_energy_data, change the datatypes. Also add this into the owid_database
    .forEach(function(data){
        // print(typeof(data))
     data.year = parseInt(data.year);
     var keys = Object.keys(data);
     keys.forEach(function(key){
                        if(key=="_id" || key == "country" || key=="iso_code" || key=="year"){
                        }
                        else{
                            var x = parseFloat(data[key])
                            data[key] = x
                        }
            }
    db.owid_database.insertOne(data)     
    )
     db.owid_energy_data.save(data);
    }
    
)
    
    
    db.owid_database.update(                // remove all the fields with NaN values from the owid_database
      {},
      [{ $replaceWith: {
        $arrayToObject: {
          $filter: {
            input: { $objectToArray: "$$ROOT" },
            as: "item",
            cond: { $ne: ["$$item.v", NaN] }
          }
        }
      }}],
      { multi: true }
    )


db.owid_database.deleteMany({               //remove all the records with no countries in them
    country:{$exists:false}
})

db.owid_data


// implementing the hierarchical structure for the owid_database_structured
db.owid_database_structured.deleteMany({});
db.createCollection("owid_database_structured");
var result = {} 
db.owid_database.find()
    .forEach(function(data){
    var x = {}
    var keys = Object.keys(data);
    keys.forEach(function(key){
                x[key] = data[key];
                        }
    )
    var b = {}
    b["year"] = data.year
    b["dataValue"] = x;
    
    var l = {}
    l["dataValue2"] = b;
    l["country"] = data.country;
    
    if(!(data.country in result)){
        result[data.country] = [b]
        db.owid_database_structured.insertOne(l)
        db.owid_database_structured.updateOne({country:{$eq:data.country}}, {$push:{"yearly_data":b}});
    }
    else{
        db.owid_database_structured.updateOne({country:{$eq:data.country}}, {$push:{"yearly_data":b}})
    }
})



db.owid_database_structured.updateMany({}, {$unset:{dataValue2:""}})


db.owid_database_structured.find();

/* Question 1 asks me to find all the countries that do*/
//NOTE: implemeneted on the original owid_energy_data collection

db.owid_energy_data.aggregate([
    {$match: {"iso_code":{$nin: [ "", /OWID/ ]}}},
    {$group: {_id: {iso_code:"$iso_code"}}}]).count()


/* Question 2 */
db.owid_database_structured.aggregate([
    
    {$unwind:"$yearly_data"},
    {
        $group:
        {
            _id:{
                "country":"$country"
            },
            'minimum_year':{'$min':"$yearly_data"},
            'maximum_year':{'$max':"$yearly_data"},
        },
    },
    {
        $project:{
            _id:1,
            "country":1,
            "maximum_year.year":1,
            "minimum_year.year":1,
            "ismax":{$eq:["$maximum_year.year",2021]},
            "ismin":{$eq:["$minimum_year.year", 1900]}
        }
    },
    {
        $match:{
            "ismax":true
        }
    },
    {
        $match:{
            "ismin":true
        }
    },
    {
        $project:{
            _id:1,  
            ismin:false,
            ismax:false,
        }
    }
    ]
)
    

/*Question 3*/
/*
Specific to Singapore, in which year does <fossil_share_energy> stop being the full
source of energy (i.e., <100)? Accordingly, show the new sources of energy. 
*/

db.owid_database_structured.aggregate(
    [
        {
            $match:
                {country:"Singapore"},
        },
        {
            $unwind:"$yearly_data"
        },
        {
            $project:{
                'country':1,
                'yearly_data.year':1,
                'yearly_data.dataValue.fossil_share_energy':1,
                "other_renewables_share_energy":1,
                "renewables_share_energy":1,
                "yearly_data.dataValue.biofuel_share_energy":1,
                "yearly_data.dataValue.hydro_share_energy":1,
                "yearly_data.dataValue.nuclear_share_energy":1,
                "yearly_data.dataValue.solar_share_energy":1,
                "yearly_data.dataValue.wind_share_energy":1
            }
        },
        {
            $match:
            {'yearly_data.dataValue.fossil_share_energy':{$lt:100}}
        },
        // {
        //     $group:{
        //         _id:{
        //             'country':"$country"
        //         },
        //         year:{
        //             $first:"$yearly_data.year"
        //         }
        //     }
        // }

        
    ]
)



/*
Q4.
Compute the average <GDP> of each ASEAN country from 2000 to 2021 (inclusive
of both years). Display the list of countries based on the descending average GDP
value
*/


db.owid_database_structured.aggregate(
    [
        {
            $match:
                {country:{$in:["Indonesia","Malaysia","Philippines","Singapore","Thailand","Brunei","Vietnam","Laos","Myanmar","Cambodia"]}},
        },
        {
            $unwind:"$yearly_data"
        },
        {
            $match:
                {$and:[{'yearly_data.year':{$gte:2000}},{'yearly_data.year':{$lte:2021}}]}
        },
        
        {
            $group:
            {
                "_id":{
                    "country":"$country",
                },
                averageGDP:{$avg:"$yearly_data.dataValue.gdp"}
            }
        }
        
    ]
)

/*
5. (Without creating additional tables/collections) For each ASEAN country, from 2000
to 2021 (inclusive of both years), compute the 3-year moving average of
<oil_consumption> (e.g., 1st: average oil consumption from 2000 to 2002, 2nd:
average oil consumption from 2001 to 2003, etc.). Based on the 3-year moving
averages, identify instances of negative changes (e.g., An instance of negative
change is detected when 1st 3-yo average = 74.232, 2nd 3-yo average = 70.353).
Based on the pair of 3-year averages, compute the corresponding 3-year moving
averages in GDP. 
*/



db.owid_database_structured.aggregate(
    [
        {
            $match:
                {country:{$in:["Indonesia","Malaysia","Philippines","Singapore","Thailand","Brunei","Vietnam","Laos","Myanmar","Cambodia"]}},
        },
        {
            $unwind:"$yearly_data"
        },
        {
            $match:
                {$and:[{'yearly_data.year':{$gte:2000}},{'yearly_data.year':{$lte:2018}}]}
        },
        
        {
            $setWindowFields:{
            partitionBy:{"year":"yearly_data.year"},
            sortBy:{
                "country":1,
                "yearly_data.year":1},
            output:{
                "3_YEAR_OIL_CONS_AVG": {
                    $avg: "$yearly_data.dataValue.oil_consumption", 
                    window: {documents: [ 0, 2 ]}},
                "3_YEAR_GDP_AVG": {
                    $avg: "$yearly_data.dataValue.gdp", 
                    window: {documents: [ 0, 2 ]}},
                
            }
            }
        },
        {
            $setWindowFields:{
            partitionBy:{"year":"yearly_data.year"},
            sortBy:{
                "country":1,
                "yearly_data.year":1},
            output:{
                "lastYearMVA": {
                    $push: "$3_YEAR_OIL_CONS_AVG", 
                    window: {documents: [ -1, -1 ],}}}
            }
        },
            {$unwind:"$lastYearMVA"},
        {
            $addFields: {
                changeInMVA: {
                    $subtract: ["$3_YEAR_OIL_CONS_AVG", "$lastYearMVA"]
                }
            }
        },
        {
            $match:
                {changeInMVA:{$lt:0}}
        }
    ]
)


/*Question 6*/

// for exports of energy product:
db.exportsofenergyproducts.aggregate([
    {$group:{_id:{"groupbyenergyproducts":"$energy_products","groupbysubproducts":"$sub_products"},"avgktoe":{$avg:{$toDecimal:"$value_ktoe"}}}}
    ])

// for imports of energy product:
db.importsofenergyproducts.aggregate([
    {$group:{_id:{"groupbyenergyproducts":"$energy_products","groupbysubproducts":"$sub_products"},"avgktoe":{$avg:{$toDecimal:"$value_ktoe"}}}}
    ])


// assumptions & actions taken: since there are more types of imported of energy products that is not exported, the left table should be the imports table so as to include them.
// since this requires a join on multiple fields (eg: year, subproducts), use the $lookup pipeline since of equality match.
// ref about lookup pipeline: https://www.stackchief.com/tutorials/%24lookup%20Examples%20%7C%20MongoDB
// ref about the join & why the records that are not found in both collections are not shown: https://www.spektor.dev/is-mongodb-lookup-really-a-left-outer-join/
// ref about set & replace null values: https://www.mongodb.com/community/forums/t/replace-empty-array-of-unwind/8126
// ref about grouping by multiple fields: https://linuxhint.com/mongodb-group-multiple-fields/

//final code:
db.importsofenergyproducts.aggregate([
    {$lookup:{
        from:"exportsofenergyproducts",
        let: {"imp_year":"$year","imp_sub":"$sub_products"},
        pipeline:[
            {$match:
            {$expr:
            {$and:
            [
                {$eq:["$year","$$imp_year"]},
                {$eq:["$sub_products","$$imp_sub"]}
                ]}}}
            ],
            as:"exportdata"
    }},
    {$set:{
        "exp_ktoe":{
        $cond:{
            if:{$eq:[{
                $size:"$exportdata",
            },0],
        },
            then:0
            ,
            else:"$exportdata.value_ktoe"
        }
    }}},
    {$unwind:"$exp_ktoe"},
    {$group:{_id:{"groupbyenergyproducts":"$energy_products","groupbysubproducts":"$sub_products"},
    "avg_impktoe":{$avg:{$toDecimal:"$value_ktoe"}},
    "avg_expktoe":{$avg:{$toDecimal:"$exp_ktoe"}}}},
    {$project:{"energy_products":1,"avg_impktoe":1,"avg_expktoe":1}}
    ])

/*Question 7*/

{
  "productpairs": "Coal and Peat-Coal and Peat",
  "value_ktoe": "8.2",
  "exp_ktoe": "0.3",
  "year": 2005,
  "differencevalue_ktoe": null,
  "differenceexp_ktoe": null
}

db.importsofenergyproducts.aggregate([
    {$lookup:{
        from:"exportsofenergyproducts",
        let: {"imp_year":"$year","imp_sub":"$sub_products"},
        pipeline:[
            {$match:
            {$expr:
            {$and:
            [
                {$eq:["$year","$$imp_year"]},
                {$eq:["$sub_products","$$imp_sub"]}
                ]}}}
            ],
            as:"exportdata"
    }},
    {$set:{
        "exp_ktoe":{
        $cond:{
            if:{$eq:[{
                $size:"$exportdata",
            },0],
        },
            then:0
            ,
            else:"$exportdata.value_ktoe"
        }
    }}},
    {$unwind:"$exp_ktoe"},
    {$project: {_id: 0, productpairs: {$concat: ["$energy_products", "-", "$sub_products"]}, "value_ktoe": {$convert: {input:"$value_ktoe", to: "decimal", onError:-1, onNull:-1}},
    "exp_ktoe": {$convert: {input:"$exp_ktoe", to: "decimal", onError:-1, onNull:-1}}, 
            "year": {$convert: {input:"$year", to: "int", onError:-1}}}} ,
    {
   $setWindowFields: {
     partitionBy: "productpairs",
     sortBy: { "productpairs": 1,"year": 1 },
     output: {
      differencevalue_ktoe: {$shift: {
                        output: "$value_ktoe",
                        by: -1}
      }
     },
   },
},
{ $set: {
    differencevalue_ktoe: { $subtract: ["$value_ktoe" , "$differencevalue_ktoe" ] }
  }},
  
  {
   $setWindowFields: {
     partitionBy: "productpairs",
     sortBy: { "productpairs": 1,"year": 1  },
     output: {
      differenceexp_ktoe: {$shift: {
                        output: "$exp_ktoe",
                        by: -1}
      }
     },
   },
},
{ $set: {
    differenceexp_ktoe: { $subtract: [ "$exp_ktoe" , "$differenceexp_ktoe" ] }
  }}
    ])

/*Question 8*/

db.householdelectricityconsumption.distinct("Region")
// 6 different regions: Central Region, East Region, North East Region, North Region, Overall, West Region.
// Since question specifically states to exclude overall region, only view the average kwh_per_acc for the remaining 5 regions.

//checking the datatype of kwh_per_acc variable.
//action needed: convert to decimal in order to compute the average.
db.householdelectricityconsumption.aggregate([
    {"$project":{"fieldType":{"$type":"$kwh_per_acc"}}}
    ])

// since there are line break or spaces ("\r") after the values in the kwh_per_acc attribute, they need to be removed before they can be converted to decimal to calculate the average values.
// ref about removing "\r" using $trim: https://www.mongodb.com/docs/manual/reference/operator/aggregation/trim/
// error encountered when running the code below: "Failed to parse number 's' in $convert with no onError value: Failed to parse string to decimal"
db.householdelectricityconsumption.aggregate([
    {$project:{"year":1,"Region":1,"kwh_per_acc":{$trim: {input:"$kwh_per_acc"}}}},
    {$match:{"Region":{$ne:"Overall"}}},
    {$group:{_id:{"groupbyregion":"$Region","groupbyyear":"$year"},
    "avgkwh":{$avg:{$toDecimal:"$kwh_per_acc"}}}}
    ])

// investigate what is number 's': turns out there are 906 documents with "s" as the kwh_per_acc 
// since replacing them with zero will affect the average value, ignore them.
db.householdelectricityconsumption.find({"kwh_per_acc":{$regex:/s/}})

// presence of "overall" in the dwelling_type field
// need to remove it to prevent double counting when computing the yearly average kwh_per_acc value.
db.householdelectricityconsumption.distinct("dwelling_type")

// presence of "annual" in the dwelling_type field
// need to remove it to prevent double counting when computing the yearly average kwh_per_acc value.
db.householdelectricityconsumption.distinct("month")

//final code:
db.householdelectricityconsumption.aggregate([
    {$match:{"Region":{$ne:"Overall"}}},
    {$match:{"month":{$ne:"Annual"}}},
    {$match:{"dwelling_type":{$ne:"Overall"}}},
    {$project:{"year":1,"Region":1,"kwh_per_acc":{$trim: {input:"$kwh_per_acc"}}}},
    {$match:{"kwh_per_acc":{$ne:"s"}}},
    {$group:{_id:{"groupbyregion":"$Region","groupbyyear":"$year"},
    "avgkwh":{$sum:{$toDecimal:"$kwh_per_acc"}}}},
    {$sort: {"_id.groupbyyear":1,"_id.groupbyregion":1}}
    ])




/*Question 9*/

db.householdelectricityconsumption.aggregate([
    {$project:{"dwelling_type":1,
    "year":1,
    "Region":1,
    "Description":1,
    "kwh_per_acc":{$trim: {input:"$kwh_per_acc"}},
    "month":1}},
    {$match:{"kwh_per_acc":{$ne:"s"}}},
    { $match: { Region: { $in: ['North Region', 'East Region', 'West Region', 'South Region', 'Central Region'] } } },
        {
        $group: {
            _id: {"Region": "$Region" , "year": {$toInt: "$year"}},"avgkwh":{$avg:{$toDecimal:"$kwh_per_acc"}}}
        },
    {
   $setWindowFields: {
     partitionBy: "_id.Region",
     sortBy: { "_id.year": 1 },
     output: {
      difference: {
        $push: "$avgkwh",
        window: { range: [-1, 0] }
      }
     },
   },
},
{ $set: {
    difference: { $subtract: [{ $last: "$difference" }, { $first: "$difference" }] }
  }},
  
    ])

/*Question 10*/


Codes:

db.householdelectricityconsumption.find()
// quarterly: Q1-Q4
// Q1: 1-3
// Q2: 4-6
// Q3: 7-9
// Q4: 10-12

// 12 months + "annual" --> remove the annual from the analysis
db.householdelectricityconsumption.distinct("month")


//ref about $in operator: https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/
//ref about if else operator: https://stackoverflow.com/questions/27479347/is-there-an-elseif-thing-in-mongodb-to-cond-while-aggregating

db.householdelectricityconsumption.aggregate([
    {$match:{"month":{$ne:"Annual"}}},
    {$match:{"Region":{$ne:"Overall"}}},
    {$project:{"dwelling_type":1,
    "year":1,
    "Region":1,
    "Description":1,
    "kwh_per_acc":{$trim: {input:"$kwh_per_acc"}},
    "month":1,
    "quarter":{"$cond":{"if":{"$in":["$month",["1","2","3"]]}, "then": "Q1",
                    "else":{
                        "$cond":{
                        "if":{"$in":["$month",["4","5","6"]]}, "then":"Q2",
                        "else":{
                            "$cond":{
                            "if":{"$in":["$month",["7","8","9"]]},"then":"Q3",
                            "else": "Q4"
                        }
                    }}}}}}},
    {$match:{"kwh_per_acc":{$ne:"s"}}},
    {$group:{_id:{"groupbyregion":"$Region","groupbyyear":"$year","groupbyquarter":"$quarter"},
    "avgkwh":{$avg:{$toDecimal:"$kwh_per_acc"}}}},
    {$sort: {"_id.groupbyregion":1,"_id.groupbyyear":1,"_id.groupbyquarter":1}}
    ])


// The query output is exported  to excel in order to analyse trend
// From the analysis, the avgkwh per quarter follows this trend (desc order): Q2, Q3, Q4, Q1
    // The avgkwh can be linked to the weather/ temperatures during that quarter. For example, the avgkwh is highest in Q2 which has the hottest temperatures in Singapore. As such, there may be greater use of electricity for air conditioners. 
// Additionally, the avgkwh also varies for the different regions (desc order) : Central region, North Region, East Region/ Nort East Region, West Region




/*Question 11*/

// There was no pattern witnessed 

db.householdtowngasconsumption.find().forEach(
        function convertDataTypes(data){
            data.avg_mthly_hh_tg_consp_kwh = parseFloat(data.avg_mthly_hh_tg_consp_kwh);
            data.month = parseInt(data.month);
            data.year = parseInt(data.year);
            db.householdtowngasconsumption.save(data);
        }
    )


// this is to remove all the error fields
db.householdtowngasconsumption.deleteMany({month:NaN})


db.householdtowngasconsumption.updateMany({},
{
    $unset:{quarter_number:""}
})

db.householdtowngasconsumption.aggregate([
    {
        $match:
            {month:{$ne:NaN}}
    },
    {
        $addFields: {
                quarter_number: {
                   "$subtract": ["$month", 1],
                }
            }
    },
    {
        $project:{
                quarter_number:
                {
                    "$multiply":["$quarter_number",(1/3)]
                },
                month:1,
                sub_housing_type:1,
                housing_type:1,
                avg_mthly_hh_tg_consp_kwh:1
        }
    },
    {
        $project:{
                quarter_number:{
                    $convert:{input:"$quarter_number", to:"int", onError:-1, onNull:-1}
                },
                month:1,
                sub_housing_type:1,
                housing_type:1,
                avg_mthly_hh_tg_consp_kwh:1
        }
    },
    {
        $project:{
                quarter_number:{
                    $add:["$quarter_number", 1],
                },
                month:1,
                sub_housing_type:1,
                housing_type:1,
                avg_mthly_hh_tg_consp_kwh:1
        }
    },
    {
        $group:{
            _id:{
                sub_housing_type:"$sub_housing_type",
                quarter_number:"$quarter_number",
            },
            quarterly_avg:{$avg:"$avg_mthly_hh_tg_consp_kwh"}
        }
    },
    {
        $sort:{
            _id:1,
            sub_housing_type:1,
            quarter_number:1
        }
    }
    ])


/*Question 12*/

db.owid_database_structured.find()

db.owid_database_structured.aggregate(
    [
        {
            $match:
                {$or:[
                        {country:"Singapore"},
                        {country:"Luxembourg"},
                        {country:"Ireland"},
                    ]}
        },
        {
            $project:{
                "_id":1,
                "country":1,
                "yearly_data.dataValue.energy_per_gdp":1,
                "yearly_data.dataValue.year":1,
                "yearly_data.dataValue.energy_per_capita":1,
                "yearly_data.dataValue.coal_consumption":1,
                "yearly_data.dataValue.gas_consumption":1,
                "yearly_data.dataValue.oil_consumption":1,
             }
        },
    ]
)

/*Question 13*/

db.owid_database_structured.aggregate([]);


db.owid_database_structured.aggregate([
    {
        $project:{
                 "country":1,
                 "avg_renewables_share_energy":{$avg:"$yearly_data.dataValue.renewables_share_energy"},
                 "avg_gdp_per_capita":{$avg:"$yearly_data.dataValue.gdp"}
        }
    },
    {
        $sort:{
            "avg_renewables_share_energy":-1
        }
    }
    ])



db.owid_database_structured.aggregate([
    {
        $project:{
                 "country":1,
                 "avg_carbon_intensity":{$avg:"$yearly_data.dataValue.carbon_intensity_elec"},
                 "avg_gdp_per_capita":{$avg:"$yearly_data.dataValue.gdp"}
        }
    },
    {
        $sort:{
            "avg_carbon_intensity":-1
        }
    }
    ])
    

/*Question 14*/

// primary energy consumption (total energy demand of the country)
// figure 14.1
// blank value for 2021 so evaluation is done for years before 2020 (inclusive)
db.owid_energy_data.find()

db.owid_energy_data.aggregate([
    {$match:{"country":{$eq:"Singapore"}}},
    {$match:{"year":{$lte:2020}}},
    {$project:{"_id":0,"year":1,"primary_energy_consumption":1}},
    {$sort:{"year":1}}
    ])

// no energy generated from wind,nuclear,hydro and biofuel
// blank values for 2021 so evaluation is done for years before 2020 (inclusive)
db.owid_energy_data.aggregate([
    {$match:{"country":{$eq:"Singapore"}}},
    {$match:{"year":{$lte:2020}}},
    {$project:{"_id":0,"year":1,"biofuel_share_energy":1,"fossil_share_energy":1,"hydro_share_energy":1,"nuclear_share_energy":1,"other_renewables_share_elec_exc_biofuel":1,"other_renewables_share_energy":1,"renewables_share_energy":1,"solar_share_energy":1,"wind_share_energy":1}},    
     {$sort:{"year":1}}
   )]


// evaluate change in energy sources aside from fossil fuels:
// blank values for 2021 so evaluation is done for years before 2020 (inclusive)
// figure 14.2
db.owid_energy_data.aggregate([
    {$match:{"country":{$eq:"Singapore"}}},
    {$match:{"year":{$lte:2020}}},
    {$project:{"_id":0,"year":1,"other_renewables_share_energy":1,"renewables_share_energy":1,"solar_share_energy":1}},    
     {$sort:{"year":1}}
   )]


// evaluate change in usage of fossil fuels:
// blank values for 2021 so evaluation is done for years before 2020 (inclusive)
// figure 14.3
db.owid_energy_data.aggregate([
    {$match:{"country":{$eq:"Singapore"}}},
    {$match:{"year":{$lte:2020}}},
    {$project:{"_id":0,"year":1,"fossil_share_energy":1}},    
     {$sort:{"year":1}}
   )]


// how much of Singapore's energy demand (in TWH) is produced by solar energy:
// figure 14.4
db.owid_energy_data.aggregate([
    {$match:{"country":{$eq:"Singapore"}}},
    {$match:{"year":{$lte:2020}}},
    {$project:{"_id":0,"year":1,"primary_energy_consumption":1,"solar_share_energy":1}},    
     {$sort:{"year":1}}
   )]
   

// Consider the change in amount of imported energy products if coal is replaced with nuclear fuel
// yearly amount of imported coal 
// figure 14.5   

db.importsofenergyproducts.aggregate([
    {$match:{"energy_products":{$eq:"Coal and Peat"}}}
    ])


// total amount of imported energy product in 2020
// output: 151,230.90ktoe
// figure 14.6
db.importsofenergyproducts.aggregate([
    {$match:{"year":{$eq:"2020"}}},
    {$group:{_id:{"groupbyyear":"$year"}, "total":{$sum:{$toDecimal:"$value_ktoe"}}}}
    
    ])



/*Question 15*/

// steps to import external dataset to be used for q15: 
//1. in mongodb compass, create database named "sustainability 2022"
//2. create the following collections and imported the respective csv files:
    // collection name: disasters, import csv file: disaster.csv 
    // collection name: globaltemperatures, import csv file: globaltemperatures.csv 
    // collection name: greenhouse_gas_inventory_data, import csv file: greenhouse_gas_inventory_data.csv
    // collection name: mass_balance_data, import csv file: mass_balance_data.csv 
    // collection name: seaice, import csv file: seaice.csv

use sustainability2022

db.greenhouse_gas_inventory_data.find()

// identifying the different categories of ghg in the dataset:
// output: 10 different categories
// the category "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent" is the column containing the summed amount of all the other ghg category values
// "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent" column will be used to evaluation
db.greenhouse_gas_inventory_data.distinct("category")


// Computing the yearly GHG emissions and cumulative emissions
// figure 15.1: average yearly GHG emissions
// figure 15.2: cumulative GHG emissions
// output: yearly and cumulative ghg emissions from 1990 to 2014 (25 rows)

db.greenhouse_gas_inventory_data.aggregate([
    {$match:{"category":{$eq:"greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent"}}},
    {$group:{_id:{"groupbyyear":"$year"},"ghgEmission":{$sum:{$toDecimal:"$value"}}}},
    {$sort:{"_id.groupbyyear":1}},
    {$group:{
        _id:0, "year":{$push:"$_id.groupbyyear"},"EmissionLevel":{$push:"$ghgEmission"}
    }
    },
    {$unwind:{path:"$year", includeArrayIndex: "index"}},
    {$project:{
        "_id":0, "year":1,"EmissionLevel":{$arrayElemAt:["$EmissionLevel","$index"]}, "CumulativeEmission":{$sum:{$slice:["$EmissionLevel",{$add:["$index",1]}]}
    }
    

// Identifying change in cumulative ghg emissions and land temperature:
//find yearly average temp from 1990 - 2014:
db.globaltemperatures.aggregate([
    {$project: 
        {"year": {$substrCP:["$recordedDate",0,4]},
        "LandAverageTemperature":1}},
    {$group:{_id:{"groupbyyear":"$year"},
    "avgtemp":{$avg:{$toDecimal:"$LandAverageTemperature"}}}},
    {$match:{$and:[
        {"_id.groupbyyear":{$gte:"1990"}},
        {"_id.groupbyyear":{$lte:"2014"}}
        ]}},
    {$project:{"avgtemp":1}},
    {$sort:{"_id.groupbyyear":1}}
    ])
// Figure 15.3: cumulative ghg emissions and average temperatures are manually compiled in Excel to generate visualisations.

// Identifying change in cumulative ghg emissions and sea ice extent:
// output: yearly and cumulative ghg emissions, seaice extent from 1990 to 2014 (25 rows)
db.seaice.aggregate([
    {$match:{$and:[
        {"Year":{$gte:"1990"}},
        {"Year":{$lte:"2014"}}
        ]}},
    {$project: {"Year":1,"Extent":1,}},
    {$group:{_id:{"groupbyyear":"$Year"},"avgextent":{$avg:{$toDecimal:"$Extent"}}}},
    {$sort:{"_id.groupbyyear":1}}
    ])
// Figure 15.4: cumulative ghg emissions and seaice extent is manually compiled in Excel to generate visualisations.


//creation and adding a new "year" field to all documents in the collection:
db.mass_balance_data.updateMany({},[{$set:{"YEAR":{$substr:["$SURVEY_DATE",0,4]}}}])

// all glaciers that have yearly records from 2005 to 2014 are extracted and evaluated. 
// This time frame is used becauseas the latest year in the greenhouse_gas_inventory table is 2014.
// output:  54 glaciers
db.mass_balance_data.aggregate([
    {$match:{"AREA":{$exists:true}}},
    {$project: 
        {"YEAR":1,"WGMS_ID":1,"AREA":1}},
    {$match:{$and:[
        {"YEAR": {$gte:"2005"}},
        {"YEAR": {$lte:"2014"}}]}},
    {$group:{_id:{"groupbyWGMS_ID":"$WGMS_ID"},"tcount":{$sum:1}}},
    {$match:{"tcount":{$eq:10}}},
    {$count:"glacier_count"}])
    

//find yearly glacier area
// output: yearly glacier area from 2005 to 2014 (10 rows)
db.mass_balance_data.aggregate([
    {$match:{"AREA":{$exists:true}}},
    {$project: 
        {"YEAR":1,"WGMS_ID":1,"AREA":1}},
    {$match:{$and:[
        {"YEAR": {$gte:"2005"}},
        {"YEAR": {$lte:"2014"}}]}},
    {$group:{_id:{"groupbyWGMS_ID":"$WGMS_ID"},"tcount":{$sum:1}}},
    {$match:{"tcount":{$eq:10}}},
    {$lookup:{
        from:"mass_balance_data"
        let:{"iddd":"$_id.groupbyWGMS_ID"},
        pipeline:[
            {$match:
                {$expr: {$and:[
                    {$eq:["$WGMS_ID","$$iddd"]},
                    {$gte:["$YEAR","2005"]},
                    {$lte:["$YEAR","2014"]}]}}
                }
            ], 
            as: "extra"}},
    {$unwind:"$extra"},
    {$match:{"extra.AREA":{$exists:true}}},
    {$project: {"_id":0,"extra.YEAR":1,"extra.WGMS_ID":1,"extra.AREA":1}},
    {$group:{_id:{"groupbyyear":"$extra.YEAR"},"glacier_area":{$sum:{$toDecimal:"$extra.AREA"}}}},
    {$sort:{"_id.groupbyyear":1}}
// figure 15.6: cumulative ghg emission figures and yearly glacier area is manually compiled in Excel to generate visualisations.


// evalute records relating to droughts, extreme temperatures, floods and storms
// observe the trend in the count of natural disaster over the years:
// output: yearly count of natural disasters from 1970 to 2021
db.disaster.aggregate([
    {$match:{$or:[
        {"Disaster": "Storm"},
        {"Disaster": "Drought"},
        {"Disaster": "Flood"},
        {"Disaster": "Extreme temperature"}}},
    {$project:{"_id":0, "Year":1, }},
    {$match:{"Year":{$gte:"1990"}}},
    {$group:{_id:{"groupbyyear":"$Year"}, "disastercount":{$sum:1}}},
    {$sort: {"_id.groupbyyear":1}}
    ])
// figure 15.7: cumulative ghg emissions and disaster count is manually compiled in Excel to generate visualisations.    
    
    

//figure 15.8: cumulative ghg emission figures and count of natural disasters are compiled in excel to generate visualisations.









