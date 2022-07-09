/* Importing instructions:
    1) Create database
    2) Create 3 collections
    3) Import each JSON file to the respective collection, through MongoDB Compass
    
    **TAKE NOTE: For covid19data collection, we imported the covid19data_2 JSON file from ntulearn as we faced issues with the other covid19data file. 
    Thus, for all queries here based on covid19data collection, it is based off the covid19data_2 JSON file. */

//---------------------------------------------------------------------------------------------------------------------------------------------------------

use BC2402_Project

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 1: Display a list of total vaccinations per day in Singapore.

db.country_vaccinations.aggregate([
    {$match: {"country" : "Singapore"}},
    {$project: {_id:0, "date": {$convert :{input :"$date", to : "date"}},"total_vaccinations":{$convert :{input :"$total_vaccinations", to : "int"}}}},
    {$sort: {date: 1}}
])

/* This query returns the total number of vaccinations for each day in Singapore. Essentially, we observed that the values in total_vaccinations
is accumulated over the days. This means that all subsequent records after the first date (2021-01-11) total_vaccinations is recorded, should be recording
values that are either the same as the previous date, or larger. 

However, we observed that majority of the total_vaccinations data is recorded as '0'. This could indicate that the total_vaccinations data on these days 
were not being updated accordingly. Hence, this implies that this country_vaccinations collection may be missing information. As such, the resulting 
query based on this collection may not be accurate or reliable. */

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 2: Display the sum of daily vaccinations among ASEAN countries.
// Where ASEAN countries are Brunei, Myanmar, Cambodia, Indonesia, Laos, Malaysia, Philippines, Singapore, Thailand and Vietnam

db.country_vaccinations.aggregate([
    {
        $match: {
            country: {
                $in: ["Brunei", "Myanmar", "Cambodia", "Indonesia", "Laos", "Malaysia", "Philippines", "Singapore", "Thailand", "Vietnam"]
            }
        }
    },
    {
        $project : {
            daily_vaccinations : {$convert :{input :"$daily_vaccinations", to : "int"}}, date: {$convert :{input :"$date", to : "date"}}
        }
    },
    {
        $group : {_id :{groupByDate :"$date"}, sum :{$sum: "$daily_vaccinations"}}
    },
    {
        $sort: {"_id.groupByDate": 1}
    }
])

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 3: Identify the maximum daily vaccinations per million of each country. Sort the list based on daily vaccinations per million in a descending order.

db.country_vaccinations.aggregate([
    {
        $project : {
            daily_vaccinations_per_million : {$convert : {input : "$daily_vaccinations_per_million", to : "int"}}, country : "$country"
        }
    },
    {
        $group : {_id : {groupByCountry : "$country"}, max : {$max : "$daily_vaccinations_per_million"}}
    },
    {
        $sort : {max : -1}
    }
])

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 4: Which is the most administrated vaccine? Display a list of total administration (i.e., sum of total vaccinations) per vaccine.

db.country_vaccinations_by_manufacturer.aggregate([
    {
        $project : {
            total_vaccinations : {$convert : {input : "$total_vaccinations", to : "int"}}, vaccine : "$vaccine"
        }
    },
    {
        $group : {_id : {groupByVaccine : "$vaccine"}, sum : {$sum : "$total_vaccinations"}}
    },
    {
        $sort : {sum : -1}
    }
])
// ANS: The most administered vaccine is PiferBioNTech, out of 8 types of vaccines.

/* However, summing up total_vaccinations data from the country_vaccinations_by_manufacturer collection, as required by the question, will not produce 
the most accurate query. We observed that values in total_vaccinations for each vaccine type is accumulated over the days. This means that the value 
of total_vaccinations for a particular vaccine on a particular day will include the total_vaccinations from the previous day for the corresponding 
vaccine type, as well. As such, summing up the total_vaccinations does not accurately display the total adminimstration per vaccine. 

To get a more accurate representation of the exact amount of the total administration per vaccine, the maximum value of the total_vaccinations per vaccine
should be considered instead. */

//---------------------------------------------------------------------------------------------------------------------------------------------------------
/* Qn 5: Italy has commenced administrating various vaccines to its populations as a vaccine becomes available. Identify the first dates of each vaccine being 
administrated, then compute the difference in days between the earliest date and the 4th date.*/

db.country_vaccinations_by_manufacturer.aggregate([
    {
        $match : {location : "Italy"}
    },
    {
        $project : {
            date : {$convert : {input :"$date", to : "date"}}, vaccine : "$vaccine"
        }
    },
    {
        $group : {_id :{groupByVaccine : "$vaccine"}, date : {$push : "$date"}}
    },
    {
        $project : {
            vaccine : 1,
            minDate : {$min : "$date"},
    
        }
    },
    {
        $group : {
            _id : 0,
            minDate : {$min : "$minDate"},
            maxDate : {$max : "$minDate"}
        }
    }
    {
        $project :{
            _id : 0,
            dayDiff : {$dateDiff : {
                startDate : "$minDate",
                endDate : "$maxDate",
                unit : "day"
            }
        }
    }
])
/* ANS: The difference in days is 90 days, where the 1st date refers to the 1st date of Pfizer/BioNTech being administed while the 4th date refers to the
1st date of Johnson&Johnson being administered in Italy. */

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 6: What is the country with the most types of administrated vaccine?

db.country_vaccinations_by_manufacturer.aggregate([
    {
        $project : { "location" : 1 , "vaccine" : 1}
    },
    {
        $group : {_id :{groupByCountry : "$location"}, vaccine : {$addToSet : "$vaccine"}}
    },
    {
        $unwind : "$vaccine"
    },
    {
        $group : {_id : "$_id", vaccineCount : {$sum : 1}}
    },
    {
        $sort : {vaccineCount :-1}
    }
])
// ANS: The country with the most types of administered vaccines is Hungary, which has 6 types of vaccines, out of all other countries. 

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 7: What are the countries that have fully vaccinated more than 60% of its people? For each country, display the vaccines administrated.

db.country_vaccinations.aggregate([
    {
        $project : {
            people_fully_vaccinated_per_hundred : {$convert : {input : "$people_fully_vaccinated_per_hundred", to : "double"}} , country : "$country", vaccines : "$vaccines"
        }
    },
    {
        $match : {people_fully_vaccinated_per_hundred : {$gte : 60}}
    },
    {
        $group : {_id : {groupByCountry : "$country"}, vaccines : {$addToSet : "$vaccines"}}
    }
])
// ANS: There are 6 countries that have fully vaccinated more than 60% of its people.

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 8: Monthly vaccination insight – display the monthly total vaccination amount of each vaccine per month in the United States.

db.country_vaccinations_by_manufacturer.aggregate([
    {
        $match : {location : "United States"}
    },
    {
        $project : {
            date : {$convert : {input :"$date", to : "date"}},
            total_vaccinations : {$convert : {input : "$total_vaccinations", to : "int"}},
            vaccine: 1
        }
    },
    {
         $project : {
            dateMonth : {$month: "$date"},
            total_vaccinations : 1,
            vaccine: 1
         }
    },
    {
        $group : {
            _id :{month : "$dateMonth", vaccine: "$vaccine"},
           total_vaccinations: {$max: "$total_vaccinations"}
        }
    },
    {
        $sort : {_id : 1}
    }
])

//---------------------------------------------------------------------------------------------------------------------------------------------------------
/* Qn 9: Days to 50 percent. Compute the number of days (i.e., using the first available date on records of a country) that each country takes to go above 
the 50% threshold of vaccination administration (i.e., total_vaccinations_per_hundred > 50) */

db.firstAvailDate.drop()

// Create a new collection containing first available dates for each country
db.country_vaccinations.aggregate([
    {$project: {
        total_vaccinations_per_hundred: {$convert: {input: "$total_vaccinations_per_hundred", to: "double"}},
        country: 1,
        date: {$convert: {input: "$date", to: "date"}}
    }},
    {$group: {
        _id: {groupByCountry: "$country"}, 
        first_available_date: {$min: "$date"}
    }},
    {$sort: {"_id.groupByCountry": 1}},
    {$out: "firstAvailDate"}
])

db.firstAvailDate.find()

// Joining 2 collections to compute the number of days that each country takes to reach the 50 percent threshold
db.country_vaccinations.aggregate([
     {$project: {
        total_vaccinations_per_hundred: {$convert: {input: "$total_vaccinations_per_hundred", to: "double"}},
        country: 1,
        date: {$convert: {input: "$date", to: "date"}}
    }},
    {$match: {"total_vaccinations_per_hundred": {$gt: 50}}},
    {$group: {
        _id: {groupByCountry: "$country"}, 
        firstDateAbv50: {$min: "$date"}
    }},
    {$lookup: {
        from: "firstAvailDate",
        localField: "_id.groupByCountry",
        foreignField: "_id.groupByCountry",
        as: "firstAvailableDate"
    }},
    {
        $unwind: "$firstAvailableDate"
    },
    {
        $project: {
            daysTo50Percent : 
                {$dateDiff : {
                    startDate : "$firstAvailableDate.first_available_date",
                    endDate : "$firstDateAbv50",
                    unit : "day"}
                }
        }
    },
    {
        $sort: {_id: 1}
    }
])

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 10: Compute the global total of vaccinations per vaccine.

db.country_vaccinations_by_manufacturer.aggregate([
   {$project: {
        total_vaccinations: {$convert: {input: "$total_vaccinations", to: "int"}},
        location: 1
        vaccine: 1,
    }},
    {$group: {
        _id: {groupByLocation: "$location", groupByVaccine: "$vaccine"}, 
        total_vaccinations: {$max: "$total_vaccinations"}
    }},
    {$group: {
        _id: {groupByVaccine: "$_id.groupByVaccine"}, 
        total_vaccinations: {$sum: "$total_vaccinations"}
    }},
    {$sort: {"total_vaccinations":1}}
])

/* As mentioned in qn 4, the total_vaccinations data for each vaccine type from the country_vaccinations_by_manufacturer collection is accumulated over the days. 
Additionally, for this question, it is important to take note that total_vaccinations data for each vaccine is accumulated over the days, within each individual 
country. This means the total_vaccinations values are not accumulated across countries. Hence, finding the maximum total vaccinations for each vaccine type
is equivalent to finding the total number of vaccinations for each vaccine type. Using those values, we can find the global total number of vaccinations across all countries,
for each vaccine type. This is done by summing up the total_vaccinations value for each vaccine type for all countries. */

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 11: What is the total population in Asia?

// Take note, location is used here instead of continent. Further explanation can be found below.
db.covid19data.aggregate([
    {$match: {location: "Asia"}},
    {$project: {continent: 1,  population: {$convert: {input: "$population", to: "double"}}}},
    {$group: {
        _id: {groupByLocation: "$location"}, 
       total_population: {$last: "$population"}
    }}
])
// ANS: The total population in Asia is 4,639,847,425.

/* In the query below, continent was considered since continent also consists of countries in Asia. Resulting total population of Asia is smaller than 
above at 4,614,068,610, when filtering by continent instead. */
db.covid19data.aggregate([
    {$match: {continent: "Asia"}},
    {$project: {continent: 1,  population: {$convert: {input: "$population", to: "double"}}}},
    {$group: {
        _id: {groupByContinent: "$continent"}, 
       population: {$addToSet: "$population"}
    }},
    {$project: {_id: 0, total_population: {$sum: "$population"}}}
])

/* This is because, in this table, there are only 50 locations under the Asia continent. However, Asia should contain 51 locations. We discovered that 
North Korea is not included as a location under Asia. Data accuracy and integrity is compromised. Hence, it would be inaccurate to filter based on 
continents. */

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 12: What is the total population among the ten ASEAN countries?

db.covid19data.aggregate([
    {$match: {$or: [
        {location: "Brunei"},
        {location: "Myanmar"},
        {location: "Cambodia"},
        {location: "Indonesia"},
        {location: "Laos"},
        {location: "Malaysia"},
        {location: "Philippines"},
        {location: "Singapore"},
        {location: "Thailand"},
        {location: "Vietnam"}
    ]}},
    {$project:{
        location: 1,
        population: {$convert: {input: "$population", to: "double"}}
    }},
    {$group: {
        _id: {groupByLocation: "$location"}
       population: {$last: "$population"}
    }},
    {$group:{
        _id: null,
       total_population: {$sum: "$population"}
    }}
])
// ANS: The total population among ASEAN countries is 667,301,412.

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 13: Generate a list of unique data sources (source_name)

db.country_vaccinations.distinct("source_name")

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 14: Specific to Singapore, display the daily total_vaccinations starting (inclusive) March-1 2021 through (inclusive) May-31 2021. 

db.country_vaccinations.aggregate([
    {$project: {
        daily_vaccinations: {$convert: {input: "$daily_vaccinations", to: "double"}},
        daily_vaccinations_raw: {$convert: {input: "$daily_vaccinations_raw", to: "double"}},
        country: 1,
        date: {$convert: {input: "$date", to: "date"}}
    }},
    {$match: {$and:[
        {date: {$gte: ISODate("2021-03-01")}},
        {date: {$lte: ISODate("2021-05-31")}},
        {country: "Singapore"}
    ]
    }},
    {$sort: {date: 1}}
])

//---------------------------------------------------------------------------------------------------------------------------------------------------------
// Qn 15: When is the first batch of vaccinations recorded in Singapore?

db.country_vaccinations.aggregate([
    {$project: {_id:0,country:1, total_vaccinations: {$convert: {input: "$total_vaccinations", to: "int"}}, date: {$convert: {input: "$date", to: "date"}}}}
    {$match: {country: "Singapore"}},
    {$match: {"total_vaccinations": {$gt:0}}},
    {$group: {_id: {groupByCountry: "$country"}, first_batch: {$min: "$date"}}}
])
// ANS: The first batch of vaccinations recorded in Singapore is 11/01/2021 (11 Jan 2021).

//---------------------------------------------------------------------------------------------------------------------------------------------------------
/* Qn 16: Based on the date identified in (5), specific to Singapore, compute the total number of new cases thereafter.For instance, if the date 
identified in (5) is Jan-1 2021, the total number of new cases will be the sum of new cases starting from (inclusive) Jan-1 to the last date in the dataset.*/

db.covid19data.aggregate([
    {$match: {location: "Singapore"}},
    {$project: {location: 1, new_cases: {$convert: {input: "$new_cases", to: "double"}}, date: {$convert: {input: "$date", to: "date"}}}},
    {$match: {date: {$gte: ISODate('2021-01-11')}}},
    {$group: {_id: {groupByLocation: "$location"}, totalNewCases: {$sum: "$new_cases"}}}
])
// ANS: The total number of new cases will be 3,710.

//---------------------------------------------------------------------------------------------------------------------------------------------------------
/* Qn 17:  Compute the total number of new cases in Singapore before the date identified in (5). For instance, if the date identified in (5) is Jan-1 2021 
and the first date recorded (in Singapore) in the dataset is Feb-1 2020, the total number of new cases will be the sum of new cases starting from (inclusive) 
Feb-1 2020 through (inclusive) Dec-31 2020.*/

db.covid19data.aggregate([
    {$match: {location: "Singapore"}},
    {$project: {location: 1,  new_cases: {$convert: {input: "$new_cases", to: "double"}}, date: {$convert: {input: "$date", to: "date"}}}},
    {$match: {date: {$lt: ISODate('2021-01-11')}}},
    {$group: {_id: {groupByLocation: "$location"}, totalNewCases: {$sum: "$new_cases"}}}
])
// ANS: The total number of new cases will be 58,907.

//---------------------------------------------------------------------------------------------------------------------------------------------------------
/* Qn 18: Herd immunity estimation. On a daily basis, specific to Germany, calculate the percentage of new cases and total vaccinations on each available 
vaccine in relation to its population. */

// Converting the type of date from string to date in country_vaccinations_by_manufacturer collection
db.country_vaccinations_by_manufacturer.updateMany(
  {},
  [{$set:{"date":{$toDate:"$date"}}}]
)

// Joining covid19data collection with country_vaccinations_by_manufacturer collection
db.covid19data.aggregate([
    {
        $match : {location : "Germany"}
    },
    {
        $project : {
            date : {$convert : {input : "$date", to : "date"}},
            new_cases : {$convert : {input :"$new_cases", to : "decimal"}},
            population : {$convert : {input : "$population", to : "decimal"}}
        }
    },
    {
        $sort : {"date" : 1}
    },
    {
        $lookup:{
            from:"country_vaccinations_by_manufacturer",
            localField : "date",
            foreignField : "date",
            as:"totalVac"
        }
    },
    {
        $unwind: "$totalVac"
    },
    {
        $match: {"totalVac.location": "Germany"} 
    },
    {
            $project : {
                _id: 0,
                "date" : 1,
                "percentageNewCases" : {$divide : ["$new_cases","$population"]},
                totalVac: 1
        }
    },
    {
        $project: {
            "totalVac.vaccine": 1,
            "totalVac.total_vaccinations": 1,
            date: 1,
            percentageNewCases:1
        }
    },
    {
        $sort: {"date": 1}
    }
])

//---------------------------------------------------------------------------------------------------------------------------------------------------------
/* Qn 19: Vaccination Drivers. Specific to Germany, based on each daily new case, display the total vaccinations of each available vaccines after 20 days, 
30 days, and 40 days. */

// Joining covid19data collection with country_vaccinations_by_manufacturer collection
db.covid19data.aggregate([
    {
        $match : {"location" : "Germany"}
    },
    {
        $project : {
            date : {$convert : {input : "$date" , to : "date"}},
            new_cases : {$convert : {input : "$new_cases", to : "double"}},
            location : "$location"
        }
    },
    {$project: {_id: 0, date: 1, "20DaysLater": {$dateAdd:  {
                                                            startDate: "$date",
                                                            unit: "day",
                                                            amount: 20
                                                            }
                                                },
                                 "30DaysLater": {$dateAdd:  {
                                                            startDate: "$date",
                                                            unit: "day",
                                                            amount: 30
                                                            }
                                                },
                                 "40DaysLater": {$dateAdd:  {
                                                            startDate: "$date",
                                                            unit: "day",
                                                            amount: 40
                                                            }
                                                }
            
    }},
    {
        $sort : {date : 1}
    },
    {
        $lookup : {
            from : "country_vaccinations_by_manufacturer",
            localField: "20DaysLater",
            foreignField : "date"
            as : "20DaysLaterVacc"
        }
    },
    {
        $lookup : {
            from : "country_vaccinations_by_manufacturer",
            localField: "30DaysLater",
            foreignField : "date"
            as : "30DaysLaterVacc"
        }
    },
    {
        $lookup : {
            from : "country_vaccinations_by_manufacturer",
            localField: "40DaysLater",
            foreignField : "date"
            as : "40DaysLaterVacc"
        }
    },
    {
        $unwind: "$20DaysLaterVacc"
    },
    {
        $unwind: "$30DaysLaterVacc"
    },
    {
        $unwind: "$40DaysLaterVacc"
    },
    {
        $match:{
            $and:[
                {"20DaysLaterVacc.location": "Germany"},
                {"30DaysLaterVacc.location": "Germany"},
                {"40DaysLaterVacc.location": "Germany"}
                ]
    }},
    {
        $project: {
            date: 1
            "20DaysLaterVacc": 1,
            "30DaysLaterVacc": 1,
            "40DaysLaterVacc": 1,
        }
    },
    {
        $project: {
            "20DaysLaterVacc._id": 0,
            "20DaysLaterVacc.location": 0,
            "30DaysLaterVacc._id": 0,
            "30DaysLaterVacc.location": 0,
            "40DaysLaterVacc._id": 0,
            "40DaysLaterVacc.location": 0,
        }
    }
])

//---------------------------------------------------------------------------------------------------------------------------------------------------------
/* Qn 20: Vaccination Effects. Specific to Germany, on a daily basis, based on the total number of accumulated vaccinations (sum of total_vaccinations of 
each vaccine in a day), generate the daily new cases after 21 days, 60 days, and 120 days. */

// Converting the type of date from string to date in covid19data collection
db.covid19data.updateMany(
  {},
  [{$set:{"date":{$toDate:"$date"}}}]
)

// Joining country_vaccinations_by_manufacturer collection with covid19data collection
db.country_vaccinations_by_manufacturer.aggregate([
    {
        $match : {location : "Germany"}
    },
    {
        $project : {
            date: 1,
            total_vaccinations : {$convert : {input : "$total_vaccinations", to : "double"}},
            vaccine: 1
        }
    },
    {
        $group :{
            _id : {groupByDate : "$date"}
            total_vaccinations : {$sum : "$total_vaccinations"}
        }
    },
    {
        $sort: {_id: 1} 
    },
    {
        $project : {
            date : 1,
            total_vaccinations : 1,
            "21DaysLater" : {$dateAdd : {startDate :"$_id.groupByDate", unit : "day", amount : 21}},
            "60DaysLater" : {$dateAdd : {startDate :"$_id.groupByDate", unit : "day", amount : 60}},
            "120DaysLater" : {$dateAdd : {startDate :"$_id.groupByDate", unit : "day", amount : 120}}
        }
    },
    {
        $sort : {"_id.groupByDate" : 1}
    },
    {
        $lookup : {
            from : "covid19data",
            localField : "21DaysLater",
            foreignField : "date",
            as : "dailyCasesAfter21Days"
        }
    },
    {
        $lookup : {
            from : "covid19data",
            localField : "60DaysLater",
            foreignField : "date",
            as : "dailyCasesAfter60Days"
        }
    },
    {
        $lookup : {
            from : "covid19data",
            localField : "120DaysLater",
            foreignField : "date",
            as : "dailyCasesAfter120Days"
        }
    },
    {
        $unwind: "$dailyCasesAfter21Days"
    },
    {
        $unwind: "$dailyCasesAfter60Days"
    },
    {
        $unwind: "$dailyCasesAfter120Days"
    },
    {
        $match:{
            $and:[
                {"dailyCasesAfter21Days.location": "Germany"},
                {"dailyCasesAfter60Days.location": "Germany"},
                {"dailyCasesAfter120Days.location": "Germany"}
                ]
    }},
    {
        $project: {
            "_id.groupByDate": 1,
            total_vaccinations: 1,
            "dailyCasesAfter21Days.date": 1,
            "dailyCasesAfter21Days.new_cases": 1,
            "dailyCasesAfter60Days.date": 1,
            "dailyCasesAfter60Days.new_cases": 1,
            "dailyCasesAfter120Days.date": 1,
            "dailyCasesAfter120Days.new_cases": 1
        }
    }
])

//---------------------------------------------------------------------------------------------------------------------------------------------------------
